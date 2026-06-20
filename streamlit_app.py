"""
Conflux — Unified Crypto Market Data Gateway
============================================
v3 dispenses with Binance L2 (Streamlit Cloud egress is blocked from
Binance's REST snapshot endpoint) and instead demonstrates two different
real-world L2 synchronization protocols on the venues that DO work:

  - Coinbase (`level2_batch` channel):
      Server pushes one `snapshot` message after subscribe, then streams
      `l2update` messages batched every 50ms. No sequence numbers — Coinbase
      guarantees ordered delivery on the channel. Recovery on disconnect is
      simply "wait for the new snapshot the server sends on resubscribe."
      (The plain `level2` channel requires authentication since 2023-08-01;
      `level2_batch` is the public, unauthenticated equivalent.)

  - Kraken v1 (`book-25` channel):
      Server pushes an initial snapshot (`as`/`bs`), then streams updates
      (`a`/`b`) with a CRC32 checksum (`c`) on each message computed over
      the top-10 price/volume strings. If our local CRC doesn't match,
      our book has silently desynced — we force-reconnect to resubscribe
      and get a fresh snapshot.

Binance still appears for trades and BBO (those WebSocket streams aren't
affected), but no L2.
"""

import json
import threading
import time
import zlib
from collections import deque
from dataclasses import asdict, dataclass

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import websocket
import bcrypt
from streamlit_autorefresh import st_autorefresh

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
VENUES = ["binance", "coinbase", "kraken"]
SYMBOLS = ["BTC-USD", "ETH-USD"]

# --- branding -------------------------------------------------------------- #
APP_NAME = "Conflux"
APP_TAGLINE = "Every venue. One stream."
APP_SUBTITLE = (
    "Live normalized feeds from Binance (trades/BBO), Coinbase, and Kraken, "
    "with real L2 order book maintainers on Coinbase and Kraken."
)


def logo_svg(width: int = 380, show_wordmark: bool = True) -> str:
    """Inline SVG: three venue strands converging into one normalized stream.
    Themeable, scales cleanly, no external image hosting required."""
    wordmark = (
        '<text x="232" y="70" font-family="Inter, Segoe UI, sans-serif" '
        'font-size="34" font-weight="700" fill="currentColor" '
        'letter-spacing="-0.5">Conflux</text>'
    ) if show_wordmark else ""
    vb_w = 470 if show_wordmark else 224
    return f'''
<svg width="{width}" viewBox="0 0 {vb_w} 120" xmlns="http://www.w3.org/2000/svg"
     role="img" aria-label="Conflux logo">
  <defs>
    <linearGradient id="cfx_merged" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0%" stop-color="#f7931a"/>
      <stop offset="100%" stop-color="#ffd27f"/>
    </linearGradient>
  </defs>
  <!-- three venue strands flowing in from the left -->
  <path d="M 10 28 C 70 28, 78 60, 128 60" fill="none"
        stroke="#26a69a" stroke-width="6" stroke-linecap="round"/>
  <path d="M 10 60 C 78 60, 80 60, 128 60" fill="none"
        stroke="#5c9ded" stroke-width="6" stroke-linecap="round"/>
  <path d="M 10 92 C 70 92, 78 60, 128 60" fill="none"
        stroke="#ef5350" stroke-width="6" stroke-linecap="round"/>
  <!-- convergence node -->
  <circle cx="130" cy="60" r="10" fill="#f7931a"/>
  <!-- single merged stream out -->
  <path d="M 130 60 L 206 60" fill="none"
        stroke="url(#cfx_merged)" stroke-width="9" stroke-linecap="round"/>
  <circle cx="210" cy="60" r="6" fill="#ffd27f"/>
  {wordmark}
</svg>'''.strip()

SYMBOL_MAP = {
    "binance":  {"BTC-USD": "btcusdt", "ETH-USD": "ethusdt"},
    "coinbase": {"BTC-USD": "BTC-USD", "ETH-USD": "ETH-USD"},
    "kraken":   {"BTC-USD": "XBT/USD", "ETH-USD": "ETH/USD"},
}

# Which (venue, symbol) pairs run a full L2 maintainer
L2_TARGETS = [
    ("coinbase", "BTC-USD"), ("coinbase", "ETH-USD"),
    ("kraken",   "BTC-USD"), ("kraken",   "ETH-USD"),
]

LADDER_DEPTH      = 20
KRAKEN_DEPTH      = 25     # we subscribe to book-25; checksum uses top 10
KRAKEN_CRC_STRICT = True   # on CRC mismatch, force-reconnect to resubscribe
MAX_TRADES_BUFFER = 1000
MAX_EVENT_LOG     = 200
REFRESH_INTERVAL_MS = 1000

# --- feature config -------------------------------------------------------- #
SLO_LATENCY_MS    = 250      # ingest-latency SLO; breaches counted per venue
STALE_SECONDS     = 12       # connected but silent this long => STALE
WATCHDOG_INTERVAL = 4        # seconds between watchdog sweeps
LARGE_TRADE_USD   = 50_000   # notional threshold for the large-trade alerter
VWAP_WINDOW       = 300      # trades retained per symbol for rolling VWAP

# --- arbitrage / fee config ------------------------------------------------ #
# Taker fees are the % paid when crossing the spread (market order). These are
# representative public spot taker fees at a low VIP tier and WILL differ for
# your actual account/volume — they're editable defaults, not gospel.
# Withdrawal fees are a flat cost (in units of the BASE asset) to move coins
# off the venue; arbitrage requires moving the asset between venues, so the
# withdrawal cost of the SELL-side venue applies to the leg being moved.
VENUE_FEES = {
    # venue:   taker_fee_pct (as fraction),  withdrawal (base-asset units) by symbol
    "binance":  {"taker": 0.00100, "withdraw": {"BTC-USD": 0.0002, "ETH-USD": 0.0015}},
    "coinbase": {"taker": 0.00120, "withdraw": {"BTC-USD": 0.0001, "ETH-USD": 0.0010}},
    "kraken":   {"taker": 0.00160, "withdraw": {"BTC-USD": 0.00005, "ETH-USD": 0.0005}},
}
# Only flag an opportunity if net edge clears this (basis points). Real arbs
# need a margin above zero to cover slippage, timing, and execution risk.
ARB_MIN_NET_BPS = 5.0
ARB_STALE_MS = 3000  # ignore a venue's quote for arb if older than this

# --------------------------------------------------------------------------- #
# Normalized schema
# --------------------------------------------------------------------------- #
@dataclass
class Trade:
    venue: str
    symbol: str
    price: float
    qty: float
    side: str
    exchange_ts: float
    ingest_ts: float
    trade_id: str = ""


@dataclass
class BBO:
    venue: str
    symbol: str
    bid: float
    bid_size: float
    ask: float
    ask_size: float
    exchange_ts: float
    ingest_ts: float


# --------------------------------------------------------------------------- #
# Arbitrage math
# --------------------------------------------------------------------------- #
def compute_arbitrage(symbol: str, quotes: dict, fees: dict,
                      now_ms: float, stale_ms: float = ARB_STALE_MS) -> dict | None:
    """Given the latest BBO per venue for one symbol, find the best
    buy-low / sell-high pair and compute both the gross and net-of-fees edge.

    quotes: {venue: {"bid","ask","bid_size","ask_size","ingest_ts"}}
    fees:   VENUE_FEES

    Returns a dict describing the best opportunity, or None if fewer than two
    venues have fresh quotes.

    Arbitrage mechanics modeled:
      - BUY at the cheapest venue's ASK (you cross the spread, pay taker fee)
      - SELL at the richest venue's BID (you cross the spread, pay taker fee)
      - MOVE the asset from buy-venue to sell-venue, paying the buy-venue's
        withdrawal fee (a flat amount in base-asset units), converted to a
        cost in quote terms at the buy price.
    Net edge = sell proceeds after fee − buy cost after fee − withdrawal cost,
    expressed in basis points of the buy cost.
    """
    fresh = {v: q for v, q in quotes.items()
             if q and q.get("ask", 0) > 0 and q.get("bid", 0) > 0
             and (now_ms - q.get("ingest_ts", 0)) <= stale_ms}
    if len(fresh) < 2:
        return None

    # Best venue to BUY: lowest ask. Best venue to SELL: highest bid.
    buy_venue = min(fresh, key=lambda v: fresh[v]["ask"])
    sell_venue = max(fresh, key=lambda v: fresh[v]["bid"])
    if buy_venue == sell_venue:
        # Same venue is both cheapest-ask and highest-bid → no cross-venue arb
        # Pick the next-best distinct pair if available.
        buys = sorted(fresh, key=lambda v: fresh[v]["ask"])
        sells = sorted(fresh, key=lambda v: -fresh[v]["bid"])
        pair = None
        for b in buys:
            for s in sells:
                if b != s:
                    pair = (b, s); break
            if pair: break
        if not pair:
            return None
        buy_venue, sell_venue = pair

    buy_ask = fresh[buy_venue]["ask"]
    sell_bid = fresh[sell_venue]["bid"]

    gross_bps = (sell_bid - buy_ask) / buy_ask * 1e4

    buy_taker = fees[buy_venue]["taker"]
    sell_taker = fees[sell_venue]["taker"]
    withdraw_base = fees[buy_venue]["withdraw"].get(symbol, 0.0)

    # Per 1 unit of base asset:
    buy_cost = buy_ask * (1 + buy_taker)            # pay ask + taker
    sell_proceeds = sell_bid * (1 - sell_taker)     # receive bid − taker
    withdraw_cost_quote = withdraw_base * buy_ask   # flat base fee in quote terms

    net_per_unit = sell_proceeds - buy_cost - withdraw_cost_quote
    net_bps = net_per_unit / buy_cost * 1e4

    # Executable size is limited by the smaller of the two top-of-book sizes
    max_size = min(fresh[buy_venue].get("ask_size", 0),
                   fresh[sell_venue].get("bid_size", 0))
    est_profit_usd = net_per_unit * max_size if max_size > 0 else 0.0

    return {
        "symbol": symbol,
        "buy_venue": buy_venue, "sell_venue": sell_venue,
        "buy_ask": buy_ask, "sell_bid": sell_bid,
        "gross_bps": gross_bps, "net_bps": net_bps,
        "buy_taker_pct": buy_taker * 100, "sell_taker_pct": sell_taker * 100,
        "withdraw_base": withdraw_base, "withdraw_cost_quote": withdraw_cost_quote,
        "max_size": max_size, "est_profit_usd": est_profit_usd,
        "profitable": net_bps >= ARB_MIN_NET_BPS,
    }


# --------------------------------------------------------------------------- #
# L2 OrderBook (string-keyed so Kraken CRC32 keeps exact precision)
# --------------------------------------------------------------------------- #
class OrderBook:
    """Bids/asks stored as price_str -> qty_str. Strings preserve the venue's
    exact decimal format which Kraken's CRC32 algorithm depends on."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}
        self.last_event_ts: float = 0.0
        self.last_apply_ts: float = 0.0

    def load_snapshot(self, bids, asks) -> None:
        self.bids.clear()
        self.asks.clear()
        for lvl in bids:
            p, q = str(lvl[0]), str(lvl[1])
            if float(q) > 0:
                self.bids[p] = q
        for lvl in asks:
            p, q = str(lvl[0]), str(lvl[1])
            if float(q) > 0:
                self.asks[p] = q
        self.last_apply_ts = time.time() * 1000.0

    def apply_updates(self, bid_levels, ask_levels, exchange_ts: float = 0.0) -> None:
        for lvl in bid_levels:
            p, q = str(lvl[0]), str(lvl[1])
            if float(q) == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q
        for lvl in ask_levels:
            p, q = str(lvl[0]), str(lvl[1])
            if float(q) == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q
        self.last_event_ts = exchange_ts
        self.last_apply_ts = time.time() * 1000.0

    def top_n_strings(self, n: int):
        """Top-n levels as (price_str, qty_str) tuples — asks ascending,
        bids descending. Used for both display and checksum."""
        asks = sorted(self.asks.items(), key=lambda kv: float(kv[0]))[:n]
        bids = sorted(self.bids.items(), key=lambda kv: -float(kv[0]))[:n]
        return asks, bids

    def top_n_floats(self, n: int = LADDER_DEPTH):
        asks_s, bids_s = self.top_n_strings(n)
        return ([(float(p), float(q)) for p, q in bids_s],
                [(float(p), float(q)) for p, q in asks_s])

    def bbo(self):
        if not self.bids or not self.asks:
            return None
        bb = max(self.bids.keys(), key=float)
        ba = min(self.asks.keys(), key=float)
        return float(bb), float(self.bids[bb]), float(ba), float(self.asks[ba])


# --------------------------------------------------------------------------- #
# Coinbase L2 Maintainer
# Protocol: in-band snapshot + ordered stream (no sequence numbers).
# --------------------------------------------------------------------------- #
class CoinbaseL2Maintainer:
    PROTOCOL = "in-band snapshot + ordered stream"

    def __init__(self, store: "DataStore", symbol_canonical: str, native: str):
        self.store = store
        self.venue = "coinbase"
        self.symbol = symbol_canonical
        self.native = native
        self.book = OrderBook(symbol_canonical)
        self.state = "INIT"
        self.snapshots_received = 0
        self.updates_applied = 0
        self.last_event_ts_iso: str | None = None
        self.timestamp_regressions = 0
        self.lock = threading.Lock()

    def on_snapshot(self, payload: dict) -> None:
        with self.lock:
            self.book.load_snapshot(payload.get("bids", []),
                                    payload.get("asks", []))
            self.state = "LIVE"
            self.snapshots_received += 1
            self.store.log("INFO", self.venue, self.symbol,
                           f"Snapshot received "
                           f"({len(payload.get('bids', []))} bids / "
                           f"{len(payload.get('asks', []))} asks). State → LIVE.")
            self._publish_top()

    def on_l2update(self, payload: dict) -> None:
        with self.lock:
            if self.state != "LIVE":
                # Drop updates that arrive before the snapshot
                return
            t = payload.get("time")
            if self.last_event_ts_iso and t and t < self.last_event_ts_iso:
                self.timestamp_regressions += 1
                self.store.log("WARN", self.venue, self.symbol,
                               f"Timestamp regression: "
                               f"{self.last_event_ts_iso} → {t}")
            self.last_event_ts_iso = t
            bids, asks = [], []
            for change in payload.get("changes", []):
                if len(change) < 3:
                    continue
                side, price, size = change[0], change[1], change[2]
                (bids if side == "buy" else asks).append((price, size))
            ex_ts = (pd.Timestamp(t).timestamp() * 1000.0) if t else 0.0
            self.book.apply_updates(bids, asks, exchange_ts=ex_ts)
            self.updates_applied += 1
            self._publish_top()

    def on_disconnect(self) -> None:
        with self.lock:
            if self.state == "LIVE":
                self.state = "INIT"
                self.store.log("WARN", self.venue, self.symbol,
                               "Disconnect — awaiting fresh snapshot on "
                               "reconnect.")

    def _publish_top(self) -> None:
        b = self.book.bbo()
        if b is None:
            return
        bb, bb_sz, ba, ba_sz = b
        now = time.time() * 1000.0
        self.store.set_bbo(BBO(
            venue=self.venue, symbol=self.symbol,
            bid=bb, bid_size=bb_sz, ask=ba, ask_size=ba_sz,
            exchange_ts=self.book.last_event_ts or now, ingest_ts=now,
        ))

    def snapshot(self) -> dict:
        with self.lock:
            bids, asks = self.book.top_n_floats(LADDER_DEPTH)
            now = time.time() * 1000.0
            return {
                "protocol": self.PROTOCOL,
                "state": self.state,
                "bids": bids, "asks": asks,
                "snapshots_received": self.snapshots_received,
                "updates_applied": self.updates_applied,
                "checksum_failures": 0,
                "checksum_successes": 0,
                "timestamp_regressions": self.timestamp_regressions,
                "n_bid_levels": len(self.book.bids),
                "n_ask_levels": len(self.book.asks),
                "book_age_ms": (now - self.book.last_apply_ts
                                if self.book.last_apply_ts else None),
            }


# --------------------------------------------------------------------------- #
# Kraken L2 Maintainer
# Protocol: in-band snapshot + CRC32 checksum on top-10.
# --------------------------------------------------------------------------- #
class KrakenL2Maintainer:
    PROTOCOL = "in-band snapshot + CRC32 checksum"

    def __init__(self, store: "DataStore", symbol_canonical: str, native: str):
        self.store = store
        self.venue = "kraken"
        self.symbol = symbol_canonical
        self.native = native  # "XBT/USD"
        self.book = OrderBook(symbol_canonical)
        self.state = "INIT"
        self.snapshots_received = 0
        self.updates_applied = 0
        self.checksum_successes = 0
        self.checksum_failures = 0
        self.resyncs_triggered = 0
        self.force_close = False  # ws layer polls this to trigger resubscribe
        self.lock = threading.Lock()

    def on_snapshot(self, payload: dict) -> None:
        with self.lock:
            self.book.load_snapshot(payload.get("bs", []),
                                    payload.get("as", []))
            self.state = "LIVE"
            self.snapshots_received += 1
            self.store.log("INFO", self.venue, self.symbol,
                           f"Snapshot received "
                           f"({len(payload.get('bs', []))} bids / "
                           f"{len(payload.get('as', []))} asks). State → LIVE.")
            self._publish_top()

    def on_update(self, bid_levels, ask_levels, checksum: str | None) -> None:
        with self.lock:
            if self.state != "LIVE":
                return
            # Kraken update levels are [price, volume, timestamp] — we only
            # need the first two; the OrderBook will str() them.
            bids = [(lvl[0], lvl[1]) for lvl in bid_levels]
            asks = [(lvl[0], lvl[1]) for lvl in ask_levels]
            self.book.apply_updates(bids, asks)
            self.updates_applied += 1

            if checksum is not None:
                result = self._verify_checksum(checksum)
                if result is True:
                    self.checksum_successes += 1
                elif result is False:
                    self.checksum_failures += 1
                    self.store.log(
                        "WARN", self.venue, self.symbol,
                        f"CRC32 mismatch — local book has desynced. "
                        f"Forcing resubscribe. (fail #{self.checksum_failures})"
                    )
                    if KRAKEN_CRC_STRICT:
                        self.state = "RESYNC"
                        self.resyncs_triggered += 1
                        self.force_close = True
                        return
                # else: None means not enough levels to compute yet
            self._publish_top()

    def _verify_checksum(self, expected: str):
        asks_top, bids_top = self.book.top_n_strings(10)
        if len(asks_top) < 10 or len(bids_top) < 10:
            return None
        return self.compute_checksum(asks_top, bids_top) == int(expected)

    @staticmethod
    def compute_checksum(asks_top10, bids_top10) -> int:
        """CRC32 over the concatenation of price + volume strings for the
        top-10 asks (ascending) then top-10 bids (descending). For each
        string, remove the decimal point and strip leading zeros.

        See: https://docs.kraken.com/api/docs/websocket-v1/book#checksum
        """
        parts = []
        for p, q in asks_top10:
            parts.append(p.replace(".", "").lstrip("0"))
            parts.append(q.replace(".", "").lstrip("0"))
        for p, q in bids_top10:
            parts.append(p.replace(".", "").lstrip("0"))
            parts.append(q.replace(".", "").lstrip("0"))
        return zlib.crc32("".join(parts).encode("ascii"))

    def on_disconnect(self) -> None:
        with self.lock:
            if self.state in ("LIVE", "RESYNC"):
                self.state = "INIT"
                self.store.log("WARN", self.venue, self.symbol,
                               "Disconnect — awaiting fresh snapshot on "
                               "reconnect.")

    def consume_force_close(self) -> bool:
        with self.lock:
            f = self.force_close
            self.force_close = False
            return f

    def inject_corruption(self) -> bool:
        """FAULT INJECTION: silently delete the best-bid level from our local
        book. The book is now desynced from Kraken's, but no error is raised —
        exactly like a real dropped message. The next update Kraken sends
        carries a CRC32 over its (correct) top-10; our corrupted book will
        produce a different CRC, the mismatch is detected, and the normal
        resync path fires. This demonstrates genuine checksum-driven gap
        detection rather than a simulated one."""
        with self.lock:
            if self.state != "LIVE" or not self.book.bids:
                self.store.log("WARN", self.venue, self.symbol,
                               "Corruption requested but book is not LIVE.")
                return False
            victim = max(self.book.bids.keys(), key=float)
            del self.book.bids[victim]
            self.store.log(
                "WARN", self.venue, self.symbol,
                f"FAULT INJECTED — best-bid level {victim} silently deleted "
                f"from local book. Next CRC32 from Kraken should mismatch.")
            return True

    def _publish_top(self) -> None:
        b = self.book.bbo()
        if b is None:
            return
        bb, bb_sz, ba, ba_sz = b
        now = time.time() * 1000.0
        self.store.set_bbo(BBO(
            venue=self.venue, symbol=self.symbol,
            bid=bb, bid_size=bb_sz, ask=ba, ask_size=ba_sz,
            exchange_ts=self.book.last_event_ts or now, ingest_ts=now,
        ))

    def snapshot(self) -> dict:
        with self.lock:
            bids, asks = self.book.top_n_floats(LADDER_DEPTH)
            now = time.time() * 1000.0
            return {
                "protocol": self.PROTOCOL,
                "state": self.state,
                "bids": bids, "asks": asks,
                "snapshots_received": self.snapshots_received,
                "updates_applied": self.updates_applied,
                "checksum_failures": self.checksum_failures,
                "checksum_successes": self.checksum_successes,
                "resyncs_triggered": self.resyncs_triggered,
                "n_bid_levels": len(self.book.bids),
                "n_ask_levels": len(self.book.asks),
                "book_age_ms": (now - self.book.last_apply_ts
                                if self.book.last_apply_ts else None),
            }


# --------------------------------------------------------------------------- #
# Downstream consumers — fan-out demonstration
# Each consumer independently subscribes to the normalized trade stream. The
# DataStore dispatches every trade to all consumers, mirroring a pub/sub
# fan-out (NATS/Kafka) without the network layer. Each consumer's
# "messages_received" counter proves it saw the full stream independently.
# --------------------------------------------------------------------------- #
class Consumer:
    name = "base"
    kind = "base"

    def __init__(self):
        self.lock = threading.Lock()
        self.messages_received = 0

    def on_trade(self, trade: Trade) -> None:
        with self.lock:
            self.messages_received += 1
        self._process(trade)

    def _process(self, trade: Trade) -> None:  # override
        pass

    def snapshot(self) -> dict:  # override
        return {"messages": self.messages_received}


class VWAPConsumer(Consumer):
    """Rolling volume-weighted average price per symbol."""
    name = "VWAP engine"
    kind = "vwap"

    def __init__(self, window: int = VWAP_WINDOW):
        super().__init__()
        self.window = window
        self.by_symbol: dict[str, deque] = {}

    def _process(self, trade: Trade) -> None:
        with self.lock:
            dq = self.by_symbol.setdefault(trade.symbol,
                                           deque(maxlen=self.window))
            dq.append((trade.price, trade.qty))

    def snapshot(self) -> dict:
        with self.lock:
            rows = []
            for sym, dq in sorted(self.by_symbol.items()):
                tot_q = sum(q for _, q in dq)
                if tot_q > 0:
                    rows.append({
                        "symbol": sym,
                        "vwap": sum(p * q for p, q in dq) / tot_q,
                        "trades": len(dq),
                    })
            return {"messages": self.messages_received, "rows": rows}


class VolumeConsumer(Consumer):
    """Running trade count and traded volume per venue+symbol."""
    name = "Volume tracker"
    kind = "volume"

    def __init__(self):
        super().__init__()
        self.stats: dict[tuple, dict] = {}

    def _process(self, trade: Trade) -> None:
        with self.lock:
            s = self.stats.setdefault((trade.venue, trade.symbol),
                                      {"count": 0, "volume": 0.0})
            s["count"] += 1
            s["volume"] += trade.qty

    def snapshot(self) -> dict:
        with self.lock:
            rows = [{"venue": v, "symbol": s,
                     "trades": d["count"], "volume": d["volume"]}
                    for (v, s), d in sorted(self.stats.items())]
            return {"messages": self.messages_received, "rows": rows}


class AlertConsumer(Consumer):
    """Fires an alert when a single trade's notional exceeds a threshold."""
    name = "Large-trade alerter"
    kind = "alert"

    def __init__(self, threshold_usd: float = LARGE_TRADE_USD,
                 max_alerts: int = 30):
        super().__init__()
        self.threshold = threshold_usd
        self.alerts: deque = deque(maxlen=max_alerts)

    def _process(self, trade: Trade) -> None:
        notional = trade.price * trade.qty
        if notional >= self.threshold:
            with self.lock:
                self.alerts.append({
                    "ts": trade.ingest_ts, "venue": trade.venue,
                    "symbol": trade.symbol, "side": trade.side,
                    "price": trade.price, "qty": trade.qty,
                    "notional": notional,
                })

    def snapshot(self) -> dict:
        with self.lock:
            return {"messages": self.messages_received,
                    "alerts": list(self.alerts)[::-1]}


# --------------------------------------------------------------------------- #
# Shared DataStore
# --------------------------------------------------------------------------- #
class DataStore:
    def __init__(self):
        self.trades: deque = deque(maxlen=MAX_TRADES_BUFFER)
        self.bbo: dict = {}
        self.lock = threading.Lock()
        self.health = {
            v: {"connected": False, "messages": 0,
                "transport_errors": 0, "parse_errors": 0,
                "last_msg_ts": 0.0, "slo_breaches": 0,
                "derived_state": "DOWN"}
            for v in VENUES
        }
        self.events: deque = deque(maxlen=MAX_EVENT_LOG)
        self.event_lock = threading.Lock()
        # Audit log — security-sensitive events (logins, role-gated actions)
        self.audit_events: deque = deque(maxlen=500)
        self.audit_lock = threading.Lock()
        self.l2: dict = {}
        for venue, sym in L2_TARGETS:
            native = SYMBOL_MAP[venue][sym]
            if venue == "coinbase":
                self.l2[(venue, sym)] = CoinbaseL2Maintainer(self, sym, native)
            elif venue == "kraken":
                self.l2[(venue, sym)] = KrakenL2Maintainer(self, sym, native)
        # downstream consumers (fan-out)
        self.consumers: list[Consumer] = [
            VWAPConsumer(), VolumeConsumer(), AlertConsumer()]
        # WebSocket handles, for fault injection
        self._ws_handles: dict = {}
        self._ws_lock = threading.Lock()
        self.stop_event = threading.Event()
        self._started = False

    # mutators
    def add_trade(self, t: Trade):
        with self.lock:
            self.trades.append(t)
            h = self.health[t.venue]
            h["messages"] += 1
            h["last_msg_ts"] = t.ingest_ts
            lat = t.ingest_ts - t.exchange_ts
            # count an SLO breach only for sane latencies (clock skew excluded)
            if 0 <= lat <= 60_000 and lat > SLO_LATENCY_MS:
                h["slo_breaches"] += 1
        # fan out to downstream consumers OUTSIDE the store lock
        for c in self.consumers:
            try:
                c.on_trade(t)
            except Exception:
                pass

    def set_bbo(self, b: BBO):
        with self.lock:
            self.bbo[(b.venue, b.symbol)] = b
            h = self.health[b.venue]; h["messages"] += 1; h["last_msg_ts"] = b.ingest_ts

    def record_transport_error(self, v): 
        with self.lock: self.health[v]["transport_errors"] += 1
    def record_parse_error(self, v):
        with self.lock: self.health[v]["parse_errors"] += 1
    def set_connected(self, v, c):
        with self.lock: self.health[v]["connected"] = c

    # WebSocket handle registry — used by fault injection
    def register_ws(self, venue, ws):
        with self._ws_lock:
            self._ws_handles[venue] = ws

    def drop_connection(self, venue) -> bool:
        """Force-close a venue's WebSocket. The reconnect loop will bring it
        back. Used to demonstrate fault tolerance / recovery on demand."""
        with self._ws_lock:
            ws = self._ws_handles.get(venue)
        if ws is None:
            self.log("WARN", venue, "*",
                     "FAULT INJECTION requested but no live socket handle.")
            return False
        self.log("WARN", venue, "*",
                 "FAULT INJECTED — forcing WebSocket close. Reconnect loop "
                 "will recover automatically.")
        try:
            ws.close()
            return True
        except Exception as e:
            self.log("ERROR", venue, "*", f"Forced close failed: {e}")
            return False

    def log(self, level, venue, symbol, msg):
        with self.event_lock:
            self.events.append({
                "ts": time.time() * 1000.0,
                "level": level, "venue": venue, "symbol": symbol, "msg": msg,
            })

    def audit(self, level, username, role, action, msg):
        """Record a security-relevant event (login, role-gated action, etc.)."""
        with self.audit_lock:
            self.audit_events.append({
                "ts": time.time() * 1000.0,
                "level": level, "user": username, "role": role,
                "action": action, "msg": msg,
            })

    def snapshot_audit(self, n: int = 100):
        with self.audit_lock:
            return list(self.audit_events)[-n:][::-1]

    # readers
    def snapshot_trades(self) -> pd.DataFrame:
        with self.lock:
            return pd.DataFrame([asdict(t) for t in self.trades]) if self.trades else pd.DataFrame()

    def snapshot_bbo(self) -> pd.DataFrame:
        with self.lock:
            return pd.DataFrame([asdict(b) for b in self.bbo.values()]) if self.bbo else pd.DataFrame()

    def quotes_for_symbol(self, symbol: str) -> dict:
        """Latest BBO per venue for one symbol, shaped for compute_arbitrage."""
        with self.lock:
            out = {}
            for (venue, sym), b in self.bbo.items():
                if sym == symbol:
                    out[venue] = {
                        "bid": b.bid, "ask": b.ask,
                        "bid_size": b.bid_size, "ask_size": b.ask_size,
                        "ingest_ts": b.ingest_ts,
                    }
            return out

    def snapshot_health(self) -> dict:
        with self.lock:
            return {v: dict(h) for v, h in self.health.items()}

    def snapshot_events(self, n=40):
        with self.event_lock:
            return list(self.events)[-n:][::-1]

    def get_l2(self, venue, symbol):
        return self.l2.get((venue, symbol))

    def start(self):
        if self._started:
            return
        self._started = True
        for fn in (run_binance, run_coinbase, run_kraken, run_watchdog):
            threading.Thread(target=fn, args=(self,), daemon=True,
                             name=f"thread-{fn.__name__}").start()


# --------------------------------------------------------------------------- #
# WebSocket clients
# --------------------------------------------------------------------------- #
def _now_ms(): return time.time() * 1000.0
def _reverse_symbol(venue, native):
    for c, n in SYMBOL_MAP[venue].items():
        if n == native: return c
    return native


# ------ Binance: trades + BBO only (no L2 — REST is blocked) --------------- #
def run_binance(store: DataStore):
    streams = []
    for sym in SYMBOLS:
        ns = SYMBOL_MAP["binance"][sym]
        streams += [f"{ns}@trade", f"{ns}@bookTicker"]
    url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)

    def on_message(_ws, raw):
        try:
            msg = json.loads(raw); stream = msg.get("stream", ""); data = msg.get("data", {})
            ingest = _now_ms()
            if "@trade" in stream:
                native = stream.split("@")[0]
                store.add_trade(Trade(
                    venue="binance", symbol=_reverse_symbol("binance", native),
                    price=float(data["p"]), qty=float(data["q"]),
                    side="sell" if data.get("m") else "buy",
                    exchange_ts=float(data["T"]), ingest_ts=ingest,
                    trade_id=str(data.get("t", "")),
                ))
            elif "@bookTicker" in stream:
                native = stream.split("@")[0]
                store.set_bbo(BBO(
                    venue="binance", symbol=_reverse_symbol("binance", native),
                    bid=float(data["b"]), bid_size=float(data["B"]),
                    ask=float(data["a"]), ask_size=float(data["A"]),
                    exchange_ts=ingest, ingest_ts=ingest,
                ))
        except Exception:
            store.record_parse_error("binance")

    def on_open(_ws):
        store.set_connected("binance", True)
        store.log("INFO", "binance", "*", "WebSocket connected.")
    def on_close(_ws, *_a):
        store.set_connected("binance", False)
        store.log("WARN", "binance", "*", "WebSocket disconnected.")
    def on_error(_ws, e):
        store.record_transport_error("binance")
        store.log("ERROR", "binance", "*", f"WebSocket error: {e}")

    _run_forever_with_backoff(url, on_message, on_open, on_close, on_error, store, "binance")


# ------ Coinbase: matches + ticker + level2_batch -------------------------- #
# NOTE: Coinbase Exchange has required AUTHENTICATION on the plain `level2`
# channel since 2023-08-01. Unauthenticated subscriptions to `level2` are
# silently ignored — no snapshot is ever sent, so the maintainer would sit
# in INIT forever. We use `level2_batch` instead, which is public (no auth)
# and delivers the same `snapshot` / `l2update` message shapes, batched
# every 50ms. No change to CoinbaseL2Maintainer is needed.
def run_coinbase(store: DataStore):
    url = "wss://ws-feed.exchange.coinbase.com"
    product_ids = [SYMBOL_MAP["coinbase"][s] for s in SYMBOLS]
    subscribe = {
        "type": "subscribe",
        "product_ids": product_ids,
        "channels": ["matches", "ticker", "level2_batch"],
    }

    # Diagnostic: remember which message types we've already logged a
    # "first sighting" for, so the event log shows each type exactly once.
    seen_types: set = set()

    def on_message(_ws, raw):
        data = None
        try:
            data = json.loads(raw)
            ingest = _now_ms()
            mtype = data.get("type")
            product = data.get("product_id")

            # --- DIAGNOSTIC: log the first time we ever see a message type.
            # If 'snapshot' never appears here, Coinbase isn't sending it.
            if mtype not in seen_types:
                seen_types.add(mtype)
                store.log("INFO", "coinbase", product or "*",
                          f"First '{mtype}' message seen "
                          f"(keys: {sorted(data.keys())})")

            # --- DIAGNOSTIC: the subscriptions confirmation tells us exactly
            # which channels Coinbase actually accepted. If 'level2_batch' is
            # missing from this list, that is the whole problem.
            if mtype == "subscriptions":
                chans = data.get("channels", [])
                names = [c.get("name") for c in chans if isinstance(c, dict)]
                store.log("INFO", "coinbase", "*",
                          f"Subscription confirmed for channels: {names}")
                return

            if not product:
                return
            sym = _reverse_symbol("coinbase", product)

            if mtype == "match":
                ex_ts = pd.Timestamp(data["time"]).timestamp() * 1000.0
                store.add_trade(Trade(
                    venue="coinbase", symbol=sym,
                    price=float(data["price"]), qty=float(data["size"]),
                    side=data["side"], exchange_ts=ex_ts, ingest_ts=ingest,
                    trade_id=str(data.get("trade_id", "")),
                ))
            elif mtype == "ticker":
                ex_ts = pd.Timestamp(data["time"]).timestamp() * 1000.0 if "time" in data else ingest
                store.set_bbo(BBO(
                    venue="coinbase", symbol=sym,
                    bid=float(data["best_bid"]),
                    bid_size=float(data.get("best_bid_size", 0) or 0),
                    ask=float(data["best_ask"]),
                    ask_size=float(data.get("best_ask_size", 0) or 0),
                    exchange_ts=ex_ts, ingest_ts=ingest,
                ))
            elif mtype == "snapshot":
                # DIAGNOSTIC: confirm the snapshot reached routing, with sizes
                store.log("INFO", "coinbase", sym,
                          f"Routing snapshot to maintainer "
                          f"({len(data.get('bids', []))} bids / "
                          f"{len(data.get('asks', []))} asks).")
                m = store.get_l2("coinbase", sym)
                if m:
                    m.on_snapshot(data)
                else:
                    store.log("WARN", "coinbase", sym,
                              f"No L2 maintainer registered for sym={sym!r} — "
                              f"snapshot dropped.")
            elif mtype == "l2update":
                m = store.get_l2("coinbase", sym)
                if m:
                    m.on_l2update(data)
            elif mtype == "error":
                store.log("ERROR", "coinbase", "*",
                          f"Subscription error: {data.get('message', '')} "
                          f"{data.get('reason', '')}".strip())
        except Exception as e:
            store.record_parse_error("coinbase")
            # DIAGNOSTIC: surface the actual exception instead of just bumping
            # a silent counter. This is how a throwing on_snapshot becomes
            # visible in the event log.
            tp = data.get("type") if isinstance(data, dict) else "<json-parse-failed>"
            store.log("ERROR", "coinbase", "*",
                      f"Exception handling '{tp}' message: "
                      f"{type(e).__name__}: {e}")

    def on_open(ws):
        store.set_connected("coinbase", True)
        store.log("INFO", "coinbase", "*",
                  "WebSocket connected — subscribing to matches, ticker, "
                  "level2_batch.")
        ws.send(json.dumps(subscribe))
    def on_close(_ws, *_a):
        store.set_connected("coinbase", False)
        for sym in SYMBOLS:
            m = store.get_l2("coinbase", sym)
            if m: m.on_disconnect()
        store.log("WARN", "coinbase", "*", "WebSocket disconnected.")
    def on_error(_ws, e):
        store.record_transport_error("coinbase")
        store.log("ERROR", "coinbase", "*", f"WebSocket error: {e}")

    _run_forever_with_backoff(url, on_message, on_open, on_close, on_error, store, "coinbase")


# ------ Kraken v1: trade + spread + book-25 -------------------------------- #
def run_kraken(store: DataStore):
    url = "wss://ws.kraken.com"
    pairs = [SYMBOL_MAP["kraken"][s] for s in SYMBOLS]

    def on_message(ws, raw):
        try:
            data = json.loads(raw); ingest = _now_ms()
            if not isinstance(data, list):
                return  # event messages (subscribe acks, heartbeats)
            # Kraken book updates can be 4 or 5 elements:
            #   4: [chanID, payload_dict, channel_name, pair]
            #   5: [chanID, asks_dict, bids_dict, channel_name, pair]
            if len(data) == 5:
                _, d1, d2, channel_name, pair = data
                payload = {**d1, **d2}
            elif len(data) == 4:
                _, payload, channel_name, pair = data
            else:
                return
            sym = _reverse_symbol("kraken", pair)

            if channel_name == "trade":
                for r in payload:  # list of trades, not dict
                    store.add_trade(Trade(
                        venue="kraken", symbol=sym,
                        price=float(r[0]), qty=float(r[1]),
                        side="buy" if r[3] == "b" else "sell",
                        exchange_ts=float(r[2]) * 1000.0, ingest_ts=ingest,
                    ))
            elif channel_name == "spread":
                s = payload
                store.set_bbo(BBO(
                    venue="kraken", symbol=sym,
                    bid=float(s[0]), ask=float(s[1]),
                    bid_size=float(s[3]), ask_size=float(s[4]),
                    exchange_ts=float(s[2]) * 1000.0, ingest_ts=ingest,
                ))
            elif isinstance(channel_name, str) and channel_name.startswith("book-"):
                m = store.get_l2("kraken", sym)
                if not m:
                    return
                if "as" in payload or "bs" in payload:
                    # Initial snapshot
                    m.on_snapshot({"as": payload.get("as", []),
                                   "bs": payload.get("bs", [])})
                elif "a" in payload or "b" in payload:
                    m.on_update(
                        payload.get("b", []),
                        payload.get("a", []),
                        payload.get("c"),
                    )
                # After processing, check if any kraken maintainer wants a resub
                for s_ in SYMBOLS:
                    mm = store.get_l2("kraken", s_)
                    if mm and mm.consume_force_close():
                        store.log("INFO", "kraken", s_,
                                  "Closing WebSocket to trigger resubscribe.")
                        ws.close()
                        return
        except Exception:
            store.record_parse_error("kraken")

    def on_open(ws):
        store.set_connected("kraken", True)
        store.log("INFO", "kraken", "*", "WebSocket connected, subscribing.")
        ws.send(json.dumps({"event": "subscribe", "pair": pairs,
                            "subscription": {"name": "trade"}}))
        ws.send(json.dumps({"event": "subscribe", "pair": pairs,
                            "subscription": {"name": "spread"}}))
        ws.send(json.dumps({"event": "subscribe", "pair": pairs,
                            "subscription": {"name": "book", "depth": KRAKEN_DEPTH}}))
    def on_close(_ws, *_a):
        store.set_connected("kraken", False)
        for sym in SYMBOLS:
            m = store.get_l2("kraken", sym)
            if m: m.on_disconnect()
        store.log("WARN", "kraken", "*", "WebSocket disconnected.")
    def on_error(_ws, e):
        store.record_transport_error("kraken")
        store.log("ERROR", "kraken", "*", f"WebSocket error: {e}")

    _run_forever_with_backoff(url, on_message, on_open, on_close, on_error, store, "kraken")


def _run_forever_with_backoff(url, on_message, on_open, on_close, on_error, store, venue):
    backoff = 1.0
    while not store.stop_event.is_set():
        try:
            ws = websocket.WebSocketApp(url, on_message=on_message, on_open=on_open,
                                        on_close=on_close, on_error=on_error)
            store.register_ws(venue, ws)  # expose handle for fault injection
            ws.run_forever(ping_interval=20, ping_timeout=10)
            backoff = 1.0
        except Exception:
            store.record_transport_error(venue)
        store.set_connected(venue, False)
        if store.stop_event.is_set(): return
        time.sleep(backoff)
        backoff = min(backoff * 2, 30.0)


# --------------------------------------------------------------------------- #
# Watchdog — derives LIVE / STALE / DOWN per venue and logs transitions.
# A feed can be "connected" yet silent; this catches that dead-but-open case.
# --------------------------------------------------------------------------- #
def run_watchdog(store: DataStore):
    prev: dict = {v: None for v in VENUES}
    while not store.stop_event.is_set():
        time.sleep(WATCHDOG_INTERVAL)
        now = _now_ms()
        for v in VENUES:
            with store.lock:
                connected = store.health[v]["connected"]
                last = store.health[v]["last_msg_ts"]
            if not connected:
                state = "DOWN"
            elif last and (now - last) / 1000.0 > STALE_SECONDS:
                state = "STALE"
            elif not last:
                state = "DOWN"
            else:
                state = "LIVE"
            with store.lock:
                store.health[v]["derived_state"] = state
            if prev[v] is not None and prev[v] != state:
                lvl = "WARN" if state in ("STALE", "DOWN") else "INFO"
                store.log(lvl, v, "*", f"Feed state: {prev[v]} → {state}")
            prev[v] = state


# --------------------------------------------------------------------------- #
# Streamlit singleton
# --------------------------------------------------------------------------- #
@st.cache_resource(show_spinner="Starting connectors and L2 maintainers…")
def get_store() -> DataStore:
    store = DataStore()
    store.start()
    return store


# --------------------------------------------------------------------------- #
# Authentication & RBAC
# --------------------------------------------------------------------------- #
# Three-role hierarchy. Higher number includes all permissions of lower roles.
ROLE_HIERARCHY = {"viewer": 0, "operator": 1, "admin": 2}


@dataclass
class Session:
    username: str
    role: str
    login_ts: float


def _get_users_from_secrets() -> dict:
    """Read the user table from Streamlit Cloud secrets.

    Expected structure (in Streamlit Cloud → Settings → Secrets):

        [auth]
        session_timeout_minutes = 60

        [auth.users.alice]
        password_hash = "$2b$12$..."
        role = "admin"
    """
    try:
        auth = st.secrets["auth"]
        users = auth["users"]
        # st.secrets returns an AttrDict — convert to plain dict so we can
        # iterate cleanly.
        return {name: dict(rec) for name, rec in dict(users).items()}
    except Exception:
        return {}


def _get_session_timeout_minutes() -> int:
    try:
        return int(st.secrets["auth"].get("session_timeout_minutes", 60))
    except Exception:
        return 60


def authenticate(username: str, password: str) -> tuple[bool, str]:
    """Returns (ok, role_or_error). Generic error message — does not reveal
    whether the username exists, which is the standard security practice."""
    GENERIC = "Invalid username or password."
    users = _get_users_from_secrets()
    if not username or username not in users:
        # Still do a bcrypt check against a dummy hash to keep timing constant
        try:
            bcrypt.checkpw(b"x", b"$2b$12$" + b"a" * 53)
        except Exception:
            pass
        return False, GENERIC
    record = users[username]
    stored = (record.get("password_hash") or "").encode("utf-8")
    if not stored or not stored.startswith(b"$2"):
        return False, GENERIC
    try:
        ok = bcrypt.checkpw(password.encode("utf-8"), stored)
    except Exception:
        return False, GENERIC
    if not ok:
        return False, GENERIC
    role = record.get("role", "viewer")
    if role not in ROLE_HIERARCHY:
        return False, f"Account misconfigured (invalid role)."
    return True, role


def current_session() -> Session | None:
    return st.session_state.get("session")


def require_role(role: str) -> bool:
    sess = current_session()
    if sess is None:
        return False
    return ROLE_HIERARCHY.get(sess.role, -1) >= ROLE_HIERARCHY[role]


def login_required(store: DataStore) -> None:
    """If no valid session, render the login form and st.stop(). Otherwise
    return after refreshing the last-activity timestamp."""
    sess = current_session()
    timeout_s = _get_session_timeout_minutes() * 60
    now = time.time()

    # session timeout check
    if sess is not None:
        last_activity = st.session_state.get("last_activity", sess.login_ts)
        if now - last_activity > timeout_s:
            store.audit("INFO", sess.username, sess.role, "session_timeout",
                        "Session expired due to inactivity.")
            del st.session_state["session"]
            st.session_state.pop("last_activity", None)
            sess = None
            st.info("Your session expired. Please log in again.")

    if sess is not None:
        st.session_state["last_activity"] = now
        return  # authenticated — fall through to dashboard

    # ---- render login form ------------------------------------------------
    st.markdown(
        f'<div style="color:#fafafa; margin-bottom:0.25rem;">'
        f'{logo_svg(width=300)}</div>',
        unsafe_allow_html=True,
    )
    st.caption(f"**{APP_TAGLINE}** &nbsp;·&nbsp; Sign in to continue.")

    users = _get_users_from_secrets()
    if not users:
        st.error(
            "No users are configured. The administrator must add an "
            "`[auth]` block to the app's secrets in Streamlit Cloud "
            "(Settings → Secrets)."
        )
        st.markdown(
            "See `.streamlit/secrets.toml.example` in the repository for the "
            "exact format, and use `generate_password_hash.py` to produce "
            "bcrypt hashes."
        )
        st.stop()

    col_a, col_b, col_c = st.columns([1, 2, 1])
    with col_b:
        with st.form("login_form", clear_on_submit=False):
            u = st.text_input("Username", autocomplete="username")
            p = st.text_input("Password", type="password",
                              autocomplete="current-password")
            submitted = st.form_submit_button("Log in", type="primary",
                                              use_container_width=True)
        if submitted:
            ok, info = authenticate(u, p)
            if ok:
                st.session_state["session"] = Session(
                    username=u, role=info, login_ts=now)
                st.session_state["last_activity"] = now
                store.audit("INFO", u, info, "login", "Successful login.")
                st.rerun()
            else:
                store.audit("WARN", u or "<empty>", "?", "login_failed", info)
                st.error(info)

    st.stop()


# --------------------------------------------------------------------------- #
# UI
# --------------------------------------------------------------------------- #
st.set_page_config(page_title=APP_NAME,
                   page_icon="🪙", layout="wide")

# get_store() runs the WebSocket threads regardless of who's logged in, so the
# feed is warm when an authorized user arrives.
store = get_store()

# Gate everything below behind authentication. Renders login form + st.stop()
# if no valid session.
login_required(store)
session = current_session()  # guaranteed non-None past this point

st.markdown(
    f'<div style="color:#fafafa; margin-bottom:0.25rem;">'
    f'{logo_svg(width=320)}</div>',
    unsafe_allow_html=True,
)
st.caption(f"**{APP_TAGLINE}** &nbsp;·&nbsp; {APP_SUBTITLE}")

st_autorefresh(interval=REFRESH_INTERVAL_MS, key="autorefresh")

# Sidebar: controls & feed health
with st.sidebar:
    # --- Signed-in user header --------------------------------------------
    _role_badge = {"viewer": "👁", "operator": "🔧", "admin": "⚙️"}.get(
        session.role, "👤")
    st.markdown(
        f"{_role_badge} **{session.username}** &nbsp;"
        f"<span style='opacity:0.7'>({session.role})</span>",
        unsafe_allow_html=True,
    )
    if st.button("Log out", use_container_width=True):
        store.audit("INFO", session.username, session.role,
                    "logout", "User logged out.")
        st.session_state.pop("session", None)
        st.session_state.pop("last_activity", None)
        st.rerun()
    st.divider()

    st.header("Controls")
    selected_symbol = st.selectbox("Symbol (cross-venue panels)", SYMBOLS, index=0)
    l2_choice = st.selectbox(
        "L2 book to view",
        options=L2_TARGETS,
        format_func=lambda x: f"{x[0]} — {x[1]}",
        index=2,  # default to kraken BTC-USD because the CRC story is the demo
    )

    st.header("Feed health")
    health = store.snapshot_health()
    now = _now_ms()
    _state_dot = {"LIVE": "🟢", "STALE": "🟡", "DOWN": "🔴"}
    for venue, h in health.items():
        dot = _state_dot.get(h.get("derived_state", "DOWN"), "⚪")
        last_ts = h.get("last_msg_ts", 0.0)
        age_txt = (f"{(now - last_ts) / 1000.0:.1f}s ago"
                   if last_ts else "no data yet")
        st.markdown(
            f"{dot} **{venue}** — {h.get('derived_state', 'DOWN')} &nbsp; "
            f"`{h.get('messages', 0):,}` msgs<br>"
            f"&nbsp;&nbsp;last: {age_txt}<br>"
            f"&nbsp;&nbsp;transport err: {h.get('transport_errors', 0)} "
            f"&nbsp;|&nbsp; parse err: {h.get('parse_errors', 0)} "
            f"&nbsp;|&nbsp; SLO breaches: {h.get('slo_breaches', 0)}",
            unsafe_allow_html=True,
        )

    # --- Fault injection (operator or admin only) -------------------------
    st.header("🔧 Fault injection")
    if not require_role("operator"):
        st.caption("_Requires `operator` or `admin` role._")
    else:
        st.caption("Trigger failures on demand to demonstrate recovery.")
        fb1, fb2 = st.columns(2)

        def _do_drop(venue: str) -> None:
            # Double-check role at action time (defense in depth)
            if not require_role("operator"):
                store.audit("WARN", session.username, session.role,
                            "denied", f"drop {venue}: insufficient role")
                return
            store.drop_connection(venue)
            store.audit("INFO", session.username, session.role,
                        "fault_inject_drop",
                        f"Force-dropped {venue} WebSocket.")

        with fb1:
            if st.button("Drop Coinbase", use_container_width=True):
                _do_drop("coinbase")
            if st.button("Drop Binance", use_container_width=True):
                _do_drop("binance")
        with fb2:
            if st.button("Drop Kraken", use_container_width=True):
                _do_drop("kraken")
            if st.button("Corrupt Kraken book", use_container_width=True):
                if not require_role("operator"):
                    store.audit("WARN", session.username, session.role,
                                "denied",
                                "corrupt kraken book: insufficient role")
                else:
                    hit = False
                    for _sym in SYMBOLS:
                        _m = store.get_l2("kraken", _sym)
                        if (isinstance(_m, KrakenL2Maintainer)
                                and _m.inject_corruption()):
                            hit = True
                    store.audit("INFO", session.username, session.role,
                                "fault_inject_corrupt",
                                f"Corrupted Kraken book (success={hit}).")
                    if not hit:
                        st.toast("Kraken book not LIVE yet — nothing to corrupt.")

    st.divider()
    st.caption(
        "Binance L2 is disabled in this build — Streamlit Cloud's egress IP "
        "is blocked from Binance's REST snapshot endpoint. Binance trades "
        "and BBO are still active where the public stream allows."
    )

# ===== Section 1: cross-venue view ======================================== #
st.header("Cross-venue view")
col_left, col_right = st.columns([2, 3])

with col_left:
    st.subheader(f"Best Bid / Offer — {selected_symbol}")
    bbo_df = store.snapshot_bbo()
    if not bbo_df.empty:
        bbo_df = bbo_df[bbo_df["symbol"] == selected_symbol].copy()
    if bbo_df.empty:
        st.info("Waiting for BBO data…")
    else:
        bbo_df["spread_bps"] = (bbo_df["ask"] - bbo_df["bid"]) / bbo_df["bid"] * 1e4
        bbo_df["latency_ms"] = bbo_df["ingest_ts"] - bbo_df["exchange_ts"]
        st.dataframe(
            bbo_df[["venue", "bid", "ask", "bid_size", "ask_size",
                    "spread_bps", "latency_ms"]].round(
                {"bid": 2, "ask": 2, "bid_size": 4, "ask_size": 4,
                 "spread_bps": 2, "latency_ms": 1}),
            use_container_width=True, hide_index=True,
        )
        best_bid = bbo_df.loc[bbo_df["bid"].idxmax()]
        best_ask = bbo_df.loc[bbo_df["ask"].idxmin()]
        x_bps = (best_bid["bid"] - best_ask["ask"]) / best_ask["ask"] * 1e4
        m1, m2, m3 = st.columns(3)
        m1.metric("Best bid", f"${best_bid['bid']:,.2f}", f"@ {best_bid['venue']}")
        m2.metric("Best ask", f"${best_ask['ask']:,.2f}", f"@ {best_ask['venue']}")
        m3.metric("Cross-venue spread", f"{x_bps:+.2f} bps")

with col_right:
    st.subheader(f"Trade tape — {selected_symbol}")
    trades_df = store.snapshot_trades()
    if not trades_df.empty:
        trades_df = trades_df[trades_df["symbol"] == selected_symbol].copy()
    if trades_df.empty:
        st.info("Waiting for trades…")
    else:
        recent = trades_df.sort_values("ingest_ts", ascending=False).head(18).copy()
        recent["latency_ms"] = recent["ingest_ts"] - recent["exchange_ts"]
        recent["time"] = (pd.to_datetime(recent["exchange_ts"], unit="ms")
                            .dt.strftime("%H:%M:%S.%f").str[:-3])
        st.dataframe(
            recent[["time", "venue", "side", "price", "qty", "latency_ms"]]
              .round({"price": 2, "qty": 6, "latency_ms": 1}),
            use_container_width=True, hide_index=True,
        )

st.divider()

# ===== Section: Cross-exchange arbitrage monitor ========================== #
st.header("💱 Cross-exchange arbitrage monitor")
st.caption(
    "Best buy-low / sell-high pair per symbol, with the edge shown **gross** "
    "(raw price gap) and **net of fees** (after taker fees on both legs plus "
    "the withdrawal cost of moving the asset between venues). Only the net "
    f"number is real — opportunities clearing **{ARB_MIN_NET_BPS:.0f} bps "
    f"net** are flagged. Execution risk, withdrawal *time*, and slippage "
    "beyond top-of-book are not modeled."
)

now_ms = _now_ms()
arb_rows = []
for sym in SYMBOLS:
    quotes = store.quotes_for_symbol(sym)
    arb = compute_arbitrage(sym, quotes, VENUE_FEES, now_ms)
    if arb:
        arb_rows.append(arb)

if not arb_rows:
    st.info("Waiting for fresh quotes on at least two venues…")
else:
    # Headline metrics for the best opportunity across symbols
    best = max(arb_rows, key=lambda r: r["net_bps"])
    a1, a2, a3, a4 = st.columns(4)
    a1.metric(f"Best net edge ({best['symbol']})", f"{best['net_bps']:+.2f} bps",
              f"gross {best['gross_bps']:+.2f} bps")
    a2.metric("Buy at", f"{best['buy_venue']}", f"${best['buy_ask']:,.2f}")
    a3.metric("Sell at", f"{best['sell_venue']}", f"${best['sell_bid']:,.2f}")
    a4.metric("Est. profit (top-of-book)",
              f"${best['est_profit_usd']:,.2f}",
              f"{best['max_size']:.4f} units")

    if best["profitable"]:
        st.success(
            f"✅ Net-profitable opportunity on {best['symbol']}: buy "
            f"{best['buy_venue']} / sell {best['sell_venue']} for "
            f"{best['net_bps']:.2f} bps after fees."
        )
    else:
        st.warning(
            f"⚠️ Best gross gap ({best['gross_bps']:+.2f} bps on "
            f"{best['symbol']}) does NOT survive fees — net "
            f"{best['net_bps']:+.2f} bps. This is the usual case, and the "
            "reason naive cross-exchange spread is misleading."
        )

    # Detailed per-symbol table
    table = []
    for r in arb_rows:
        table.append({
            "symbol": r["symbol"],
            "buy @": r["buy_venue"],
            "buy ask": r["buy_ask"],
            "sell @": r["sell_venue"],
            "sell bid": r["sell_bid"],
            "gross bps": r["gross_bps"],
            "fees bps": r["gross_bps"] - r["net_bps"],
            "net bps": r["net_bps"],
            "max size": r["max_size"],
            "net profit $": r["est_profit_usd"],
            "status": "✅ profitable" if r["profitable"] else "— sub-threshold",
        })
    arb_df = pd.DataFrame(table).round({
        "buy ask": 2, "sell bid": 2, "gross bps": 2, "fees bps": 2,
        "net bps": 2, "max size": 5, "net profit $": 2})
    st.dataframe(arb_df, use_container_width=True, hide_index=True)

    with st.expander("How the net edge is calculated (and the fee assumptions)"):
        st.markdown(
            "For one unit of the base asset:\n\n"
            "- **Buy cost** = buy-venue ask × (1 + buy taker fee)\n"
            "- **Sell proceeds** = sell-venue bid × (1 − sell taker fee)\n"
            "- **Withdrawal cost** = buy-venue withdrawal fee (in base units) "
            "× buy price — the cost of moving the coin to the sell venue\n"
            "- **Net edge** = (sell proceeds − buy cost − withdrawal cost) / "
            "buy cost, in basis points\n\n"
            "Taker and withdrawal fees are representative public spot fees and "
            "are editable in `VENUE_FEES` at the top of the file. Your actual "
            "account tier will differ. Withdrawal *time* (minutes to hours of "
            "price risk while the transfer confirms) is a real cost this "
            "model does **not** capture — a production arb screener would."
        )
        fee_rows = []
        for v, f in VENUE_FEES.items():
            fee_rows.append({
                "venue": v,
                "taker %": f["taker"] * 100,
                "BTC withdraw": f["withdraw"].get("BTC-USD", 0),
                "ETH withdraw": f["withdraw"].get("ETH-USD", 0),
            })
        st.dataframe(pd.DataFrame(fee_rows).round({"taker %": 3}),
                     use_container_width=True, hide_index=True)

st.divider()

# ===== Section 2: L2 order book =========================================== #
venue_sel, sym_sel = l2_choice
st.header(f"📊 L2 order book — {venue_sel} {sym_sel}")
maintainer = store.get_l2(venue_sel, sym_sel)
snap = maintainer.snapshot() if maintainer else None

if not snap:
    st.info("No L2 maintainer for this selection.")
else:
    st.markdown(f"**Sync protocol:** `{snap['protocol']}`")

    state_color = {
        "INIT": "🟡", "BUFFERING": "🟡", "SYNCING": "🟡",
        "LIVE": "🟢", "RESYNC": "🔴",
    }.get(snap["state"], "⚪")
    book_age = (f"{snap['book_age_ms']:.0f} ms"
                if snap["book_age_ms"] is not None else "—")

    # Health card — different counters per protocol
    if venue_sel == "kraken":
        crc_total = snap["checksum_successes"] + snap["checksum_failures"]
        crc_rate = (f"{snap['checksum_successes'] / crc_total * 100:.1f}%"
                    if crc_total else "—")
        h1, h2, h3, h4, h5, h6 = st.columns(6)
        h1.metric("State", f"{state_color} {snap['state']}")
        h2.metric("Snapshots", snap["snapshots_received"])
        h3.metric("Updates applied", f"{snap['updates_applied']:,}")
        h4.metric("CRC ok / fail", f"{snap['checksum_successes']:,} / {snap['checksum_failures']}")
        h5.metric("CRC pass rate", crc_rate)
        h6.metric("Book age", book_age)
    else:  # coinbase
        h1, h2, h3, h4, h5, h6 = st.columns(6)
        h1.metric("State", f"{state_color} {snap['state']}")
        h2.metric("Snapshots", snap["snapshots_received"])
        h3.metric("Updates applied", f"{snap['updates_applied']:,}")
        h4.metric("Time regressions", snap.get("timestamp_regressions", 0))
        h5.metric("Levels (bid/ask)", f"{snap['n_bid_levels']}/{snap['n_ask_levels']}")
        h6.metric("Book age", book_age)

    # Ladder + depth chart
    book_l, book_r = st.columns([1, 1])
    with book_l:
        st.markdown("**Ladder (top 20 each side)**")
        if not snap["bids"] or not snap["asks"]:
            st.info("Book not yet populated — waiting for sync…")
        else:
            asks_df = pd.DataFrame(snap["asks"], columns=["price", "qty"])
            asks_df["side"] = "ask"
            bids_df = pd.DataFrame(snap["bids"], columns=["price", "qty"])
            bids_df["side"] = "bid"
            asks_df = asks_df.iloc[::-1].reset_index(drop=True)  # highest at top
            ladder = pd.concat([asks_df, bids_df], ignore_index=True)
            ladder["price"] = ladder["price"].round(4)
            ladder["qty"] = ladder["qty"].round(6)
            max_qty = max(ladder["qty"].max(), 1e-9)
            ladder["depth"] = ladder["qty"].apply(
                lambda q: "▰" * int(round(q / max_qty * 12)))
            st.dataframe(ladder[["side", "price", "qty", "depth"]],
                         use_container_width=True, hide_index=True, height=735)

    with book_r:
        st.markdown("**Depth chart (cumulative liquidity)**")
        if not snap["bids"] or not snap["asks"]:
            st.info("Waiting for book…")
        else:
            bids_df = pd.DataFrame(snap["bids"], columns=["price", "qty"]) \
                        .sort_values("price", ascending=False).reset_index(drop=True)
            asks_df = pd.DataFrame(snap["asks"], columns=["price", "qty"]) \
                        .sort_values("price", ascending=True).reset_index(drop=True)
            bids_df["cum"] = bids_df["qty"].cumsum()
            asks_df["cum"] = asks_df["qty"].cumsum()
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=bids_df["price"], y=bids_df["cum"], mode="lines",
                line=dict(shape="hv", color="#26a69a", width=2),
                fill="tozeroy", fillcolor="rgba(38,166,154,0.18)", name="bids cum"))
            fig.add_trace(go.Scatter(
                x=asks_df["price"], y=asks_df["cum"], mode="lines",
                line=dict(shape="hv", color="#ef5350", width=2),
                fill="tozeroy", fillcolor="rgba(239,83,80,0.18)", name="asks cum"))
            fig.update_layout(height=700, margin=dict(l=0, r=0, t=10, b=0),
                              xaxis_title="Price (USD)", yaxis_title="Cumulative size",
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

st.divider()

# ===== Section 3: live event log ========================================== #
st.header("📜 Maintainer event log")
events = store.snapshot_events(40)
if not events:
    st.info("No events yet.")
else:
    ev_df = pd.DataFrame(events)
    ev_df["time"] = (pd.to_datetime(ev_df["ts"], unit="ms")
                       .dt.strftime("%H:%M:%S.%f").str[:-3])
    ev_df = ev_df[["time", "level", "venue", "symbol", "msg"]]
    def _row_style(row):
        color = {"INFO": "", "WARN": "color: #f9a825;",
                 "ERROR": "color: #ef5350;"}.get(row["level"], "")
        return [color] * len(row)
    try:
        st.dataframe(ev_df.style.apply(_row_style, axis=1),
                     use_container_width=True, hide_index=True, height=420)
    except Exception:
        st.dataframe(ev_df, use_container_width=True, hide_index=True, height=420)

st.divider()

# ===== Section: Latency SLO monitor ======================================= #
st.header("⏱️ Latency SLO monitor")
st.caption(
    f"SLO threshold: **{SLO_LATENCY_MS} ms** ingest latency. Percentiles are "
    f"over the in-memory trade buffer (~{MAX_TRADES_BUFFER} trades); the SLO "
    f"breach counter is a running total since startup. Latency here reflects "
    f"the Streamlit Cloud VM's path to each venue, not a production system."
)
trades_all = store.snapshot_trades()
health = store.snapshot_health()
if trades_all.empty:
    st.info("Waiting for trades…")
else:
    t = trades_all.copy()
    t["latency_ms"] = t["ingest_ts"] - t["exchange_ts"]
    t = t[(t["latency_ms"] >= 0) & (t["latency_ms"] < 60_000)]  # drop clock skew
    rows = []
    for venue in VENUES:
        lat = t[t["venue"] == venue]["latency_ms"]
        if len(lat) == 0:
            continue
        breaches = health[venue].get("slo_breaches", 0)
        total = health[venue].get("messages", 0)
        rows.append({
            "venue": venue,
            "p50 ms": lat.quantile(0.50),
            "p99 ms": lat.quantile(0.99),
            "p99.9 ms": lat.quantile(0.999),
            "max ms": lat.max(),
            "SLO breaches": breaches,
            "breach rate": (f"{breaches / total * 100:.2f}%"
                            if total else "—"),
        })
    if rows:
        slo_df = pd.DataFrame(rows)
        st.dataframe(
            slo_df.round({"p50 ms": 1, "p99 ms": 1, "p99.9 ms": 1, "max ms": 1}),
            use_container_width=True, hide_index=True,
        )
        worst = max(rows, key=lambda r: r["p99 ms"])
        if worst["p99 ms"] > SLO_LATENCY_MS:
            st.warning(
                f"⚠️ {worst['venue']} p99 latency ({worst['p99 ms']:.0f} ms) "
                f"exceeds the {SLO_LATENCY_MS} ms SLO — in production this "
                f"would page the on-call engineer."
            )
        else:
            st.success(
                f"All venues within the {SLO_LATENCY_MS} ms p99 SLO.")
    else:
        st.info("Not enough clean latency samples yet…")

st.divider()

# ===== Section: Consumer fan-out ========================================== #
st.header("🔀 Consumer fan-out")
st.caption(
    "Three independent downstream consumers, each subscribing to the same "
    "normalized trade stream. The DataStore dispatches every trade to all of "
    "them — this is the pub/sub fan-out pattern (one source, many consumers) "
    "without the network layer. Each consumer's message count proves it "
    "received the full stream independently."
)
cons_cols = st.columns(3)
for col, consumer in zip(cons_cols, store.consumers):
    csnap = consumer.snapshot()
    with col:
        st.markdown(f"**{consumer.name}**")
        st.metric("Messages received", f"{csnap['messages']:,}")

        if consumer.kind == "vwap":
            if csnap["rows"]:
                st.dataframe(
                    pd.DataFrame(csnap["rows"]).round({"vwap": 2}),
                    use_container_width=True, hide_index=True)
            else:
                st.caption("Awaiting trades…")
            st.caption("Rolling volume-weighted average price per symbol.")

        elif consumer.kind == "volume":
            if csnap["rows"]:
                st.dataframe(
                    pd.DataFrame(csnap["rows"]).round({"volume": 6}),
                    use_container_width=True, hide_index=True)
            else:
                st.caption("Awaiting trades…")
            st.caption("Running trade count and volume per venue+symbol.")

        elif consumer.kind == "alert":
            alerts = csnap["alerts"]
            st.metric("Alerts fired", len(alerts))
            if alerts:
                adf = pd.DataFrame(alerts)
                adf["time"] = (pd.to_datetime(adf["ts"], unit="ms")
                                 .dt.strftime("%H:%M:%S"))
                adf["notional"] = adf["notional"].round(0)
                st.dataframe(
                    adf[["time", "venue", "symbol", "side", "notional"]],
                    use_container_width=True, hide_index=True, height=220)
            else:
                st.caption(f"No trades ≥ ${LARGE_TRADE_USD:,.0f} notional yet.")
            st.caption(f"Fires when a single trade exceeds "
                       f"${LARGE_TRADE_USD:,.0f} notional.")

st.divider()

# ===== Section: Audit log (admin only) ==================================== #
if require_role("admin"):
    st.header("🛡 Audit log")
    st.caption(
        "Security-sensitive events: logins, failed logins, role-gated "
        "actions, fault injection. Admin-only view. In production this would "
        "be an append-only sink (S3, SIEM) rather than in-memory."
    )
    audit_events = store.snapshot_audit(80)
    if not audit_events:
        st.info("No audit events yet.")
    else:
        adf = pd.DataFrame(audit_events)
        adf["time"] = (pd.to_datetime(adf["ts"], unit="ms")
                         .dt.strftime("%H:%M:%S.%f").str[:-3])
        adf = adf[["time", "level", "user", "role", "action", "msg"]]
        def _audit_style(row):
            color = {"INFO": "", "WARN": "color: #f9a825;",
                     "ERROR": "color: #ef5350;"}.get(row["level"], "")
            return [color] * len(row)
        try:
            st.dataframe(adf.style.apply(_audit_style, axis=1),
                         use_container_width=True, hide_index=True, height=320)
        except Exception:
            st.dataframe(adf, use_container_width=True, hide_index=True, height=320)

    # User roster
    st.markdown("**Configured users**")
    users_cfg = _get_users_from_secrets()
    if users_cfg:
        urows = [{"username": u, "role": r.get("role", "?")}
                 for u, r in sorted(users_cfg.items())]
        st.dataframe(pd.DataFrame(urows),
                     use_container_width=True, hide_index=True)
    else:
        st.info("No users configured in secrets.")
    st.divider()

# ===== Section 4: price chart + latency =================================== #
st.subheader(f"Recent trade prices across venues — {selected_symbol}")
trades_all = store.snapshot_trades()
if trades_all.empty:
    st.info("Waiting for trades…")
else:
    plot_df = trades_all[trades_all["symbol"] == selected_symbol].copy()
    if not plot_df.empty:
        plot_df["time"] = pd.to_datetime(plot_df["exchange_ts"], unit="ms")
        fig = go.Figure()
        for venue in plot_df["venue"].unique():
            v = plot_df[plot_df["venue"] == venue].sort_values("time")
            fig.add_trace(go.Scatter(x=v["time"], y=v["price"],
                                     mode="lines+markers", name=venue,
                                     marker=dict(size=4)))
        fig.update_layout(height=320, margin=dict(l=0, r=0, t=10, b=0),
                          xaxis_title="Exchange time", yaxis_title="Price (USD)",
                          legend=dict(orientation="h", y=1.1))
        st.plotly_chart(fig, use_container_width=True)

st.subheader("Ingest latency by venue (last 300 trades)")
if not trades_all.empty:
    recent = trades_all.tail(300).copy()
    recent["latency_ms"] = recent["ingest_ts"] - recent["exchange_ts"]
    recent = recent[(recent["latency_ms"] >= -1000) & (recent["latency_ms"] < 10_000)]
    if not recent.empty:
        fig = go.Figure()
        for venue in recent["venue"].unique():
            v = recent[recent["venue"] == venue]
            fig.add_trace(go.Box(y=v["latency_ms"], name=venue, boxpoints="outliers"))
        fig.update_layout(height=260, yaxis_title="Latency (ms)",
                          margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

with st.expander("📦 Normalized JSON — what downstream clients would consume"):
    with store.lock:
        sample = [asdict(t) for t in list(store.trades)[-5:]]
    if sample:
        st.code(json.dumps(sample, indent=2), language="json")
    else:
        st.write("_No trades yet._")
