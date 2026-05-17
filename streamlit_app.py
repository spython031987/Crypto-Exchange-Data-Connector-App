"""
Unified Crypto Market Data Gateway — v2
=======================================
v2 adds a real L2 order book maintainer for Binance (BTCUSDT, ETHUSDT)
implementing Binance's documented synchronization protocol:

  1. Subscribe to <symbol>@depth@100ms diff stream and BUFFER events.
  2. Fetch REST snapshot at /api/v3/depth?limit=1000 (has `lastUpdateId`).
  3. Drop buffered events where u <= lastUpdateId.
  4. Verify the first remaining event satisfies U <= lastUpdateId+1 <= u.
     If not, snapshot is stale — refetch.
  5. Load snapshot into the book, replay surviving buffered events.
  6. Go LIVE. For each new event, check U == prev_u + 1; on mismatch,
     declare a GAP and resync from step 1.

This protocol — buffer-while-fetching, sequence validation, gap detection,
and recovery — is the "hard problem" the job description is hinting at.
The dashboard visualizes it directly: ladder, depth chart, book health
counters, and a live event log of the maintainer's transitions.
"""

import json
import threading
import time
import urllib.request
import urllib.error
from collections import deque
from dataclasses import asdict, dataclass

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import websocket
from streamlit_autorefresh import st_autorefresh

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
VENUES = ["binance", "coinbase", "kraken"]
SYMBOLS = ["BTC-USD", "ETH-USD"]

SYMBOL_MAP = {
    "binance":  {"BTC-USD": "btcusdt", "ETH-USD": "ethusdt"},
    "coinbase": {"BTC-USD": "BTC-USD", "ETH-USD": "ETH-USD"},
    "kraken":   {"BTC-USD": "XBT/USD", "ETH-USD": "ETH/USD"},
}

# Which (venue, symbol) pairs run a full L2 maintainer
L2_TARGETS = [("binance", "BTC-USD"), ("binance", "ETH-USD")]

LADDER_DEPTH = 20            # levels per side shown in the dashboard
MAX_TRADES_BUFFER = 1000
MAX_EVENT_LOG = 200
MAX_BUFFER_EVENTS = 5000     # cap on per-symbol pending event buffer
REFRESH_INTERVAL_MS = 1000

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
# L2 Order Book
# --------------------------------------------------------------------------- #
class OrderBook:
    """Plain price->qty maps with sort-on-read. Fine for PoC scale (~hundreds
    of mutations per second, ~1Hz render). Production would use SortedDict."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_event_ts: float = 0.0     # exchange time (ms) of last applied event
        self.last_apply_ts: float = 0.0     # our wall clock (ms) of last apply

    def load_snapshot(self, bids, asks) -> None:
        self.bids = {float(p): float(q) for p, q in bids if float(q) > 0}
        self.asks = {float(p): float(q) for p, q in asks if float(q) > 0}
        self.last_apply_ts = time.time() * 1000.0

    def apply_diff(self, bid_updates, ask_updates, exchange_ts: float) -> None:
        for p_str, q_str in bid_updates:
            p, q = float(p_str), float(q_str)
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q
        for p_str, q_str in ask_updates:
            p, q = float(p_str), float(q_str)
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q
        self.last_event_ts = exchange_ts
        self.last_apply_ts = time.time() * 1000.0

    def top_n(self, n: int = LADDER_DEPTH):
        bids = sorted(self.bids.items(), key=lambda kv: -kv[0])[:n]
        asks = sorted(self.asks.items(), key=lambda kv:  kv[0])[:n]
        return bids, asks

    def bbo(self):
        if not self.bids or not self.asks:
            return None
        bb = max(self.bids); ba = min(self.asks)
        return bb, self.bids[bb], ba, self.asks[ba]


# --------------------------------------------------------------------------- #
# L2 Book Maintainer (Binance synchronization protocol)
# --------------------------------------------------------------------------- #
class L2BookMaintainer:
    """One instance per (venue, symbol). Implements Binance's documented
    diff-depth synchronization protocol with gap detection and resnapshot."""

    REST_URL = "https://api.binance.com/api/v3/depth?symbol={}&limit=1000"

    # States: INIT -> BUFFERING -> SYNCING -> LIVE
    #         LIVE -> RESYNC -> SYNCING -> LIVE
    def __init__(self, store: "DataStore", venue: str, symbol_canonical: str,
                 symbol_native: str):
        self.store = store
        self.venue = venue
        self.symbol = symbol_canonical
        self.native = symbol_native            # "btcusdt"
        self.rest_sym = symbol_native.upper()  # "BTCUSDT"
        self.book = OrderBook(symbol_canonical)

        self.state = "INIT"
        self.buffer: deque = deque(maxlen=MAX_BUFFER_EVENTS)
        self.last_u: int = 0
        self.gaps_detected = 0
        self.resnapshots = 0
        self.events_applied = 0
        self.buffer_overflows = 0
        self.lock = threading.Lock()

    # --- inbound from WebSocket --------------------------------------------- #
    def on_event(self, event: dict) -> None:
        """event has keys U (first update id), u (final update id),
        b (bid updates), a (ask updates), E (event time)."""
        with self.lock:
            if self.state == "INIT":
                self.state = "BUFFERING"
                pre_len = len(self.buffer)
                self.buffer.append(event)
                if pre_len == MAX_BUFFER_EVENTS:
                    self.buffer_overflows += 1
                self.store.log("INFO", self.venue, self.symbol,
                               f"First depth event received (U={event['U']}, "
                               f"u={event['u']}). Starting initial sync.")
                self._spawn_resync()
                return

            if self.state in ("BUFFERING", "SYNCING", "RESYNC"):
                pre_len = len(self.buffer)
                self.buffer.append(event)
                if pre_len == MAX_BUFFER_EVENTS:
                    self.buffer_overflows += 1
                    self.store.log("WARN", self.venue, self.symbol,
                                   "Buffer overflow — oldest event dropped.")
                return

            if self.state == "LIVE":
                if event["U"] != self.last_u + 1:
                    # GAP. Binance contract: each event's U must equal previous u+1.
                    self.gaps_detected += 1
                    self.store.log(
                        "WARN", self.venue, self.symbol,
                        f"Sequence gap — expected U={self.last_u + 1}, "
                        f"got U={event['U']} (Δ={event['U'] - self.last_u - 1}). "
                        f"Triggering resnapshot."
                    )
                    self.state = "RESYNC"
                    self.buffer.clear()
                    self.buffer.append(event)
                    self._spawn_resync()
                    return

                self.book.apply_diff(event["b"], event["a"],
                                     exchange_ts=float(event.get("E", 0)))
                self.last_u = event["u"]
                self.events_applied += 1
                self._publish_top_of_book()

    # --- snapshot fetch + replay ------------------------------------------- #
    def _spawn_resync(self) -> None:
        threading.Thread(target=self._fetch_and_replay, daemon=True,
                         name=f"l2-resync-{self.venue}-{self.symbol}").start()

    def _fetch_and_replay(self) -> None:
        self.store.log("INFO", self.venue, self.symbol,
                       f"Fetching REST snapshot for {self.rest_sym}…")
        try:
            url = self.REST_URL.format(self.rest_sym)
            req = urllib.request.Request(
                url, headers={"User-Agent": "crypto-gateway-poc/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                snap = json.loads(resp.read())
        except Exception as e:
            self.store.log("ERROR", self.venue, self.symbol,
                           f"Snapshot fetch failed: {type(e).__name__}: {e}. "
                           f"Retrying in 2s.")
            threading.Timer(2.0, self._spawn_resync).start()
            return

        snap_id = int(snap["lastUpdateId"])

        with self.lock:
            self.state = "SYNCING"
            self.store.log("INFO", self.venue, self.symbol,
                           f"Snapshot received (lastUpdateId={snap_id}, "
                           f"{len(snap['bids'])} bids / {len(snap['asks'])} asks). "
                           f"Buffer holds {len(self.buffer)} events.")

            # Drop buffered events that are already in the snapshot
            usable = [e for e in self.buffer if e["u"] > snap_id]

            if not usable:
                # No usable events yet — load snapshot, wait for live events
                self.book.load_snapshot(snap["bids"], snap["asks"])
                self.last_u = snap_id
                self.buffer.clear()
                self.state = "LIVE"
                self.resnapshots += 1
                self.store.log("INFO", self.venue, self.symbol,
                               "Snapshot loaded. No buffered events to replay. "
                               "State → LIVE.")
                self._publish_top_of_book()
                return

            first = usable[0]
            # Binance contract: first event after snapshot must straddle snap_id+1
            if not (first["U"] <= snap_id + 1 <= first["u"]):
                self.store.log(
                    "WARN", self.venue, self.symbol,
                    f"Snapshot is stale (snap={snap_id}, first_buffered "
                    f"event U={first['U']}, u={first['u']}). Refetching."
                )
                self.state = "RESYNC"
                threading.Timer(1.0, self._spawn_resync).start()
                return

            # Load snapshot, replay events strictly after snap_id
            self.book.load_snapshot(snap["bids"], snap["asks"])
            applied = 0
            prev_u = snap_id
            for ev in usable:
                if ev["u"] <= snap_id:
                    continue
                # Check intra-buffer continuity (except for the first replayed)
                if applied > 0 and ev["U"] != prev_u + 1:
                    self.store.log(
                        "WARN", self.venue, self.symbol,
                        f"Gap in buffered events during replay "
                        f"(expected U={prev_u + 1}, got U={ev['U']}). "
                        f"Restarting sync."
                    )
                    self.state = "RESYNC"
                    threading.Timer(1.0, self._spawn_resync).start()
                    return
                self.book.apply_diff(ev["b"], ev["a"],
                                     exchange_ts=float(ev.get("E", 0)))
                prev_u = ev["u"]
                applied += 1

            self.last_u = prev_u
            self.events_applied += applied
            self.buffer.clear()
            self.state = "LIVE"
            self.resnapshots += 1
            self.store.log("INFO", self.venue, self.symbol,
                           f"Replayed {applied} buffered events. State → LIVE. "
                           f"last_u={self.last_u}.")
            self._publish_top_of_book()

    def _publish_top_of_book(self) -> None:
        b = self.book.bbo()
        if b is None:
            return
        bb, bb_sz, ba, ba_sz = b
        now = time.time() * 1000.0
        self.store.set_bbo(BBO(
            venue=self.venue, symbol=self.symbol,
            bid=bb, bid_size=bb_sz, ask=ba, ask_size=ba_sz,
            exchange_ts=self.book.last_event_ts or now,
            ingest_ts=now,
        ))

    # --- snapshot for dashboard -------------------------------------------- #
    def snapshot(self) -> dict:
        with self.lock:
            bids, asks = self.book.top_n(LADDER_DEPTH)
            now = time.time() * 1000.0
            return {
                "state": self.state,
                "bids": bids,
                "asks": asks,
                "last_u": self.last_u,
                "gaps_detected": self.gaps_detected,
                "resnapshots": self.resnapshots,
                "events_applied": self.events_applied,
                "buffer_size": len(self.buffer),
                "buffer_overflows": self.buffer_overflows,
                "n_bid_levels": len(self.book.bids),
                "n_ask_levels": len(self.book.asks),
                "book_age_ms": (now - self.book.last_apply_ts
                                if self.book.last_apply_ts else None),
            }


# --------------------------------------------------------------------------- #
# DataStore — shared between background threads and Streamlit
# --------------------------------------------------------------------------- #
class DataStore:
    def __init__(self):
        self.trades: deque = deque(maxlen=MAX_TRADES_BUFFER)
        self.bbo: dict = {}
        self.lock = threading.Lock()
        self.health = {
            v: {"connected": False, "messages": 0,
                "transport_errors": 0, "parse_errors": 0,
                "last_msg_ts": 0.0}
            for v in VENUES
        }
        self.events: deque = deque(maxlen=MAX_EVENT_LOG)
        self.event_lock = threading.Lock()
        self.l2: dict = {}  # (venue, symbol) -> L2BookMaintainer
        for venue, sym in L2_TARGETS:
            native = SYMBOL_MAP[venue][sym]
            self.l2[(venue, sym)] = L2BookMaintainer(self, venue, sym, native)
        self.stop_event = threading.Event()
        self._started = False

    # --- trade / bbo / health (mutators) ----------------------------------- #
    def add_trade(self, t: Trade) -> None:
        with self.lock:
            self.trades.append(t)
            h = self.health[t.venue]
            h["messages"] += 1
            h["last_msg_ts"] = t.ingest_ts

    def set_bbo(self, b: BBO) -> None:
        with self.lock:
            self.bbo[(b.venue, b.symbol)] = b
            h = self.health[b.venue]
            h["messages"] += 1
            h["last_msg_ts"] = b.ingest_ts

    def record_transport_error(self, venue: str) -> None:
        with self.lock:
            self.health[venue]["transport_errors"] += 1

    def record_parse_error(self, venue: str) -> None:
        with self.lock:
            self.health[venue]["parse_errors"] += 1

    def set_connected(self, venue: str, connected: bool) -> None:
        with self.lock:
            self.health[venue]["connected"] = connected

    # --- event log --------------------------------------------------------- #
    def log(self, level: str, venue: str, symbol: str, msg: str) -> None:
        with self.event_lock:
            self.events.append({
                "ts": time.time() * 1000.0,
                "level": level,
                "venue": venue,
                "symbol": symbol,
                "msg": msg,
            })

    # --- snapshots (readers) ----------------------------------------------- #
    def snapshot_trades(self) -> pd.DataFrame:
        with self.lock:
            if not self.trades:
                return pd.DataFrame()
            return pd.DataFrame([asdict(t) for t in self.trades])

    def snapshot_bbo(self) -> pd.DataFrame:
        with self.lock:
            if not self.bbo:
                return pd.DataFrame()
            return pd.DataFrame([asdict(b) for b in self.bbo.values()])

    def snapshot_health(self) -> dict:
        with self.lock:
            return {v: dict(h) for v, h in self.health.items()}

    def snapshot_events(self, n: int = 40) -> list:
        with self.event_lock:
            return list(self.events)[-n:][::-1]

    def get_l2(self, venue: str, symbol: str) -> L2BookMaintainer | None:
        return self.l2.get((venue, symbol))

    # --- lifecycle --------------------------------------------------------- #
    def start(self) -> None:
        if self._started:
            return
        self._started = True
        for fn in (run_binance, run_coinbase, run_kraken):
            t = threading.Thread(target=fn, args=(self,), daemon=True,
                                 name=f"ws-{fn.__name__}")
            t.start()


# --------------------------------------------------------------------------- #
# WebSocket clients
# --------------------------------------------------------------------------- #
def _now_ms() -> float:
    return time.time() * 1000.0


def _reverse_symbol(venue: str, native: str) -> str:
    for canonical, n in SYMBOL_MAP[venue].items():
        if n == native:
            return canonical
    return native


# ------ Binance: trade + bookTicker + depth -------------------------------- #
def run_binance(store: DataStore) -> None:
    streams = []
    for sym in SYMBOLS:
        ns = SYMBOL_MAP["binance"][sym]
        streams.append(f"{ns}@trade")
        streams.append(f"{ns}@bookTicker")
        if ("binance", sym) in L2_TARGETS:
            streams.append(f"{ns}@depth@100ms")
    url = "wss://stream.binance.com:9443/stream?streams=" + "/".join(streams)

    def on_message(_ws, raw: str) -> None:
        try:
            msg = json.loads(raw)
            stream = msg.get("stream", "")
            data = msg.get("data", {})
            ingest = _now_ms()

            if "@trade" in stream:
                native = stream.split("@")[0]
                store.add_trade(Trade(
                    venue="binance",
                    symbol=_reverse_symbol("binance", native),
                    price=float(data["p"]), qty=float(data["q"]),
                    side="sell" if data.get("m") else "buy",
                    exchange_ts=float(data["T"]), ingest_ts=ingest,
                    trade_id=str(data.get("t", "")),
                ))
            elif "@bookTicker" in stream:
                native = stream.split("@")[0]
                store.set_bbo(BBO(
                    venue="binance",
                    symbol=_reverse_symbol("binance", native),
                    bid=float(data["b"]), bid_size=float(data["B"]),
                    ask=float(data["a"]), ask_size=float(data["A"]),
                    exchange_ts=ingest, ingest_ts=ingest,
                ))
            elif "@depth" in stream:
                native = stream.split("@")[0]
                sym = _reverse_symbol("binance", native)
                maintainer = store.get_l2("binance", sym)
                if maintainer is not None:
                    maintainer.on_event(data)
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

    _run_forever_with_backoff(url, on_message, on_open, on_close, on_error,
                              store, "binance")


# ------ Coinbase Exchange (public feed) ------------------------------------ #
def run_coinbase(store: DataStore) -> None:
    url = "wss://ws-feed.exchange.coinbase.com"
    subscribe = {
        "type": "subscribe",
        "product_ids": [SYMBOL_MAP["coinbase"][s] for s in SYMBOLS],
        "channels": ["matches", "ticker"],
    }

    def on_message(_ws, raw: str) -> None:
        try:
            data = json.loads(raw)
            ingest = _now_ms()
            mtype = data.get("type")
            product = data.get("product_id")
            if not product:
                return
            sym = _reverse_symbol("coinbase", product)

            if mtype == "match":
                ex_ts = pd.Timestamp(data["time"]).timestamp() * 1000.0
                store.add_trade(Trade(
                    venue="coinbase", symbol=sym,
                    price=float(data["price"]), qty=float(data["size"]),
                    side=data["side"],
                    exchange_ts=ex_ts, ingest_ts=ingest,
                    trade_id=str(data.get("trade_id", "")),
                ))
            elif mtype == "ticker":
                ex_ts = (pd.Timestamp(data["time"]).timestamp() * 1000.0
                         if "time" in data else ingest)
                store.set_bbo(BBO(
                    venue="coinbase", symbol=sym,
                    bid=float(data["best_bid"]),
                    bid_size=float(data.get("best_bid_size", 0) or 0),
                    ask=float(data["best_ask"]),
                    ask_size=float(data.get("best_ask_size", 0) or 0),
                    exchange_ts=ex_ts, ingest_ts=ingest,
                ))
        except Exception:
            store.record_parse_error("coinbase")

    def on_open(ws):
        store.set_connected("coinbase", True)
        store.log("INFO", "coinbase", "*", "WebSocket connected, subscribing.")
        ws.send(json.dumps(subscribe))
    def on_close(_ws, *_a):
        store.set_connected("coinbase", False)
        store.log("WARN", "coinbase", "*", "WebSocket disconnected.")
    def on_error(_ws, e):
        store.record_transport_error("coinbase")
        store.log("ERROR", "coinbase", "*", f"WebSocket error: {e}")

    _run_forever_with_backoff(url, on_message, on_open, on_close, on_error,
                              store, "coinbase")


# ------ Kraken v1 public WS ------------------------------------------------ #
def run_kraken(store: DataStore) -> None:
    url = "wss://ws.kraken.com"
    pairs = [SYMBOL_MAP["kraken"][s] for s in SYMBOLS]

    def on_message(_ws, raw: str) -> None:
        try:
            data = json.loads(raw)
            ingest = _now_ms()
            if isinstance(data, list) and len(data) >= 4:
                channel_name = data[2]
                pair = data[3]
                sym = _reverse_symbol("kraken", pair)

                if channel_name == "trade":
                    for r in data[1]:
                        store.add_trade(Trade(
                            venue="kraken", symbol=sym,
                            price=float(r[0]), qty=float(r[1]),
                            side="buy" if r[3] == "b" else "sell",
                            exchange_ts=float(r[2]) * 1000.0,
                            ingest_ts=ingest,
                        ))
                elif channel_name == "spread":
                    s = data[1]
                    store.set_bbo(BBO(
                        venue="kraken", symbol=sym,
                        bid=float(s[0]), ask=float(s[1]),
                        bid_size=float(s[3]), ask_size=float(s[4]),
                        exchange_ts=float(s[2]) * 1000.0,
                        ingest_ts=ingest,
                    ))
        except Exception:
            store.record_parse_error("kraken")

    def on_open(ws):
        store.set_connected("kraken", True)
        store.log("INFO", "kraken", "*", "WebSocket connected, subscribing.")
        ws.send(json.dumps({"event": "subscribe", "pair": pairs,
                            "subscription": {"name": "trade"}}))
        ws.send(json.dumps({"event": "subscribe", "pair": pairs,
                            "subscription": {"name": "spread"}}))
    def on_close(_ws, *_a):
        store.set_connected("kraken", False)
        store.log("WARN", "kraken", "*", "WebSocket disconnected.")
    def on_error(_ws, e):
        store.record_transport_error("kraken")
        store.log("ERROR", "kraken", "*", f"WebSocket error: {e}")

    _run_forever_with_backoff(url, on_message, on_open, on_close, on_error,
                              store, "kraken")


def _run_forever_with_backoff(url, on_message, on_open, on_close, on_error,
                              store, venue):
    backoff = 1.0
    while not store.stop_event.is_set():
        try:
            ws = websocket.WebSocketApp(
                url, on_message=on_message, on_open=on_open,
                on_close=on_close, on_error=on_error,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
            backoff = 1.0
        except Exception:
            store.record_transport_error(venue)
        store.set_connected(venue, False)
        if store.stop_event.is_set():
            return
        time.sleep(backoff)
        backoff = min(backoff * 2, 30.0)


# --------------------------------------------------------------------------- #
# Streamlit cached singleton
# --------------------------------------------------------------------------- #
@st.cache_resource(show_spinner="Starting WebSocket connectors and L2 maintainers…")
def get_store() -> DataStore:
    store = DataStore()
    store.start()
    return store


# --------------------------------------------------------------------------- #
# UI
# --------------------------------------------------------------------------- #
st.set_page_config(page_title="Crypto Market Data Gateway — v2",
                   page_icon="🪙", layout="wide")
st.title("🪙 Unified Crypto Market Data Gateway — v2")
st.caption(
    "Live normalized feeds from Binance, Coinbase, and Kraken, plus a real "
    "L2 order book maintainer for Binance with sequence-gap detection and "
    "resnapshot recovery."
)

store = get_store()
st_autorefresh(interval=REFRESH_INTERVAL_MS, key="autorefresh")

# ------ Sidebar: controls + feed health ----------------------------------- #
with st.sidebar:
    st.header("Controls")
    selected_symbol = st.selectbox("Symbol (cross-venue panels)", SYMBOLS, index=0)
    l2_symbol = st.selectbox("L2 book symbol (Binance)", SYMBOLS, index=0)

    st.header("Feed health")
    health = store.snapshot_health()
    now = _now_ms()
    for venue, h in health.items():
        dot = "🟢" if h["connected"] else "🔴"
        age_txt = (f"{(now - h['last_msg_ts']) / 1000.0:.1f}s ago"
                   if h["last_msg_ts"] else "no data yet")
        st.markdown(
            f"{dot} **{venue}** &nbsp; `{h['messages']:,}` msgs<br>"
            f"&nbsp;&nbsp;last: {age_txt}<br>"
            f"&nbsp;&nbsp;transport err: {h['transport_errors']} &nbsp;|&nbsp; "
            f"parse err: {h['parse_errors']}",
            unsafe_allow_html=True,
        )

    st.divider()
    st.caption(
        "**transport_errors**: WebSocket-level failures (connection drops, "
        "TLS errors).&nbsp; **parse_errors**: exceptions inside our message "
        "handlers (unknown shape, type mismatch). Slow rise on parse_errors "
        "is usually benign — heartbeats and subscription acks fall through."
    )

# ===== Section 1: cross-venue BBO + trade tape ============================ #
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

# ===== Section 2: L2 order book =========================================== #
st.header(f"📊 L2 order book — Binance {l2_symbol}")
maintainer = store.get_l2("binance", l2_symbol)
snap = maintainer.snapshot() if maintainer else None

if not snap:
    st.info("No L2 maintainer for this symbol.")
else:
    # Book health card
    state_color = {
        "INIT": "🟡", "BUFFERING": "🟡", "SYNCING": "🟡",
        "LIVE": "🟢", "RESYNC": "🔴",
    }.get(snap["state"], "⚪")
    book_age = (f"{snap['book_age_ms']:.0f} ms"
                if snap["book_age_ms"] is not None else "—")

    h1, h2, h3, h4, h5, h6 = st.columns(6)
    h1.metric("State", f"{state_color} {snap['state']}")
    h2.metric("Gaps detected", snap["gaps_detected"])
    h3.metric("Resnapshots", snap["resnapshots"])
    h4.metric("Events applied", f"{snap['events_applied']:,}")
    h5.metric("Levels (bid/ask)",
              f"{snap['n_bid_levels']}/{snap['n_ask_levels']}")
    h6.metric("Book age", book_age)

    # Ladder + depth chart
    book_l, book_r = st.columns([1, 1])

    with book_l:
        st.markdown("**Ladder (top 20 each side)**")
        if not snap["bids"] or not snap["asks"]:
            st.info("Book not yet populated — waiting for sync to complete…")
        else:
            # Ladder as one combined frame, asks descending top, then bids descending
            asks_df = pd.DataFrame(snap["asks"], columns=["price", "qty"])
            asks_df["side"] = "ask"
            bids_df = pd.DataFrame(snap["bids"], columns=["price", "qty"])
            bids_df["side"] = "bid"
            # Asks: highest at top → reverse order
            asks_df = asks_df.iloc[::-1].reset_index(drop=True)
            ladder = pd.concat([asks_df, bids_df], ignore_index=True)
            ladder["price"] = ladder["price"].round(2)
            ladder["qty"] = ladder["qty"].round(5)
            # Pretty: use color-coded "qty bar" emoji-ish — keep numeric for clarity
            max_qty = max(ladder["qty"].max(), 1e-9)
            ladder["depth"] = ladder["qty"].apply(
                lambda q: "▰" * int(round(q / max_qty * 12)))
            st.dataframe(
                ladder[["side", "price", "qty", "depth"]],
                use_container_width=True, hide_index=True, height=735,
            )

    with book_r:
        st.markdown("**Depth chart (cumulative liquidity)**")
        if not snap["bids"] or not snap["asks"]:
            st.info("Waiting for book…")
        else:
            bids_df = pd.DataFrame(snap["bids"], columns=["price", "qty"])
            asks_df = pd.DataFrame(snap["asks"], columns=["price", "qty"])
            bids_df = bids_df.sort_values("price", ascending=False).reset_index(drop=True)
            asks_df = asks_df.sort_values("price", ascending=True ).reset_index(drop=True)
            bids_df["cum"] = bids_df["qty"].cumsum()
            asks_df["cum"] = asks_df["qty"].cumsum()

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=bids_df["price"], y=bids_df["cum"], mode="lines",
                line=dict(shape="hv", color="#26a69a", width=2),
                fill="tozeroy", fillcolor="rgba(38,166,154,0.18)",
                name="bids cum",
            ))
            fig.add_trace(go.Scatter(
                x=asks_df["price"], y=asks_df["cum"], mode="lines",
                line=dict(shape="hv", color="#ef5350", width=2),
                fill="tozeroy", fillcolor="rgba(239,83,80,0.18)",
                name="asks cum",
            ))
            fig.update_layout(
                height=700, margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title="Price (USD)", yaxis_title="Cumulative size",
                showlegend=False,
            )
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
    # color-code level (best effort via st.dataframe formatter)
    def _row_style(row):
        color = {"INFO": "", "WARN": "color: #f9a825;",
                 "ERROR": "color: #ef5350;"}.get(row["level"], "")
        return [color] * len(row)
    try:
        styled = ev_df.style.apply(_row_style, axis=1)
        st.dataframe(styled, use_container_width=True, hide_index=True, height=420)
    except Exception:
        st.dataframe(ev_df, use_container_width=True, hide_index=True, height=420)

st.divider()

# ===== Section 4: legacy chart + latency =================================== #
st.subheader(f"Recent trade prices across venues — {selected_symbol}")
trades_all = store.snapshot_trades()
if trades_all.empty:
    st.info("Waiting for trades…")
else:
    plot_df = trades_all[trades_all["symbol"] == selected_symbol].copy()
    if plot_df.empty:
        st.info("Waiting for trades for the selected symbol…")
    else:
        plot_df["time"] = pd.to_datetime(plot_df["exchange_ts"], unit="ms")
        fig = go.Figure()
        for venue in plot_df["venue"].unique():
            v = plot_df[plot_df["venue"] == venue].sort_values("time")
            fig.add_trace(go.Scatter(
                x=v["time"], y=v["price"], mode="lines+markers",
                name=venue, marker=dict(size=4),
            ))
        fig.update_layout(
            height=320, margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title="Exchange time", yaxis_title="Price (USD)",
            legend=dict(orientation="h", y=1.1),
        )
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
        fig.update_layout(
            height=260, yaxis_title="Latency (ms)",
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough latency samples yet…")

# Raw normalized JSON
with st.expander("📦 Normalized JSON — what downstream clients would consume"):
    with store.lock:
        sample = [asdict(t) for t in list(store.trades)[-5:]]
    if sample:
        st.code(json.dumps(sample, indent=2), language="json")
    else:
        st.write("_No trades yet._")
