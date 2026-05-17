"""
Unified Crypto Market Data Gateway — Streamlit Community Cloud edition
======================================================================
Same architecture as the PoC:
  - WebSocket connectors to Binance, Coinbase Exchange, and Kraken
  - Normalization into a single internal schema (Trade, BBO)
  - Thread-safe in-memory store fed by background threads
  - Live dashboard

Cloud-specific hardening vs the Colab version:
  - Threads are owned by an @st.cache_resource singleton so they survive
    Streamlit script reruns (which happen on every UI interaction and
    auto-refresh tick).
  - Threads are daemon=True so they die cleanly when the container restarts.
  - Reconnect loop has bounded exponential backoff and watches a stop event.
  - No ngrok, no pyngrok, no Colab. Streamlit Cloud handles hosting.
"""

import json
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import websocket  # from `websocket-client` package
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

MAX_TRADES_BUFFER = 1000
REFRESH_INTERVAL_MS = 1000

# --------------------------------------------------------------------------- #
# Unified normalized schema
# --------------------------------------------------------------------------- #
@dataclass
class Trade:
    venue: str
    symbol: str
    price: float
    qty: float
    side: str            # "buy" or "sell" — taker side
    exchange_ts: float   # ms since epoch, as reported by venue
    ingest_ts: float     # ms since epoch, when we received it
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
# Thread-safe shared data store
# --------------------------------------------------------------------------- #
class DataStore:
    def __init__(self, max_trades: int = MAX_TRADES_BUFFER):
        self.trades: deque = deque(maxlen=max_trades)
        self.bbo: dict = {}  # (venue, symbol) -> BBO
        self.lock = threading.Lock()
        self.health = {
            v: {"connected": False, "messages": 0, "errors": 0,
                "last_msg_ts": 0.0, "started_ts": 0.0}
            for v in VENUES
        }
        self.stop_event = threading.Event()
        self._started = False
        self._threads: list = []

    # ---- mutators (called from WebSocket threads) ------------------------- #
    def add_trade(self, trade: Trade) -> None:
        with self.lock:
            self.trades.append(trade)
            h = self.health[trade.venue]
            h["messages"] += 1
            h["last_msg_ts"] = trade.ingest_ts

    def set_bbo(self, bbo: BBO) -> None:
        with self.lock:
            self.bbo[(bbo.venue, bbo.symbol)] = bbo
            h = self.health[bbo.venue]
            h["messages"] += 1
            h["last_msg_ts"] = bbo.ingest_ts

    def record_error(self, venue: str) -> None:
        with self.lock:
            self.health[venue]["errors"] += 1

    def set_connected(self, venue: str, connected: bool) -> None:
        with self.lock:
            self.health[venue]["connected"] = connected

    # ---- snapshots (called from Streamlit's render thread) ---------------- #
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

    # ---- lifecycle -------------------------------------------------------- #
    def start(self) -> None:
        if self._started:
            return
        self._started = True
        now = _now_ms()
        for v in VENUES:
            self.health[v]["started_ts"] = now
        for fn in (run_binance, run_coinbase, run_kraken):
            t = threading.Thread(target=fn, args=(self,), daemon=True,
                                 name=f"ws-{fn.__name__}")
            t.start()
            self._threads.append(t)


# --------------------------------------------------------------------------- #
# WebSocket clients (one per venue), with auto-reconnect
# --------------------------------------------------------------------------- #
def _now_ms() -> float:
    return time.time() * 1000.0


def _reverse_symbol(venue: str, native: str) -> str:
    for canonical, n in SYMBOL_MAP[venue].items():
        if n == native:
            return canonical
    return native


# ------ Binance ------------------------------------------------------------ #
def run_binance(store: DataStore) -> None:
    streams = []
    for sym in SYMBOLS:
        ns = SYMBOL_MAP["binance"][sym]
        streams.append(f"{ns}@trade")
        streams.append(f"{ns}@bookTicker")
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
                    price=float(data["p"]),
                    qty=float(data["q"]),
                    side="sell" if data.get("m") else "buy",
                    exchange_ts=float(data["T"]),
                    ingest_ts=ingest,
                    trade_id=str(data.get("t", "")),
                ))
            elif "@bookTicker" in stream:
                native = stream.split("@")[0]
                store.set_bbo(BBO(
                    venue="binance",
                    symbol=_reverse_symbol("binance", native),
                    bid=float(data["b"]),
                    bid_size=float(data["B"]),
                    ask=float(data["a"]),
                    ask_size=float(data["A"]),
                    exchange_ts=ingest,
                    ingest_ts=ingest,
                ))
        except Exception:
            store.record_error("binance")

    def on_open(_ws):  store.set_connected("binance", True)
    def on_close(_ws, *_a): store.set_connected("binance", False)
    def on_error(_ws, _e):  store.record_error("binance")

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
            store.record_error("coinbase")

    def on_open(ws):
        store.set_connected("coinbase", True)
        ws.send(json.dumps(subscribe))

    def on_close(_ws, *_a): store.set_connected("coinbase", False)
    def on_error(_ws, _e):  store.record_error("coinbase")

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
            store.record_error("kraken")

    def on_open(ws):
        store.set_connected("kraken", True)
        ws.send(json.dumps({"event": "subscribe", "pair": pairs,
                            "subscription": {"name": "trade"}}))
        ws.send(json.dumps({"event": "subscribe", "pair": pairs,
                            "subscription": {"name": "spread"}}))

    def on_close(_ws, *_a): store.set_connected("kraken", False)
    def on_error(_ws, _e):  store.record_error("kraken")

    _run_forever_with_backoff(url, on_message, on_open, on_close, on_error,
                              store, "kraken")


def _run_forever_with_backoff(url, on_message, on_open, on_close, on_error,
                              store, venue):
    """Reconnect loop with bounded exponential backoff."""
    backoff = 1.0
    while not store.stop_event.is_set():
        try:
            ws = websocket.WebSocketApp(
                url,
                on_message=on_message,
                on_open=on_open,
                on_close=on_close,
                on_error=on_error,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
            backoff = 1.0  # reset after a clean disconnect
        except Exception:
            store.record_error(venue)
        store.set_connected(venue, False)
        if store.stop_event.is_set():
            return
        time.sleep(backoff)
        backoff = min(backoff * 2, 30.0)


# --------------------------------------------------------------------------- #
# Streamlit cached singleton — survives reruns
# --------------------------------------------------------------------------- #
@st.cache_resource(show_spinner="Starting WebSocket connectors…")
def get_store() -> DataStore:
    store = DataStore()
    store.start()
    return store


# --------------------------------------------------------------------------- #
# UI
# --------------------------------------------------------------------------- #
st.set_page_config(
    page_title="Crypto Market Data Gateway — PoC",
    page_icon="🪙",
    layout="wide",
)
st.title("🪙 Unified Crypto Market Data Gateway — PoC")
st.caption(
    "Live normalized WebSocket feeds from Binance, Coinbase, and Kraken, "
    "mapped into a single internal schema. Hosted on Streamlit Community Cloud."
)

store = get_store()
st_autorefresh(interval=REFRESH_INTERVAL_MS, key="autorefresh")

# ------ Sidebar: controls + feed health ----------------------------------- #
with st.sidebar:
    st.header("Controls")
    selected_symbol = st.selectbox("Symbol", SYMBOLS, index=0)

    st.header("Feed health")
    health = store.snapshot_health()
    now = _now_ms()
    for venue, h in health.items():
        dot = "🟢" if h["connected"] else "🔴"
        if h["last_msg_ts"]:
            age_txt = f"{(now - h['last_msg_ts']) / 1000.0:.1f}s ago"
        else:
            age_txt = "no data yet"
        st.markdown(
            f"{dot} **{venue}** &nbsp; "
            f"`{h['messages']:,}` msgs &nbsp;|&nbsp; "
            f"last: {age_txt} &nbsp;|&nbsp; "
            f"errors: {h['errors']}"
        )

    st.divider()
    st.caption(
        "Replacing one connector or adding a venue should not require any "
        "change to the dashboard or downstream consumers. That's the test "
        "of the normalization layer."
    )

# ------ Main: BBO + trade tape -------------------------------------------- #
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
        m1.metric("Best bid (across venues)",
                  f"${best_bid['bid']:,.2f}", f"@ {best_bid['venue']}")
        m2.metric("Best ask (across venues)",
                  f"${best_ask['ask']:,.2f}", f"@ {best_ask['venue']}")
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

# ------ Price chart across venues ----------------------------------------- #
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
            height=360, margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title="Exchange time", yaxis_title="Price (USD)",
            legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, use_container_width=True)

# ------ Latency distribution ---------------------------------------------- #
st.subheader("Ingest latency by venue (last 300 trades, all symbols)")
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
            height=280, yaxis_title="Latency (ms)",
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough latency samples yet…")

# ------ Raw normalized JSON sample ---------------------------------------- #
with st.expander("📦 Normalized JSON — what downstream clients would consume"):
    with store.lock:
        sample = [asdict(t) for t in list(store.trades)[-5:]]
    if sample:
        st.code(json.dumps(sample, indent=2), language="json")
    else:
        st.write("_No trades yet._")

st.divider()
st.caption(
    "Hosted on Streamlit Community Cloud. Latency reflects the Streamlit "
    "Cloud VM's network path to each exchange — not a production system. "
    "A real product would colocate ingestors in the same region as each "
    "venue's matchers and use binary serialization."
)
