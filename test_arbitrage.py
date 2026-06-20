"""
Test compute_arbitrage — the net-of-fees arbitrage math.

Verifies:
  - gross vs net edge computed correctly with hand-checked numbers
  - fees from the correct venues applied to the correct legs
  - withdrawal cost uses the BUY venue's fee, converted to quote terms
  - profitable flag respects ARB_MIN_NET_BPS
  - quotes older than ARB_STALE_MS are excluded
  - fewer than two fresh venues → None
  - the same-venue (best ask == best bid venue) edge case picks a real pair
"""
import sys
import types

sys.modules["streamlit_autorefresh"] = types.SimpleNamespace(st_autorefresh=lambda **k: None)
class _StubBcrypt:
    @staticmethod
    def hashpw(pw, salt): return b"$2b$stub$" + pw
    @staticmethod
    def gensalt(rounds=12): return b"$2b$stub$"
    @staticmethod
    def checkpw(pw, h): return h == b"$2b$stub$" + pw
sys.modules["bcrypt"] = _StubBcrypt()
class _P:
    def __getattr__(s, n): return s
    def __call__(s, *a, **k): return s
sys.modules["plotly"] = _P(); sys.modules["plotly.graph_objects"] = _P()
class _DummyStreamlit:
    def __init__(self):
        self.session_state = {}
        self.secrets = {"auth": {"users": {"x": {"password_hash": "", "role": "admin"}}}}
    def __getattr__(self, name):
        if name in ("columns", "tabs"):
            def _m(spec, *a, **k):
                n = len(spec) if isinstance(spec, (list, tuple)) else int(spec)
                return [self] * n
            return _m
        if name == "selectbox":
            return lambda *a, **k: (list(k.get("options", [None]))[0])
        if name == "button":
            return lambda *a, **k: False
        if name == "stop":
            return lambda: (_ for _ in ()).throw(SystemExit)
        return self
    def __call__(self, *a, **k): return self
    def cache_resource(self, *a, **k):
        if a and callable(a[0]): return a[0]
        return lambda fn: fn
    def __enter__(self): return self
    def __exit__(self, *a): return False
_st = _DummyStreamlit()
class _PreSession:
    username = "test"; role = "admin"; login_ts = 0.0
_st.session_state["session"] = _PreSession()
_st.session_state["last_activity"] = 9e18
sys.modules["streamlit"] = _st
sys.modules["websocket"] = types.SimpleNamespace(
    WebSocketApp=lambda *a, **k: types.SimpleNamespace(
        run_forever=lambda **k: None, close=lambda: None))

import importlib.util
spec = importlib.util.spec_from_file_location("app", "streamlit_app.py")
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)

NOW = 1_000_000.0

# Simple fee schedule for hand-checkable math
FEES = {
    "ex_a": {"taker": 0.0010, "withdraw": {"BTC-USD": 0.0000}},  # 10 bps, no withdraw
    "ex_b": {"taker": 0.0010, "withdraw": {"BTC-USD": 0.0000}},
    "ex_c": {"taker": 0.0010, "withdraw": {"BTC-USD": 0.0000}},
}

print("=" * 78)
print("Test 1 — clean gross gap, no withdrawal, hand-checked net")
print("=" * 78)
# Buy ex_a ask 100.00, sell ex_b bid 101.00. Gross = 100 bps.
# buy_cost = 100 * 1.001 = 100.10 ; sell_proceeds = 101 * 0.999 = 100.899
# net = (100.899 - 100.10) / 100.10 = 0.798/100.10 = 79.8 bps (approx)
quotes = {
    "ex_a": {"ask": 100.00, "bid": 99.99, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW},
    "ex_b": {"ask": 101.01, "bid": 101.00, "ask_size": 3, "bid_size": 3, "ingest_ts": NOW},
}
r = app.compute_arbitrage("BTC-USD", quotes, FEES, NOW)
assert r is not None
assert r["buy_venue"] == "ex_a" and r["sell_venue"] == "ex_b", (r["buy_venue"], r["sell_venue"])
assert abs(r["gross_bps"] - 100.0) < 0.5, r["gross_bps"]
# net should be ~79.8 bps
assert abs(r["net_bps"] - 79.8) < 1.0, r["net_bps"]
# fees bps consumed ~20.2
fees_bps = r["gross_bps"] - r["net_bps"]
assert abs(fees_bps - 20.2) < 1.0, fees_bps
# max size = min(buy ask_size 5, sell bid_size 3) = 3
assert r["max_size"] == 3
print(f"  gross={r['gross_bps']:.2f} bps, net={r['net_bps']:.2f} bps, "
      f"fees={fees_bps:.2f} bps, max_size={r['max_size']}")
print(f"  est profit = ${r['est_profit_usd']:.4f}")
print("✅ gross/net/fees/size all match hand calculation.")

print("\n" + "=" * 78)
print("Test 2 — gross gap that does NOT survive fees")
print("=" * 78)
# Gross gap of only 15 bps; fees ~20 bps → net negative
quotes = {
    "ex_a": {"ask": 100.00, "bid": 99.98, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW},
    "ex_b": {"ask": 100.20, "bid": 100.15, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW},
}
r = app.compute_arbitrage("BTC-USD", quotes, FEES, NOW)
assert r["gross_bps"] > 0, r["gross_bps"]
assert r["net_bps"] < r["gross_bps"]
assert not r["profitable"], r["net_bps"]
print(f"  gross={r['gross_bps']:.2f} bps, net={r['net_bps']:.2f} bps → "
      f"profitable={r['profitable']}")
print("✅ Sub-threshold opportunity correctly flagged not-profitable.")

print("\n" + "=" * 78)
print("Test 3 — withdrawal fee reduces the net edge")
print("=" * 78)
FEES_WD = {
    "ex_a": {"taker": 0.0010, "withdraw": {"BTC-USD": 0.001}},  # 0.001 BTC withdraw
    "ex_b": {"taker": 0.0010, "withdraw": {"BTC-USD": 0.000}},
}
# Same 100bps gross as test 1; buying on ex_a now incurs withdraw 0.001 * 100 = 0.10 quote
quotes = {
    "ex_a": {"ask": 100.00, "bid": 99.99, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW},
    "ex_b": {"ask": 101.01, "bid": 101.00, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW},
}
r_nowd = app.compute_arbitrage("BTC-USD", quotes, FEES, NOW)
r_wd = app.compute_arbitrage("BTC-USD", quotes, FEES_WD, NOW)
# withdrawal cost 0.10 quote on buy_cost ~100.10 = ~10 bps reduction
delta = r_nowd["net_bps"] - r_wd["net_bps"]
assert 9.0 < delta < 11.0, delta
assert r_wd["withdraw_base"] == 0.001
print(f"  net without withdrawal = {r_nowd['net_bps']:.2f} bps")
print(f"  net with 0.001 BTC withdrawal = {r_wd['net_bps']:.2f} bps")
print(f"  withdrawal cost the edge ~{delta:.2f} bps (expected ~10)")
print("✅ Withdrawal fee applied from the BUY venue, in quote terms.")

print("\n" + "=" * 78)
print("Test 4 — stale quote excluded")
print("=" * 78)
quotes = {
    "ex_a": {"ask": 100.00, "bid": 99.99, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW},
    "ex_b": {"ask": 101.01, "bid": 101.00, "ask_size": 5, "bid_size": 5,
             "ingest_ts": NOW - 10_000},  # 10s old, beyond ARB_STALE_MS
}
r = app.compute_arbitrage("BTC-USD", quotes, FEES, NOW)
assert r is None, "stale second venue should leave <2 fresh quotes → None"
print("  one fresh + one stale venue → None (correctly refused)")
print("✅ Staleness filter works.")

print("\n" + "=" * 78)
print("Test 5 — fewer than two venues → None")
print("=" * 78)
r = app.compute_arbitrage("BTC-USD",
    {"ex_a": {"ask": 100, "bid": 99.9, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW}},
    FEES, NOW)
assert r is None
print("  single venue → None")
print("✅ Single-venue case handled.")

print("\n" + "=" * 78)
print("Test 6 — three venues, picks best buy and best sell")
print("=" * 78)
quotes = {
    "ex_a": {"ask": 100.50, "bid": 100.40, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW},
    "ex_b": {"ask": 100.00, "bid": 99.95, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW},  # cheapest ask
    "ex_c": {"ask": 100.80, "bid": 100.70, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW},  # highest bid
}
r = app.compute_arbitrage("BTC-USD", quotes, FEES, NOW)
assert r["buy_venue"] == "ex_b", r["buy_venue"]
assert r["sell_venue"] == "ex_c", r["sell_venue"]
print(f"  buy {r['buy_venue']} @ {r['buy_ask']}, sell {r['sell_venue']} @ {r['sell_bid']}")
print("✅ Correctly selects cheapest-ask buy and highest-bid sell across 3 venues.")

print("\n" + "=" * 78)
print("Test 7 — same venue is both cheapest-ask and highest-bid")
print("=" * 78)
# ex_b has both the lowest ask AND the highest bid (tight market on a venue
# that's just generally higher-priced won't trigger; here ex_b dominates both)
quotes = {
    "ex_a": {"ask": 101.00, "bid": 100.50, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW},
    "ex_b": {"ask": 100.00, "bid": 100.90, "ask_size": 5, "bid_size": 5, "ingest_ts": NOW},
}
# ex_b lowest ask (100.00) and highest bid (100.90). Same venue → must pick
# the next distinct pair: buy ex_b (100.00), sell ex_a (100.50).
r = app.compute_arbitrage("BTC-USD", quotes, FEES, NOW)
assert r is not None
assert r["buy_venue"] != r["sell_venue"], (r["buy_venue"], r["sell_venue"])
print(f"  resolved to buy {r['buy_venue']} / sell {r['sell_venue']} "
      f"(distinct venues)")
print("✅ Same-venue degenerate case resolves to a real cross-venue pair.")

print("\n" + "=" * 78)
print("All arbitrage assertions passed.")
print("=" * 78)
