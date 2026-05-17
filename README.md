# Unified Crypto Market Data Gateway — Streamlit Cloud edition

A live, normalized crypto market data PoC. Hosted on **Streamlit Community Cloud**,
no Colab, no ngrok, just a public URL pointed at a GitHub repo.

## Files in this repo

| File | Purpose |
|---|---|
| `streamlit_app.py` | The whole app — WebSocket clients, normalization, dashboard |
| `requirements.txt` | Dependencies Streamlit Cloud installs at build time |
| `runtime.txt` | Pins Python 3.11 |
| `.streamlit/config.toml` | Theme, headless mode, disable telemetry |

Streamlit Cloud looks for a file named `streamlit_app.py` by default — that's
why the entrypoint is named that and not `app.py`.

## Deploy in 5 steps

1. **Create a GitHub repo** (public is required for the free tier) and push these
   four files to it. Folder structure must be:
   ```
   your-repo/
   ├── streamlit_app.py
   ├── requirements.txt
   ├── runtime.txt
   └── .streamlit/
       └── config.toml
   ```
2. Go to **[share.streamlit.io](https://share.streamlit.io)** and sign in with GitHub.
3. Click **Create app** → **Deploy a public app from GitHub**.
4. Pick your repo, branch `main`, main file `streamlit_app.py`. Leave the rest at defaults.
5. Click **Deploy**. First build takes 2–4 minutes. You'll get a URL like
   `https://<your-app>.streamlit.app`.

That's it. Push commits to `main` and the app auto-redeploys.

## What this proves

- **Normalization works.** Three venues with very different message shapes
  (Binance dict streams, Coinbase typed messages, Kraken positional arrays)
  produce identical `Trade` and `BBO` records. Downstream code never branches
  on venue.
- **Architecture survives the rerun model.** Streamlit reruns the entire script
  on every interaction and every auto-refresh tick. The `@st.cache_resource`
  singleton keeps the WebSocket threads and data store alive across those
  reruns — no reconnection storms, no lost messages.
- **Real-time fan-out at PoC scale.** Background ingestor threads feed an
  in-memory store; the render thread reads consistent snapshots under a lock.
  Replace the store with NATS/Redpanda and consumers can be remote.

## What this does *not* prove

- **Latency.** Streamlit Cloud runs on a shared US-based VM. Numbers you see
  reflect that path, not what a colocated production ingestor would achieve
  (sub-50ms target). Expect hundreds of milliseconds, especially to Kraken.
- **Throughput.** Python + GIL + JSON parsing tops out long before what
  Rust/Go/C++ ingestors handle. Fine for two symbols across three venues at
  retail rates; not fine for full L2 books across 50+ pairs.
- **Order book reconstruction.** The PoC uses pre-aggregated BBO channels
  (`bookTicker`/`ticker`/`spread`). A real product would maintain L2 books
  with sequence-gap detection and snapshot recovery — explicitly out of scope.
- **Durability.** No persistence, no replay, no auth. The in-memory store
  resets every time Streamlit Cloud restarts the container.

## Known Streamlit Cloud gotchas

- **Container sleeps after ~7 days of zero visitors.** First visitor after that
  triggers a cold start (~30s). Irrelevant for an actively-used demo.
- **1 GB RAM, 1 CPU.** Plenty for this PoC; would not be plenty for full L2
  across many venues.
- **Some exchanges occasionally rate-limit cloud IPs.** If Binance refuses to
  connect from the Streamlit Cloud egress IP, the other two venues will still
  work and demonstrate the normalization layer.
- **No control over region.** You can't pin the app close to a specific
  exchange. That's fine for a PoC, not for a production latency story.

## Why this is a better PoC host than Colab

| | Colab | Streamlit Cloud |
|---|---|---|
| Idle timeout | ~90 min | ~7 days (functionally always-on) |
| Public URL | ngrok tunnel, rotates | Stable `*.streamlit.app` |
| Deploy | Upload notebook, paste token, run cells | `git push` |
| Auth setup | Sign up for ngrok | None |
| WebSockets survive restarts | No | Yes (until container restart) |
| Free tier RAM | Variable | 1 GB |

## Local development

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

App opens at `http://localhost:8501`.
