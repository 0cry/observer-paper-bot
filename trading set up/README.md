# Observer Paper Desk

Rust service that observes a live YouTube trading stream and simulates option trades against live INDstocks ticks. It is **paper-only**: there is no broker-order API or live-order routing in this repository.

## Current pipeline

1. `yt-dlp` resolves the live edge and FFmpeg records exact three-second transport-stream segments. It never replays the stream from its beginning.
2. ElevenLabs transcribes each three-second segment.
3. A lightweight blocker evaluates every individual transcript segment. Blocked material is not sent onward. A selected dispatch normally has four retained segments (12 seconds); a must-pass segment may dispatch alone, and incomplete candidates expire after 30 seconds.
4. OpenAI Responses (`gpt-5.6-luna` by default) receives the selected transcript, current timestamp/data age, current option ticks, open paper state, and bounded rolling context. The original latest JPEG frame is included only on every fifth successfully committed analysis call.
5. Rust validates model output, routes contracts, enforces freshness and affordability, and creates paper orders only. A pending entry fills only from a later fresh market tick.
6. The broker updates paper P/L tick by tick. `LLM_EXIT` queues a fresh-tick exit; `MOVING_SL` follows the deterministic trail.

The model never proves a trade result. Entry state becomes `ENTRY_CALLED` only after the paper broker actually placed an order. Runtime-authored outcomes are bounded and carried into the next call; model-provided context cannot forge them.

There is no confidence score or confidence threshold in entry, exit, persistence, API, CSV, or dashboard acceptance. Rust `accepted` status is the source of truth.

## Requirements

- Rust/Cargo
- `yt-dlp`
- FFmpeg
- One to three OpenAI API keys loaded into RAM from the dashboard after startup
- One or more ElevenLabs API keys
- INDstocks token access; TOTP settings only if token renewal is configured

## Secrets and configuration

Never put a real `.env` in this repository. On Windows the default external secret file is:

```powershell
${env:LOCALAPPDATA}\observer-trading\.env
```

Set `OBSERVER_ENV_PATH` to use another external file. In hosted environments, including Render, provide only non-OpenAI service configuration as environment variables. Do not copy the Windows vault into the image, Git, logs, or documentation. **Do not add OpenAI keys to Render or Git:** after the dashboard loads, paste one to three keys into the Runtime OpenAI keys panel. They live only in process memory, are never shown again, and are cleared on process restart or after all loaded keys fail.

```dotenv
OPENAI_MODEL=gpt-5.6-luna

ELEVENLABS_API_KEY_1=<private-key>
# Optional fallback keys: ELEVENLABS_API_KEY_2, ELEVENLABS_API_KEY_3, ...
DATABASE_URL=<private-neon-url>
YOUTUBE_CHANNEL_URL=https://www.youtube.com/@TRADINGCAFEINDIA

PAPER_ACCOUNTS=account_1:5000,account_2:10000,account_3:2000,account_4:15000,account_5:20000
NIFTY_LOT_SIZE=65
SENSEX_LOT_SIZE=20
ENTRY_BUFFER_POINTS=2
CLIPS_TO_KEEP=3
STT_CONCURRENCY=4
ELEVENLABS_KEY_LIMIT=3
YT_DLP_PATH=yt-dlp
FFMPEG_PATH=ffmpeg
DASHBOARD_BIND=127.0.0.1:8787
```

OpenAI keys are intentionally absent from this configuration example. The dashboard key endpoint accepts a maximum of three values, accepts only additions/clear-all, and never returns values, fragments, or headers.

## Model request and limits

The service uses the OpenAI Responses API with `service_tier: fast`, `reasoning.effort: low`, strict JSON schema, and no stored provider conversation. It sends a stable static prompt prefix plus changing runtime input. Rolling context and authoritative outcomes are bounded before each request.

Local IST counters and HTTP response headers are recorded for rate-limit visibility. They are observability only: failures remain safe, and no key or quota detail is exposed in the dashboard. Model, schema, or context-commit failures do not create paper orders.

## Paper execution

Each accepted BUY setup is independently simulated for every configured account under both approaches:

- **Approach 1 / LLM Exit**: streamer's hard stop remains active; a validated LLM exit queues for the next fresh tick.
- **Approach 2 / Moving SL**: the deterministic phase trail manages the exit.

Orders use whole affordable lots, include entry/exit charges, use the extracted entry plus `ENTRY_BUFFER_POINTS` as the cap, and require a fresh post-order tick to fill. Invalid levels, stale evidence, unresolved contracts, duplicate setups, insufficient capital, and zero-order placements are rejected without consuming the candidate watch. NIFTY is 65 units/lot and SENSEX is 20 units/lot by default.

### Moving-stop phases

| Phase | Trigger | Stop action |
|---|---|---|
| 0 | Entry filled | Streamer's hard SL |
| 1 | Halfway from entry to T1 | Entry + 30% of entry-to-T1 distance |
| 2 | T1 | Entry + 50% of entry-to-T1 distance |
| 3 | Halfway from T1 to T2 | Previous SL + 30% of T2 distance |
| 4 | T2 | T2 - 5 NIFTY / T2 - 10 SENSEX |
| 5 | Runner | +5/+4 NIFTY; +8/+6 SENSEX trail increments |

Without T2, Phase 5 begins after T1.

## Dashboard and health

The dashboard is a live paper-trading view; it also restores durable wallet/history information when workers are offline. It shows account allocation, positions, pending orders, realized/unrealized P&L, signals, component health, and sanitized provider-key health.

Local URL: [http://127.0.0.1:8787](http://127.0.0.1:8787)

- `GET /api/health` — health, tick age, uptime, and components
- `GET /api/state` — dashboard state
- `GET /api/events` — Server-Sent Events revision stream
- `GET /api/history` — paginated closed-trade history
- `GET /api/export.csv` — history CSV
- `GET /api/logs?limit=100&level=ERROR&component=analysis` — sanitized operational events

Signals display the backend decision reason first, including routing, freshness, duplicate, or affordability outcomes. Public logs exclude credentials, prompts, transcripts, broker payloads, and media URLs.

## Persistence and restart safety

- `data/paper/state_latest.json` — local paper-broker snapshot
- `data/paper/stream_context.json` — bounded context for one stream URL and IST date
- `data/paper/trade_history.json` — deduplicated closed-trade history
- `data/paper/sessions/paper_<UTC timestamp>/events.jsonl` — audit events
- `data/media/clips/` — latest retained media; older acknowledged clips are deleted

When Neon is configured, runtime state is saved as a paired checkpoint. At startup the newest valid local/Neon state is selected by checkpoint timestamp; equal timestamps deterministically favor local state. Context is reconciled against the selected broker snapshot so an orphan context cannot prove an order.

## Run locally

```powershell
cargo test
cargo build --release

cargo run --release -- paper `
  --stream-url "https://www.youtube.com/watch?v=dRn3NYVaiIQ"
```

Use `Ctrl+C` to stop a foreground run. `run_paper.ps1` can launch a hidden background paper session and records its PID/log paths under `data/runtime/`.

## Automation and hosting

The daemon uses Asia/Kolkata time, skips weekends/configured holidays, discovers the configured YouTube channel during market hours, and ends workers at the configured cutoff. Raw media/tick artifacts are cleaned after sessions according to runtime retention rules; durable paper state remains.

For Render, set secrets in Render's Environment page and bind through `PORT`. Do not deploy a `.env` file. The dashboard is intentionally unauthenticated if exposed publicly, so use a private service/network layer if public visibility is not acceptable.

## Other commands

```powershell
cargo run -- auth-check
cargo run -- live --contract "nifty 13 aug 2026 25000 ce"
cargo run -- backtest --contract "sensex 13 aug 2026 78800 pe" --date 2026-08-07
```

Historical provider data is candle data, not reconstructed tick-by-tick history.
