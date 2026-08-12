# Live-Edge Paper Trading Market Manager

Rust service for watching a live YouTube trading stream, transcribing it, extracting structured option-trade instructions with Gemini, and simulating those trades against live INDstocks ticks. It includes a real-time live/history dashboard.

This project is **paper-only**. It reads market data but contains no broker order endpoint and cannot place a real order.

## Paper pipeline

The `paper` command runs the complete pipeline:

1. `yt-dlp` resolves the stream's current playback URL. A long-running FFmpeg process begins at the current live edge; it does not replay the stream from the beginning.
2. FFmpeg closes an exact 4-second MPEG-TS segment at a time. ElevenLabs Scribe v2 transcribes each segment, with bounded concurrency.
3. Every two consecutive segments are remuxed into one 8-second MP4. Gemini receives that clip, the two timestamped transcript chunks, prompt-send time/data age, watched option LTPs, and any open LLM-managed trades.
4. Gemini returns strict structured actions such as watch, place entry, update levels, cancel, hold, or exit. The paper broker rejects trade and exit instructions below the configured confidence threshold.
5. Candidate, pending, and open contracts drive dynamic INDstocks subscriptions. Fresh ticks fill paper orders, update P/L, advance mechanical stops, and close positions.
6. State, audit events, trade history, and dashboard views are updated continuously. At the configured IST end-of-day time, pending entries are cancelled and open positions close on their first fresh tick at or after the cutoff.

Completed clips are retained under `data/media/clips/`; with the default `CLIPS_TO_KEEP=3`, acknowledged older clips are deleted. Temporary 4-second segments are removed after transcription and window assembly acknowledge them.

## Rolling multimodal context

Every 8-second Gemini response includes a complete, bounded context snapshot: a detailed spoken summary, detailed visual summary, combined summary, structured key visual points, and active trade episodes. The runtime feeds that snapshot into the next Gemini call in FIFO order, so contracts, stated levels, and episode state can carry across a long stream without an ever-growing transcript.

The latest snapshot is stored in `data/paper/stream_context.json` and is restored only when its stream URL and IST trading date match the current run. Earlier context may preserve contract identity and explicitly stated levels, but it is not fresh evidence and cannot trigger an order by itself; actionable commands still require evidence from the current 8-second window. All resulting execution remains paper-only.

Gemini credentials form one unnamed shared round-robin pool (`GEMINI_API_KEY_1` through `GEMINI_API_KEY_16`). Quota, authentication, transport, timeout, and server failures cool down the affected slot and retry the same window with another healthy key. Per-slot health is exposed without revealing credential values.

## Requirements

- Rust/Cargo toolchain
- `yt-dlp`
- FFmpeg
- Gemini API key with access to the configured model
- One or more ElevenLabs keys in the configured credential vault
- INDstocks `token.txt`, plus `totp.txt` for one-time token renewal when necessary

Both media tools may be on `PATH` or configured with absolute paths:

```powershell
Get-Command yt-dlp
Get-Command ffmpeg
```

## Secure configuration

The local secret file is deliberately stored outside this project at:

```powershell
${env:LOCALAPPDATA}\observer-trading\.env
```

The runtime loads that path automatically on Windows. Set `OBSERVER_ENV_PATH`
to select a different external file. Render should provide the same settings as
service environment variables and does not require a file. Never place a real
`.env` inside this project, print credentials in terminal output, or commit
secrets.

The important local/Render settings are:

```dotenv
DATABASE_URL=<private-neon-url>
GEMINI_API_KEY_1=<private-key>
GEMINI_API_KEY_2=<private-key>
GEMINI_API_KEY_3=<private-key>
ELEVENLABS_API_KEY_1=<private-key>
ELEVENLABS_API_KEY_2=<private-key>
ELEVENLABS_API_KEY_3=<private-key>
BROKER_CLIENT_ID=<private-client-id>
BROKER_MPIN=<private-mpin>
BROKER_TOTP_SECRET=<private-totp-secret>
YOUTUBE_CHANNEL_URL=https://www.youtube.com/@TRADINGCAFEINDIA
GEMINI_MODEL=gemini-3.5-flash-lite

PAPER_ACCOUNTS=account_1:5000,account_2:10000,account_3:2000,account_4:15000,account_5:20000
NIFTY_LOT_SIZE=65
SENSEX_LOT_SIZE=20
MIN_TRADE_CONFIDENCE=65
ENTRY_BUFFER_POINTS=2
CLIPS_TO_KEEP=3
STT_CONCURRENCY=4

YT_DLP_PATH=yt-dlp
FFMPEG_PATH=ffmpeg
DASHBOARD_BIND=127.0.0.1:8787
```

Keep the current lot-size values at 65 for NIFTY and 20 for SENSEX.

Relative application paths are resolved from this project directory. Process
environment variables override non-secret file values; when the external file
contains `GEMINI_API_KEY`, that file value is authoritative and cannot be
silently replaced by an inherited shell value. Environment-file patterns,
runtime data, and build output are gitignored.

Gemini 3.5 Flash-Lite deprecates sampling controls such as `temperature`, so this runtime does not send them. Deterministic extraction is enforced with the strict system instruction, JSON response schema, semantic validation, and `thinking_level=minimal` instead.

The runtime uses `BROKER_CLIENT_ID`, `BROKER_MPIN`, and
`BROKER_TOTP_SECRET` when present. The old `totp.txt` format remains a local
fallback only. The access token is validated first and regenerated once when
needed; failures are not retried indefinitely.

## Render deployment

`render.yaml` and `Dockerfile` run the `daemon` command. Render supplies
`PORT`; the dashboard binds to `0.0.0.0:$PORT`, while local runs retain
`127.0.0.1:8787`. The dashboard and `/api/health` are publicly reachable on
the Render URL.

The daemon verifies Asia/Kolkata time, skips weekends and the configured NSE
F&O holidays, checks the configured channel every minute from 09:00 through
15:30 IST, and gives a discovered session a hard 16:00 IST worker deadline.
After a session, raw media and live tick-log directories are cleared. Durable
wallets, broker state, rolling context, closed trades, and daily account rows
are stored in Neon. Render secrets must be entered in its Environment page;
the external Windows `.env` is never copied into the image.

Render Free still needs an external wake monitor. Configure your cron service
to request `https://<service>.onrender.com/api/health` at least every ten
minutes from approximately 08:55 through 16:00 IST on market weekdays. Stop
those requests after 16:00 so the free web service can sleep.

## Build and run

Build and test from this directory:

```powershell
cargo test
cargo build --release
```

For a continuous hidden/background run, use the launcher:

```powershell
.\run_paper.ps1 -StreamUrl "https://www.youtube.com/watch?v=dRn3NYVaiIQ"
```

The launcher requires the release binary. It returns the PID and log paths, stores the same information in `data/runtime/paper_process.json`, and redirects output to timestamped files under `data/logs/`.

For a foreground run with logs in the current terminal:

```powershell
cargo run --release -- paper `
  --stream-url "https://www.youtube.com/watch?v=dRn3NYVaiIQ"
```

Press `Ctrl+C` to stop a foreground run. For a finite diagnostic run, add `--duration-seconds 120`, or use `-DurationSeconds 120` with `run_paper.ps1`. Omit the duration for continuous operation.

## Paper execution and account sizing

The default wallets are INR 5,000, 10,000, 2,000, 15,000, and 20,000. Each accepted setup is independently simulated in both strategy books for every account:

- `LLM_EXIT`: keeps the streamer's hard stop active and allows a confidence-qualified Gemini exit instruction. The exit executes on the next accepted fresh tick.
- `MOVING_SL`: ignores LLM exit requests and follows the deterministic phase trail below.

Each account uses the maximum number of whole lots that fits its free cash while reserving entry and exit charges. Accounts that cannot afford one complete lot do not receive that order. NIFTY uses 65 units per lot; SENSEX uses 20.

The entry cap is the extracted entry plus `ENTRY_BUFFER_POINTS` (2 by default). A pending entry fills only on a fresh, post-order tick at or below that cap, records the actual tick LTP as the fill price, and expires after 60 seconds if unfilled. Version 1 accepts BUY option setups with valid `SL < entry < T1 < T2` ordering; T2 is optional. If more than one weekly contract matches a strike/type, an explicit or previously established expiry is required.

### Moving-stop phases

| Phase | Trigger | Stop action |
|---|---|---|
| 0 | Entry filled | Streamer's hard SL |
| 1 | Halfway from entry to T1 | Entry + 30% of the entry-to-T1 distance |
| 2 | T1 reached | Entry + 50% of the entry-to-T1 distance |
| 3 | Halfway from T1 to T2 | Previous SL + 30% of the T1-to-T2 distance |
| 4 | T2 reached | T2 - 5 for NIFTY; T2 - 10 for SENSEX |
| 5 | Runner | Every +5 NIFTY points moves SL +4; every +8 SENSEX points moves SL +6 |

If T2 is absent, the runner starts from T1 after Phase 2. Stops and phases only move upward, including when one gap-up tick crosses several phases.

## Dashboard

Open [http://127.0.0.1:8787](http://127.0.0.1:8787) while the paper runtime is active. The dashboard shows session and component health, transcript/Gemini timing, market freshness, signals, pending entries, live positions, per-account capital and P/L, equity, and filterable trade history. It updates through Server-Sent Events rather than page reloads.

HTTP endpoints:

- `GET /api/health` — health, tick age, uptime, and component status
- `GET /api/state` — complete current dashboard snapshot
- `GET /api/events` — Server-Sent Events revision stream
- `GET /api/history` — paginated/filterable closed-trade history
- `GET /api/export.csv` — CSV export using the same history filters

- `GET /api/logs?limit=100&level=ERROR&component=gemini` returns newest-first sanitized operational events; `limit` must be from 1 through 200.

Example health check:

```powershell
Invoke-RestMethod http://127.0.0.1:8787/api/health
```

The dashboard binds only to localhost by default. Change `DASHBOARD_BIND` deliberately if remote access is required. Render intentionally exposes the dashboard without authentication. The public logs endpoint contains only bounded operational metadata: it strips credential shapes, authorization values, database URLs, HTTP query strings, and control characters; it never includes prompts, transcripts, broker payloads, or media URLs. Neon retains the newest 1,000 sanitized events and the daemon reloads the latest 200 after restart.

## Runtime files

- `data/paper/state_latest.json` — latest durable broker snapshot
- `data/paper/trade_history.json` — cumulative, deduplicated closed-trade history
- `data/paper/stream_context.json` — bounded rolling multimodal context, scoped to the exact stream URL and IST trading date
- `data/paper/sessions/paper_<UTC timestamp>/events.jsonl` — per-run pipeline audit and broker events
- `data/media/clips/` — retained 8-second MP4 windows
- `data/media/segments/<session>/` — temporary exact 4-second capture segments
- `data/logs/paper_<timestamp>.stdout.log` and `.stderr.log` — background-launch logs
- `data/runtime/paper_process.json` — background PID, start time, stream URL, and log paths

## Other market-data commands

Validate INDstocks authentication:

```powershell
cargo run -- auth-check
```

Record one or more live contracts without starting the paper pipeline:

```powershell
cargo run -- live `
  --contract "sensex 13 aug 2026 78800 pe" `
  --contract "nifty 13 aug 2026 25000 ce" `
  --interval-seconds 10
```

Fetch provider-supplied historical candles:

```powershell
cargo run -- backtest `
  --contract "sensex 13 aug 2026 78800 pe" `
  --date 2026-08-07
```

Live samples are written under `data/live/`; backtest CSVs are written under `data/backtest/`. Historical data is accepted only when provider timestamps match the requested date and expected trading-day coverage.
