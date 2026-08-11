# Render Deployment and Runtime Logs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a secret-safe persistent runtime logs API, publish the paper-only bot from a private GitHub repository to Render, and verify the public service for unattended IST market-day operation.

**Architecture:** `DashboardState` gains a bounded live log buffer and `/api/logs` filtering. Neon remains the durable source for the last 1,000 operational events, while scheduler and paper-runtime error sites publish sanitized events to both stores. The existing Docker deployment is pushed to a private repository, created as one Render Docker web service, and configured with secrets only through Render environment variables.

**Tech Stack:** Rust 2024, Axum 0.8, Tokio, Serde, SQLx/PostgreSQL, Neon, Docker, GitHub REST/Git, Render REST API.

---

### Task 1: Secret-safe dashboard log model and endpoint

**Files:**
- Modify: `trading set up/src/dashboard.rs`
- Test: `trading set up/src/dashboard.rs` unit tests

- [ ] **Step 1: Write the failing tests**

Add `runtime_log_sanitizer_redacts_secrets_and_bounds_output` and `runtime_logs_response_filters_orders_and_validates_limit`. The tests call the desired `sanitize_log_message` and `runtime_logs_response` functions. Inputs include multiline text, control characters, `github_pat_`, `rnd_`, `AIza`, `sk_`, a PostgreSQL URL, and an HTTP query string. Assertions require one line, at most 512 characters, no secret fragments or query parameters, newest-first results, case-insensitive level/component filters, and rejection of limits outside `1..=200`.

- [ ] **Step 2: Run the tests and verify RED**

Run `cargo test --quiet dashboard::tests::runtime_log` from `trading set up`.
Expected: compilation fails because the desired log APIs do not exist.

- [ ] **Step 3: Implement the minimal model**

Add `RuntimeLogEntry { event_id: i64, occurred_at: String, occurred_at_ist: String, level: String, component: String, code: String, message: String }`, `logs: Vec<RuntimeLogEntry>` on `DashboardState`, `MAX_RUNTIME_LOGS = 1_000`, and `MAX_LOG_PAGE_SIZE = 200`. Implement `sanitize_log_message(&str) -> String` as a one-line, 512-character transformation that redacts known token shapes/database URLs, strips HTTP query strings, removes control characters, and collapses whitespace.

- [ ] **Step 4: Implement recording and the endpoint**

Add `DashboardHandle::record_log` and `replace_logs`. Preserve logs across `DashboardHandle::replace`. Add `RuntimeLogQuery { limit, level, component }`, `RuntimeLogsResponse { items, total, limit }`, `runtime_logs_response`, and `GET /api/logs`. Invalid limits use the existing safe JSON `400` response. Results are newest-first and sanitized again at the response boundary. Recording emits the existing SSE event channel with event name `runtime_log`.

- [ ] **Step 5: Verify GREEN and commit**

Run `cargo test --quiet dashboard::tests::runtime_log`; expect all focused tests to pass. Commit `trading set up/src/dashboard.rs` with message `feat: add sanitized runtime logs API`.

### Task 2: Durable Neon event loading

**Files:**
- Modify: `trading set up/src/neon.rs`
- Test: `trading set up/src/neon.rs` unit tests

- [ ] **Step 1: Write and run a failing bounds test**

Add `service_event_limit_is_bounded`, asserting `None -> 100`, `Some(1) -> 1`, `Some(200) -> 200`, and errors for `Some(0)`/`Some(201)`. Run `cargo test --quiet neon::tests::service_event_limit`; expect missing-helper compilation failure.

- [ ] **Step 2: Implement durable event rows**

Add `ServiceEventRow` deriving `sqlx::FromRow` with `id`, UTC `occurred_at`, `service`, `level`, `code`, and `message`. Implement `normalize_service_event_limit` and `NeonStore::list_service_events(limit)` using a bound `LIMIT $1`, ordered by `id DESC`, capped at 200. Preserve the existing 1,000-row retention transaction.

- [ ] **Step 3: Verify and commit**

Run `cargo test --quiet neon::tests::service_event_limit`; expect pass. Commit `trading set up/src/neon.rs` with message `feat: load durable service events`.

### Task 3: Wire scheduler and runtime failures

**Files:**
- Modify: `trading set up/src/scheduler.rs`
- Modify: `trading set up/src/paper_runtime.rs`
- Test: focused unit tests in both files

- [ ] **Step 1: Write failing conversion/recording tests**

Add a scheduler test converting a `ServiceEventRow` to `RuntimeLogEntry` and asserting UTC/IST timestamps, mapping, and sanitizer use. Add a runtime test recording an operational error into a `DashboardHandle` and asserting one bounded sanitized log and a revision change. Run focused tests and verify missing-helper compilation failures.

- [ ] **Step 2: Load durable logs at daemon startup**

Connect to `config.database.url`, load the newest 200 events, and seed dashboard logs. If loading fails, create only an in-memory `ERROR/persistence/NEON_LOG_LOAD_FAILED` entry; never include the URL or raw provider response.

- [ ] **Step 3: Record operational failures**

Add a helper that records to `DashboardHandle` immediately and best-effort persists through `NeonStore::record_service_event`. Use stable scheduler codes `DASHBOARD_SERVER_STOPPED`, `YOUTUBE_DISCOVERY_FAILED`, `PAPER_SESSION_FAILED`, and `RAW_DATA_CLEANUP_FAILED`. Wire capture faults, STT errors, Gemini errors, market-feed closure/degradation, persistence checkpoint failures, Neon sync failures, and unsafe shutdown/ack failures in `paper_runtime.rs`. Do not log ordinary trade rejections, raw prompts/transcripts, or provider/broker bodies.

- [ ] **Step 4: Verify and commit**

Run the focused scheduler/paper-runtime tests, then `cargo test --quiet`; expect all tests to pass. Commit with message `feat: publish runtime failures to dashboard logs`.

### Task 4: Repository and deployment hygiene

**Files:**
- Modify: `.gitignore`
- Modify: `trading set up/.dockerignore`
- Modify: `render.yaml`
- Modify: `trading set up/README.md`

- [ ] **Step 1: Verify ignore boundaries**

Use `git check-ignore` to prove `.env`, `git and render.txt`, `api-keys.txt`, `token.txt`, `totp.txt`, `target`, and `data` are ignored. Add exact patterns for any failed path.

- [ ] **Step 2: Harden manifests and documentation**

Keep every secret key in `render.yaml` as `sync: false` with no value. Exclude `.env*`, data, target, logs, Git metadata, and credential filenames from Docker context. Document `/api/logs`, its public sanitized boundary, and Render operation.

- [ ] **Step 3: Run secret and release gates**

Scan staged candidates for token/database/MPIN/TOTP values; variable names are allowed, values are not. Run `cargo fmt -- --check`, `cargo test --quiet`, `node --check dashboard/app.js`, and `cargo build --release --locked`; require success.

- [ ] **Step 4: Commit only deployable files**

Stage `.gitignore`, `render.yaml`, `docs`, and `trading set up`. Review `git diff --cached --name-only`; root credential/transcript/tool files must not appear. Commit `build: prepare Render deployment`.

### Task 5: Private GitHub publication

**External resource:** GitHub repository `observer-paper-bot`

- [ ] **Step 1: Create or reuse the private repository**

Use credential line 1 only as a GitHub Authorization header. Create `observer-paper-bot` with `private: true`, or verify a matching existing repository belongs to the authenticated account and is private.

- [ ] **Step 2: Push without storing credentials**

Set an HTTPS remote containing no token. Authenticate one push with an ephemeral ask-pass/header mechanism. Verify `git remote -v` contains no credential and the remote tree excludes every ignored secret file.

- [ ] **Step 3: Verify the remote commit**

Query GitHub and assert the remote `main` SHA equals local `HEAD`.

### Task 6: Render service and environment

**External resource:** Render Docker web service `observer-paper-bot`

- [ ] **Step 1: Resolve workspace/service**

Use credential line 2 only as a Render Bearer header. Resolve the workspace, reuse a matching service or create a Singapore Free Docker web service rooted at `trading set up`, running the Dockerfile, with `/api/health`.

- [ ] **Step 2: Send secrets only to Render**

Read the external `.env` in memory and send only required values to Render: `DATABASE_URL`, three Gemini keys, three ElevenLabs keys, broker client ID/MPIN/TOTP, plus non-secret channel/model/schedule values. Never generate another env file, commit values, print response values, or send them to GitHub.

- [ ] **Step 3: Deploy and monitor**

Trigger the pushed `main` commit. Poll deploy state at bounded intervals until `live` or terminal failure. On failure, inspect sanitized Render logs, fix, verify, push, and retry.

### Task 7: Public URL and unattended verification

**External resources:** Render URL and Neon

- [ ] **Step 1: Verify HTTP APIs**

Require HTTPS `200` for `/`, `/api/health`, `/api/state`, `/api/history`, and `/api/logs?limit=20`; require safe JSON `400` for `/api/logs?limit=0`.

- [ ] **Step 2: Prove no public secret exposure**

Scan all public responses against known secret prefixes and every value from the external `.env`; require zero matches.

- [ ] **Step 3: Verify scheduler/persistence**

Confirm configured API slot health, Neon health, IST scheduler state, and process survival across repeated polls. Outside market hours, confirm capture/STT/Gemini/market-feed workers remain stopped.

- [ ] **Step 4: Verify diagnostics and restart**

Inspect recent Render error logs, restart/redeploy once, confirm `/api/logs` still returns durable events, and record the final `onrender.com` URL/service ID without credentials.

- [ ] **Step 5: Completion audit**

Compare every approved requirement with tests, GitHub state, Render state/logs, Neon state, and public HTTP evidence. Complete the goal only when every requirement is proven.
