# Always-On Durable Dashboard Design

## Goal

Keep the paper-trading dashboard populated whenever the Render web service is reachable, even when livestream discovery and all market workers are idle or stopped. The dashboard must show ten strategy-wallet rows (five LLM Exit and five Moving SL), durable capital, trade history, last known positions and P&L, and an honest freshness state.

## Current failure

Daemon mode creates a scheduler-only `DashboardState` before livestream discovery. Every scheduler transition replaces the entire state with `waiting_state`, whose account, position, order, history, and equity collections are empty. Neon broker restoration and configured account creation currently happen only inside `paper_runtime::run_with_dashboard`, so the dashboard shows zero equity until a live stream starts.

## Selected approach

Use a durable dashboard preload while keeping market workers session-scoped. At daemon startup, reuse the existing Neon connection to load the `paper-primary` durable broker state. Validate it against the configured five accounts and broker settings using the same restoration path used by the live runtime. If no durable state exists, construct a fresh paper broker from `PAPER_ACCOUNTS`. Convert its two strategy shadows into the normal dashboard representation, producing ten wallet rows.

This is preferred over synthesizing rows directly from environment variables because synthesized rows cannot preserve capital or P&L. It is preferred over keeping the entire trading runtime alive because idle capture, STT, Gemini, and market-feed resources would waste Render capacity and complicate session boundaries.

## State ownership and data flow

1. `scheduler::run` loads and validates application configuration.
2. It opens Neon once for operational logs and durable paper-state preload.
3. A `paper_runtime` read-only preload helper restores `DurablePaperState`, or creates a new configured broker when no state exists, and converts the broker snapshot plus durable closed history into `DashboardState`.
4. The scheduler overlays its current session and component-health fields onto that desk state. It must not clear accounts, positions, pending orders, metrics, equity history, trade history, signals, or logs.
5. Waiting, holiday, channel-check, discovery-closed, worker-stopped, discovery-failed, and session-failed transitions update status/health while preserving the last durable desk data.
6. On livestream discovery, `paper_runtime::run_with_dashboard` remains authoritative. It restores the same Neon state, starts workers, and publishes tick-level snapshots.
7. When a session ends, the final live snapshot remains visible. Subsequent scheduler status overlays change freshness and worker status without erasing desk data.

No concurrent broker is kept alive while workers are running. The idle preload is a snapshot only, eliminating competing writers.

## Freshness semantics

The dashboard header can remain `Live sync` when its SSE connection is healthy, but market data must not be represented as current while workers are stopped. Session and market status show the scheduler state (`WAITING_FOR_LIVE`, `MARKET_CLOSED`, `DISCOVERY_CLOSED`, or `WORKERS_STOPPED`). Last tick time and tick age remain visible when available. Positions and unrealized P&L are explicitly last-known values until the market feed resumes; wallet cash, realized P&L, and closed history remain durable facts.

## Failure handling

- If Neon is reachable and durable state exists, restore it exactly.
- If Neon is reachable but no durable state exists, show ten rows derived from the configured five starting wallets.
- If Neon is unavailable, show ten configured starting-wallet rows, mark persistence `DEGRADED`, retain in-memory operational logs, and refuse to imply that historical capital was restored.
- If persisted state is corrupt or incompatible with the current account configuration, fail closed for durable restoration, expose a sanitized degraded status, and do not silently overwrite Neon.
- Public APIs and logs continue to exclude credentials, database URLs, transcripts, prompts, signed media URLs, and broker payloads.

## Persistence and capital continuity

Neon remains authoritative for broker state, account balances, closed trades, daily capital continuity, and rolling context. The preload path is read-only. Capital changes continue to be persisted only by the active paper runtime. The next market day therefore begins from the most recently committed equity rather than the original environment amount.

## Scope

Included:

- Always-visible ten account rows.
- Durable history, wallet balances, equity curve, last-known positions, orders, and P&L while workers are idle.
- Honest stale/idle health presentation.
- Preservation across Render restarts and scheduler state transitions.

Excluded:

- Running market feeds outside trading sessions.
- Recalculating unrealized P&L without fresh ticks.
- Live brokerage order placement.
- Frontend redesign or authentication changes.

## Verification

Tests must prove:

1. No durable state produces ten configured strategy-wallet rows totaling the expected capital per strategy.
2. Durable Neon state restores changed wallet equity and closed history before livestream discovery.
3. Every idle scheduler transition preserves accounts, history, positions, pending orders, metrics, signals, equity curve, and logs.
4. Worker-stopped state reports stale/idle market components without changing durable wallet values.
5. Neon failure yields configured fallback wallets plus degraded persistence health.
6. Incompatible durable state fails closed without overwriting it.
7. Live-runtime transition replaces the idle snapshot with tick-updated state and preserves capital continuity.
8. Full Rust tests, formatting, release build, Render deploy, `/api/health`, `/api/state`, `/api/history`, `/api/logs`, and public secret scans pass.

## Deployment

Commit and push the tested Rust changes to `0cry/observer-paper-bot`, trigger a manual Render deploy for the public-repository service, wait for `live`, and verify that `/api/state` contains ten accounts while the session remains `WORKERS_STOPPED` or another idle state.
