# Render Deployment and Runtime Logs Design

## Goal

Deploy the existing paper-only Rust trading observer as a private GitHub-backed Render web service, expose its live dashboard, and add a bounded sanitized logs endpoint that makes unattended failures diagnosable without exposing credentials, prompts, transcripts, broker payloads, or signed media URLs.

## Deployment architecture

The existing multi-stage Dockerfile remains the production runtime. Render builds it from the `trading set up` root directory and runs `market-manager daemon`. The daemon binds the Axum dashboard to `0.0.0.0:$PORT`, uses `/api/health` for Render health checks, polls the configured YouTube channel on the IST schedule, and starts paper trading only for an active live stream during the configured discovery window.

The source repository is private. All credentials remain Render runtime environment variables marked `sync: false` in `render.yaml`. Local credential files, generated broker tokens, `.env` files, media, ticks, transcripts, and build output are excluded from Git and Docker contexts. Neon remains authoritative for durable paper state, capital continuity, closed trades, daily account state, rolling stream context, and service events. Render's local filesystem is treated as disposable scratch space.

## Logs endpoint

`GET /api/logs` returns newest-first structured JSON. Supported query parameters are `limit` (default 100, maximum 200), `level`, and `component`. Each entry contains only an event ID, UTC/IST timestamp, severity, component, stable code, and sanitized bounded message.

The dashboard state owns a bounded live buffer so new faults appear immediately. Neon stores the most recent 1,000 service events so errors remain visible after a Render restart. On daemon startup, the most recent events are loaded into the live buffer. Runtime error paths for scheduler discovery, capture, market feed, STT, Gemini, persistence, Neon checkpoints, and session shutdown record structured events.

Messages are normalized to one line, capped at 512 characters, and redacted before storage. Redaction removes API-key shapes, authorization tokens, database URLs, query strings, signed URLs, and control characters. Raw request or response bodies, prompts, transcripts, video paths, credential values, MPIN/TOTP values, and broker payloads are never accepted as log messages.

The endpoint is public because the dashboard is intentionally public. Its response includes operational metadata only and never raw user or provider data. Invalid query values return a safe `400` response. Database unavailability returns the in-memory events and reports persistence degradation through health state rather than exposing a database error.

## Data flow

1. A runtime component emits a severity, component, stable code, and safe message.
2. The sanitizer removes secret-like material and enforces bounds.
3. The dashboard handle appends the event to its bounded live buffer and notifies SSE clients.
4. When Neon is available, the event is also inserted into `service_events`; the existing 1,000-row retention bound is preserved.
5. `/api/logs` filters and returns the newest permitted entries.
6. The dashboard can fetch the endpoint for operator diagnostics without gaining access to secrets.

## Failure handling

Logging must never crash or block trading. In-memory recording is best effort and bounded. Neon event persistence uses the existing small connection pool and failures only degrade persistence health. Repeated provider failures remain summarized by stable codes and key-slot health; secret values are never included. Fatal startup failures are recorded when the dashboard is already available and remain visible through Render's native service logs when startup cannot reach the dashboard stage.

## Verification

Implementation follows test-first development. Unit tests cover redaction, truncation, ordering, retention, query filtering, invalid limits, and secret-free JSON. Integration tests cover `/api/logs`, SSE notification, Neon service-event loading, and fallback behavior. Deployment verification requires: full Rust tests, frontend syntax check, release build, live Render Blueprint/schema validation, successful private Git push, successful Render deploy, HTTP 200 from `/api/health`, valid dashboard HTML, valid `/api/logs` JSON, no secret-shaped text in public responses, Neon health, and a controlled non-trading scheduler smoke test.

## Operational boundary

The deployed service remains paper-only. It observes and simulates trades but does not place live brokerage orders. The public dashboard and logs endpoint are monitoring surfaces, not control interfaces.
