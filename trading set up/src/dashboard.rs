//! Local, read-only HTTP dashboard for the paper-trading engine.
//!
//! The engine owns the values in [`DashboardState`] and updates them through a
//! cheap cloneable [`DashboardHandle`].  HTTP clients receive snapshots over
//! JSON and revision notifications over SSE; a notification intentionally does
//! not contain the full state so high-frequency ticks do not get copied into
//! every connected client queue.

use std::{
    cmp::Ordering,
    convert::Infallible,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Component, Path},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Json as AxumJson, Path as AxumPath, Query, State},
    http::{HeaderValue, StatusCode, Uri, header},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, patch, post},
};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use chrono_tz::Asia::Kolkata;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    sync::{RwLock, broadcast},
};

use crate::{analysis::RuntimeKeyVault, cron_jobs, neon::NeonStore};

pub const DEFAULT_DASHBOARD_PORT: u16 = 8787;
pub const MAX_RECENT_SIGNALS: usize = 500;
pub const MAX_EQUITY_POINTS: usize = 10_000;
const EVENT_CHANNEL_CAPACITY: usize = 256;
const MAX_HISTORY_PAGE_SIZE: usize = 10_000;
const MAX_RUNTIME_LOGS: usize = 1_000;
const MAX_LOG_PAGE_SIZE: usize = 200;
const DEFAULT_LOG_PAGE_SIZE: usize = 100;
const MAX_LOG_MESSAGE_CHARS: usize = 512;

const INDEX_HTML: &[u8] = include_bytes!("../dashboard/index.html");
const STYLES_CSS: &[u8] = include_bytes!("../dashboard/styles.css");
const APP_JS: &[u8] = include_bytes!("../dashboard/app.js");

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct DashboardState {
    pub revision: u64,
    pub updated_at: String,
    pub session: SessionView,
    pub health: HealthView,
    pub metrics: MetricsView,
    pub accounts: Vec<AccountView>,
    pub positions: Vec<PositionView>,
    pub pending_orders: Vec<PendingOrderView>,
    pub signals: Vec<SignalView>,
    pub equity_curve: Vec<EquityPoint>,
    pub history: Vec<HistoryTrade>,
    pub logs: Vec<RuntimeLogEntry>,
}

impl DashboardState {
    pub fn empty() -> Self {
        Self {
            updated_at: now_rfc3339(),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(default)]
pub struct RuntimeLogEntry {
    pub event_id: i64,
    pub occurred_at: String,
    pub occurred_at_ist: String,
    pub level: String,
    pub component: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct SessionView {
    pub session_id: String,
    pub status: String,
    pub mode: String,
    pub started_at: String,
    pub stream_url: String,
    pub stream_title: String,
    pub market_status: String,
    pub clip_window_start: Option<String>,
    pub clip_window_end: Option<String>,
    pub clip_age_ms: Option<u64>,
    pub transcript_segments_ready: usize,
    pub transcription_latency_ms: Option<u64>,
    pub analysis_latency_ms: Option<u64>,
    /// Sanitized sparse-frame cadence only; raw images are never retained.
    pub visual_status: Option<String>,
    pub last_visual_at: Option<String>,
    pub last_prompt_at: Option<String>,
    pub last_tick_at: Option<String>,
    pub tick_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct HealthView {
    pub overall: String,
    #[serde(rename = "stream", alias = "stream_capture")]
    pub stream_capture: ComponentHealth,
    pub transcription: ComponentHealth,
    pub analysis: ComponentHealth,
    pub market_feed: ComponentHealth,
    pub persistence: ComponentHealth,
    /// Sanitized slot state only. Credential values and fragments are never exposed.
    pub api_keys: Vec<ApiKeyHealthView>,
    pub last_tick_at: Option<String>,
    pub tick_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(default)]
pub struct ApiKeyHealthView {
    pub provider: String,
    pub slot: usize,
    pub status: String,
    pub successes: u64,
    pub failures: u64,
    pub cooldown_remaining_ms: u64,
    pub last_failure: Option<String>,
    pub request_limit: Option<u64>,
    pub request_remaining: Option<u64>,
    pub request_reset_ms: Option<u64>,
    pub token_limit: Option<u64>,
    pub token_remaining: Option<u64>,
    pub token_reset_ms: Option<u64>,
    pub retry_after_ms: Option<u64>,
    pub observed_day_ist: Option<String>,
    pub observed_daily_requests: u64,
    pub observed_daily_input_tokens: u64,
    pub observed_daily_output_tokens: u64,
    pub observed_daily_total_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct ComponentHealth {
    pub status: String,
    pub message: String,
    pub last_success_at: Option<String>,
    pub latency_ms: Option<u64>,
    pub reconnects: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct MetricsView {
    pub starting_capital: f64,
    pub available_cash: f64,
    pub reserved_cash: f64,
    pub deployed_capital: f64,
    pub equity: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub total_pnl: f64,
    pub total_return_pct: f64,
    pub gross_profit: f64,
    pub gross_loss: f64,
    pub charges: f64,
    pub open_positions: usize,
    pub pending_orders: usize,
    pub trades_today: usize,
    pub closed_trades: usize,
    pub wins: usize,
    pub losses: usize,
    pub breakeven: usize,
    pub win_rate_pct: f64,
    pub profit_factor: Option<f64>,
    pub max_drawdown: f64,
    pub max_drawdown_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct AccountView {
    pub account_id: String,
    pub account_name: String,
    pub strategy: String,
    pub starting_capital: f64,
    pub available_cash: f64,
    pub reserved_cash: f64,
    pub deployed_capital: f64,
    pub equity: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub total_pnl: f64,
    pub return_pct: f64,
    pub open_positions: usize,
    pub pending_orders: usize,
    pub trades: usize,
    pub wins: usize,
    pub losses: usize,
    pub charges: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct PositionView {
    pub position_id: String,
    pub setup_id: String,
    pub account_id: String,
    pub account_name: String,
    pub strategy: String,
    pub symbol: String,
    pub underlying: String,
    pub expiry: String,
    pub strike: f64,
    pub option_type: String,
    pub side: String,
    pub quantity: u32,
    pub lots: u32,
    pub entry_price: f64,
    #[serde(rename = "ltp", alias = "current_ltp")]
    pub current_ltp: f64,
    pub streamer_sl: f64,
    pub effective_sl: f64,
    pub target_1: f64,
    pub target_2: Option<f64>,
    pub trailing_phase: String,
    pub opened_at: String,
    pub last_tick_at: Option<String>,
    pub tick_age_ms: Option<u64>,
    pub gross_pnl: f64,
    pub estimated_exit_charge: f64,
    #[serde(rename = "pnl", alias = "net_pnl")]
    pub net_pnl: f64,
    pub return_pct: f64,
    pub max_favorable_price: f64,
    pub max_adverse_price: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct PendingOrderView {
    pub order_id: String,
    pub setup_id: String,
    pub account_id: String,
    #[serde(rename = "account", alias = "account_name")]
    pub account_name: String,
    pub strategy: String,
    pub symbol: String,
    pub underlying: String,
    pub expiry: String,
    pub strike: f64,
    pub option_type: String,
    pub side: String,
    pub quantity: u32,
    pub lots: u32,
    #[serde(rename = "entry_cap", alias = "requested_entry")]
    pub requested_entry: f64,
    pub maximum_fill_price: f64,
    pub entry_buffer: f64,
    #[serde(rename = "ltp", alias = "current_ltp")]
    pub current_ltp: Option<f64>,
    pub reserved_cash: f64,
    pub status: String,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub last_tick_at: Option<String>,
    pub rejection_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct SignalView {
    pub signal_id: String,
    pub setup_id: String,
    #[serde(rename = "timestamp", alias = "received_at")]
    pub received_at: String,
    pub evidence_start: Option<String>,
    pub evidence_end: Option<String>,
    pub action: String,
    pub accepted: bool,
    pub symbol: String,
    pub underlying: String,
    pub expiry: String,
    pub strike: Option<f64>,
    pub option_type: String,
    pub side: String,
    pub entry: Option<f64>,
    pub stop_loss: Option<f64>,
    pub target_1: Option<f64>,
    pub target_2: Option<f64>,
    pub market_bias: String,
    pub source_age_ms: Option<u64>,
    pub freshness: String,
    pub transcript_excerpt: String,
    #[serde(rename = "reason", alias = "decision_reason")]
    pub decision_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct EquityPoint {
    pub timestamp: String,
    pub account_id: Option<String>,
    pub strategy: Option<String>,
    pub equity: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(default)]
pub struct HistoryTrade {
    pub trade_id: String,
    pub setup_id: String,
    pub account_id: String,
    pub account_name: String,
    pub strategy: String,
    pub symbol: String,
    pub underlying: String,
    pub expiry: String,
    pub strike: f64,
    pub option_type: String,
    pub side: String,
    pub status: String,
    pub quantity: u32,
    pub lots: u32,
    pub entry_price: f64,
    pub exit_price: f64,
    pub streamer_sl: f64,
    pub final_sl: f64,
    pub stop_loss: f64,
    pub target_1: f64,
    pub target_2: Option<f64>,
    pub opened_at: String,
    pub closed_at: String,
    #[serde(rename = "duration_seconds", alias = "hold_seconds")]
    pub hold_seconds: u64,
    pub exit_reason: String,
    pub exit_phase: String,
    pub gross_pnl: f64,
    pub charges: f64,
    pub net_pnl: f64,
    pub return_pct: f64,
    pub max_favorable_price: f64,
    pub max_adverse_price: f64,
    pub notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DashboardEvent {
    pub event: String,
    pub revision: u64,
    pub at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_id: Option<String>,
}

#[derive(Clone)]
pub struct DashboardHandle {
    state: Arc<RwLock<DashboardState>>,
    events: broadcast::Sender<DashboardEvent>,
    server_started: Instant,
    sse_clients: Arc<AtomicUsize>,
    openai_vault: Arc<RuntimeKeyVault>,
    cron_store: Option<NeonStore>,
}

impl DashboardHandle {
    pub fn new(initial_state: DashboardState) -> Self {
        let mut initial_state = initial_state;
        if initial_state.updated_at.is_empty() {
            initial_state.updated_at = now_rfc3339();
        }
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            state: Arc::new(RwLock::new(initial_state)),
            events,
            server_started: Instant::now(),
            sse_clients: Arc::new(AtomicUsize::new(0)),
            openai_vault: Arc::new(RuntimeKeyVault::empty()),
            cron_store: None,
        }
    }

    pub fn empty() -> Self {
        Self::new(DashboardState::empty())
    }

    /// Returns the shared state for integrations that need to perform a batch
    /// read. Prefer [`Self::update`] for mutations so SSE clients are notified.
    pub fn shared_state(&self) -> Arc<RwLock<DashboardState>> {
        Arc::clone(&self.state)
    }

    /// The only application owner of runtime-supplied OpenAI keys. The vault
    /// is process-local and exposes no method to read raw credentials.
    pub fn openai_vault(&self) -> Arc<RuntimeKeyVault> {
        Arc::clone(&self.openai_vault)
    }

    pub fn with_cron_store(mut self, store: Option<NeonStore>) -> Self {
        self.cron_store = store;
        self
    }

    fn cron_store(&self) -> Result<NeonStore, StatusCode> {
        self.cron_store
            .clone()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }

    pub async fn snapshot(&self) -> DashboardState {
        self.state.read().await.clone()
    }

    /// Applies one atomic dashboard mutation and publishes its new revision.
    pub async fn update<F>(
        &self,
        event: impl Into<String>,
        entity_id: Option<String>,
        mutate: F,
    ) -> u64
    where
        F: FnOnce(&mut DashboardState),
    {
        let event = event.into();
        let at = now_rfc3339();
        let revision = {
            let mut state = self.state.write().await;
            mutate(&mut state);
            state.revision = state.revision.saturating_add(1);
            state.updated_at.clone_from(&at);
            state.revision
        };
        let _ = self.events.send(DashboardEvent {
            event,
            revision,
            at,
            entity_id,
        });
        revision
    }

    pub async fn replace(&self, mut replacement: DashboardState) -> u64 {
        self.update("snapshot_replaced", None, move |state| {
            if replacement.logs.is_empty() {
                replacement.logs.clone_from(&state.logs);
            }
            replacement.revision = state.revision;
            *state = replacement;
        })
        .await
    }

    pub async fn replace_logs(&self, logs: Vec<RuntimeLogEntry>) -> u64 {
        self.update("runtime_logs_replaced", None, move |state| {
            state.logs = normalized_runtime_logs(logs);
        })
        .await
    }

    pub async fn record_log(&self, entry: RuntimeLogEntry) -> u64 {
        let entry = normalize_runtime_log_entry(entry);
        let entity_id = Some(entry.code.clone());
        self.update("runtime_log", entity_id, move |state| {
            state.logs.retain(|value| value.event_id != entry.event_id);
            state.logs.push(entry);
            state.logs.sort_by(runtime_log_order_oldest_first);
            trim_oldest(&mut state.logs, MAX_RUNTIME_LOGS);
        })
        .await
    }

    pub async fn set_session(&self, session: SessionView) -> u64 {
        self.update("session", None, move |state| state.session = session)
            .await
    }

    pub async fn set_health(&self, health: HealthView) -> u64 {
        self.update("health", None, move |state| state.health = health)
            .await
    }

    pub async fn set_metrics(&self, metrics: MetricsView) -> u64 {
        self.update("metrics", None, move |state| state.metrics = metrics)
            .await
    }

    pub async fn upsert_account(&self, account: AccountView) -> u64 {
        let entity_id = account.account_id.clone();
        self.update("account", Some(entity_id), move |state| {
            upsert_by(&mut state.accounts, account, |value| &value.account_id);
        })
        .await
    }

    pub async fn upsert_position(&self, position: PositionView) -> u64 {
        let entity_id = position.position_id.clone();
        self.update("position", Some(entity_id), move |state| {
            upsert_by(&mut state.positions, position, |value| &value.position_id);
        })
        .await
    }

    pub async fn remove_position(&self, position_id: &str) -> u64 {
        let position_id = position_id.to_owned();
        self.update("position_closed", Some(position_id.clone()), move |state| {
            state
                .positions
                .retain(|value| value.position_id != position_id);
        })
        .await
    }

    pub async fn upsert_pending_order(&self, order: PendingOrderView) -> u64 {
        let entity_id = order.order_id.clone();
        self.update("pending_order", Some(entity_id), move |state| {
            upsert_by(&mut state.pending_orders, order, |value| &value.order_id);
        })
        .await
    }

    pub async fn remove_pending_order(&self, order_id: &str) -> u64 {
        let order_id = order_id.to_owned();
        self.update(
            "pending_order_removed",
            Some(order_id.clone()),
            move |state| {
                state
                    .pending_orders
                    .retain(|value| value.order_id != order_id);
            },
        )
        .await
    }

    pub async fn record_signal(&self, signal: SignalView) -> u64 {
        let entity_id = signal.signal_id.clone();
        self.update("signal", Some(entity_id), move |state| {
            state.signals.push(signal);
            trim_oldest(&mut state.signals, MAX_RECENT_SIGNALS);
        })
        .await
    }

    pub async fn record_equity_point(&self, point: EquityPoint) -> u64 {
        self.update("equity", None, move |state| {
            state.equity_curve.push(point);
            trim_oldest(&mut state.equity_curve, MAX_EQUITY_POINTS);
        })
        .await
    }

    pub async fn record_trade(&self, trade: HistoryTrade) -> u64 {
        let entity_id = trade.trade_id.clone();
        self.update("trade_closed", Some(entity_id), move |state| {
            upsert_by(&mut state.history, trade, |value| &value.trade_id);
        })
        .await
    }

    /// Publishes a lightweight event without changing the dashboard revision.
    pub async fn notify(&self, event: impl Into<String>, entity_id: Option<String>) {
        let revision = self.state.read().await.revision;
        let _ = self.events.send(DashboardEvent {
            event: event.into(),
            revision,
            at: now_rfc3339(),
            entity_id,
        });
    }
}

impl Default for DashboardHandle {
    fn default() -> Self {
        Self::empty()
    }
}

fn upsert_by<T, F>(items: &mut Vec<T>, replacement: T, key: F)
where
    F: Fn(&T) -> &str,
{
    let replacement_key = key(&replacement).to_owned();
    if let Some(existing) = items.iter_mut().find(|item| key(item) == replacement_key) {
        *existing = replacement;
    } else {
        items.push(replacement);
    }
}

fn trim_oldest<T>(items: &mut Vec<T>, maximum: usize) {
    if items.len() > maximum {
        items.drain(..items.len() - maximum);
    }
}

pub fn default_bind_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_DASHBOARD_PORT)
}

pub fn router(handle: DashboardHandle) -> Router {
    Router::new()
        .route("/", get(static_index))
        .route("/api/health", get(api_health))
        .route("/api/state", get(api_state))
        .route("/api/logs", get(api_logs))
        .route("/api/history", get(api_history))
        .route("/api/events", get(api_events))
        .route("/api/export.csv", get(api_export_csv))
        .route("/api/llm/keys", post(api_add_llm_keys))
        .route("/api/llm/keys/clear", post(api_clear_llm_keys))
        .route("/api/llm/keys/health", get(api_llm_key_health))
        .route(
            "/api/cron/jobs",
            get(api_list_cron_jobs).post(api_create_cron_job),
        )
        .route(
            "/api/cron/jobs/{id}",
            patch(api_set_cron_job).delete(api_delete_cron_job),
        )
        .route("/api/cron/jobs/{id}/runs", get(api_list_cron_runs))
        .fallback(get(static_asset))
        // The only credential-bearing request is a maximum of three runtime
        // keys; a small body cap prevents accidental bulk submission.
        .layer(DefaultBodyLimit::max(4_096))
        .with_state(handle)
}

const MAX_RUNTIME_KEY_SLOTS: usize = 3;

#[derive(Debug, Deserialize)]
struct AddLlmKeysRequest {
    keys: Vec<String>,
}

#[derive(Debug, Serialize)]
struct LlmKeysWriteResponse {
    accepted_slots: usize,
    loaded_slots: usize,
    state: crate::analysis::VaultState,
}

async fn api_add_llm_keys(
    State(handle): State<DashboardHandle>,
    AxumJson(request): AxumJson<AddLlmKeysRequest>,
) -> Result<Json<LlmKeysWriteResponse>, (StatusCode, Json<serde_json::Value>)> {
    if request.keys.is_empty() || request.keys.len() > MAX_RUNTIME_KEY_SLOTS {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"submit between one and three keys"})),
        ));
    }
    let vault = handle.openai_vault();
    let accepted_slots = vault.add(request.keys).await.map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"submitted key material was rejected"})),
        )
    })?;
    let health = vault.health().await;
    let loaded_slots = health.loaded_slots;
    handle
        .update("llm_keys_changed", None, |state| {
            state.health.analysis = ComponentHealth {
                status: "READY".to_owned(),
                message: format!("{loaded_slots} runtime OpenAI key slot(s) loaded"),
                ..ComponentHealth::default()
            };
        })
        .await;
    Ok(Json(LlmKeysWriteResponse {
        accepted_slots,
        loaded_slots: health.loaded_slots,
        state: health.state,
    }))
}

async fn api_clear_llm_keys(State(handle): State<DashboardHandle>) -> Json<LlmKeysWriteResponse> {
    let vault = handle.openai_vault();
    vault.clear().await;
    let health = vault.health().await;
    handle
        .update("llm_keys_cleared", None, |state| {
            state.health.analysis = ComponentHealth {
                status: "KEYS_REQUIRED".to_owned(),
                message: "add up to three runtime OpenAI keys in the dashboard".to_owned(),
                ..ComponentHealth::default()
            };
        })
        .await;
    Json(LlmKeysWriteResponse {
        accepted_slots: 0,
        loaded_slots: health.loaded_slots,
        state: health.state,
    })
}

async fn api_llm_key_health(
    State(handle): State<DashboardHandle>,
) -> Json<crate::analysis::VaultHealth> {
    Json(handle.openai_vault().health().await)
}

#[derive(Debug, Deserialize)]
struct SetCronJobRequest {
    enabled: bool,
}

async fn api_list_cron_jobs(
    State(handle): State<DashboardHandle>,
) -> Result<Json<Vec<cron_jobs::CronJobView>>, StatusCode> {
    let rows = handle
        .cron_store()?
        .list_cron_jobs()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(
        rows.into_iter().map(cron_jobs::row_to_public).collect(),
    ))
}

async fn api_create_cron_job(
    State(handle): State<DashboardHandle>,
    AxumJson(request): AxumJson<cron_jobs::CreateCronJob>,
) -> Result<(StatusCode, Json<cron_jobs::CronJobView>), (StatusCode, Json<serde_json::Value>)> {
    let store = handle.cron_store().map_err(|status| {
        (
            status,
            Json(serde_json::json!({"error":"cron storage unavailable"})),
        )
    })?;
    let job = cron_jobs::validate_create(request, Utc::now()).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"invalid cron job"})),
        )
    })?;
    let row = store.create_cron_job(&job).await.map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"cron storage unavailable"})),
        )
    })?;
    handle
        .notify("cron_job_created", Some(row.id.to_string()))
        .await;
    Ok((StatusCode::CREATED, Json(cron_jobs::row_to_public(row))))
}

async fn api_set_cron_job(
    State(handle): State<DashboardHandle>,
    AxumPath(id): AxumPath<i64>,
    AxumJson(request): AxumJson<SetCronJobRequest>,
) -> Result<StatusCode, StatusCode> {
    let updated = handle
        .cron_store()?
        .set_cron_job_enabled(id, request.enabled)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    if !updated {
        return Err(StatusCode::NOT_FOUND);
    }
    handle
        .notify("cron_job_updated", Some(id.to_string()))
        .await;
    Ok(StatusCode::NO_CONTENT)
}

async fn api_delete_cron_job(
    State(handle): State<DashboardHandle>,
    AxumPath(id): AxumPath<i64>,
) -> Result<StatusCode, StatusCode> {
    let deleted = handle
        .cron_store()?
        .delete_cron_job(id)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    if !deleted {
        return Err(StatusCode::NOT_FOUND);
    }
    handle
        .notify("cron_job_deleted", Some(id.to_string()))
        .await;
    Ok(StatusCode::NO_CONTENT)
}

async fn api_list_cron_runs(
    State(handle): State<DashboardHandle>,
    AxumPath(id): AxumPath<i64>,
) -> Result<Json<Vec<cron_jobs::CronRunView>>, StatusCode> {
    let rows = handle
        .cron_store()?
        .list_cron_job_runs(id, 50)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(
        rows.into_iter().map(cron_jobs::run_to_public).collect(),
    ))
}

pub async fn serve(handle: DashboardHandle, bind: SocketAddr) -> io::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    axum::serve(listener, router(handle)).await
}

pub async fn serve_default(handle: DashboardHandle) -> io::Result<()> {
    serve(handle, default_bind_address()).await
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub status: String,
    pub revision: u64,
    pub server_time: String,
    pub dashboard_uptime_seconds: u64,
    pub connected_event_clients: usize,
    pub last_state_update_at: String,
    pub last_tick_at: Option<String>,
    pub tick_age_ms: Option<u64>,
    pub components: HealthView,
}

async fn api_health(State(handle): State<DashboardHandle>) -> Json<HealthResponse> {
    let state = handle.state.read().await;
    let status = if state.health.overall.is_empty() {
        "starting".to_owned()
    } else {
        state.health.overall.clone()
    };
    let ok = matches!(
        status.to_ascii_lowercase().as_str(),
        "ok" | "healthy" | "running"
    );
    Json(HealthResponse {
        ok,
        status,
        revision: state.revision,
        server_time: now_rfc3339(),
        dashboard_uptime_seconds: handle.server_started.elapsed().as_secs(),
        connected_event_clients: handle.sse_clients.load(AtomicOrdering::Relaxed),
        last_state_update_at: state.updated_at.clone(),
        last_tick_at: state.health.last_tick_at.clone(),
        tick_age_ms: state.health.tick_age_ms,
        components: state.health.clone(),
    })
}

async fn api_state(State(handle): State<DashboardHandle>) -> Json<DashboardState> {
    Json(handle.snapshot().await)
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct RuntimeLogQuery {
    pub limit: Option<usize>,
    pub level: Option<String>,
    pub component: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RuntimeLogsResponse {
    pub items: Vec<RuntimeLogEntry>,
    pub total: usize,
    pub limit: usize,
}

async fn api_logs(
    State(handle): State<DashboardHandle>,
    Query(query): Query<RuntimeLogQuery>,
) -> Result<Json<RuntimeLogsResponse>, ApiError> {
    let state = handle.state.read().await;
    runtime_logs_response(&state.logs, &query).map(Json)
}

fn runtime_logs_response(
    logs: &[RuntimeLogEntry],
    query: &RuntimeLogQuery,
) -> Result<RuntimeLogsResponse, ApiError> {
    let limit = query.limit.unwrap_or(DEFAULT_LOG_PAGE_SIZE);
    if limit == 0 || limit > MAX_LOG_PAGE_SIZE {
        return Err(ApiError::bad_request(format!(
            "limit must be between 1 and {MAX_LOG_PAGE_SIZE}"
        )));
    }
    let level = query
        .level
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty());
    let component = query
        .component
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let mut items: Vec<_> = logs
        .iter()
        .filter(|entry| {
            level.is_none_or(|value| entry.level.eq_ignore_ascii_case(value))
                && component.is_none_or(|value| entry.component.eq_ignore_ascii_case(value))
        })
        .cloned()
        .map(normalize_runtime_log_entry)
        .collect();
    items.sort_by(|left, right| runtime_log_order_oldest_first(right, left));
    let total = items.len();
    items.truncate(limit);
    Ok(RuntimeLogsResponse {
        items,
        total,
        limit,
    })
}

fn normalized_runtime_logs(logs: Vec<RuntimeLogEntry>) -> Vec<RuntimeLogEntry> {
    let mut logs: Vec<_> = logs.into_iter().map(normalize_runtime_log_entry).collect();
    logs.sort_by(runtime_log_order_oldest_first);
    if logs.len() > MAX_RUNTIME_LOGS {
        logs.drain(..logs.len() - MAX_RUNTIME_LOGS);
    }
    logs
}

fn normalize_runtime_log_entry(mut entry: RuntimeLogEntry) -> RuntimeLogEntry {
    if entry.occurred_at.trim().is_empty() {
        entry.occurred_at = now_rfc3339();
    }
    if entry.occurred_at_ist.trim().is_empty() {
        entry.occurred_at_ist = DateTime::parse_from_rfc3339(&entry.occurred_at)
            .map(|value| value.with_timezone(&Kolkata).to_rfc3339())
            .unwrap_or_else(|_| Utc::now().with_timezone(&Kolkata).to_rfc3339());
    }
    entry.level = bounded_log_field(&entry.level, 16, "INFO");
    entry.component = bounded_log_field(&entry.component, 48, "runtime");
    entry.code = bounded_log_field(&entry.code, 64, "RUNTIME_EVENT");
    entry.message = sanitize_log_message(&entry.message);
    entry
}

fn bounded_log_field(value: &str, maximum: usize, fallback: &str) -> String {
    let value: String = value
        .chars()
        .filter(|character| !character.is_control())
        .take(maximum)
        .collect();
    let value = value.trim();
    if value.is_empty() {
        fallback.to_owned()
    } else {
        value.to_owned()
    }
}

fn runtime_log_order_oldest_first(left: &RuntimeLogEntry, right: &RuntimeLogEntry) -> Ordering {
    left.event_id
        .cmp(&right.event_id)
        .then_with(|| left.occurred_at.cmp(&right.occurred_at))
}

pub fn sanitize_log_message(raw: &str) -> String {
    let flattened: String = raw
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    let mut output = String::new();
    let mut authorization_words_to_skip = 0usize;
    for word in flattened.split_whitespace() {
        if authorization_words_to_skip > 0 {
            authorization_words_to_skip -= 1;
            continue;
        }
        let lower = word.to_ascii_lowercase();
        let safe = if lower.starts_with("authorization:") || lower.starts_with("authorization=") {
            authorization_words_to_skip = 2;
            "[REDACTED]"
        } else if contains_secret_shape(&lower) {
            "[REDACTED]"
        } else if lower.starts_with("http://") || lower.starts_with("https://") {
            word.split_once('?').map_or(word, |(base, _)| base)
        } else {
            word
        };
        if !output.is_empty() {
            output.push(' ');
        }
        output.push_str(safe);
        if output.chars().count() >= MAX_LOG_MESSAGE_CHARS {
            break;
        }
    }
    output.chars().take(MAX_LOG_MESSAGE_CHARS).collect()
}

fn contains_secret_shape(lower: &str) -> bool {
    lower.contains("github_pat_")
        || lower.contains("ghp_")
        || lower.contains("gho_")
        || lower.contains("rnd_")
        || lower.contains("aiza")
        || lower.contains("sk-")
        || lower.contains("sk_")
        || lower.contains("postgresql://")
        || lower.contains("postgres://")
        || lower.contains("database_url=")
        || lower.contains("authorization=")
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct HistoryQuery {
    #[serde(alias = "q")]
    pub search: Option<String>,
    pub account: Option<String>,
    #[serde(alias = "mode")]
    pub strategy: Option<String>,
    pub underlying: Option<String>,
    pub symbol: Option<String>,
    pub option_type: Option<String>,
    pub side: Option<String>,
    pub exit_reason: Option<String>,
    #[serde(alias = "status")]
    pub outcome: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub min_pnl: Option<f64>,
    pub max_pnl: Option<f64>,
    pub sort: Option<String>,
    pub order: Option<String>,
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct HistorySummary {
    pub trades: usize,
    pub wins: usize,
    pub losses: usize,
    pub breakeven: usize,
    pub win_rate_pct: f64,
    pub gross_pnl: f64,
    pub charges: f64,
    pub net_pnl: f64,
    pub average_pnl: f64,
    pub average_return_pct: f64,
    pub average_hold_seconds: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct HistoryResponse {
    pub items: Vec<HistoryTrade>,
    pub page: usize,
    pub page_size: usize,
    pub total: usize,
    pub total_pages: usize,
    pub summary: HistorySummary,
}

async fn api_history(
    State(handle): State<DashboardHandle>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<HistoryResponse>, ApiError> {
    let state = handle.state.read().await;
    history_response(&state.history, &query).map(Json)
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

fn history_response(
    history: &[HistoryTrade],
    query: &HistoryQuery,
) -> Result<HistoryResponse, ApiError> {
    let page = query.page.unwrap_or(1);
    if page == 0 {
        return Err(ApiError::bad_request("page must be at least 1"));
    }
    let page_size = query.page_size.unwrap_or(50);
    if page_size == 0 || page_size > MAX_HISTORY_PAGE_SIZE {
        return Err(ApiError::bad_request(format!(
            "page_size must be between 1 and {MAX_HISTORY_PAGE_SIZE}"
        )));
    }

    let filtered = filtered_history(history, query)?;
    let summary = summarize_history(&filtered);
    let total = filtered.len();
    let total_pages = total.div_ceil(page_size);
    let start = page.saturating_sub(1).saturating_mul(page_size).min(total);
    let end = start.saturating_add(page_size).min(total);

    Ok(HistoryResponse {
        items: filtered[start..end].to_vec(),
        page,
        page_size,
        total,
        total_pages,
        summary,
    })
}

fn filtered_history(
    history: &[HistoryTrade],
    query: &HistoryQuery,
) -> Result<Vec<HistoryTrade>, ApiError> {
    validate_outcome(query.outcome.as_deref())?;
    let from = query
        .from
        .as_deref()
        .map(|value| parse_filter_time(value, false))
        .transpose()?;
    let to = query
        .to
        .as_deref()
        .map(|value| parse_filter_time(value, true))
        .transpose()?;
    if from.zip(to).is_some_and(|(from, to)| from > to) {
        return Err(ApiError::bad_request("from must not be later than to"));
    }

    let mut filtered = history
        .iter()
        .filter(|trade| {
            text_filter(
                &query.account,
                &[&trade.account_id, &trade.account_name],
                false,
            )
        })
        .filter(|trade| strategy_filter(&query.strategy, &trade.strategy))
        .filter(|trade| text_filter(&query.underlying, &[&trade.underlying], false))
        .filter(|trade| text_filter(&query.symbol, &[&trade.symbol], true))
        .filter(|trade| text_filter(&query.option_type, &[&trade.option_type], false))
        .filter(|trade| text_filter(&query.side, &[&trade.side], false))
        .filter(|trade| text_filter(&query.exit_reason, &[&trade.exit_reason], true))
        .filter(|trade| outcome_matches(trade, query.outcome.as_deref()))
        .filter(|trade| query.min_pnl.is_none_or(|value| trade.net_pnl >= value))
        .filter(|trade| query.max_pnl.is_none_or(|value| trade.net_pnl <= value))
        .filter(|trade| search_matches(trade, query.search.as_deref()))
        .filter(|trade| {
            if from.is_none() && to.is_none() {
                return true;
            }
            trade_time_millis(trade).is_some_and(|timestamp| {
                from.is_none_or(|minimum| timestamp >= minimum)
                    && to.is_none_or(|maximum| timestamp <= maximum)
            })
        })
        .cloned()
        .collect::<Vec<_>>();

    sort_history(&mut filtered, query.sort.as_deref(), query.order.as_deref())?;
    Ok(filtered)
}

fn validate_outcome(outcome: Option<&str>) -> Result<(), ApiError> {
    if outcome.is_some_and(|value| {
        !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "win"
                | "won"
                | "loss"
                | "lost"
                | "breakeven"
                | "open"
                | "cancelled"
                | "canceled"
                | "closed"
                | "complete"
                | "completed"
        )
    }) {
        return Err(ApiError::bad_request(
            "status must be won, lost, breakeven, open, cancelled, or closed",
        ));
    }
    Ok(())
}

fn text_filter(filter: &Option<String>, candidates: &[&str], contains: bool) -> bool {
    let Some(filter) = filter
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return true;
    };
    candidates.iter().any(|candidate| {
        if contains {
            candidate.to_lowercase().contains(&filter.to_lowercase())
        } else {
            candidate.eq_ignore_ascii_case(filter)
        }
    })
}

fn strategy_filter(filter: &Option<String>, strategy: &str) -> bool {
    let Some(filter) = filter
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return true;
    };
    canonical_strategy(filter) == canonical_strategy(strategy)
}

fn canonical_strategy(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase().replace([' ', '-'], "_");
    match normalized.as_str() {
        "llm" | "analysis" | "ai" | "llmexit" | "llm_exit" => "llm_exit".to_owned(),
        "moving" | "trail" | "trailing" | "movingsl" | "moving_sl" | "rule" => {
            "moving_sl".to_owned()
        }
        _ => normalized,
    }
}

fn outcome_matches(trade: &HistoryTrade, outcome: Option<&str>) -> bool {
    match outcome.map(|value| value.trim().to_ascii_lowercase()) {
        None => true,
        Some(value) if matches!(value.as_str(), "win" | "won") => trade.net_pnl > 0.0,
        Some(value) if matches!(value.as_str(), "loss" | "lost") => trade.net_pnl < 0.0,
        Some(value) if value == "breakeven" => trade.net_pnl.abs() < f64::EPSILON,
        Some(value) if matches!(value.as_str(), "closed" | "complete" | "completed") => !matches!(
            trade.status.trim().to_ascii_lowercase().as_str(),
            "open" | "cancelled" | "canceled"
        ),
        Some(value) if matches!(value.as_str(), "cancelled" | "canceled") => {
            matches!(
                trade.status.trim().to_ascii_lowercase().as_str(),
                "cancelled" | "canceled"
            )
        }
        Some(value) if value == "open" => trade.status.eq_ignore_ascii_case("open"),
        Some(_) => false,
    }
}

fn search_matches(trade: &HistoryTrade, search: Option<&str>) -> bool {
    let Some(needle) = search.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    let needle = needle.to_lowercase();
    [
        &trade.trade_id,
        &trade.setup_id,
        &trade.account_id,
        &trade.account_name,
        &trade.strategy,
        &trade.symbol,
        &trade.underlying,
        &trade.expiry,
        &trade.option_type,
        &trade.side,
        &trade.exit_reason,
        &trade.exit_phase,
        &trade.notes,
    ]
    .iter()
    .any(|value| value.to_lowercase().contains(&needle))
}

fn trade_time_millis(trade: &HistoryTrade) -> Option<i64> {
    parse_stored_time(&trade.closed_at).or_else(|| parse_stored_time(&trade.opened_at))
}

fn parse_stored_time(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.timestamp_millis())
        .ok()
}

fn parse_filter_time(value: &str, end_of_day: bool) -> Result<i64, ApiError> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.timestamp_millis());
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let next_date = date
            .succ_opt()
            .ok_or_else(|| ApiError::bad_request(format!("invalid date '{value}'")))?;
        let local = if end_of_day {
            Kolkata
                .from_local_datetime(&next_date.and_hms_opt(0, 0, 0).unwrap())
                .single()
                .map(|timestamp| timestamp.timestamp_millis() - 1)
        } else {
            Kolkata
                .from_local_datetime(&date.and_hms_opt(0, 0, 0).unwrap())
                .single()
                .map(|timestamp| timestamp.timestamp_millis())
        };
        return local.ok_or_else(|| ApiError::bad_request(format!("invalid date '{value}'")));
    }
    Err(ApiError::bad_request(format!(
        "invalid date/time '{value}'; use YYYY-MM-DD or RFC3339"
    )))
}

fn sort_history(
    history: &mut [HistoryTrade],
    sort: Option<&str>,
    order: Option<&str>,
) -> Result<(), ApiError> {
    let key = sort.unwrap_or("closed_at").trim().to_ascii_lowercase();
    let descending = match order.unwrap_or("desc").trim().to_ascii_lowercase().as_str() {
        "asc" => false,
        "desc" => true,
        _ => return Err(ApiError::bad_request("order must be asc or desc")),
    };

    let comparator = |left: &HistoryTrade, right: &HistoryTrade| -> Ordering {
        let primary = match key.as_str() {
            "closed_at" => trade_time_millis(left).cmp(&trade_time_millis(right)),
            "opened_at" => {
                parse_stored_time(&left.opened_at).cmp(&parse_stored_time(&right.opened_at))
            }
            "net_pnl" | "realized_pnl" => left.net_pnl.total_cmp(&right.net_pnl),
            "gross_pnl" => left.gross_pnl.total_cmp(&right.gross_pnl),
            "entry_price" => left.entry_price.total_cmp(&right.entry_price),
            "exit_price" => left.exit_price.total_cmp(&right.exit_price),
            "return_pct" => left.return_pct.total_cmp(&right.return_pct),
            "hold_seconds" | "duration_seconds" => left.hold_seconds.cmp(&right.hold_seconds),
            "quantity" => left.quantity.cmp(&right.quantity),
            "symbol" | "contract" => left.symbol.to_lowercase().cmp(&right.symbol.to_lowercase()),
            "account" | "account_name" => left
                .account_name
                .to_lowercase()
                .cmp(&right.account_name.to_lowercase()),
            "strategy" | "mode" => left
                .strategy
                .to_lowercase()
                .cmp(&right.strategy.to_lowercase()),
            "exit_reason" => left
                .exit_reason
                .to_lowercase()
                .cmp(&right.exit_reason.to_lowercase()),
            _ => Ordering::Equal,
        };
        let primary = if descending {
            primary.reverse()
        } else {
            primary
        };
        primary.then_with(|| left.trade_id.cmp(&right.trade_id))
    };

    if !matches!(
        key.as_str(),
        "closed_at"
            | "opened_at"
            | "net_pnl"
            | "realized_pnl"
            | "gross_pnl"
            | "entry_price"
            | "exit_price"
            | "return_pct"
            | "hold_seconds"
            | "duration_seconds"
            | "quantity"
            | "symbol"
            | "contract"
            | "account"
            | "account_name"
            | "strategy"
            | "mode"
            | "exit_reason"
    ) {
        return Err(ApiError::bad_request(format!(
            "unsupported sort field '{key}'"
        )));
    }
    history.sort_by(comparator);
    Ok(())
}

fn summarize_history(history: &[HistoryTrade]) -> HistorySummary {
    let wins = history.iter().filter(|trade| trade.net_pnl > 0.0).count();
    let losses = history.iter().filter(|trade| trade.net_pnl < 0.0).count();
    let breakeven = history.len().saturating_sub(wins + losses);
    let gross_pnl = history.iter().map(|trade| trade.gross_pnl).sum();
    let charges = history.iter().map(|trade| trade.charges).sum();
    let net_pnl = history.iter().map(|trade| trade.net_pnl).sum();
    let return_sum: f64 = history.iter().map(|trade| trade.return_pct).sum();
    let hold_sum: u64 = history.iter().map(|trade| trade.hold_seconds).sum();
    let denominator = history.len() as f64;
    HistorySummary {
        trades: history.len(),
        wins,
        losses,
        breakeven,
        win_rate_pct: if history.is_empty() {
            0.0
        } else {
            wins as f64 * 100.0 / denominator
        },
        gross_pnl,
        charges,
        net_pnl,
        average_pnl: if history.is_empty() {
            0.0
        } else {
            net_pnl / denominator
        },
        average_return_pct: if history.is_empty() {
            0.0
        } else {
            return_sum / denominator
        },
        average_hold_seconds: if history.is_empty() {
            0.0
        } else {
            hold_sum as f64 / denominator
        },
    }
}

async fn api_export_csv(
    State(handle): State<DashboardHandle>,
    Query(query): Query<HistoryQuery>,
) -> Result<Response, ApiError> {
    let state = handle.state.read().await;
    let trades = filtered_history(&state.history, &query)?;
    let bytes = history_csv(&trades)?;
    let filename = format!("paper-trades-{}.csv", Utc::now().format("%Y%m%dT%H%M%SZ"));
    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
        .map_err(|error| ApiError::internal(format!("could not build export header: {error}")))?;
    let mut response = Response::new(Body::from(bytes));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/csv; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(header::CONTENT_DISPOSITION, disposition);
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(response)
}

fn history_csv(history: &[HistoryTrade]) -> Result<Vec<u8>, ApiError> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer
        .write_record([
            "trade_id",
            "setup_id",
            "account_id",
            "account_name",
            "strategy",
            "symbol",
            "underlying",
            "expiry",
            "strike",
            "option_type",
            "side",
            "quantity",
            "lots",
            "entry_price",
            "exit_price",
            "streamer_sl",
            "final_sl",
            "target_1",
            "target_2",
            "opened_at",
            "closed_at",
            "hold_seconds",
            "exit_reason",
            "exit_phase",
            "gross_pnl",
            "charges",
            "net_pnl",
            "return_pct",
            "max_favorable_price",
            "max_adverse_price",
            "notes",
        ])
        .map_err(|error| ApiError::internal(format!("could not create CSV: {error}")))?;

    for trade in history {
        writer
            .write_record([
                csv_safe(&trade.trade_id),
                csv_safe(&trade.setup_id),
                csv_safe(&trade.account_id),
                csv_safe(&trade.account_name),
                csv_safe(&trade.strategy),
                csv_safe(&trade.symbol),
                csv_safe(&trade.underlying),
                csv_safe(&trade.expiry),
                trade.strike.to_string(),
                csv_safe(&trade.option_type),
                csv_safe(&trade.side),
                trade.quantity.to_string(),
                trade.lots.to_string(),
                trade.entry_price.to_string(),
                trade.exit_price.to_string(),
                trade.streamer_sl.to_string(),
                trade.final_sl.to_string(),
                trade.target_1.to_string(),
                trade
                    .target_2
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                csv_safe(&trade.opened_at),
                csv_safe(&trade.closed_at),
                trade.hold_seconds.to_string(),
                csv_safe(&trade.exit_reason),
                csv_safe(&trade.exit_phase),
                trade.gross_pnl.to_string(),
                trade.charges.to_string(),
                trade.net_pnl.to_string(),
                trade.return_pct.to_string(),
                trade.max_favorable_price.to_string(),
                trade.max_adverse_price.to_string(),
                csv_safe(&trade.notes),
            ])
            .map_err(|error| ApiError::internal(format!("could not create CSV: {error}")))?;
    }
    writer
        .into_inner()
        .map_err(|error| ApiError::internal(format!("could not finish CSV: {error}")))
}

/// Prevent spreadsheet programs from interpreting text as a formula.
fn csv_safe(value: &str) -> String {
    if value
        .trim_start()
        .starts_with(|character| matches!(character, '=' | '+' | '-' | '@'))
    {
        format!("'{value}")
    } else {
        value.to_owned()
    }
}

struct SseClientState {
    receiver: broadcast::Receiver<DashboardEvent>,
    _guard: SseClientGuard,
}

struct SseClientGuard(Arc<AtomicUsize>);

impl Drop for SseClientGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, AtomicOrdering::Relaxed);
    }
}

async fn api_events(
    State(handle): State<DashboardHandle>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    handle.sse_clients.fetch_add(1, AtomicOrdering::Relaxed);
    let revision = handle.state.read().await.revision;
    let initial = DashboardEvent {
        event: "ready".to_owned(),
        revision,
        at: now_rfc3339(),
        entity_id: None,
    };
    let first = stream::once(async move { Ok(event_to_sse(&initial)) });
    let ongoing = stream::unfold(
        SseClientState {
            receiver: handle.events.subscribe(),
            _guard: SseClientGuard(Arc::clone(&handle.sse_clients)),
        },
        |mut client| async move {
            let event = loop {
                match client.receiver.recv().await {
                    Ok(event) => break event,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        break DashboardEvent {
                            event: "resync_required".to_owned(),
                            revision: 0,
                            at: now_rfc3339(),
                            entity_id: Some(skipped.to_string()),
                        };
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            };
            Some((Ok(event_to_sse(&event)), client))
        },
    );

    Sse::new(first.chain(ongoing)).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("heartbeat"),
    )
}

fn event_to_sse(event: &DashboardEvent) -> Event {
    let data = serde_json::to_string(event).unwrap_or_else(|_| "{}".to_owned());
    Event::default().event(event.event.clone()).data(data)
}

async fn static_index() -> Response {
    asset_response("index.html", INDEX_HTML)
}

async fn static_asset(uri: Uri) -> Response {
    match normalize_asset_path(uri.path()) {
        Ok(path) if path == "index.html" => asset_response("index.html", INDEX_HTML),
        Ok(path) if path == "styles.css" => asset_response("styles.css", STYLES_CSS),
        Ok(path) if path == "app.js" => asset_response("app.js", APP_JS),
        Ok(_) => StatusCode::NOT_FOUND.into_response(),
        Err(()) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "invalid asset path".to_owned(),
            }),
        )
            .into_response(),
    }
}

fn normalize_asset_path(path: &str) -> Result<String, ()> {
    let decoded = percent_decode(path).ok_or(())?;
    let relative = decoded.trim_start_matches('/');
    if relative.is_empty() {
        return Ok("index.html".to_owned());
    }
    if decoded.contains('\\') || decoded.contains('\0') || decoded.contains(':') {
        return Err(());
    }
    let relative = match relative {
        "dashboard" | "dashboard/" => "index.html",
        value => value.strip_prefix("dashboard/").unwrap_or(value),
    };
    let candidate = Path::new(relative);
    if candidate
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(());
    }
    Ok(relative.to_owned())
}

fn percent_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = *bytes.get(index + 1)?;
            let low = *bytes.get(index + 2)?;
            decoded.push((hex_value(high)? << 4) | hex_value(low)?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn asset_response(name: &str, bytes: &'static [u8]) -> Response {
    let content_type = match Path::new(name).extension().and_then(|value| value.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        _ => "application/octet-stream",
    };
    let mut response = Response::new(Body::from(bytes));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_health_serializes_sanitized_rate_and_daily_usage_only() {
        let view = ApiKeyHealthView {
            provider: "OpenAI / Luna".to_owned(),
            slot: 1,
            status: "READY".to_owned(),
            request_remaining: Some(299),
            token_remaining: Some(400_000),
            observed_day_ist: Some("2026-08-15".to_owned()),
            observed_daily_requests: 12,
            observed_daily_total_tokens: 42_000,
            ..ApiKeyHealthView::default()
        };
        let value = serde_json::to_value(view).unwrap();
        assert_eq!(value["request_remaining"], 299);
        assert_eq!(value["token_remaining"], 400_000);
        assert_eq!(value["observed_daily_requests"], 12);
        assert!(value.get("api_key").is_none());
        assert!(value.get("authorization").is_none());
    }

    #[test]
    fn dashboard_rate_labels_state_their_data_provenance() {
        let app = include_str!("../dashboard/app.js");
        assert!(app.contains("server headers"));
        assert!(app.contains("local IST observed"));
    }

    #[test]
    fn dashboard_contains_runtime_key_and_cron_controls() {
        let html = include_str!("../dashboard/index.html");
        let app = include_str!("../dashboard/app.js");
        for required in [
            "cron-view",
            "runtime-key-form",
            "runtime-key-clear",
            "runtime-key-health",
            "cron-job-form",
            "cron-jobs-body",
        ] {
            assert!(
                html.contains(required),
                "missing dashboard element: {required}"
            );
        }
        for required in ["/api/llm/keys", "/api/cron/jobs", "loadCron", "renderCron"] {
            assert!(
                app.contains(required),
                "missing dashboard behavior: {required}"
            );
        }
    }

    #[test]
    fn dashboard_uses_simple_service_status_and_two_approaches() {
        let html = include_str!("../dashboard/index.html");
        let app = include_str!("../dashboard/app.js");

        assert!(!html.contains("session-started"));
        assert!(!html.contains("last-updated"));
        assert!(!html.contains("tick-age"));
        assert!(!html.contains("value=\"all\""));
        assert!(html.contains("Approach 1"));
        assert!(html.contains("Approach 2"));
        assert!(app.contains("hour12: true"));
        assert!(app.contains("\"Online\" : \"Offline\""));
        assert!(!app.contains("Market ${market}"));
    }

    #[test]
    fn dashboard_chart_requires_complete_strategy_wallet_snapshots() {
        let app = include_str!("../dashboard/app.js");
        assert!(app.contains("completeStrategyCurve"));
        assert!(app.contains("expectedAccountIds"));
    }

    #[test]
    fn dashboard_resets_scroll_when_switching_views() {
        let app = include_str!("../dashboard/app.js");
        assert!(app.contains("window.scrollTo({ top: 0, left: 0"));
    }

    #[test]
    fn dashboard_history_empty_state_uses_the_visible_panel_width() {
        let app = include_str!("../dashboard/app.js");
        let css = include_str!("../dashboard/styles.css");
        assert!(app.contains("classList.toggle(\"is-empty\", !trades.length)"));
        assert!(css.contains(".history-table.is-empty { min-width: 0; }"));
    }

    fn trade(
        id: &str,
        account: &str,
        strategy: &str,
        symbol: &str,
        closed_at: &str,
        pnl: f64,
    ) -> HistoryTrade {
        HistoryTrade {
            trade_id: id.to_owned(),
            account_id: account.to_lowercase(),
            account_name: account.to_owned(),
            strategy: strategy.to_owned(),
            symbol: symbol.to_owned(),
            underlying: if symbol.starts_with("NIFTY") {
                "NIFTY"
            } else {
                "SENSEX"
            }
            .to_owned(),
            option_type: if symbol.ends_with("CE") { "CE" } else { "PE" }.to_owned(),
            side: "BUY".to_owned(),
            closed_at: closed_at.to_owned(),
            opened_at: "2026-08-11T04:00:00Z".to_owned(),
            net_pnl: pnl,
            gross_pnl: pnl + 40.0,
            charges: 40.0,
            return_pct: pnl / 100.0,
            hold_seconds: 120,
            ..HistoryTrade::default()
        }
    }

    fn sample_history() -> Vec<HistoryTrade> {
        vec![
            trade(
                "t1",
                "5K",
                "llm",
                "NIFTY-25000-CE",
                "2026-08-11T05:00:00Z",
                100.0,
            ),
            trade(
                "t2",
                "10K",
                "moving_sl",
                "SENSEX-80000-PE",
                "2026-08-11T06:00:00Z",
                -50.0,
            ),
            trade(
                "t3",
                "5K",
                "moving_sl",
                "NIFTY-25100-PE",
                "2026-08-12T05:00:00Z",
                200.0,
            ),
            trade(
                "t4",
                "5K",
                "llm",
                "NIFTY-25200-CE",
                "2026-08-12T06:00:00Z",
                0.0,
            ),
        ]
    }

    #[test]
    fn history_filters_search_sorts_and_paginates_without_score_filtering() {
        let query = HistoryQuery {
            account: Some("5k".to_owned()),
            underlying: Some("nifty".to_owned()),
            sort: Some("net_pnl".to_owned()),
            order: Some("desc".to_owned()),
            page: Some(1),
            page_size: Some(1),
            ..HistoryQuery::default()
        };
        let result = history_response(&sample_history(), &query).unwrap();
        assert_eq!(result.total, 3);
        assert_eq!(result.total_pages, 3);
        assert_eq!(result.items[0].trade_id, "t3");
        assert_eq!(result.summary.wins, 2);
        assert_eq!(result.summary.net_pnl, 300.0);
    }

    #[test]
    fn history_date_and_outcome_filters_use_ist_calendar_dates() {
        let query = HistoryQuery {
            from: Some("2026-08-12".to_owned()),
            to: Some("2026-08-12".to_owned()),
            outcome: Some("win".to_owned()),
            ..HistoryQuery::default()
        };
        let result = history_response(&sample_history(), &query).unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].trade_id, "t3");
    }

    #[test]
    fn frontend_history_aliases_and_sort_fields_are_supported() {
        let query: HistoryQuery = serde_json::from_value(serde_json::json!({
            "q": "NIFTY",
            "status": "won",
            "mode": "llm_exit",
            "sort": "contract",
            "order": "asc"
        }))
        .unwrap();
        let result = history_response(&sample_history(), &query).unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.items[0].trade_id, "t1");
        assert_eq!(result.summary.average_pnl, 100.0);

        for key in [
            "closed_at",
            "contract",
            "account",
            "mode",
            "quantity",
            "entry_price",
            "exit_price",
            "exit_reason",
            "realized_pnl",
            "duration_seconds",
        ] {
            let query = HistoryQuery {
                sort: Some(key.to_owned()),
                ..HistoryQuery::default()
            };
            assert!(
                history_response(&sample_history(), &query).is_ok(),
                "sort key {key}"
            );
        }
    }

    #[test]
    fn invalid_history_parameters_return_a_client_error() {
        let query = HistoryQuery {
            page_size: Some(MAX_HISTORY_PAGE_SIZE + 1),
            ..HistoryQuery::default()
        };
        let error = history_response(&sample_history(), &query).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);

        let query = HistoryQuery {
            sort: Some("secret_field".to_owned()),
            ..HistoryQuery::default()
        };
        assert!(filtered_history(&sample_history(), &query).is_err());
    }

    #[test]
    fn csv_export_quotes_fields_and_blocks_spreadsheet_formulas() {
        let mut item = trade(
            "t1",
            "5K",
            "llm",
            "NIFTY-25000-CE",
            "2026-08-11T05:00:00Z",
            100.0,
        );
        item.notes = "=HYPERLINK(\"https://bad.invalid\")".to_owned();
        let bytes = history_csv(&[item]).unwrap();
        let csv = String::from_utf8(bytes).unwrap();
        assert!(csv.contains("'=HYPERLINK"));
        assert_eq!(csv.lines().count(), 2);
    }

    #[test]
    fn asset_paths_cannot_escape_dashboard_directory() {
        assert_eq!(
            normalize_asset_path("/styles.css"),
            Ok("styles.css".to_owned())
        );
        assert_eq!(
            normalize_asset_path("/dashboard"),
            Ok("index.html".to_owned())
        );
        assert_eq!(
            normalize_asset_path("/dashboard/"),
            Ok("index.html".to_owned())
        );
        assert_eq!(
            normalize_asset_path("/dashboard/styles.css"),
            Ok("styles.css".to_owned())
        );
        assert_eq!(
            normalize_asset_path("/dashboard/app.js"),
            Ok("app.js".to_owned())
        );
        assert!(normalize_asset_path("/../Cargo.toml").is_err());
        assert!(normalize_asset_path("/%2e%2e/Cargo.toml").is_err());
        assert!(normalize_asset_path("/dashboard/%2e%2e/Cargo.toml").is_err());
        assert!(normalize_asset_path("/..%5cCargo.toml").is_err());
    }

    #[tokio::test]
    async fn health_api_reports_the_current_revision() {
        let mut state = DashboardState::empty();
        state.revision = 7;
        state.health.overall = "healthy".to_owned();
        let Json(response) = api_health(State(DashboardHandle::new(state))).await;
        assert!(response.ok);
        assert_eq!(response.revision, 7);
        assert_eq!(response.status, "healthy");
    }

    #[tokio::test]
    async fn handle_updates_state_and_revision_atomically() {
        let handle = DashboardHandle::empty();
        handle
            .upsert_account(AccountView {
                account_id: "5k-llm".to_owned(),
                account_name: "5K".to_owned(),
                strategy: "llm".to_owned(),
                ..AccountView::default()
            })
            .await;
        let snapshot = handle.snapshot().await;
        assert_eq!(snapshot.revision, 1);
        assert_eq!(snapshot.accounts.len(), 1);
    }

    #[tokio::test]
    async fn runtime_log_buffer_is_bounded_and_notifies_live_clients() {
        let handle = DashboardHandle::empty();
        for event_id in 1..=MAX_RUNTIME_LOGS as i64 {
            handle
                .record_log(RuntimeLogEntry {
                    event_id,
                    message: format!("event {event_id}"),
                    ..RuntimeLogEntry::default()
                })
                .await;
        }
        let mut events = handle.events.subscribe();
        handle
            .record_log(RuntimeLogEntry {
                event_id: MAX_RUNTIME_LOGS as i64 + 1,
                message: "newest".to_owned(),
                ..RuntimeLogEntry::default()
            })
            .await;

        let event = events.recv().await.unwrap();
        let logs = handle.snapshot().await.logs;
        assert_eq!(event.event, "runtime_log");
        assert_eq!(logs.len(), MAX_RUNTIME_LOGS);
        assert_eq!(logs.first().unwrap().event_id, 2);
        assert_eq!(logs.last().unwrap().event_id, 1001);
    }

    #[test]
    fn runtime_log_sanitizer_redacts_secrets_and_bounds_output() {
        let raw = format!(
            "first line\nAuthorization: Bearer ordinary-secret github_pat_secret rnd_secret AIzaSecret sk_secret \
             postgresql://owner:password@db.example/neon?sslmode=require \
             https://example.test/live?signature=secret\u{7} {}",
            "x".repeat(700)
        );
        let sanitized = sanitize_log_message(&raw);

        assert!(!sanitized.contains('\n'));
        assert!(!sanitized.contains('\r'));
        assert!(!sanitized.contains("github_pat_"));
        assert!(!sanitized.contains("ordinary-secret"));
        assert!(!sanitized.contains("rnd_"));
        assert!(!sanitized.contains("AIza"));
        assert!(!sanitized.contains("sk_"));
        assert!(!sanitized.contains("password"));
        assert!(!sanitized.contains("signature="));
        assert!(!sanitized.contains('\u{7}'));
        assert!(sanitized.chars().count() <= 512);
    }

    #[test]
    fn runtime_log_sanitizer_redacts_openai_standard_and_project_shapes() {
        let standard = format!("sk-{}", "x".repeat(32));
        let project = format!("sk-proj-{}", "y".repeat(32));
        let sanitized = sanitize_log_message(&format!("failure {standard} then {project}"));

        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains(&standard));
        assert!(!sanitized.contains(&project));
        assert!(!sanitized.contains("sk-"));
    }

    #[test]
    fn runtime_logs_response_filters_orders_and_validates_limit() {
        let logs = vec![
            RuntimeLogEntry {
                event_id: 1,
                occurred_at: "2026-08-11T10:00:00Z".to_owned(),
                occurred_at_ist: "2026-08-11T15:30:00+05:30".to_owned(),
                level: "INFO".to_owned(),
                component: "scheduler".to_owned(),
                code: "WAITING".to_owned(),
                message: "waiting".to_owned(),
            },
            RuntimeLogEntry {
                event_id: 2,
                occurred_at: "2026-08-11T10:01:00Z".to_owned(),
                occurred_at_ist: "2026-08-11T15:31:00+05:30".to_owned(),
                level: "ERROR".to_owned(),
                component: "Analysis".to_owned(),
                code: "ANALYSIS_FAILED".to_owned(),
                message: "provider failed".to_owned(),
            },
            RuntimeLogEntry {
                event_id: 3,
                occurred_at: "2026-08-11T10:02:00Z".to_owned(),
                occurred_at_ist: "2026-08-11T15:32:00+05:30".to_owned(),
                level: "ERROR".to_owned(),
                component: "analysis".to_owned(),
                code: "ANALYSIS_RETRY".to_owned(),
                message: "retrying".to_owned(),
            },
        ];
        let query = RuntimeLogQuery {
            limit: Some(2),
            level: Some("error".to_owned()),
            component: Some("analysis".to_owned()),
        };
        let response = runtime_logs_response(&logs, &query).unwrap();

        assert_eq!(response.total, 2);
        assert_eq!(response.limit, 2);
        assert_eq!(response.items[0].event_id, 3);
        assert_eq!(response.items[1].event_id, 2);

        let invalid = RuntimeLogQuery {
            limit: Some(0),
            ..RuntimeLogQuery::default()
        };
        assert_eq!(
            runtime_logs_response(&logs, &invalid).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
        let too_large = RuntimeLogQuery {
            limit: Some(201),
            ..RuntimeLogQuery::default()
        };
        assert_eq!(
            runtime_logs_response(&logs, &too_large).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn dashboard_key_routes_are_write_only_and_clear_all_slots() {
        let handle = DashboardHandle::empty();
        let added = api_add_llm_keys(
            State(handle.clone()),
            AxumJson(AddLlmKeysRequest {
                keys: vec![
                    "route-test-key-one".to_owned(),
                    "route-test-key-two".to_owned(),
                ],
            }),
        )
        .await
        .unwrap()
        .0;
        let serialized = serde_json::to_string(&added).unwrap();
        assert_eq!(added.loaded_slots, 2);
        assert!(!serialized.contains("route-test-key-one"));
        assert!(
            !serde_json::to_string(&api_llm_key_health(State(handle.clone())).await.0)
                .unwrap()
                .contains("route-test-key-one")
        );
        assert_eq!(handle.snapshot().await.health.analysis.status, "READY");

        let cleared = api_clear_llm_keys(State(handle.clone())).await.0;
        assert_eq!(cleared.loaded_slots, 0);
        assert_eq!(cleared.state, crate::analysis::VaultState::KeysRequired);
        assert_eq!(
            handle.snapshot().await.health.analysis.status,
            "KEYS_REQUIRED"
        );
    }
}
