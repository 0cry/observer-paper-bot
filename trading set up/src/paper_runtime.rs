//! End-to-end live-edge paper-trading orchestration.
//!
//! This module is deliberately paper-only. It receives market prices, but it
//! contains no broker order endpoint and cannot place a real order.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::pending,
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use chrono_tz::Asia::Kolkata;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, interval, sleep_until},
};

use crate::{
    InstrumentRow, TokenManager,
    analysis::{
        self, ActionKind, AnalysisClient, AnalysisClientConfig, AnalysisInput, ClipWindow,
        ExitMode, OptionType as AnalysisOptionType, PriceSnapshot, TradeAction, TradeDirection,
        Underlying as AnalysisUnderlying, ValidatedAnalysis, WatchedOptionSnapshot,
    },
    blocker::{DispatchEvent, Dispatcher, InputClip, RetainedClip},
    capture::{
        CaptureConfig, CaptureEvent, CaptureSession, MediaSegment, SEGMENT_SECONDS,
        extract_latest_selected_jpeg,
    },
    config::AppConfig,
    dashboard::{
        self, AccountView, ApiKeyHealthView, ComponentHealth, DashboardHandle, DashboardState,
        EquityPoint, HealthView, HistoryTrade, MetricsView, PendingOrderView, PositionView,
        SessionView, SignalView,
    },
    fetch_instruments,
    market_feed::{
        FeedConnectionState, LatestTicks, MarketFeedConfig, MarketFeedHandle, ResolvedInstrument,
        SubscriptionLease, SubscriptionReason, Tick, TickSource, spawn_market_feed,
        token_provider_fn,
    },
    neon::NeonStore,
    paper::{
        self, AccountSpec, BrokerEvent, ClosedTrade, MarketTick, OptionContract as PaperContract,
        OptionKind, PaperBroker, PaperBrokerConfig, PaperBrokerSnapshot, PlacementStatus,
        ShadowMode, TradeLevels as PaperLevels, TradeSetup, TradeSide,
        Underlying as PaperUnderlying,
    },
    parse_instrument_expiry,
    persistence::{JsonlEventWriter, SyncPolicy, atomic_write_json_snapshot, load_json_snapshot},
    recovery_buffer::{RecoveryBuffer, RecoveryImage},
    runtime_logs::RuntimeEventLogger,
    stt::{ElevenLabsSttClient, SegmentInput, SttOptions, TranscriptChunk, TranscriptStatus},
    trailing::{TrailLevels, TrailPhase, TrailState, Underlying as TrailUnderlying},
};

const DASHBOARD_REFRESH_MS: u64 = 250;
const SNAPSHOT_SECONDS: u64 = 1;
const NEON_SYNC_SECONDS: u64 = 60;
const EQUITY_SAMPLE_SECONDS: u64 = 5;
const MIN_CANDIDATE_OBSERVATION_SECONDS: u64 = 10;
const CANDIDATE_RENEW_AHEAD_SECONDS: u64 = 2;
const MAX_SIGNALS: usize = 500;
const MAX_EQUITY_POINTS: usize = 10_000;
const STREAM_CONTEXT_SCHEMA_VERSION: u32 = 1;
const MAX_STREAM_CONTEXT_FILE_BYTES: u64 = 64 * 1024;
const MAX_EXECUTABLE_SIGNAL_AGE_MS: i64 = 45_000;

#[derive(Clone)]
struct RoutedContract {
    analysis: analysis::OptionContract,
    paper: PaperContract,
    instrument: ResolvedInstrument,
}

struct CandidateWatch {
    route: RoutedContract,
    lease: SubscriptionLease,
    watched_since: Instant,
    watched_since_timestamp_ms: i64,
}

#[derive(Debug)]
struct SttCompleted {
    segment: MediaSegment,
    transcript: TranscriptChunk,
    latency_ms: u64,
}

/// STT requests complete concurrently, but downstream blocker decisions must
/// see source segments in capture order. The first capture sequence establishes
/// the contiguous drain point; failed transcripts are buffered and drained like
/// successful ones so they cannot leave a permanent sequence gap.
#[derive(Debug)]
struct OrderedSttCompletionBuffer<T> {
    next_sequence: Option<u64>,
    buffered: BTreeMap<u64, T>,
}

impl<T> Default for OrderedSttCompletionBuffer<T> {
    fn default() -> Self {
        Self {
            next_sequence: None,
            buffered: BTreeMap::new(),
        }
    }
}

impl<T> OrderedSttCompletionBuffer<T> {
    fn begin_at(&mut self, sequence: u64) {
        if self.next_sequence.is_none() {
            self.next_sequence = Some(sequence);
        }
    }

    fn insert(&mut self, sequence: u64, completed: T) -> Vec<T> {
        let Some(next_sequence) = self.next_sequence else {
            self.next_sequence = Some(sequence.saturating_add(1));
            return vec![completed];
        };
        if sequence < next_sequence || self.buffered.contains_key(&sequence) {
            return Vec::new();
        }
        self.buffered.insert(sequence, completed);

        let mut ordered = Vec::new();
        let mut next = next_sequence;
        while let Some(completed) = self.buffered.remove(&next) {
            ordered.push(completed);
            next = next.saturating_add(1);
        }
        self.next_sequence = Some(next);
        ordered
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.buffered.is_empty()
    }
}

struct AnalysisCompleted {
    dispatch: ReadyDispatch,
    input: AnalysisInput,
    transcript_excerpt: String,
    latency_ms: u64,
    visual_sent: bool,
    visual_note: Option<String>,
    recovery_image: Option<RecoveryImage>,
    result: std::result::Result<ValidatedAnalysis, String>,
}

/// Sparse visual input is deliberately tied to durable context commits rather
/// than requests. A failed model call or failed commit never advances cadence.
#[derive(Debug, Default, Clone)]
struct VisualCadence {
    commits_since_visual: u8,
    last_visual_at: Option<DateTime<Utc>>,
}

impl VisualCadence {
    fn visual_due(&self) -> bool {
        self.commits_since_visual >= 4
    }

    fn record_committed(&mut self, visual_sent: bool, at: DateTime<Utc>) {
        if visual_sent {
            self.commits_since_visual = 0;
            self.last_visual_at = Some(at);
        } else if !self.visual_due() {
            self.commits_since_visual = self.commits_since_visual.saturating_add(1);
        }
    }

    fn status(&self) -> String {
        if self.visual_due() {
            "frame due on next eligible analysis".to_owned()
        } else {
            format!(
                "frame after {} more committed analysis call(s)",
                4 - self.commits_since_visual
            )
        }
    }
}

#[derive(Debug, Clone)]
struct ReadyDispatch {
    sequence: u64,
    clips: Vec<RetainedClip>,
    segments: Vec<MediaSegment>,
}

impl ReadyDispatch {
    fn source_sequences(&self) -> impl Iterator<Item = u64> + '_ {
        self.segments.iter().map(|segment| segment.sequence)
    }

    fn started_at(&self) -> DateTime<Utc> {
        self.segments
            .iter()
            .map(|segment| segment.started_at_utc)
            .min()
            .expect("ready dispatch always owns a source segment")
    }

    fn ended_at(&self) -> DateTime<Utc> {
        self.segments
            .iter()
            .map(|segment| segment.ended_at_utc)
            .max()
            .expect("ready dispatch always owns a source segment")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct StreamContextEnvelope {
    schema_version: u32,
    stream_url: String,
    trading_date_ist: String,
    updated_at: DateTime<Utc>,
    source_window_sequence: u64,
    source_clip_ended_at: DateTime<Utc>,
    rolling_context: analysis::RollingContext,
}

impl StreamContextEnvelope {
    fn for_analysis(
        stream_url: String,
        source_window_sequence: u64,
        source_clip_ended_at: DateTime<Utc>,
        rolling_context: analysis::RollingContext,
        observed_at: DateTime<Utc>,
    ) -> Self {
        let trading_date_ist = ist_trading_date(source_clip_ended_at);
        Self {
            schema_version: STREAM_CONTEXT_SCHEMA_VERSION,
            stream_url,
            trading_date_ist,
            // The segment clock can legitimately be slightly ahead of the
            // sampled wall clock at live edge. A committed envelope must not
            // claim to have been updated before its own source evidence ends.
            updated_at: observed_at.max(source_clip_ended_at),
            source_window_sequence,
            source_clip_ended_at,
            rolling_context,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurablePaperState {
    broker: PaperBroker,
    stream_url: String,
    trading_date_ist: String,
    rolling_context: Option<analysis::RollingContext>,
    history: Vec<HistoryTrade>,
    #[serde(default)]
    equity_curve: Vec<EquityPoint>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableStateSource {
    Local,
    Neon,
}

/// Choose by the checkpoint timestamp. Equal/invalid timestamps resolve to
/// local so a locally durable broker order is never discarded merely because
/// a remote replica has the same coarse timestamp.
fn select_newest_durable_state<T>(
    local: Option<T>,
    neon: Option<T>,
    updated_at: impl Fn(&T) -> DateTime<Utc>,
) -> Option<(DurableStateSource, T)> {
    match (local, neon) {
        (Some(local), Some(neon)) => {
            if updated_at(&neon) > updated_at(&local) {
                Some((DurableStateSource::Neon, neon))
            } else {
                Some((DurableStateSource::Local, local))
            }
        }
        (Some(local), None) => Some((DurableStateSource::Local, local)),
        (None, Some(neon)) => Some((DurableStateSource::Neon, neon)),
        (None, None) => None,
    }
}

fn local_broker_checkpoint_time(path: &Path) -> DateTime<Utc> {
    std::fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
        .map(DateTime::<Utc>::from)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).expect("Unix epoch is valid"))
}

fn load_local_durable_state(
    broker_path: &Path,
    history_path: &Path,
    context_path: &Path,
    stream_url: &str,
    trading_date_ist: &str,
) -> Result<Option<DurablePaperState>> {
    let Some(broker) = load_json_snapshot::<PaperBroker>(broker_path)? else {
        return Ok(None);
    };
    // Context is optional and cannot elevate a broker/order claim. Any
    // invalid/orphan context is ignored until it can be reconciled below.
    let rolling_context =
        load_stream_context(context_path, stream_url, trading_date_ist).unwrap_or(None);
    Ok(Some(DurablePaperState {
        broker,
        stream_url: stream_url.to_owned(),
        trading_date_ist: trading_date_ist.to_owned(),
        rolling_context,
        history: load_json_snapshot::<Vec<HistoryTrade>>(history_path)?.unwrap_or_default(),
        equity_curve: Vec::new(),
        updated_at: local_broker_checkpoint_time(broker_path),
    }))
}

struct ContextCommitCompleted {
    completed: AnalysisCompleted,
    analysis: ValidatedAnalysis,
    envelope: StreamContextEnvelope,
    result: std::result::Result<(), String>,
}

/// A Analysis call remains active until its validated rolling context has been
/// durably committed. This makes the context supplied to call N+1 exactly the
/// context returned by call N, while the rest of the runtime remains async.
#[derive(Debug, Default, PartialEq, Eq)]
struct AnalysisDispatchState {
    active_sequence: Option<u64>,
}

impl AnalysisDispatchState {
    fn try_begin(&mut self, sequence: u64) -> bool {
        if self.active_sequence.is_some() {
            return false;
        }
        self.active_sequence = Some(sequence);
        true
    }

    fn owns(&self, sequence: u64) -> bool {
        self.active_sequence == Some(sequence)
    }

    fn finish(&mut self, sequence: u64) -> bool {
        if !self.owns(sequence) {
            return false;
        }
        self.active_sequence = None;
        true
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RuntimeAuditEvent {
    Broker {
        event: BrokerEvent,
    },
    Pipeline {
        timestamp: String,
        component: String,
        status: String,
        detail: String,
    },
}

/// Run the complete live-edge paper pipeline until Ctrl-C, an optional test
/// deadline, or a fatal capture failure. The dashboard and market manager keep
/// running after the YouTube stream ends so open paper positions can still be
/// managed through EOD.
pub async fn run(
    project_dir: &Path,
    stream_url: String,
    http: Client,
    duration_seconds: Option<u64>,
) -> Result<()> {
    run_internal(project_dir, stream_url, http, duration_seconds, None, None).await
}

pub async fn run_with_dashboard(
    project_dir: &Path,
    stream_url: String,
    http: Client,
    duration_seconds: Option<u64>,
    dashboard_handle: DashboardHandle,
    runtime_logger: RuntimeEventLogger,
) -> Result<()> {
    run_internal(
        project_dir,
        stream_url,
        http,
        duration_seconds,
        Some(dashboard_handle),
        Some(runtime_logger),
    )
    .await
}

async fn run_internal(
    project_dir: &Path,
    stream_url: String,
    http: Client,
    duration_seconds: Option<u64>,
    shared_dashboard: Option<DashboardHandle>,
    shared_runtime_logger: Option<RuntimeEventLogger>,
) -> Result<()> {
    let config = AppConfig::load(project_dir).context("paper runtime configuration is invalid")?;
    let started_at = Utc::now();
    let session_id = format!("paper_{}", started_at.format("%Y%m%dT%H%M%SZ"));
    let paper_root = config.paths.data_dir.join("paper");
    let session_dir = paper_root.join("sessions").join(&session_id);
    tokio::fs::create_dir_all(&session_dir)
        .await
        .with_context(|| format!("could not create {}", session_dir.display()))?;

    let mut session_view = SessionView {
        session_id: session_id.clone(),
        status: "STARTING".to_owned(),
        mode: "PAPER_ONLY".to_owned(),
        started_at: started_at.to_rfc3339(),
        stream_url: stream_url.clone(),
        stream_title: "Live YouTube stream".to_owned(),
        market_status: "STARTING".to_owned(),
        ..SessionView::default()
    };
    let mut health = initial_health();
    if let Some(handle) = shared_dashboard.as_ref() {
        let shared_health = handle.snapshot().await.health;
        seed_shared_discovery_health(&mut health, &shared_health);
    }
    let initial_dashboard_state = DashboardState {
        session: session_view.clone(),
        health: health.clone(),
        ..DashboardState::empty()
    };
    let (dashboard_handle, dashboard_task) = match shared_dashboard {
        Some(handle) => {
            handle
                .update("live_runtime_starting", None, |state| {
                    apply_live_start_status(state, session_view.clone(), health.clone());
                })
                .await;
            (handle, None)
        }
        None => {
            let handle = DashboardHandle::new(initial_dashboard_state);
            let listener = tokio::net::TcpListener::bind(config.dashboard.bind)
                .await
                .with_context(|| {
                    format!("could not bind dashboard at {}", config.dashboard.bind)
                })?;
            let dashboard_router = dashboard::router(handle.clone());
            let task: JoinHandle<std::io::Result<()>> =
                tokio::spawn(async move { axum::serve(listener, dashboard_router).await });
            println!("Dashboard: http://{}", config.dashboard.bind);
            (handle, Some(task))
        }
    };
    let provided_runtime_logger = shared_runtime_logger.is_some();
    let mut runtime_logger = shared_runtime_logger
        .unwrap_or_else(|| RuntimeEventLogger::new(dashboard_handle.clone(), None));
    runtime_logger
        .record(
            "INFO",
            "runtime",
            "PAPER_SESSION_STARTING",
            "paper-only live-edge session is initializing",
        )
        .await;

    let mut audit_writer = JsonlEventWriter::open(
        session_dir.join("events.jsonl"),
        SyncPolicy::SyncEveryEvents(NonZeroUsize::new(20).expect("non-zero")),
    )?;
    append_pipeline_audit(
        &mut audit_writer,
        "runtime",
        "starting",
        "paper-only pipeline initialized",
    )?;

    let direct_broker_credentials = match (
        config.broker.client_id.as_ref(),
        config.broker.mpin.as_ref(),
        config.broker.totp_secret.as_ref(),
    ) {
        (Some(client_id), Some(mpin), Some(totp_secret)) => Some((
            client_id.expose_secret().to_owned(),
            mpin.expose_secret().to_owned(),
            totp_secret.expose_secret().to_owned(),
        )),
        _ => None,
    };
    let token_manager = TokenManager::with_paths(
        http.clone(),
        config.paths.observer_token_path.clone(),
        config.paths.observer_totp_path.clone(),
        direct_broker_credentials,
    );
    let token = token_manager.ensure_valid_token().await?;
    let instruments = fetch_instruments(&http, &token).await?;
    drop(token);

    let token_manager_for_feed = token_manager.clone();
    let token_provider = token_provider_fn(move || {
        let manager = token_manager_for_feed.clone();
        async move { manager.ensure_valid_token().await }
    });
    let mut feed_config = MarketFeedConfig::default();
    feed_config.candidate_watch_ttl = Duration::from_secs(config.trading.candidate_ttl_seconds);
    let feed_runtime = spawn_market_feed(feed_config, token_provider)?;
    let feed_handle = feed_runtime.handle.clone();
    let latest_ticks = feed_handle.latest_ticks();
    let mut tick_receiver = feed_handle.subscribe_ticks();
    let mut feed_state = feed_handle.connection_state();

    let mut stt_options = SttOptions::default();
    stt_options.concurrency = config.media.stt_concurrency;
    stt_options.credential_limit = config.media.elevenlabs_key_limit;
    let stt_client = ElevenLabsSttClient::from_keys_with_options(
        config
            .elevenlabs
            .api_keys
            .iter()
            .map(|key| key.expose_secret()),
        stt_options,
    )?;
    let credential_count = stt_client.credential_count().await;
    health.transcription = component(
        "READY",
        format!("Scribe v2 ready with {credential_count} credential slot(s)"),
    );

    let analysis_config = AnalysisClientConfig {
        model: config.openai.model.clone(),
        ..AnalysisClientConfig::default()
    };
    let analysis_client = Arc::new(AnalysisClient::from_runtime_vault(
        dashboard_handle.openai_vault(),
        analysis_config,
    )?);
    let analysis_credential_count = analysis_client.credential_count().await;
    health.analysis = component(
        if analysis_credential_count == 0 {
            "KEYS_REQUIRED"
        } else {
            "READY"
        },
        if analysis_credential_count == 0 {
            "add an OpenAI key in the dashboard".to_owned()
        } else {
            format!(
                "{} ready with {analysis_credential_count} credential slot(s)",
                config.openai.model
            )
        },
    );

    let broker_config = paper_broker_config(&config)?;
    let account_specs = paper_account_specs(&config)?;
    let now = Utc::now();
    let trading_date_ist = now
        .with_timezone(&Kolkata)
        .date_naive()
        .format("%Y-%m-%d")
        .to_string();
    let neon_store = match config.database.url.as_ref() {
        Some(url) => Some(
            NeonStore::connect(url.expose_secret())
                .await
                .context("configured Neon storage is unavailable; refusing unsafe startup")?,
        ),
        None => None,
    };
    if !provided_runtime_logger {
        runtime_logger = RuntimeEventLogger::new(dashboard_handle.clone(), neon_store.clone());
        let _ = runtime_logger.load_recent(200).await;
    }
    let broker_state_path = paper_root.join("state_latest.json");
    let stream_context_path = paper_root.join("stream_context.json");
    let history_path = paper_root.join("trade_history.json");
    let neon_state = match neon_store.as_ref() {
        Some(store) => store
            .load_runtime_state::<DurablePaperState>("paper-primary")
            .await
            .context("could not restore durable Neon paper state")?,
        None => None,
    };
    let local_state = load_local_durable_state(
        &broker_state_path,
        &history_path,
        &stream_context_path,
        &stream_url,
        &trading_date_ist,
    )?;
    let selected_state =
        select_newest_durable_state(local_state, neon_state, |state| state.updated_at);
    let remote_state = selected_state.as_ref().map(|(_, state)| state);
    let persisted_broker = remote_state.map(|state| state.broker.clone());
    let restored_broker = persisted_broker.is_some();
    let mut broker = match persisted_broker {
        Some(persisted) => PaperBroker::restore_from_persisted(
            persisted,
            broker_config.clone(),
            account_specs.clone(),
        )
        .context("persisted paper broker state is incompatible with current configuration")?,
        None => PaperBroker::with_accounts(broker_config, account_specs)?,
    };
    let remote_context = remote_state.as_ref().and_then(|state| {
        (state.stream_url == stream_url && state.trading_date_ist == trading_date_ist)
            .then(|| state.rolling_context.clone())
            .flatten()
    });
    let mut rolling_context = if let Some(context) = remote_context {
        append_pipeline_audit(
            &mut audit_writer,
            "stream_context",
            "restored",
            "restored bounded rolling context with the atomic Neon broker state",
        )?;
        Some(context)
    } else {
        match load_stream_context(&stream_context_path, &stream_url, &trading_date_ist) {
            Ok(Some(context)) => {
                append_pipeline_audit(
                    &mut audit_writer,
                    "stream_context",
                    "restored",
                    "restored bounded rolling context for this stream and IST trading date",
                )?;
                Some(context)
            }
            Ok(None) => {
                append_pipeline_audit(
                    &mut audit_writer,
                    "stream_context",
                    "new",
                    "no matching rolling context for this stream and IST trading date",
                )?;
                None
            }
            Err(error) => {
                append_pipeline_audit(
                    &mut audit_writer,
                    "stream_context",
                    "ignored",
                    &format!("saved rolling context was not usable: {error:#}"),
                )?;
                None
            }
        }
    };
    if let Some(context) = rolling_context.as_mut() {
        reconcile_restored_context_with_broker(context, &broker);
    }
    broker
        .start_trading_day_ist(&trading_date_ist, now.timestamp_millis())
        .context("could not initialize the current IST paper-trading date")?;
    append_pipeline_audit(
        &mut audit_writer,
        "persistence",
        if restored_broker { "restored" } else { "new" },
        if restored_broker {
            "validated and restored state_latest.json"
        } else {
            "no prior broker snapshot; initialized fresh paper state"
        },
    )?;

    let mut historical_trades = remote_state
        .as_ref()
        .map(|state| state.history.clone())
        .or(load_json_snapshot::<Vec<HistoryTrade>>(&history_path)?)
        .unwrap_or_default();
    let mut equity_curve = remote_state
        .as_ref()
        .map(|state| state.equity_curve.clone())
        .unwrap_or_default();
    deduplicate_history(&mut historical_trades);
    if let Some(store) = neon_store.as_ref() {
        save_neon_runtime(
            store,
            &broker,
            &stream_url,
            &trading_date_ist,
            rolling_context.as_ref(),
            &historical_trades,
            &equity_curve,
        )
        .await
        .context("could not establish initial durable Neon checkpoint")?;
        health.persistence = component("HEALTHY", "atomic Neon state is connected");
    }

    let capture_config = CaptureConfig {
        output_dir: config.paths.media_dir.clone(),
        yt_dlp_path: config.paths.yt_dlp_path.clone(),
        ffmpeg_path: config.paths.ffmpeg_path.clone(),
        clip_retention: config.media.clips_to_keep,
        ..CaptureConfig::default()
    };
    let mut capture = CaptureSession::start(capture_config, &stream_url)
        .await
        .context("could not start current-live-edge capture")?;
    let capture_controller = capture.controller();
    session_view.status = "RUNNING".to_owned();
    health.stream_capture = component(
        "STARTING",
        "waiting for the first closed 3-second source segment",
    );
    health.persistence = component(
        "HEALTHY",
        if neon_store.is_some() {
            "atomic Neon state plus local session cache".to_owned()
        } else {
            format!("local-only session {}", session_dir.display())
        },
    );
    append_pipeline_audit(
        &mut audit_writer,
        "capture",
        "started",
        "live-edge ingest process started",
    )?;

    let (stt_sender, mut stt_receiver) = mpsc::channel::<SttCompleted>(32);
    let (analysis_sender, mut analysis_receiver) = mpsc::channel::<AnalysisCompleted>(8);
    let (context_commit_sender, mut context_commit_receiver) =
        mpsc::channel::<ContextCommitCompleted>(1);
    let mut source_segments = BTreeMap::<u64, MediaSegment>::new();
    let mut recovery_buffer = RecoveryBuffer::default();
    let mut stt_completion_buffer = OrderedSttCompletionBuffer::default();
    let mut blocker_dispatcher = Dispatcher::default();
    let mut pending_dispatch = Option::<ReadyDispatch>::None;
    let mut analysis_dispatch = AnalysisDispatchState::default();
    let mut visual_cadence = VisualCadence::default();
    session_view.visual_status = Some(visual_cadence.status());
    let mut candidate_watches = Vec::<CandidateWatch>::new();
    let mut persistent_leases = HashMap::<(String, SubscriptionReason), SubscriptionLease>::new();
    let mut known_routes = routes_from_broker(&broker)?;
    reconcile_persistent_subscriptions(
        &feed_handle,
        &broker.snapshot(Utc::now().timestamp_millis()),
        &known_routes,
        &mut persistent_leases,
    )
    .await?;
    let mut signals = Vec::<SignalView>::new();
    let mut last_event_sequence = 0u64;
    let mut last_closed_count = broker.closed_trade_history().len();
    let mut capture_active = true;
    let mut dashboard_dirty = true;
    let mut active_trading_date_ist = trading_date_ist;
    let mut eod_triggered = broker
        .snapshot(Utc::now().timestamp_millis())
        .end_of_day_timestamp_ms
        .is_some();

    let mut dashboard_timer = interval(Duration::from_millis(DASHBOARD_REFRESH_MS));
    dashboard_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut snapshot_timer = interval(Duration::from_secs(SNAPSHOT_SECONDS));
    snapshot_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut neon_timer = interval(Duration::from_secs(NEON_SYNC_SECONDS));
    neon_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut equity_timer = interval(Duration::from_secs(EQUITY_SAMPLE_SECONDS));
    equity_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut blocker_timer = interval(Duration::from_millis(250));
    blocker_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let deadline = duration_seconds.map(|seconds| Instant::now() + Duration::from_secs(seconds));
    let deadline_wait = async {
        match deadline {
            Some(deadline) => sleep_until(deadline).await,
            None => pending::<()>().await,
        }
    };
    tokio::pin!(deadline_wait);
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);

    loop {
        tokio::select! {
            _ = &mut interrupt => {
                session_view.status = "STOPPING".to_owned();
                break;
            }
            _ = &mut deadline_wait => {
                session_view.status = "TEST_DURATION_COMPLETE".to_owned();
                break;
            }
            _ = neon_timer.tick(), if neon_store.is_some() => {
                let store = neon_store.as_ref().expect("guarded Neon store");
                let durable_result = save_neon_runtime(
                    store,
                    &broker,
                    &stream_url,
                    &active_trading_date_ist,
                    rolling_context.as_ref(),
                    &historical_trades,
                    &equity_curve,
                ).await;
                if let Err(error) = durable_result {
                    health.persistence = component("DEGRADED", "Neon checkpoint failed");
                    runtime_logger
                        .record(
                            "ERROR",
                            "persistence",
                            "CHECKPOINT_FAILED",
                            &error.to_string(),
                        )
                        .await;
                } else {
                    let detail_state = dashboard_state(
                        &broker.snapshot(Utc::now().timestamp_millis()),
                        session_view.clone(),
                        health.clone(),
                        signals.clone(),
                        equity_curve.clone(),
                        historical_trades.clone(),
                        config.trading.charge_per_fill_rupees,
                    );
                    if let Err(error) = sync_neon_rows(
                        store,
                        &active_trading_date_ist,
                        &detail_state,
                    ).await {
                        health.persistence = component("DEGRADED", "Neon detail-row sync failed");
                        runtime_logger
                            .record(
                                "ERROR",
                                "persistence",
                                "DETAIL_SYNC_FAILED",
                                &error.to_string(),
                            )
                            .await;
                    } else {
                        health.persistence = component("HEALTHY", "atomic Neon checkpoint current");
                    }
                }
                dashboard_dirty = true;
            }
            capture_event = capture.next_event(), if capture_active => {
                match capture_event {
                    Some(CaptureEvent::SegmentReady(segment)) => {
                        health.stream_capture = component("HEALTHY", "receiving exact 3-second live-edge source segments");
                        session_view.transcript_segments_ready = source_segments.len();
                        stt_completion_buffer.begin_at(segment.sequence);
                        spawn_stt_job(stt_client.clone(), stt_sender.clone(), segment);
                    }
                    Some(CaptureEvent::Fault { message, .. }) => {
                        health.stream_capture = component("DEGRADED", message.clone());
                        append_pipeline_audit(&mut audit_writer, "capture", "fault", &message)?;
                        runtime_logger
                            .record(
                                "ERROR",
                                "capture",
                                "CAPTURE_FAULT",
                                "live-edge capture reported a recoverable fault",
                            )
                            .await;
                        dashboard_dirty = true;
                    }
                    Some(CaptureEvent::Stopped { reason, .. }) => {
                        capture_active = false;
                        health.stream_capture = component("STOPPED", format!("stream capture stopped: {reason:?}"));
                        session_view.status = "STREAM_ENDED_MARKET_MANAGEMENT_ACTIVE".to_owned();
                        let cancelled = broker.cancel_all_pending_entries(
                            Utc::now().timestamp_millis(),
                            "pending entry cancelled because the stream ended",
                        );
                        if !cancelled.is_empty() {
                            append_pipeline_audit(
                                &mut audit_writer,
                                "paper",
                                "pending_entries_cancelled",
                                "stream ended before pending entry filled",
                            )?;
                        }
                        append_pipeline_audit(&mut audit_writer, "capture", "stopped", &format!("{reason:?}"))?;
                        runtime_logger
                            .record(
                                "INFO",
                                "capture",
                                "STREAM_CAPTURE_STOPPED",
                                &format!("stream capture stopped: {reason:?}"),
                            )
                            .await;
                        dashboard_dirty = true;
                    }
                    None => {
                        capture_active = false;
                        health.stream_capture = component("STOPPED", "capture event channel closed");
                        runtime_logger
                            .record(
                                "WARN",
                                "capture",
                                "CAPTURE_CHANNEL_CLOSED",
                                "capture event channel closed",
                            )
                            .await;
                        dashboard_dirty = true;
                    }
                }
            }
            Some(completed) = stt_receiver.recv() => {
                let ordered_completions = stt_completion_buffer
                    .insert(completed.segment.sequence, completed);
                let mut ready_dispatches = Vec::new();
                for completed in ordered_completions {
                let complete = completed.transcript.status == TranscriptStatus::Complete;
                session_view.transcription_latency_ms = Some(completed.latency_ms);
                health.transcription = if complete {
                    healthy_with_latency("Scribe v2 segment complete", completed.latency_ms)
                } else {
                    component("DEGRADED", format!("segment {} incomplete: {:?}", completed.segment.sequence, completed.transcript.failure))
                };
                if !complete {
                    runtime_logger
                        .record(
                            "WARN",
                            "transcription",
                            "SEGMENT_INCOMPLETE",
                            &format!(
                                "segment {} incomplete: {:?}",
                                completed.segment.sequence, completed.transcript.failure
                            ),
                        )
                        .await;
                }
                let sequence = completed.segment.sequence;
                let segment = completed.segment;
                recovery_buffer.push_transcript(completed.transcript.clone());
                let text = complete.then_some(completed.transcript.text).unwrap_or_default();
                source_segments.insert(sequence, segment.clone());
                let events = blocker_dispatcher.ingest(InputClip::new(
                    sequence,
                    segment.started_at_utc.timestamp_millis(),
                    segment.duration_ms,
                    text,
                ));
                for event in events {
                    match event {
                        DispatchEvent::Blocked { audit, .. } | DispatchEvent::ExpiredNormal { audit } => {
                            acknowledge_terminal_source_segment(
                                &capture_controller,
                                &mut source_segments,
                                audit.sequence,
                            ).await?;
                        }
                        DispatchEvent::FullSet { clips } => {
                            if let Some(ready) = take_ready_dispatch(clips, &mut source_segments) {
                                ready_dispatches.push(ready);
                            }
                        }
                        DispatchEvent::MustSolo { clip } => {
                            if let Some(ready) = take_ready_dispatch(vec![clip], &mut source_segments) {
                                ready_dispatches.push(ready);
                            }
                        }
                    }
                }
                }
                for ready in ready_dispatches {
                    enqueue_ready_dispatch(
                        &capture_controller,
                        &mut audit_writer,
                        &mut pending_dispatch,
                        ready,
                    )
                    .await?;
                }
                session_view.transcript_segments_ready = source_segments.len();
                launch_next_ready_dispatch(
                    &mut pending_dispatch,
                    &mut candidate_watches,
                    &known_routes,
                    &broker,
                    &latest_ticks,
                    analysis_client.clone(),
                    analysis_sender.clone(),
                    &mut health,
                    &mut session_view,
                    rolling_context.as_ref(),
                    &mut analysis_dispatch,
                    &config.paths.ffmpeg_path,
                    &visual_cadence,
                );
                dashboard_dirty = true;
            }
            _ = blocker_timer.tick() => {
                let mut ready_dispatches = Vec::new();
                for event in blocker_dispatcher.advance_to(Utc::now().timestamp_millis()) {
                    match event {
                        DispatchEvent::Blocked { audit, .. } | DispatchEvent::ExpiredNormal { audit } => {
                            acknowledge_terminal_source_segment(
                                &capture_controller,
                                &mut source_segments,
                                audit.sequence,
                            ).await?;
                        }
                        DispatchEvent::FullSet { clips } => {
                            if let Some(ready) = take_ready_dispatch(clips, &mut source_segments) {
                                ready_dispatches.push(ready);
                            }
                        }
                        DispatchEvent::MustSolo { clip } => {
                            if let Some(ready) = take_ready_dispatch(vec![clip], &mut source_segments) {
                                ready_dispatches.push(ready);
                            }
                        }
                    }
                }
                for ready in ready_dispatches {
                    enqueue_ready_dispatch(
                        &capture_controller,
                        &mut audit_writer,
                        &mut pending_dispatch,
                        ready,
                    )
                    .await?;
                }
                launch_next_ready_dispatch(
                    &mut pending_dispatch,
                    &mut candidate_watches,
                    &known_routes,
                    &broker,
                    &latest_ticks,
                    analysis_client.clone(),
                    analysis_sender.clone(),
                    &mut health,
                    &mut session_view,
                    rolling_context.as_ref(),
                    &mut analysis_dispatch,
                    &config.paths.ffmpeg_path,
                    &visual_cadence,
                );
            }
            Some(completed) = analysis_receiver.recv() => {
                if let Some(image) = completed.recovery_image.clone() {
                    recovery_buffer.push_image(image);
                }
                session_view.analysis_latency_ms = Some(completed.latency_ms);
                if let Some(note) = completed.visual_note.as_deref() {
                    append_pipeline_audit(&mut audit_writer, "analysis", "visual_fallback", note)?;
                    runtime_logger
                        .record("WARN", "analysis", "VISUAL_FALLBACK", note)
                        .await;
                }
                let sequence = completed.dispatch.sequence;
                if !analysis_dispatch.owns(sequence) {
                    acknowledge_ready_dispatch(&capture_controller, &completed.dispatch).await?;
                    health.analysis = component(
                        "DEGRADED",
                        format!("discarded out-of-sequence Analysis result for window {sequence}"),
                    );
                    append_pipeline_audit(
                        &mut audit_writer,
                        "analysis",
                        "sequence_mismatch",
                        &format!("received window {sequence} while {:?} was active", analysis_dispatch.active_sequence),
                    )?;
                    runtime_logger
                        .record(
                            "WARN",
                            "analysis",
                            "SEQUENCE_MISMATCH",
                            &format!("discarded out-of-sequence Analysis result for window {sequence}"),
                        )
                        .await;
                } else {
                    match completed.result.clone() {
                        Ok(analysis) => {
                            let envelope = StreamContextEnvelope::for_analysis(
                                stream_url.clone(),
                                sequence,
                                completed.dispatch.ended_at(),
                                analysis.rolling_context.clone(),
                                Utc::now(),
                            );
                            health.analysis = component(
                                "PROCESSING",
                                "validated analysis complete; committing rolling context",
                            );
                            spawn_context_commit_job(
                                stream_context_path.clone(),
                                completed,
                                analysis,
                                envelope,
                                context_commit_sender.clone(),
                            );
                        }
                        Err(error) => {
                            acknowledge_ready_dispatch(&capture_controller, &completed.dispatch).await?;
                            health.analysis = if error.starts_with("OpenAI keys are required") {
                                component("KEYS_REQUIRED", "add one to three OpenAI keys in the dashboard")
                            } else {
                                component("DEGRADED", error.clone())
                            };
                            append_pipeline_audit(&mut audit_writer, "analysis", "error", &error)?;
                            runtime_logger
                                .record("ERROR", "analysis", "ANALYSIS_FAILED", &error)
                                .await;
                            let _ = analysis_dispatch.finish(sequence);
                            launch_next_ready_dispatch(
                                &mut pending_dispatch,
                                &mut candidate_watches,
                                &known_routes,
                                &broker,
                                &latest_ticks,
                                analysis_client.clone(),
                                analysis_sender.clone(),
                                &mut health,
                                &mut session_view,
                                rolling_context.as_ref(),
                                &mut analysis_dispatch,
                                &config.paths.ffmpeg_path,
                                &visual_cadence,
                            );
                        }
                    }
                }
                dashboard_dirty = true;
            }
            Some(committed) = context_commit_receiver.recv() => {
                let sequence = committed.envelope.source_window_sequence;
                if !analysis_dispatch.owns(sequence) {
                    acknowledge_ready_dispatch(&capture_controller, &committed.completed.dispatch).await?;
                    health.analysis = component(
                        "DEGRADED",
                        format!("discarded out-of-sequence context commit for window {sequence}"),
                    );
                    append_pipeline_audit(
                        &mut audit_writer,
                        "stream_context",
                        "sequence_mismatch",
                        &format!("committed window {sequence} while {:?} was active", analysis_dispatch.active_sequence),
                    )?;
                    runtime_logger
                        .record(
                            "WARN",
                            "stream_context",
                            "COMMIT_SEQUENCE_MISMATCH",
                            &format!("discarded out-of-sequence context commit for window {sequence}"),
                        )
                        .await;
                } else {
                    match committed.result {
                        Ok(()) if stream_context_envelope_matches(
                            &committed.envelope,
                            &stream_url,
                            &active_trading_date_ist,
                        ) => {
                            rolling_context = Some(committed.envelope.rolling_context.clone());
                            visual_cadence.record_committed(
                                committed.completed.visual_sent,
                                Utc::now(),
                            );
                            session_view.visual_status = Some(visual_cadence.status());
                            session_view.last_visual_at = visual_cadence
                                .last_visual_at
                                .map(|timestamp| timestamp.to_rfc3339());
                            health.persistence = component(
                                "HEALTHY",
                                format!("rolling context committed through window {sequence}"),
                            );
                            health.analysis = healthy_with_latency(
                                "strict multimodal analysis and context commit complete",
                                committed.completed.latency_ms,
                            );
                            // Observing a quote merely makes a candidate
                            // eligible for analysis. Keep its renewing feed
                            // until an actual paper order is placed; route,
                            // freshness, capital, duplicate, and zero-order
                            // results must remain observable/retriable.
                            // The model-supplied context is provisional.  Keep a
                            // recoverable copy until a paper-broker checkpoint
                            // exists; otherwise a crash between these two writes
                            // could permanently suppress a valid entry call.
                            let provisional_context = rolling_context
                                .as_ref()
                                .expect("committed rolling context was just installed")
                                .clone();
                            let mut finalized_envelope = committed.envelope.clone();
                            let placed_candidate_ids = apply_analysis(
                                committed.analysis,
                                &committed.completed,
                                &instruments,
                                &feed_handle,
                                &mut candidate_watches,
                                &mut known_routes,
                                &mut broker,
                                &mut signals,
                                rolling_context
                                    .as_mut()
                                    .expect("committed rolling context was just installed"),
                            ).await;
                            candidate_watches.retain(|watch| {
                                !candidate_consumed_by_actual_placement(
                                    &watch.route.paper.instrument_id,
                                    &placed_candidate_ids,
                                )
                            });
                            let broker_snapshot_ok = match atomic_write_json_snapshot(
                                &broker_state_path,
                                &broker,
                            ) {
                                Ok(()) => true,
                                Err(error) => {
                                health.persistence = component(
                                    "DEGRADED",
                                    "paper action outcome could not be snapshotted immediately",
                                );
                                append_pipeline_audit(
                                    &mut audit_writer,
                                    "persistence",
                                    "post_action_snapshot_failed",
                                    &format!("{error:#}"),
                                )?;
                                runtime_logger
                                    .record(
                                        "ERROR",
                                        "persistence",
                                        "POST_ACTION_SNAPSHOT_FAILED",
                                        &error.to_string(),
                                    )
                                    .await;
                                false
                            }
                            };
                            let action_context = rolling_context
                                .as_ref()
                                .expect("committed rolling context remains installed")
                                .clone();
                            rolling_context = Some(context_after_broker_checkpoint(
                                provisional_context,
                                action_context,
                                broker_snapshot_ok,
                            ));
                            if !broker_snapshot_ok {
                                // Do not let a final context persist an entry
                                // outcome that has no durable broker evidence.
                                health.analysis = component(
                                    "DEGRADED",
                                    "paper outcome is not durable; entry context remains retriable",
                                );
                            }
                            finalized_envelope.updated_at = Utc::now();
                            finalized_envelope.rolling_context = rolling_context
                                .as_ref()
                                .expect("committed rolling context remains installed")
                                .clone();
                            if broker_snapshot_ok && let Err(error) = atomic_write_json_snapshot(
                                &stream_context_path,
                                &finalized_envelope,
                            ) {
                                health.persistence = component(
                                    "DEGRADED",
                                    "paper actions applied, but final context-outcome persistence failed",
                                );
                                append_pipeline_audit(
                                    &mut audit_writer,
                                    "stream_context",
                                    "outcome_commit_failed",
                                    &format!("{error:#}"),
                                )?;
                                runtime_logger
                                    .record(
                                        "ERROR",
                                        "stream_context",
                                        "OUTCOME_COMMIT_FAILED",
                                        &error.to_string(),
                                    )
                                    .await;
                            }
                            if broker_snapshot_ok && let Some(store) = neon_store.as_ref()
                                && let Err(error) = save_neon_runtime(
                                    store,
                                    &broker,
                                    &stream_url,
                                    &active_trading_date_ist,
                                    rolling_context.as_ref(),
                                    &historical_trades,
                                    &equity_curve,
                                )
                                .await
                            {
                                health.persistence = component(
                                    "DEGRADED",
                                    "paper action applied but atomic Neon checkpoint failed",
                                );
                                runtime_logger
                                    .record(
                                        "ERROR",
                                        "persistence",
                                        "POST_ACTION_CHECKPOINT_FAILED",
                                        &error.to_string(),
                                    )
                                    .await;
                            }
                            reconcile_persistent_subscriptions(
                                &feed_handle,
                                &broker.snapshot(Utc::now().timestamp_millis()),
                                &known_routes,
                                &mut persistent_leases,
                            ).await?;
                            persist_broker_events(&broker, &mut last_event_sequence, &mut audit_writer)?;
                            append_pipeline_audit(
                                &mut audit_writer,
                                "stream_context",
                                "committed",
                                &format!("rolling context advanced through window {sequence}"),
                            )?;
                        }
                        Ok(()) => {
                            health.analysis = component(
                                "DEGRADED",
                                "discarded analysis because its context envelope no longer matches the active stream day",
                            );
                            append_pipeline_audit(
                                &mut audit_writer,
                                "stream_context",
                                "scope_mismatch",
                                &format!("window {sequence} completed outside its stream/date scope"),
                            )?;
                            runtime_logger
                                .record(
                                    "WARN",
                                    "stream_context",
                                    "SCOPE_MISMATCH",
                                    &format!("discarded window {sequence} outside its stream/date scope"),
                                )
                                .await;
                        }
                        Err(error) => {
                            health.persistence = component(
                                "DEGRADED",
                                "rolling context commit failed; trade actions were discarded",
                            );
                            health.analysis = component(
                                "DEGRADED",
                                "analysis discarded because rolling context was not committed",
                            );
                            append_pipeline_audit(
                                &mut audit_writer,
                                "stream_context",
                                "commit_failed",
                                &error,
                            )?;
                            runtime_logger
                                .record("ERROR", "stream_context", "COMMIT_FAILED", &error)
                                .await;
                        }
                    }
                    acknowledge_ready_dispatch(&capture_controller, &committed.completed.dispatch).await?;
                    let _ = analysis_dispatch.finish(sequence);
                    launch_next_ready_dispatch(
                        &mut pending_dispatch,
                        &mut candidate_watches,
                        &known_routes,
                        &broker,
                        &latest_ticks,
                        analysis_client.clone(),
                        analysis_sender.clone(),
                        &mut health,
                        &mut session_view,
                        rolling_context.as_ref(),
                        &mut analysis_dispatch,
                        &config.paths.ffmpeg_path,
                        &visual_cadence,
                    );
                }
                dashboard_dirty = true;
            }
            tick_result = tick_receiver.recv() => {
                match tick_result {
                    Ok(tick) => {
                        if let Some(paper_tick) = market_tick(&tick) {
                            let now_ms = Utc::now().timestamp_millis();
                            let mut policy = moving_stop_from_context;
                            let result = broker.on_tick_with_policy(paper_tick, now_ms, &mut policy);
                            if result.accepted {
                                let at = timestamp_from_ms(tick.received_timestamp_ms);
                                session_view.last_tick_at = Some(at.to_rfc3339());
                                session_view.tick_age_ms = Some(0);
                                health.last_tick_at = session_view.last_tick_at.clone();
                                health.tick_age_ms = Some(0);
                                let source = match tick.source {
                                    TickSource::WebSocket => "WebSocket",
                                    TickSource::RestFallback => "REST fallback",
                                };
                                health.market_feed = component("HEALTHY", format!("fresh {source} tick for {}", tick.instrument.label));
                            }
                            reconcile_persistent_subscriptions(
                                &feed_handle,
                                &broker.snapshot(now_ms),
                                &known_routes,
                                &mut persistent_leases,
                            ).await?;
                            persist_broker_events(&broker, &mut last_event_sequence, &mut audit_writer)?;
                            dashboard_dirty = true;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        let now_ms = Utc::now().timestamp_millis();
                        let (considered, accepted, newest_received_ms) =
                            resync_broker_from_latest(&mut broker, &latest_ticks, now_ms);
                        if let Some(received_ms) = newest_received_ms {
                            let at = timestamp_from_ms(received_ms);
                            session_view.last_tick_at = Some(at.to_rfc3339());
                            session_view.tick_age_ms = Some(
                                now_ms.saturating_sub(received_ms).max(0) as u64,
                            );
                            health.last_tick_at = session_view.last_tick_at.clone();
                            health.tick_age_ms = session_view.tick_age_ms;
                        }
                        health.market_feed = component(
                            if accepted > 0 { "HEALTHY" } else { "DEGRADED" },
                            format!(
                                "tick receiver lagged by {skipped}; resynced {accepted}/{considered} active latest quote(s)"
                            ),
                        );
                        runtime_logger
                            .record(
                                "WARN",
                                "market_feed",
                                "TICK_RECEIVER_LAGGED",
                                &format!(
                                    "tick receiver lagged by {skipped}; resynced {accepted}/{considered} active quote(s)"
                                ),
                            )
                            .await;
                        reconcile_persistent_subscriptions(
                            &feed_handle,
                            &broker.snapshot(now_ms),
                            &known_routes,
                            &mut persistent_leases,
                        ).await?;
                        persist_broker_events(&broker, &mut last_event_sequence, &mut audit_writer)?;
                        dashboard_dirty = true;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        health.market_feed = component("STOPPED", "market tick channel closed");
                        runtime_logger
                            .record(
                                "ERROR",
                                "market_feed",
                                "TICK_CHANNEL_CLOSED",
                                "market tick channel closed",
                            )
                            .await;
                    }
                }
            }
            changed = feed_state.changed() => {
                if changed.is_ok() {
                    let state = *feed_state.borrow();
                    session_view.market_status = format!("{state:?}").to_ascii_uppercase();
                    health.market_feed = feed_health(state);
                    dashboard_dirty = true;
                }
            }
            _ = equity_timer.tick() => {
                append_equity_samples(&broker.snapshot(Utc::now().timestamp_millis()), &mut equity_curve);
                dashboard_dirty = true;
            }
            _ = snapshot_timer.tick() => {
                let now = Utc::now();
                let current_trading_date_ist = now
                    .with_timezone(&Kolkata)
                    .date_naive()
                    .format("%Y-%m-%d")
                    .to_string();
                if current_trading_date_ist != active_trading_date_ist {
                    match broker.start_trading_day_ist(
                        &current_trading_date_ist,
                        now.timestamp_millis(),
                    ) {
                        Ok(_) => {
                            active_trading_date_ist = current_trading_date_ist;
                            rolling_context = None;
                            eod_triggered = false;
                            health.persistence = component(
                                "HEALTHY",
                                format!("session {}", session_dir.display()),
                            );
                            append_pipeline_audit(
                                &mut audit_writer,
                                "paper",
                                "trading_day_started",
                                &format!("IST trading date advanced to {active_trading_date_ist}"),
                            )?;
                            append_pipeline_audit(
                                &mut audit_writer,
                                "stream_context",
                                "reset",
                                "cleared rolling context at the IST trading-date boundary",
                            )?;
                        }
                        Err(error) => {
                            health.persistence = component(
                                "DEGRADED",
                                format!("could not advance IST trading date safely: {error}"),
                            );
                            runtime_logger
                                .record(
                                    "ERROR",
                                    "paper",
                                    "TRADING_DAY_ROLLOVER_FAILED",
                                    &error.to_string(),
                                )
                                .await;
                        }
                    }
                }
                if !eod_triggered && now.with_timezone(&Kolkata).time() >= config.trading.end_of_day_exit_ist {
                    eod_triggered = true;
                    broker.trigger_end_of_day(now.timestamp_millis());
                    runtime_logger
                        .record(
                            "INFO",
                            "paper",
                            "END_OF_DAY_TRIGGERED",
                            "paper broker end-of-day exit handling was triggered",
                        )
                        .await;
                    reconcile_persistent_subscriptions(
                        &feed_handle,
                        &broker.snapshot(now.timestamp_millis()),
                        &known_routes,
                        &mut persistent_leases,
                    ).await?;
                    persist_broker_events(&broker, &mut last_event_sequence, &mut audit_writer)?;
                }
                if capture_active {
                    let renewal_failures = renew_candidate_watches(
                        &feed_handle,
                        &mut candidate_watches,
                    ).await;
                    if renewal_failures > 0 {
                        health.market_feed = component(
                            "DEGRADED",
                            format!("{renewal_failures} candidate quote subscription renewal(s) failed"),
                        );
                    }
                } else {
                    candidate_watches.retain(|watch| {
                        watch.lease.remaining().is_none_or(|remaining| !remaining.is_zero())
                    });
                }
                atomic_write_json_snapshot(&broker_state_path, &broker)?;
                let closed = broker.closed_trade_history();
                if closed.len() != last_closed_count {
                    last_closed_count = closed.len();
                    merge_closed_history(&mut historical_trades, &closed, &broker.snapshot(now.timestamp_millis()));
                    atomic_write_json_snapshot(&history_path, &historical_trades)?;
                }
                dashboard_dirty = true;
            }
            _ = dashboard_timer.tick(), if dashboard_dirty => {
                update_live_ages(&mut session_view, &mut health);
                health.api_keys = analysis_client
                    .key_health()
                    .await
                    .into_iter()
                    .map(|slot| ApiKeyHealthView {
                        provider: "OpenAI / Luna".to_owned(),
                        slot: slot.slot,
                        status: slot.state,
                        successes: slot.successes,
                        failures: slot.failures,
                        cooldown_remaining_ms: slot.cooldown_remaining_ms,
                        last_failure: slot.last_failure,
                        request_limit: slot.rate_limit.as_ref().and_then(|rate| rate.request_limit),
                        request_remaining: slot.rate_limit.as_ref().and_then(|rate| rate.request_remaining),
                        request_reset_ms: slot.rate_limit.as_ref().and_then(|rate| rate.request_reset_ms),
                        token_limit: slot.rate_limit.as_ref().and_then(|rate| rate.token_limit),
                        token_remaining: slot.rate_limit.as_ref().and_then(|rate| rate.token_remaining),
                        token_reset_ms: slot.rate_limit.as_ref().and_then(|rate| rate.token_reset_ms),
                        retry_after_ms: slot.rate_limit.as_ref().and_then(|rate| rate.retry_after_ms),
                        observed_day_ist: (!slot.daily_usage.day_ist.is_empty())
                            .then_some(slot.daily_usage.day_ist),
                        observed_daily_requests: slot.daily_usage.request_count,
                        observed_daily_input_tokens: slot.daily_usage.input_tokens,
                        observed_daily_output_tokens: slot.daily_usage.output_tokens,
                        observed_daily_total_tokens: slot.daily_usage.total_tokens,
                    })
                    .chain(stt_client.key_health().await.into_iter().map(|slot| {
                        ApiKeyHealthView {
                            provider: "ElevenLabs".to_owned(),
                            slot: slot.slot,
                            status: slot.state,
                            successes: slot.successes,
                            failures: slot.failures,
                            cooldown_remaining_ms: slot.cooldown_remaining_ms,
                            last_failure: slot.last_failure,
                            ..ApiKeyHealthView::default()
                        }
                    }))
                    .collect();
                health.overall = overall_health(&health);
                let state = dashboard_state(
                    &broker.snapshot(Utc::now().timestamp_millis()),
                    session_view.clone(),
                    health.clone(),
                    signals.clone(),
                    equity_curve.clone(),
                    historical_trades.clone(),
                    config.trading.charge_per_fill_rupees,
                );
                dashboard_handle.replace(state).await;
                dashboard_dirty = false;
            }
        }
    }

    persist_broker_events(&broker, &mut last_event_sequence, &mut audit_writer)?;
    let final_snapshot = broker.snapshot(Utc::now().timestamp_millis());
    let final_closed = broker.closed_trade_history();
    merge_closed_history(&mut historical_trades, &final_closed, &final_snapshot);
    atomic_write_json_snapshot(&history_path, &historical_trades)?;
    atomic_write_json_snapshot(&broker_state_path, &broker)?;
    if let Some(store) = neon_store.as_ref() {
        save_neon_runtime(
            store,
            &broker,
            &stream_url,
            &active_trading_date_ist,
            rolling_context.as_ref(),
            &historical_trades,
            &equity_curve,
        )
        .await
        .context("final Neon checkpoint failed")?;
    }
    runtime_logger
        .record(
            "INFO",
            "runtime",
            "PAPER_SESSION_STOPPED",
            "paper-only session completed its final persistence checkpoint",
        )
        .await;
    audit_writer.finish()?;
    drop(candidate_watches);
    drop(persistent_leases);
    let _ = capture.shutdown().await;
    let _ = feed_runtime.shutdown().await;
    if let Some(task) = dashboard_task {
        task.abort();
    }
    Ok(())
}

fn apply_live_start_status(state: &mut DashboardState, session: SessionView, health: HealthView) {
    let discovery = (!state.health.youtube_discovery.status.is_empty())
        .then(|| state.health.youtube_discovery.clone());
    state.session = session;
    state.health = health;
    if let Some(discovery) = discovery {
        state.health.youtube_discovery = discovery;
    }
}

fn seed_shared_discovery_health(runtime: &mut HealthView, shared: &HealthView) {
    if !shared.youtube_discovery.status.is_empty() {
        runtime.youtube_discovery = shared.youtube_discovery.clone();
    }
}

async fn save_neon_runtime(
    store: &NeonStore,
    broker: &PaperBroker,
    stream_url: &str,
    trading_date_ist: &str,
    rolling_context: Option<&analysis::RollingContext>,
    history: &[HistoryTrade],
    equity_curve: &[EquityPoint],
) -> Result<()> {
    let state = DurablePaperState {
        broker: broker.clone(),
        stream_url: stream_url.to_owned(),
        trading_date_ist: trading_date_ist.to_owned(),
        rolling_context: rolling_context.cloned(),
        history: history.to_vec(),
        equity_curve: equity_curve.to_vec(),
        updated_at: Utc::now(),
    };
    store.save_runtime_state("paper-primary", &state).await
}

async fn sync_neon_rows(
    store: &NeonStore,
    trading_date_ist: &str,
    dashboard: &DashboardState,
) -> Result<()> {
    let trading_date = NaiveDate::parse_from_str(trading_date_ist, "%Y-%m-%d")
        .context("invalid active IST trading date")?;
    for account in &dashboard.accounts {
        store
            .upsert_daily_account(
                trading_date,
                &account.account_id,
                &account.strategy,
                account,
            )
            .await?;
    }
    for trade in &dashboard.history {
        let closed_at = DateTime::parse_from_rfc3339(&trade.closed_at)
            .context("closed trade has invalid timestamp")?
            .with_timezone(&Utc);
        let trade_date = closed_at.with_timezone(&Kolkata).date_naive();
        store
            .upsert_trade(
                &trade.trade_id,
                trade_date,
                &trade.account_id,
                &trade.strategy,
                closed_at,
                trade,
            )
            .await?;
    }
    Ok(())
}

fn spawn_stt_job(
    client: ElevenLabsSttClient,
    sender: mpsc::Sender<SttCompleted>,
    segment: MediaSegment,
) {
    tokio::spawn(async move {
        let started = Instant::now();
        let start_sec = segment.sequence as f64 * SEGMENT_SECONDS as f64;
        let input = SegmentInput::new(
            segment.sequence,
            start_sec,
            start_sec + SEGMENT_SECONDS as f64,
            segment.path.clone(),
        );
        let transcript = client.transcribe_segment(input).await;
        let completed = SttCompleted {
            segment,
            transcript,
            latency_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        };
        let _ = sender.send(completed).await;
    });
}

fn take_ready_dispatch(
    clips: Vec<RetainedClip>,
    source_segments: &mut BTreeMap<u64, MediaSegment>,
) -> Option<ReadyDispatch> {
    if !clips
        .iter()
        .all(|clip| source_segments.contains_key(&clip.sequence))
    {
        return None;
    }
    let mut segments = Vec::with_capacity(clips.len());
    for clip in &clips {
        segments.push(
            source_segments
                .remove(&clip.sequence)
                .expect("source-segment presence was verified before transfer"),
        );
    }
    let sequence = clips.iter().map(|clip| clip.sequence).max()?;
    Some(ReadyDispatch {
        sequence,
        clips,
        segments,
    })
}

struct ReadyDispatchEnqueue {
    superseded: Option<ReadyDispatch>,
    incoming: ReadyDispatch,
}

/// Remove the old pending dispatch before its terminal release. The caller
/// must ACK/audit `superseded` and only then commit `incoming` as pending.
fn begin_ready_dispatch_enqueue(
    pending: &mut Option<ReadyDispatch>,
    incoming: ReadyDispatch,
) -> ReadyDispatchEnqueue {
    ReadyDispatchEnqueue {
        superseded: pending.take(),
        incoming,
    }
}

impl ReadyDispatchEnqueue {
    fn commit(self, pending: &mut Option<ReadyDispatch>) {
        debug_assert!(pending.is_none());
        *pending = Some(self.incoming);
    }
}

/// Retain only the newest unsent dispatch. A superseded dispatch is terminally
/// ACKed and audited before the replacement becomes visible to the launcher.
/// This applies for every blocker event regardless of active model state.
async fn enqueue_ready_dispatch(
    controller: &crate::capture::CaptureController,
    audit_writer: &mut JsonlEventWriter,
    pending: &mut Option<ReadyDispatch>,
    incoming: ReadyDispatch,
) -> Result<()> {
    let enqueue = begin_ready_dispatch_enqueue(pending, incoming);
    if let Some(stale) = enqueue.superseded.as_ref() {
        acknowledge_ready_dispatch(controller, stale).await?;
        append_pipeline_audit(
            audit_writer,
            "analysis",
            "SUPERSEDED",
            &format!("discarded unsent dispatch {}", stale.sequence),
        )?;
    }
    enqueue.commit(pending);
    Ok(())
}

async fn acknowledge_ready_dispatch(
    controller: &crate::capture::CaptureController,
    dispatch: &ReadyDispatch,
) -> Result<()> {
    for segment in &dispatch.segments {
        controller.acknowledge_segment(segment.id.clone()).await?;
    }
    Ok(())
}

async fn acknowledge_terminal_source_segment(
    controller: &crate::capture::CaptureController,
    source_segments: &mut BTreeMap<u64, MediaSegment>,
    sequence: u64,
) -> Result<()> {
    if let Some(segment) = source_segments.remove(&sequence) {
        controller.acknowledge_segment(segment.id).await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn launch_next_ready_dispatch(
    pending: &mut Option<ReadyDispatch>,
    candidates: &mut Vec<CandidateWatch>,
    known_routes: &HashMap<String, RoutedContract>,
    broker: &PaperBroker,
    latest_ticks: &LatestTicks,
    client: Arc<AnalysisClient>,
    sender: mpsc::Sender<AnalysisCompleted>,
    health: &mut HealthView,
    session: &mut SessionView,
    rolling_context: Option<&analysis::RollingContext>,
    dispatch: &mut AnalysisDispatchState,
    ffmpeg_path: &Path,
    visual_cadence: &VisualCadence,
) {
    if dispatch.active_sequence.is_some() {
        return;
    }

    // There is never a FIFO backlog: while one call is active, the runtime
    // retains only the latest fully selected dispatch. This prevents stale
    // scalp decisions from accumulating behind a slow provider response.
    let Some(ready) = pending.take() else {
        return;
    };
    let sequence = ready.sequence;
    if !dispatch.try_begin(sequence) {
        *pending = Some(ready);
        return;
    }
    let sent_at = Utc::now();
    let input = build_analysis_input(
        &ready,
        sent_at,
        candidates,
        known_routes,
        broker,
        latest_ticks,
        rolling_context,
    );
    let transcript_excerpt = ready
        .clips
        .iter()
        .map(|chunk| chunk.text.trim())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    session.last_prompt_at = Some(sent_at.to_rfc3339());
    session.clip_age_ms = Some(input.clip.data_age_ms);
    session.transcript_segments_ready = 0;
    let visual_due = visual_cadence.visual_due();
    session.visual_status = Some(if visual_due {
        "frame extraction scheduled for this analysis".to_owned()
    } else {
        visual_cadence.status()
    });
    health.analysis = component(
        "PROCESSING",
        format!(
            "analyzing {} selected 3-second source segments through {sequence}",
            ready.clips.len()
        ),
    );
    let ffmpeg_path = ffmpeg_path.to_path_buf();
    tokio::spawn(async move {
        let started = Instant::now();
        let (jpeg, visual_sent, visual_note) = if visual_due {
            match extract_latest_selected_jpeg(&ffmpeg_path, &ready.segments).await {
                Ok(jpeg) => (Some(jpeg), true, None),
                Err(_) => (
                    None,
                    false,
                    Some("original frame unavailable; continuing text-only and retaining visual due state".to_owned()),
                ),
            }
        } else {
            (None, false, None)
        };
        let result = client
            .analyze(&input, jpeg.as_deref())
            .await
            .map_err(|error| format!("{error:#}"));
        let recovery_image = jpeg.map(|jpeg| RecoveryImage {
            source_sequence: ready
                .segments
                .iter()
                .map(|segment| segment.sequence)
                .max()
                .unwrap_or_default(),
            jpeg,
        });
        let completed = AnalysisCompleted {
            dispatch: ready,
            input,
            transcript_excerpt,
            latency_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            visual_sent,
            visual_note,
            recovery_image,
            result,
        };
        let _ = sender.send(completed).await;
    });
}

fn spawn_context_commit_job(
    path: std::path::PathBuf,
    completed: AnalysisCompleted,
    analysis: ValidatedAnalysis,
    envelope: StreamContextEnvelope,
    sender: mpsc::Sender<ContextCommitCompleted>,
) {
    tokio::spawn(async move {
        let snapshot = envelope.clone();
        let result = tokio::task::spawn_blocking(move || {
            let encoded_size = serde_json::to_vec(&snapshot)
                .map_err(|error| format!("could not size rolling-context envelope: {error}"))?
                .len() as u64;
            if encoded_size > MAX_STREAM_CONTEXT_FILE_BYTES {
                return Err(format!(
                    "rolling-context envelope is {encoded_size} bytes, above the {MAX_STREAM_CONTEXT_FILE_BYTES}-byte safety limit"
                ));
            }
            atomic_write_json_snapshot(&path, &snapshot)
                .map_err(|error| format!("atomic rolling-context commit failed: {error:#}"))
        })
        .await
        .map_err(|error| format!("rolling-context commit task failed: {error}"))
        .and_then(|result| result);
        let _ = sender
            .send(ContextCommitCompleted {
                completed,
                analysis,
                envelope,
                result,
            })
            .await;
    });
}

fn candidate_observation_is_complete(watch: &CandidateWatch, latest: &LatestTicks) -> bool {
    watch.watched_since.elapsed() >= Duration::from_secs(MIN_CANDIDATE_OBSERVATION_SECONDS)
        && latest
            .get(&watch.route.instrument)
            .is_some_and(|tick| tick.received_timestamp_ms >= watch.watched_since_timestamp_ms)
}

fn candidate_consumed_by_actual_placement(
    instrument_id: &str,
    actual_placement_instruments: &HashSet<String>,
) -> bool {
    actual_placement_instruments.contains(instrument_id)
}

fn candidate_lease_needs_renewal(remaining: Option<Duration>) -> bool {
    remaining
        .is_some_and(|remaining| remaining <= Duration::from_secs(CANDIDATE_RENEW_AHEAD_SECONDS))
}

async fn renew_candidate_watches(
    handle: &MarketFeedHandle,
    candidates: &mut [CandidateWatch],
) -> usize {
    let mut failures = 0;
    for watch in candidates {
        if !candidate_lease_needs_renewal(watch.lease.remaining()) {
            continue;
        }
        match handle
            .subscribe(
                watch.route.instrument.clone(),
                SubscriptionReason::CandidateWatch,
            )
            .await
        {
            Ok(renewed) => watch.lease = renewed,
            Err(_) => failures += 1,
        }
    }
    failures
}

fn build_analysis_input(
    dispatch: &ReadyDispatch,
    sent_at: DateTime<Utc>,
    candidates: &[CandidateWatch],
    known_routes: &HashMap<String, RoutedContract>,
    broker: &PaperBroker,
    latest: &LatestTicks,
    rolling_context: Option<&analysis::RollingContext>,
) -> AnalysisInput {
    let transcripts = dispatch
        .segments
        .iter()
        .zip(dispatch.clips.iter())
        .map(|(segment, clip)| analysis::TranscriptChunk {
            source_sequence: segment.sequence,
            started_at: segment.started_at_utc,
            ended_at: segment.ended_at_utc,
            text: clip.text.clone(),
            complete: true,
        })
        .collect::<Vec<_>>();

    let broker_snapshot = broker.snapshot(sent_at.timestamp_millis());
    let mut active_ids = broker_snapshot
        .shadows
        .iter()
        .flat_map(|shadow| shadow.accounts.iter())
        .flat_map(|account| {
            account
                .pending_entries
                .iter()
                .map(|order| order.contract.instrument_id.trim().to_ascii_uppercase())
                .chain(account.open_positions.iter().map(|position| {
                    position
                        .position
                        .contract
                        .instrument_id
                        .trim()
                        .to_ascii_uppercase()
                }))
        })
        .collect::<HashSet<_>>();
    for candidate in candidates {
        if candidate_observation_is_complete(candidate, latest) {
            active_ids.insert(candidate.route.paper.instrument_id.clone());
        }
    }

    let watched_options = active_ids
        .iter()
        .filter_map(|instrument_id| known_routes.get(instrument_id))
        .map(|route| {
            let tick = latest.get(&route.instrument);
            let age = tick.as_ref().map(Tick::age);
            let remaining = candidates
                .iter()
                .filter(|candidate| {
                    candidate.route.paper.instrument_id == route.paper.instrument_id
                })
                .filter_map(|candidate| candidate.lease.remaining())
                .max()
                .unwrap_or_default();
            WatchedOptionSnapshot {
                contract: route.analysis.clone(),
                price: PriceSnapshot {
                    ltp: tick.as_ref().map(|tick| tick.ltp),
                    observed_at: tick
                        .as_ref()
                        .map(|tick| timestamp_from_ms(tick.received_timestamp_ms)),
                    age_ms: age.map(|age| age.as_millis().min(u128::from(u64::MAX)) as u64),
                    fresh: tick
                        .as_ref()
                        .is_some_and(|tick| tick.is_fresh(Duration::from_secs(5))),
                },
                watch_remaining_ms: remaining.as_millis().min(u128::from(u64::MAX)) as u64,
            }
        })
        .collect();

    AnalysisInput {
        clip: ClipWindow {
            started_at: dispatch.started_at(),
            ended_at: dispatch.ended_at(),
            sent_at,
            data_age_ms: age_ms(dispatch.ended_at(), sent_at),
            complete: matches!(dispatch.segments.len(), 1 | 4)
                && dispatch
                    .segments
                    .iter()
                    .all(|segment| (2_500..=3_500).contains(&segment.duration_ms)),
        },
        transcripts,
        watched_options,
        open_trades: open_trade_snapshots(&broker_snapshot, known_routes, latest),
        rolling_context: rolling_context.cloned(),
    }
}

fn open_trade_snapshots(
    snapshot: &PaperBrokerSnapshot,
    known_routes: &HashMap<String, RoutedContract>,
    latest: &LatestTicks,
) -> Vec<analysis::OpenTradeSnapshot> {
    let Some(llm_shadow) = snapshot
        .shadows
        .iter()
        .find(|shadow| shadow.mode == ShadowMode::LlmExit)
    else {
        return Vec::new();
    };
    let mut grouped = BTreeMap::<String, Vec<&paper::PositionSnapshot>>::new();
    for account in &llm_shadow.accounts {
        for position in &account.open_positions {
            grouped
                .entry(position.position.setup_id.clone())
                .or_default()
                .push(position);
        }
    }
    grouped
        .into_iter()
        .filter_map(|(setup_id, positions)| {
            let first = positions.first()?.position.clone();
            let route = known_routes.get(&first.contract.instrument_id)?;
            let quantity = positions
                .iter()
                .map(|position| position.position.quantity)
                .sum();
            let unrealized_paise: i64 = positions
                .iter()
                .map(|position| position.net_unrealized_pnl_paise)
                .sum();
            let tick = latest.get(&route.instrument);
            let tick_age = tick.as_ref().map(Tick::age);
            Some(analysis::OpenTradeSnapshot {
                trade_id: setup_id,
                contract: route.analysis.clone(),
                quantity,
                entry_price: paise_to_rupees(first.entry_price_paise),
                price: PriceSnapshot {
                    ltp: tick.as_ref().map(|tick| tick.ltp),
                    observed_at: tick
                        .as_ref()
                        .map(|tick| timestamp_from_ms(tick.received_timestamp_ms)),
                    age_ms: tick_age.map(|age| age.as_millis().min(u128::from(u64::MAX)) as u64),
                    fresh: tick
                        .as_ref()
                        .is_some_and(|tick| tick.is_fresh(Duration::from_secs(5))),
                },
                unrealized_pnl: paise_to_rupees(unrealized_paise),
                hard_sl: paise_to_rupees(first.levels.hard_sl_paise),
                effective_sl: paise_to_rupees(first.effective_sl_paise),
                t1: paise_to_rupees(first.levels.t1_paise),
                t2: first.levels.t2_paise.map(paise_to_rupees),
                trailing_phase: 0,
                exit_mode: ExitMode::Llm,
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn apply_analysis(
    analysis: ValidatedAnalysis,
    completed: &AnalysisCompleted,
    instruments: &[InstrumentRow],
    feed_handle: &MarketFeedHandle,
    candidates: &mut Vec<CandidateWatch>,
    known_routes: &mut HashMap<String, RoutedContract>,
    broker: &mut PaperBroker,
    signals: &mut Vec<SignalView>,
    rolling_context: &mut analysis::RollingContext,
) -> HashSet<String> {
    let received_at = Utc::now();
    let bias = format!("{:?}", analysis.market_bias.direction).to_ascii_uppercase();
    let freshness = format!("{:?}", analysis.freshness.status).to_ascii_uppercase();
    let mut actual_placement_instruments = HashSet::new();

    for rejected in analysis.rejected_actions {
        push_signal(
            signals,
            signal_view(
                &rejected.action,
                completed,
                false,
                String::new(),
                &bias,
                &freshness,
                format!("Rust semantic rejection: {}", rejected.reason),
                None,
            ),
        );
    }

    for action in analysis.actions {
        if let Some(reason) = executable_action_freshness_issue(&action, completed, received_at) {
            reconcile_entry_application(rolling_context, &action, false);
            record_authoritative_outcome(
                rolling_context,
                &action,
                false,
                None,
                &format!("Runtime action rejection: {reason}"),
                received_at,
            );
            push_signal(
                signals,
                signal_view(
                    &action,
                    completed,
                    false,
                    String::new(),
                    &bias,
                    &freshness,
                    format!("Runtime action rejection: {reason}"),
                    None,
                ),
            );
            continue;
        }
        let route_result = match action.contract.as_ref() {
            Some(contract) => resolve_route(instruments, contract),
            None if action.action == ActionKind::Ignore => Err(anyhow!("IGNORE has no contract")),
            None => Err(anyhow!("action has no resolvable contract")),
        };
        let mut accepted = true;
        let mut setup_id = action.trade_id.clone().unwrap_or_default();
        let mut decision = action.rationale.clone();
        let route_for_signal;

        match action.action {
            ActionKind::Ignore => {
                route_for_signal = None;
            }
            _ => match route_result {
                Ok(route) => {
                    route_for_signal = Some(route.clone());
                    known_routes.insert(route.paper.instrument_id.clone(), route.clone());
                    match action.action {
                        ActionKind::Watch => {
                            let watched_since = candidates
                                .iter()
                                .find(|candidate| {
                                    candidate.route.paper.instrument_id == route.paper.instrument_id
                                })
                                .map(|candidate| candidate.watched_since)
                                .unwrap_or_else(Instant::now);
                            let watched_since_timestamp_ms = candidates
                                .iter()
                                .find(|candidate| {
                                    candidate.route.paper.instrument_id == route.paper.instrument_id
                                })
                                .map(|candidate| candidate.watched_since_timestamp_ms)
                                .unwrap_or_else(|| Utc::now().timestamp_millis());
                            match feed_handle
                                .subscribe(
                                    route.instrument.clone(),
                                    SubscriptionReason::CandidateWatch,
                                )
                                .await
                            {
                                Ok(lease) => {
                                    candidates.retain(|candidate| {
                                        candidate.route.paper.instrument_id
                                            != route.paper.instrument_id
                                    });
                                    candidates.push(CandidateWatch {
                                        route,
                                        lease,
                                        watched_since,
                                        watched_since_timestamp_ms,
                                    });
                                    decision.push_str(
                                        "; candidate quote will be renewed until a later prompt after at least 10 seconds",
                                    );
                                }
                                Err(error) => {
                                    accepted = false;
                                    decision = format!("candidate subscription failed: {error:#}");
                                }
                            }
                        }
                        ActionKind::PlaceEntry => {
                            match action_to_setup(&action, &route, completed, received_at) {
                                Ok(mut setup) => {
                                    setup.ensure_stable_id();
                                    setup_id = setup.setup_id.clone();
                                    let placement =
                                        broker.place_setup(setup, received_at.timestamp_millis());
                                    accepted = placement_effectively_accepted(
                                        placement.status,
                                        placement.orders_placed,
                                    );
                                    if accepted {
                                        actual_placement_instruments
                                            .insert(route.paper.instrument_id.clone());
                                    }
                                    decision = placement.rejection_reason.unwrap_or_else(|| {
                                        if placement.orders_placed > 0 {
                                            format!(
                                                "{:?}; {} shadow order(s) placed",
                                                placement.status, placement.orders_placed
                                            )
                                        } else {
                                            format!(
                                                "{:?}; no new shadow order was placed",
                                                placement.status
                                            )
                                        }
                                    });
                                }
                                Err(error) => {
                                    accepted = false;
                                    decision = format!("entry conversion rejected: {error:#}");
                                }
                            }
                        }
                        ActionKind::CancelEntry => {
                            if setup_id.is_empty() {
                                setup_id = find_setup_for_contract(broker, &route.paper)
                                    .unwrap_or_default();
                            }
                            let events = broker
                                .cancel_pending_setup(&setup_id, received_at.timestamp_millis());
                            accepted = !events.is_empty();
                            if !accepted {
                                decision = "no matching pending entry exists".to_owned();
                            }
                        }
                        ActionKind::UpdateLevels => {
                            if setup_id.is_empty() {
                                setup_id = find_setup_for_contract(broker, &route.paper)
                                    .unwrap_or_default();
                            }
                            match action_levels(&action) {
                                Ok(levels) => {
                                    let events = broker.update_open_levels(
                                        &setup_id,
                                        levels,
                                        received_at.timestamp_millis(),
                                    );
                                    accepted = events.iter().any(|event| {
                                        event.event_type == paper::EventType::LevelsUpdated
                                    });
                                    decision = events
                                        .last()
                                        .map(|event| event.message.clone())
                                        .unwrap_or_else(|| {
                                            "level update produced no event".to_owned()
                                        });
                                }
                                Err(error) => {
                                    accepted = false;
                                    decision = format!("level update rejected: {error:#}");
                                }
                            }
                        }
                        ActionKind::Exit => {
                            if setup_id.is_empty() {
                                setup_id = find_setup_for_contract(broker, &route.paper)
                                    .unwrap_or_default();
                            }
                            let events =
                                broker.request_llm_exit(&setup_id, received_at.timestamp_millis());
                            accepted = events
                                .iter()
                                .any(|event| event.event_type == paper::EventType::LlmExitQueued);
                            decision = events
                                .last()
                                .map(|event| event.message.clone())
                                .unwrap_or_else(|| "LLM exit produced no event".to_owned());
                        }
                        ActionKind::Hold => {
                            decision.push_str("; no paper state mutation");
                        }
                        ActionKind::Ignore => unreachable!(),
                    }
                }
                Err(error) => {
                    route_for_signal = None;
                    accepted = false;
                    decision = format!("instrument routing rejected: {error:#}");
                }
            },
        }

        record_authoritative_outcome(
            rolling_context,
            &action,
            accepted,
            (!setup_id.trim().is_empty()).then_some(setup_id.clone()),
            &decision,
            received_at,
        );
        push_signal(
            signals,
            signal_view(
                &action,
                completed,
                accepted,
                setup_id,
                &bias,
                &freshness,
                decision,
                route_for_signal.as_ref(),
            ),
        );
        reconcile_entry_application(rolling_context, &action, accepted);
    }
    actual_placement_instruments
}

fn record_authoritative_outcome(
    context: &mut analysis::RollingContext,
    action: &TradeAction,
    accepted: bool,
    setup_id: Option<String>,
    detail: &str,
    occurred_at: DateTime<Utc>,
) {
    let status = if accepted { "APPLIED" } else { "REJECTED" };
    context
        .authoritative_outcomes
        .push(analysis::AuthoritativeOutcome {
            action: action.action,
            episode_id: action.episode_id.clone(),
            event_id: action.event_id.clone(),
            setup_id,
            status: status.to_owned(),
            detail: detail.chars().take(280).collect(),
            occurred_at: occurred_at.to_rfc3339(),
        });
    const MAX_AUTHORITATIVE_OUTCOMES: usize = 24;
    if context.authoritative_outcomes.len() > MAX_AUTHORITATIVE_OUTCOMES {
        let drop_count = context.authoritative_outcomes.len() - MAX_AUTHORITATIVE_OUTCOMES;
        context.authoritative_outcomes.drain(..drop_count);
    }
}

fn reconcile_entry_application(
    context: &mut analysis::RollingContext,
    action: &TradeAction,
    placed: bool,
) {
    if action.action != ActionKind::PlaceEntry {
        return;
    }
    let episode_index = action
        .episode_id
        .as_deref()
        .and_then(|episode_id| {
            context
                .episodes
                .iter()
                .position(|episode| episode.episode_id == episode_id)
        })
        .or_else(|| {
            let event_id = action.event_id.as_deref()?;
            context
                .episodes
                .iter()
                .position(|episode| episode.entry_event_id.as_deref() == Some(event_id))
        });
    let Some(episode_index) = episode_index else {
        return;
    };
    if placed {
        let episode = &mut context.episodes[episode_index];
        episode.entry_event_id = action.event_id.clone();
        episode.status = analysis::TradeEpisodeStatus::EntryCalled;
    } else {
        let episode_id = context.episodes[episode_index].episode_id.clone();
        let entry_event_id = context.episodes[episode_index].entry_event_id.clone();
        let current_entry_is_proven = entry_event_id.as_deref().is_some_and(|event_id| {
            context.authoritative_outcomes.iter().any(|outcome| {
                outcome.action == ActionKind::PlaceEntry
                    && outcome.status == "APPLIED"
                    && outcome.episode_id.as_deref() == Some(episode_id.as_str())
                    && outcome.event_id.as_deref() == Some(event_id)
            })
        });
        if current_entry_is_proven {
            return;
        }
        // A rejected proposal must never leave an ENTRY_CALLED/event marker
        // behind merely because a model supplied a different id than the
        // tentative context value. A later fresh call stays retriable.
        let episode = &mut context.episodes[episode_index];
        episode.entry_event_id = None;
        if episode.status == analysis::TradeEpisodeStatus::EntryCalled {
            episode.status = analysis::TradeEpisodeStatus::ConditionalEntry;
        }
    }
}

fn context_after_broker_checkpoint(
    provisional: analysis::RollingContext,
    action_outcome: analysis::RollingContext,
    broker_snapshot_ok: bool,
) -> analysis::RollingContext {
    if broker_snapshot_ok {
        action_outcome
    } else {
        provisional
    }
}

/// A rolling context is supplementary memory, never proof of an order. On
/// restart only retain a placement outcome when the selected broker snapshot
/// contains the corresponding accepted setup.
fn reconcile_restored_context_with_broker(
    context: &mut analysis::RollingContext,
    broker: &PaperBroker,
) {
    context.authoritative_outcomes.retain(|outcome| {
        outcome.action != ActionKind::PlaceEntry
            || outcome.status != "APPLIED"
            || outcome
                .setup_id
                .as_deref()
                .is_some_and(|setup_id| broker.contains_accepted_setup(setup_id))
    });

    let runtime_outcomes = context.authoritative_outcomes.clone();
    for episode in &mut context.episodes {
        let is_entry_marked = episode.status == analysis::TradeEpisodeStatus::EntryCalled
            || episode.entry_event_id.is_some();
        if !is_entry_marked {
            continue;
        }
        let proven = episode.entry_event_id.as_deref().is_some_and(|event_id| {
            runtime_outcomes.iter().any(|outcome| {
                outcome.action == ActionKind::PlaceEntry
                    && outcome.status == "APPLIED"
                    && outcome.episode_id.as_deref() == Some(episode.episode_id.as_str())
                    && outcome.event_id.as_deref() == Some(event_id)
                    && outcome
                        .setup_id
                        .as_deref()
                        .is_some_and(|setup_id| broker.contains_accepted_setup(setup_id))
            })
        });
        if !proven {
            episode.entry_event_id = None;
            if matches!(
                episode.status,
                analysis::TradeEpisodeStatus::EntryCalled
                    | analysis::TradeEpisodeStatus::Open
                    | analysis::TradeEpisodeStatus::Managing
            ) {
                episode.status = analysis::TradeEpisodeStatus::ConditionalEntry;
            }
        }
    }
}

fn executable_action_freshness_issue(
    action: &TradeAction,
    completed: &AnalysisCompleted,
    received_at: DateTime<Utc>,
) -> Option<String> {
    if !action.action.is_trade_command() {
        return None;
    }
    if completed.input.clip.started_at != completed.dispatch.started_at()
        || completed.input.clip.ended_at != completed.dispatch.ended_at()
    {
        return Some("analysis input does not match its source media window".to_owned());
    }
    let clip_age_ms = received_at
        .signed_duration_since(completed.dispatch.ended_at())
        .num_milliseconds();
    if !(0..=MAX_EXECUTABLE_SIGNAL_AGE_MS).contains(&clip_age_ms) {
        return Some(format!(
            "source clip is {clip_age_ms} ms old; maximum is {MAX_EXECUTABLE_SIGNAL_AGE_MS} ms"
        ));
    }
    let Some(latest_evidence) = action.evidence_timestamps.iter().max_by(|left, right| {
        left.seconds_from_clip_start
            .total_cmp(&right.seconds_from_clip_start)
    }) else {
        return Some("executable action has no evidence in the current clip".to_owned());
    };
    let evidence_at = completed.dispatch.started_at()
        + chrono::Duration::milliseconds(
            (latest_evidence.seconds_from_clip_start * 1_000.0).round() as i64,
        );
    let evidence_age_ms = received_at
        .signed_duration_since(evidence_at)
        .num_milliseconds();
    if !(0..=MAX_EXECUTABLE_SIGNAL_AGE_MS).contains(&evidence_age_ms) {
        return Some(format!(
            "newest action evidence is {evidence_age_ms} ms old; maximum is {MAX_EXECUTABLE_SIGNAL_AGE_MS} ms"
        ));
    }
    None
}

fn placement_effectively_accepted(status: PlacementStatus, orders_placed: usize) -> bool {
    status != PlacementStatus::Rejected && orders_placed > 0
}

fn action_to_setup(
    action: &TradeAction,
    route: &RoutedContract,
    completed: &AnalysisCompleted,
    received_at: DateTime<Utc>,
) -> Result<TradeSetup> {
    let levels = entry_levels_with_fallback(action, route.paper.underlying)?;
    let evidence_timestamp_ms = action
        .evidence_timestamps
        .first()
        .map(|evidence| {
            completed.dispatch.started_at().timestamp_millis()
                + (evidence.seconds_from_clip_start * 1_000.0).round() as i64
        })
        .unwrap_or_else(|| completed.dispatch.ended_at().timestamp_millis());
    Ok(TradeSetup {
        setup_id: action.trade_id.clone().unwrap_or_default(),
        contract: route.paper.clone(),
        side: TradeSide::Buy,
        levels,
        evidence_timestamp_ms,
        received_timestamp_ms: received_at.timestamp_millis(),
    })
}

fn action_levels(action: &TradeAction) -> Result<PaperLevels> {
    let levels = action
        .levels
        .as_ref()
        .ok_or_else(|| anyhow!("action has no levels"))?;
    Ok(PaperLevels {
        entry_paise: points_to_paise(levels.entry.ok_or_else(|| anyhow!("entry missing"))?)?,
        hard_sl_paise: points_to_paise(levels.hard_sl.ok_or_else(|| anyhow!("hard SL missing"))?)?,
        t1_paise: points_to_paise(levels.t1.ok_or_else(|| anyhow!("T1 missing"))?)?,
        t2_paise: levels.t2.map(points_to_paise).transpose()?,
    })
}

/// Uses explicit streamer levels whenever they exist. The fixed fallback is
/// only for a current, validated entry whose episode contains no explicit SL
/// or T1: NIFTY is entry-8 / entry+12 and SENSEX is entry-12 / entry+15.
fn entry_levels_with_fallback(
    action: &TradeAction,
    underlying: PaperUnderlying,
) -> Result<PaperLevels> {
    let levels = action
        .levels
        .as_ref()
        .ok_or_else(|| anyhow!("entry action has no levels"))?;
    let entry = levels.entry.ok_or_else(|| anyhow!("entry missing"))?;
    let (fallback_sl_points, fallback_t1_points) = match underlying {
        PaperUnderlying::Nifty => (8.0, 12.0),
        PaperUnderlying::Sensex => (12.0, 15.0),
    };
    let hard_sl = levels.hard_sl.unwrap_or(entry - fallback_sl_points);
    let t1 = levels.t1.unwrap_or(entry + fallback_t1_points);
    let resolved = PaperLevels {
        entry_paise: points_to_paise(entry)?,
        hard_sl_paise: points_to_paise(hard_sl)?,
        t1_paise: points_to_paise(t1)?,
        t2_paise: levels.t2.map(points_to_paise).transpose()?,
    };
    if resolved.hard_sl_paise <= 0
        || resolved.hard_sl_paise >= resolved.entry_paise
        || resolved.t1_paise <= resolved.entry_paise
        || resolved
            .t2_paise
            .is_some_and(|t2_paise| t2_paise <= resolved.t1_paise)
    {
        bail!("entry levels and fallback must satisfy BUY level ordering");
    }
    Ok(resolved)
}

fn resolve_route(
    rows: &[InstrumentRow],
    contract: &analysis::OptionContract,
) -> Result<RoutedContract> {
    if contract.direction != TradeDirection::Buy {
        bail!("only BUY option contracts can be routed");
    }
    let underlying = match contract.underlying {
        AnalysisUnderlying::Nifty => ("NIFTY", PaperUnderlying::Nifty),
        AnalysisUnderlying::Sensex => ("SENSEX", PaperUnderlying::Sensex),
    };
    let option_label = match contract.option_type {
        AnalysisOptionType::Ce => "CE",
        AnalysisOptionType::Pe => "PE",
    };
    let explicit_expiry = contract.expiry.as_deref().map(parse_expiry).transpose()?;
    let today = Utc::now().with_timezone(&Kolkata).date_naive();
    let prefix = format!("{}-", underlying.0);
    let mut matches = rows
        .iter()
        .filter(|row| row.trading_symbol.to_ascii_uppercase().starts_with(&prefix))
        .filter(|row| row.option_type.eq_ignore_ascii_case(option_label))
        .filter(|row| {
            row.strike_price
                .parse::<f64>()
                .is_ok_and(|strike| (strike - contract.strike).abs() < 0.01)
        })
        .filter_map(|row| parse_instrument_expiry(&row.expiry_date).map(|expiry| (row, expiry)))
        .filter(|(_, expiry)| explicit_expiry.map_or(*expiry >= today, |wanted| *expiry == wanted))
        .collect::<Vec<_>>();
    matches.sort_by(|(left, left_expiry), (right, right_expiry)| {
        left_expiry
            .cmp(right_expiry)
            .then_with(|| left.trading_symbol.cmp(&right.trading_symbol))
    });
    if explicit_expiry.is_none() {
        let distinct_expiries = matches
            .iter()
            .map(|(_, expiry)| *expiry)
            .collect::<HashSet<_>>();
        if distinct_expiries.len() > 1 {
            bail!(
                "contract expiry is missing and {} current expiries match this strike/type",
                distinct_expiries.len()
            );
        }
    }
    let (row, expiry) = matches.first().copied().ok_or_else(|| {
        anyhow!("contract is absent from the current INDstocks instrument master")
    })?;
    let segment = if row.exchange.eq_ignore_ascii_case("BSE") {
        "BFO"
    } else if row.exchange.eq_ignore_ascii_case("NSE") {
        "NFO"
    } else {
        bail!("unsupported derivatives exchange {}", row.exchange);
    };
    let websocket_code = format!("{segment}:{}", row.security_id);
    let instrument = ResolvedInstrument::new(
        websocket_code.clone(),
        row.security_id.clone(),
        row.trading_symbol.clone(),
    )?;
    let resolved_analysis = analysis::OptionContract {
        expiry: Some(expiry.format("%Y-%m-%d").to_string()),
        ..contract.clone()
    };
    let paper = PaperContract {
        instrument_id: websocket_code,
        trading_symbol: row.trading_symbol.clone(),
        underlying: underlying.1,
        expiry: expiry.format("%Y-%m-%d").to_string(),
        strike_paise: points_to_paise(contract.strike)?,
        option_kind: match contract.option_type {
            AnalysisOptionType::Ce => OptionKind::Ce,
            AnalysisOptionType::Pe => OptionKind::Pe,
        },
    };
    Ok(RoutedContract {
        analysis: resolved_analysis,
        paper,
        instrument,
    })
}

fn parse_expiry(value: &str) -> Result<NaiveDate> {
    let normalized = value.trim().to_ascii_uppercase();
    ["%Y-%m-%d", "%d %b %Y", "%d-%m-%Y", "%d/%m/%Y"]
        .into_iter()
        .find_map(|format| NaiveDate::parse_from_str(&normalized, format).ok())
        .ok_or_else(|| anyhow!("unsupported expiry format"))
}

fn find_setup_for_contract(broker: &PaperBroker, contract: &PaperContract) -> Option<String> {
    broker
        .snapshot(Utc::now().timestamp_millis())
        .accepted_setups
        .into_iter()
        .filter(|setup| setup.contract.instrument_id == contract.instrument_id)
        .max_by_key(|setup| setup.received_timestamp_ms)
        .map(|setup| setup.setup_id)
}

fn routes_from_broker(broker: &PaperBroker) -> Result<HashMap<String, RoutedContract>> {
    let snapshot = broker.snapshot(Utc::now().timestamp_millis());
    snapshot
        .accepted_setups
        .iter()
        .map(|setup| {
            let route = route_from_paper_contract(&setup.contract)?;
            Ok((setup.contract.instrument_id.clone(), route))
        })
        .collect()
}

fn route_from_paper_contract(contract: &PaperContract) -> Result<RoutedContract> {
    let (_, security_id) = contract
        .instrument_id
        .split_once(':')
        .ok_or_else(|| anyhow!("paper instrument ID is not SEGMENT:TOKEN"))?;
    let instrument = ResolvedInstrument::new(
        contract.instrument_id.clone(),
        security_id,
        contract.trading_symbol.clone(),
    )?;
    let analysis = analysis::OptionContract {
        underlying: match contract.underlying {
            PaperUnderlying::Nifty => AnalysisUnderlying::Nifty,
            PaperUnderlying::Sensex => AnalysisUnderlying::Sensex,
        },
        expiry: Some(contract.expiry.clone()),
        strike: paise_to_rupees(contract.strike_paise),
        option_type: match contract.option_kind {
            OptionKind::Ce => AnalysisOptionType::Ce,
            OptionKind::Pe => AnalysisOptionType::Pe,
        },
        direction: TradeDirection::Buy,
    };
    Ok(RoutedContract {
        analysis,
        paper: contract.clone(),
        instrument,
    })
}

async fn reconcile_persistent_subscriptions(
    handle: &MarketFeedHandle,
    snapshot: &PaperBrokerSnapshot,
    known_routes: &HashMap<String, RoutedContract>,
    leases: &mut HashMap<(String, SubscriptionReason), SubscriptionLease>,
) -> Result<()> {
    let mut required = HashSet::<(String, SubscriptionReason)>::new();
    for shadow in &snapshot.shadows {
        for account in &shadow.accounts {
            for order in &account.pending_entries {
                required.insert((
                    order.contract.instrument_id.clone(),
                    SubscriptionReason::PendingOrder,
                ));
            }
            for position in &account.open_positions {
                required.insert((
                    position.position.contract.instrument_id.clone(),
                    SubscriptionReason::OpenPosition,
                ));
            }
        }
    }

    leases.retain(|key, _| required.contains(key));
    for (instrument_id, reason) in required {
        let key = (instrument_id.clone(), reason);
        if leases.contains_key(&key) {
            continue;
        }
        let instrument = if let Some(route) = known_routes.get(&instrument_id) {
            route.instrument.clone()
        } else {
            instrument_from_id(&instrument_id)?
        };
        let lease = handle.subscribe(instrument, reason).await?;
        leases.insert(key, lease);
    }
    Ok(())
}

fn instrument_from_id(value: &str) -> Result<ResolvedInstrument> {
    let (_, security_id) = value
        .split_once(':')
        .ok_or_else(|| anyhow!("paper instrument ID is not SEGMENT:TOKEN"))?;
    ResolvedInstrument::new(value, security_id, value)
}

fn market_tick(tick: &Tick) -> Option<MarketTick> {
    if !tick.ltp.is_finite() || tick.ltp <= 0.0 {
        return None;
    }
    let exchange_timestamp_ms = tick
        .exchange_timestamp_ms
        .unwrap_or(tick.received_timestamp_ms);
    Some(MarketTick {
        instrument_id: tick.instrument.websocket_code.clone(),
        ltp_paise: points_to_paise(tick.ltp).ok()?,
        exchange_timestamp_ms,
        received_timestamp_ms: tick.received_timestamp_ms,
    })
}

fn resync_broker_from_latest(
    broker: &mut PaperBroker,
    latest: &LatestTicks,
    now_ms: i64,
) -> (usize, usize, Option<i64>) {
    let snapshot = broker.snapshot(now_ms);
    let active_instruments = snapshot
        .shadows
        .iter()
        .flat_map(|shadow| shadow.accounts.iter())
        .flat_map(|account| {
            account
                .pending_entries
                .iter()
                .map(|order| order.contract.instrument_id.clone())
                .chain(
                    account
                        .open_positions
                        .iter()
                        .map(|position| position.position.contract.instrument_id.clone()),
                )
        })
        .collect::<HashSet<_>>();
    let mut ticks = latest
        .snapshot()
        .into_values()
        .filter(|tick| {
            active_instruments.contains(&tick.instrument.websocket_code.trim().to_ascii_uppercase())
        })
        .filter_map(|tick| market_tick(&tick))
        .collect::<Vec<_>>();
    ticks.sort_by(|left, right| {
        left.exchange_timestamp_ms
            .cmp(&right.exchange_timestamp_ms)
            .then_with(|| left.received_timestamp_ms.cmp(&right.received_timestamp_ms))
            .then_with(|| left.instrument_id.cmp(&right.instrument_id))
    });

    let considered = ticks.len();
    let mut accepted = 0;
    let mut newest_received_ms = None;
    for tick in ticks {
        let received_ms = tick.received_timestamp_ms;
        let mut policy = moving_stop_from_context;
        if broker
            .on_tick_with_policy(tick, now_ms, &mut policy)
            .accepted
        {
            accepted += 1;
            newest_received_ms = Some(
                newest_received_ms.map_or(received_ms, |previous: i64| previous.max(received_ms)),
            );
        }
    }
    (considered, accepted, newest_received_ms)
}

fn moving_stop_from_context(context: &paper::MovingStopContext) -> Option<i64> {
    let underlying = match context.contract.underlying {
        PaperUnderlying::Nifty => TrailUnderlying::Nifty,
        PaperUnderlying::Sensex => TrailUnderlying::Sensex,
    };
    let levels = TrailLevels::new(
        paise_to_rupees(context.entry_price_paise),
        paise_to_rupees(context.levels.hard_sl_paise),
        paise_to_rupees(context.levels.t1_paise),
        context.levels.t2_paise.map(paise_to_rupees),
    )
    .ok()?;
    let mut trail = TrailState::new(underlying, levels).ok()?;
    trail
        .update_on_tick(paise_to_rupees(context.maximum_ltp_paise))
        .ok()?;
    points_to_paise(trail.effective_sl).ok()
}

fn persist_broker_events(
    broker: &PaperBroker,
    last_sequence: &mut u64,
    writer: &mut JsonlEventWriter,
) -> Result<()> {
    let page = broker.event_page_after(*last_sequence);
    for event in page.events {
        *last_sequence = (*last_sequence).max(event.sequence);
        writer.append(&RuntimeAuditEvent::Broker { event })?;
    }
    Ok(())
}

fn append_pipeline_audit(
    writer: &mut JsonlEventWriter,
    component_name: &str,
    status: &str,
    detail: &str,
) -> Result<()> {
    writer.append(&RuntimeAuditEvent::Pipeline {
        timestamp: Utc::now().to_rfc3339(),
        component: component_name.to_owned(),
        status: status.to_owned(),
        detail: detail.to_owned(),
    })
}

fn load_stream_context(
    path: &Path,
    stream_url: &str,
    trading_date_ist: &str,
) -> Result<Option<analysis::RollingContext>> {
    match std::fs::metadata(path) {
        Ok(metadata) if metadata.len() > MAX_STREAM_CONTEXT_FILE_BYTES => {
            bail!(
                "{} is {} bytes, above the {}-byte rolling-context safety limit",
                path.display(),
                metadata.len(),
                MAX_STREAM_CONTEXT_FILE_BYTES,
            );
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("could not inspect {}", path.display()));
        }
    }

    let Some(envelope) = load_json_snapshot::<StreamContextEnvelope>(path)? else {
        return Ok(None);
    };
    if !stream_context_envelope_matches(&envelope, stream_url, trading_date_ist) {
        return Ok(None);
    }
    let context_size = serde_json::to_vec(&envelope.rolling_context)
        .context("could not size restored rolling context")?
        .len() as u64;
    if context_size > MAX_STREAM_CONTEXT_FILE_BYTES {
        bail!("restored rolling context is {context_size} bytes, above the safety limit");
    }
    Ok(Some(envelope.rolling_context))
}

fn stream_context_envelope_matches(
    envelope: &StreamContextEnvelope,
    stream_url: &str,
    trading_date_ist: &str,
) -> bool {
    envelope.schema_version == STREAM_CONTEXT_SCHEMA_VERSION
        && envelope.stream_url == stream_url
        && envelope.trading_date_ist == trading_date_ist
        && ist_trading_date(envelope.source_clip_ended_at) == trading_date_ist
        && envelope.updated_at >= envelope.source_clip_ended_at
}

fn signal_view(
    action: &TradeAction,
    completed: &AnalysisCompleted,
    accepted: bool,
    setup_id: String,
    bias: &str,
    freshness: &str,
    decision: String,
    route: Option<&RoutedContract>,
) -> SignalView {
    let contract = action.contract.as_ref();
    let levels = action.levels.as_ref();
    let evidence_start = action.evidence_timestamps.first().map(|evidence| {
        (completed.dispatch.started_at()
            + chrono::Duration::milliseconds(
                (evidence.seconds_from_clip_start * 1_000.0).round() as i64
            ))
        .to_rfc3339()
    });
    let evidence_end = action.evidence_timestamps.last().map(|evidence| {
        (completed.dispatch.started_at()
            + chrono::Duration::milliseconds(
                (evidence.seconds_from_clip_start * 1_000.0).round() as i64
            ))
        .to_rfc3339()
    });
    let symbol = route
        .map(|route| route.paper.trading_symbol.clone())
        .unwrap_or_default();
    let expiry = route
        .map(|route| route.paper.expiry.clone())
        .or_else(|| contract.and_then(|contract| contract.expiry.clone()))
        .unwrap_or_default();
    SignalView {
        signal_id: format!(
            "{}-{}-{}",
            format!("dispatch-{}", completed.dispatch.sequence),
            format!("{:?}", action.action).to_ascii_lowercase(),
            "scoreless"
        ),
        setup_id,
        received_at: Utc::now().to_rfc3339(),
        evidence_start,
        evidence_end,
        action: format!("{:?}", action.action).to_ascii_uppercase(),
        accepted,
        symbol,
        underlying: contract
            .map(|contract| format!("{:?}", contract.underlying).to_ascii_uppercase())
            .unwrap_or_default(),
        expiry,
        strike: contract.map(|contract| contract.strike),
        option_type: contract
            .map(|contract| format!("{:?}", contract.option_type).to_ascii_uppercase())
            .unwrap_or_default(),
        side: contract
            .map(|contract| format!("{:?}", contract.direction).to_ascii_uppercase())
            .unwrap_or_default(),
        entry: levels.and_then(|levels| levels.entry),
        stop_loss: levels.and_then(|levels| levels.hard_sl),
        target_1: levels.and_then(|levels| levels.t1),
        target_2: levels.and_then(|levels| levels.t2),
        market_bias: bias.to_owned(),
        source_age_ms: Some(completed.input.clip.data_age_ms),
        freshness: freshness.to_owned(),
        transcript_excerpt: truncate_chars(&completed.transcript_excerpt, 300),
        decision_reason: decision,
    }
}

fn push_signal(signals: &mut Vec<SignalView>, signal: SignalView) {
    signals.push(signal);
    if signals.len() > MAX_SIGNALS {
        signals.drain(0..signals.len() - MAX_SIGNALS);
    }
}

fn append_equity_samples(snapshot: &PaperBrokerSnapshot, points: &mut Vec<EquityPoint>) {
    let timestamp = timestamp_from_ms(snapshot.as_of_timestamp_ms).to_rfc3339();
    points.push(EquityPoint {
        timestamp: timestamp.clone(),
        account_id: None,
        strategy: None,
        equity: paise_to_rupees(snapshot.combined_shadow_totals.liquidation_equity_paise),
        realized_pnl: paise_to_rupees(snapshot.combined_shadow_totals.realized_pnl_paise),
        unrealized_pnl: paise_to_rupees(snapshot.combined_shadow_totals.net_unrealized_pnl_paise),
    });
    for shadow in &snapshot.shadows {
        let strategy = shadow_label(shadow.mode).to_owned();
        for account in &shadow.accounts {
            points.push(EquityPoint {
                timestamp: timestamp.clone(),
                account_id: Some(dashboard_account_id(shadow.mode, &account.account_id)),
                strategy: Some(strategy.clone()),
                equity: paise_to_rupees(account.totals.liquidation_equity_paise),
                realized_pnl: paise_to_rupees(account.totals.realized_pnl_paise),
                unrealized_pnl: paise_to_rupees(account.totals.net_unrealized_pnl_paise),
            });
        }
    }
    if points.len() > MAX_EQUITY_POINTS {
        points.drain(0..points.len() - MAX_EQUITY_POINTS);
    }
}

fn merge_closed_history(
    history: &mut Vec<HistoryTrade>,
    closed: &[ClosedTrade],
    snapshot: &PaperBrokerSnapshot,
) {
    let known = history
        .iter()
        .map(|trade| trade.trade_id.clone())
        .collect::<HashSet<_>>();
    let account_names = snapshot
        .shadows
        .iter()
        .flat_map(|shadow| {
            shadow.accounts.iter().map(move |account| {
                (
                    (shadow.mode, account.account_id.clone()),
                    account.display_name.clone(),
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    let setups = snapshot
        .accepted_setups
        .iter()
        .map(|setup| (setup.setup_id.clone(), setup))
        .collect::<HashMap<_, _>>();
    for trade in closed
        .iter()
        .filter(|trade| !known.contains(&trade.trade_id))
    {
        let setup = setups.get(&trade.setup_id).copied();
        let entry_notional = trade.entry_price_paise * i64::from(trade.quantity);
        let return_pct = if entry_notional > 0 {
            trade.net_pnl_paise as f64 / entry_notional as f64 * 100.0
        } else {
            0.0
        };
        history.push(HistoryTrade {
            trade_id: trade.trade_id.clone(),
            setup_id: trade.setup_id.clone(),
            account_id: dashboard_account_id(trade.mode, &trade.account_id),
            account_name: account_names
                .get(&(trade.mode, trade.account_id.clone()))
                .cloned()
                .unwrap_or_else(|| trade.account_id.clone()),
            strategy: shadow_label(trade.mode).to_owned(),
            symbol: trade.contract.trading_symbol.clone(),
            underlying: format!("{:?}", trade.contract.underlying).to_ascii_uppercase(),
            expiry: trade.contract.expiry.clone(),
            strike: paise_to_rupees(trade.contract.strike_paise),
            option_type: format!("{:?}", trade.contract.option_kind).to_ascii_uppercase(),
            side: "BUY".to_owned(),
            status: if trade.net_pnl_paise > 0 {
                "WIN"
            } else if trade.net_pnl_paise < 0 {
                "LOSS"
            } else {
                "BREAKEVEN"
            }
            .to_owned(),
            quantity: trade.quantity,
            lots: trade.lots,
            entry_price: paise_to_rupees(trade.entry_price_paise),
            exit_price: paise_to_rupees(trade.exit_price_paise),
            streamer_sl: setup
                .map(|setup| paise_to_rupees(setup.levels.hard_sl_paise))
                .unwrap_or_else(|| paise_to_rupees(trade.final_sl_paise)),
            final_sl: paise_to_rupees(trade.final_sl_paise),
            stop_loss: paise_to_rupees(trade.final_sl_paise),
            target_1: setup
                .map(|setup| paise_to_rupees(setup.levels.t1_paise))
                .unwrap_or_default(),
            target_2: setup.and_then(|setup| setup.levels.t2_paise.map(paise_to_rupees)),
            opened_at: timestamp_from_ms(trade.opened_timestamp_ms).to_rfc3339(),
            closed_at: timestamp_from_ms(trade.closed_timestamp_ms).to_rfc3339(),
            hold_seconds: trade
                .closed_timestamp_ms
                .saturating_sub(trade.opened_timestamp_ms)
                .max(0) as u64
                / 1_000,
            exit_reason: format!("{:?}", trade.exit_reason).to_ascii_uppercase(),
            exit_phase: setup
                .map(|setup| {
                    trailing_phase_label(
                        trade.mode,
                        trade.contract.underlying,
                        trade.entry_price_paise,
                        &setup.levels,
                        trade.maximum_ltp_paise,
                    )
                })
                .unwrap_or_default(),
            gross_pnl: paise_to_rupees(trade.gross_pnl_paise),
            charges: paise_to_rupees(trade.entry_charge_paise + trade.exit_charge_paise),
            net_pnl: paise_to_rupees(trade.net_pnl_paise),
            return_pct,
            max_favorable_price: paise_to_rupees(trade.maximum_ltp_paise),
            max_adverse_price: paise_to_rupees(trade.minimum_ltp_paise),
            notes: format!("paper-only {:?} exit", trade.exit_reason),
        });
    }
    history.sort_by(|left, right| left.closed_at.cmp(&right.closed_at));
    deduplicate_history(history);
}

fn deduplicate_history(history: &mut Vec<HistoryTrade>) {
    let mut seen = HashSet::new();
    history.retain(|trade| seen.insert(trade.trade_id.clone()));
}

fn paper_broker_config(config: &AppConfig) -> Result<PaperBrokerConfig> {
    Ok(PaperBrokerConfig {
        entry_buffer_paise: points_to_paise(config.trading.entry_buffer_points)?,
        entry_charge_paise: points_to_paise(config.trading.charge_per_fill_rupees)?,
        exit_charge_paise: points_to_paise(config.trading.charge_per_fill_rupees)?,
        pending_entry_ttl_ms: paper::DEFAULT_PENDING_ENTRY_TTL_MS,
        ..PaperBrokerConfig::default()
    })
}

fn paper_account_specs(config: &AppConfig) -> Result<Vec<AccountSpec>> {
    config
        .accounts
        .iter()
        .map(|account| {
            Ok(AccountSpec {
                account_id: account.id.clone(),
                display_name: format!("{} (INR {:.0})", account.id, account.initial_capital_rupees),
                starting_capital_paise: points_to_paise(account.initial_capital_rupees)?,
            })
        })
        .collect()
}

pub(crate) async fn load_idle_dashboard_state(
    config: &AppConfig,
    store: Option<&NeonStore>,
) -> Result<DashboardState> {
    let durable = match store {
        Some(store) => store
            .load_runtime_state::<DurablePaperState>("paper-primary")
            .await
            .context("could not restore durable Neon paper state")?,
        None => None,
    };
    idle_dashboard_from_parts(
        paper_broker_config(config)?,
        paper_account_specs(config)?,
        durable,
        config.trading.charge_per_fill_rupees,
    )
}

fn idle_dashboard_from_parts(
    broker_config: PaperBrokerConfig,
    account_specs: Vec<AccountSpec>,
    durable: Option<DurablePaperState>,
    exit_charge_rupees: f64,
) -> Result<DashboardState> {
    let (persisted_broker, history, equity_curve) = match durable {
        Some(state) => (Some(state.broker), state.history, state.equity_curve),
        None => (None, Vec::new(), Vec::new()),
    };
    let broker = match persisted_broker {
        Some(persisted) => PaperBroker::restore_from_persisted(
            persisted,
            broker_config.clone(),
            account_specs.clone(),
        )
        .context("persisted paper broker state is incompatible with current configuration")?,
        None => PaperBroker::with_accounts(broker_config, account_specs)?,
    };
    Ok(dashboard_state(
        &broker.snapshot(Utc::now().timestamp_millis()),
        SessionView::default(),
        HealthView::default(),
        Vec::new(),
        equity_curve,
        history,
        exit_charge_rupees,
    ))
}

#[allow(clippy::too_many_arguments)]
fn dashboard_state(
    snapshot: &PaperBrokerSnapshot,
    session: SessionView,
    health: HealthView,
    signals: Vec<SignalView>,
    equity_curve: Vec<EquityPoint>,
    history: Vec<HistoryTrade>,
    exit_charge_rupees: f64,
) -> DashboardState {
    let setups = snapshot
        .accepted_setups
        .iter()
        .map(|setup| (setup.setup_id.clone(), setup))
        .collect::<HashMap<_, _>>();
    let ticks = snapshot
        .latest_ticks
        .iter()
        .map(|tick| (tick.instrument_id.clone(), tick))
        .collect::<HashMap<_, _>>();
    let now_ms = snapshot.as_of_timestamp_ms;
    let mut accounts = Vec::new();
    let mut positions = Vec::new();
    let mut pending_orders = Vec::new();

    for shadow in &snapshot.shadows {
        let strategy = shadow_label(shadow.mode).to_owned();
        for account in &shadow.accounts {
            let wins = account
                .closed_trades
                .iter()
                .filter(|trade| trade.net_pnl_paise > 0)
                .count();
            let losses = account
                .closed_trades
                .iter()
                .filter(|trade| trade.net_pnl_paise < 0)
                .count();
            let total_pnl = paise_to_rupees(account.totals.total_pnl_paise);
            let starting = paise_to_rupees(account.totals.starting_capital_paise);
            accounts.push(AccountView {
                account_id: dashboard_account_id(shadow.mode, &account.account_id),
                account_name: account.display_name.clone(),
                strategy: strategy.clone(),
                starting_capital: starting,
                available_cash: paise_to_rupees(account.totals.free_cash_paise),
                reserved_cash: paise_to_rupees(account.totals.total_reserved_paise),
                deployed_capital: paise_to_rupees(account.totals.gross_market_value_paise),
                equity: paise_to_rupees(account.totals.liquidation_equity_paise),
                realized_pnl: paise_to_rupees(account.totals.realized_pnl_paise),
                unrealized_pnl: paise_to_rupees(account.totals.net_unrealized_pnl_paise),
                total_pnl,
                return_pct: if starting > 0.0 {
                    total_pnl / starting * 100.0
                } else {
                    0.0
                },
                open_positions: account.totals.open_position_count,
                pending_orders: account.totals.pending_order_count,
                trades: account.totals.closed_trade_count,
                wins,
                losses,
                charges: paise_to_rupees(account.totals.charges_paid_paise),
            });

            for position in &account.open_positions {
                let open = &position.position;
                let entry_notional = open.entry_price_paise * i64::from(open.quantity);
                let net_pnl = position.net_unrealized_pnl_paise;
                positions.push(PositionView {
                    position_id: open.position_id.clone(),
                    setup_id: open.setup_id.clone(),
                    account_id: dashboard_account_id(shadow.mode, &account.account_id),
                    account_name: account.display_name.clone(),
                    strategy: strategy.clone(),
                    symbol: open.contract.trading_symbol.clone(),
                    underlying: format!("{:?}", open.contract.underlying).to_ascii_uppercase(),
                    expiry: open.contract.expiry.clone(),
                    strike: paise_to_rupees(open.contract.strike_paise),
                    option_type: format!("{:?}", open.contract.option_kind).to_ascii_uppercase(),
                    side: "BUY".to_owned(),
                    quantity: open.quantity,
                    lots: open.lots,
                    entry_price: paise_to_rupees(open.entry_price_paise),
                    current_ltp: paise_to_rupees(open.last_ltp_paise),
                    streamer_sl: paise_to_rupees(open.levels.hard_sl_paise),
                    effective_sl: paise_to_rupees(open.effective_sl_paise),
                    target_1: paise_to_rupees(open.levels.t1_paise),
                    target_2: open.levels.t2_paise.map(paise_to_rupees),
                    trailing_phase: trailing_phase_label(
                        shadow.mode,
                        open.contract.underlying,
                        open.entry_price_paise,
                        &open.levels,
                        open.maximum_ltp_paise,
                    ),
                    opened_at: timestamp_from_ms(open.opened_timestamp_ms).to_rfc3339(),
                    last_tick_at: Some(timestamp_from_ms(open.last_tick_timestamp_ms).to_rfc3339()),
                    tick_age_ms: Some(
                        now_ms.saturating_sub(open.last_tick_timestamp_ms).max(0) as u64
                    ),
                    gross_pnl: paise_to_rupees(position.gross_unrealized_pnl_paise),
                    estimated_exit_charge: exit_charge_rupees,
                    net_pnl: paise_to_rupees(net_pnl),
                    return_pct: if entry_notional > 0 {
                        net_pnl as f64 / entry_notional as f64 * 100.0
                    } else {
                        0.0
                    },
                    max_favorable_price: paise_to_rupees(open.maximum_ltp_paise),
                    max_adverse_price: paise_to_rupees(open.minimum_ltp_paise),
                });
            }

            for order in &account.pending_entries {
                let tick = ticks.get(&order.contract.instrument_id).copied();
                let _setup = setups.get(&order.setup_id).copied();
                pending_orders.push(PendingOrderView {
                    order_id: order.order_id.clone(),
                    setup_id: order.setup_id.clone(),
                    account_id: dashboard_account_id(shadow.mode, &account.account_id),
                    account_name: account.display_name.clone(),
                    strategy: strategy.clone(),
                    symbol: order.contract.trading_symbol.clone(),
                    underlying: format!("{:?}", order.contract.underlying).to_ascii_uppercase(),
                    expiry: order.contract.expiry.clone(),
                    strike: paise_to_rupees(order.contract.strike_paise),
                    option_type: format!("{:?}", order.contract.option_kind).to_ascii_uppercase(),
                    side: "BUY".to_owned(),
                    quantity: order.quantity,
                    lots: order.lots,
                    requested_entry: paise_to_rupees(order.levels.entry_paise),
                    maximum_fill_price: paise_to_rupees(order.trigger_cap_paise),
                    entry_buffer: paise_to_rupees(
                        order.trigger_cap_paise - order.levels.entry_paise,
                    ),
                    current_ltp: tick.map(|tick| paise_to_rupees(tick.ltp_paise)),
                    reserved_cash: paise_to_rupees(order.reserved_paise),
                    status: "WAITING_FOR_FRESH_MATCH".to_owned(),
                    created_at: timestamp_from_ms(order.created_timestamp_ms).to_rfc3339(),
                    expires_at: None,
                    last_tick_at: tick
                        .map(|tick| timestamp_from_ms(tick.received_timestamp_ms).to_rfc3339()),
                    rejection_reason: None,
                });
            }
        }
    }

    let closed = &snapshot.closed_trade_history;
    let wins = closed
        .iter()
        .filter(|trade| trade.net_pnl_paise > 0)
        .count();
    let losses = closed
        .iter()
        .filter(|trade| trade.net_pnl_paise < 0)
        .count();
    let breakeven = closed.len().saturating_sub(wins + losses);
    let gross_profit_paise: i64 = closed
        .iter()
        .filter(|trade| trade.net_pnl_paise > 0)
        .map(|trade| trade.net_pnl_paise)
        .sum();
    let gross_loss_paise: i64 = closed
        .iter()
        .filter(|trade| trade.net_pnl_paise < 0)
        .map(|trade| trade.net_pnl_paise.abs())
        .sum();
    let totals = &snapshot.combined_shadow_totals;
    let starting = paise_to_rupees(totals.starting_capital_paise);
    let total_pnl = paise_to_rupees(totals.total_pnl_paise);
    let (max_drawdown, max_drawdown_pct) = drawdown(&equity_curve);
    let metrics = MetricsView {
        starting_capital: starting,
        available_cash: paise_to_rupees(totals.free_cash_paise),
        reserved_cash: paise_to_rupees(totals.total_reserved_paise),
        deployed_capital: paise_to_rupees(totals.gross_market_value_paise),
        equity: paise_to_rupees(totals.liquidation_equity_paise),
        realized_pnl: paise_to_rupees(totals.realized_pnl_paise),
        unrealized_pnl: paise_to_rupees(totals.net_unrealized_pnl_paise),
        total_pnl,
        total_return_pct: if starting > 0.0 {
            total_pnl / starting * 100.0
        } else {
            0.0
        },
        gross_profit: paise_to_rupees(gross_profit_paise),
        gross_loss: paise_to_rupees(gross_loss_paise),
        charges: paise_to_rupees(totals.charges_paid_paise),
        open_positions: totals.open_position_count,
        pending_orders: totals.pending_order_count,
        trades_today: closed.len(),
        closed_trades: closed.len(),
        wins,
        losses,
        breakeven,
        win_rate_pct: if closed.is_empty() {
            0.0
        } else {
            wins as f64 / closed.len() as f64 * 100.0
        },
        profit_factor: (gross_loss_paise > 0)
            .then_some(gross_profit_paise as f64 / gross_loss_paise as f64),
        max_drawdown,
        max_drawdown_pct,
    };

    DashboardState {
        revision: 0,
        updated_at: Utc::now().to_rfc3339(),
        session,
        health,
        metrics,
        accounts,
        positions,
        pending_orders,
        signals,
        equity_curve,
        history,
        logs: Vec::new(),
    }
}

fn trailing_phase_label(
    mode: ShadowMode,
    underlying: PaperUnderlying,
    entry_paise: i64,
    levels: &PaperLevels,
    maximum_ltp_paise: i64,
) -> String {
    if mode == ShadowMode::LlmExit {
        return "LLM_MANAGED".to_owned();
    }
    let underlying = match underlying {
        PaperUnderlying::Nifty => TrailUnderlying::Nifty,
        PaperUnderlying::Sensex => TrailUnderlying::Sensex,
    };
    let Ok(levels) = TrailLevels::new(
        paise_to_rupees(entry_paise),
        paise_to_rupees(levels.hard_sl_paise),
        paise_to_rupees(levels.t1_paise),
        levels.t2_paise.map(paise_to_rupees),
    ) else {
        return "PHASE_0".to_owned();
    };
    let Ok(mut state) = TrailState::new(underlying, levels) else {
        return "PHASE_0".to_owned();
    };
    let _ = state.update_on_tick(paise_to_rupees(maximum_ltp_paise));
    match state.phase {
        TrailPhase::Phase0 => "PHASE_0",
        TrailPhase::Phase1 => "PHASE_1",
        TrailPhase::Phase2 => "PHASE_2",
        TrailPhase::Phase3 => "PHASE_3",
        TrailPhase::Phase4 => "PHASE_4",
        TrailPhase::Phase5 => "PHASE_5_RUNNER",
    }
    .to_owned()
}

fn drawdown(points: &[EquityPoint]) -> (f64, f64) {
    let mut peak = 0.0f64;
    let mut max_drawdown = 0.0f64;
    let mut max_pct = 0.0f64;
    for point in points
        .iter()
        .filter(|point| point.account_id.is_none() && point.strategy.is_none())
    {
        peak = peak.max(point.equity);
        if peak > 0.0 {
            let drawdown = (peak - point.equity).max(0.0);
            max_drawdown = max_drawdown.max(drawdown);
            max_pct = max_pct.max(drawdown / peak * 100.0);
        }
    }
    (max_drawdown, max_pct)
}

fn initial_health() -> HealthView {
    HealthView {
        overall: "STARTING".to_owned(),
        youtube_discovery: component(
            "DIRECT_STREAM_URL",
            "runtime received a stream URL; capture will verify playback",
        ),
        stream_capture: component("STARTING", "initializing live-edge capture"),
        transcription: component("STARTING", "loading Scribe v2 credentials"),
        analysis: component("STARTING", "initializing strict multimodal client"),
        market_feed: component("STARTING", "initializing dynamic market feed"),
        persistence: component("STARTING", "opening durable session files"),
        api_keys: Vec::new(),
        last_tick_at: None,
        tick_age_ms: None,
    }
}

fn component(status: impl Into<String>, message: impl Into<String>) -> ComponentHealth {
    ComponentHealth {
        status: status.into(),
        message: message.into(),
        ..ComponentHealth::default()
    }
}

fn healthy_with_latency(message: impl Into<String>, latency_ms: u64) -> ComponentHealth {
    ComponentHealth {
        status: "HEALTHY".to_owned(),
        message: message.into(),
        last_success_at: Some(Utc::now().to_rfc3339()),
        latency_ms: Some(latency_ms),
        reconnects: 0,
    }
}

fn feed_health(state: FeedConnectionState) -> ComponentHealth {
    match state {
        FeedConnectionState::Idle => component("IDLE", "no active option subscription"),
        FeedConnectionState::Connecting => component("CONNECTING", "opening INDstocks WebSocket"),
        FeedConnectionState::Connected => component("HEALTHY", "INDstocks WebSocket connected"),
        FeedConnectionState::BackingOff => {
            component("DEGRADED", "market feed reconnect backoff active")
        }
        FeedConnectionState::Stopped => component("STOPPED", "market feed actor stopped"),
    }
}

fn overall_health(health: &HealthView) -> String {
    let statuses = [
        &health.stream_capture.status,
        &health.transcription.status,
        &health.analysis.status,
        &health.market_feed.status,
        &health.persistence.status,
    ];
    if statuses
        .iter()
        .any(|status| matches!(status.as_str(), "STOPPED" | "ERROR" | "FAILED"))
    {
        "DEGRADED".to_owned()
    } else if statuses.iter().any(|status| status.as_str() == "DEGRADED") {
        "DEGRADED".to_owned()
    } else if statuses
        .iter()
        .any(|status| matches!(status.as_str(), "STARTING" | "PROCESSING" | "CONNECTING"))
    {
        "STARTING".to_owned()
    } else {
        "HEALTHY".to_owned()
    }
}

fn update_live_ages(session: &mut SessionView, health: &mut HealthView) {
    let now = Utc::now();
    if let Some(ended) = session
        .clip_window_end
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
    {
        session.clip_age_ms = Some(
            now.signed_duration_since(ended.with_timezone(&Utc))
                .num_milliseconds()
                .max(0) as u64,
        );
    }
    if let Some(last_tick) = session
        .last_tick_at
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
    {
        let age = now
            .signed_duration_since(last_tick.with_timezone(&Utc))
            .num_milliseconds()
            .max(0) as u64;
        session.tick_age_ms = Some(age);
        health.tick_age_ms = Some(age);
    }
}

fn dashboard_account_id(mode: ShadowMode, account_id: &str) -> String {
    format!("{}:{account_id}", shadow_label(mode))
}

fn shadow_label(mode: ShadowMode) -> &'static str {
    match mode {
        ShadowMode::LlmExit => "LLM_EXIT",
        ShadowMode::MovingSl => "MOVING_SL",
    }
}

fn points_to_paise(value: f64) -> Result<i64> {
    if !value.is_finite() || value < 0.0 || value > i64::MAX as f64 / 100.0 {
        bail!("price/currency value is outside the supported range");
    }
    Ok((value * 100.0).round() as i64)
}

fn paise_to_rupees(value: i64) -> f64 {
    value as f64 / 100.0
}

fn timestamp_from_ms(timestamp_ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .unwrap_or_else(Utc::now)
}

fn ist_trading_date(timestamp: DateTime<Utc>) -> String {
    timestamp
        .with_timezone(&Kolkata)
        .date_naive()
        .format("%Y-%m-%d")
        .to_string()
}

fn age_ms(then: DateTime<Utc>, now: DateTime<Utc>) -> u64 {
    now.signed_duration_since(then).num_milliseconds().max(0) as u64
}

fn truncate_chars(value: &str, maximum: usize) -> String {
    let mut chars = value.chars();
    let prefix = chars.by_ref().take(maximum).collect::<String>();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_visual_cadence_uses_frame_only_after_four_committed_text_calls() {
        let mut cadence = VisualCadence::default();
        assert!(!cadence.visual_due());
        for _ in 0..4 {
            cadence.record_committed(false, Utc::now());
            assert!(!cadence.visual_due() || cadence.commits_since_visual == 4);
        }
        assert!(cadence.visual_due());
        // A due call whose frame extraction fails stays due even if its text
        // analysis commits, so the next eligible dispatch retries the frame.
        cadence.record_committed(false, Utc::now());
        assert!(cadence.visual_due());
        cadence.record_committed(true, Utc::now());
        assert!(!cadence.visual_due());
        assert_eq!(cadence.commits_since_visual, 0);
    }

    #[test]
    fn stt_reorder_buffer_delivers_out_of_order_completions_in_capture_sequence_order() {
        let mut reorder = OrderedSttCompletionBuffer::default();
        reorder.begin_at(1);

        assert!(reorder.insert(2, "failed-2").is_empty());
        assert_eq!(
            reorder.insert(1, "complete-1"),
            vec!["complete-1", "failed-2"]
        );
        assert_eq!(reorder.insert(3, "complete-3"), vec!["complete-3"]);
        assert!(reorder.is_empty());
    }

    #[test]
    fn expiry_parser_accepts_model_and_human_forms() {
        let expected = NaiveDate::from_ymd_opt(2026, 8, 13).unwrap();
        assert_eq!(parse_expiry("2026-08-13").unwrap(), expected);
        assert_eq!(parse_expiry("13 Aug 2026").unwrap(), expected);
        assert_eq!(parse_expiry("13/08/2026").unwrap(), expected);
    }

    #[test]
    fn route_requires_expiry_when_multiple_weeklies_match() {
        let rows = vec![
            InstrumentRow {
                exchange: "NSE".to_owned(),
                security_id: "101".to_owned(),
                trading_symbol: "NIFTY-20990813-25000-CE".to_owned(),
                expiry_date: "2099-08-13".to_owned(),
                strike_price: "25000".to_owned(),
                option_type: "CE".to_owned(),
            },
            InstrumentRow {
                exchange: "NSE".to_owned(),
                security_id: "102".to_owned(),
                trading_symbol: "NIFTY-20990820-25000-CE".to_owned(),
                expiry_date: "2099-08-20".to_owned(),
                strike_price: "25000".to_owned(),
                option_type: "CE".to_owned(),
            },
        ];
        let mut contract = analysis::OptionContract {
            underlying: AnalysisUnderlying::Nifty,
            expiry: None,
            strike: 25_000.0,
            option_type: AnalysisOptionType::Ce,
            direction: TradeDirection::Buy,
        };
        let error = match resolve_route(&rows, &contract) {
            Ok(_) => panic!("missing expiry must not pick one of multiple weekly contracts"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("expiry is missing"));

        contract.expiry = Some("2099-08-20".to_owned());
        let route = resolve_route(&rows, &contract).unwrap();
        assert_eq!(route.paper.instrument_id, "NFO:102");
    }

    #[test]
    fn currency_conversion_rounds_to_paise() {
        assert_eq!(points_to_paise(112.345).unwrap(), 11_235);
        assert_eq!(paise_to_rupees(11_235), 112.35);
    }

    #[test]
    fn account_ids_are_unique_across_shadow_books() {
        assert_ne!(
            dashboard_account_id(ShadowMode::LlmExit, "a"),
            dashboard_account_id(ShadowMode::MovingSl, "a")
        );
    }

    #[test]
    fn idle_dashboard_without_durable_state_has_ten_strategy_wallets() {
        let accounts = [5_000_i64, 10_000, 2_000, 15_000, 20_000]
            .into_iter()
            .enumerate()
            .map(|(index, rupees)| AccountSpec {
                account_id: format!("account_{}", index + 1),
                display_name: format!("Account {}", index + 1),
                starting_capital_paise: rupees * 100,
            })
            .collect::<Vec<_>>();

        let state =
            idle_dashboard_from_parts(PaperBrokerConfig::default(), accounts, None, 20.0).unwrap();

        assert_eq!(state.accounts.len(), 10);
        assert_eq!(
            state
                .accounts
                .iter()
                .filter(|account| account.strategy == "LLM_EXIT")
                .count(),
            5
        );
        assert_eq!(
            state
                .accounts
                .iter()
                .filter(|account| account.strategy == "MOVING_SL")
                .count(),
            5
        );
        assert_eq!(state.metrics.starting_capital, 104_000.0);
    }

    #[test]
    fn idle_dashboard_restores_durable_history_and_equity_curve() {
        let broker_config = PaperBrokerConfig::default();
        let accounts = vec![AccountSpec {
            account_id: "account_1".to_owned(),
            display_name: "Account 1".to_owned(),
            starting_capital_paise: 500_000,
        }];
        let broker = PaperBroker::with_accounts(broker_config.clone(), accounts.clone()).unwrap();
        let history = vec![HistoryTrade {
            trade_id: "trade-1".to_owned(),
            net_pnl: 125.0,
            ..HistoryTrade::default()
        }];
        let equity_curve = vec![EquityPoint {
            timestamp: "2026-08-11T10:00:00Z".to_owned(),
            equity: 10_125.0,
            realized_pnl: 125.0,
            ..EquityPoint::default()
        }];
        let durable = DurablePaperState {
            broker,
            stream_url: "https://www.youtube.com/watch?v=test".to_owned(),
            trading_date_ist: "2026-08-11".to_owned(),
            rolling_context: None,
            history: history.clone(),
            equity_curve: equity_curve.clone(),
            updated_at: Utc::now(),
        };

        let state =
            idle_dashboard_from_parts(broker_config, accounts, Some(durable), 20.0).unwrap();

        assert_eq!(state.history, history);
        assert_eq!(state.equity_curve, equity_curve);
    }

    #[test]
    fn legacy_durable_state_without_equity_curve_still_loads() {
        let durable = DurablePaperState {
            broker: PaperBroker::with_accounts(
                PaperBrokerConfig::default(),
                vec![AccountSpec {
                    account_id: "account_1".to_owned(),
                    display_name: "Account 1".to_owned(),
                    starting_capital_paise: 500_000,
                }],
            )
            .unwrap(),
            stream_url: String::new(),
            trading_date_ist: "2026-08-11".to_owned(),
            rolling_context: None,
            history: Vec::new(),
            equity_curve: Vec::new(),
            updated_at: Utc::now(),
        };
        let mut value = serde_json::to_value(durable).unwrap();
        value.as_object_mut().unwrap().remove("equity_curve");

        let restored: DurablePaperState = serde_json::from_value(value).unwrap();

        assert!(restored.equity_curve.is_empty());
    }

    #[tokio::test]
    async fn idle_preload_without_neon_uses_configured_accounts() {
        let config = AppConfig::from_values(
            "C:/project",
            [
                ("OPENAI_API_KEY", "test-analysis-key"),
                ("ELEVENLABS_API_KEY", "test-elevenlabs-key"),
            ],
        )
        .unwrap();

        let state = load_idle_dashboard_state(&config, None).await.unwrap();

        assert_eq!(state.accounts.len(), 10);
        assert_eq!(state.metrics.starting_capital, 104_000.0);
    }

    #[test]
    fn live_start_status_preserves_preloaded_desk_data() {
        let mut state = DashboardState::empty();
        state.accounts.push(AccountView {
            account_id: "LLM_EXIT:account_1".to_owned(),
            ..AccountView::default()
        });
        state.history.push(HistoryTrade {
            trade_id: "trade-1".to_owned(),
            ..HistoryTrade::default()
        });

        apply_live_start_status(
            &mut state,
            SessionView {
                status: "STARTING".to_owned(),
                ..SessionView::default()
            },
            HealthView {
                overall: "STARTING".to_owned(),
                ..HealthView::default()
            },
        );

        assert_eq!(state.session.status, "STARTING");
        assert_eq!(state.health.overall, "STARTING");
        assert_eq!(state.accounts.len(), 1);
        assert_eq!(state.history.len(), 1);
    }

    #[test]
    fn direct_runtime_start_does_not_claim_official_live_discovery() {
        let health = initial_health();
        assert_eq!(health.youtube_discovery.status, "DIRECT_STREAM_URL");
        assert!(!health.youtube_discovery.message.contains("confirmed"));
    }

    #[test]
    fn shared_runtime_start_preserves_scheduler_discovery_proof() {
        let mut state = DashboardState::empty();
        state.health.youtube_discovery = component(
            "LIVE_FOUND",
            "Official YouTube Data API confirmed an active live broadcast",
        );

        apply_live_start_status(
            &mut state,
            SessionView {
                status: "STARTING".to_owned(),
                ..SessionView::default()
            },
            initial_health(),
        );

        assert_eq!(state.health.youtube_discovery.status, "LIVE_FOUND");
    }

    #[test]
    fn shared_runtime_seeds_mutable_health_for_later_replacements() {
        let scheduler_health = HealthView {
            youtube_discovery: component(
                "LIVE_FOUND",
                "Official YouTube Data API confirmed an active live broadcast",
            ),
            ..HealthView::default()
        };
        let mut runtime_health = initial_health();

        seed_shared_discovery_health(&mut runtime_health, &scheduler_health);

        let mut later_snapshot = DashboardState::empty();
        later_snapshot.health = runtime_health;
        assert_eq!(later_snapshot.health.youtube_discovery.status, "LIVE_FOUND");
    }

    #[test]
    fn candidate_lease_is_renewed_before_expiry() {
        assert!(candidate_lease_needs_renewal(Some(Duration::ZERO)));
        assert!(candidate_lease_needs_renewal(Some(Duration::from_secs(2))));
        assert!(!candidate_lease_needs_renewal(Some(Duration::from_secs(3))));
        assert!(!candidate_lease_needs_renewal(None));
    }

    #[test]
    fn place_entry_requires_a_real_new_order() {
        assert!(placement_effectively_accepted(PlacementStatus::Accepted, 1));
        assert!(!placement_effectively_accepted(
            PlacementStatus::Accepted,
            0
        ));
        assert!(!placement_effectively_accepted(
            PlacementStatus::Duplicate,
            0
        ));
        assert!(!placement_effectively_accepted(
            PlacementStatus::Rejected,
            1
        ));
    }

    #[test]
    fn entry_without_streamer_levels_uses_underlying_specific_fallbacks() {
        let action = TradeAction {
            action: ActionKind::PlaceEntry,
            episode_id: Some("episode-1".to_owned()),
            event_id: Some("event-1".to_owned()),
            trade_id: None,
            contract: None,
            levels: Some(analysis::TradeLevels {
                entry: Some(100.0),
                hard_sl: None,
                t1: None,
                t2: None,
            }),
            evidence_timestamps: Vec::new(),
            rationale: "current entry call".to_owned(),
        };

        let nifty = entry_levels_with_fallback(&action, PaperUnderlying::Nifty).unwrap();
        assert_eq!(nifty.entry_paise, points_to_paise(100.0).unwrap());
        assert_eq!(nifty.hard_sl_paise, points_to_paise(92.0).unwrap());
        assert_eq!(nifty.t1_paise, points_to_paise(112.0).unwrap());
        assert_eq!(nifty.t2_paise, None);

        let sensex = entry_levels_with_fallback(&action, PaperUnderlying::Sensex).unwrap();
        assert_eq!(sensex.hard_sl_paise, points_to_paise(88.0).unwrap());
        assert_eq!(sensex.t1_paise, points_to_paise(115.0).unwrap());
    }

    #[test]
    fn rolling_entry_event_tracks_actual_paper_placement_outcome() {
        let mut context = analysis::RollingContext {
            episodes: vec![analysis::TradeEpisodeContext {
                episode_id: "episode-1".to_owned(),
                contract: None,
                status: analysis::TradeEpisodeStatus::EntryCalled,
                levels: None,
                latest_instruction: "enter now".to_owned(),
                entry_event_id: Some("event-1".to_owned()),
                first_seen_at: String::new(),
                last_updated_at: String::new(),
            }],
            ..analysis::RollingContext::default()
        };
        let action = TradeAction {
            action: ActionKind::PlaceEntry,
            episode_id: Some("episode-1".to_owned()),
            event_id: Some("event-1".to_owned()),
            trade_id: None,
            contract: None,
            levels: None,
            evidence_timestamps: Vec::new(),
            rationale: "enter now".to_owned(),
        };

        reconcile_entry_application(&mut context, &action, false);
        assert_eq!(context.episodes[0].entry_event_id, None);
        assert_eq!(
            context.episodes[0].status,
            analysis::TradeEpisodeStatus::ConditionalEntry
        );

        reconcile_entry_application(&mut context, &action, true);
        assert_eq!(
            context.episodes[0].entry_event_id.as_deref(),
            Some("event-1")
        );
        assert_eq!(
            context.episodes[0].status,
            analysis::TradeEpisodeStatus::EntryCalled
        );
    }

    #[test]
    fn analysis_dispatch_is_single_flight_through_context_commit() {
        let mut dispatch = AnalysisDispatchState::default();
        assert!(dispatch.try_begin(7));
        assert!(!dispatch.try_begin(8));
        assert!(!dispatch.finish(8));
        assert!(dispatch.owns(7));
        assert!(dispatch.finish(7));
        assert!(dispatch.try_begin(8));
    }

    #[test]
    fn newer_unsent_dispatch_supersedes_and_releases_the_older_owned_segments() {
        let at = Utc.with_ymd_and_hms(2026, 8, 11, 5, 0, 0).single().unwrap();
        let ready = |sequence| ReadyDispatch {
            sequence,
            clips: vec![RetainedClip {
                sequence,
                start_ms: at.timestamp_millis(),
                duration_ms: 3_000,
                text: format!("segment {sequence}"),
                must_terms: Vec::new(),
            }],
            segments: vec![MediaSegment {
                id: format!("segment-{sequence}"),
                sequence,
                path: std::path::PathBuf::from(format!("segment-{sequence}.ts")),
                started_at_utc: at,
                ended_at_utc: at + chrono::Duration::seconds(3),
                duration_ms: 3_000,
                size_bytes: 1,
            }],
        };
        let mut pending = Some(ready(4));
        let enqueue = begin_ready_dispatch_enqueue(&mut pending, ready(8));
        let superseded = enqueue.superseded.as_ref().unwrap();

        assert_eq!(superseded.source_sequences().collect::<Vec<_>>(), vec![4]);
        assert!(pending.is_none());
        enqueue.commit(&mut pending);
        assert_eq!(
            pending
                .as_ref()
                .unwrap()
                .source_sequences()
                .collect::<Vec<_>>(),
            vec![8]
        );
    }

    #[test]
    fn blocker_event_batch_with_no_active_call_keeps_only_newest_dispatch_and_releases_older() {
        let at = Utc.with_ymd_and_hms(2026, 8, 11, 5, 0, 0).single().unwrap();
        let ready = |sequence| ReadyDispatch {
            sequence,
            clips: vec![RetainedClip {
                sequence,
                start_ms: at.timestamp_millis(),
                duration_ms: 3_000,
                text: format!("segment {sequence}"),
                must_terms: Vec::new(),
            }],
            segments: vec![MediaSegment {
                id: format!("segment-{sequence}"),
                sequence,
                path: std::path::PathBuf::from(format!("segment-{sequence}.ts")),
                started_at_utc: at,
                ended_at_utc: at + chrono::Duration::seconds(3),
                duration_ms: 3_000,
                size_bytes: 1,
            }],
        };
        let mut pending: Option<ReadyDispatch> = None;

        let mut released = Vec::new();
        for incoming in [ready(12), ready(16)] {
            let mut enqueue = begin_ready_dispatch_enqueue(&mut pending, incoming);
            if let Some(stale) = enqueue.superseded.take() {
                assert!(pending.is_none(), "release happens before replacement");
                released.push(stale);
            }
            enqueue.commit(&mut pending);
        }

        assert_eq!(
            released
                .iter()
                .flat_map(ReadyDispatch::source_sequences)
                .collect::<Vec<_>>(),
            vec![12]
        );
        assert_eq!(
            pending
                .as_ref()
                .unwrap()
                .source_sequences()
                .collect::<Vec<_>>(),
            vec![16]
        );
    }

    #[test]
    fn ready_dispatch_preserves_nonconsecutive_source_segments_without_early_ack_transfer() {
        let at = Utc.with_ymd_and_hms(2026, 8, 11, 5, 0, 0).single().unwrap();
        let mut available = BTreeMap::new();
        for sequence in [1_u64, 4, 5, 6] {
            available.insert(
                sequence,
                MediaSegment {
                    id: format!("segment-{sequence}"),
                    sequence,
                    path: std::path::PathBuf::from(format!("segment-{sequence}.ts")),
                    started_at_utc: at + chrono::Duration::seconds((sequence * 3) as i64),
                    ended_at_utc: at + chrono::Duration::seconds(((sequence + 1) * 3) as i64),
                    duration_ms: 3_000,
                    size_bytes: 1,
                },
            );
        }
        let clips = [1_u64, 4, 5, 6]
            .into_iter()
            .map(|sequence| RetainedClip {
                sequence,
                start_ms: at.timestamp_millis() + (sequence as i64 * 3_000),
                duration_ms: 3_000,
                text: format!("segment {sequence}"),
                must_terms: Vec::new(),
            })
            .collect();
        let dispatch = take_ready_dispatch(clips, &mut available).unwrap();

        assert!(available.is_empty());
        assert_eq!(
            dispatch.source_sequences().collect::<Vec<_>>(),
            vec![1, 4, 5, 6]
        );
        assert!(dispatch.segments.iter().all(|segment| {
            segment
                .path
                .extension()
                .is_some_and(|extension| extension == "ts")
        }));
    }

    #[test]
    fn stream_context_envelope_requires_exact_stream_date_schema_and_timestamp() {
        let clip_end = Utc.with_ymd_and_hms(2026, 8, 11, 5, 0, 0).single().unwrap();
        let mut envelope = StreamContextEnvelope {
            schema_version: STREAM_CONTEXT_SCHEMA_VERSION,
            stream_url: "https://www.youtube.com/watch?v=test".to_owned(),
            trading_date_ist: "2026-08-11".to_owned(),
            updated_at: clip_end + chrono::Duration::seconds(3),
            source_window_sequence: 12,
            source_clip_ended_at: clip_end,
            rolling_context: analysis::RollingContext::default(),
        };
        assert!(stream_context_envelope_matches(
            &envelope,
            "https://www.youtube.com/watch?v=test",
            "2026-08-11",
        ));
        assert!(!stream_context_envelope_matches(
            &envelope,
            "https://www.youtube.com/watch?v=other",
            "2026-08-11",
        ));
        assert!(!stream_context_envelope_matches(
            &envelope,
            "https://www.youtube.com/watch?v=test",
            "2026-08-12",
        ));
        envelope.schema_version += 1;
        assert!(!stream_context_envelope_matches(
            &envelope,
            "https://www.youtube.com/watch?v=test",
            "2026-08-11",
        ));
        envelope.schema_version = STREAM_CONTEXT_SCHEMA_VERSION;
        envelope.updated_at = clip_end - chrono::Duration::milliseconds(1);
        assert!(!stream_context_envelope_matches(
            &envelope,
            "https://www.youtube.com/watch?v=test",
            "2026-08-11",
        ));
    }

    #[test]
    fn context_envelope_commit_timestamp_never_predates_a_live_edge_clip_end() {
        let now = Utc.with_ymd_and_hms(2026, 8, 11, 5, 0, 0).single().unwrap();
        let clip_end = now + chrono::Duration::seconds(3);
        let envelope = StreamContextEnvelope::for_analysis(
            "https://www.youtube.com/watch?v=test".to_owned(),
            12,
            clip_end,
            analysis::RollingContext::default(),
            now,
        );

        assert_eq!(envelope.updated_at, clip_end);
        assert!(stream_context_envelope_matches(
            &envelope,
            "https://www.youtube.com/watch?v=test",
            "2026-08-11",
        ));
    }

    #[test]
    fn executable_actions_require_current_window_evidence_and_a_fresh_clip() {
        let received_at = Utc
            .with_ymd_and_hms(2026, 8, 11, 5, 0, 30)
            .single()
            .unwrap();
        let ended_at = received_at - chrono::Duration::seconds(5);
        let started_at = ended_at - chrono::Duration::seconds(3);
        let dispatch = ReadyDispatch {
            sequence: 1,
            clips: vec![RetainedClip {
                sequence: 1,
                start_ms: started_at.timestamp_millis(),
                duration_ms: 3_000,
                text: "enter now".to_owned(),
                must_terms: vec!["entry".to_owned()],
            }],
            segments: vec![MediaSegment {
                id: "test-segment".to_owned(),
                sequence: 1,
                path: std::path::PathBuf::from("test.ts"),
                started_at_utc: started_at,
                ended_at_utc: ended_at,
                duration_ms: 3_000,
                size_bytes: 1,
            }],
        };
        let input = AnalysisInput {
            clip: ClipWindow {
                started_at,
                ended_at,
                sent_at: ended_at,
                data_age_ms: 0,
                complete: true,
            },
            transcripts: Vec::new(),
            watched_options: Vec::new(),
            open_trades: Vec::new(),
            rolling_context: None,
        };
        let completed = AnalysisCompleted {
            dispatch,
            input,
            transcript_excerpt: String::new(),
            latency_ms: 1,
            visual_sent: false,
            visual_note: None,
            recovery_image: None,
            result: Err("unused".to_owned()),
        };
        let mut action = TradeAction {
            action: ActionKind::PlaceEntry,
            episode_id: None,
            event_id: Some("current-event".to_owned()),
            trade_id: None,
            contract: None,
            levels: None,
            evidence_timestamps: vec![analysis::EvidenceTimestamp {
                seconds_from_clip_start: 7.0,
                source: analysis::EvidenceSource::Both,
                source_segment_sequence: Some(1),
                detail: None,
            }],
            rationale: "current entry call".to_owned(),
        };
        assert!(executable_action_freshness_issue(&action, &completed, received_at).is_none());

        action.evidence_timestamps.clear();
        assert!(
            executable_action_freshness_issue(&action, &completed, received_at)
                .unwrap()
                .contains("no evidence")
        );
        action
            .evidence_timestamps
            .push(analysis::EvidenceTimestamp {
                seconds_from_clip_start: 7.0,
                source: analysis::EvidenceSource::Both,
                source_segment_sequence: Some(1),
                detail: None,
            });
        let stale_now = ended_at + chrono::Duration::milliseconds(MAX_EXECUTABLE_SIGNAL_AGE_MS + 1);
        assert!(
            executable_action_freshness_issue(&action, &completed, stale_now)
                .unwrap()
                .contains("source clip")
        );

        action.action = ActionKind::Exit;
        assert!(
            executable_action_freshness_issue(&action, &completed, stale_now)
                .unwrap()
                .contains("source clip")
        );
        action.action = ActionKind::Hold;
        assert!(executable_action_freshness_issue(&action, &completed, stale_now).is_none());
    }

    #[test]
    fn failed_broker_checkpoint_keeps_entry_context_unconsumed_for_retry() {
        let mut provisional = analysis::RollingContext::default();
        provisional.episodes.push(analysis::TradeEpisodeContext {
            episode_id: "episode-1".to_owned(),
            contract: None,
            status: analysis::TradeEpisodeStatus::ConditionalEntry,
            levels: None,
            latest_instruction: "entry if confirmed".to_owned(),
            entry_event_id: None,
            first_seen_at: String::new(),
            last_updated_at: String::new(),
        });
        let mut applied = provisional.clone();
        applied.episodes[0].status = analysis::TradeEpisodeStatus::EntryCalled;
        applied.episodes[0].entry_event_id = Some("event-1".to_owned());

        let recovered = context_after_broker_checkpoint(provisional.clone(), applied, false);
        assert_eq!(recovered, provisional);
        assert_eq!(
            recovered.episodes[0].status,
            analysis::TradeEpisodeStatus::ConditionalEntry
        );
    }

    #[test]
    fn newest_durable_checkpoint_preserves_newer_local_and_accepts_newer_neon() {
        #[derive(Clone)]
        struct Checkpoint {
            label: &'static str,
            updated_at: DateTime<Utc>,
        }
        let older = Utc.with_ymd_and_hms(2026, 8, 15, 3, 0, 0).single().unwrap();
        let newer = older + chrono::Duration::seconds(1);
        let local = Checkpoint {
            label: "local-order",
            updated_at: newer,
        };
        let neon = Checkpoint {
            label: "neon-old",
            updated_at: older,
        };
        let (source, selected) =
            select_newest_durable_state(Some(local), Some(neon), |state| state.updated_at).unwrap();
        assert_eq!(source, DurableStateSource::Local);
        assert_eq!(selected.label, "local-order");

        let (source, selected) = select_newest_durable_state(
            Some(Checkpoint {
                label: "local-old",
                updated_at: older,
            }),
            Some(Checkpoint {
                label: "neon-new",
                updated_at: newer,
            }),
            |state| state.updated_at,
        )
        .unwrap();
        assert_eq!(source, DurableStateSource::Neon);
        assert_eq!(selected.label, "neon-new");
    }

    #[test]
    fn candidates_survive_rejected_placement_and_leave_only_after_actual_order() {
        let instrument_id = "NIFTY-25000-CE";
        let rejected = HashSet::new();
        assert!(
            !candidate_consumed_by_actual_placement(instrument_id, &rejected),
            "routing/freshness/capital/duplicate/zero-order failures retain the watch"
        );
        let accepted = HashSet::from([instrument_id.to_owned()]);
        assert!(candidate_consumed_by_actual_placement(
            instrument_id,
            &accepted
        ));
    }
}
