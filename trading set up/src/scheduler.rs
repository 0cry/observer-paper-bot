//! Asia/Kolkata market-day supervisor for Render deployment.

use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::{Datelike, NaiveDate, Timelike, Utc, Weekday};
use chrono_tz::Asia::Kolkata;
use reqwest::Client;
use serde::Deserialize;
use tokio::{
    process::Command,
    time::{sleep, timeout},
};

use crate::{
    config::AppConfig,
    dashboard::{
        self, ApiKeyHealthView, ComponentHealth, DashboardHandle, DashboardState, HealthView,
        SessionView,
    },
    neon::NeonStore,
    paper_runtime,
    runtime_logs::RuntimeEventLogger,
    youtube::{ApiDiscovery, YouTubeApiDiscovery},
};

pub async fn run(project_dir: &Path, http: Client) -> Result<()> {
    let config = AppConfig::load(project_dir).context("daemon configuration is invalid")?;
    let channel_url = config
        .scheduler
        .youtube_channel_url
        .clone()
        .ok_or_else(|| anyhow::anyhow!("YOUTUBE_CHANNEL_URL is required for daemon mode"))?;
    let mut storage_error = None;
    let log_store = match config.database.url.as_ref() {
        Some(url) => match NeonStore::connect(url.expose_secret()).await {
            Ok(store) => Some(store),
            Err(error) => {
                storage_error = Some(error.to_string());
                None
            }
        },
        None => None,
    };
    let mut initial =
        match paper_runtime::load_idle_dashboard_state(&config, log_store.as_ref()).await {
            Ok(state) => state,
            Err(error) => {
                storage_error = Some(error.to_string());
                paper_runtime::load_idle_dashboard_state(&config, None)
                    .await
                    .context("could not initialize fallback paper wallets")?
            }
        };
    apply_waiting_status(
        &mut initial,
        "STARTING",
        "IST scheduler initializing",
        &channel_url,
        &config,
        storage_error.is_some(),
    );
    let handle = DashboardHandle::new(initial).with_cron_store(log_store.clone());
    let mut youtube_api =
        YouTubeApiDiscovery::new(http.clone(), handle.youtube_vault(), channel_url.clone());
    let runtime_logger = RuntimeEventLogger::new(handle.clone(), log_store.clone());
    if let Err(error) = runtime_logger.load_recent(200).await {
        storage_error = Some(error.to_string());
    }
    let storage_degraded = storage_error.is_some();
    if let Some(error) = storage_error {
        runtime_logger
            .record(
                "ERROR",
                "persistence",
                "LOG_RESTORE_DEGRADED",
                &format!("durable operational logs are unavailable: {error}"),
            )
            .await;
        handle
            .update("persistence_degraded", None, |state| {
                state.health.persistence = component_health(
                    "DEGRADED",
                    "durable paper state is unavailable; configured fallback wallets are displayed",
                );
                state.health.overall = "DEGRADED".to_owned();
            })
            .await;
    }
    let listener = tokio::net::TcpListener::bind(config.dashboard.bind)
        .await
        .with_context(|| format!("could not bind dashboard at {}", config.dashboard.bind))?;
    let router = dashboard::router(handle.clone());
    let server_logger = runtime_logger.clone();
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            server_logger
                .record(
                    "ERROR",
                    "dashboard",
                    "SERVER_STOPPED",
                    &format!("dashboard server stopped: {error}"),
                )
                .await;
        }
    });
    println!("Dashboard: http://{}", config.dashboard.bind);
    if let Some(cron_store) = log_store.clone() {
        let cron_logger = runtime_logger.clone();
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(15));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                timer.tick().await;
                if let Err(error) =
                    crate::cron_jobs::execute_due_jobs(&cron_store, Utc::now()).await
                {
                    cron_logger
                        .record(
                            "WARN",
                            "cron",
                            "RUNNER_DEGRADED",
                            &format!("cron runner cycle failed: {error}"),
                        )
                        .await;
                }
            }
        });
    }
    runtime_logger
        .record(
            "INFO",
            "scheduler",
            "DAEMON_READY",
            "IST paper-trading supervisor and public dashboard are ready",
        )
        .await;

    let mut completed_session_date: Option<NaiveDate> = None;
    loop {
        let now = Utc::now().with_timezone(&Kolkata);
        let date = now.date_naive();
        let time = now.time();
        let trading_day = is_trading_day(date, &config.scheduler.market_holidays_ist);

        if !trading_day {
            publish_waiting_status(
                &handle,
                "MARKET_CLOSED",
                "Weekend or configured NSE F&O holiday",
                &channel_url,
                &config,
                storage_degraded,
            )
            .await;
            sleep(Duration::from_secs(60)).await;
            continue;
        }
        if time < config.scheduler.poll_start_ist {
            publish_waiting_status(
                &handle,
                "WAITING_FOR_09_00_IST",
                "Market-day supervisor is idle until 09:00 IST",
                &channel_url,
                &config,
                storage_degraded,
            )
            .await;
            sleep(Duration::from_secs(60)).await;
            continue;
        }
        if time > config.scheduler.last_discovery_ist || completed_session_date == Some(date) {
            let status = if time >= config.scheduler.worker_stop_ist {
                "WORKERS_STOPPED"
            } else {
                "DISCOVERY_CLOSED"
            };
            publish_waiting_status(
                &handle,
                status,
                "No new stream discovery after 15:30 IST; workers remain stopped",
                &channel_url,
                &config,
                storage_degraded,
            )
            .await;
            sleep(Duration::from_secs(60)).await;
            continue;
        }

        publish_waiting_status(
            &handle,
            "CHECKING_CHANNEL",
            "Checking Trading Cafe India for a current live stream",
            &channel_url,
            &config,
            storage_degraded,
        )
        .await;
        publish_youtube_discovery_health(
            &handle,
            "CHECKING_CHANNEL",
            "Checking official YouTube live status",
        )
        .await;
        match discover_live_url(
            &mut youtube_api,
            &handle,
            &http,
            &config.paths.yt_dlp_path,
            &channel_url,
        )
        .await
        {
            Ok(Some(stream_url)) => {
                runtime_logger
                    .record(
                        "INFO",
                        "scheduler",
                        "LIVE_STREAM_DISCOVERED",
                        "current YouTube live stream discovered; starting the paper-only pipeline",
                    )
                    .await;
                let stop = config.scheduler.worker_stop_ist;
                let seconds_now = u64::from(time.num_seconds_from_midnight());
                let seconds_stop = u64::from(stop.num_seconds_from_midnight());
                let remaining = seconds_stop.saturating_sub(seconds_now).max(1);
                let result = paper_runtime::run_with_dashboard(
                    project_dir,
                    stream_url,
                    http.clone(),
                    Some(remaining),
                    handle.clone(),
                    runtime_logger.clone(),
                )
                .await;
                completed_session_date = Some(date);
                if let Err(error) = result {
                    runtime_logger
                        .record(
                            "ERROR",
                            "runtime",
                            "SESSION_FAILED",
                            &format!("paper session stopped safely: {error}"),
                        )
                        .await;
                    publish_waiting_status(
                        &handle,
                        "SESSION_FAILED",
                        &format!("Paper session stopped safely: {error}"),
                        &channel_url,
                        &config,
                        storage_degraded,
                    )
                    .await;
                }
                if let Err(error) = purge_raw_session_data(
                    project_dir,
                    &config.paths.data_dir,
                    &config.paths.media_dir,
                )
                .await
                {
                    runtime_logger
                        .record(
                            "ERROR",
                            "retention",
                            "RAW_DATA_PURGE_FAILED",
                            &format!("session raw-data cleanup failed: {error}"),
                        )
                        .await;
                } else {
                    runtime_logger
                        .record(
                            "INFO",
                            "retention",
                            "RAW_DATA_PURGED",
                            "session clips, transcripts, and raw tick logs were cleared",
                        )
                        .await;
                }
            }
            Ok(None) => {
                publish_waiting_status(
                    &handle,
                    "WAITING_FOR_LIVE",
                    "No live stream was confirmed; next check follows the configured interval",
                    &channel_url,
                    &config,
                    storage_degraded,
                )
                .await;
            }
            Err(error) => {
                runtime_logger
                    .record(
                        "WARN",
                        "scheduler",
                        "DISCOVERY_FAILED",
                        &format!("YouTube discovery failed safely: {error}"),
                    )
                    .await;
                publish_waiting_status(
                    &handle,
                    "DISCOVERY_DEGRADED",
                    &format!("YouTube discovery failed safely: {error}"),
                    &channel_url,
                    &config,
                    storage_degraded,
                )
                .await;
            }
        }
        sleep(Duration::from_secs(
            config.scheduler.poll_interval_seconds.min(60),
        ))
        .await;
    }
}

fn is_trading_day(date: NaiveDate, holidays: &[NaiveDate]) -> bool {
    !matches!(date.weekday(), Weekday::Sat | Weekday::Sun) && !holidays.contains(&date)
}

async fn discover_live_url(
    youtube_api: &mut YouTubeApiDiscovery,
    handle: &DashboardHandle,
    http: &Client,
    yt_dlp: &Path,
    channel_url: &str,
) -> Result<Option<String>> {
    let api_outcome = youtube_api.discover().await;
    match &api_outcome {
        ApiDiscovery::Live(_) => {
            publish_youtube_discovery_health(
                handle,
                "LIVE_FOUND",
                "Official YouTube Data API confirmed an active live broadcast",
            )
            .await;
        }
        ApiDiscovery::NotLive => {
            publish_youtube_discovery_health(
                handle,
                "NOT_LIVE",
                "Official YouTube Data API reports no active live broadcast",
            )
            .await;
        }
        ApiDiscovery::Indeterminate => {
            publish_youtube_discovery_health(
                handle,
                "NOT_YET_CONFIRMED",
                "Recent uploads contain no active live broadcast; quota-bounded live search is not due yet",
            )
            .await;
        }
        ApiDiscovery::NoKey => {
            publish_youtube_discovery_health(
                handle,
                "KEY_REQUIRED",
                "YouTube Data API key is not loaded; using safe legacy discovery fallback",
            )
            .await;
        }
        ApiDiscovery::Unavailable(detail) => {
            publish_youtube_discovery_health(
                handle,
                "DEGRADED",
                &format!("Official YouTube discovery unavailable: {detail}"),
            )
            .await;
        }
    }
    let allow_yt_dlp = allow_yt_dlp_fallback(&api_outcome);
    if let Some(result) = api_first_result(&api_outcome) {
        return Ok(result);
    }

    let live_page = format!("{}/live", channel_url.trim_end_matches('/'));

    if let Ok(Ok(response)) = timeout(
        Duration::from_secs(15),
        http.get(&live_page)
            .header(
                "User-Agent",
                "Mozilla/5.0 (compatible; ObserverPaperBot/1.0)",
            )
            .header("Cache-Control", "no-cache")
            .send(),
    )
    .await
        && response.status().is_success()
        && let Ok(bytes) = response.bytes().await
        && bytes.len() <= 4 * 1024 * 1024
        && let Some(stream_url) = parse_live_page(&String::from_utf8_lossy(&bytes))
    {
        publish_youtube_discovery_health(
            handle,
            "FALLBACK_LIVE_FOUND",
            "Channel page fallback confirmed an active live broadcast",
        )
        .await;
        return Ok(Some(stream_url));
    }

    if !allow_yt_dlp {
        return Ok(None);
    }

    let mut command = Command::new(yt_dlp);
    command
        .arg("--no-warnings")
        .arg("--no-playlist")
        .arg("--skip-download")
        .arg("--extractor-args")
        .arg(youtube_player_client_fallback())
        .arg("--print")
        .arg("%(id)s\t%(live_status)s\t%(webpage_url)s")
        .arg(live_page)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = timeout(Duration::from_secs(45), command.output())
        .await
        .context("yt-dlp discovery timed out")?
        .context("could not execute yt-dlp discovery")?;
    if !output.status.success() {
        let detail = bounded_yt_dlp_failure_detail(&output.stderr);
        if detail.is_empty() {
            bail!("yt-dlp discovery exited with status {}", output.status);
        }
        bail!(
            "yt-dlp discovery exited with status {}: {detail}",
            output.status
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() >= 3 && fields[1].eq_ignore_ascii_case("is_live") {
            publish_youtube_discovery_health(
                handle,
                "FALLBACK_LIVE_FOUND",
                "Final resolver fallback confirmed an active live broadcast",
            )
            .await;
            return Ok(Some(fields[2].trim().to_owned()));
        }
    }
    Ok(None)
}

fn api_first_result(outcome: &ApiDiscovery) -> Option<Option<String>> {
    match outcome {
        ApiDiscovery::Live(url) => Some(Some(url.clone())),
        ApiDiscovery::NotLive => Some(None),
        ApiDiscovery::Indeterminate | ApiDiscovery::NoKey | ApiDiscovery::Unavailable(_) => None,
    }
}

fn allow_yt_dlp_fallback(outcome: &ApiDiscovery) -> bool {
    matches!(outcome, ApiDiscovery::NoKey | ApiDiscovery::Unavailable(_))
}

async fn publish_youtube_discovery_health(handle: &DashboardHandle, status: &str, message: &str) {
    handle
        .update("youtube_discovery_status", None, |state| {
            state.health.youtube_discovery = component_health(status, message);
        })
        .await;
}

pub(crate) fn youtube_player_client_fallback() -> &'static str {
    "youtube:player_client=android_vr"
}

/// Keeps resolver diagnostics useful without exposing signed URLs or terminal control text.
pub(crate) fn bounded_yt_dlp_failure_detail(stderr: &[u8]) -> String {
    let mut clean = String::new();
    let mut in_escape = false;
    let mut csi_escape = false;
    for character in String::from_utf8_lossy(stderr).chars() {
        if in_escape {
            if !csi_escape && character == '[' {
                csi_escape = true;
                continue;
            }
            if (csi_escape && ('@'..='~').contains(&character))
                || (!csi_escape && character.is_ascii_alphabetic())
            {
                in_escape = false;
                csi_escape = false;
            }
            continue;
        }
        if character == '\u{1b}' {
            in_escape = true;
            csi_escape = false;
        } else if character.is_ascii_graphic() || character.is_ascii_whitespace() {
            clean.push(character);
        }
    }
    let mut words = Vec::new();
    let mut length = 0usize;
    for word in clean.split_whitespace() {
        let replacement = if word.starts_with("https://") || word.starts_with("http://") {
            "[URL redacted]"
        } else {
            word
        };
        length += replacement.len() + usize::from(!words.is_empty());
        words.push(replacement);
        if length >= 480 {
            break;
        }
    }
    words.join(" ").chars().take(480).collect::<String>()
}

fn parse_live_page(html: &str) -> Option<String> {
    const CANONICAL: &str = "<link rel=\"canonical\" href=\"https://www.youtube.com/watch?v=";
    let start = html.find(CANONICAL)? + CANONICAL.len();
    let video_id = html.get(start..start + 11)?;
    if !video_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return None;
    }

    let player_json = extract_initial_player_response(html)?;
    let player: InitialPlayerResponse = serde_json::from_str(player_json).ok()?;
    let details = player.video_details?;
    if details.video_id != video_id || !details.is_live {
        return None;
    }
    let explicitly_offline = player
        .microformat
        .and_then(|value| value.player_microformat_renderer)
        .and_then(|value| value.live_broadcast_details)
        .and_then(|value| value.is_live_now)
        == Some(false);
    if explicitly_offline {
        return None;
    }
    Some(format!("https://www.youtube.com/watch?v={video_id}"))
}

#[derive(Deserialize)]
struct InitialPlayerResponse {
    #[serde(default, rename = "videoDetails")]
    video_details: Option<InitialPlayerVideoDetails>,
    #[serde(default)]
    microformat: Option<InitialPlayerMicroformat>,
}

#[derive(Deserialize)]
struct InitialPlayerVideoDetails {
    #[serde(rename = "videoId")]
    video_id: String,
    #[serde(default, rename = "isLive")]
    is_live: bool,
}

#[derive(Deserialize)]
struct InitialPlayerMicroformat {
    #[serde(default, rename = "playerMicroformatRenderer")]
    player_microformat_renderer: Option<PlayerMicroformatRenderer>,
}

#[derive(Deserialize)]
struct PlayerMicroformatRenderer {
    #[serde(default, rename = "liveBroadcastDetails")]
    live_broadcast_details: Option<PlayerLiveBroadcastDetails>,
}

#[derive(Deserialize)]
struct PlayerLiveBroadcastDetails {
    #[serde(default, rename = "isLiveNow")]
    is_live_now: Option<bool>,
}

fn extract_initial_player_response(html: &str) -> Option<&str> {
    const MARKER: &str = "ytInitialPlayerResponse";
    const MAX_PLAYER_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

    let marker_end = html.find(MARKER)? + MARKER.len();
    let tail = html.get(marker_end..)?;
    let object_offset = tail.find('{')?;
    if !tail[..object_offset].chars().all(|character| {
        character.is_ascii_whitespace() || matches!(character, '=' | ':' | '"' | '\'' | ']')
    }) {
        return None;
    }
    let object_start = marker_end + object_offset;
    let bytes = html.as_bytes();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for index in object_start..bytes.len() {
        if index - object_start >= MAX_PLAYER_RESPONSE_BYTES {
            return None;
        }
        let byte = bytes[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' => depth = depth.checked_add(1)?,
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return html.get(object_start..=index);
                }
            }
            _ => {}
        }
    }
    None
}

fn waiting_state(
    status: &str,
    _message: &str,
    channel_url: &str,
    config: &AppConfig,
) -> DashboardState {
    DashboardState {
        session: SessionView {
            status: status.to_owned(),
            mode: "PAPER_ONLY".to_owned(),
            stream_url: channel_url.to_owned(),
            stream_title: "Trading Cafe India channel monitor".to_owned(),
            market_status: status.to_owned(),
            ..SessionView::default()
        },
        health: HealthView {
            overall: if status.contains("FAILED") || status.contains("DEGRADED") {
                "DEGRADED".to_owned()
            } else {
                "HEALTHY".to_owned()
            },
            youtube_discovery: stopped_component("Official YouTube discovery has not run yet"),
            stream_capture: stopped_component("Starts only after live discovery"),
            transcription: stopped_component("Starts only after live discovery"),
            analysis: stopped_component("Starts only after live discovery"),
            market_feed: stopped_component("Starts only after live discovery"),
            persistence: ComponentHealth {
                status: "READY".to_owned(),
                message: "Durable storage initializes with a live session".to_owned(),
                ..ComponentHealth::default()
            },
            api_keys: (1..=config.elevenlabs.api_keys.len())
                .map(|slot| ApiKeyHealthView {
                    provider: "ElevenLabs".to_owned(),
                    slot,
                    status: "READY".to_owned(),
                    ..ApiKeyHealthView::default()
                })
                .collect(),
            ..HealthView::default()
        },
        ..DashboardState::empty()
    }
}

fn apply_waiting_status(
    state: &mut DashboardState,
    status: &str,
    message: &str,
    channel_url: &str,
    config: &AppConfig,
    storage_degraded: bool,
) {
    let waiting = with_storage_health(
        waiting_state(status, message, channel_url, config),
        storage_degraded,
    );
    state.session = waiting.session;
    state.health = waiting.health;
}

async fn publish_waiting_status(
    handle: &DashboardHandle,
    status: &str,
    message: &str,
    channel_url: &str,
    config: &AppConfig,
    storage_degraded: bool,
) {
    handle
        .update("scheduler_status", None, |state| {
            let preserve_discovery = (!state.health.youtube_discovery.status.is_empty())
                .then(|| state.health.youtube_discovery.clone());
            apply_waiting_status(
                state,
                status,
                message,
                channel_url,
                config,
                storage_degraded,
            );
            if let Some(discovery) = preserve_discovery {
                state.health.youtube_discovery = discovery;
            }
        })
        .await;
}

fn stopped_component(message: &str) -> ComponentHealth {
    ComponentHealth {
        status: "IDLE".to_owned(),
        message: message.to_owned(),
        ..ComponentHealth::default()
    }
}

fn component_health(status: &str, message: &str) -> ComponentHealth {
    ComponentHealth {
        status: status.to_owned(),
        message: message.to_owned(),
        ..ComponentHealth::default()
    }
}

fn with_storage_health(mut state: DashboardState, degraded: bool) -> DashboardState {
    if degraded {
        state.health.persistence = component_health(
            "DEGRADED",
            "durable paper state is unavailable; configured fallback wallets are displayed",
        );
        state.health.overall = "DEGRADED".to_owned();
    }
    state
}

async fn purge_raw_session_data(
    project_dir: &Path,
    data_dir: &Path,
    media_dir: &Path,
) -> Result<()> {
    let project = canonical_or_absolute(project_dir)?;
    let data = canonical_or_absolute(data_dir)?;
    if !data.starts_with(&project) {
        bail!("refusing raw-data cleanup outside the project data directory");
    }
    for target in [
        media_dir.to_path_buf(),
        data_dir.join("live"),
        data_dir.join("paper").join("sessions"),
    ] {
        let absolute = canonical_or_absolute(&target)?;
        if !absolute.starts_with(&data) || absolute == data {
            bail!(
                "refusing unsafe raw-data cleanup target {}",
                absolute.display()
            );
        }
        match tokio::fs::remove_dir_all(&absolute).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("could not clear {}", absolute.display()));
            }
        }
        tokio::fs::create_dir_all(&absolute).await?;
    }
    Ok(())
}

fn canonical_or_absolute(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return path
            .canonicalize()
            .with_context(|| format!("could not resolve {}", path.display()));
    }
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()?.join(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AppConfig {
        AppConfig::from_values(
            "C:/project",
            [
                ("OPENAI_API_KEY", "test-analysis-key"),
                ("ELEVENLABS_API_KEY", "test-elevenlabs-key"),
            ],
        )
        .unwrap()
    }

    #[test]
    fn weekends_and_configured_holidays_are_closed() {
        let holiday = NaiveDate::from_ymd_opt(2026, 1, 26).unwrap();
        assert!(!is_trading_day(holiday, &[holiday]));
        assert!(!is_trading_day(
            NaiveDate::from_ymd_opt(2026, 8, 16).unwrap(),
            &[]
        ));
        assert!(is_trading_day(
            NaiveDate::from_ymd_opt(2026, 8, 11).unwrap(),
            &[]
        ));
    }

    #[test]
    fn waiting_status_preserves_durable_desk_data() {
        let config = test_config();
        let mut state = DashboardState::empty();
        state.accounts.push(Default::default());
        state.positions.push(Default::default());
        state.pending_orders.push(Default::default());
        state.metrics.starting_capital = 104_000.0;
        state.signals.push(Default::default());
        state.equity_curve.push(Default::default());
        state.history.push(Default::default());
        state.logs.push(Default::default());
        let durable = (
            state.accounts.clone(),
            state.positions.clone(),
            state.pending_orders.clone(),
            state.metrics.clone(),
            state.signals.clone(),
            state.equity_curve.clone(),
            state.history.clone(),
            state.logs.clone(),
        );

        apply_waiting_status(
            &mut state,
            "WORKERS_STOPPED",
            "Trading workers are offline",
            "https://www.youtube.com/@TRADINGCAFEINDIA",
            &config,
            false,
        );

        assert_eq!(state.session.status, "WORKERS_STOPPED");
        assert_eq!(state.accounts, durable.0);
        assert_eq!(state.positions, durable.1);
        assert_eq!(state.pending_orders, durable.2);
        assert_eq!(state.metrics, durable.3);
        assert_eq!(state.signals, durable.4);
        assert_eq!(state.equity_curve, durable.5);
        assert_eq!(state.history, durable.6);
        assert_eq!(state.logs, durable.7);
    }

    #[test]
    fn degraded_waiting_status_keeps_fallback_wallets_visible() {
        let config = test_config();
        let mut state = DashboardState::empty();
        state.accounts.resize_with(10, Default::default);

        apply_waiting_status(
            &mut state,
            "WORKERS_STOPPED",
            "Trading workers are offline",
            "https://www.youtube.com/@TRADINGCAFEINDIA",
            &config,
            true,
        );

        assert_eq!(state.accounts.len(), 10);
        assert_eq!(state.health.overall, "DEGRADED");
        assert_eq!(state.health.persistence.status, "DEGRADED");
        assert_eq!(
            state.health.persistence.message,
            "durable paper state is unavailable; configured fallback wallets are displayed"
        );
    }

    #[test]
    fn live_page_fallback_extracts_only_an_active_youtube_broadcast() {
        let html = r#"<link rel="canonical" href="https://www.youtube.com/watch?v=gLc-pEPGZjI">
            <script>var ytInitialPlayerResponse = {
              "videoDetails":{"videoId":"gLc-pEPGZjI","isLive":true},
              "microformat":{"playerMicroformatRenderer":{"liveBroadcastDetails":{"isLiveNow":true}}}
            };</script>"#;
        assert_eq!(
            parse_live_page(html),
            Some("https://www.youtube.com/watch?v=gLc-pEPGZjI".to_owned())
        );

        let ended = r#"<link rel="canonical" href="https://www.youtube.com/watch?v=gLc-pEPGZjI">
            <meta itemprop="isLiveBroadcast" content="True">
            <script>var ytInitialPlayerResponse = {
              "videoDetails":{"videoId":"gLc-pEPGZjI","isLive":false},
              "microformat":{"playerMicroformatRenderer":{"liveBroadcastDetails":{"isLiveNow":false}}}
            };</script>"#;
        assert_eq!(parse_live_page(ended), None);
    }

    #[test]
    fn live_page_ignores_an_unrelated_recommended_live_card() {
        let archive_with_live_recommendation = r#"
          <link rel="canonical" href="https://www.youtube.com/watch?v=gLc-pEPGZjI">
          <meta itemprop="isLiveBroadcast" content="True">
          <script>var ytInitialPlayerResponse = {
            "videoDetails":{"videoId":"gLc-pEPGZjI","isLive":false},
            "microformat":{"playerMicroformatRenderer":{"liveBroadcastDetails":{"isLiveNow":false}}}
          };</script>
          <script>window.recommended = {"videoId":"live1234567","isLiveNow":true};</script>
        "#;

        assert_eq!(parse_live_page(archive_with_live_recommendation), None);
    }

    #[test]
    fn live_page_requires_player_video_id_to_match_the_canonical_video() {
        let mismatched = r#"
          <link rel="canonical" href="https://www.youtube.com/watch?v=gLc-pEPGZjI">
          <script>var ytInitialPlayerResponse = {
            "videoDetails":{"videoId":"live1234567","isLive":true}
          };</script>
        "#;

        assert_eq!(parse_live_page(mismatched), None);
    }

    #[test]
    fn yt_dlp_failure_detail_is_bounded_and_redacts_urls() {
        let detail = bounded_yt_dlp_failure_detail(
            b"ERROR: Sign in to confirm you\x1b[31m are not a bot\nhttps://example.test/secret?token=abc",
        );

        assert_eq!(
            detail,
            "ERROR: Sign in to confirm you are not a bot [URL redacted]"
        );
    }

    #[test]
    fn youtube_client_fallback_uses_a_live_stream_compatible_client() {
        assert_eq!(
            youtube_player_client_fallback(),
            "youtube:player_client=android_vr"
        );
    }

    #[test]
    fn authoritative_api_results_skip_legacy_discovery_but_unavailability_falls_back() {
        assert_eq!(
            api_first_result(&crate::youtube::ApiDiscovery::Live(
                "https://www.youtube.com/watch?v=live1234567".to_owned()
            )),
            Some(Some(
                "https://www.youtube.com/watch?v=live1234567".to_owned()
            ))
        );
        assert_eq!(
            api_first_result(&crate::youtube::ApiDiscovery::NotLive),
            Some(None)
        );
        assert_eq!(api_first_result(&crate::youtube::ApiDiscovery::NoKey), None);
        assert_eq!(
            api_first_result(&crate::youtube::ApiDiscovery::Unavailable(
                "provider unavailable".to_owned()
            )),
            None
        );
    }

    #[test]
    fn deferred_official_search_tries_only_the_lightweight_page_fallback() {
        let outcome = crate::youtube::ApiDiscovery::Indeterminate;

        assert_eq!(api_first_result(&outcome), None);
        assert!(!allow_yt_dlp_fallback(&outcome));
        assert!(allow_yt_dlp_fallback(&crate::youtube::ApiDiscovery::NoKey));
        assert!(allow_yt_dlp_fallback(
            &crate::youtube::ApiDiscovery::Unavailable("provider unavailable".to_owned())
        ));
    }

    #[test]
    fn waiting_state_keeps_discovery_health_separate_from_capture_health() {
        let state = waiting_state(
            "CHECKING_CHANNEL",
            "Checking official YouTube live status",
            "https://www.youtube.com/@TRADINGCAFEINDIA",
            &test_config(),
        );

        assert_eq!(state.health.youtube_discovery.status, "IDLE");
        assert_eq!(state.health.stream_capture.status, "IDLE");
    }

    #[tokio::test]
    async fn generic_waiting_update_preserves_the_specific_api_discovery_result() {
        let handle = DashboardHandle::empty();
        handle
            .update("test", None, |state| {
                state.health.youtube_discovery =
                    component_health("KEY_REQUIRED", "load the runtime API key");
            })
            .await;

        publish_waiting_status(
            &handle,
            "DISCOVERY_DEGRADED",
            "Legacy resolver failed",
            "https://www.youtube.com/@TRADINGCAFEINDIA",
            &test_config(),
            false,
        )
        .await;

        assert_eq!(
            handle.snapshot().await.health.youtube_discovery.status,
            "KEY_REQUIRED"
        );
    }

    #[tokio::test]
    async fn session_failure_does_not_erase_live_discovery_proof() {
        let handle = DashboardHandle::empty();
        handle
            .update("test", None, |state| {
                state.health.youtube_discovery = component_health(
                    "LIVE_FOUND",
                    "Official YouTube Data API confirmed an active live broadcast",
                );
            })
            .await;

        publish_waiting_status(
            &handle,
            "SESSION_FAILED",
            "Paper session stopped safely",
            "https://www.youtube.com/@TRADINGCAFEINDIA",
            &test_config(),
            false,
        )
        .await;

        assert_eq!(
            handle.snapshot().await.health.youtube_discovery.status,
            "LIVE_FOUND"
        );
    }

    #[tokio::test]
    async fn generic_end_of_day_states_do_not_erase_live_discovery_proof() {
        for status in ["DISCOVERY_CLOSED", "WORKERS_STOPPED"] {
            let handle = DashboardHandle::empty();
            handle
                .update("test", None, |state| {
                    state.health.youtube_discovery = component_health(
                        "LIVE_FOUND",
                        "Official YouTube Data API confirmed an active live broadcast",
                    );
                })
                .await;

            publish_waiting_status(
                &handle,
                status,
                "Generic scheduler state",
                "https://www.youtube.com/@TRADINGCAFEINDIA",
                &test_config(),
                false,
            )
            .await;

            let discovery = handle.snapshot().await.health.youtube_discovery;
            assert_eq!(discovery.status, "LIVE_FOUND", "incoming {status}");
            assert_eq!(
                discovery.message,
                "Official YouTube Data API confirmed an active live broadcast"
            );
        }
    }

    #[tokio::test]
    async fn explicit_discovery_writer_can_replace_previous_proof() {
        let handle = DashboardHandle::empty();
        handle
            .update("test", None, |state| {
                state.health.youtube_discovery = component_health(
                    "LIVE_FOUND",
                    "Official YouTube Data API confirmed an active live broadcast",
                );
            })
            .await;

        publish_youtube_discovery_health(
            &handle,
            "CHECKING_CHANNEL",
            "Checking official YouTube live status",
        )
        .await;

        assert_eq!(
            handle.snapshot().await.health.youtube_discovery.status,
            "CHECKING_CHANNEL"
        );
    }
}
