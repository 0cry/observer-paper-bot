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
};

pub async fn run(project_dir: &Path, http: Client) -> Result<()> {
    let config = AppConfig::load(project_dir).context("daemon configuration is invalid")?;
    let channel_url = config
        .scheduler
        .youtube_channel_url
        .clone()
        .ok_or_else(|| anyhow::anyhow!("YOUTUBE_CHANNEL_URL is required for daemon mode"))?;
    let initial = waiting_state(
        "STARTING",
        "IST scheduler initializing",
        &channel_url,
        &config,
    );
    let handle = DashboardHandle::new(initial);
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
    let runtime_logger = RuntimeEventLogger::new(handle.clone(), log_store);
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
                    "durable operational log storage is unavailable; in-memory logs remain active",
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
            handle
                .replace(with_storage_health(
                    waiting_state(
                        "MARKET_CLOSED",
                        "Weekend or configured NSE F&O holiday",
                        &channel_url,
                        &config,
                    ),
                    storage_degraded,
                ))
                .await;
            sleep(Duration::from_secs(60)).await;
            continue;
        }
        if time < config.scheduler.poll_start_ist {
            handle
                .replace(with_storage_health(
                    waiting_state(
                        "WAITING_FOR_09_00_IST",
                        "Market-day supervisor is idle until 09:00 IST",
                        &channel_url,
                        &config,
                    ),
                    storage_degraded,
                ))
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
            handle
                .replace(with_storage_health(
                    waiting_state(
                        status,
                        "No new stream discovery after 15:30 IST; workers remain stopped",
                        &channel_url,
                        &config,
                    ),
                    storage_degraded,
                ))
                .await;
            sleep(Duration::from_secs(60)).await;
            continue;
        }

        handle
            .replace(with_storage_health(
                waiting_state(
                    "CHECKING_CHANNEL",
                    "Checking Trading Cafe India for a current live stream",
                    &channel_url,
                    &config,
                ),
                storage_degraded,
            ))
            .await;
        match discover_live_url(&config.paths.yt_dlp_path, &channel_url).await {
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
                    handle
                        .replace(with_storage_health(
                            waiting_state(
                                "SESSION_FAILED",
                                &format!("Paper session stopped safely: {error}"),
                                &channel_url,
                                &config,
                            ),
                            storage_degraded,
                        ))
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
                handle
                    .replace(with_storage_health(
                        waiting_state(
                            "WAITING_FOR_LIVE",
                            "Channel is not live; next check follows the configured interval",
                            &channel_url,
                            &config,
                        ),
                        storage_degraded,
                    ))
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
                handle
                    .replace(with_storage_health(
                        waiting_state(
                            "DISCOVERY_DEGRADED",
                            &format!("YouTube discovery failed safely: {error}"),
                            &channel_url,
                            &config,
                        ),
                        storage_degraded,
                    ))
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

async fn discover_live_url(yt_dlp: &Path, channel_url: &str) -> Result<Option<String>> {
    let live_page = format!("{}/live", channel_url.trim_end_matches('/'));
    let mut command = Command::new(yt_dlp);
    command
        .arg("--no-warnings")
        .arg("--no-playlist")
        .arg("--skip-download")
        .arg("--print")
        .arg("%(id)s\t%(live_status)s\t%(webpage_url)s")
        .arg(live_page)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let output = timeout(Duration::from_secs(45), command.output())
        .await
        .context("yt-dlp discovery timed out")?
        .context("could not execute yt-dlp discovery")?;
    if !output.status.success() {
        return Ok(None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() >= 3 && fields[1].eq_ignore_ascii_case("is_live") {
            return Ok(Some(fields[2].trim().to_owned()));
        }
    }
    Ok(None)
}

fn waiting_state(
    status: &str,
    message: &str,
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
            stream_capture: ComponentHealth {
                status: status.to_owned(),
                message: message.to_owned(),
                ..ComponentHealth::default()
            },
            transcription: stopped_component("Starts only after live discovery"),
            gemini: stopped_component("Starts only after live discovery"),
            market_feed: stopped_component("Starts only after live discovery"),
            persistence: ComponentHealth {
                status: "READY".to_owned(),
                message: "Durable storage initializes with a live session".to_owned(),
                ..ComponentHealth::default()
            },
            api_keys: (1..=config.gemini.api_keys.len())
                .map(|slot| ApiKeyHealthView {
                    provider: "Gemini".to_owned(),
                    slot,
                    status: "READY".to_owned(),
                    ..ApiKeyHealthView::default()
                })
                .chain(
                    (1..=config.elevenlabs.api_keys.len()).map(|slot| ApiKeyHealthView {
                        provider: "ElevenLabs".to_owned(),
                        slot,
                        status: "READY".to_owned(),
                        ..ApiKeyHealthView::default()
                    }),
                )
                .collect(),
            ..HealthView::default()
        },
        ..DashboardState::empty()
    }
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
            "durable operational log storage is unavailable; in-memory logs remain active",
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
}
