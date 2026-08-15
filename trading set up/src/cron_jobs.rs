//! Durable, ordinary scheduled HTTPS GET jobs. They are intentionally
//! separate from the trading scheduler and cannot execute shell commands.

use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    time::Duration,
};

use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Utc};
use chrono_tz::Asia::Kolkata;
use cron::Schedule;
use reqwest::{Client, Url, redirect::Policy};
use serde::{Deserialize, Serialize};

use crate::neon::{CronJobRow, CronRunRow, NeonStore};

pub const CRON_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
pub const CRON_CLAIM_SECONDS: i64 = 30;
pub const MAX_CRON_LABEL_CHARS: usize = 80;
pub const MAX_CRON_URL_CHARS: usize = 2_048;
const INVALID_SCHEDULE_RETRY_MINUTES: i64 = 15;

#[derive(Debug, Clone, Deserialize)]
pub struct CreateCronJob {
    pub label: String,
    pub expression: String,
    pub target_url: String,
}

pub fn validate_create(input: CreateCronJob, now: DateTime<Utc>) -> Result<NewCronJob> {
    let label = input.label.trim();
    if label.is_empty() || label.chars().count() > MAX_CRON_LABEL_CHARS {
        bail!("cron label must be 1 to {MAX_CRON_LABEL_CHARS} characters");
    }
    let target_url = validate_https_target(&input.target_url)?;
    let expression = normalize_cron_expression(&input.expression)?;
    let next_run_at = next_run_ist(&expression, now)?;
    Ok(NewCronJob {
        label: label.to_owned(),
        expression,
        target_url,
        next_run_at,
    })
}

#[derive(Debug, Clone)]
pub struct NewCronJob {
    pub label: String,
    pub expression: String,
    pub target_url: String,
    pub next_run_at: DateTime<Utc>,
}

pub fn validate_https_target(raw: &str) -> Result<String> {
    let raw = raw.trim();
    if raw.is_empty() || raw.chars().count() > MAX_CRON_URL_CHARS {
        bail!("cron URL must be present and at most {MAX_CRON_URL_CHARS} characters");
    }
    let parsed = Url::parse(raw).map_err(|_| anyhow!("cron URL is invalid"))?;
    if parsed.scheme() != "https" || !parsed.username().is_empty() || parsed.password().is_some() {
        bail!("cron target must be a public HTTPS URL without user credentials");
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("cron URL must include a host"))?;
    if host.eq_ignore_ascii_case("localhost") || host.ends_with(".local") {
        bail!("cron target must not address localhost");
    }
    if let Ok(ip) = IpAddr::from_str(host) {
        if !is_public_ip(ip) {
            bail!("cron target must not address a private network");
        }
    }
    Ok(parsed.into())
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_documentation())
        }
        IpAddr::V6(ip) => {
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local())
        }
    }
}

fn all_resolved_addresses_are_public(addresses: impl IntoIterator<Item = SocketAddr>) -> bool {
    let mut any = false;
    for address in addresses {
        any = true;
        if !is_public_ip(address.ip()) {
            return false;
        }
    }
    any
}

async fn resolve_public_target(target_url: &str) -> Result<()> {
    let parsed = Url::parse(target_url).map_err(|_| anyhow!("cron URL is invalid"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("cron URL must include a host"))?;
    let port = parsed.port_or_known_default().unwrap_or(443);
    let addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| anyhow!("cron hostname could not be resolved"))?
        .collect::<Vec<_>>();
    if !all_resolved_addresses_are_public(addresses) {
        bail!("cron hostname resolved to a non-public network");
    }
    Ok(())
}

/// Converts ordinary five-field cron syntax to the crate's six-field form by
/// pinning seconds to zero. All expressions are evaluated in Asia/Kolkata.
pub fn normalize_cron_expression(raw: &str) -> Result<String> {
    let fields = raw.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 {
        bail!("cron expression must have five fields: minute hour day month weekday");
    }
    let normalized = format!("0 {}", fields.join(" "));
    Schedule::from_str(&normalized).map_err(|_| anyhow!("cron expression is invalid"))?;
    Ok(normalized)
}

pub fn next_run_ist(expression: &str, now: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let schedule =
        Schedule::from_str(expression).map_err(|_| anyhow!("cron expression is invalid"))?;
    schedule
        .after(&now.with_timezone(&Kolkata))
        .next()
        .map(|next| next.with_timezone(&Utc))
        .ok_or_else(|| anyhow!("cron expression has no future occurrence"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompletionSchedule {
    next_run_at: DateTime<Utc>,
    status: &'static str,
    error: Option<&'static str>,
}

/// A corrupted database row must never abort the runner's remaining jobs.
/// The corrupted job is surfaced and retried later, after an operator repairs
/// or deletes it through the dashboard.
fn completion_schedule(expression: &str, now: DateTime<Utc>) -> CompletionSchedule {
    match next_run_ist(expression, now) {
        Ok(next_run_at) => CompletionSchedule {
            next_run_at,
            status: "OK",
            error: None,
        },
        Err(_) => CompletionSchedule {
            next_run_at: now + chrono::Duration::minutes(INVALID_SCHEDULE_RETRY_MINUTES),
            status: "SCHEDULE_ERROR",
            error: Some("stored cron expression is invalid"),
        },
    }
}

pub async fn execute_due_jobs(store: &NeonStore, now: DateTime<Utc>) -> Result<()> {
    let claimed = store.claim_due_cron_jobs(now, CRON_CLAIM_SECONDS).await?;
    if claimed.is_empty() {
        return Ok(());
    }
    let client = Client::builder()
        .redirect(Policy::none())
        .timeout(CRON_REQUEST_TIMEOUT)
        .build()?;
    for job in claimed {
        let schedule = completion_schedule(&job.expression, now);
        if schedule.status == "SCHEDULE_ERROR" {
            store
                .finish_cron_job_run(
                    job.id,
                    schedule.next_run_at,
                    schedule.status,
                    None,
                    0,
                    schedule.error,
                )
                .await?;
            continue;
        }
        let started = std::time::Instant::now();
        let result = match resolve_public_target(&job.target_url).await {
            Ok(()) => client.get(&job.target_url).send().await,
            Err(_) => {
                let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as i64;
                store
                    .finish_cron_job_run(
                        job.id,
                        schedule.next_run_at,
                        "TARGET_REJECTED",
                        None,
                        duration_ms,
                        Some("cron target could not be reached safely"),
                    )
                    .await?;
                continue;
            }
        };
        let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as i64;
        let (status, http_status, error) = match result {
            Ok(response) => (
                if response.status().is_success() {
                    "OK"
                } else {
                    "HTTP_ERROR"
                },
                Some(i32::from(response.status().as_u16())),
                None,
            ),
            Err(_) => (
                "TRANSPORT_ERROR",
                None,
                Some("HTTPS request failed".to_owned()),
            ),
        };
        store
            .finish_cron_job_run(
                job.id,
                schedule.next_run_at,
                status,
                http_status,
                duration_ms,
                error.as_deref(),
            )
            .await?;
    }
    Ok(())
}

pub fn row_to_public(row: CronJobRow) -> CronJobView {
    CronJobView {
        id: row.id,
        label: row.label,
        expression: row.expression.trim_start_matches("0 ").to_owned(),
        target_url: row.target_url,
        enabled: row.enabled,
        next_run_at: row.next_run_at.to_rfc3339(),
        last_status: row.last_status,
        last_http_status: row.last_http_status,
        last_duration_ms: row.last_duration_ms,
        last_error: row.last_error,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CronJobView {
    pub id: i64,
    pub label: String,
    pub expression: String,
    pub target_url: String,
    pub enabled: bool,
    pub next_run_at: String,
    pub last_status: Option<String>,
    pub last_http_status: Option<i32>,
    pub last_duration_ms: Option<i64>,
    pub last_error: Option<String>,
}

pub fn run_to_public(row: CronRunRow) -> CronRunView {
    CronRunView {
        id: row.id,
        occurred_at: row.occurred_at.to_rfc3339(),
        status: row.status,
        http_status: row.http_status,
        duration_ms: row.duration_ms,
        error: row.error,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CronRunView {
    pub id: i64,
    pub occurred_at: String,
    pub status: String,
    pub http_status: Option<i32>,
    pub duration_ms: i64,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Timelike};

    #[test]
    fn accepts_public_https_and_rejects_http_or_private_targets() {
        assert_eq!(
            validate_https_target("https://observer.onrender.com/health").unwrap(),
            "https://observer.onrender.com/health"
        );
        assert!(validate_https_target("http://observer.onrender.com/health").is_err());
        assert!(validate_https_target("https://127.0.0.1/health").is_err());
    }

    #[test]
    fn private_or_special_dns_answers_are_never_eligible_for_a_cron_request() {
        assert!(all_resolved_addresses_are_public(["8.8.8.8:443"
            .parse()
            .unwrap()]));
        assert!(!all_resolved_addresses_are_public(["127.0.0.1:443"
            .parse()
            .unwrap()]));
        assert!(!all_resolved_addresses_are_public(["169.254.169.254:443"
            .parse()
            .unwrap()]));
    }

    #[test]
    fn five_field_schedule_is_evaluated_in_ist() {
        let now = Utc.with_ymd_and_hms(2026, 8, 16, 3, 29, 0).unwrap(); // 08:59 IST Sunday
        let expression = normalize_cron_expression("0 9 * * *").unwrap();
        let next = next_run_ist(&expression, now)
            .unwrap()
            .with_timezone(&Kolkata);
        assert_eq!(next.hour(), 9);
        assert_eq!(next.minute(), 0);
    }

    #[test]
    fn invalid_persisted_schedule_is_isolated_with_a_retry_time() {
        let now = Utc.with_ymd_and_hms(2026, 8, 16, 3, 29, 0).unwrap();
        let decision = completion_schedule("definitely invalid", now);
        assert_eq!(decision.status, "SCHEDULE_ERROR");
        assert_eq!(decision.next_run_at, now + chrono::Duration::minutes(15));
        assert!(decision.error.is_some());
    }
}
