//! Best-effort, credential-safe operational logging for the public dashboard.

use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::Result;
use chrono::Utc;
use chrono_tz::Asia::Kolkata;

use crate::{
    dashboard::{DashboardHandle, RuntimeLogEntry, sanitize_log_message},
    neon::{NeonStore, ServiceEventRow},
};

static NEXT_EVENT_ID: AtomicI64 = AtomicI64::new(0);

#[derive(Clone)]
pub struct RuntimeEventLogger {
    dashboard: DashboardHandle,
    store: Option<NeonStore>,
}

impl RuntimeEventLogger {
    pub fn new(dashboard: DashboardHandle, store: Option<NeonStore>) -> Self {
        Self { dashboard, store }
    }

    pub async fn load_recent(&self, limit: usize) -> Result<usize> {
        let Some(store) = self.store.as_ref() else {
            return Ok(0);
        };
        let rows = store.list_service_events(Some(limit)).await?;
        let count = rows.len();
        self.dashboard
            .replace_logs(rows.into_iter().map(service_event_to_runtime_log).collect())
            .await;
        Ok(count)
    }

    /// Updates the in-memory dashboard before returning. Durable persistence is
    /// deliberately detached so a slow or unavailable database cannot stall
    /// stream capture, transcription, model analysis, or tick processing.
    pub async fn record(&self, level: &str, component: &str, code: &str, message: &str) {
        let sanitized = sanitize_log_message(message);
        let now = Utc::now();
        self.dashboard
            .record_log(RuntimeLogEntry {
                event_id: next_event_id(now.timestamp_micros()),
                occurred_at: now.to_rfc3339(),
                occurred_at_ist: now.with_timezone(&Kolkata).to_rfc3339(),
                level: level.to_owned(),
                component: component.to_owned(),
                code: code.to_owned(),
                message: sanitized.clone(),
            })
            .await;

        if let Some(store) = self.store.clone() {
            let component = component.to_owned();
            let level = level.to_owned();
            let code = code.to_owned();
            tokio::spawn(async move {
                let _ = store
                    .record_service_event(&component, &level, &code, &sanitized)
                    .await;
            });
        }
    }
}

fn next_event_id(timestamp_micros: i64) -> i64 {
    let mut observed = NEXT_EVENT_ID.load(Ordering::Relaxed);
    loop {
        let candidate = timestamp_micros.max(observed.saturating_add(1));
        match NEXT_EVENT_ID.compare_exchange_weak(
            observed,
            candidate,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return candidate,
            Err(actual) => observed = actual,
        }
    }
}

pub(crate) fn service_event_to_runtime_log(row: ServiceEventRow) -> RuntimeLogEntry {
    RuntimeLogEntry {
        event_id: row.id,
        occurred_at: row.occurred_at.to_rfc3339(),
        occurred_at_ist: row.occurred_at.with_timezone(&Kolkata).to_rfc3339(),
        level: row.level,
        component: row.service,
        code: row.code,
        message: sanitize_log_message(&row.message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::{DashboardHandle, DashboardState};

    #[tokio::test]
    async fn records_a_sanitized_dashboard_event_without_durable_storage() {
        let handle = DashboardHandle::new(DashboardState::empty());
        let logger = RuntimeEventLogger::new(handle.clone(), None);

        logger
            .record(
                "ERROR",
                "analysis",
                "REQUEST_FAILED",
                "request failed with AIza-secret-value at https://example.test/path?token=secret",
            )
            .await;

        let logs = handle.snapshot().await.logs;
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].component, "analysis");
        assert_eq!(logs[0].code, "REQUEST_FAILED");
        assert!(logs[0].message.contains("[REDACTED]"));
        assert!(logs[0].message.contains("https://example.test/path"));
        assert!(!logs[0].message.contains("secret-value"));
        assert!(!logs[0].message.contains("token=secret"));
    }

    #[tokio::test]
    async fn redacts_openai_standard_and_project_secret_shapes() {
        let handle = DashboardHandle::new(DashboardState::empty());
        let logger = RuntimeEventLogger::new(handle.clone(), None);
        let standard = format!("sk-{}", "x".repeat(32));
        let project = format!("sk-proj-{}", "y".repeat(32));

        logger
            .record(
                "ERROR",
                "analysis",
                "REQUEST_FAILED",
                &format!("provider rejected {standard} and {project}"),
            )
            .await;

        let message = &handle.snapshot().await.logs[0].message;
        assert!(message.contains("[REDACTED]"));
        assert!(!message.contains(&standard));
        assert!(!message.contains(&project));
    }

    #[test]
    fn durable_rows_map_to_public_log_shape() {
        let occurred_at = chrono::DateTime::parse_from_rfc3339("2026-08-11T04:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let entry = service_event_to_runtime_log(crate::neon::ServiceEventRow {
            id: 42,
            occurred_at,
            service: "scheduler".to_owned(),
            level: "INFO".to_owned(),
            code: "DAEMON_READY".to_owned(),
            message: "ready".to_owned(),
        });

        assert_eq!(entry.event_id, 42);
        assert_eq!(entry.occurred_at, "2026-08-11T04:00:00+00:00");
        assert_eq!(entry.occurred_at_ist, "2026-08-11T09:30:00+05:30");
    }
}
