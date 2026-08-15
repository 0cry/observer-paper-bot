//! Small durable PostgreSQL repository for Render-safe paper state.
//!
//! Raw media, transcripts, ticks, prompts, and credentials are deliberately
//! excluded. The runtime snapshot is a single JSONB row so broker state and
//! rolling context recover from the same commit point.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};

const SCHEMA_VERSION: i32 = 1;
const DEFAULT_SERVICE_EVENT_LIMIT: usize = 100;
const MAX_SERVICE_EVENT_LIMIT: usize = 200;

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ServiceEventRow {
    pub id: i64,
    pub occurred_at: chrono::DateTime<chrono::Utc>,
    pub service: String,
    pub level: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CronJobRow {
    pub id: i64,
    pub label: String,
    /// Internal six-field expression with seconds pinned to zero.
    pub expression: String,
    pub target_url: String,
    pub enabled: bool,
    pub next_run_at: chrono::DateTime<chrono::Utc>,
    pub last_status: Option<String>,
    pub last_http_status: Option<i32>,
    pub last_duration_ms: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CronRunRow {
    pub id: i64,
    pub occurred_at: chrono::DateTime<chrono::Utc>,
    pub status: String,
    pub http_status: Option<i32>,
    pub duration_ms: i64,
    pub error: Option<String>,
}

pub(crate) fn normalize_service_event_limit(limit: Option<usize>) -> Result<usize> {
    let limit = limit.unwrap_or(DEFAULT_SERVICE_EVENT_LIMIT);
    if limit == 0 || limit > MAX_SERVICE_EVENT_LIMIT {
        bail!("service event limit must be between 1 and {MAX_SERVICE_EVENT_LIMIT}");
    }
    Ok(limit)
}

#[derive(Clone)]
pub struct NeonStore {
    pool: PgPool,
}

impl NeonStore {
    pub async fn connect(database_url: &str) -> Result<Self> {
        if database_url.trim().is_empty() {
            bail!("DATABASE_URL must not be empty");
        }
        let pool = PgPoolOptions::new()
            .max_connections(3)
            .min_connections(0)
            .acquire_timeout(Duration::from_secs(12))
            .idle_timeout(Duration::from_secs(120))
            .connect(database_url)
            .await
            .context("could not connect to durable PostgreSQL storage")?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<()> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS runtime_state (
                state_key TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
        )
        .execute(&self.pool)
        .await
        .context("could not create runtime_state")?;
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                trading_date DATE NOT NULL,
                account_id TEXT NOT NULL,
                strategy TEXT NOT NULL,
                closed_at TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
        )
        .execute(&self.pool)
        .await
        .context("could not create trades")?;
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS daily_accounts (
                trading_date DATE NOT NULL,
                account_id TEXT NOT NULL,
                strategy TEXT NOT NULL,
                payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (trading_date, account_id, strategy)
            )"#,
        )
        .execute(&self.pool)
        .await
        .context("could not create daily_accounts")?;
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS service_events (
                id BIGSERIAL PRIMARY KEY,
                occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                service TEXT NOT NULL,
                level TEXT NOT NULL,
                code TEXT NOT NULL,
                message TEXT NOT NULL
            )"#,
        )
        .execute(&self.pool)
        .await
        .context("could not create service_events")?;
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS cron_jobs (
                id BIGSERIAL PRIMARY KEY,
                label TEXT NOT NULL,
                expression TEXT NOT NULL,
                target_url TEXT NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                next_run_at TIMESTAMPTZ NOT NULL,
                claimed_until TIMESTAMPTZ,
                last_status TEXT,
                last_http_status INTEGER,
                last_duration_ms BIGINT,
                last_error TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"#,
        )
        .execute(&self.pool)
        .await
        .context("could not create cron_jobs")?;
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS cron_job_runs (
                id BIGSERIAL PRIMARY KEY,
                cron_job_id BIGINT NOT NULL REFERENCES cron_jobs(id) ON DELETE CASCADE,
                occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                status TEXT NOT NULL,
                http_status INTEGER,
                duration_ms BIGINT NOT NULL,
                error TEXT
            )"#,
        )
        .execute(&self.pool)
        .await
        .context("could not create cron_job_runs")?;
        Ok(())
    }

    pub async fn ping(&self) -> Result<()> {
        let value: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .context("durable PostgreSQL ping failed")?;
        if value != 1 {
            bail!("durable PostgreSQL returned an invalid health value");
        }
        Ok(())
    }

    pub async fn load_runtime_state<T: DeserializeOwned>(
        &self,
        state_key: &str,
    ) -> Result<Option<T>> {
        let row: Option<(i32, Value)> = sqlx::query_as(
            "SELECT schema_version, payload FROM runtime_state WHERE state_key = $1",
        )
        .bind(state_key)
        .fetch_optional(&self.pool)
        .await
        .context("could not load runtime state")?;
        let Some((schema_version, payload)) = row else {
            return Ok(None);
        };
        if schema_version != SCHEMA_VERSION {
            bail!("unsupported durable runtime schema version {schema_version}");
        }
        serde_json::from_value(payload)
            .context("durable runtime state contains invalid JSON")
            .map(Some)
    }

    pub async fn save_runtime_state<T: Serialize>(&self, state_key: &str, state: &T) -> Result<()> {
        let payload = serde_json::to_value(state).context("could not serialize runtime state")?;
        sqlx::query(
            r#"INSERT INTO runtime_state (state_key, schema_version, payload, updated_at)
               VALUES ($1, $2, $3, now())
               ON CONFLICT (state_key) DO UPDATE SET
                 schema_version = EXCLUDED.schema_version,
                 payload = EXCLUDED.payload,
                 updated_at = now()"#,
        )
        .bind(state_key)
        .bind(SCHEMA_VERSION)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("could not save runtime state")?;
        Ok(())
    }

    pub async fn upsert_trade<T: Serialize>(
        &self,
        trade_id: &str,
        trading_date: chrono::NaiveDate,
        account_id: &str,
        strategy: &str,
        closed_at: chrono::DateTime<chrono::Utc>,
        trade: &T,
    ) -> Result<()> {
        let payload = serde_json::to_value(trade).context("could not serialize closed trade")?;
        sqlx::query(
            r#"INSERT INTO trades
               (trade_id, trading_date, account_id, strategy, closed_at, payload, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, now())
               ON CONFLICT (trade_id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = now()"#,
        )
        .bind(trade_id)
        .bind(trading_date)
        .bind(account_id)
        .bind(strategy)
        .bind(closed_at)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("could not upsert closed trade")?;
        Ok(())
    }

    pub async fn upsert_daily_account<T: Serialize>(
        &self,
        trading_date: chrono::NaiveDate,
        account_id: &str,
        strategy: &str,
        account: &T,
    ) -> Result<()> {
        let payload = serde_json::to_value(account).context("could not serialize daily account")?;
        sqlx::query(
            r#"INSERT INTO daily_accounts
               (trading_date, account_id, strategy, payload, updated_at)
               VALUES ($1, $2, $3, $4, now())
               ON CONFLICT (trading_date, account_id, strategy) DO UPDATE SET
                 payload = EXCLUDED.payload,
                 updated_at = now()"#,
        )
        .bind(trading_date)
        .bind(account_id)
        .bind(strategy)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("could not upsert daily account")?;
        Ok(())
    }

    pub async fn record_service_event(
        &self,
        service: &str,
        level: &str,
        code: &str,
        message: &str,
    ) -> Result<()> {
        let bounded: String = message.chars().take(512).collect();
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO service_events (service, level, code, message) VALUES ($1, $2, $3, $4)",
        )
        .bind(service)
        .bind(level)
        .bind(code)
        .bind(bounded)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "DELETE FROM service_events WHERE id NOT IN (SELECT id FROM service_events ORDER BY id DESC LIMIT 1000)",
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn list_service_events(&self, limit: Option<usize>) -> Result<Vec<ServiceEventRow>> {
        let limit = normalize_service_event_limit(limit)? as i64;
        let rows: Vec<(
            i64,
            chrono::DateTime<chrono::Utc>,
            String,
            String,
            String,
            String,
        )> = sqlx::query_as(
            r#"SELECT id, occurred_at, service, level, code, message
                   FROM service_events
                   ORDER BY id DESC
                   LIMIT $1"#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("could not load service events")?;
        Ok(rows
            .into_iter()
            .map(
                |(id, occurred_at, service, level, code, message)| ServiceEventRow {
                    id,
                    occurred_at,
                    service,
                    level,
                    code,
                    message,
                },
            )
            .collect())
    }

    pub async fn create_cron_job(&self, job: &crate::cron_jobs::NewCronJob) -> Result<CronJobRow> {
        let row: (i64, String, String, String, bool, chrono::DateTime<chrono::Utc>, Option<String>, Option<i32>, Option<i64>, Option<String>) = sqlx::query_as(
            r#"INSERT INTO cron_jobs (label, expression, target_url, enabled, next_run_at)
               VALUES ($1, $2, $3, TRUE, $4)
               RETURNING id, label, expression, target_url, enabled, next_run_at, last_status, last_http_status, last_duration_ms, last_error"#,
        )
        .bind(&job.label)
        .bind(&job.expression)
        .bind(&job.target_url)
        .bind(job.next_run_at)
        .fetch_one(&self.pool)
        .await
        .context("could not create cron job")?;
        Ok(cron_job_from_row(row))
    }

    pub async fn list_cron_jobs(&self) -> Result<Vec<CronJobRow>> {
        let rows: Vec<CronJobTuple> = sqlx::query_as(
            r#"SELECT id, label, expression, target_url, enabled, next_run_at, last_status, last_http_status, last_duration_ms, last_error
               FROM cron_jobs ORDER BY id DESC"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("could not list cron jobs")?;
        Ok(rows.into_iter().map(cron_job_from_row).collect())
    }

    pub async fn set_cron_job_enabled(&self, id: i64, enabled: bool) -> Result<bool> {
        let result = sqlx::query("UPDATE cron_jobs SET enabled = $2, claimed_until = NULL, updated_at = now() WHERE id = $1")
            .bind(id)
            .bind(enabled)
            .execute(&self.pool)
            .await
            .context("could not update cron job")?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn delete_cron_job(&self, id: i64) -> Result<bool> {
        let result = sqlx::query("DELETE FROM cron_jobs WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .context("could not delete cron job")?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn claim_due_cron_jobs(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        claim_seconds: i64,
    ) -> Result<Vec<CronJobRow>> {
        let claim_until = now + chrono::Duration::seconds(claim_seconds);
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("could not begin cron claim")?;
        let rows: Vec<CronJobTuple> = sqlx::query_as(
            r#"SELECT id, label, expression, target_url, enabled, next_run_at, last_status, last_http_status, last_duration_ms, last_error
               FROM cron_jobs
               WHERE enabled = TRUE AND next_run_at <= $1 AND (claimed_until IS NULL OR claimed_until < $1)
               ORDER BY next_run_at ASC
               FOR UPDATE SKIP LOCKED
               LIMIT 16"#,
        )
        .bind(now)
        .fetch_all(&mut *transaction)
        .await
        .context("could not claim due cron jobs")?;
        for (id, ..) in &rows {
            sqlx::query(
                "UPDATE cron_jobs SET claimed_until = $2, updated_at = now() WHERE id = $1",
            )
            .bind(id)
            .bind(claim_until)
            .execute(&mut *transaction)
            .await
            .context("could not mark cron job claimed")?;
        }
        transaction
            .commit()
            .await
            .context("could not commit cron claim")?;
        Ok(rows.into_iter().map(cron_job_from_row).collect())
    }

    pub async fn finish_cron_job_run(
        &self,
        id: i64,
        next_run_at: chrono::DateTime<chrono::Utc>,
        status: &str,
        http_status: Option<i32>,
        duration_ms: i64,
        error: Option<&str>,
    ) -> Result<()> {
        let error = error.map(|value| value.chars().take(512).collect::<String>());
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("could not begin cron finish")?;
        sqlx::query(
            "UPDATE cron_jobs SET next_run_at = $2, claimed_until = NULL, last_status = $3, last_http_status = $4, last_duration_ms = $5, last_error = $6, updated_at = now() WHERE id = $1",
        )
        .bind(id)
        .bind(next_run_at)
        .bind(status)
        .bind(http_status)
        .bind(duration_ms)
        .bind(error.as_deref())
        .execute(&mut *transaction)
        .await
        .context("could not finish cron job")?;
        sqlx::query("INSERT INTO cron_job_runs (cron_job_id, status, http_status, duration_ms, error) VALUES ($1, $2, $3, $4, $5)")
            .bind(id)
            .bind(status)
            .bind(http_status)
            .bind(duration_ms)
            .bind(error.as_deref())
            .execute(&mut *transaction)
            .await
            .context("could not record cron run")?;
        sqlx::query("DELETE FROM cron_job_runs WHERE id NOT IN (SELECT id FROM cron_job_runs ORDER BY id DESC LIMIT 1000)")
            .execute(&mut *transaction)
            .await
            .context("could not trim cron runs")?;
        transaction
            .commit()
            .await
            .context("could not commit cron result")?;
        Ok(())
    }

    pub async fn list_cron_job_runs(&self, id: i64, limit: i64) -> Result<Vec<CronRunRow>> {
        let rows = sqlx::query_as(
            r#"SELECT id, occurred_at, status, http_status, duration_ms, error
               FROM cron_job_runs WHERE cron_job_id = $1 ORDER BY id DESC LIMIT $2"#,
        )
        .bind(id)
        .bind(limit.clamp(1, 100))
        .fetch_all(&self.pool)
        .await
        .context("could not list cron job runs")?;
        Ok(rows
            .into_iter()
            .map(
                |(id, occurred_at, status, http_status, duration_ms, error)| CronRunRow {
                    id,
                    occurred_at,
                    status,
                    http_status,
                    duration_ms,
                    error,
                },
            )
            .collect())
    }
}

type CronJobTuple = (
    i64,
    String,
    String,
    String,
    bool,
    chrono::DateTime<chrono::Utc>,
    Option<String>,
    Option<i32>,
    Option<i64>,
    Option<String>,
);

fn cron_job_from_row(
    (
        id,
        label,
        expression,
        target_url,
        enabled,
        next_run_at,
        last_status,
        last_http_status,
        last_duration_ms,
        last_error,
    ): CronJobTuple,
) -> CronJobRow {
    CronJobRow {
        id,
        label,
        expression,
        target_url,
        enabled,
        next_run_at,
        last_status,
        last_http_status,
        last_duration_ms,
        last_error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_event_limit_is_bounded() {
        assert_eq!(normalize_service_event_limit(None).unwrap(), 100);
        assert_eq!(normalize_service_event_limit(Some(1)).unwrap(), 1);
        assert_eq!(normalize_service_event_limit(Some(200)).unwrap(), 200);
        assert!(normalize_service_event_limit(Some(0)).is_err());
        assert!(normalize_service_event_limit(Some(201)).is_err());
    }
}
