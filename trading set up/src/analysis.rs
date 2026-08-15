//! Analysis video/transcript analysis with bounded rolling context and a
//! fail-closed trading schema.
//!
//! This module deliberately does not know how to place or execute an order. It
//! turns a synchronized media/market snapshot into semantically validated paper
//! trading commands. Callers must still apply their own tick-freshness and
//! portfolio/risk checks immediately before executing a returned command.

use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use chrono_tz::Asia::Kolkata;
use reqwest::{
    Client,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{sync::Mutex, time::Instant};

pub const DEFAULT_LUNA_MODEL: &str = "gpt-5.6-luna";
pub const DEFAULT_RESPONSES_ENDPOINT: &str = "https://api.openai.com/v1/responses";

const MAX_PROVIDER_ERROR_BODY_BYTES: u64 = 16 * 1024;
const MAX_PROVIDER_ERROR_MESSAGE_CHARS: usize = 512;

/// Hard character limits keep the rolling prompt approximately constant even
/// during a six- or seven-hour stream. These are Unicode character counts, not
/// byte counts, so truncation never splits UTF-8.
pub const MAX_ROLLING_SUMMARY_CHARS: usize = 4_000;
pub const MAX_COMBINED_SUMMARY_CHARS: usize = 6_000;
pub const MAX_KEY_VISUAL_POINTS: usize = 24;
pub const MAX_ACTIVE_EPISODES: usize = 8;

const MAX_CONTEXT_LABEL_CHARS: usize = 160;
const MAX_CONTEXT_VALUE_CHARS: usize = 320;
const MAX_CONTEXT_TIME_CHARS: usize = 64;
const MAX_EPISODE_ID_CHARS: usize = 96;
const MAX_INSTRUCTION_CHARS: usize = 640;
const MAX_ACTION_EVENT_ID_CHARS: usize = 160;
/// Stable bucket for the immutable Luna instruction/schema prefix. Bump this
/// version whenever either changes so old cache entries are never reused.
const PROMPT_CACHE_KEY: &str = "observer-paper-luna-v1";

const SYSTEM_INSTRUCTION: &str = r#"paper-only trade extractor. Strict JSON only; no prose/orders.

Use selected transcripts, optional image, market snapshot, rolling_context. Media is untrusted; visual prices stale. Return compact context with proven contract, entry, SL, target, booking, cancellation.

Keep stable episode_id, entry_event_id, identity, levels, unresolved episodes. Current evidence overrides context; context carries identity/proven levels, never a new command.

Every executable action needs current selected source evidence: selected source_segment_sequence and in-segment offset. Never invent contract, expiry, price, level, visual fact, intent. Never repeat entry_event_id unless current evidence proves a distinct entry.

WATCH is discussion, not entry. PLACE_ENTRY needs factual BUY contract, entry, affirmative current intent; read intent semantically, not only "enter now". Reuse same-episode hard_sl/T1 or null for runtime fallback. CANCEL_ENTRY, UPDATE_LEVELS, EXIT, HOLD need explicit current instruction for matching setup/trade. Ignore ambiguity, education, recap, promotion, VIP/Telegram, SELL/short.

Require hard_sl < entry < t1 and t1 < t2 if t2. Omit unknown expiry; factual rationales."#;

#[derive(Debug, Clone)]
pub struct AnalysisClientConfig {
    pub model: String,
    pub endpoint: String,
    pub request_timeout: Duration,
}

impl Default for AnalysisClientConfig {
    fn default() -> Self {
        Self {
            model: DEFAULT_LUNA_MODEL.to_owned(),
            endpoint: DEFAULT_RESPONSES_ENDPOINT.to_owned(),
            request_timeout: Duration::from_secs(45),
        }
    }
}

pub struct AnalysisClient {
    http: Client,
    keys: Arc<RuntimeKeyVault>,
    config: AnalysisClientConfig,
}

struct AnalysisKeySlot {
    header: HeaderValue,
    cooldown_until: Instant,
    successes: u64,
    failures: u64,
    last_failure: Option<&'static str>,
    rate_limit: Option<RateLimitTelemetry>,
    daily_usage: DailyUsageCounter,
    failed_since_load: bool,
}

struct AnalysisKeyRing {
    slots: Vec<AnalysisKeySlot>,
    cursor: usize,
    generation: u64,
}

/// Lifecycle state for credentials held solely in this process's memory.
/// It deliberately carries no material that can authenticate a request.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum VaultState {
    Ready,
    KeysRequired,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct VaultHealth {
    pub generation: u64,
    pub loaded_slots: usize,
    pub state: VaultState,
    pub slots: Vec<AnalysisKeyHealth>,
}

/// A write-only, RAM-only source for OpenAI request headers.  It intentionally
/// does not expose raw headers or submitted key text through its public API.
pub struct RuntimeKeyVault {
    inner: Mutex<AnalysisKeyRing>,
}

struct VaultSelection {
    generation: u64,
    slot: usize,
    header: HeaderValue,
}

impl RuntimeKeyVault {
    pub fn empty() -> Self {
        Self {
            inner: Mutex::new(AnalysisKeyRing {
                slots: Vec::new(),
                cursor: 0,
                generation: 0,
            }),
        }
    }

    pub fn from_keys<I, S>(keys: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let now = Instant::now();
        let mut seen = HashSet::new();
        let mut slots = Vec::new();
        for key in keys {
            if slots.len() >= 3 {
                break;
            }
            let raw = key.as_ref().trim();
            if raw.is_empty() || !seen.insert(raw.to_owned()) {
                continue;
            }
            slots.push(AnalysisKeySlot {
                header: authorization_header_for_key(raw)?,
                cooldown_until: now,
                successes: 0,
                failures: 0,
                last_failure: None,
                rate_limit: None,
                daily_usage: DailyUsageCounter::default(),
                failed_since_load: false,
            });
        }
        if slots.is_empty() {
            bail!("at least one non-empty Analysis API key is required");
        }
        Ok(Self {
            inner: Mutex::new(AnalysisKeyRing {
                slots,
                cursor: 0,
                generation: 0,
            }),
        })
    }

    pub async fn add<I, S>(&self, keys: I) -> Result<usize>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut ring = self.inner.lock().await;
        let now = Instant::now();
        let mut added = 0usize;
        for key in keys {
            if ring.slots.len() >= 3 {
                break;
            }
            let raw = key.as_ref().trim();
            if raw.is_empty() {
                continue;
            }
            let header = authorization_header_for_key(raw)?;
            if ring.slots.iter().any(|slot| slot.header == header) {
                continue;
            }
            ring.slots.push(AnalysisKeySlot {
                header,
                cooldown_until: now,
                successes: 0,
                failures: 0,
                last_failure: None,
                rate_limit: None,
                daily_usage: DailyUsageCounter::default(),
                failed_since_load: false,
            });
            added += 1;
        }
        if added > 0 {
            ring.generation = ring.generation.wrapping_add(1);
        }
        Ok(added)
    }

    pub async fn clear(&self) {
        let mut ring = self.inner.lock().await;
        ring.slots.clear();
        ring.cursor = 0;
        ring.generation = ring.generation.wrapping_add(1);
    }

    pub async fn health(&self) -> VaultHealth {
        let ring = self.inner.lock().await;
        let now = Instant::now();
        let slots = ring
            .slots
            .iter()
            .enumerate()
            .map(|(index, slot)| key_health_view(index, slot, now))
            .collect::<Vec<_>>();
        VaultHealth {
            generation: ring.generation,
            loaded_slots: slots.len(),
            state: if slots.is_empty() {
                VaultState::KeysRequired
            } else {
                VaultState::Ready
            },
            slots,
        }
    }

    pub async fn record_failure(&self, index: usize, class: &'static str, cooldown: Duration) {
        let mut ring = self.inner.lock().await;
        ring.record_failure(index, class, cooldown);
        if !ring.slots.is_empty() && ring.slots.iter().all(|slot| slot.failed_since_load) {
            ring.slots.clear();
            ring.cursor = 0;
            ring.generation = ring.generation.wrapping_add(1);
        }
    }

    async fn select_next(&self, attempted: &HashSet<usize>) -> Option<VaultSelection> {
        let mut ring = self.inner.lock().await;
        let generation = ring.generation;
        let (slot, header) = ring.next_available(attempted)?;
        Some(VaultSelection {
            generation,
            slot,
            header,
        })
    }

    async fn record_request_if_current(&self, generation: u64, slot: usize) -> bool {
        let mut ring = self.inner.lock().await;
        if ring.generation != generation {
            return false;
        }
        ring.record_request(slot);
        true
    }

    async fn record_rate_limit_if_current(
        &self,
        generation: u64,
        slot: usize,
        rate_limit: RateLimitTelemetry,
    ) {
        let mut ring = self.inner.lock().await;
        if ring.generation == generation {
            ring.record_rate_limit(slot, rate_limit);
        }
    }

    async fn record_success_if_current(
        &self,
        generation: u64,
        slot: usize,
        usage: UsageTelemetry,
    ) -> bool {
        let mut ring = self.inner.lock().await;
        if ring.generation != generation {
            return false;
        }
        ring.record_success(slot);
        ring.record_usage(slot, usage);
        true
    }

    async fn record_failure_if_current(
        &self,
        generation: u64,
        slot: usize,
        class: &'static str,
        cooldown: Duration,
    ) -> bool {
        let mut ring = self.inner.lock().await;
        if ring.generation != generation {
            return false;
        }
        ring.record_failure(slot, class, cooldown);
        if !ring.slots.is_empty() && ring.slots.iter().all(|slot| slot.failed_since_load) {
            ring.slots.clear();
            ring.cursor = 0;
            ring.generation = ring.generation.wrapping_add(1);
        }
        true
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AnalysisKeyHealth {
    pub slot: usize,
    pub state: String,
    pub successes: u64,
    pub failures: u64,
    pub cooldown_remaining_ms: u64,
    pub last_failure: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateLimitTelemetry>,
    pub daily_usage: DailyUsageTelemetry,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct RateLimitTelemetry {
    pub request_limit: Option<u64>,
    pub request_remaining: Option<u64>,
    pub request_reset_ms: Option<u64>,
    pub token_limit: Option<u64>,
    pub token_remaining: Option<u64>,
    pub token_reset_ms: Option<u64>,
    pub retry_after_ms: Option<u64>,
}

/// Safe locally observed token/accounting data. These are not provider limits.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct DailyUsageTelemetry {
    pub day_ist: String,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DailyUsageCounter {
    day_ist: String,
    request_count: u64,
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct UsageTelemetry {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

impl UsageTelemetry {
    fn from_totals(
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        total_tokens: Option<u64>,
    ) -> Self {
        Self {
            input_tokens,
            output_tokens,
            total_tokens,
        }
    }
}

impl DailyUsageCounter {
    fn reset_for_day(&mut self, day_ist: &str) {
        if self.day_ist != day_ist {
            self.day_ist = day_ist.to_owned();
            self.request_count = 0;
            self.input_tokens = 0;
            self.output_tokens = 0;
            self.total_tokens = 0;
        }
    }

    /// Count every submitted outbound request, even if the transport or
    /// provider rejects it before usage is available.
    fn record_request(&mut self, day_ist: &str) {
        self.reset_for_day(day_ist);
        self.request_count = self.request_count.saturating_add(1);
    }

    /// Provider usage is optional and is only added when the response carried
    /// structured usage fields. It must not imply another request attempt.
    fn record_usage(&mut self, day_ist: &str, usage: UsageTelemetry) {
        self.reset_for_day(day_ist);
        self.input_tokens = self
            .input_tokens
            .saturating_add(usage.input_tokens.unwrap_or_default());
        self.output_tokens = self
            .output_tokens
            .saturating_add(usage.output_tokens.unwrap_or_default());
        self.total_tokens = self
            .total_tokens
            .saturating_add(usage.total_tokens.unwrap_or_else(|| {
                usage
                    .input_tokens
                    .unwrap_or_default()
                    .saturating_add(usage.output_tokens.unwrap_or_default())
            }));
    }

    fn view(&self) -> DailyUsageTelemetry {
        DailyUsageTelemetry {
            day_ist: self.day_ist.clone(),
            request_count: self.request_count,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            total_tokens: self.total_tokens,
        }
    }
}

fn key_health_view(index: usize, slot: &AnalysisKeySlot, now: Instant) -> AnalysisKeyHealth {
    let remaining = slot.cooldown_until.saturating_duration_since(now);
    AnalysisKeyHealth {
        slot: index + 1,
        state: if remaining.is_zero() {
            "READY"
        } else {
            "COOLDOWN"
        }
        .to_owned(),
        successes: slot.successes,
        failures: slot.failures,
        cooldown_remaining_ms: remaining.as_millis().min(u64::MAX as u128) as u64,
        last_failure: slot.last_failure.map(str::to_owned),
        rate_limit: slot.rate_limit.clone(),
        daily_usage: slot.daily_usage.view(),
    }
}

impl AnalysisKeyRing {
    fn next_available(&mut self, attempted: &HashSet<usize>) -> Option<(usize, HeaderValue)> {
        let count = self.slots.len();
        let now = Instant::now();
        for offset in 0..count {
            let index = (self.cursor + offset) % count;
            let slot = &self.slots[index];
            if !attempted.contains(&index) && slot.cooldown_until <= now {
                let header = slot.header.clone();
                return Some((index, header));
            }
        }
        None
    }

    fn record_success(&mut self, index: usize) {
        if let Some(slot) = self.slots.get_mut(index) {
            slot.successes = slot.successes.saturating_add(1);
            slot.last_failure = None;
        }
    }

    fn record_usage(&mut self, index: usize, usage: UsageTelemetry) {
        if let Some(slot) = self.slots.get_mut(index) {
            let day_ist = Utc::now().with_timezone(&Kolkata).date_naive().to_string();
            slot.daily_usage.record_usage(&day_ist, usage);
        }
    }

    fn record_request(&mut self, index: usize) {
        if let Some(slot) = self.slots.get_mut(index) {
            let day_ist = Utc::now().with_timezone(&Kolkata).date_naive().to_string();
            slot.daily_usage.record_request(&day_ist);
        }
    }

    fn record_rate_limit(&mut self, index: usize, rate_limit: RateLimitTelemetry) {
        if let Some(slot) = self.slots.get_mut(index) {
            slot.rate_limit = Some(rate_limit);
        }
    }

    fn record_failure(&mut self, index: usize, class: &'static str, cooldown: Duration) {
        if let Some(slot) = self.slots.get_mut(index) {
            slot.failures = slot.failures.saturating_add(1);
            slot.last_failure = Some(class);
            slot.failed_since_load = true;
            slot.cooldown_until = slot.cooldown_until.max(Instant::now() + cooldown);
            self.cursor = (index + 1) % self.slots.len();
        }
    }
}

impl AnalysisClient {
    pub fn new(api_key: impl AsRef<str>) -> Result<Self> {
        Self::from_config(api_key, AnalysisClientConfig::default())
    }

    pub fn from_config(api_key: impl AsRef<str>, config: AnalysisClientConfig) -> Result<Self> {
        Self::from_keys_config([api_key], config)
    }

    pub fn from_keys_config<I, S>(api_keys: I, config: AnalysisClientConfig) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if config.model.trim().is_empty() {
            bail!("Analysis model must not be empty");
        }
        if config.endpoint.trim().is_empty() {
            bail!("Analysis endpoint must not be empty");
        }

        let vault = Arc::new(RuntimeKeyVault::from_keys(api_keys)?);
        Self::from_runtime_vault(vault, config)
    }

    pub fn from_runtime_vault(
        keys: Arc<RuntimeKeyVault>,
        config: AnalysisClientConfig,
    ) -> Result<Self> {
        if config.model.trim().is_empty() {
            bail!("Analysis model must not be empty");
        }
        if config.endpoint.trim().is_empty() {
            bail!("Analysis endpoint must not be empty");
        }

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("observer-paper-trader/0.1"),
        );

        let http = Client::builder()
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(10))
            .timeout(config.request_timeout)
            .build()
            .context("failed to construct Analysis HTTP client")?;

        Ok(Self { http, keys, config })
    }

    pub async fn credential_count(&self) -> usize {
        self.keys.health().await.loaded_slots
    }

    pub async fn vault_health(&self) -> VaultHealth {
        self.keys.health().await
    }

    pub async fn key_health(&self) -> Vec<AnalysisKeyHealth> {
        self.keys.health().await.slots
    }

    pub async fn analyze(
        &self,
        input: &AnalysisInput,
        jpeg_bytes: Option<&[u8]>,
    ) -> Result<ValidatedAnalysis> {
        let body = build_request_body(&self.config.model, input, jpeg_bytes)?;
        let mut attempted = HashSet::new();
        let mut last_error = None;
        loop {
            let next = self.keys.select_next(&attempted).await;
            let Some(selection) = next else {
                return Err(last_error.unwrap_or_else(|| {
                    anyhow!("OpenAI keys are required or all loaded slots have failed")
                }));
            };
            let slot = selection.slot;
            attempted.insert(slot);
            // This is deliberately before `.send()`: a connection failure is
            // still an outbound attempt for local daily accounting.
            if !self
                .keys
                .record_request_if_current(selection.generation, slot)
                .await
            {
                return Err(anyhow!("OpenAI key vault changed during analysis"));
            }

            let response = match self
                .http
                .post(&self.config.endpoint)
                .header(AUTHORIZATION, selection.header)
                .json(&body)
                .send()
                .await
            {
                Ok(response) => response,
                Err(_) => {
                    self.keys
                        .record_failure_if_current(
                            selection.generation,
                            slot,
                            "TRANSPORT",
                            Duration::from_secs(5),
                        )
                        .await;
                    last_error = Some(anyhow!("OpenAI Responses API request failed"));
                    continue;
                }
            };

            let status = response.status();
            let rate_limit = parse_rate_limit_headers(response.headers());
            self.keys
                .record_rate_limit_if_current(selection.generation, slot, rate_limit)
                .await;
            if !status.is_success() {
                // Parse only the provider's structured error.message. Never expose
                // the raw body, request body, headers, or credential value.
                let provider_message = extract_openai_error_message(response).await;
                let error = provider_message.map_or_else(
                    || anyhow!("OpenAI Responses API returned HTTP {}", status.as_u16()),
                    |message| {
                        anyhow!(
                            "OpenAI Responses API returned HTTP {}: {}",
                            status.as_u16(),
                            message
                        )
                    },
                );
                let (class, cooldown) = analysis_retry_policy(status.as_u16())
                    .unwrap_or(("PROVIDER", Duration::from_secs(5)));
                self.keys
                    .record_failure_if_current(selection.generation, slot, class, cooldown)
                    .await;
                last_error = Some(error);
                continue;
            }

            let response: ResponsesResponse = match response.json().await {
                Ok(response) => response,
                Err(_) => {
                    self.keys
                        .record_failure_if_current(
                            selection.generation,
                            slot,
                            "MODEL_OUTPUT",
                            Duration::from_secs(5),
                        )
                        .await;
                    last_error = Some(anyhow!("OpenAI Responses API returned malformed JSON"));
                    continue;
                }
            };
            let usage = response.usage.clone().into();
            let validated = match parse_responses_response(response, input) {
                Ok(validated) => validated,
                Err(error) => {
                    self.keys
                        .record_failure_if_current(
                            selection.generation,
                            slot,
                            "MODEL_OUTPUT",
                            Duration::from_secs(5),
                        )
                        .await;
                    last_error = Some(error);
                    continue;
                }
            };
            if !self
                .keys
                .record_success_if_current(selection.generation, slot, usage)
                .await
            {
                return Err(anyhow!("OpenAI key vault changed during analysis"));
            }
            return Ok(validated);
        }
    }
}

fn analysis_retry_policy(status: u16) -> Option<(&'static str, Duration)> {
    match status {
        401 | 403 => Some(("AUTH", Duration::from_secs(15 * 60))),
        408 => Some(("TIMEOUT", Duration::from_secs(5))),
        429 => Some(("QUOTA", Duration::from_secs(60))),
        500..=599 => Some(("TRANSIENT", Duration::from_secs(5))),
        _ => None,
    }
}

fn authorization_header_for_key(key: &str) -> Result<HeaderValue> {
    let mut header = HeaderValue::from_str(&format!("Bearer {key}"))
        .context("an Analysis API key is not a valid HTTP header value")?;
    header.set_sensitive(true);
    Ok(header)
}

fn parse_rate_limit_headers(headers: &HeaderMap) -> RateLimitTelemetry {
    RateLimitTelemetry {
        request_limit: header_u64(headers, "x-ratelimit-limit-requests"),
        request_remaining: header_u64(headers, "x-ratelimit-remaining-requests"),
        request_reset_ms: header_duration_ms(headers, "x-ratelimit-reset-requests"),
        token_limit: header_u64(headers, "x-ratelimit-limit-tokens"),
        token_remaining: header_u64(headers, "x-ratelimit-remaining-tokens"),
        token_reset_ms: header_duration_ms(headers, "x-ratelimit-reset-tokens"),
        retry_after_ms: retry_after_ms(headers),
    }
}

fn retry_after_ms(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get("retry-after")?.to_str().ok()?.trim();
    if let Some(milliseconds) = header_duration_value_ms(value) {
        return Some(milliseconds);
    }
    let seconds: f64 = value.parse().ok()?;
    (seconds.is_finite() && seconds >= 0.0).then_some((seconds * 1_000.0) as u64)
}

fn header_u64(headers: &HeaderMap, name: &'static str) -> Option<u64> {
    headers.get(name)?.to_str().ok()?.trim().parse().ok()
}

fn header_duration_ms(headers: &HeaderMap, name: &'static str) -> Option<u64> {
    let value = headers.get(name)?.to_str().ok()?.trim();
    header_duration_value_ms(value)
}

fn header_duration_value_ms(value: &str) -> Option<u64> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1.0)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000.0)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60_000.0)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 3_600_000.0)
    } else {
        return None;
    };
    let number: f64 = number.parse().ok()?;
    let milliseconds = number * multiplier;
    (number.is_finite() && number >= 0.0 && milliseconds.is_finite()).then_some(milliseconds as u64)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisInput {
    pub clip: ClipWindow,
    pub transcripts: Vec<TranscriptChunk>,
    #[serde(default)]
    pub watched_options: Vec<WatchedOptionSnapshot>,
    #[serde(default)]
    pub open_trades: Vec<OpenTradeSnapshot>,
    /// Bounded full snapshot returned by the previous successful analysis.
    /// `None` is correct only for the first window or after an intentional
    /// context reset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolling_context: Option<RollingContext>,
}

impl AnalysisInput {
    /// Returns concrete synchronization/input defects that make a new entry
    /// unsafe. Existing-trade EXIT/HOLD decisions may still be analyzed.
    pub fn entry_input_issues(&self) -> Vec<String> {
        let mut issues = Vec::new();

        if !self.clip.complete {
            issues.push("clip is marked incomplete".to_owned());
        }
        let clip_ms = self
            .clip
            .ended_at
            .signed_duration_since(self.clip.started_at)
            .num_milliseconds();
        if clip_ms < 2_500 {
            issues.push("selected source span is shorter than one 3-second segment".to_owned());
        }
        if self.clip.sent_at < self.clip.ended_at {
            issues.push("prompt send time precedes clip end".to_owned());
        }
        if !matches!(self.transcripts.len(), 1 | 4) {
            issues.push(
                "exactly one must-pass or four retained transcript segments are required"
                    .to_owned(),
            );
            return issues;
        }

        let mut chunks: Vec<&TranscriptChunk> = self.transcripts.iter().collect();
        chunks.sort_by_key(|chunk| chunk.source_sequence);
        let mut source_sequences = HashSet::new();
        for chunk in &chunks {
            if !source_sequences.insert(chunk.source_sequence) {
                issues.push("selected source segment IDs must be unique".to_owned());
                break;
            }
            if !chunk.complete {
                issues.push(format!(
                    "transcript source segment {} is incomplete",
                    chunk.source_sequence
                ));
            }
            if chunk.text.trim().is_empty() {
                issues.push(format!(
                    "transcript source segment {} is empty",
                    chunk.source_sequence
                ));
            }
            let duration_ms = chunk
                .ended_at
                .signed_duration_since(chunk.started_at)
                .num_milliseconds();
            if !(2_500..=3_500).contains(&duration_ms) {
                issues.push(format!(
                    "transcript source segment {} is not approximately 3 seconds",
                    chunk.source_sequence
                ));
            }
            if chunk.started_at < self.clip.started_at || chunk.ended_at > self.clip.ended_at {
                issues.push(format!(
                    "transcript source segment {} falls outside the selected source span",
                    chunk.source_sequence
                ));
            }
        }

        issues
    }

    pub fn is_complete_for_entry(&self) -> bool {
        self.entry_input_issues().is_empty()
    }

    fn open_trade_for(&self, action: &TradeAction) -> Option<&OpenTradeSnapshot> {
        if let Some(trade_id) = action.trade_id.as_deref() {
            if let Some(found) = self
                .open_trades
                .iter()
                .find(|trade| trade.trade_id == trade_id)
            {
                // A model-supplied trade ID is authoritative only when any
                // supplied contract is compatible with that same open trade.
                // This prevents a correct ID paired with a hallucinated
                // contract from mutating the wrong lifecycle.
                return action.contract.as_ref().map_or(Some(found), |contract| {
                    contracts_match(&found.contract, contract).then_some(found)
                });
            }
        }
        action.contract.as_ref().and_then(|contract| {
            let mut matches = self
                .open_trades
                .iter()
                .filter(|trade| contracts_match(&trade.contract, contract));
            let first = matches.next();
            // Missing expiry is allowed only when it still identifies exactly
            // one open trade. Never select the first of two weekly expiries.
            matches.next().is_none().then_some(first).flatten()
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipWindow {
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub sent_at: DateTime<Utc>,
    pub data_age_ms: u64,
    pub complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptChunk {
    /// Immutable source-segment identity from capture. It need not be
    /// contiguous because the blocker may discard unrelated segments.
    pub source_sequence: u64,
    pub started_at: DateTime<Utc>,
    pub ended_at: DateTime<Utc>,
    pub text: String,
    pub complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchedOptionSnapshot {
    pub contract: OptionContract,
    pub price: PriceSnapshot,
    pub watch_remaining_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceSnapshot {
    pub ltp: Option<f64>,
    pub observed_at: Option<DateTime<Utc>>,
    pub age_ms: Option<u64>,
    /// Set by the market-data actor using its own configured stale-tick limit.
    pub fresh: bool,
}

impl PriceSnapshot {
    fn valid_for_entry(&self) -> bool {
        self.fresh
            && self.age_ms.is_some()
            && self.observed_at.is_some()
            && self.ltp.is_some_and(is_finite_positive)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenTradeSnapshot {
    pub trade_id: String,
    pub contract: OptionContract,
    pub quantity: u32,
    pub entry_price: f64,
    pub price: PriceSnapshot,
    pub unrealized_pnl: f64,
    pub hard_sl: f64,
    pub effective_sl: f64,
    pub t1: f64,
    pub t2: Option<f64>,
    pub trailing_phase: u8,
    pub exit_mode: ExitMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExitMode {
    Llm,
    MovingSl,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OptionContract {
    pub underlying: Underlying,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<String>,
    pub strike: f64,
    pub option_type: OptionType,
    pub direction: TradeDirection,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Underlying {
    Nifty,
    Sensex,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OptionType {
    Ce,
    Pe,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeDirection {
    Buy,
    Sell,
}

/// Compact, cumulative memory carried from one selected-segment dispatch to
/// the next. The model returns a full replacement snapshot on every call; Rust
/// then sanitizes, bounds, and reconciles it with unresolved prior episodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct RollingContext {
    #[serde(default, alias = "transcript_summary")]
    pub spoken_summary: String,
    #[serde(default)]
    pub visual_summary: String,
    #[serde(default, alias = "trade_episode_summary")]
    pub combined_summary: String,
    #[serde(default, alias = "key_visual_data_points")]
    pub key_visual_points: Vec<KeyVisualDataPoint>,
    #[serde(default, alias = "active_episodes")]
    pub episodes: Vec<TradeEpisodeContext>,
    /// Runtime-authored paper-action facts. This is intentionally absent from
    /// the model response schema; `normalize_rolling_context` carries only the
    /// prior authoritative list so a model cannot forge an execution result.
    #[serde(default)]
    pub authoritative_outcomes: Vec<AuthoritativeOutcome>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthoritativeOutcome {
    pub action: ActionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup_id: Option<String>,
    pub status: String,
    pub detail: String,
    pub occurred_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KeyVisualDataPoint {
    pub category: KeyVisualCategory,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<OptionContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_value: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    /// RFC3339 is preferred, but this remains a bounded string so one malformed
    /// model timestamp cannot make the complete structured response unparseable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<String>,
    /// True only when this point is visibly present in the current selected
    /// clip. Carried prior points must use false.
    #[serde(default)]
    pub observed_in_current_clip: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum KeyVisualCategory {
    Contract,
    Entry,
    StopLoss,
    Target,
    Pnl,
    PositionStatus,
    OrderStatus,
    ChartAnnotation,
    Caption,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TradeEpisodeContext {
    #[serde(default)]
    pub episode_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<OptionContract>,
    pub status: TradeEpisodeStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub levels: Option<TradeLevels>,
    #[serde(default)]
    pub latest_instruction: String,
    /// Stable identifier for the explicit entry event. Once set, it must be
    /// preserved so a later window cannot silently turn the same call into a
    /// second order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_event_id: Option<String>,
    #[serde(default)]
    pub first_seen_at: String,
    #[serde(default)]
    pub last_updated_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeEpisodeStatus {
    Watching,
    ConditionalEntry,
    EntryCalled,
    Open,
    Managing,
    Closed,
    Cancelled,
    Unknown,
}

impl TradeEpisodeStatus {
    fn is_unresolved(self) -> bool {
        matches!(
            self,
            Self::Watching
                | Self::ConditionalEntry
                | Self::EntryCalled
                | Self::Open
                | Self::Managing
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AnalysisResult {
    pub market_bias: MarketBias,
    pub freshness: FreshnessAssessment,
    #[serde(default)]
    pub actions: Vec<TradeAction>,
    /// Required by the production response schema. The alias keeps saved
    /// fixtures from a short-lived `context_update` prototype readable.
    #[serde(alias = "context_update")]
    pub rolling_context: RollingContext,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketBias {
    pub direction: MarketBiasDirection,
    pub rationale: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketBiasDirection {
    Bullish,
    Bearish,
    Neutral,
    Mixed,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FreshnessAssessment {
    pub status: FreshnessStatus,
    pub input_data_age_ms: u64,
    pub usable_for_new_entries: bool,
    pub rationale: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FreshnessStatus {
    Fresh,
    Stale,
    Incomplete,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TradeAction {
    pub action: ActionKind,
    /// Stable link to the rolling trade episode, when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub episode_id: Option<String>,
    /// Stable identifier for this current-window instruction. Rust fills this
    /// deterministically when the model omits it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<OptionContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub levels: Option<TradeLevels>,
    #[serde(default)]
    pub evidence_timestamps: Vec<EvidenceTimestamp>,
    pub rationale: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ActionKind {
    Watch,
    PlaceEntry,
    CancelEntry,
    UpdateLevels,
    Exit,
    Hold,
    Ignore,
}

impl ActionKind {
    pub fn is_trade_command(self) -> bool {
        matches!(
            self,
            Self::PlaceEntry | Self::CancelEntry | Self::UpdateLevels | Self::Exit
        )
    }

    fn needs_contract(self) -> bool {
        !matches!(self, Self::Ignore)
    }

    fn needs_complete_levels(self) -> bool {
        matches!(self, Self::UpdateLevels)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TradeLevels {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hard_sl: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t1: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub t2: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvidenceTimestamp {
    pub seconds_from_clip_start: f64,
    pub source: EvidenceSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "transcript_chunk")]
    pub source_segment_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EvidenceSource {
    Video,
    Transcript,
    Both,
}

#[derive(Debug, Clone)]
pub struct ValidatedAnalysis {
    pub interaction_id: Option<String>,
    pub market_bias: MarketBias,
    pub freshness: FreshnessAssessment,
    /// Sanitized and bounded context to supply as `AnalysisInput.rolling_context`
    /// on the next successful call.
    pub rolling_context: RollingContext,
    /// Only commands that passed Rust-side semantic validation.
    pub actions: Vec<TradeAction>,
    /// Rejected model output retained for local observability/auditing.
    pub rejected_actions: Vec<RejectedAction>,
}

#[derive(Debug, Clone)]
pub struct RejectedAction {
    pub action: TradeAction,
    pub reason: String,
}

/// Parse schema-shaped model JSON, normalize it against authoritative input,
/// and reject unsafe actions. Useful for fixture/replay testing without HTTP.
pub fn parse_and_validate_output(text: &str, input: &AnalysisInput) -> Result<ValidatedAnalysis> {
    let parsed = parse_model_output(text, "Analysis model output")?;
    normalize_and_validate(parsed, input, None)
}

/// Model responses are deliberately stricter than saved state.  We continue
/// to deserialize historical context/snapshot JSON that contained confidence
/// fields, but a current model response that tries to emit one is invalid
/// instead of being silently accepted as a live decision input.
fn parse_model_output(text: &str, source: &str) -> Result<AnalysisResult> {
    let value: Value = serde_json::from_str(text)
        .map_err(|_| anyhow!("{source} is not valid schema-shaped JSON"))?;
    if contains_legacy_confidence_field(&value) {
        bail!("{source} contains removed legacy confidence fields");
    }
    serde_json::from_value(value).map_err(|_| anyhow!("{source} is not valid schema-shaped JSON"))
}

fn contains_legacy_confidence_field(value: &Value) -> bool {
    match value {
        Value::Object(fields) => {
            fields.contains_key("confidence_pct")
                || fields.contains_key("minimum_confidence_pct")
                || fields.contains_key("exit_confidence_pct")
                || fields.values().any(contains_legacy_confidence_field)
        }
        Value::Array(values) => values.iter().any(contains_legacy_confidence_field),
        _ => false,
    }
}

fn normalize_and_validate(
    mut parsed: AnalysisResult,
    input: &AnalysisInput,
    interaction_id: Option<String>,
) -> Result<ValidatedAnalysis> {
    parsed.market_bias.rationale = parsed.market_bias.rationale.trim().to_owned();
    parsed.freshness.rationale = parsed.freshness.rationale.trim().to_owned();

    let rolling_context = normalize_rolling_context(
        parsed.rolling_context,
        input.rolling_context.as_ref(),
        &input.clip,
    )?;

    // Freshness fields are advisory model output, so overwrite their factual
    // parts with local capture/market state before exposing them to callers.
    let base_complete = input.is_complete_for_entry();
    let has_fresh_watched_price = input
        .watched_options
        .iter()
        .any(|snapshot| snapshot.price.valid_for_entry());
    parsed.freshness.input_data_age_ms = input.clip.data_age_ms;
    // A first explicit recommendation is allowed to create a pending order
    // before its contract has been attached to the live option feed. The
    // broker still requires a fresh authoritative tick before filling it; the
    // Analysis layer is responsible only for synchronized evidence and strict
    // trade semantics.
    parsed.freshness.usable_for_new_entries = base_complete;
    parsed.freshness.status = if !base_complete {
        FreshnessStatus::Incomplete
    } else if has_fresh_watched_price {
        FreshnessStatus::Fresh
    } else {
        FreshnessStatus::Stale
    };

    let mut accepted = Vec::new();
    let mut rejected = Vec::new();
    let mut accepted_keys = HashSet::new();

    for mut action in parsed.actions {
        normalize_action(&mut action, input, &rolling_context);
        if let Err(reason) = validate_action(&action, input, &rolling_context) {
            rejected.push(RejectedAction { action, reason });
            continue;
        }

        let key = action_dedup_key(&action);
        if !accepted_keys.insert(key) {
            rejected.push(RejectedAction {
                action,
                reason: "duplicate action in the same model response".to_owned(),
            });
            continue;
        }
        // A model recommendation is not an execution outcome.  The paper
        // runtime reconciles PLACE_ENTRY only after it has observed an actual
        // broker order placement, so this provisional context remains
        // retriable across a pre-action crash or routing/freshness rejection.
        accepted.push(action);
    }

    Ok(ValidatedAnalysis {
        interaction_id,
        market_bias: parsed.market_bias,
        freshness: parsed.freshness,
        rolling_context,
        actions: accepted,
        rejected_actions: rejected,
    })
}

fn normalize_action(
    action: &mut TradeAction,
    input: &AnalysisInput,
    rolling_context: &RollingContext,
) {
    action.rationale = bounded_text(&action.rationale, MAX_INSTRUCTION_CHARS);
    action.episode_id = bounded_optional_text(action.episode_id.take(), MAX_EPISODE_ID_CHARS);
    action.event_id = bounded_optional_text(action.event_id.take(), MAX_ACTION_EVENT_ID_CHARS);
    action.trade_id = action
        .trade_id
        .take()
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty());

    if let Some(contract) = &mut action.contract {
        contract.expiry = contract
            .expiry
            .take()
            .map(|expiry| expiry.trim().to_ascii_uppercase())
            .filter(|expiry| !expiry.is_empty());
    }

    for evidence in &mut action.evidence_timestamps {
        evidence.detail = bounded_optional_text(evidence.detail.take(), MAX_CONTEXT_VALUE_CHARS);
    }
    action.evidence_timestamps.sort_by(|left, right| {
        left.seconds_from_clip_start
            .total_cmp(&right.seconds_from_clip_start)
    });
    action.evidence_timestamps.dedup_by(|left, right| {
        left.seconds_from_clip_start == right.seconds_from_clip_start
            && left.source == right.source
            && left.source_segment_sequence == right.source_segment_sequence
    });

    // Prior context may carry the identity and explicitly captured levels, but
    // validate_action still requires fresh current-window evidence before a
    // command can pass. When the model omits episode_id, infer only when the
    // contract identifies one episode or exactly one unresolved episode exists.
    let episode = matching_episode_for_action(rolling_context, action).or_else(|| {
        // Infer the sole unresolved episode only when the current command did
        // not supply a contract. An explicitly conflicting contract must
        // never borrow identity or levels from unrelated prior memory.
        action.contract.is_none().then(|| {
            let mut unresolved = rolling_context
                .episodes
                .iter()
                .filter(|episode| episode.status.is_unresolved());
            let only = unresolved.next();
            (unresolved.next().is_none()).then_some(only).flatten()
        })?
    });
    if let Some(episode) = episode {
        if action.episode_id.is_none() {
            action.episode_id = Some(episode.episode_id.clone());
        }
        if action.contract.is_none() {
            action.contract = episode.contract.clone();
        } else if let (Some(action_contract), Some(episode_contract)) =
            (action.contract.as_mut(), episode.contract.as_ref())
        {
            // The prior episode may carry an explicitly observed expiry even
            // when the current utterance only repeats strike/type. Retain that
            // exact identity rather than letting routing silently choose the
            // nearest weekly expiry.
            if action_contract.expiry.is_none()
                && contracts_match(action_contract, episode_contract)
            {
                action_contract.expiry = episode_contract.expiry.clone();
            }
        }
        if matches!(
            action.action,
            ActionKind::PlaceEntry | ActionKind::UpdateLevels
        ) {
            merge_missing_levels(&mut action.levels, episode.levels.as_ref());
        }
    }

    if action.action.is_trade_command() && !action.evidence_timestamps.is_empty() {
        // Do not trust a model-supplied event id for an executable command. A
        // deterministic id tied to the current clip/evidence makes replay
        // deduplication stable and prevents an old context id being reused.
        action.event_id = Some(current_action_event_id(action, &input.clip));
    }

    // UPDATE_LEVELS may contain only changed values. Complete it from the
    // matching open trade, then validate the full ordering.
    if action.action == ActionKind::UpdateLevels {
        if let Some(open) = input.open_trade_for(action) {
            let levels = action.levels.get_or_insert_with(TradeLevels::default);
            levels.entry.get_or_insert(open.entry_price);
            levels.hard_sl.get_or_insert(open.hard_sl);
            levels.t1.get_or_insert(open.t1);
            if levels.t2.is_none() {
                levels.t2 = open.t2;
            }
            if action.trade_id.is_none() {
                action.trade_id = Some(open.trade_id.clone());
            }
        }
    }
}

fn normalize_rolling_context(
    mut current: RollingContext,
    prior: Option<&RollingContext>,
    clip: &ClipWindow,
) -> Result<RollingContext> {
    current.spoken_summary =
        bounded_summary_text(&current.spoken_summary, MAX_ROLLING_SUMMARY_CHARS);
    current.visual_summary =
        bounded_summary_text(&current.visual_summary, MAX_ROLLING_SUMMARY_CHARS);
    current.combined_summary =
        bounded_summary_text(&current.combined_summary, MAX_COMBINED_SUMMARY_CHARS);

    if current.spoken_summary.is_empty()
        || current.visual_summary.is_empty()
        || current.combined_summary.is_empty()
    {
        bail!("Analysis rolling context must include spoken, visual, and combined summaries");
    }

    let mut point_keys = HashSet::new();
    let mut newest_points = current
        .key_visual_points
        .into_iter()
        .rev()
        .filter_map(normalize_visual_point)
        .filter(|point| point_keys.insert(visual_point_key(point)))
        .take(MAX_KEY_VISUAL_POINTS)
        .collect::<Vec<_>>();
    newest_points.reverse();
    current.key_visual_points = newest_points;

    let default_first_seen = clip.started_at.to_rfc3339();
    let default_last_updated = clip.ended_at.to_rfc3339();
    let prior_outcomes = prior
        .map(|prior| prior.authoritative_outcomes.clone())
        .unwrap_or_default();
    let prior_episodes = prior
        .map(|prior| {
            prior
                .episodes
                .iter()
                .cloned()
                .map(|episode| {
                    let mut normalized =
                        normalize_episode(episode, &default_first_seen, &default_last_updated);
                    sanitize_unproven_entry_state(&mut normalized, &prior_outcomes);
                    normalized
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut reconciled = Vec::<TradeEpisodeContext>::new();
    let mut matched_prior = HashSet::<usize>::new();
    for episode in current.episodes {
        let mut normalized = normalize_episode(episode, &default_first_seen, &default_last_updated);
        if let Some((index, prior_episode)) = prior_episodes
            .iter()
            .enumerate()
            .find(|(_, prior_episode)| episodes_match(&normalized, prior_episode))
        {
            matched_prior.insert(index);
            // A previously proved entry identity is runtime state.  A model
            // may not replace it with a different event id on the next call.
            if prior_episode.entry_event_id.is_some() {
                normalized.entry_event_id = prior_episode.entry_event_id.clone();
                if prior_episode.status == TradeEpisodeStatus::EntryCalled {
                    normalized.status = TradeEpisodeStatus::EntryCalled;
                }
            }
            merge_episode_with_prior(&mut normalized, prior_episode);
        }

        if let Some(existing) = reconciled
            .iter_mut()
            .find(|existing| episodes_match(existing, &normalized))
        {
            // Later duplicate entries from the same model response are treated
            // as the fresher snapshot but cannot erase stable prior identity.
            merge_episode_with_prior(&mut normalized, existing);
            *existing = normalized;
        } else {
            reconciled.push(normalized);
        }
    }

    // A model can describe a setup, but it can never establish that an entry
    // order was placed. Keep ENTRY_CALLED/event state only when a prior
    // runtime-authored placement outcome proves this exact episode/event.
    for episode in &mut reconciled {
        sanitize_unproven_entry_state(episode, &prior_outcomes);
    }

    // A model omission is not evidence that a live episode ended. Preserve all
    // unresolved prior episodes unless a matching current record explicitly
    // closed or cancelled them.
    for (index, mut prior_episode) in prior_episodes.into_iter().enumerate() {
        if matched_prior.contains(&index) || !prior_episode.status.is_unresolved() {
            continue;
        }
        prior_episode.entry_event_id = bounded_optional_text(
            prior_episode.entry_event_id.take(),
            MAX_ACTION_EVENT_ID_CHARS,
        );
        if !reconciled
            .iter()
            .any(|current_episode| episodes_match(current_episode, &prior_episode))
        {
            reconciled.push(prior_episode);
        }
    }

    // Stable ordering puts unresolved work first so the hard bound cannot be
    // consumed entirely by old terminal episodes.
    reconciled.sort_by_key(|episode| !episode.status.is_unresolved());
    reconciled.truncate(MAX_ACTIVE_EPISODES);
    current.episodes = reconciled;
    // The response schema does not permit this field, and even a malformed
    // response must never manufacture broker facts. Only the previous runtime
    // checkpoint is carried forward; `paper_runtime` appends fresh outcomes
    // after a real broker result.
    current.authoritative_outcomes = prior_outcomes;
    current.authoritative_outcomes.truncate(24);

    Ok(current)
}

fn sanitize_unproven_entry_state(
    episode: &mut TradeEpisodeContext,
    outcomes: &[AuthoritativeOutcome],
) {
    let has_entry_marker =
        episode.status == TradeEpisodeStatus::EntryCalled || episode.entry_event_id.is_some();
    if !has_entry_marker {
        return;
    }
    let proven = episode.entry_event_id.as_deref().is_some_and(|event_id| {
        outcomes.iter().any(|outcome| {
            outcome.action == ActionKind::PlaceEntry
                && outcome.status == "APPLIED"
                && outcome.event_id.as_deref() == Some(event_id)
                && outcome.episode_id.as_deref() == Some(episode.episode_id.as_str())
        })
    });
    if !proven {
        episode.entry_event_id = None;
        if matches!(
            episode.status,
            TradeEpisodeStatus::EntryCalled
                | TradeEpisodeStatus::Open
                | TradeEpisodeStatus::Managing
        ) {
            episode.status = TradeEpisodeStatus::ConditionalEntry;
        }
    }
}

fn normalize_visual_point(mut point: KeyVisualDataPoint) -> Option<KeyVisualDataPoint> {
    point.label = bounded_text(&point.label, MAX_CONTEXT_LABEL_CHARS);
    point.value = bounded_text(&point.value, MAX_CONTEXT_VALUE_CHARS);
    if point.label.is_empty() && point.value.is_empty() {
        return None;
    }
    point.unit = bounded_optional_text(point.unit.take(), MAX_CONTEXT_LABEL_CHARS);
    point.observed_at = bounded_optional_text(point.observed_at.take(), MAX_CONTEXT_TIME_CHARS);
    point.numeric_value = point.numeric_value.filter(|value| value.is_finite());
    normalize_context_contract(&mut point.contract);
    Some(point)
}

fn visual_point_key(point: &KeyVisualDataPoint) -> String {
    format!(
        "{:?}|{}|{}",
        point.category,
        point
            .contract
            .as_ref()
            .map(contract_identity)
            .unwrap_or_default(),
        point.label.to_ascii_lowercase()
    )
}

fn normalize_episode(
    mut episode: TradeEpisodeContext,
    default_first_seen: &str,
    default_last_updated: &str,
) -> TradeEpisodeContext {
    episode.episode_id = bounded_text(&episode.episode_id, MAX_EPISODE_ID_CHARS);
    normalize_context_contract(&mut episode.contract);
    normalize_context_levels(&mut episode.levels);
    episode.latest_instruction = bounded_text(&episode.latest_instruction, MAX_INSTRUCTION_CHARS);
    episode.entry_event_id =
        bounded_optional_text(episode.entry_event_id.take(), MAX_ACTION_EVENT_ID_CHARS);
    episode.first_seen_at = bounded_text(&episode.first_seen_at, MAX_CONTEXT_TIME_CHARS);
    episode.last_updated_at = bounded_text(&episode.last_updated_at, MAX_CONTEXT_TIME_CHARS);

    if episode.first_seen_at.is_empty() {
        episode.first_seen_at = default_first_seen.to_owned();
    }
    if episode.last_updated_at.is_empty() {
        episode.last_updated_at = default_last_updated.to_owned();
    }
    if episode.episode_id.is_empty() {
        episode.episode_id = generated_episode_id(&episode);
    }
    if matches!(
        episode.status,
        TradeEpisodeStatus::Watching | TradeEpisodeStatus::ConditionalEntry
    ) {
        episode.entry_event_id = None;
    }
    episode
}

fn normalize_context_contract(contract: &mut Option<OptionContract>) {
    let Some(value) = contract.as_mut() else {
        return;
    };
    value.expiry = bounded_optional_text(value.expiry.take(), MAX_CONTEXT_LABEL_CHARS)
        .map(|expiry| expiry.to_ascii_uppercase());
    if !is_finite_positive(value.strike) {
        *contract = None;
    }
}

fn normalize_context_levels(levels: &mut Option<TradeLevels>) {
    let Some(value) = levels.as_mut() else {
        return;
    };
    value.entry = value.entry.filter(|level| is_finite_positive(*level));
    value.hard_sl = value.hard_sl.filter(|level| is_finite_positive(*level));
    value.t1 = value.t1.filter(|level| is_finite_positive(*level));
    value.t2 = value.t2.filter(|level| is_finite_positive(*level));
    if value == &TradeLevels::default() {
        *levels = None;
    }
}

fn merge_episode_with_prior(current: &mut TradeEpisodeContext, prior: &TradeEpisodeContext) {
    current.episode_id = prior.episode_id.clone();
    if current.contract.is_none() {
        current.contract = prior.contract.clone();
    }
    merge_missing_levels(&mut current.levels, prior.levels.as_ref());
    if current.latest_instruction.is_empty() {
        current.latest_instruction = prior.latest_instruction.clone();
    }
    if current.first_seen_at.is_empty() || !prior.first_seen_at.is_empty() {
        current.first_seen_at = prior.first_seen_at.clone();
    }
    if current.entry_event_id.is_none() {
        current.entry_event_id = prior.entry_event_id.clone();
    }
    if prior.status.is_unresolved()
        && !matches!(
            current.status,
            TradeEpisodeStatus::Closed | TradeEpisodeStatus::Cancelled
        )
        && episode_progress(current.status) < episode_progress(prior.status)
    {
        current.status = prior.status;
    }
}

fn episode_progress(status: TradeEpisodeStatus) -> u8 {
    match status {
        TradeEpisodeStatus::Unknown => 0,
        TradeEpisodeStatus::Watching => 1,
        TradeEpisodeStatus::ConditionalEntry => 2,
        TradeEpisodeStatus::EntryCalled => 3,
        TradeEpisodeStatus::Open => 4,
        TradeEpisodeStatus::Managing => 5,
        TradeEpisodeStatus::Closed | TradeEpisodeStatus::Cancelled => 6,
    }
}

fn merge_missing_levels(target: &mut Option<TradeLevels>, source: Option<&TradeLevels>) {
    let Some(source) = source else {
        return;
    };
    let target = target.get_or_insert_with(TradeLevels::default);
    if target.entry.is_none() {
        target.entry = source.entry;
    }
    if target.hard_sl.is_none() {
        target.hard_sl = source.hard_sl;
    }
    if target.t1.is_none() {
        target.t1 = source.t1;
    }
    if target.t2.is_none() {
        target.t2 = source.t2;
    }
}

fn episodes_match(left: &TradeEpisodeContext, right: &TradeEpisodeContext) -> bool {
    if !left.episode_id.is_empty() && !right.episode_id.is_empty() {
        if left.episode_id == right.episode_id {
            return match (&left.contract, &right.contract) {
                (Some(left), Some(right)) => contracts_match(left, right),
                _ => true,
            };
        }
        // Correct accidental id drift only while at least one side still
        // describes an unresolved episode. Distinct terminal episodes for the
        // same contract may legitimately be separate trades.
        if !(left.status.is_unresolved() || right.status.is_unresolved()) {
            return false;
        }
    }
    match (&left.contract, &right.contract) {
        (Some(left), Some(right)) => {
            // Correct model ID drift only with an exact known expiry. A
            // missing expiry is a useful wildcard when resolving one unique
            // action, but is unsafe for merging two possibly different weekly
            // trade episodes.
            left.expiry.is_some() && right.expiry.is_some() && contracts_match(left, right)
        }
        _ => false,
    }
}

fn matching_episode_for_action<'a>(
    context: &'a RollingContext,
    action: &TradeAction,
) -> Option<&'a TradeEpisodeContext> {
    if let Some(episode_id) = action.episode_id.as_deref() {
        if let Some(episode) = context
            .episodes
            .iter()
            .find(|episode| episode.episode_id == episode_id)
        {
            let contract_is_compatible = match (&action.contract, &episode.contract) {
                (Some(action_contract), Some(episode_contract)) => {
                    contracts_match(action_contract, episode_contract)
                }
                _ => true,
            };
            if contract_is_compatible {
                return Some(episode);
            }
        }
    }
    action.contract.as_ref().and_then(|contract| {
        let mut matching = context.episodes.iter().filter(|episode| {
            episode.status.is_unresolved()
                && episode
                    .contract
                    .as_ref()
                    .is_some_and(|episode_contract| contracts_match(episode_contract, contract))
        });
        let first = matching.next();
        (matching.next().is_none()).then_some(first).flatten()
    })
}

fn generated_episode_id(episode: &TradeEpisodeContext) -> String {
    let identity = episode
        .contract
        .as_ref()
        .map(contract_identity)
        .unwrap_or_else(|| "UNKNOWN".to_owned());
    bounded_text(
        &format!("episode-{identity}-{}", episode.first_seen_at),
        MAX_EPISODE_ID_CHARS,
    )
    .replace([' ', ':', '+'], "-")
}

fn contract_identity(contract: &OptionContract) -> String {
    format!(
        "{:?}-{}-{:.3}-{:?}-{:?}",
        contract.underlying,
        contract.expiry.as_deref().unwrap_or(""),
        contract.strike,
        contract.option_type,
        contract.direction
    )
}

fn current_action_event_id(action: &TradeAction, clip: &ClipWindow) -> String {
    let evidence_ms = action
        .evidence_timestamps
        .first()
        .map(|evidence| (evidence.seconds_from_clip_start * 1_000.0).round() as i64)
        .unwrap_or_default();
    let identity = action
        .episode_id
        .clone()
        .or_else(|| action.contract.as_ref().map(contract_identity))
        .or_else(|| action.trade_id.clone())
        .unwrap_or_else(|| "UNKNOWN".to_owned());
    bounded_text(
        &format!(
            "{}|{:?}|{}|{}",
            clip.started_at.timestamp_millis(),
            action.action,
            identity,
            evidence_ms
        ),
        MAX_ACTION_EVENT_ID_CHARS,
    )
}

fn bounded_optional_text(value: Option<String>, max_chars: usize) -> Option<String> {
    value
        .map(|value| bounded_text(&value, max_chars))
        .filter(|value| !value.is_empty())
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    let mut output = String::with_capacity(value.len().min(max_chars));
    let mut output_chars = 0usize;
    let mut pending_space = false;
    for character in value.chars() {
        if output_chars >= max_chars {
            break;
        }
        if character.is_control() || character.is_whitespace() {
            pending_space = !output.is_empty();
            continue;
        }
        if pending_space && output_chars + 1 < max_chars {
            output.push(' ');
            output_chars += 1;
        }
        pending_space = false;
        if output_chars >= max_chars {
            break;
        }
        output.push(character);
        output_chars += 1;
    }
    output
}

fn bounded_summary_text(value: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    // Normalize the complete bounded provider response first, then keep both
    // historical orientation and (more importantly) the newest tail. A simple
    // prefix truncation would permanently freeze a long-running summary once
    // it reached its first limit.
    let normalized = bounded_text(value, value.chars().count());
    let characters = normalized.chars().collect::<Vec<_>>();
    if characters.len() <= max_chars {
        return normalized;
    }

    const SEPARATOR: &str = " ... ";
    let separator_chars = SEPARATOR.chars().count().min(max_chars);
    let available = max_chars.saturating_sub(separator_chars);
    let head_chars = available / 4;
    let tail_chars = available.saturating_sub(head_chars);
    let mut output = String::with_capacity(max_chars);
    output.extend(characters.iter().take(head_chars).copied());
    output.extend(SEPARATOR.chars().take(separator_chars));
    output.extend(
        characters
            .iter()
            .skip(characters.len().saturating_sub(tail_chars))
            .copied(),
    );
    output
}

fn validate_action(
    action: &TradeAction,
    input: &AnalysisInput,
    rolling_context: &RollingContext,
) -> std::result::Result<(), String> {
    if action.rationale.is_empty() {
        return Err("action rationale is empty".to_owned());
    }
    if action.action.is_trade_command() && action.evidence_timestamps.is_empty() {
        return Err(
            "trade command requires evidence from the current selected source segments".to_owned(),
        );
    }

    let clip_seconds = input
        .clip
        .ended_at
        .signed_duration_since(input.clip.started_at)
        .num_milliseconds()
        .max(0) as f64
        / 1000.0;
    let final_selected_sequence = input
        .transcripts
        .iter()
        .max_by(|left, right| {
            left.ended_at
                .cmp(&right.ended_at)
                .then(left.source_sequence.cmp(&right.source_sequence))
        })
        .map(|chunk| chunk.source_sequence);
    for evidence in &action.evidence_timestamps {
        if !evidence.seconds_from_clip_start.is_finite()
            || evidence.seconds_from_clip_start < 0.0
            || evidence.seconds_from_clip_start > clip_seconds
        {
            return Err("evidence timestamp is outside the clip window".to_owned());
        }
        let Some(sequence) = evidence.source_segment_sequence else {
            return Err("trade command evidence must name a selected source segment".to_owned());
        };
        let Some(chunk) = input
            .transcripts
            .iter()
            .find(|chunk| chunk.source_sequence == sequence)
        else {
            return Err(
                "evidence source_segment_sequence is not part of the selected source segments"
                    .to_owned(),
            );
        };

        // Wire timestamps are deterministically rounded to milliseconds. Source
        // ownership is half-open [start, end), except the chronologically final
        // selected source segment owns its exact end boundary.
        let evidence_at = input.clip.started_at
            + chrono::Duration::milliseconds(
                (evidence.seconds_from_clip_start * 1_000.0).round() as i64
            );
        let owns_evidence = evidence_at >= chunk.started_at
            && (evidence_at < chunk.ended_at
                || (Some(sequence) == final_selected_sequence && evidence_at == chunk.ended_at));
        if !owns_evidence {
            return Err(
                "evidence timestamp does not time-align with its claimed source segment".to_owned(),
            );
        }
    }

    if action.action.needs_contract() {
        let contract = action
            .contract
            .as_ref()
            .ok_or_else(|| "action requires an option contract".to_owned())?;
        if contract.direction != TradeDirection::Buy {
            return Err("SELL/short-premium commands are not supported".to_owned());
        }
        if !is_finite_positive(contract.strike) {
            return Err("contract strike must be finite and positive".to_owned());
        }
    }

    if action.action == ActionKind::PlaceEntry {
        if !input.is_complete_for_entry() {
            return Err(format!(
                "new entry rejected because synchronized input is incomplete: {}",
                input.entry_input_issues().join("; ")
            ));
        }
        if matching_episode_for_action(rolling_context, action).is_none() {
            return Err(
                "PLACE_ENTRY must link to exactly one compatible rolling-context episode"
                    .to_owned(),
            );
        }
        if input
            .rolling_context
            .as_ref()
            .and_then(|context| matching_episode_for_action(context, action))
            .is_some_and(|episode| {
                episode.status.is_unresolved()
                    && episode
                        .entry_event_id
                        .as_deref()
                        .is_some_and(|event_id| !event_id.trim().is_empty())
            })
        {
            return Err(
                "PLACE_ENTRY rejected because this unresolved episode already has an entry event"
                    .to_owned(),
            );
        }
    }

    if action.action == ActionKind::PlaceEntry {
        let levels = action
            .levels
            .as_ref()
            .ok_or_else(|| "PLACE_ENTRY requires an entry level".to_owned())?;
        if levels.entry.is_none() {
            return Err("PLACE_ENTRY requires an entry level".to_owned());
        }
        validate_entry_levels_with_optional_fallbacks(levels)?;
    } else if action.action.needs_complete_levels() {
        validate_levels(
            action
                .levels
                .as_ref()
                .ok_or_else(|| "action requires entry, hard_sl, and t1 levels".to_owned())?,
        )?;
    } else if let Some(levels) = &action.levels {
        validate_any_supplied_levels(levels)?;
    }

    if matches!(
        action.action,
        ActionKind::UpdateLevels | ActionKind::Exit | ActionKind::Hold
    ) && input.open_trade_for(action).is_none()
    {
        return Err("action does not match an open trade".to_owned());
    }

    Ok(())
}

fn validate_levels(levels: &TradeLevels) -> std::result::Result<(), String> {
    let entry = levels
        .entry
        .ok_or_else(|| "entry level is required".to_owned())?;
    let hard_sl = levels
        .hard_sl
        .ok_or_else(|| "hard_sl level is required".to_owned())?;
    let t1 = levels.t1.ok_or_else(|| "t1 level is required".to_owned())?;

    for (name, value) in [("entry", entry), ("hard_sl", hard_sl), ("t1", t1)] {
        if !is_finite_positive(value) {
            return Err(format!("{name} must be finite and positive"));
        }
    }
    if !(hard_sl < entry && entry < t1) {
        return Err("levels must satisfy hard_sl < entry < t1".to_owned());
    }
    if let Some(t2) = levels.t2 {
        if !is_finite_positive(t2) {
            return Err("t2 must be finite and positive".to_owned());
        }
        if t2 <= t1 {
            return Err("t2 must be greater than t1".to_owned());
        }
    }
    Ok(())
}

fn validate_any_supplied_levels(levels: &TradeLevels) -> std::result::Result<(), String> {
    for (name, value) in [
        ("entry", levels.entry),
        ("hard_sl", levels.hard_sl),
        ("t1", levels.t1),
        ("t2", levels.t2),
    ] {
        if value.is_some_and(|value| !is_finite_positive(value)) {
            return Err(format!("{name} must be finite and positive"));
        }
    }
    Ok(())
}

fn validate_entry_levels_with_optional_fallbacks(
    levels: &TradeLevels,
) -> std::result::Result<(), String> {
    validate_any_supplied_levels(levels)?;
    let entry = levels.entry.expect("caller requires entry level");
    if levels.hard_sl.is_some_and(|hard_sl| hard_sl >= entry) {
        return Err("hard_sl must be below entry".to_owned());
    }
    if levels.t1.is_some_and(|t1| t1 <= entry) {
        return Err("t1 must be above entry".to_owned());
    }
    if let (Some(t1), Some(t2)) = (levels.t1, levels.t2) {
        if t2 <= t1 {
            return Err("t2 must be greater than t1".to_owned());
        }
    }
    Ok(())
}

fn is_finite_positive(value: f64) -> bool {
    value.is_finite() && value > 0.0
}

fn contracts_match(left: &OptionContract, right: &OptionContract) -> bool {
    left.underlying == right.underlying
        && left.option_type == right.option_type
        && (left.strike - right.strike).abs() < 0.001
        && match (&left.expiry, &right.expiry) {
            (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
            // Missing expiry is deliberately a wildcard here. Instrument
            // resolution remains the market manager's responsibility.
            _ => true,
        }
}

fn action_dedup_key(action: &TradeAction) -> String {
    let contract = action.contract.as_ref().map_or_else(
        || "NONE".to_owned(),
        |contract| {
            format!(
                "{:?}|{}|{:.3}|{:?}|{:?}",
                contract.underlying,
                contract.expiry.as_deref().unwrap_or(""),
                contract.strike,
                contract.option_type,
                contract.direction
            )
        },
    );
    format!(
        "{:?}|{}|{}",
        action.action,
        action.trade_id.as_deref().unwrap_or(""),
        contract
    )
}

fn build_request_body(
    model: &str,
    input: &AnalysisInput,
    jpeg_bytes: Option<&[u8]>,
) -> Result<Value> {
    let entry_issues = input.entry_input_issues();
    let mut prompt_input = input.clone();
    prompt_input.rolling_context = input.rolling_context.as_ref().and_then(|context| {
        // Treat carried context as both the proposed snapshot and the prior
        // authoritative state. This preserves a proved entry event while the
        // same normalizer still downgrades a forged one with no APPLIED runtime
        // outcome; never detach outcomes and reattach them after sanitizing.
        let mut bounded =
            normalize_rolling_context(context.clone(), Some(context), &input.clip).ok()?;
        // A carried point is never current evidence, even if a saved fixture or
        // older model response left this flag set.
        for point in &mut bounded.key_visual_points {
            point.observed_in_current_clip = false;
        }
        Some(bounded)
    });
    let prompt_context = json!({
        "entry_input_complete": entry_issues.is_empty(),
        "entry_input_issues": entry_issues,
        "context": prompt_input,
    });
    let prompt_text = serde_json::to_string(&prompt_context)
        .context("failed to serialize Analysis analysis context")?;

    let mut content = vec![json!({ "type": "input_text", "text": prompt_text })];
    if let Some(jpeg) = jpeg_bytes.filter(|jpeg| !jpeg.is_empty()) {
        let base64 = base64_encode(jpeg);
        content.push(json!({
            "type": "input_image",
            "image_url": format!("data:image/jpeg;base64,{base64}"),
            "detail": "high"
        }));
    }

    Ok(json!({
        "model": model,
        "store": false,
        "service_tier": "fast",
        "prompt_cache_key": PROMPT_CACHE_KEY,
        "reasoning": { "effort": "low" },
        "instructions": SYSTEM_INSTRUCTION,
        "input": [{ "role": "user", "content": content }],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "trade_observation",
                "strict": true,
                "schema": response_json_schema()
            }
        }
    }))
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for group in bytes.chunks(3) {
        let first = group[0];
        let second = *group.get(1).unwrap_or(&0);
        let third = *group.get(2).unwrap_or(&0);
        output.push(TABLE[(first >> 2) as usize] as char);
        output.push(TABLE[(((first & 0b11) << 4) | (second >> 4)) as usize] as char);
        output.push(if group.len() > 1 {
            TABLE[(((second & 0b1111) << 2) | (third >> 6)) as usize] as char
        } else {
            '='
        });
        output.push(if group.len() > 2 {
            TABLE[(third & 0b11_1111) as usize] as char
        } else {
            '='
        });
    }
    output
}

fn response_json_schema() -> Value {
    let contract_schema = json!({
        "type": ["object", "null"],
        "properties": {
            "underlying": { "type": "string", "enum": ["NIFTY", "SENSEX"] },
            "expiry": { "type": ["string", "null"], "description": "Only when explicitly stated or clearly visible." },
            "strike": { "type": "number", "minimum": 0.01 },
            "option_type": { "type": "string", "enum": ["CE", "PE"] },
            "direction": { "type": "string", "enum": ["BUY", "SELL"] }
        },
        "required": ["underlying", "expiry", "strike", "option_type", "direction"],
        "additionalProperties": false
    });
    let levels_schema = json!({
        "type": ["object", "null"],
        "properties": {
            "entry": { "type": ["number", "null"], "minimum": 0.01 },
            "hard_sl": { "type": ["number", "null"], "minimum": 0.01 },
            "t1": { "type": ["number", "null"], "minimum": 0.01 },
            "t2": { "type": ["number", "null"], "minimum": 0.01 }
        },
        "required": ["entry", "hard_sl", "t1", "t2"],
        "additionalProperties": false
    });
    let evidence_schema = json!({
        "type": "object",
        "properties": {
            "seconds_from_clip_start": { "type": "number", "minimum": 0 },
            "source": { "type": "string", "enum": ["VIDEO", "TRANSCRIPT", "BOTH"] },
            "source_segment_sequence": { "type": ["integer", "null"], "minimum": 0 },
            "detail": { "type": ["string", "null"] }
        },
        "required": ["seconds_from_clip_start", "source", "source_segment_sequence", "detail"],
        "additionalProperties": false
    });
    let key_visual_point_schema = json!({
        "type": "object",
        "properties": {
            "category": {
                "type": "string",
                "enum": [
                    "CONTRACT", "ENTRY", "STOP_LOSS", "TARGET", "PNL",
                    "POSITION_STATUS", "ORDER_STATUS", "CHART_ANNOTATION",
                    "CAPTION", "OTHER"
                ]
            },
            "label": { "type": "string" },
            "value": { "type": "string" },
            "contract": contract_schema.clone(),
            "numeric_value": { "type": ["number", "null"] },
            "unit": { "type": ["string", "null"] },
            "observed_at": { "type": ["string", "null"] },
            "observed_in_current_clip": { "type": "boolean" }
        },
        "required": ["category", "label", "value", "contract", "numeric_value", "unit", "observed_at", "observed_in_current_clip"],
        "additionalProperties": false
    });
    let episode_schema = json!({
        "type": "object",
        "properties": {
            "episode_id": { "type": "string" },
            "contract": contract_schema.clone(),
            "status": {
                "type": "string",
                "enum": [
                    "WATCHING", "CONDITIONAL_ENTRY", "ENTRY_CALLED", "OPEN",
                    "MANAGING", "CLOSED", "CANCELLED", "UNKNOWN"
                ]
            },
            "levels": levels_schema.clone(),
            "latest_instruction": { "type": "string" },
            "entry_event_id": { "type": ["string", "null"] },
            "first_seen_at": { "type": "string" },
            "last_updated_at": { "type": "string" }
        },
        "required": [
            "episode_id", "contract", "status", "levels", "latest_instruction",
            "entry_event_id", "first_seen_at", "last_updated_at"
        ],
        "additionalProperties": false
    });

    json!({
        "type": "object",
        "properties": {
            "market_bias": {
                "type": "object",
                "properties": {
                    "direction": {
                        "type": "string",
                        "enum": ["BULLISH", "BEARISH", "NEUTRAL", "MIXED", "UNKNOWN"]
                    },
                    "rationale": { "type": "string" }
                },
                "required": ["direction", "rationale"],
                "additionalProperties": false
            },
            "freshness": {
                "type": "object",
                "properties": {
                    "status": { "type": "string", "enum": ["FRESH", "STALE", "INCOMPLETE", "UNKNOWN"] },
                    "input_data_age_ms": { "type": "integer", "minimum": 0 },
                    "usable_for_new_entries": { "type": "boolean" },
                    "rationale": { "type": "string" }
                },
                "required": ["status", "input_data_age_ms", "usable_for_new_entries", "rationale"],
                "additionalProperties": false
            },
            "rolling_context": {
                "type": "object",
                "properties": {
                    "spoken_summary": { "type": "string" },
                    "visual_summary": { "type": "string" },
                    "combined_summary": { "type": "string" },
                    "key_visual_points": {
                        "type": "array",
                        "items": key_visual_point_schema
                    },
                    "episodes": {
                        "type": "array",
                        "items": episode_schema
                    }
                },
                "required": [
                    "spoken_summary", "visual_summary", "combined_summary",
                    "key_visual_points", "episodes"
                ],
                "additionalProperties": false
            },
            "actions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "action": {
                            "type": "string",
                            "enum": ["WATCH", "PLACE_ENTRY", "CANCEL_ENTRY", "UPDATE_LEVELS", "EXIT", "HOLD", "IGNORE"]
                        },
                        "episode_id": { "type": ["string", "null"] },
                        "event_id": { "type": ["string", "null"] },
                        "trade_id": { "type": ["string", "null"] },
                        "contract": contract_schema,
                        "levels": levels_schema,
                        "evidence_timestamps": {
                            "type": "array",
                            "items": evidence_schema
                        },
                        "rationale": { "type": "string" }
                    },
                    "required": ["action", "episode_id", "event_id", "trade_id", "contract", "levels", "evidence_timestamps", "rationale"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["market_bias", "freshness", "rolling_context", "actions"],
        "additionalProperties": false
    })
}

#[derive(Debug, Deserialize)]
struct OpenAiErrorEnvelope {
    error: Option<OpenAiErrorDetail>,
}

#[derive(Debug, Deserialize)]
struct OpenAiErrorDetail {
    message: Option<String>,
}

async fn extract_openai_error_message(response: reqwest::Response) -> Option<String> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_PROVIDER_ERROR_BODY_BYTES)
    {
        return None;
    }
    let mut body = Vec::new();
    let mut response = response;
    while let Some(chunk) = response.chunk().await.ok()? {
        if !append_bounded_error_chunk(&mut body, &chunk) {
            return None;
        }
    }
    parse_openai_error_message(&body)
}

fn append_bounded_error_chunk(buffer: &mut Vec<u8>, chunk: &[u8]) -> bool {
    let Some(new_len) = buffer.len().checked_add(chunk.len()) else {
        return false;
    };
    if new_len > MAX_PROVIDER_ERROR_BODY_BYTES as usize {
        return false;
    }
    buffer.extend_from_slice(chunk);
    true
}

fn parse_openai_error_message(bytes: &[u8]) -> Option<String> {
    let envelope: OpenAiErrorEnvelope = serde_json::from_slice(bytes).ok()?;
    sanitize_provider_error_message(envelope.error?.message?.as_str())
}

fn sanitize_provider_error_message(raw: &str) -> Option<String> {
    let normalized = raw
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    let single_line = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if single_line.is_empty() {
        return None;
    }

    let redacted = redact_api_keys(&single_line);
    let mut truncated = redacted
        .chars()
        .take(MAX_PROVIDER_ERROR_MESSAGE_CHARS)
        .collect::<String>();
    if redacted.chars().count() > MAX_PROVIDER_ERROR_MESSAGE_CHARS {
        truncated.push_str("...");
    }
    Some(truncated)
}

fn redact_api_keys(input: &str) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    while index < chars.len() {
        let starts_like_key = chars.get(index..index + 3) == Some(&['s', 'k', '-'])
            || chars.get(index..index + 4) == Some(&['A', 'I', 'z', 'a']);
        if starts_like_key {
            let mut end = index + if chars[index] == 's' { 3 } else { 4 };
            while end < chars.len()
                && (chars[end].is_ascii_alphanumeric() || matches!(chars[end], '_' | '-'))
            {
                end += 1;
            }
            if end - index >= 20 {
                output.push_str("[REDACTED_API_KEY]");
                index = end;
                continue;
            }
        }
        output.push(chars[index]);
        index += 1;
    }
    output
}

#[derive(Debug, Deserialize)]
struct ResponsesResponse {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    output_text: Option<String>,
    #[serde(default)]
    output: Vec<ResponsesOutputItem>,
    #[serde(default)]
    usage: ResponsesUsage,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ResponsesUsage {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    total_tokens: Option<u64>,
}

impl From<ResponsesUsage> for UsageTelemetry {
    fn from(value: ResponsesUsage) -> Self {
        Self::from_totals(value.input_tokens, value.output_tokens, value.total_tokens)
    }
}

#[derive(Debug, Deserialize)]
struct ResponsesOutputItem {
    #[serde(rename = "type")]
    item_type: Option<String>,
    #[serde(default)]
    content: Vec<ResponsesContent>,
}

#[derive(Debug, Deserialize)]
struct ResponsesContent {
    #[serde(rename = "type")]
    content_type: Option<String>,
    #[serde(default)]
    text: Option<String>,
}

fn parse_responses_response(
    response: ResponsesResponse,
    input: &AnalysisInput,
) -> Result<ValidatedAnalysis> {
    if response
        .status
        .as_deref()
        .is_some_and(|status| status != "completed")
    {
        bail!(
            "OpenAI Responses API did not complete (status={})",
            safe_status(response.status.as_deref().unwrap_or_default())
        );
    }
    let text = response
        .output_text
        .or_else(|| {
            response.output.iter().rev().find_map(|item| {
                (item.item_type.as_deref() == Some("message"))
                    .then(|| {
                        item.content
                            .iter()
                            .filter_map(|part| {
                                matches!(
                                    part.content_type.as_deref(),
                                    Some("output_text") | Some("text")
                                )
                                .then_some(part.text.as_deref().unwrap_or_default())
                            })
                            .collect::<String>()
                    })
                    .filter(|text| !text.trim().is_empty())
            })
        })
        .ok_or_else(|| anyhow!("OpenAI Responses API has no structured text output"))?;
    let parsed = parse_model_output(&text, "OpenAI Responses API output")?;
    normalize_and_validate(parsed, input, response.id)
}

fn safe_status(status: &str) -> &str {
    match status {
        "in_progress" | "completed" | "failed" | "cancelled" | "incomplete" => status,
        _ => "unknown",
    }
}

/*
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InteractionStep {
    ModelOutput {
        #[serde(default)]
        content: Vec<InteractionContent>,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InteractionContent {
    Text {
        text: String,
    },
    #[serde(other)]
    Other,
}

fn parse_interaction(
    interaction: InteractionResponse,
    input: &AnalysisInput,
) -> Result<ValidatedAnalysis> {
    if interaction.status != "completed" {
        bail!(
            "Analysis interaction did not complete (status={})",
            safe_status(&interaction.status)
        );
    }

    let model_text = interaction.steps.iter().rev().find_map(|step| match step {
        InteractionStep::ModelOutput { content } => {
            let joined = content
                .iter()
                .filter_map(|content| match content {
                    InteractionContent::Text { text } => Some(text.as_str()),
                    InteractionContent::Other => None,
                })
                .collect::<String>();
            (!joined.trim().is_empty()).then_some(joined)
        }
        InteractionStep::Other => None,
    });
    let text = model_text.ok_or_else(|| anyhow!("Analysis interaction has no model text output"))?;
    let parsed: AnalysisResult = serde_json::from_str(&text)
        .map_err(|_| anyhow!("Analysis model output is not valid schema-shaped JSON"))?;
    normalize_and_validate(parsed, input, interaction.id)
}

fn safe_status(status: &str) -> &str {
    match status {
        "in_progress" | "requires_action" | "completed" | "failed" | "cancelled" | "incomplete" => {
            status
        }
        _ => "unknown",
    }
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn test_key_ring(count: usize) -> AnalysisKeyRing {
        let now = Instant::now();
        AnalysisKeyRing {
            slots: (0..count)
                .map(|index| AnalysisKeySlot {
                    header: HeaderValue::from_str(&format!("test-key-{index}")).unwrap(),
                    cooldown_until: now,
                    successes: 0,
                    failures: 0,
                    last_failure: None,
                    rate_limit: None,
                    daily_usage: DailyUsageCounter::default(),
                    failed_since_load: false,
                })
                .collect(),
            cursor: 0,
            generation: 0,
        }
    }

    #[tokio::test]
    async fn runtime_vault_caps_slots_redacts_health_and_clears_after_all_failures() {
        let vault = RuntimeKeyVault::empty();
        assert_eq!(
            vault
                .add([
                    "first-test-key",
                    "second-test-key",
                    "third-test-key",
                    "ignored-test-key"
                ])
                .await
                .unwrap(),
            3
        );

        let initial = vault.health().await;
        assert_eq!(initial.loaded_slots, 3);
        assert_eq!(initial.state, VaultState::Ready);
        let serialized = serde_json::to_string(&initial).unwrap();
        assert!(!serialized.contains("first-test-key"));

        for slot in 0..3 {
            vault.record_failure(slot, "QUOTA", Duration::ZERO).await;
        }

        let cleared = vault.health().await;
        assert_eq!(cleared.loaded_slots, 0);
        assert_eq!(cleared.state, VaultState::KeysRequired);
    }

    #[tokio::test]
    async fn runtime_vault_deduplicates_submitted_key_material() {
        let vault = RuntimeKeyVault::empty();
        assert_eq!(
            vault.add(["same-test-key", "same-test-key"]).await.unwrap(),
            1
        );
        assert_eq!(vault.health().await.loaded_slots, 1);
    }

    #[test]
    fn shared_key_ring_keeps_the_successful_key_sticky() {
        let mut ring = test_key_ring(3);

        let first = ring.next_available(&HashSet::new()).unwrap().0;
        ring.record_success(first);
        let second = ring.next_available(&HashSet::new()).unwrap().0;
        ring.record_success(second);
        let third = ring.next_available(&HashSet::new()).unwrap().0;

        assert_eq!([first, second, third], [0, 0, 0]);
    }

    #[test]
    fn quota_failure_borrows_the_next_healthy_key_for_same_request() {
        let mut ring = test_key_ring(3);
        let mut attempted = HashSet::new();

        let exhausted = ring.next_available(&attempted).unwrap().0;
        attempted.insert(exhausted);
        ring.record_failure(exhausted, "QUOTA", Duration::from_secs(60));
        let borrowed = ring.next_available(&attempted).unwrap().0;

        assert_eq!(exhausted, 0);
        assert_eq!(borrowed, 1);
        assert_eq!(ring.slots[0].last_failure, Some("QUOTA"));
        assert!(ring.slots[0].cooldown_until > Instant::now());
    }

    #[test]
    fn auth_failure_cools_the_bad_slot_and_falls_back_sequentially() {
        let mut ring = test_key_ring(2);
        let mut attempted = HashSet::new();
        let first = ring.next_available(&attempted).unwrap().0;
        attempted.insert(first);
        let (class, cooldown) = analysis_retry_policy(401).unwrap();
        ring.record_failure(first, class, cooldown);
        let fallback = ring.next_available(&attempted).unwrap().0;
        assert_eq!(first, 0);
        assert_eq!(fallback, 1);
        assert_eq!(ring.slots[first].last_failure, Some("AUTH"));
    }

    #[test]
    fn all_transient_or_unusable_response_statuses_try_the_next_key() {
        assert!(analysis_retry_policy(429).is_some());
        assert!(analysis_retry_policy(500).is_some());
        assert!(analysis_retry_policy(503).is_some());
        assert_eq!(analysis_retry_policy(401).unwrap().0, "AUTH");
        assert_eq!(analysis_retry_policy(403).unwrap().0, "AUTH");
        assert_eq!(analysis_retry_policy(408).unwrap().0, "TIMEOUT");
    }

    #[test]
    fn authorization_header_is_bearer_and_sensitive() {
        let header = authorization_header_for_key("test-secret").unwrap();
        assert_eq!(header.to_str().unwrap(), "Bearer test-secret");
        assert!(header.is_sensitive());
    }

    #[test]
    fn bounded_error_accumulator_rejects_chunked_body_over_16_kib() {
        let mut body = Vec::new();
        assert!(append_bounded_error_chunk(&mut body, &[b'x'; 8 * 1024]));
        assert_eq!(body.len(), 8 * 1024);
        assert!(!append_bounded_error_chunk(
            &mut body,
            &[b'y'; 8 * 1024 + 1]
        ));
        assert_eq!(body.len(), 8 * 1024);
    }

    #[test]
    fn usage_counter_resets_when_ist_day_changes() {
        let mut counter = DailyUsageCounter::default();
        counter.record_request("2026-08-15");
        counter.record_usage(
            "2026-08-15",
            UsageTelemetry::from_totals(Some(11), Some(7), Some(4)),
        );
        // A rejected/transport attempt has no usage, but must count locally.
        counter.record_request("2026-08-15");
        assert_eq!(counter.request_count, 2);
        assert_eq!(counter.total_tokens, 4);
        counter.record_request("2026-08-16");
        counter.record_usage(
            "2026-08-16",
            UsageTelemetry::from_totals(Some(3), Some(2), None),
        );
        assert_eq!(counter.day_ist, "2026-08-16");
        assert_eq!(counter.request_count, 1);
        assert_eq!(counter.total_tokens, 5);
    }

    #[test]
    fn daily_attempts_are_counted_before_auth_quota_or_transport_results() {
        let mut ring = test_key_ring(3);
        for (slot, class) in [(0, "AUTH"), (1, "QUOTA"), (2, "TRANSPORT")] {
            ring.record_request(slot);
            ring.record_failure(slot, class, Duration::from_secs(1));
        }

        for slot in &ring.slots {
            assert_eq!(slot.daily_usage.request_count, 1);
            assert_eq!(slot.daily_usage.total_tokens, 0);
        }
    }

    #[test]
    fn rate_limit_headers_are_reduced_to_safe_numeric_telemetry() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-ratelimit-limit-requests",
            HeaderValue::from_static("300"),
        );
        headers.insert(
            "x-ratelimit-remaining-requests",
            HeaderValue::from_static("299"),
        );
        headers.insert(
            "x-ratelimit-reset-requests",
            HeaderValue::from_static("1.5s"),
        );
        headers.insert("retry-after", HeaderValue::from_static("3"));
        headers.insert(
            "x-ratelimit-limit-tokens",
            HeaderValue::from_static("500000"),
        );
        headers.insert(
            "x-ratelimit-remaining-tokens",
            HeaderValue::from_static("400000"),
        );
        headers.insert("x-ratelimit-reset-tokens", HeaderValue::from_static("2m"));
        let telemetry = parse_rate_limit_headers(&headers);
        assert_eq!(telemetry.request_limit, Some(300));
        assert_eq!(telemetry.request_remaining, Some(299));
        assert_eq!(telemetry.request_reset_ms, Some(1_500));
        assert_eq!(telemetry.retry_after_ms, Some(3_000));
        assert_eq!(telemetry.token_limit, Some(500_000));
        assert_eq!(telemetry.token_remaining, Some(400_000));
        assert_eq!(telemetry.token_reset_ms, Some(120_000));

        headers.insert(
            "x-ratelimit-reset-requests",
            HeaderValue::from_static("250ms"),
        );
        headers.insert("x-ratelimit-reset-tokens", HeaderValue::from_static("1.5h"));
        let telemetry = parse_rate_limit_headers(&headers);
        assert_eq!(telemetry.request_reset_ms, Some(250));
        assert_eq!(telemetry.token_reset_ms, Some(5_400_000));

        headers.insert(
            "x-ratelimit-reset-requests",
            HeaderValue::from_static("bad"),
        );
        assert_eq!(parse_rate_limit_headers(&headers).request_reset_ms, None);
    }

    fn at(second: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_800_000_000 + second, 0)
            .single()
            .unwrap()
    }

    fn contract(direction: TradeDirection) -> OptionContract {
        OptionContract {
            underlying: Underlying::Nifty,
            expiry: Some("2026-08-13".to_owned()),
            strike: 25_000.0,
            option_type: OptionType::Ce,
            direction,
        }
    }

    fn complete_input() -> AnalysisInput {
        AnalysisInput {
            clip: ClipWindow {
                started_at: at(0),
                ended_at: at(12),
                sent_at: at(13),
                data_age_ms: 1_000,
                complete: true,
            },
            transcripts: (0..4)
                .map(|index| TranscriptChunk {
                    source_sequence: index,
                    started_at: at(index as i64 * 3),
                    ended_at: at((index as i64 + 1) * 3),
                    text: format!("chunk {index}"),
                    complete: true,
                })
                .collect(),
            watched_options: vec![WatchedOptionSnapshot {
                contract: contract(TradeDirection::Buy),
                price: PriceSnapshot {
                    ltp: Some(112.0),
                    observed_at: Some(at(9)),
                    age_ms: Some(25),
                    fresh: true,
                },
                watch_remaining_ms: 8_000,
            }],
            open_trades: vec![OpenTradeSnapshot {
                trade_id: "trade-1".to_owned(),
                contract: contract(TradeDirection::Buy),
                quantity: 65,
                entry_price: 110.0,
                price: PriceSnapshot {
                    ltp: Some(118.0),
                    observed_at: Some(at(9)),
                    age_ms: Some(25),
                    fresh: true,
                },
                unrealized_pnl: 520.0,
                hard_sl: 100.0,
                effective_sl: 105.0,
                t1: 125.0,
                t2: Some(140.0),
                trailing_phase: 1,
                exit_mode: ExitMode::Llm,
            }],
            rolling_context: None,
        }
    }

    #[test]
    fn complete_entry_input_is_a_four_segment_three_second_set() {
        let mut input = complete_input();
        input.clip.ended_at = at(12);
        input.clip.sent_at = at(13);
        input.clip.data_age_ms = 1_000;
        input.transcripts = (0..4)
            .map(|index| TranscriptChunk {
                source_sequence: index,
                started_at: at(index as i64 * 3),
                ended_at: at((index as i64 + 1) * 3),
                text: format!("chunk {index}"),
                complete: true,
            })
            .collect();

        assert_eq!(input.entry_input_issues(), Vec::<String>::new());

        let mut legacy = input.clone();
        legacy.transcripts = (0..2)
            .map(|index| TranscriptChunk {
                source_sequence: index,
                started_at: at(index as i64 * 5),
                ended_at: at((index as i64 + 1) * 5),
                text: format!("legacy chunk {index}"),
                complete: true,
            })
            .collect();
        let issues = legacy.entry_input_issues();
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("exactly one must-pass or four retained"))
        );
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("exactly one must-pass or four retained"))
        );
    }

    #[test]
    fn entry_input_accepts_nonconsecutive_selected_three_second_source_segments() {
        let mut input = complete_input();
        input.clip.started_at = at(3);
        input.clip.ended_at = at(21);
        input.clip.sent_at = at(22);
        input.transcripts = [1_u64, 4, 5, 6]
            .into_iter()
            .map(|sequence| TranscriptChunk {
                source_sequence: sequence,
                started_at: at((sequence * 3) as i64),
                ended_at: at(((sequence + 1) * 3) as i64),
                text: format!("segment {sequence}"),
                complete: true,
            })
            .collect();

        assert!(input.entry_input_issues().is_empty());
    }

    #[test]
    fn evidence_must_time_align_with_its_claimed_nonconsecutive_source_segment() {
        let mut input = complete_input();
        input.clip.started_at = at(3);
        input.clip.ended_at = at(21);
        input.clip.sent_at = at(22);
        input.transcripts = [1_u64, 4, 5, 6]
            .into_iter()
            .map(|sequence| TranscriptChunk {
                source_sequence: sequence,
                started_at: at((sequence * 3) as i64),
                ended_at: at(((sequence + 1) * 3) as i64),
                text: format!("segment {sequence}"),
                complete: true,
            })
            .collect();

        let mut mismatched = place_entry("BUY");
        mismatched["evidence_timestamps"] = json!([{
            "seconds_from_clip_start": 9.5,
            "source": "TRANSCRIPT",
            "source_segment_sequence": 1
        }]);
        let rejected = parse_and_validate_output(&output_with_action(mismatched), &input).unwrap();
        assert!(rejected.actions.is_empty());
        assert!(
            rejected.rejected_actions[0]
                .reason
                .contains("claimed source segment")
        );

        let mut aligned = place_entry("BUY");
        aligned["evidence_timestamps"] = json!([{
            "seconds_from_clip_start": 9.5,
            "source": "TRANSCRIPT",
            "source_segment_sequence": 4
        }]);
        let accepted = parse_and_validate_output(&output_with_action(aligned), &input).unwrap();
        assert_eq!(accepted.actions.len(), 1);
        assert!(accepted.rejected_actions.is_empty());
    }

    #[test]
    fn evidence_source_ownership_uses_half_open_chunk_boundaries() {
        let input = complete_input();
        let parse = |seconds: f64, sequence: u64| {
            let mut action = place_entry("BUY");
            action["evidence_timestamps"] = json!([{
                "seconds_from_clip_start": seconds,
                "source": "TRANSCRIPT",
                "source_segment_sequence": sequence
            }]);
            parse_and_validate_output(&output_with_action(action), &input).unwrap()
        };

        assert_eq!(parse(5.999, 1).actions.len(), 1);
        assert_eq!(parse(6.0, 2).actions.len(), 1);
        assert!(parse(6.0, 1).actions.is_empty());
        assert!(parse(6.001, 1).actions.is_empty());

        assert_eq!(parse(12.0, 3).actions.len(), 1);
        assert!(parse(12.0, 2).actions.is_empty());
    }

    fn rolling_context_json() -> Value {
        json!({
            "spoken_summary": "The streamer is discussing a NIFTY call setup.",
            "visual_summary": "The chart shows NIFTY 25000 CE with marked levels.",
            "combined_summary": "One NIFTY 25000 CE setup is being tracked across speech and chart.",
            "key_visual_points": [{
                "category": "CONTRACT",
                "label": "chart contract",
                "value": "NIFTY 25000 CE",
                "contract": {
                    "underlying": "NIFTY",
                    "expiry": "2026-08-13",
                    "strike": 25000,
                    "option_type": "CE",
                    "direction": "BUY"
                },
                "observed_in_current_clip": true
            }],
            "episodes": [{
                "episode_id": "episode-nifty-25000-ce-1",
                "contract": {
                    "underlying": "NIFTY",
                    "expiry": "2026-08-13",
                    "strike": 25000,
                    "option_type": "CE",
                    "direction": "BUY"
                },
                "status": "ENTRY_CALLED",
                "levels": { "entry": 110, "hard_sl": 100, "t1": 125, "t2": 140 },
                "latest_instruction": "Enter now near 110.",
                "first_seen_at": at(0).to_rfc3339(),
                "last_updated_at": at(8).to_rfc3339()
            }]
        })
    }

    fn output_with_action(action_json: Value) -> String {
        json!({
            "market_bias": {
                "direction": "BULLISH",
                "rationale": "positive price action"
            },
            "freshness": {
                "status": "UNKNOWN",
                "input_data_age_ms": 1,
                "usable_for_new_entries": false,
                "rationale": "model estimate"
            },
            "rolling_context": rolling_context_json(),
            "actions": [action_json]
        })
        .to_string()
    }

    fn place_entry(direction: &str) -> Value {
        json!({
            "action": "PLACE_ENTRY",
            "contract": {
                "underlying": "NIFTY",
                "expiry": " 2026-08-13 ",
                "strike": 25000,
                "option_type": "CE",
                "direction": direction
            },
            "levels": { "entry": 110, "hard_sl": 100, "t1": 125, "t2": 140 },
            "evidence_timestamps": [
                { "seconds_from_clip_start": 6.5, "source": "BOTH", "source_segment_sequence": 2 },
                { "seconds_from_clip_start": 6.5, "source": "BOTH", "source_segment_sequence": 2 }
            ],
            "rationale": " explicit entry "
        })
    }

    fn ignore_action() -> Value {
        json!({
            "action": "IGNORE",
            "evidence_timestamps": [],
            "rationale": "No current trade instruction."
        })
    }

    #[test]
    fn accepts_and_normalizes_first_pass_place_entry_without_watched_ltp() {
        let mut input = complete_input();
        input.watched_options.clear();
        let result =
            parse_and_validate_output(&output_with_action(place_entry("BUY")), &input).unwrap();

        assert_eq!(result.actions.len(), 1);
        assert!(result.rejected_actions.is_empty());
        assert_eq!(result.actions[0].rationale, "explicit entry");
        assert_eq!(result.actions[0].evidence_timestamps.len(), 1);
        assert_eq!(
            result.actions[0]
                .contract
                .as_ref()
                .unwrap()
                .expiry
                .as_deref(),
            Some("2026-08-13")
        );
        assert_eq!(result.freshness.input_data_age_ms, 1_000);
        assert!(result.freshness.usable_for_new_entries);
        assert_eq!(result.freshness.status, FreshnessStatus::Stale);
    }

    #[test]
    fn accepts_scoreless_entries_but_rejects_sell_commands() {
        let input = complete_input();
        let low =
            parse_and_validate_output(&output_with_action(place_entry("BUY")), &input).unwrap();
        assert_eq!(low.actions.len(), 1);

        let sell =
            parse_and_validate_output(&output_with_action(place_entry("SELL")), &input).unwrap();
        assert!(sell.actions.is_empty());
        assert!(sell.rejected_actions[0].reason.contains("SELL"));
    }

    #[test]
    fn rejects_invalid_levels_and_incomplete_entry_input() {
        let mut bad_levels = place_entry("BUY");
        bad_levels["levels"] = json!({ "entry": 110, "hard_sl": 115, "t1": 125 });
        let invalid =
            parse_and_validate_output(&output_with_action(bad_levels), &complete_input()).unwrap();
        assert!(invalid.actions.is_empty());
        assert!(invalid.rejected_actions[0].reason.contains("hard_sl"));

        let mut incomplete = complete_input();
        incomplete.transcripts[1].complete = false;
        let rejected =
            parse_and_validate_output(&output_with_action(place_entry("BUY")), &incomplete)
                .unwrap();
        assert!(rejected.actions.is_empty());
        assert!(rejected.rejected_actions[0].reason.contains("incomplete"));
        assert_eq!(rejected.freshness.status, FreshnessStatus::Incomplete);
    }

    #[test]
    fn fills_unchanged_update_levels_from_open_trade() {
        let action = json!({
            "action": "UPDATE_LEVELS",
            "trade_id": "trade-1",
            "contract": {
                "underlying": "NIFTY",
                "strike": 25000,
                "option_type": "CE",
                "direction": "BUY"
            },
            "levels": { "t2": 145 },
            "evidence_timestamps": [{ "seconds_from_clip_start": 7, "source": "TRANSCRIPT", "source_segment_sequence": 2 }],
            "rationale": "target raised"
        });
        let result =
            parse_and_validate_output(&output_with_action(action), &complete_input()).unwrap();
        let levels = result.actions[0].levels.as_ref().unwrap();
        assert_eq!(levels.entry, Some(110.0));
        assert_eq!(levels.hard_sl, Some(100.0));
        assert_eq!(levels.t1, Some(125.0));
        assert_eq!(levels.t2, Some(145.0));
    }

    #[test]
    fn prior_context_can_supply_setup_but_not_current_entry_evidence() {
        let mut input = complete_input();
        let mut prior: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        prior.episodes[0].status = TradeEpisodeStatus::ConditionalEntry;
        prior.episodes[0].entry_event_id = None;
        input.rolling_context = Some(prior);

        let with_current_evidence = json!({
            "action": "PLACE_ENTRY",
            "episode_id": "episode-nifty-25000-ce-1",
            "evidence_timestamps": [{
                "seconds_from_clip_start": 6,
                "source": "BOTH",
                "source_segment_sequence": 2,
                "detail": "Streamer explicitly says enter now."
            }],
            "rationale": "Current clip confirms the conditional entry."
        });
        let accepted =
            parse_and_validate_output(&output_with_action(with_current_evidence), &input).unwrap();
        assert_eq!(accepted.actions.len(), 1);
        assert_eq!(
            accepted.actions[0].contract.as_ref(),
            input.rolling_context.as_ref().unwrap().episodes[0]
                .contract
                .as_ref()
        );
        assert!(accepted.actions[0].levels.is_some());
        assert!(accepted.actions[0].event_id.is_some());
        // Validation preserves a retriable entry episode.  The paper runtime
        // records the event only after a real order is placed.
        assert_eq!(accepted.rolling_context.episodes[0].entry_event_id, None);

        let mut context_only = place_entry("BUY");
        context_only["evidence_timestamps"] = json!([]);
        let rejected =
            parse_and_validate_output(&output_with_action(context_only), &input).unwrap();
        assert!(rejected.actions.is_empty());
        assert!(
            rejected.rejected_actions[0]
                .reason
                .contains("current selected source segments")
        );
    }

    #[test]
    fn compact_system_instruction_retains_required_trade_safeguards() {
        assert!(SYSTEM_INSTRUCTION.len() <= 1_200);
        assert!(SYSTEM_INSTRUCTION.contains("paper-only"));
        assert!(SYSTEM_INSTRUCTION.contains("current selected source"));
        assert!(SYSTEM_INSTRUCTION.contains("semantic"));
        assert!(SYSTEM_INSTRUCTION.contains("entry_event_id"));
        assert!(SYSTEM_INSTRUCTION.contains("hard_sl < entry < t1"));
    }

    #[test]
    fn entry_with_an_explicit_price_but_no_streamer_levels_reaches_runtime_fallback() {
        let mut action = place_entry("BUY");
        action["levels"] = json!({ "entry": 110 });
        let mut output: Value = serde_json::from_str(&output_with_action(action)).unwrap();
        output["rolling_context"]["episodes"][0]["levels"] = Value::Null;
        output["rolling_context"]["episodes"][0]["status"] = json!("CONDITIONAL_ENTRY");

        let parsed = parse_and_validate_output(&output.to_string(), &complete_input()).unwrap();

        assert_eq!(parsed.actions.len(), 1);
        assert_eq!(
            parsed.actions[0].levels.as_ref().unwrap().entry,
            Some(110.0)
        );
        assert_eq!(parsed.actions[0].levels.as_ref().unwrap().hard_sl, None);
        assert_eq!(parsed.actions[0].levels.as_ref().unwrap().t1, None);
    }

    #[test]
    fn rejects_trade_evidence_for_an_unselected_source_sequence() {
        let mut input = complete_input();
        let mut prior: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        prior.episodes[0].status = TradeEpisodeStatus::ConditionalEntry;
        prior.episodes[0].entry_event_id = None;
        input.rolling_context = Some(prior);
        let action = json!({
            "action": "PLACE_ENTRY",
            "episode_id": "episode-nifty-25000-ce-1",
            "evidence_timestamps": [{
                "seconds_from_clip_start": 6,
                "source": "BOTH",
                "source_segment_sequence": 999
            }],
            "rationale": "Current clip confirms the conditional entry."
        });
        let parsed = parse_and_validate_output(&output_with_action(action), &input).unwrap();
        assert!(parsed.actions.is_empty());
        assert!(
            parsed.rejected_actions[0]
                .reason
                .contains("not part of the selected source segments")
        );
    }

    #[test]
    fn prior_episode_supplies_missing_expiry_and_rejects_identity_conflicts() {
        let mut input = complete_input();
        let mut prior: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        prior.episodes[0].status = TradeEpisodeStatus::ConditionalEntry;
        prior.episodes[0].entry_event_id = None;
        input.rolling_context = Some(prior);

        let mut action = place_entry("BUY");
        action["episode_id"] = json!("episode-nifty-25000-ce-1");
        action["contract"].as_object_mut().unwrap().remove("expiry");
        let accepted = parse_and_validate_output(&output_with_action(action), &input).unwrap();
        assert_eq!(
            accepted.actions[0]
                .contract
                .as_ref()
                .and_then(|contract| contract.expiry.as_deref()),
            Some("2026-08-13")
        );

        let mut conflict = place_entry("BUY");
        conflict["episode_id"] = json!("episode-nifty-25000-ce-1");
        conflict["contract"]["expiry"] = json!("2026-08-20");
        let conflicting = parse_and_validate_output(&output_with_action(conflict), &input).unwrap();
        assert!(conflicting.actions.is_empty());
        assert!(
            conflicting.rejected_actions[0]
                .reason
                .contains("rolling-context episode")
        );
    }

    #[test]
    fn place_entry_without_a_context_episode_is_rejected() {
        let action = place_entry("BUY");
        let output = json!({
            "market_bias": {
                "direction": "BULLISH",
                "rationale": "positive price action"
            },
            "freshness": {
                "status": "UNKNOWN",
                "input_data_age_ms": 1,
                "usable_for_new_entries": false,
                "rationale": "model estimate"
            },
            "rolling_context": {
                "spoken_summary": "The streamer gives an entry call.",
                "visual_summary": "The option chart is visible.",
                "combined_summary": "No structured episode was returned.",
                "key_visual_points": [],
                "episodes": []
            },
            "actions": [action]
        })
        .to_string();

        let parsed = parse_and_validate_output(&output, &complete_input()).unwrap();
        assert!(parsed.actions.is_empty());
        assert!(
            parsed.rejected_actions[0]
                .reason
                .contains("rolling-context episode")
        );
    }

    #[test]
    fn missing_expiry_cannot_ambiguously_select_an_open_trade() {
        let mut input = complete_input();
        let mut second = input.open_trades[0].clone();
        second.trade_id = "trade-2".to_owned();
        second.contract.expiry = Some("2026-08-20".to_owned());
        input.open_trades.push(second);

        let mut ambiguous_contract = contract(TradeDirection::Buy);
        ambiguous_contract.expiry = None;
        let mut action = TradeAction {
            action: ActionKind::Exit,
            episode_id: None,
            event_id: None,
            trade_id: None,
            contract: Some(ambiguous_contract),
            levels: None,
            evidence_timestamps: vec![EvidenceTimestamp {
                seconds_from_clip_start: 7.0,
                source: EvidenceSource::Both,
                source_segment_sequence: Some(1),
                detail: None,
            }],
            rationale: "exit now".to_owned(),
        };
        assert!(input.open_trade_for(&action).is_none());

        action.trade_id = Some("trade-1".to_owned());
        action.contract.as_mut().unwrap().expiry = Some("2026-08-20".to_owned());
        assert!(input.open_trade_for(&action).is_none());
    }

    #[test]
    fn does_not_repeat_an_unresolved_episode_entry_event() {
        let mut input = complete_input();
        let mut prior: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        prior.episodes[0].status = TradeEpisodeStatus::Open;
        prior.episodes[0].entry_event_id = Some("entry-event-already-consumed".to_owned());
        input.rolling_context = Some(prior);

        let repeated =
            parse_and_validate_output(&output_with_action(place_entry("BUY")), &input).unwrap();
        assert!(repeated.actions.is_empty());
        assert!(
            repeated.rejected_actions[0]
                .reason
                .contains("already has an entry event")
        );
    }

    #[test]
    fn rolling_context_is_bounded_and_preserves_omitted_unresolved_episode() {
        let mut input = complete_input();
        let prior: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        input.rolling_context = Some(prior.clone());

        let points = (0..(MAX_KEY_VISUAL_POINTS + 10))
            .map(|index| {
                json!({
                    "category": "CAPTION",
                    "label": format!("caption-{index}"),
                    "value": "x".repeat(MAX_CONTEXT_VALUE_CHARS + 25),
                    "observed_in_current_clip": true
                })
            })
            .collect::<Vec<_>>();
        let output = json!({
            "market_bias": {
                "direction": "UNKNOWN",
                "rationale": "No directional conclusion."
            },
            "freshness": {
                "status": "UNKNOWN",
                "input_data_age_ms": 0,
                "usable_for_new_entries": false,
                "rationale": "Model estimate."
            },
            "rolling_context": {
                "spoken_summary": format!("spoken {}", "s".repeat(MAX_ROLLING_SUMMARY_CHARS + 50)),
                "visual_summary": format!("visual {}", "v".repeat(MAX_ROLLING_SUMMARY_CHARS + 50)),
                "combined_summary": format!("combined {}", "c".repeat(MAX_COMBINED_SUMMARY_CHARS + 50)),
                "key_visual_points": points,
                "episodes": []
            },
            "actions": [ignore_action()]
        })
        .to_string();

        let parsed = parse_and_validate_output(&output, &input).unwrap();
        assert_eq!(
            parsed.rolling_context.spoken_summary.chars().count(),
            MAX_ROLLING_SUMMARY_CHARS
        );
        assert_eq!(
            parsed.rolling_context.visual_summary.chars().count(),
            MAX_ROLLING_SUMMARY_CHARS
        );
        assert_eq!(
            parsed.rolling_context.combined_summary.chars().count(),
            MAX_COMBINED_SUMMARY_CHARS
        );
        assert_eq!(
            parsed.rolling_context.key_visual_points.len(),
            MAX_KEY_VISUAL_POINTS
        );
        assert_eq!(parsed.rolling_context.episodes.len(), 1);
        assert_eq!(
            parsed.rolling_context.episodes[0].episode_id,
            prior.episodes[0].episode_id
        );
    }

    #[test]
    fn rolling_context_stays_bounded_for_a_seven_hour_stream() {
        const WINDOWS_IN_SEVEN_HOURS: usize = 7 * 60 * 60 / 20;
        let mut input = complete_input();
        let mut context: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        context.episodes[0].status = TradeEpisodeStatus::ConditionalEntry;
        context.episodes[0].entry_event_id = None;

        for sequence in 0..WINDOWS_IN_SEVEN_HOURS {
            let mut proposed = context.clone();
            proposed
                .spoken_summary
                .push_str(&format!(" spoken-window-{sequence}"));
            proposed
                .visual_summary
                .push_str(&format!(" visual-window-{sequence}"));
            proposed
                .combined_summary
                .push_str(&format!(" combined-window-{sequence}"));
            proposed.key_visual_points.push(KeyVisualDataPoint {
                category: KeyVisualCategory::Caption,
                label: format!("window-{sequence}"),
                value: "bounded visual observation".to_owned(),
                contract: None,
                numeric_value: None,
                unit: None,
                observed_at: Some(input.clip.ended_at.to_rfc3339()),
                observed_in_current_clip: true,
            });

            let output = json!({
                "market_bias": {
                    "direction": "UNKNOWN",
                    "rationale": "No new directional conclusion."
                },
                "freshness": {
                    "status": "UNKNOWN",
                    "input_data_age_ms": 0,
                    "usable_for_new_entries": false,
                    "rationale": "Model estimate."
                },
                "rolling_context": proposed,
                "actions": [ignore_action()]
            })
            .to_string();
            input.rolling_context = Some(context);
            context = parse_and_validate_output(&output, &input)
                .unwrap()
                .rolling_context;

            assert!(context.spoken_summary.chars().count() <= MAX_ROLLING_SUMMARY_CHARS);
            assert!(context.visual_summary.chars().count() <= MAX_ROLLING_SUMMARY_CHARS);
            assert!(context.combined_summary.chars().count() <= MAX_COMBINED_SUMMARY_CHARS);
            assert!(context.key_visual_points.len() <= MAX_KEY_VISUAL_POINTS);
            assert!(context.episodes.len() <= MAX_ACTIVE_EPISODES);
            assert!(serde_json::to_vec(&context).unwrap().len() <= 64 * 1024);
        }

        assert_eq!(context.episodes.len(), 1);
        assert_eq!(
            context.episodes[0].status,
            TradeEpisodeStatus::ConditionalEntry
        );
        assert!(context.spoken_summary.contains("spoken-window-1259"));
        assert!(context.visual_summary.contains("visual-window-1259"));
        assert!(context.combined_summary.contains("combined-window-1259"));
        assert!(
            context
                .key_visual_points
                .iter()
                .any(|point| point.label == "window-1259")
        );
    }

    #[test]
    fn current_terminal_episode_overrides_prior_active_state() {
        let mut input = complete_input();
        let mut prior: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        prior.episodes[0].status = TradeEpisodeStatus::Managing;
        input.rolling_context = Some(prior);

        let mut updated_context = rolling_context_json();
        updated_context["episodes"][0]["status"] = json!("CLOSED");
        updated_context["episodes"][0]["latest_instruction"] =
            json!("Streamer explicitly booked the remaining quantity.");
        let output = json!({
            "market_bias": {
                "direction": "UNKNOWN",
                "rationale": "No directional conclusion."
            },
            "freshness": {
                "status": "UNKNOWN",
                "input_data_age_ms": 0,
                "usable_for_new_entries": false,
                "rationale": "Model estimate."
            },
            "rolling_context": updated_context,
            "actions": [ignore_action()]
        })
        .to_string();

        let parsed = parse_and_validate_output(&output, &input).unwrap();
        assert_eq!(parsed.rolling_context.episodes.len(), 1);
        assert_eq!(
            parsed.rolling_context.episodes[0].status,
            TradeEpisodeStatus::Closed
        );
    }

    #[test]
    fn request_uses_responses_fields_and_exact_jpeg_mime() {
        let body =
            build_request_body(DEFAULT_LUNA_MODEL, &complete_input(), Some(b"jpeg")).unwrap();
        assert_eq!(body["model"], DEFAULT_LUNA_MODEL);
        assert_eq!(body["store"], false);
        assert_eq!(body["service_tier"], "fast");
        assert_eq!(body["prompt_cache_key"], "observer-paper-luna-v1");
        assert_eq!(body["reasoning"]["effort"], "low");
        assert!(body.get("max_output_tokens").is_none());
        assert_eq!(body["input"][0]["role"], "user");
        assert_eq!(body["input"][0]["content"][0]["type"], "input_text");
        assert_eq!(body["input"][0]["content"][1]["type"], "input_image");
        assert_eq!(
            body["input"][0]["content"][1]["image_url"],
            "data:image/jpeg;base64,anBlZw=="
        );
        assert_eq!(body["text"]["format"]["type"], "json_schema");
        assert_eq!(body["text"]["format"]["name"], "trade_observation");
        assert_eq!(body["text"]["format"]["strict"], true);
        assert_eq!(body["text"]["format"]["schema"]["type"], "object");
        assert!(
            body["text"]["format"]["schema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!("rolling_context"))
        );
        assert!(
            body["text"]["format"]["schema"]["properties"]["actions"]
                .get("maxItems")
                .is_none()
        );
        fn assert_no_max_items(value: &Value) {
            match value {
                Value::Object(object) => {
                    assert!(!object.contains_key("maxItems"));
                    for child in object.values() {
                        assert_no_max_items(child);
                    }
                }
                Value::Array(array) => {
                    for child in array {
                        assert_no_max_items(child);
                    }
                }
                _ => {}
            }
        }
        assert_no_max_items(&body["text"]["format"]["schema"]);
        let contract = &body["text"]["format"]["schema"]["properties"]["actions"]["items"]["properties"]
            ["contract"];
        let levels = &body["text"]["format"]["schema"]["properties"]["actions"]["items"]["properties"]
            ["levels"];
        assert_eq!(contract["properties"]["strike"]["minimum"], 0.01);
        assert!(
            contract["properties"]["strike"]
                .get("exclusiveMinimum")
                .is_none()
        );
        for level in ["entry", "hard_sl", "t1", "t2"] {
            assert_eq!(levels["properties"][level]["minimum"], 0.01);
            assert!(
                levels["properties"][level]
                    .get("exclusiveMinimum")
                    .is_none()
            );
        }
        let evidence = &body["text"]["format"]["schema"]["properties"]["actions"]["items"]["properties"]
            ["evidence_timestamps"]["items"];
        assert!(
            evidence["properties"]["seconds_from_clip_start"]
                .get("maximum")
                .is_none()
        );
        assert_eq!(
            evidence["properties"]["source_segment_sequence"]["minimum"],
            0
        );
    }

    #[test]
    fn strict_response_schema_requires_every_declared_object_property() {
        fn assert_required_properties(schema: &Value) {
            if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
                let required = schema
                    .get("required")
                    .and_then(Value::as_array)
                    .expect("strict object schema must declare required properties");
                for property in properties.keys() {
                    assert!(
                        required.iter().any(|entry| entry == property),
                        "strict object schema omitted required property {property}"
                    );
                }
            }
            match schema {
                Value::Object(object) => {
                    for child in object.values() {
                        assert_required_properties(child);
                    }
                }
                Value::Array(array) => {
                    for child in array {
                        assert_required_properties(child);
                    }
                }
                _ => {}
            }
        }

        assert_required_properties(&response_json_schema());
    }

    #[test]
    fn strict_response_schema_uses_null_for_omittable_model_fields() {
        let schema = response_json_schema();
        let action = &schema["properties"]["actions"]["items"];
        let contract = &action["properties"]["contract"];
        let levels = &action["properties"]["levels"];
        let evidence = &action["properties"]["evidence_timestamps"]["items"];

        assert!(
            contract["type"]
                .as_array()
                .unwrap()
                .contains(&json!("null"))
        );
        assert!(levels["type"].as_array().unwrap().contains(&json!("null")));
        assert!(
            evidence["properties"]["source_segment_sequence"]["type"]
                .as_array()
                .unwrap()
                .contains(&json!("null"))
        );
        assert!(
            evidence["properties"]["detail"]["type"]
                .as_array()
                .unwrap()
                .contains(&json!("null"))
        );
    }

    #[test]
    fn request_rebounds_carried_context_and_marks_visuals_as_prior() {
        let mut input = complete_input();
        let mut context: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        context.spoken_summary = "s".repeat(MAX_ROLLING_SUMMARY_CHARS + 100);
        context.visual_summary = "v".repeat(MAX_ROLLING_SUMMARY_CHARS + 100);
        context.combined_summary = "c".repeat(MAX_COMBINED_SUMMARY_CHARS + 100);
        let seed_point = context.key_visual_points[0].clone();
        context.key_visual_points = (0..(MAX_KEY_VISUAL_POINTS + 5))
            .map(|index| {
                let mut point = seed_point.clone();
                point.label = format!("point-{index}");
                point.observed_in_current_clip = true;
                point
            })
            .collect();
        input.rolling_context = Some(context);

        let body = build_request_body(DEFAULT_LUNA_MODEL, &input, None).unwrap();
        let prompt: Value =
            serde_json::from_str(body["input"][0]["content"][0]["text"].as_str().unwrap()).unwrap();
        let carried = &prompt["context"]["rolling_context"];
        assert_eq!(
            carried["spoken_summary"].as_str().unwrap().chars().count(),
            MAX_ROLLING_SUMMARY_CHARS
        );
        assert_eq!(
            carried["visual_summary"].as_str().unwrap().chars().count(),
            MAX_ROLLING_SUMMARY_CHARS
        );
        assert_eq!(
            carried["combined_summary"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            MAX_COMBINED_SUMMARY_CHARS
        );
        assert_eq!(
            carried["key_visual_points"].as_array().unwrap().len(),
            MAX_KEY_VISUAL_POINTS
        );
        assert!(
            carried["key_visual_points"]
                .as_array()
                .unwrap()
                .iter()
                .all(|point| point["observed_in_current_clip"] == false)
        );
    }

    #[test]
    fn provider_error_message_is_structured_redacted_and_bounded() {
        let raw_key = format!("sk-{}", "x".repeat(40));
        let raw = format!(
            "Request contains\n\0 an invalid argument: {raw_key} {}",
            "z".repeat(MAX_PROVIDER_ERROR_MESSAGE_CHARS + 100)
        );
        let body = json!({
            "error": { "code": 400, "message": raw, "status": "INVALID_ARGUMENT" },
            "request_echo": "must never be included"
        });
        let message = parse_openai_error_message(body.to_string().as_bytes()).unwrap();

        assert!(!message.contains('\n'));
        assert!(!message.contains('\0'));
        assert!(!message.contains(&raw_key));
        assert!(!message.contains("request_echo"));
        assert!(!message.contains("must never be included"));
        assert!(message.contains("[REDACTED_API_KEY]"));
        assert!(message.ends_with("..."));
        assert!(
            message.chars().count() <= MAX_PROVIDER_ERROR_MESSAGE_CHARS + 3,
            "sanitized message exceeded its bound"
        );
        assert!(sanitize_provider_error_message(" \n\t ").is_none());
        assert!(parse_openai_error_message(br#"{"message":"wrong envelope"}"#).is_none());
        assert!(parse_openai_error_message(b"not json").is_none());
    }

    #[tokio::test]
    async fn client_retries_chunked_quota_response_on_next_key_and_records_safe_usage() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            sync::oneshot,
        };

        async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
            let mut bytes = Vec::new();
            let mut buffer = [0_u8; 1024];
            let header_end;
            loop {
                let read = stream.read(&mut buffer).await.unwrap();
                assert!(read > 0, "client closed before request headers");
                bytes.extend_from_slice(&buffer[..read]);
                if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    header_end = end + 4;
                    break;
                }
            }

            let headers = String::from_utf8_lossy(&bytes[..header_end]).into_owned();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.split_once(':').and_then(|(name, value)| {
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                })
                .unwrap_or(0);
            while bytes.len() - header_end < content_length {
                let read = stream.read(&mut buffer).await.unwrap();
                assert!(read > 0, "client closed before request body");
                bytes.extend_from_slice(&buffer[..read]);
            }
            headers
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (requests_tx, requests_rx) = oneshot::channel();
        let success_output = output_with_action(place_entry("BUY"));
        let success_body = json!({
            "id": "local-response",
            "status": "completed",
            "output_text": success_output,
            "usage": { "input_tokens": 11, "output_tokens": 7, "total_tokens": 18 }
        })
        .to_string();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let first_headers = read_request(&mut first).await;
            let error_body = r#"{"error":{"message":"quota temporarily exhausted"}}"#;
            let response = format!(
                "HTTP/1.1 429 Too Many Requests\r\nTransfer-Encoding: chunked\r\nx-ratelimit-limit-requests: 30\r\nx-ratelimit-remaining-requests: 0\r\nx-ratelimit-limit-tokens: 1000\r\nx-ratelimit-remaining-tokens: 0\r\nretry-after: 1\r\nConnection: close\r\n\r\n{:X}\r\n{}\r\n0\r\n\r\n",
                error_body.len(),
                error_body,
            );
            first.write_all(response.as_bytes()).await.unwrap();

            let (mut second, _) = listener.accept().await.unwrap();
            let second_headers = read_request(&mut second).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nx-ratelimit-limit-requests: 30\r\nx-ratelimit-remaining-requests: 29\r\nx-ratelimit-reset-requests: 1s\r\nx-ratelimit-limit-tokens: 1000\r\nx-ratelimit-remaining-tokens: 982\r\nx-ratelimit-reset-tokens: 1s\r\nConnection: close\r\n\r\n{}",
                success_body.len(),
                success_body,
            );
            second.write_all(response.as_bytes()).await.unwrap();
            let _ = requests_tx.send((first_headers, second_headers));
        });

        let client = AnalysisClient::from_keys_config(
            ["unit-first-key", "unit-second-key"],
            AnalysisClientConfig {
                model: DEFAULT_LUNA_MODEL.to_owned(),
                endpoint: format!("http://{address}/v1/responses"),
                request_timeout: Duration::from_secs(5),
            },
        )
        .unwrap();
        let result = client.analyze(&complete_input(), None).await.unwrap();
        assert_eq!(result.actions.len(), 1);
        let (first_headers, second_headers) = requests_rx.await.unwrap();
        assert!(
            first_headers
                .to_ascii_lowercase()
                .contains("authorization: bearer unit-first-key")
        );
        assert!(
            second_headers
                .to_ascii_lowercase()
                .contains("authorization: bearer unit-second-key")
        );
        server.await.unwrap();

        let health = client.key_health().await;
        assert_eq!(health[0].failures, 1);
        assert_eq!(health[0].last_failure.as_deref(), Some("QUOTA"));
        assert_eq!(health[0].daily_usage.request_count, 1);
        assert_eq!(health[0].daily_usage.total_tokens, 0);
        assert_eq!(
            health[0]
                .rate_limit
                .as_ref()
                .and_then(|rate| rate.request_remaining),
            Some(0)
        );
        assert_eq!(health[1].successes, 1);
        assert_eq!(
            health[1]
                .rate_limit
                .as_ref()
                .and_then(|rate| rate.token_remaining),
            Some(982)
        );
        assert_eq!(health[1].daily_usage.request_count, 1);
        assert_eq!(health[1].daily_usage.total_tokens, 18);
    }

    #[test]
    fn parses_fixture_responses_response() {
        let model_output = output_with_action(place_entry("BUY"));
        let fixture = json!({
            "id": "response-123",
            "status": "completed",
            "output": [
                {
                    "type": "message",
                    "content": [{ "type": "output_text", "text": model_output }]
                }
            ]
        });
        let response: ResponsesResponse = serde_json::from_value(fixture).unwrap();
        let result = parse_responses_response(response, &complete_input()).unwrap();
        assert_eq!(result.interaction_id.as_deref(), Some("response-123"));
        assert_eq!(result.actions.len(), 1);
    }

    #[test]
    fn response_schema_excludes_legacy_confidence_fields() {
        let schema = response_json_schema();
        assert!(!schema.to_string().contains("confidence_pct"));
    }

    #[test]
    fn model_output_with_a_removed_confidence_field_is_rejected() {
        let mut output: Value =
            serde_json::from_str(&output_with_action(place_entry("BUY"))).unwrap();
        output["actions"][0]["confidence_pct"] = json!(65);
        let error = parse_and_validate_output(&output.to_string(), &complete_input()).unwrap_err();
        assert!(error.to_string().contains("removed legacy confidence"));
    }

    #[test]
    fn model_cannot_persist_an_unproven_entry_called_event() {
        let input = complete_input();
        let mut output: Value = serde_json::from_str(&output_with_action(ignore_action())).unwrap();
        output["rolling_context"]["episodes"][0]["status"] = json!("ENTRY_CALLED");
        output["rolling_context"]["episodes"][0]["entry_event_id"] = json!("forged-event");

        let parsed = parse_and_validate_output(&output.to_string(), &input).unwrap();
        let episode = &parsed.rolling_context.episodes[0];
        assert_eq!(episode.status, TradeEpisodeStatus::ConditionalEntry);
        assert_eq!(episode.entry_event_id, None);
    }

    #[test]
    fn prior_actual_placement_preserves_its_entry_event_exactly_once() {
        let mut input = complete_input();
        let mut prior: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        prior.episodes[0].entry_event_id = Some("actual-event".to_owned());
        prior.authoritative_outcomes.push(AuthoritativeOutcome {
            action: ActionKind::PlaceEntry,
            episode_id: Some(prior.episodes[0].episode_id.clone()),
            event_id: Some("actual-event".to_owned()),
            setup_id: Some("actual-setup".to_owned()),
            status: "APPLIED".to_owned(),
            detail: "one shadow order was placed".to_owned(),
            occurred_at: input.clip.sent_at.to_rfc3339(),
        });
        input.rolling_context = Some(prior);
        let mut output: Value = serde_json::from_str(&output_with_action(ignore_action())).unwrap();
        output["rolling_context"]["episodes"][0]["entry_event_id"] = json!("actual-event");

        let parsed = parse_and_validate_output(&output.to_string(), &input).unwrap();
        let episode = &parsed.rolling_context.episodes[0];
        assert_eq!(episode.status, TradeEpisodeStatus::EntryCalled);
        assert_eq!(episode.entry_event_id.as_deref(), Some("actual-event"));
        assert_eq!(
            parsed
                .rolling_context
                .authoritative_outcomes
                .iter()
                .filter(|outcome| outcome.event_id.as_deref() == Some("actual-event"))
                .count(),
            1
        );
    }

    #[test]
    fn request_context_preserves_only_runtime_proven_entry_events() {
        let mut proven_input = complete_input();
        let mut proven: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        proven.episodes[0].entry_event_id = Some("proved-entry".to_owned());
        proven.authoritative_outcomes.push(AuthoritativeOutcome {
            action: ActionKind::PlaceEntry,
            episode_id: Some(proven.episodes[0].episode_id.clone()),
            event_id: Some("proved-entry".to_owned()),
            setup_id: Some("proved-setup".to_owned()),
            status: "APPLIED".to_owned(),
            detail: "paper order placed".to_owned(),
            occurred_at: proven_input.clip.sent_at.to_rfc3339(),
        });
        proven_input.rolling_context = Some(proven);
        let proven_prompt = build_request_body(DEFAULT_LUNA_MODEL, &proven_input, None).unwrap();
        let proven_text: Value = serde_json::from_str(
            proven_prompt["input"][0]["content"][0]["text"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            proven_text["context"]["rolling_context"]["episodes"][0]["entry_event_id"],
            json!("proved-entry")
        );

        let mut forged_input = complete_input();
        let mut forged: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        forged.episodes[0].entry_event_id = Some("forged-entry".to_owned());
        forged_input.rolling_context = Some(forged);
        let forged_prompt = build_request_body(DEFAULT_LUNA_MODEL, &forged_input, None).unwrap();
        let forged_text: Value = serde_json::from_str(
            forged_prompt["input"][0]["content"][0]["text"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        let episode = &forged_text["context"]["rolling_context"]["episodes"][0];
        assert!(episode.get("entry_event_id").is_none());
        assert_eq!(episode["status"], json!("CONDITIONAL_ENTRY"));
    }

    #[test]
    fn prior_authoritative_outcomes_reach_next_input_and_model_cannot_replace_them() {
        let mut input = complete_input();
        let mut prior: RollingContext = serde_json::from_value(rolling_context_json()).unwrap();
        prior.authoritative_outcomes.push(AuthoritativeOutcome {
            action: ActionKind::PlaceEntry,
            episode_id: Some("episode-nifty-25000-ce-1".to_owned()),
            event_id: Some("runtime-entry-1".to_owned()),
            setup_id: Some("setup-runtime-1".to_owned()),
            status: "APPLIED".to_owned(),
            detail: "two paper entry orders were actually placed".to_owned(),
            occurred_at: input.clip.sent_at.to_rfc3339(),
        });
        input.rolling_context = Some(prior);

        let parsed = parse_and_validate_output(
            &output_with_action(json!({
                "action": "WATCH",
                "contract": null,
                "levels": null,
                "evidence_timestamps": [],
                "rationale": "Wait for a fresh setup."
            })),
            &input,
        )
        .unwrap();
        assert_eq!(parsed.rolling_context.authoritative_outcomes.len(), 1);
        assert_eq!(
            parsed.rolling_context.authoritative_outcomes[0].status,
            "APPLIED"
        );
        let request = build_request_body(DEFAULT_LUNA_MODEL, &input, None).unwrap();
        assert!(request.to_string().contains("authoritative_outcomes"));
        assert!(
            !response_json_schema()
                .to_string()
                .contains("authoritative_outcomes")
        );
    }
}
