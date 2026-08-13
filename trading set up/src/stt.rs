//! Credential-safe, bounded ElevenLabs Scribe v2 transcription.
//!
//! The module deliberately keeps provider credentials private and in memory.
//! It never emits log records, provider response bodies, or key material.

use std::{
    collections::{BTreeMap, HashSet},
    fmt, fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use futures_util::future::join_all;
use reqwest::{
    Client, StatusCode,
    header::{CONTENT_LENGTH, CONTENT_TYPE, RETRY_AFTER},
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, Semaphore},
    time::{Instant, timeout},
};

pub const SEGMENT_SECONDS: f64 = 5.0;
pub const WINDOW_CHUNKS: usize = 4;
pub const DEFAULT_STT_CONCURRENCY: usize = 4;
const MAX_STT_ATTEMPTS_PER_SEGMENT: usize = 2;

const ELEVENLABS_STT_ENDPOINT: &str = "https://api.elevenlabs.io/v1/speech-to-text";
const MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RESPONSE_BYTES: u64 = 2 * 1024 * 1024;
const TIMELINE_EPSILON_SECONDS: f64 = 0.001;

static MULTIPART_COUNTER: AtomicU64 = AtomicU64::new(0);

/// One exact five-second input cut from the shared stream clock.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentInput {
    pub index: u64,
    pub start_sec: f64,
    pub end_sec: f64,
    pub path: PathBuf,
}

impl SegmentInput {
    pub fn new(index: u64, start_sec: f64, end_sec: f64, path: impl Into<PathBuf>) -> Self {
        Self {
            index,
            start_sec,
            end_sec,
            path: path.into(),
        }
    }

    fn is_exact_segment(&self) -> bool {
        self.start_sec.is_finite()
            && self.end_sec.is_finite()
            && self.start_sec >= 0.0
            && self.end_sec > self.start_sec
            && ((self.end_sec - self.start_sec) - SEGMENT_SECONDS).abs() <= TIMELINE_EPSILON_SECONDS
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptStatus {
    Complete,
    Incomplete,
}

/// Stable, credential-free failure classes suitable for persistence and UI.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptFailure {
    InvalidSegment,
    InvalidWindow,
    MediaUnavailable,
    MediaTooLarge,
    TimedOut,
    Authentication,
    Quota,
    ProviderTransient,
    ProviderRejected,
    InvalidResponse,
    CredentialsCoolingDown,
    MissingResult,
}

/// A timestamped provider token. Times are absolute on the stream timeline,
/// not relative to the five-second file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WordTimestamp {
    pub text: String,
    pub start_sec: Option<f64>,
    pub end_sec: Option<f64>,
    pub speaker_id: Option<String>,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TranscriptChunk {
    pub index: u64,
    pub start_sec: f64,
    pub end_sec: f64,
    pub status: TranscriptStatus,
    pub failure: Option<TranscriptFailure>,
    pub text: String,
    pub word_timestamps: Vec<WordTimestamp>,
    /// Unique speaker IDs in first-appearance order.
    pub speakers: Vec<String>,
    pub language_code: Option<String>,
}

impl TranscriptChunk {
    fn incomplete(segment: &SegmentInput, failure: TranscriptFailure) -> Self {
        Self {
            index: segment.index,
            start_sec: segment.start_sec,
            end_sec: segment.end_sec,
            status: TranscriptStatus::Incomplete,
            failure: Some(failure),
            text: String::new(),
            word_timestamps: Vec::new(),
            speakers: Vec::new(),
            language_code: None,
        }
    }

    #[cfg(test)]
    fn test_complete(segment: &SegmentInput, text: &str) -> Self {
        Self {
            index: segment.index,
            start_sec: segment.start_sec,
            end_sec: segment.end_sec,
            status: TranscriptStatus::Complete,
            failure: None,
            text: text.to_owned(),
            word_timestamps: Vec::new(),
            speakers: Vec::new(),
            language_code: Some("en".to_owned()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TranscriptWindow {
    pub first_chunk_index: u64,
    pub start_sec: f64,
    pub end_sec: f64,
    pub complete: bool,
    pub incomplete_count: usize,
    pub text: String,
    pub speakers: Vec<String>,
    pub chunks: Vec<TranscriptChunk>,
}

#[derive(Debug, Clone)]
pub struct SttOptions {
    pub concurrency: usize,
    /// Maximum number of credential slots loaded from the vault, in file order.
    pub credential_limit: usize,
    /// Hard wall-clock deadline for an entire chunk, including queue time and
    /// all credential fallback attempts.
    pub segment_timeout: Duration,
    /// Timeout for one provider attempt. The segment deadline remains the
    /// final upper bound.
    pub request_timeout: Duration,
    pub auth_cooldown: Duration,
    pub quota_cooldown: Duration,
    pub transient_cooldown: Duration,
    endpoint: String,
}

impl Default for SttOptions {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_STT_CONCURRENCY,
            credential_limit: 3,
            segment_timeout: Duration::from_secs(6),
            request_timeout: Duration::from_secs(4),
            auth_cooldown: Duration::from_secs(15 * 60),
            quota_cooldown: Duration::from_secs(60),
            transient_cooldown: Duration::from_secs(5),
            endpoint: ELEVENLABS_STT_ENDPOINT.to_owned(),
        }
    }
}

impl SttOptions {
    fn validate(&self) -> Result<()> {
        if !(1..=64).contains(&self.concurrency) {
            bail!("STT concurrency must be between 1 and 64");
        }
        if !(1..=16).contains(&self.credential_limit) {
            bail!("STT credential limit must be between 1 and 16");
        }
        if self.segment_timeout.is_zero() || self.request_timeout.is_zero() {
            bail!("STT timeouts must be greater than zero");
        }
        if self.request_timeout >= self.segment_timeout {
            bail!("STT request timeout must be shorter than the segment deadline");
        }
        if !self.endpoint.starts_with("https://") && !self.endpoint.starts_with("http://") {
            bail!("STT endpoint must be HTTP or HTTPS");
        }
        Ok(())
    }
}

/// The only secret-bearing type in this module. Debug output is always
/// redacted, and the inner value has no public accessor.
#[derive(Clone)]
struct SecretKey(Arc<str>);

impl SecretKey {
    fn expose_for_request(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretKey([REDACTED])")
    }
}

#[derive(Debug)]
struct KeySlot {
    key: SecretKey,
    cooldown_until: Instant,
    successes: u64,
    failures: u64,
    last_failure: Option<&'static str>,
}

#[derive(Debug)]
struct KeyRingState {
    slots: Vec<KeySlot>,
    active_index: usize,
}

#[derive(Debug)]
struct KeyRing {
    state: Mutex<KeyRingState>,
}

impl KeyRing {
    fn new(keys: Vec<SecretKey>) -> Self {
        let now = Instant::now();
        Self {
            state: Mutex::new(KeyRingState {
                slots: keys
                    .into_iter()
                    .map(|key| KeySlot {
                        key,
                        cooldown_until: now,
                        successes: 0,
                        failures: 0,
                        last_failure: None,
                    })
                    .collect(),
                active_index: 0,
            }),
        }
    }

    async fn next_available(&self, attempted: &HashSet<usize>) -> Option<(usize, SecretKey)> {
        let mut state = self.state.lock().await;
        let count = state.slots.len();
        if count == 0 {
            return None;
        }
        let now = Instant::now();
        let active_index = state.active_index % count;
        for offset in 0..count {
            let index = (active_index + offset) % count;
            let slot = &state.slots[index];
            if !attempted.contains(&index) && slot.cooldown_until <= now {
                let key = slot.key.clone();
                state.active_index = index;
                return Some((index, key));
            }
        }
        None
    }

    async fn record_failure(&self, slot_index: usize, class: &'static str, duration: Duration) {
        let mut state = self.state.lock().await;
        if let Some(slot) = state.slots.get_mut(slot_index) {
            slot.failures = slot.failures.saturating_add(1);
            slot.last_failure = Some(class);
            let proposed = Instant::now() + duration;
            if proposed > slot.cooldown_until {
                slot.cooldown_until = proposed;
            }
        }
    }

    async fn record_success(&self, slot_index: usize) {
        let mut state = self.state.lock().await;
        if let Some(slot) = state.slots.get_mut(slot_index) {
            slot.successes = slot.successes.saturating_add(1);
            slot.last_failure = None;
        }
    }

    async fn has_unattempted_key(&self, attempted: &HashSet<usize>) -> bool {
        let state = self.state.lock().await;
        (0..state.slots.len()).any(|index| !attempted.contains(&index))
    }

    async fn len(&self) -> usize {
        self.state.lock().await.slots.len()
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SttKeyHealth {
    pub slot: usize,
    pub state: String,
    pub successes: u64,
    pub failures: u64,
    pub cooldown_remaining_ms: u64,
    pub last_failure: Option<String>,
}

#[derive(Clone)]
pub struct ElevenLabsSttClient {
    inner: Arc<SttInner>,
}

struct SttInner {
    http: Client,
    keys: KeyRing,
    semaphore: Arc<Semaphore>,
    options: SttOptions,
}

impl fmt::Debug for ElevenLabsSttClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ElevenLabsSttClient")
            .field("concurrency", &self.inner.options.concurrency)
            .field("segment_timeout", &self.inner.options.segment_timeout)
            .field("request_timeout", &self.inner.options.request_timeout)
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}

impl ElevenLabsSttClient {
    pub fn from_vault(vault_path: impl AsRef<Path>) -> Result<Self> {
        Self::from_vault_with_options(vault_path, SttOptions::default())
    }

    pub fn from_vault_with_options(
        vault_path: impl AsRef<Path>,
        options: SttOptions,
    ) -> Result<Self> {
        options.validate()?;
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(options.concurrency)
            .build()
            .context("could not construct ElevenLabs HTTP client")?;
        Self::from_vault_with_client(http, vault_path, options)
    }

    pub fn from_keys_with_options<I, S>(api_keys: I, options: SttOptions) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        options.validate()?;
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(options.concurrency)
            .build()
            .context("could not construct ElevenLabs HTTP client")?;
        let mut seen = HashSet::new();
        let keys = api_keys
            .into_iter()
            .map(|key| key.as_ref().trim().to_owned())
            .filter(|key| !key.is_empty() && seen.insert(key.clone()))
            .take(options.credential_limit)
            .map(|key| SecretKey(Arc::from(key)))
            .collect::<Vec<_>>();
        Self::from_secret_keys_with_client(http, keys, options)
    }

    /// Accept a shared HTTP client while retaining the same secret-loading and
    /// validation guarantees.
    pub fn from_vault_with_client(
        http: Client,
        vault_path: impl AsRef<Path>,
        options: SttOptions,
    ) -> Result<Self> {
        options.validate()?;
        let vault_bytes = fs::read(vault_path.as_ref()).with_context(|| {
            format!(
                "ElevenLabs credential vault is unavailable at {}",
                vault_path.as_ref().display()
            )
        })?;
        let vault_text = String::from_utf8_lossy(&vault_bytes);
        let keys = parse_vault_keys(&vault_text)
            .into_iter()
            .take(options.credential_limit)
            .collect::<Vec<_>>();
        Self::from_secret_keys_with_client(http, keys, options)
    }

    fn from_secret_keys_with_client(
        http: Client,
        keys: Vec<SecretKey>,
        options: SttOptions,
    ) -> Result<Self> {
        if keys.is_empty() {
            bail!("no usable ElevenLabs credentials were found in the configured vault");
        }

        Ok(Self {
            inner: Arc::new(SttInner {
                http,
                keys: KeyRing::new(keys),
                semaphore: Arc::new(Semaphore::new(options.concurrency)),
                options,
            }),
        })
    }

    /// Safe credential inventory for startup health reporting.
    pub async fn credential_count(&self) -> usize {
        self.inner.keys.len().await
    }

    pub async fn key_health(&self) -> Vec<SttKeyHealth> {
        let state = self.inner.keys.state.lock().await;
        let now = Instant::now();
        state
            .slots
            .iter()
            .enumerate()
            .map(|(index, slot)| {
                let remaining = slot.cooldown_until.saturating_duration_since(now);
                SttKeyHealth {
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
                }
            })
            .collect()
    }

    /// Transcribe one declared five-second segment. Operational failures are
    /// returned as an incomplete chunk so downstream window construction never
    /// waits indefinitely or loses the segment's timeline position.
    pub async fn transcribe_segment(&self, segment: SegmentInput) -> TranscriptChunk {
        if !segment.is_exact_segment() {
            return TranscriptChunk::incomplete(&segment, TranscriptFailure::InvalidSegment);
        }

        match timeout(
            self.inner.options.segment_timeout,
            self.transcribe_segment_inner(&segment),
        )
        .await
        {
            Ok(chunk) => chunk,
            Err(_) => TranscriptChunk::incomplete(&segment, TranscriptFailure::TimedOut),
        }
    }

    /// Transcribe a 20-second window as four exact, consecutive five-second
    /// chunks. All four requests run concurrently (subject to the global
    /// semaphore), and the returned chunks are always in chronological order.
    pub async fn transcribe_window(
        &self,
        mut segments: [SegmentInput; WINDOW_CHUNKS],
    ) -> TranscriptWindow {
        segments.sort_by(|left, right| {
            left.index
                .cmp(&right.index)
                .then_with(|| left.start_sec.total_cmp(&right.start_sec))
        });

        if !valid_window_layout(&segments) {
            let chunks = segments
                .iter()
                .map(|segment| {
                    TranscriptChunk::incomplete(segment, TranscriptFailure::InvalidWindow)
                })
                .collect();
            return assemble_window(&segments, chunks);
        }

        let tasks = segments
            .iter()
            .cloned()
            .map(|segment| self.transcribe_segment(segment));
        let chunks = join_all(tasks).await;
        assemble_window(&segments, chunks)
    }

    async fn transcribe_segment_inner(&self, segment: &SegmentInput) -> TranscriptChunk {
        let _permit = match self.inner.semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                return TranscriptChunk::incomplete(segment, TranscriptFailure::ProviderTransient);
            }
        };

        let media = match read_segment(&segment.path).await {
            Ok(media) => media,
            Err(failure) => return TranscriptChunk::incomplete(segment, failure),
        };
        let boundary = multipart_boundary();
        let content_type = format!("multipart/form-data; boundary={boundary}");
        let multipart = build_multipart_body(&boundary, &segment.path, &media);

        let mut attempted = HashSet::new();
        let mut last_failure = TranscriptFailure::CredentialsCoolingDown;

        loop {
            if attempted.len() >= MAX_STT_ATTEMPTS_PER_SEGMENT {
                return TranscriptChunk::incomplete(segment, last_failure);
            }
            let Some((slot_index, key)) = self.inner.keys.next_available(&attempted).await else {
                if self.inner.keys.has_unattempted_key(&attempted).await {
                    last_failure = TranscriptFailure::CredentialsCoolingDown;
                }
                return TranscriptChunk::incomplete(segment, last_failure);
            };
            attempted.insert(slot_index);

            let response = self
                .inner
                .http
                .post(&self.inner.options.endpoint)
                .header(CONTENT_TYPE, &content_type)
                .header("xi-api-key", key.expose_for_request())
                .timeout(self.inner.options.request_timeout)
                .body(multipart.clone())
                .send()
                .await;

            let response = match response {
                Ok(response) => response,
                Err(_) => {
                    last_failure = TranscriptFailure::ProviderTransient;
                    self.inner
                        .keys
                        .record_failure(
                            slot_index,
                            "TRANSIENT",
                            self.inner.options.transient_cooldown,
                        )
                        .await;
                    continue;
                }
            };

            let status = response.status();
            if status.is_success() {
                if response
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .is_some_and(|length| length > MAX_RESPONSE_BYTES)
                {
                    last_failure = TranscriptFailure::InvalidResponse;
                    self.inner
                        .keys
                        .record_failure(
                            slot_index,
                            "INVALID_RESPONSE",
                            self.inner.options.transient_cooldown,
                        )
                        .await;
                    continue;
                }

                let payload = match response.bytes().await {
                    Ok(payload) if payload.len() as u64 <= MAX_RESPONSE_BYTES => payload,
                    _ => {
                        last_failure = TranscriptFailure::InvalidResponse;
                        self.inner
                            .keys
                            .record_failure(
                                slot_index,
                                "INVALID_RESPONSE",
                                self.inner.options.transient_cooldown,
                            )
                            .await;
                        continue;
                    }
                };
                let parsed = match serde_json::from_slice::<ApiTranscriptResponse>(&payload) {
                    Ok(parsed) => parsed,
                    Err(_) => {
                        last_failure = TranscriptFailure::InvalidResponse;
                        self.inner
                            .keys
                            .record_failure(
                                slot_index,
                                "INVALID_RESPONSE",
                                self.inner.options.transient_cooldown,
                            )
                            .await;
                        continue;
                    }
                };
                self.inner.keys.record_success(slot_index).await;
                return complete_chunk(segment, parsed);
            }

            if is_auth_failure(status) {
                last_failure = TranscriptFailure::Authentication;
                self.inner
                    .keys
                    .record_failure(slot_index, "AUTH", self.inner.options.auth_cooldown)
                    .await;
                continue;
            }
            if is_quota_failure(status) {
                last_failure = TranscriptFailure::Quota;
                let retry_after = retry_after_duration(response.headers())
                    .unwrap_or(self.inner.options.quota_cooldown)
                    .max(self.inner.options.quota_cooldown);
                self.inner
                    .keys
                    .record_failure(slot_index, "QUOTA", retry_after)
                    .await;
                continue;
            }
            if is_transient_failure(status) {
                last_failure = TranscriptFailure::ProviderTransient;
                let retry_after = retry_after_duration(response.headers())
                    .unwrap_or(self.inner.options.transient_cooldown)
                    .max(self.inner.options.transient_cooldown);
                self.inner
                    .keys
                    .record_failure(slot_index, "TRANSIENT", retry_after)
                    .await;
                continue;
            }

            return TranscriptChunk::incomplete(segment, TranscriptFailure::ProviderRejected);
        }
    }
}

async fn read_segment(path: &Path) -> std::result::Result<Vec<u8>, TranscriptFailure> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|_| TranscriptFailure::MediaUnavailable)?;
    if !metadata.is_file() {
        return Err(TranscriptFailure::MediaUnavailable);
    }
    if metadata.len() > MAX_SEGMENT_BYTES {
        return Err(TranscriptFailure::MediaTooLarge);
    }
    let media = tokio::fs::read(path)
        .await
        .map_err(|_| TranscriptFailure::MediaUnavailable)?;
    if media.is_empty() {
        return Err(TranscriptFailure::MediaUnavailable);
    }
    if media.len() as u64 > MAX_SEGMENT_BYTES {
        return Err(TranscriptFailure::MediaTooLarge);
    }
    Ok(media)
}

fn is_auth_failure(status: StatusCode) -> bool {
    matches!(status.as_u16(), 401 | 403)
}

fn is_quota_failure(status: StatusCode) -> bool {
    matches!(status.as_u16(), 402 | 429)
}

fn is_transient_failure(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 425 | 500 | 502 | 503 | 504) || status.is_server_error()
}

fn retry_after_duration(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|seconds| Duration::from_secs(seconds.min(60 * 60)))
}

#[derive(Debug, Deserialize)]
struct ApiTranscriptResponse {
    #[serde(default)]
    text: String,
    #[serde(default)]
    words: Vec<ApiWord>,
    language_code: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ApiWord {
    #[serde(default)]
    text: String,
    start: Option<f64>,
    end: Option<f64>,
    speaker_id: Option<String>,
    #[serde(rename = "type", default = "default_word_kind")]
    kind: String,
}

fn default_word_kind() -> String {
    "word".to_owned()
}

fn complete_chunk(segment: &SegmentInput, response: ApiTranscriptResponse) -> TranscriptChunk {
    let mut speakers = Vec::new();
    let mut seen_speakers = HashSet::new();
    let word_timestamps = response
        .words
        .into_iter()
        .map(|word| {
            if let Some(speaker_id) = word.speaker_id.as_ref()
                && seen_speakers.insert(speaker_id.clone())
            {
                speakers.push(speaker_id.clone());
            }

            let start_sec = absolute_provider_time(segment, word.start);
            let mut end_sec = absolute_provider_time(segment, word.end);
            if let (Some(start), Some(end)) = (start_sec, end_sec)
                && end < start
            {
                end_sec = Some(start);
            }
            WordTimestamp {
                text: word.text,
                start_sec,
                end_sec,
                speaker_id: word.speaker_id,
                kind: word.kind,
            }
        })
        .collect();

    TranscriptChunk {
        index: segment.index,
        start_sec: segment.start_sec,
        end_sec: segment.end_sec,
        status: TranscriptStatus::Complete,
        failure: None,
        text: response.text.trim().to_owned(),
        word_timestamps,
        speakers,
        language_code: response
            .language_code
            .filter(|language| !language.trim().is_empty()),
    }
}

fn absolute_provider_time(segment: &SegmentInput, value: Option<f64>) -> Option<f64> {
    value
        .filter(|time| time.is_finite())
        .map(|time| segment.start_sec + time.clamp(0.0, segment.end_sec - segment.start_sec))
}

fn valid_window_layout(segments: &[SegmentInput; WINDOW_CHUNKS]) -> bool {
    if !segments.iter().all(SegmentInput::is_exact_segment) {
        return false;
    }

    segments.windows(2).all(|pair| {
        pair[1].index == pair[0].index + 1
            && (pair[1].start_sec - pair[0].end_sec).abs() <= TIMELINE_EPSILON_SECONDS
    })
}

fn assemble_window(
    expected_segments: &[SegmentInput; WINDOW_CHUNKS],
    completed_chunks: Vec<TranscriptChunk>,
) -> TranscriptWindow {
    let mut by_index = BTreeMap::new();
    for chunk in completed_chunks {
        by_index.entry(chunk.index).or_insert(chunk);
    }

    let chunks = expected_segments
        .iter()
        .map(|segment| {
            by_index.remove(&segment.index).unwrap_or_else(|| {
                TranscriptChunk::incomplete(segment, TranscriptFailure::MissingResult)
            })
        })
        .collect::<Vec<_>>();
    let incomplete_count = chunks
        .iter()
        .filter(|chunk| chunk.status == TranscriptStatus::Incomplete)
        .count();
    let text = chunks
        .iter()
        .map(|chunk| chunk.text.trim())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    let mut speakers = Vec::new();
    let mut seen_speakers = HashSet::new();
    for speaker in chunks.iter().flat_map(|chunk| chunk.speakers.iter()) {
        if seen_speakers.insert(speaker.clone()) {
            speakers.push(speaker.clone());
        }
    }

    TranscriptWindow {
        first_chunk_index: expected_segments[0].index,
        start_sec: expected_segments[0].start_sec,
        end_sec: expected_segments[WINDOW_CHUNKS - 1].end_sec,
        complete: incomplete_count == 0,
        incomplete_count,
        text,
        speakers,
        chunks,
    }
}

fn parse_vault_keys(contents: &str) -> Vec<SecretKey> {
    let mut keys = Vec::new();
    let mut seen = HashSet::new();
    let mut candidate = String::new();

    let finish_candidate =
        |candidate: &mut String, keys: &mut Vec<SecretKey>, seen: &mut HashSet<String>| {
            if is_supported_key_shape(candidate) && seen.insert(candidate.clone()) {
                let value: Arc<str> = Arc::from(candidate.as_str());
                keys.push(SecretKey(value));
            }
            candidate.clear();
        };

    for character in contents.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
            candidate.push(character);
        } else if !candidate.is_empty() {
            finish_candidate(&mut candidate, &mut keys, &mut seen);
        }
    }
    if !candidate.is_empty() {
        finish_candidate(&mut candidate, &mut keys, &mut seen);
    }
    keys
}

fn is_supported_key_shape(candidate: &str) -> bool {
    if let Some(suffix) = candidate.strip_prefix("sk_") {
        return suffix.len() >= 20
            && suffix.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
            });
    }
    candidate.len() >= 32
        && candidate
            .chars()
            .all(|character| character.is_ascii_hexdigit())
}

fn multipart_boundary() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let counter = MULTIPART_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("codex-stt-{now:x}-{counter:x}")
}

fn build_multipart_body(boundary: &str, path: &Path, media: &[u8]) -> Vec<u8> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .filter(|value| {
            !value.is_empty()
                && value
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric())
        })
        .unwrap_or("bin")
        .to_ascii_lowercase();
    let filename = format!("segment.{extension}");
    let media_type = media_type_for_extension(&extension);
    let mut body = Vec::with_capacity(media.len() + 1_024);

    push_text_part(&mut body, boundary, "model_id", "scribe_v2");
    push_text_part(&mut body, boundary, "diarize", "true");
    push_text_part(&mut body, boundary, "tag_audio_events", "true");
    push_text_part(&mut body, boundary, "timestamps_granularity", "character");
    push_text_part(&mut body, boundary, "no_verbatim", "false");
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(format!("Content-Type: {media_type}\r\n\r\n").as_bytes());
    body.extend_from_slice(media);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

fn push_text_part(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(value.as_bytes());
    body.extend_from_slice(b"\r\n");
}

fn media_type_for_extension(extension: &str) -> &'static str {
    match extension {
        "aac" => "audio/aac",
        "flac" => "audio/flac",
        "m4a" => "audio/mp4",
        "mp3" => "audio/mpeg",
        "ogg" | "opus" => "audio/ogg",
        "wav" => "audio/wav",
        "webm" => "audio/webm",
        "mp4" => "video/mp4",
        // FFmpeg's segment muxer writes MPEG-2 transport stream chunks.  Use
        // the registered media type so Scribe receives the five-second `.ts`
        // segment as media instead of an opaque binary upload.
        "ts" => "video/mp2t",
        "mov" => "video/quicktime",
        "mkv" => "video/x-matroska",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::State,
        http::{HeaderMap as AxumHeaderMap, StatusCode},
        routing::post,
    };
    use chrono::Utc;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Clone)]
    struct TestSttEndpointState {
        requests: Arc<AtomicUsize>,
    }

    async fn test_stt_endpoint(
        State(state): State<TestSttEndpointState>,
        headers: AxumHeaderMap,
    ) -> (StatusCode, Json<serde_json::Value>) {
        state.requests.fetch_add(1, Ordering::SeqCst);
        let key = headers
            .get("xi-api-key")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        if key == "test-key-2" {
            (
                StatusCode::OK,
                Json(json!({"text": "third key must not be reached", "words": []})),
            )
        } else {
            (StatusCode::UNAUTHORIZED, Json(json!({"detail": "test"})))
        }
    }

    async fn spawn_test_stt_endpoint() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let requests = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/speech-to-text", post(test_stt_endpoint))
            .with_state(TestSttEndpointState {
                requests: requests.clone(),
            });
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{address}/speech-to-text"), requests, task)
    }

    fn segment(index: u64) -> SegmentInput {
        let start_sec = index as f64 * SEGMENT_SECONDS;
        SegmentInput::new(
            index,
            start_sec,
            start_sec + SEGMENT_SECONDS,
            format!("segment_{index}.wav"),
        )
    }

    #[test]
    fn transcription_contract_is_four_five_second_chunks() {
        assert_eq!(SEGMENT_SECONDS, 5.0);
        assert_eq!(WINDOW_CHUNKS, 4);
    }

    #[tokio::test]
    async fn successful_stt_key_selection_stays_on_the_same_slot() {
        let ring = KeyRing::new(vec![
            SecretKey(Arc::from("test-key-0")),
            SecretKey(Arc::from("test-key-1")),
            SecretKey(Arc::from("test-key-2")),
        ]);
        let attempted = HashSet::new();

        let (first_slot, _) = ring.next_available(&attempted).await.unwrap();
        ring.record_success(first_slot).await;
        let (second_slot, _) = ring.next_available(&attempted).await.unwrap();
        ring.record_success(second_slot).await;
        let (third_slot, _) = ring.next_available(&attempted).await.unwrap();

        assert_eq!([first_slot, second_slot, third_slot], [0, 0, 0]);
    }

    #[tokio::test]
    async fn failed_stt_key_moves_to_a_sticky_fallback_slot() {
        let ring = KeyRing::new(vec![
            SecretKey(Arc::from("test-key-0")),
            SecretKey(Arc::from("test-key-1")),
            SecretKey(Arc::from("test-key-2")),
        ]);
        let attempted = HashSet::new();

        let (primary_slot, _) = ring.next_available(&attempted).await.unwrap();
        ring.record_failure(primary_slot, "QUOTA", Duration::from_secs(60))
            .await;
        let (fallback_slot, _) = ring.next_available(&attempted).await.unwrap();
        ring.record_success(fallback_slot).await;
        let (next_slot, _) = ring.next_available(&attempted).await.unwrap();

        assert_eq!(primary_slot, 0);
        assert_eq!(fallback_slot, 1);
        assert_eq!(next_slot, fallback_slot);
    }

    #[test]
    fn default_deadlines_bound_a_stalled_segment_near_one_capture_interval() {
        let options = SttOptions::default();

        assert_eq!(options.request_timeout, Duration::from_secs(4));
        assert_eq!(options.segment_timeout, Duration::from_secs(6));
        assert!(options.request_timeout < options.segment_timeout);
    }

    #[test]
    fn parses_and_deduplicates_supported_key_shapes_without_exposable_debug() {
        let first = ["sk", "_0123456789abcdefghijklmnop"].concat();
        let second = "0123456789ABCDEF0123456789ABCDEF";
        let contents =
            format!("ELEVENLABS_API_KEY={first}\nduplicate: '{first}'\nhex={second}\nshort=sk_bad");

        let keys = parse_vault_keys(&contents);

        assert_eq!(keys.len(), 2);
        let debug = format!("{keys:?}");
        assert!(!debug.contains(&first));
        assert!(!debug.contains(second));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn assembles_window_in_order_and_marks_a_missing_chunk_incomplete() {
        let expected = [segment(40), segment(41), segment(42), segment(43)];
        let completed = vec![
            TranscriptChunk::test_complete(&expected[2], "three"),
            TranscriptChunk::test_complete(&expected[0], "one"),
        ];

        let window = assemble_window(&expected, completed);

        assert_eq!(
            window
                .chunks
                .iter()
                .map(|chunk| chunk.index)
                .collect::<Vec<_>>(),
            vec![40, 41, 42, 43]
        );
        assert_eq!(window.chunks[1].status, TranscriptStatus::Incomplete);
        assert_eq!(
            window.chunks[1].failure,
            Some(TranscriptFailure::MissingResult)
        );
        assert_eq!(window.incomplete_count, 2);
        assert!(!window.complete);
        assert_eq!(window.text, "one three");
        assert_eq!(window.start_sec, 200.0);
        assert_eq!(window.end_sec, 220.0);
    }

    #[test]
    fn multipart_contains_all_required_scribe_v2_controls() {
        let body = build_multipart_body("test-boundary", Path::new("chunk.wav"), b"audio");
        let body = String::from_utf8(body).unwrap();

        for required in [
            "name=\"model_id\"\r\n\r\nscribe_v2",
            "name=\"diarize\"\r\n\r\ntrue",
            "name=\"tag_audio_events\"\r\n\r\ntrue",
            "name=\"timestamps_granularity\"\r\n\r\ncharacter",
            "name=\"no_verbatim\"\r\n\r\nfalse",
        ] {
            assert!(
                body.contains(required),
                "missing multipart field {required}"
            );
        }
        assert!(body.ends_with("--test-boundary--\r\n"));
    }

    #[test]
    fn mpeg_ts_segment_uses_registered_media_type() {
        assert_eq!(media_type_for_extension("ts"), "video/mp2t");

        let body = build_multipart_body("test-boundary", Path::new("segment_00042.TS"), b"mpeg-ts");
        let body = String::from_utf8(body).unwrap();

        assert!(body.contains("filename=\"segment.ts\""));
        assert!(body.contains("Content-Type: video/mp2t\r\n\r\nmpeg-ts"));
    }

    #[test]
    fn exact_window_requires_four_contiguous_five_second_chunks() {
        let mut segments = [segment(0), segment(1), segment(2), segment(3)];
        assert!(valid_window_layout(&segments));

        segments[1].start_sec += 0.01;
        segments[1].end_sec += 0.01;
        assert!(!valid_window_layout(&segments));
    }

    #[tokio::test]
    async fn one_segment_never_walks_a_third_credential() {
        let (endpoint, requests, server) = spawn_test_stt_endpoint().await;
        let mut options = SttOptions::default();
        options.endpoint = endpoint;
        options.segment_timeout = Duration::from_secs(2);
        options.request_timeout = Duration::from_millis(500);
        options.auth_cooldown = Duration::from_millis(10);
        let client = ElevenLabsSttClient::from_keys_with_options(
            ["test-key-0", "test-key-1", "test-key-2"],
            options,
        )
        .unwrap();
        let path = std::env::temp_dir().join(format!(
            "observer-stt-two-attempt-{}-{}.ts",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        tokio::fs::write(&path, b"test media").await.unwrap();
        let input = SegmentInput::new(0, 0.0, SEGMENT_SECONDS, path.clone());

        let transcript = client.transcribe_segment(input).await;

        let _ = tokio::fs::remove_file(path).await;
        server.abort();
        assert_eq!(transcript.status, TranscriptStatus::Incomplete);
        assert_eq!(transcript.failure, Some(TranscriptFailure::Authentication));
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }
}
