use std::{sync::Arc, time::Duration};

use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, NaiveDate, Utc};
use chrono_tz::America::Los_Angeles;
use reqwest::{Client, header::HeaderValue};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, time::Instant};

const API_ROOT: &str = "https://www.googleapis.com/youtube/v3";
const SEARCH_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MAX_SEARCH_CALLS_PER_DAY: u16 = 90;
const MAX_API_BODY_BYTES: usize = 2 * 1024 * 1024;
const UPLOADS_CANDIDATE_LIMIT: usize = 50;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum YouTubeVaultState {
    Loaded,
    Ready,
    KeyRequired,
    InvalidOrRevoked,
    Quota,
    RateLimited,
    Degraded,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct YouTubeVaultHealth {
    #[serde(skip_serializing)]
    pub generation: u64,
    pub loaded_slots: usize,
    pub state: YouTubeVaultState,
}

/// A one-slot, write-only, process-local YouTube Data API credential store.
///
/// The key is held as a sensitive HTTP header so it never becomes part of a
/// request URL, response, persisted dashboard state, or debug representation.
pub struct YouTubeKeyVault {
    inner: Mutex<YouTubeKeyState>,
}

struct YouTubeKeyState {
    generation: u64,
    header: Option<HeaderValue>,
    state: YouTubeVaultState,
}

#[derive(Clone)]
struct YouTubeKeySelection {
    generation: u64,
    header: HeaderValue,
}

impl YouTubeKeyVault {
    pub fn empty() -> Self {
        Self {
            inner: Mutex::new(YouTubeKeyState {
                generation: 0,
                header: None,
                state: YouTubeVaultState::KeyRequired,
            }),
        }
    }

    pub async fn replace(&self, key: &str) -> Result<usize> {
        let raw = key.trim();
        if raw.is_empty() || raw.len() > 512 {
            bail!("submitted key material was rejected");
        }
        let mut header = HeaderValue::from_str(raw)
            .map_err(|_| anyhow!("submitted key material was rejected"))?;
        header.set_sensitive(true);
        let mut state = self.inner.lock().await;
        state.header = Some(header);
        state.state = YouTubeVaultState::Loaded;
        state.generation = state.generation.wrapping_add(1);
        Ok(1)
    }

    pub async fn clear(&self) {
        let mut state = self.inner.lock().await;
        state.header = None;
        state.state = YouTubeVaultState::KeyRequired;
        state.generation = state.generation.wrapping_add(1);
    }

    pub async fn health(&self) -> YouTubeVaultHealth {
        let state = self.inner.lock().await;
        let loaded_slots = usize::from(state.header.is_some());
        YouTubeVaultHealth {
            generation: state.generation,
            loaded_slots,
            state: state.state,
        }
    }

    async fn record_success(&self, generation: u64) {
        let mut state = self.inner.lock().await;
        if state.generation == generation && state.header.is_some() {
            state.state = YouTubeVaultState::Ready;
        }
    }

    async fn record_failure(&self, generation: u64, class: ProviderFailureClass) {
        let mut state = self.inner.lock().await;
        if state.generation == generation && state.header.is_some() {
            state.state = class.vault_state();
        }
    }

    async fn selection(&self) -> Option<YouTubeKeySelection> {
        let state = self.inner.lock().await;
        Some(YouTubeKeySelection {
            generation: state.generation,
            header: state.header.clone()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiDiscovery {
    Live(String),
    NotLive,
    Indeterminate,
    NoKey,
    Unavailable(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderFailureClass {
    InvalidOrRevoked,
    Quota,
    RateLimited,
    Degraded,
}

impl ProviderFailureClass {
    fn vault_state(self) -> YouTubeVaultState {
        match self {
            Self::InvalidOrRevoked => YouTubeVaultState::InvalidOrRevoked,
            Self::Quota => YouTubeVaultState::Quota,
            Self::RateLimited => YouTubeVaultState::RateLimited,
            Self::Degraded => YouTubeVaultState::Degraded,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChannelDetails {
    channel_id: String,
    uploads_playlist_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ChannelLookup {
    Id(String),
    Handle(String),
    Username(String),
}

#[derive(Debug, Default)]
struct SearchBudget {
    day: Option<NaiveDate>,
    calls: u16,
    last_call: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchAvailability {
    Due,
    IntervalLimited,
    DailyCapReached,
}

impl SearchBudget {
    fn availability(&mut self, now: Instant, day: NaiveDate) -> SearchAvailability {
        if self.day != Some(day) {
            self.day = Some(day);
            self.calls = 0;
        }
        if self.calls >= MAX_SEARCH_CALLS_PER_DAY {
            return SearchAvailability::DailyCapReached;
        }
        if self
            .last_call
            .is_some_and(|last| now.duration_since(last) < SEARCH_INTERVAL)
        {
            return SearchAvailability::IntervalLimited;
        }
        SearchAvailability::Due
    }

    #[cfg(test)]
    fn is_due(&mut self, now: Instant, day: NaiveDate) -> bool {
        self.availability(now, day) == SearchAvailability::Due
    }

    fn record_call(&mut self, now: Instant) {
        self.calls = self.calls.saturating_add(1);
        self.last_call = Some(now);
    }
}

/// Stateful, quota-bounded YouTube Data API live discovery.
pub struct YouTubeApiDiscovery {
    http: Client,
    vault: Arc<YouTubeKeyVault>,
    channel_url: String,
    key_generation: Option<u64>,
    channel: Option<ChannelDetails>,
    search_budget: SearchBudget,
}

impl YouTubeApiDiscovery {
    pub fn new(http: Client, vault: Arc<YouTubeKeyVault>, channel_url: impl Into<String>) -> Self {
        Self {
            http,
            vault,
            channel_url: channel_url.into(),
            key_generation: None,
            channel: None,
            search_budget: SearchBudget::default(),
        }
    }

    pub async fn discover(&mut self) -> ApiDiscovery {
        match self.try_discover().await {
            Ok(outcome) => {
                if !matches!(&outcome, ApiDiscovery::NoKey)
                    && let Some(generation) = self.key_generation
                {
                    self.vault.record_success(generation).await;
                }
                outcome
            }
            Err(error) => {
                let detail = bounded_api_failure_detail(&error.to_string());
                if let Some(generation) = self.key_generation {
                    self.vault
                        .record_failure(generation, classify_provider_failure(&detail))
                        .await;
                }
                ApiDiscovery::Unavailable(detail)
            }
        }
    }

    async fn try_discover(&mut self) -> Result<ApiDiscovery> {
        let Some(key) = self.vault.selection().await else {
            return Ok(ApiDiscovery::NoKey);
        };
        self.observe_key_generation(key.generation);

        if self.channel.is_none() {
            let lookup = channel_lookup(&self.channel_url)?;
            let mut query = vec![("part", "contentDetails".to_owned())];
            match lookup {
                ChannelLookup::Id(value) => query.push(("id", value)),
                ChannelLookup::Handle(value) => query.push(("forHandle", value)),
                ChannelLookup::Username(value) => query.push(("forUsername", value)),
            }
            let body = request_api(&self.http, &key, "channels", &query).await?;
            self.channel = parse_channel_details(&body)?;
            if self.channel.is_none() {
                bail!("YouTube Data API did not resolve the configured channel");
            }
        }
        let channel = self
            .channel
            .as_ref()
            .expect("channel checked above")
            .clone();

        let playlist_body = request_api(
            &self.http,
            &key,
            "playlistItems",
            &[
                ("part", "contentDetails".to_owned()),
                ("playlistId", channel.uploads_playlist_id.clone()),
                ("maxResults", UPLOADS_CANDIDATE_LIMIT.to_string()),
            ],
        )
        .await?;
        let recent_ids = parse_playlist_video_ids(&playlist_body)?;
        if let Some(url) = self.active_video_from_ids(&key, &recent_ids).await? {
            return Ok(ApiDiscovery::Live(url));
        }

        let now = Instant::now();
        let availability = self
            .search_budget
            .availability(now, youtube_quota_day(Utc::now()));
        if availability != SearchAvailability::Due {
            return Ok(discovery_without_search(availability));
        }
        self.search_budget.record_call(now);
        let search_body = request_api(
            &self.http,
            &key,
            "search",
            &[
                ("part", "snippet".to_owned()),
                ("channelId", channel.channel_id),
                ("eventType", "live".to_owned()),
                ("type", "video".to_owned()),
                ("maxResults", "5".to_owned()),
            ],
        )
        .await?;
        let search_ids = parse_search_video_ids(&search_body)?;
        if let Some(url) = self.active_video_from_ids(&key, &search_ids).await? {
            return Ok(ApiDiscovery::Live(url));
        }
        Ok(ApiDiscovery::NotLive)
    }

    /// A credential replacement invalidates only credential-bound channel
    /// lookup state. The process-local daily search allowance intentionally
    /// survives replacement so public write-only slot updates cannot reset it.
    fn observe_key_generation(&mut self, generation: u64) {
        if self.key_generation != Some(generation) {
            self.key_generation = Some(generation);
            self.channel = None;
        }
    }

    async fn active_video_from_ids(
        &self,
        key: &YouTubeKeySelection,
        ids: &[String],
    ) -> Result<Option<String>> {
        if ids.is_empty() {
            return Ok(None);
        }
        let body = request_api(
            &self.http,
            key,
            "videos",
            &[
                ("part", "snippet,liveStreamingDetails".to_owned()),
                ("id", ids.join(",")),
            ],
        )
        .await?;
        parse_active_video(&body)
    }
}

fn youtube_quota_day(now: DateTime<Utc>) -> NaiveDate {
    now.with_timezone(&Los_Angeles).date_naive()
}

fn discovery_without_search(availability: SearchAvailability) -> ApiDiscovery {
    debug_assert_ne!(availability, SearchAvailability::Due);
    ApiDiscovery::Indeterminate
}

async fn request_api(
    http: &Client,
    key: &YouTubeKeySelection,
    resource: &str,
    query: &[(&str, String)],
) -> Result<String> {
    let url = format!("{API_ROOT}/{resource}");
    let response = tokio::time::timeout(
        Duration::from_secs(12),
        http.get(url)
            .header("x-goog-api-key", key.header.clone())
            .query(query)
            .send(),
    )
    .await
    .map_err(|_| anyhow!("YouTube Data API request timed out"))?
    .map_err(|_| anyhow!("YouTube Data API request failed"))?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .map_err(|_| anyhow!("YouTube Data API response could not be read"))?;
    if bytes.len() > MAX_API_BODY_BYTES {
        bail!("YouTube Data API response exceeded the safe size limit");
    }
    let body = String::from_utf8_lossy(&bytes).into_owned();
    if !status.is_success() {
        let detail = api_failure_detail(&body, Some(&key.header));
        if detail.is_empty() {
            bail!("YouTube Data API returned HTTP {}", status.as_u16());
        }
        bail!(
            "YouTube Data API returned HTTP {}: {detail}",
            status.as_u16()
        );
    }
    Ok(body)
}

fn channel_lookup(channel_url: &str) -> Result<ChannelLookup> {
    let configured = channel_url.trim();
    if let Some(handle) = configured.strip_prefix('@') {
        if !safe_identifier(handle, 3, 128) {
            bail!("configured YouTube channel identifier is invalid");
        }
        return Ok(ChannelLookup::Handle(handle.to_owned()));
    }
    if configured.starts_with("UC") && configured.len() == 24 && safe_identifier(configured, 24, 24)
    {
        return Ok(ChannelLookup::Id(configured.to_owned()));
    }

    let url = reqwest::Url::parse(configured)
        .map_err(|_| anyhow!("configured YouTube channel URL is invalid"))?;
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if host != "youtube.com" && host != "www.youtube.com" && host != "m.youtube.com" {
        bail!("configured channel URL is not a supported YouTube URL");
    }
    let segments = url
        .path_segments()
        .map(|items| items.filter(|item| !item.is_empty()).collect::<Vec<_>>())
        .unwrap_or_default();
    let lookup = match segments.as_slice() {
        [handle] if handle.starts_with('@') => {
            ChannelLookup::Handle(handle.trim_start_matches('@').to_owned())
        }
        ["channel", id, ..] => ChannelLookup::Id((*id).to_owned()),
        ["user", username, ..] => ChannelLookup::Username((*username).to_owned()),
        _ => bail!("configured YouTube channel URL cannot be resolved by the Data API"),
    };
    let value = match &lookup {
        ChannelLookup::Id(value)
        | ChannelLookup::Handle(value)
        | ChannelLookup::Username(value) => value,
    };
    if !safe_identifier(value, 3, 128) {
        bail!("configured YouTube channel identifier is invalid");
    }
    Ok(lookup)
}

fn safe_identifier(value: &str, minimum: usize, maximum: usize) -> bool {
    (minimum..=maximum).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn safe_video_id(value: &str) -> bool {
    value.len() == 11
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

#[derive(Deserialize)]
struct ChannelsResponse {
    #[serde(default)]
    items: Vec<ChannelItem>,
}

#[derive(Deserialize)]
struct ChannelItem {
    id: String,
    #[serde(rename = "contentDetails")]
    content_details: ChannelContentDetails,
}

#[derive(Deserialize)]
struct ChannelContentDetails {
    #[serde(rename = "relatedPlaylists")]
    related_playlists: RelatedPlaylists,
}

#[derive(Deserialize)]
struct RelatedPlaylists {
    uploads: String,
}

fn parse_channel_details(body: &str) -> Result<Option<ChannelDetails>> {
    let response: ChannelsResponse = serde_json::from_str(body)
        .map_err(|_| anyhow!("YouTube channels response was malformed"))?;
    Ok(response.items.into_iter().find_map(|item| {
        (safe_identifier(&item.id, 3, 128)
            && safe_identifier(&item.content_details.related_playlists.uploads, 3, 128))
        .then_some(ChannelDetails {
            channel_id: item.id,
            uploads_playlist_id: item.content_details.related_playlists.uploads,
        })
    }))
}

#[derive(Deserialize)]
struct PlaylistItemsResponse {
    #[serde(default)]
    items: Vec<PlaylistItem>,
}

#[derive(Deserialize)]
struct PlaylistItem {
    #[serde(rename = "contentDetails")]
    content_details: PlaylistItemContentDetails,
}

#[derive(Deserialize)]
struct PlaylistItemContentDetails {
    #[serde(rename = "videoId")]
    video_id: String,
}

fn parse_playlist_video_ids(body: &str) -> Result<Vec<String>> {
    let response: PlaylistItemsResponse = serde_json::from_str(body)
        .map_err(|_| anyhow!("YouTube uploads response was malformed"))?;
    Ok(response
        .items
        .into_iter()
        .map(|item| item.content_details.video_id)
        .filter(|id| safe_video_id(id))
        .collect())
}

#[derive(Deserialize)]
struct SearchResponse {
    #[serde(default)]
    items: Vec<SearchItem>,
}

#[derive(Deserialize)]
struct SearchItem {
    id: SearchItemId,
}

#[derive(Deserialize)]
struct SearchItemId {
    #[serde(rename = "videoId")]
    video_id: Option<String>,
}

fn parse_search_video_ids(body: &str) -> Result<Vec<String>> {
    let response: SearchResponse = serde_json::from_str(body)
        .map_err(|_| anyhow!("YouTube live-search response was malformed"))?;
    Ok(response
        .items
        .into_iter()
        .filter_map(|item| item.id.video_id)
        .filter(|id| safe_video_id(id))
        .collect())
}

#[derive(Deserialize)]
struct VideosResponse {
    #[serde(default)]
    items: Vec<VideoItem>,
}

#[derive(Deserialize)]
struct VideoItem {
    id: String,
    #[serde(default)]
    snippet: VideoSnippet,
    #[serde(default, rename = "liveStreamingDetails")]
    live_streaming_details: Option<LiveStreamingDetails>,
}

#[derive(Default, Deserialize)]
struct VideoSnippet {
    #[serde(default, rename = "liveBroadcastContent")]
    live_broadcast_content: String,
}

#[derive(Default, Deserialize)]
struct LiveStreamingDetails {
    #[serde(default, rename = "actualStartTime")]
    actual_start_time: Option<String>,
    #[serde(default, rename = "actualEndTime")]
    actual_end_time: Option<String>,
}

fn parse_active_video(body: &str) -> Result<Option<String>> {
    let response: VideosResponse =
        serde_json::from_str(body).map_err(|_| anyhow!("YouTube videos response was malformed"))?;
    Ok(response.items.into_iter().find_map(|item| {
        if !safe_video_id(&item.id) {
            return None;
        }
        let details = item.live_streaming_details.as_ref();
        if details.is_some_and(|details| details.actual_end_time.is_some()) {
            return None;
        }
        let details_active = details.is_some_and(|details| details.actual_start_time.is_some());
        (item
            .snippet
            .live_broadcast_content
            .eq_ignore_ascii_case("live")
            || details_active)
            .then(|| format!("https://www.youtube.com/watch?v={}", item.id))
    }))
}

/// Produces useful provider diagnostics without URLs, query strings, control
/// characters, or unbounded response text.
pub(crate) fn bounded_api_failure_detail(raw: &str) -> String {
    let clean = raw
        .chars()
        .filter(|character| {
            (character.is_ascii_graphic() || character.is_ascii_whitespace())
                && *character != '\u{1b}'
        })
        .collect::<String>();
    let mut result = String::new();
    for word in clean.split_whitespace() {
        let safe = if word.starts_with("http://")
            || word.starts_with("https://")
            || word.to_ascii_lowercase().contains("key=")
        {
            "[redacted]"
        } else {
            word
        };
        if !result.is_empty() {
            result.push(' ');
        }
        result.push_str(safe);
        if result.len() >= 240 {
            result.truncate(240);
            break;
        }
    }
    result
}

fn api_failure_detail(raw: &str, secret: Option<&HeaderValue>) -> String {
    let redacted = secret
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map_or_else(|| raw.to_owned(), |value| raw.replace(value, "[redacted]"));
    bounded_api_failure_detail(&redacted)
}

fn classify_provider_failure(detail: &str) -> ProviderFailureClass {
    let normalized = detail.to_ascii_lowercase();
    if normalized.contains("keyinvalid")
        || normalized.contains("api key not valid")
        || normalized.contains("invalid api key")
        || normalized.contains("http 401")
    {
        ProviderFailureClass::InvalidOrRevoked
    } else if normalized.contains("quotaexceeded")
        || normalized.contains("dailylimitexceeded")
        || normalized.contains("quota exceeded")
    {
        ProviderFailureClass::Quota
    } else if normalized.contains("ratelimitexceeded")
        || normalized.contains("http 429")
        || normalized.contains("rate limit")
    {
        ProviderFailureClass::RateLimited
    } else {
        ProviderFailureClass::Degraded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runtime_key_slot_replaces_and_clears_without_exposing_secret() {
        let vault = YouTubeKeyVault::empty();
        assert_eq!(vault.replace("first-youtube-test-key").await.unwrap(), 1);
        assert_eq!(vault.replace("second-youtube-test-key").await.unwrap(), 1);

        let health = vault.health().await;
        assert_eq!(health.loaded_slots, 1);
        assert_eq!(health.state, YouTubeVaultState::Loaded);
        let serialized = serde_json::to_string(&health).unwrap();
        assert!(!serialized.contains("generation"));
        assert!(!serialized.contains("first-youtube-test-key"));
        assert!(!serialized.contains("second-youtube-test-key"));

        let selection = vault.selection().await.unwrap();
        assert!(selection.header.is_sensitive());
        assert_eq!(selection.generation, health.generation);

        vault.clear().await;
        assert_eq!(vault.health().await.loaded_slots, 0);
        assert!(vault.selection().await.is_none());
    }

    #[test]
    fn channel_urls_map_to_supported_official_api_filters() {
        assert_eq!(
            channel_lookup("@TRADINGCAFEINDIA").unwrap(),
            ChannelLookup::Handle("TRADINGCAFEINDIA".to_owned())
        );
        assert_eq!(
            channel_lookup("UCCW6WdKJfqFUfoio0Lr0iHw").unwrap(),
            ChannelLookup::Id("UCCW6WdKJfqFUfoio0Lr0iHw".to_owned())
        );
        assert_eq!(
            channel_lookup("https://www.youtube.com/@TRADINGCAFEINDIA").unwrap(),
            ChannelLookup::Handle("TRADINGCAFEINDIA".to_owned())
        );
        assert_eq!(
            channel_lookup("https://www.youtube.com/channel/UCCW6WdKJfqFUfoio0Lr0iHw").unwrap(),
            ChannelLookup::Id("UCCW6WdKJfqFUfoio0Lr0iHw".to_owned())
        );
        assert_eq!(
            channel_lookup("https://www.youtube.com/user/legacy-name").unwrap(),
            ChannelLookup::Username("legacy-name".to_owned())
        );
    }

    #[test]
    fn official_video_response_accepts_only_current_active_live_broadcasts() {
        let response = r#"{
          "items": [
            {"id":"ended123456", "snippet":{"liveBroadcastContent":"none"},
             "liveStreamingDetails":{"actualStartTime":"2026-08-17T03:30:00Z","actualEndTime":"2026-08-17T04:00:00Z"}},
            {"id":"live1234567", "snippet":{"liveBroadcastContent":"live"},
             "liveStreamingDetails":{"actualStartTime":"2026-08-17T04:01:00Z"}}
          ]
        }"#;

        assert_eq!(
            parse_active_video(response).unwrap(),
            Some("https://www.youtube.com/watch?v=live1234567".to_owned())
        );
    }

    #[test]
    fn explicit_actual_end_time_vetoes_a_stale_live_snippet() {
        let response = r#"{
          "items": [
            {"id":"ended123456", "snippet":{"liveBroadcastContent":"live"},
             "liveStreamingDetails":{"actualStartTime":"2026-08-17T03:30:00Z","actualEndTime":"2026-08-17T04:00:00Z"}}
          ]
        }"#;

        assert_eq!(parse_active_video(response).unwrap(), None);
    }

    #[test]
    fn official_api_payload_parsers_extract_uploads_and_candidate_ids() {
        let channel = r#"{"items":[{"id":"UC123","contentDetails":{"relatedPlaylists":{"uploads":"UU123"}}}]}"#;
        assert_eq!(
            parse_channel_details(channel).unwrap(),
            Some(ChannelDetails {
                channel_id: "UC123".to_owned(),
                uploads_playlist_id: "UU123".to_owned(),
            })
        );
        let playlist = r#"{"items":[{"contentDetails":{"videoId":"abc123DEF_-"}},{"contentDetails":{"videoId":"bad"}}]}"#;
        assert_eq!(
            parse_playlist_video_ids(playlist).unwrap(),
            vec!["abc123DEF_-".to_owned()]
        );
        let search = r#"{"items":[{"id":{"videoId":"xyz123ABC_-"}}]}"#;
        assert_eq!(
            parse_search_video_ids(search).unwrap(),
            vec!["xyz123ABC_-".to_owned()]
        );
    }

    #[test]
    fn api_failure_detail_is_bounded_and_redacts_urls_and_key_material() {
        let detail = bounded_api_failure_detail(
            "request failed https://www.googleapis.com/youtube/v3/videos?id=x&key=secret-value\nquota exceeded",
        );
        assert!(!detail.contains("secret-value"));
        assert!(!detail.contains("https://"));
        assert!(detail.len() <= 240);
    }

    #[test]
    fn provider_error_cannot_echo_the_submitted_key_into_diagnostics() {
        let mut secret = HeaderValue::from_static("directly-echoed-secret");
        secret.set_sensitive(true);
        let detail = api_failure_detail("API key directly-echoed-secret is invalid", Some(&secret));

        assert_eq!(detail, "API key [redacted] is invalid");
    }

    #[tokio::test]
    async fn vault_health_tracks_provider_result_without_returning_failure_or_key_text() {
        let vault = YouTubeKeyVault::empty();
        vault.replace("provider-health-secret").await.unwrap();
        let generation = vault.health().await.generation;

        vault
            .record_failure(generation, ProviderFailureClass::Quota)
            .await;
        let failed = vault.health().await;
        assert_eq!(failed.state, YouTubeVaultState::Quota);
        assert!(
            !serde_json::to_string(&failed)
                .unwrap()
                .contains("provider-health-secret")
        );

        vault.record_success(generation).await;
        assert_eq!(vault.health().await.state, YouTubeVaultState::Ready);
    }

    #[test]
    fn provider_failure_classification_is_safe_and_actionable() {
        assert_eq!(
            classify_provider_failure("YouTube Data API returned HTTP 403: keyInvalid"),
            ProviderFailureClass::InvalidOrRevoked
        );
        assert_eq!(
            classify_provider_failure("YouTube Data API returned HTTP 403: quotaExceeded"),
            ProviderFailureClass::Quota
        );
        assert_eq!(
            classify_provider_failure("YouTube Data API returned HTTP 429"),
            ProviderFailureClass::RateLimited
        );
        assert_eq!(
            classify_provider_failure("YouTube Data API returned HTTP 403: rateLimitExceeded"),
            ProviderFailureClass::RateLimited
        );
        assert_eq!(
            classify_provider_failure("YouTube Data API request timed out"),
            ProviderFailureClass::Degraded
        );
    }

    #[test]
    fn replacing_a_key_does_not_reset_the_process_search_budget() {
        let vault = Arc::new(YouTubeKeyVault::empty());
        let mut discovery = YouTubeApiDiscovery::new(
            Client::new(),
            vault,
            "https://www.youtube.com/@TRADINGCAFEINDIA",
        );
        let now = Instant::now();
        let day = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        assert!(discovery.search_budget.is_due(now, day));
        discovery.search_budget.record_call(now);

        discovery.observe_key_generation(1);
        discovery.observe_key_generation(2);

        assert_eq!(discovery.search_budget.calls, 1);
        assert!(
            !discovery
                .search_budget
                .is_due(now + Duration::from_secs(60), day)
        );
    }

    #[test]
    fn search_fallback_stops_at_the_process_daily_cap() {
        let mut budget = SearchBudget::default();
        let start = Instant::now();
        let day = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        for call in 0..MAX_SEARCH_CALLS_PER_DAY {
            let now = start + SEARCH_INTERVAL * u32::from(call);
            assert!(budget.is_due(now, day));
            budget.record_call(now);
        }
        assert!(!budget.is_due(
            start + SEARCH_INTERVAL * u32::from(MAX_SEARCH_CALLS_PER_DAY),
            day
        ));
    }

    #[test]
    fn day_rollover_resets_only_the_daily_count_not_the_five_minute_interval() {
        let mut budget = SearchBudget::default();
        let first_day = NaiveDate::from_ymd_opt(2026, 8, 17).unwrap();
        let next_day = first_day.succ_opt().unwrap();
        let first_call = Instant::now();
        assert!(budget.is_due(first_call, first_day));
        budget.record_call(first_call);

        assert!(!budget.is_due(first_call + Duration::from_secs(60), next_day));
        assert_eq!(budget.calls, 0);
        assert!(budget.is_due(first_call + SEARCH_INTERVAL, next_day));
    }

    #[test]
    fn quota_day_rolls_over_at_pacific_midnight_instead_of_utc_midnight() {
        use chrono::TimeZone;

        let before_pacific_midnight = Utc.with_ymd_and_hms(2026, 8, 17, 6, 59, 59).unwrap();
        let after_pacific_midnight = Utc.with_ymd_and_hms(2026, 8, 17, 7, 0, 1).unwrap();

        assert_eq!(
            youtube_quota_day(before_pacific_midnight),
            NaiveDate::from_ymd_opt(2026, 8, 16).unwrap()
        );
        assert_eq!(
            youtube_quota_day(after_pacific_midnight),
            NaiveDate::from_ymd_opt(2026, 8, 17).unwrap()
        );
    }

    #[test]
    fn deferred_search_is_indeterminate_and_not_an_authoritative_negative() {
        assert_eq!(
            discovery_without_search(SearchAvailability::IntervalLimited),
            ApiDiscovery::Indeterminate
        );
        assert_eq!(
            discovery_without_search(SearchAvailability::DailyCapReached),
            ApiDiscovery::Indeterminate
        );
    }

    #[test]
    fn uploads_poll_uses_the_full_single_request_candidate_capacity() {
        assert_eq!(UPLOADS_CANDIDATE_LIMIT, 50);
    }
}
