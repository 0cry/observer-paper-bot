//! Dynamic, read-only INDstocks market-data actor.
//!
//! The actor keeps one authenticated price WebSocket while instruments are
//! needed. Candidate subscriptions expire automatically; pending-order and
//! open-position subscriptions remain until their lease is released. The
//! WebSocket is always the primary source. An optional, rate-limited REST LTP
//! poll fills gaps only when a subscription has not received a fresh
//! WebSocket tick.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    sync::{broadcast, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::{Instant, MissedTickBehavior, interval, sleep, sleep_until},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

const DEFAULT_WS_URL: &str = "wss://ws-prices.indstocks.com/api/v1/ws/prices";
const DEFAULT_API_BASE: &str = "https://api.indstocks.com";
const USER_AGENT: &str = "observer-market-feed/0.1";
const REST_MAX_INSTRUMENTS: usize = 1_000;
const MIN_REST_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// A fully resolved instrument accepted by both INDstocks quote transports.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResolvedInstrument {
    /// WebSocket form, for example `BFO:847862`.
    pub websocket_code: String,
    /// Bare token from the instrument master, for example `847862`.
    pub security_id: String,
    /// Human-facing contract label.
    pub label: String,
}

impl ResolvedInstrument {
    pub fn new(
        websocket_code: impl Into<String>,
        security_id: impl Into<String>,
        label: impl Into<String>,
    ) -> Result<Self> {
        Self {
            websocket_code: websocket_code.into(),
            security_id: security_id.into(),
            label: label.into(),
        }
        .normalized()
    }

    /// REST quote form, for example `BFO_847862`.
    pub fn rest_code(&self) -> String {
        let (segment, _) = self
            .websocket_code
            .split_once(':')
            .unwrap_or((self.websocket_code.as_str(), self.security_id.as_str()));
        format!("{}_{}", segment.to_ascii_uppercase(), self.security_id)
    }

    fn key(&self) -> String {
        self.websocket_code.trim().to_ascii_uppercase()
    }

    fn normalized(mut self) -> Result<Self> {
        self.websocket_code = self.websocket_code.trim().to_ascii_uppercase();
        self.security_id = self.security_id.trim().to_string();
        self.label = self.label.trim().to_string();

        let Some((segment, token)) = self.websocket_code.split_once(':') else {
            bail!("WebSocket instrument must use SEGMENT:TOKEN format");
        };
        if segment.is_empty() || token.is_empty() || token != self.security_id {
            bail!("WebSocket instrument and security ID do not match");
        }
        if self.label.is_empty() {
            bail!("instrument label must not be empty");
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TickSource {
    WebSocket,
    RestFallback,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tick {
    pub instrument: ResolvedInstrument,
    pub ltp: f64,
    pub exchange_timestamp_ms: Option<i64>,
    pub received_timestamp_ms: i64,
    pub source: TickSource,
}

impl Tick {
    /// Age based on when this process received the quote, never negative.
    pub fn age(&self) -> Duration {
        self.age_at(Utc::now().timestamp_millis())
    }

    pub fn age_at(&self, now_timestamp_ms: i64) -> Duration {
        Duration::from_millis(
            now_timestamp_ms
                .saturating_sub(self.received_timestamp_ms)
                .max(0) as u64,
        )
    }

    pub fn is_fresh(&self, maximum_age: Duration) -> bool {
        self.age() <= maximum_age
    }
}

/// Why an instrument must remain subscribed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionReason {
    /// Short-lived LLM candidate observation. It expires automatically.
    CandidateWatch,
    /// A limit entry is waiting for a market match.
    PendingOrder,
    /// A paper position is open and needs tick-by-tick management.
    OpenPosition,
}

impl SubscriptionReason {
    fn is_temporary(self) -> bool {
        matches!(self, Self::CandidateWatch)
    }
}

#[derive(Debug, Clone)]
pub struct RestFallbackConfig {
    /// Maximum REST request frequency. Values below one second are rejected.
    pub poll_interval: Duration,
    /// Start fallback polling after no WebSocket tick for this long.
    pub websocket_stale_after: Duration,
    pub request_timeout: Duration,
    /// Upper bound for retry/back-pressure delays after HTTP failures or 429.
    pub maximum_backoff: Duration,
}

impl Default for RestFallbackConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            websocket_stale_after: Duration::from_secs(2),
            request_timeout: Duration::from_secs(2),
            maximum_backoff: Duration::from_secs(30),
        }
    }
}

/// Shared REST cadence across connected and reconnecting WebSocket phases.
/// Keeping one schedule prevents an immediate duplicate poll every time the
/// WebSocket state machine transitions or a handshake fails quickly.
#[derive(Debug)]
struct RestPollSchedule {
    poll_interval: Duration,
    maximum_backoff: Duration,
    next_allowed_at: Instant,
    failure_delay: Duration,
}

impl RestPollSchedule {
    fn new(config: &RestFallbackConfig, now: Instant) -> Self {
        Self {
            poll_interval: config.poll_interval,
            maximum_backoff: config.maximum_backoff,
            next_allowed_at: now,
            failure_delay: config.poll_interval,
        }
    }

    fn is_ready(&self, now: Instant) -> bool {
        now >= self.next_allowed_at
    }

    fn record_success(&mut self, now: Instant) {
        self.failure_delay = self.poll_interval;
        self.next_allowed_at = now + self.poll_interval;
    }

    fn record_failure(&mut self, now: Instant) {
        self.next_allowed_at = now + self.failure_delay;
        self.failure_delay = doubled_delay(self.failure_delay, self.maximum_backoff);
    }

    fn record_retry_after(&mut self, now: Instant, requested: Option<Duration>) {
        let delay = requested
            .unwrap_or(self.failure_delay)
            .max(self.poll_interval)
            .min(self.maximum_backoff);
        self.next_allowed_at = now + delay;
        self.failure_delay = doubled_delay(delay, self.maximum_backoff);
    }
}

#[derive(Debug, Clone)]
pub struct MarketFeedConfig {
    pub websocket_url: String,
    pub rest_api_base: String,
    pub candidate_watch_ttl: Duration,
    pub command_capacity: usize,
    pub tick_broadcast_capacity: usize,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub reconnect_initial_delay: Duration,
    pub reconnect_maximum_delay: Duration,
    /// `None` disables REST polling completely.
    pub rest_fallback: Option<RestFallbackConfig>,
}

impl Default for MarketFeedConfig {
    fn default() -> Self {
        Self {
            websocket_url: DEFAULT_WS_URL.to_string(),
            rest_api_base: DEFAULT_API_BASE.to_string(),
            candidate_watch_ttl: Duration::from_secs(10),
            command_capacity: 256,
            tick_broadcast_capacity: 1_024,
            heartbeat_interval: Duration::from_secs(15),
            heartbeat_timeout: Duration::from_secs(45),
            reconnect_initial_delay: Duration::from_secs(1),
            reconnect_maximum_delay: Duration::from_secs(30),
            rest_fallback: Some(RestFallbackConfig::default()),
        }
    }
}

impl MarketFeedConfig {
    fn validate(&self) -> Result<()> {
        if self.websocket_url.trim().is_empty() {
            bail!("WebSocket URL must not be empty");
        }
        if self.rest_api_base.trim().is_empty() {
            bail!("REST API base URL must not be empty");
        }
        if self.candidate_watch_ttl.is_zero() {
            bail!("candidate watch TTL must be greater than zero");
        }
        if self.command_capacity == 0 || self.tick_broadcast_capacity == 0 {
            bail!("feed channel capacities must be greater than zero");
        }
        if self.heartbeat_interval.is_zero() || self.heartbeat_timeout <= self.heartbeat_interval {
            bail!("heartbeat timeout must be greater than heartbeat interval");
        }
        if self.reconnect_initial_delay.is_zero()
            || self.reconnect_maximum_delay < self.reconnect_initial_delay
        {
            bail!("invalid reconnect delay range");
        }
        if let Some(rest) = &self.rest_fallback {
            if rest.poll_interval < MIN_REST_POLL_INTERVAL {
                bail!("REST fallback poll interval must be at least one second");
            }
            if rest.websocket_stale_after.is_zero()
                || rest.request_timeout.is_zero()
                || rest.maximum_backoff < rest.poll_interval
            {
                bail!("invalid REST fallback timing configuration");
            }
        }
        Ok(())
    }
}

/// Async token callback abstraction. Implementations must never log the token.
pub type AccessTokenFuture<'a> = Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>>;

pub trait AccessTokenProvider: Send + Sync + 'static {
    fn access_token(&self) -> AccessTokenFuture<'_>;
}

struct CallbackTokenProvider<F>(F);

impl<F, Fut> AccessTokenProvider for CallbackTokenProvider<F>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String>> + Send + 'static,
{
    fn access_token(&self) -> AccessTokenFuture<'_> {
        Box::pin((self.0)())
    }
}

/// Wrap an async callback as an [`AccessTokenProvider`].
pub fn token_provider_fn<F, Fut>(callback: F) -> Arc<dyn AccessTokenProvider>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String>> + Send + 'static,
{
    Arc::new(CallbackTokenProvider(callback))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedConnectionState {
    Idle,
    Connecting,
    Connected,
    BackingOff,
    Stopped,
}

/// Shared latest-tick map with a cheap watch revision instead of cloning the
/// entire map on every market tick.
#[derive(Clone)]
pub struct LatestTicks {
    inner: Arc<RwLock<HashMap<String, Tick>>>,
    revision: watch::Receiver<u64>,
}

impl LatestTicks {
    pub fn revision(&self) -> u64 {
        *self.revision.borrow()
    }

    pub async fn changed(&mut self) -> Result<()> {
        self.revision
            .changed()
            .await
            .map_err(|_| anyhow!("market-feed latest-tick watcher closed"))
    }

    pub fn get(&self, instrument: &ResolvedInstrument) -> Option<Tick> {
        read_lock(&self.inner).get(&instrument.key()).cloned()
    }

    pub fn get_by_websocket_code(&self, websocket_code: &str) -> Option<Tick> {
        read_lock(&self.inner)
            .get(&websocket_code.trim().to_ascii_uppercase())
            .cloned()
    }

    pub fn snapshot(&self) -> HashMap<String, Tick> {
        read_lock(&self.inner).clone()
    }
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone)]
pub struct MarketFeedHandle {
    commands: mpsc::Sender<FeedCommand>,
    ticks: broadcast::Sender<Tick>,
    latest: LatestTicks,
    connection_state: watch::Receiver<FeedConnectionState>,
}

impl MarketFeedHandle {
    pub async fn subscribe(
        &self,
        instrument: ResolvedInstrument,
        reason: SubscriptionReason,
    ) -> Result<SubscriptionLease> {
        let instrument = instrument.normalized()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(FeedCommand::Acquire {
                instrument: instrument.clone(),
                reason,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow!("market-feed actor stopped"))?;
        let grant = reply_rx
            .await
            .map_err(|_| anyhow!("market-feed actor stopped before subscribing"))??;
        Ok(SubscriptionLease {
            lease_id: grant.lease_id,
            instrument,
            reason,
            expires_at: grant.expires_at,
            commands: self.commands.clone(),
            released: false,
        })
    }

    pub fn subscribe_ticks(&self) -> broadcast::Receiver<Tick> {
        self.ticks.subscribe()
    }

    pub fn latest_ticks(&self) -> LatestTicks {
        self.latest.clone()
    }

    pub fn connection_state(&self) -> watch::Receiver<FeedConnectionState> {
        self.connection_state.clone()
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(FeedCommand::Shutdown { reply: reply_tx })
            .await
            .map_err(|_| anyhow!("market-feed actor already stopped"))?;
        reply_rx
            .await
            .map_err(|_| anyhow!("market-feed actor stopped during shutdown"))
    }
}

pub struct SubscriptionLease {
    lease_id: u64,
    instrument: ResolvedInstrument,
    reason: SubscriptionReason,
    expires_at: Option<Instant>,
    commands: mpsc::Sender<FeedCommand>,
    released: bool,
}

impl SubscriptionLease {
    pub fn instrument(&self) -> &ResolvedInstrument {
        &self.instrument
    }

    pub fn reason(&self) -> SubscriptionReason {
        self.reason
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.expires_at
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    pub async fn release(mut self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(FeedCommand::Release {
                lease_id: self.lease_id,
                reply: Some(reply_tx),
            })
            .await
            .map_err(|_| anyhow!("market-feed actor already stopped"))?;
        self.released = true;
        reply_rx
            .await
            .map_err(|_| anyhow!("market-feed actor stopped while releasing subscription"))
    }
}

impl Drop for SubscriptionLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let command = FeedCommand::Release {
            lease_id: self.lease_id,
            reply: None,
        };
        match self.commands.try_send(command) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                let commands = self.commands.clone();
                if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                    runtime.spawn(async move {
                        let _ = commands.send(command).await;
                    });
                }
            }
        }
    }
}

pub struct MarketFeedRuntime {
    pub handle: MarketFeedHandle,
    task: JoinHandle<Result<()>>,
}

impl MarketFeedRuntime {
    pub async fn join(self) -> Result<()> {
        self.task.await.context("market-feed supervisor panicked")?
    }

    pub async fn shutdown(self) -> Result<()> {
        let shutdown_result = self.handle.shutdown().await;
        let join_result = self.task.await.context("market-feed supervisor panicked")?;
        shutdown_result.and(join_result)
    }
}

/// Start the feed actor. This function performs no network I/O before return.
pub fn spawn_market_feed(
    config: MarketFeedConfig,
    token_provider: Arc<dyn AccessTokenProvider>,
) -> Result<MarketFeedRuntime> {
    config.validate()?;

    let rest_client = if let Some(rest) = &config.rest_fallback {
        Some(
            Client::builder()
                .user_agent(USER_AGENT)
                .timeout(rest.request_timeout)
                .build()
                .context("could not build REST fallback client")?,
        )
    } else {
        None
    };

    let (command_tx, command_rx) = mpsc::channel(config.command_capacity);
    let (tick_tx, _) = broadcast::channel(config.tick_broadcast_capacity);
    let latest_map = Arc::new(RwLock::new(HashMap::new()));
    let (latest_revision_tx, latest_revision_rx) = watch::channel(0u64);
    let latest = LatestTicks {
        inner: latest_map.clone(),
        revision: latest_revision_rx,
    };
    let (connection_tx, connection_rx) = watch::channel(FeedConnectionState::Idle);
    let (desired_tx, desired_rx) = watch::channel(Arc::new(HashMap::new()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let handle = MarketFeedHandle {
        commands: command_tx,
        ticks: tick_tx.clone(),
        latest,
        connection_state: connection_rx,
    };

    let coordinator_config = config.clone();
    let coordinator_shutdown = shutdown_tx.clone();
    let coordinator = tokio::spawn(run_subscription_coordinator(
        command_rx,
        desired_tx,
        coordinator_shutdown,
        coordinator_config.candidate_watch_ttl,
    ));

    let network = tokio::spawn(run_network_worker(
        config,
        token_provider,
        rest_client,
        desired_rx,
        shutdown_rx,
        tick_tx,
        latest_map,
        latest_revision_tx,
        connection_tx,
    ));

    let task = tokio::spawn(async move {
        let mut coordinator = coordinator;
        let mut network = network;
        tokio::select! {
            result = &mut coordinator => {
                let _ = shutdown_tx.send(true);
                result.context("market-feed subscription coordinator panicked")??;
                network.await.context("market-feed network worker panicked")??;
            }
            result = &mut network => {
                result.context("market-feed network worker panicked")??;
                coordinator.abort();
                match coordinator.await {
                    Ok(result) => result?,
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => return Err(error).context("market-feed coordinator panicked"),
                }
            }
        }
        Ok(())
    });

    Ok(MarketFeedRuntime { handle, task })
}

#[derive(Debug)]
enum FeedCommand {
    Acquire {
        instrument: ResolvedInstrument,
        reason: SubscriptionReason,
        reply: oneshot::Sender<Result<LeaseGrant>>,
    },
    Release {
        lease_id: u64,
        reply: Option<oneshot::Sender<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Debug)]
struct LeaseGrant {
    lease_id: u64,
    expires_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct LeaseRecord {
    instrument_key: String,
    reason: SubscriptionReason,
    expires_at: Option<Instant>,
}

#[derive(Debug)]
struct SubscriptionEntry {
    instrument: ResolvedInstrument,
    lease_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BookDelta {
    Subscribe(ResolvedInstrument),
    Unsubscribe(ResolvedInstrument),
}

#[derive(Default)]
struct SubscriptionBook {
    next_lease_id: u64,
    leases: HashMap<u64, LeaseRecord>,
    entries: HashMap<String, SubscriptionEntry>,
}

impl SubscriptionBook {
    fn acquire(
        &mut self,
        instrument: ResolvedInstrument,
        reason: SubscriptionReason,
        now: Instant,
        candidate_ttl: Duration,
    ) -> Result<(LeaseGrant, Option<BookDelta>)> {
        let instrument = instrument.normalized()?;
        let key = instrument.key();
        if let Some(existing) = self.entries.get(&key)
            && existing.instrument.security_id != instrument.security_id
        {
            bail!("conflicting instrument metadata for WebSocket code");
        }

        self.next_lease_id = self.next_lease_id.wrapping_add(1).max(1);
        while self.leases.contains_key(&self.next_lease_id) {
            self.next_lease_id = self.next_lease_id.wrapping_add(1).max(1);
        }
        let lease_id = self.next_lease_id;
        let expires_at = reason.is_temporary().then(|| now + candidate_ttl);
        let is_new_subscription = !self.entries.contains_key(&key);

        self.leases.insert(
            lease_id,
            LeaseRecord {
                instrument_key: key.clone(),
                reason,
                expires_at,
            },
        );
        self.entries
            .entry(key)
            .or_insert_with(|| SubscriptionEntry {
                instrument: instrument.clone(),
                lease_ids: Vec::new(),
            })
            .lease_ids
            .push(lease_id);

        Ok((
            LeaseGrant {
                lease_id,
                expires_at,
            },
            is_new_subscription.then(|| BookDelta::Subscribe(instrument)),
        ))
    }

    fn release(&mut self, lease_id: u64) -> Option<BookDelta> {
        let lease = self.leases.remove(&lease_id)?;
        let mut remove_entry = false;
        if let Some(entry) = self.entries.get_mut(&lease.instrument_key) {
            entry.lease_ids.retain(|existing| *existing != lease_id);
            remove_entry = entry.lease_ids.is_empty();
        }
        if remove_entry {
            self.entries
                .remove(&lease.instrument_key)
                .map(|entry| BookDelta::Unsubscribe(entry.instrument))
        } else {
            None
        }
    }

    fn expire(&mut self, now: Instant) -> Vec<BookDelta> {
        let expired = self
            .leases
            .iter()
            .filter_map(|(lease_id, lease)| {
                lease
                    .expires_at
                    .is_some_and(|deadline| deadline <= now)
                    .then_some(*lease_id)
            })
            .collect::<Vec<_>>();
        expired
            .into_iter()
            .filter_map(|lease_id| self.release(lease_id))
            .collect()
    }

    fn active(&self) -> HashMap<String, ResolvedInstrument> {
        self.entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.instrument.clone()))
            .collect()
    }

    #[cfg(test)]
    fn reason_count(&self, instrument: &ResolvedInstrument, reason: SubscriptionReason) -> usize {
        let Some(entry) = self.entries.get(&instrument.key()) else {
            return 0;
        };
        entry
            .lease_ids
            .iter()
            .filter(|lease_id| {
                self.leases
                    .get(lease_id)
                    .is_some_and(|lease| lease.reason == reason)
            })
            .count()
    }
}

async fn run_subscription_coordinator(
    mut commands: mpsc::Receiver<FeedCommand>,
    desired: watch::Sender<Arc<HashMap<String, ResolvedInstrument>>>,
    shutdown: watch::Sender<bool>,
    candidate_ttl: Duration,
) -> Result<()> {
    let mut book = SubscriptionBook::default();
    let mut expiry_sweep = interval(Duration::from_millis(100));
    expiry_sweep.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            command = commands.recv() => {
                match command {
                    Some(FeedCommand::Acquire { instrument, reason, reply }) => {
                        let result = book.acquire(
                            instrument,
                            reason,
                            Instant::now(),
                            candidate_ttl,
                        );
                        let changed = result.as_ref().is_ok_and(|(_, delta)| delta.is_some());
                        let grant = result.map(|(grant, _)| grant);
                        let _ = reply.send(grant);
                        if changed {
                            desired.send_replace(Arc::new(book.active()));
                        }
                    }
                    Some(FeedCommand::Release { lease_id, reply }) => {
                        let changed = book.release(lease_id).is_some();
                        if let Some(reply) = reply {
                            let _ = reply.send(());
                        }
                        if changed {
                            desired.send_replace(Arc::new(book.active()));
                        }
                    }
                    Some(FeedCommand::Shutdown { reply }) => {
                        desired.send_replace(Arc::new(HashMap::new()));
                        let _ = shutdown.send(true);
                        let _ = reply.send(());
                        return Ok(());
                    }
                    None => {
                        desired.send_replace(Arc::new(HashMap::new()));
                        let _ = shutdown.send(true);
                        return Ok(());
                    }
                }
            }
            _ = expiry_sweep.tick() => {
                if !book.expire(Instant::now()).is_empty() {
                    desired.send_replace(Arc::new(book.active()));
                }
            }
        }
    }
}

type PriceSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[allow(clippy::too_many_arguments)]
async fn run_network_worker(
    config: MarketFeedConfig,
    token_provider: Arc<dyn AccessTokenProvider>,
    rest_client: Option<Client>,
    mut desired: watch::Receiver<Arc<HashMap<String, ResolvedInstrument>>>,
    mut shutdown: watch::Receiver<bool>,
    tick_sender: broadcast::Sender<Tick>,
    latest: Arc<RwLock<HashMap<String, Tick>>>,
    latest_revision: watch::Sender<u64>,
    connection_state: watch::Sender<FeedConnectionState>,
) -> Result<()> {
    let mut reconnect_delay = config.reconnect_initial_delay;
    let mut fallback_token = None::<String>;
    let mut rest_schedule = config
        .rest_fallback
        .as_ref()
        .map(|settings| RestPollSchedule::new(settings, Instant::now()));

    'worker: loop {
        if *shutdown.borrow() {
            break;
        }
        if desired.borrow().is_empty() {
            connection_state.send_replace(FeedConnectionState::Idle);
            tokio::select! {
                changed = desired.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
            continue;
        }

        connection_state.send_replace(FeedConnectionState::Connecting);
        let token_future = token_provider.access_token();
        tokio::pin!(token_future);
        let token = loop {
            tokio::select! {
                result = &mut token_future => match result {
                    Ok(token) if !token.trim().is_empty() => break Some(token),
                    _ => break None,
                },
                changed = desired.changed() => {
                    if changed.is_err() {
                        break 'worker;
                    }
                    if desired.borrow().is_empty() {
                        continue 'worker;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break 'worker;
                    }
                }
            }
        };
        let Some(token) = token else {
            connection_state.send_replace(FeedConnectionState::BackingOff);
            let unauthorized = wait_for_reconnect_delay(
                reconnect_delay,
                fallback_token.as_deref(),
                &config,
                rest_client.as_ref(),
                &mut rest_schedule,
                &mut desired,
                &mut shutdown,
                &tick_sender,
                &latest,
                &latest_revision,
            )
            .await;
            if unauthorized {
                fallback_token = None;
            }
            reconnect_delay = doubled_delay(reconnect_delay, config.reconnect_maximum_delay);
            continue;
        };
        fallback_token = Some(token.clone());

        let request = match websocket_request(&config.websocket_url, &token) {
            Ok(request) => request,
            Err(_) => {
                connection_state.send_replace(FeedConnectionState::BackingOff);
                let unauthorized = wait_for_reconnect_delay(
                    reconnect_delay,
                    Some(&token),
                    &config,
                    rest_client.as_ref(),
                    &mut rest_schedule,
                    &mut desired,
                    &mut shutdown,
                    &tick_sender,
                    &latest,
                    &latest_revision,
                )
                .await;
                if unauthorized {
                    fallback_token = None;
                }
                reconnect_delay = doubled_delay(reconnect_delay, config.reconnect_maximum_delay);
                continue;
            }
        };

        let connection = connect_async(request);
        tokio::pin!(connection);
        let websocket = loop {
            tokio::select! {
                result = &mut connection => match result {
                    Ok((websocket, _)) => break Some(websocket),
                    Err(_) => break None,
                },
                changed = desired.changed() => {
                    if changed.is_err() {
                        break 'worker;
                    }
                    if desired.borrow().is_empty() {
                        continue 'worker;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break 'worker;
                    }
                }
            }
        };
        let Some(websocket) = websocket else {
            connection_state.send_replace(FeedConnectionState::BackingOff);
            let unauthorized = wait_for_reconnect_delay(
                reconnect_delay,
                Some(&token),
                &config,
                rest_client.as_ref(),
                &mut rest_schedule,
                &mut desired,
                &mut shutdown,
                &tick_sender,
                &latest,
                &latest_revision,
            )
            .await;
            if unauthorized {
                fallback_token = None;
            }
            reconnect_delay = doubled_delay(reconnect_delay, config.reconnect_maximum_delay);
            continue;
        };

        connection_state.send_replace(FeedConnectionState::Connected);
        reconnect_delay = config.reconnect_initial_delay;
        let outcome = run_connected_session(
            websocket,
            &token,
            &config,
            rest_client.as_ref(),
            &mut desired,
            &mut shutdown,
            &tick_sender,
            &latest,
            &latest_revision,
            &mut rest_schedule,
        )
        .await;

        match outcome {
            SessionOutcome::Shutdown => break,
            SessionOutcome::NoSubscriptions => continue,
            SessionOutcome::Reconnect => {
                connection_state.send_replace(FeedConnectionState::BackingOff);
                let unauthorized = wait_for_reconnect_delay(
                    reconnect_delay,
                    Some(&token),
                    &config,
                    rest_client.as_ref(),
                    &mut rest_schedule,
                    &mut desired,
                    &mut shutdown,
                    &tick_sender,
                    &latest,
                    &latest_revision,
                )
                .await;
                if unauthorized {
                    fallback_token = None;
                }
                reconnect_delay = doubled_delay(reconnect_delay, config.reconnect_maximum_delay);
            }
        }
    }

    connection_state.send_replace(FeedConnectionState::Stopped);
    Ok(())
}

fn websocket_request(
    url: &str,
    token: &str,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    let mut request = url
        .into_client_request()
        .context("invalid market-feed WebSocket URL")?;
    request.headers_mut().insert(
        "Authorization",
        token
            .parse()
            .context("access token cannot be represented as an HTTP header")?,
    );
    request.headers_mut().insert(
        "User-Agent",
        USER_AGENT.parse().expect("static user-agent is valid"),
    );
    Ok(request)
}

/// Wait for the WebSocket reconnect deadline while keeping one batched REST
/// fallback request in flight at most. Returns `true` when REST rejected the
/// cached token so the caller can stop reusing it during later token failures.
#[allow(clippy::too_many_arguments)]
async fn wait_for_reconnect_delay(
    delay: Duration,
    token: Option<&str>,
    config: &MarketFeedConfig,
    rest_client: Option<&Client>,
    rest_schedule: &mut Option<RestPollSchedule>,
    desired: &mut watch::Receiver<Arc<HashMap<String, ResolvedInstrument>>>,
    shutdown: &mut watch::Receiver<bool>,
    tick_sender: &broadcast::Sender<Tick>,
    latest: &Arc<RwLock<HashMap<String, Tick>>>,
    latest_revision: &watch::Sender<u64>,
) -> bool {
    let reconnect_deadline = sleep(delay);
    tokio::pin!(reconnect_deadline);

    let initial_rest_deadline = rest_schedule
        .as_ref()
        .map(|schedule| schedule.next_allowed_at)
        .unwrap_or_else(|| Instant::now() + delay);
    let rest_wakeup = sleep_until(initial_rest_deadline);
    tokio::pin!(rest_wakeup);

    let mut rest_tasks = JoinSet::<RestPollReport>::new();
    let mut reconnect_elapsed = false;
    let mut token_unauthorized = false;
    let mut rest_enabled = token.is_some()
        && rest_client.is_some()
        && config.rest_fallback.is_some()
        && rest_schedule.is_some();

    loop {
        if reconnect_elapsed && rest_tasks.is_empty() {
            return token_unauthorized;
        }
        if rest_enabled && rest_tasks.is_empty() && !reconnect_elapsed {
            let next_allowed_at = rest_schedule
                .as_ref()
                .expect("enabled REST fallback has a schedule")
                .next_allowed_at;
            rest_wakeup.as_mut().reset(next_allowed_at);
        }

        tokio::select! {
            _ = &mut reconnect_deadline, if !reconnect_elapsed => {
                reconnect_elapsed = true;
            }
            changed = desired.changed() => {
                if changed.is_err() || desired.borrow().is_empty() {
                    rest_tasks.abort_all();
                    return token_unauthorized;
                }
                let current = desired.borrow().clone();
                prune_latest(latest, latest_revision, &current);
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    rest_tasks.abort_all();
                    return token_unauthorized;
                }
            }
            _ = &mut rest_wakeup,
                if rest_enabled && rest_tasks.is_empty() && !reconnect_elapsed =>
            {
                let now = Instant::now();
                let schedule = rest_schedule
                    .as_mut()
                    .expect("enabled REST fallback has a schedule");
                if !schedule.is_ready(now) {
                    continue;
                }

                let settings = config
                    .rest_fallback
                    .as_ref()
                    .expect("enabled REST fallback has settings");
                let current = desired.borrow().clone();
                let due = disconnected_rest_due_instruments(
                    &current,
                    latest,
                    settings.websocket_stale_after,
                    Utc::now().timestamp_millis(),
                );
                if due.is_empty() {
                    // A recent WebSocket quote still covers every active
                    // instrument. Recheck at the normal batched cadence.
                    schedule.record_success(now);
                    continue;
                }

                let report_client = rest_client
                    .expect("enabled REST fallback has a client")
                    .clone();
                let api_base = config.rest_api_base.clone();
                let report_token = token
                    .expect("enabled REST fallback has a token")
                    .to_string();
                rest_tasks.spawn(async move {
                    poll_rest_ltp(report_client, api_base, report_token, due).await
                });
            }
            completed = rest_tasks.join_next(), if !rest_tasks.is_empty() => {
                let now = Instant::now();
                let Some(Ok(report)) = completed else {
                    rest_schedule
                        .as_mut()
                        .expect("REST task requires a schedule")
                        .record_failure(now);
                    continue;
                };

                match report.outcome {
                    RestPollOutcome::Ticks(ticks) => {
                        rest_schedule
                            .as_mut()
                            .expect("REST task requires a schedule")
                            .record_success(now);
                        for tick in ticks {
                            if desired.borrow().contains_key(&tick.instrument.key()) {
                                publish_tick(tick, tick_sender, latest, latest_revision);
                            }
                        }
                    }
                    RestPollOutcome::Unauthorized => {
                        rest_schedule
                            .as_mut()
                            .expect("REST task requires a schedule")
                            .record_failure(now);
                        token_unauthorized = true;
                        rest_enabled = false;
                    }
                    RestPollOutcome::RetryAfter(delay) => {
                        rest_schedule
                            .as_mut()
                            .expect("REST task requires a schedule")
                            .record_retry_after(now, delay);
                    }
                    RestPollOutcome::Failed => {
                        rest_schedule
                            .as_mut()
                            .expect("REST task requires a schedule")
                            .record_failure(now);
                    }
                }
            }
        }
    }
}

fn doubled_delay(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionOutcome {
    Reconnect,
    NoSubscriptions,
    Shutdown,
}

#[allow(clippy::too_many_arguments)]
async fn run_connected_session(
    mut websocket: PriceSocket,
    token: &str,
    config: &MarketFeedConfig,
    rest_client: Option<&Client>,
    desired: &mut watch::Receiver<Arc<HashMap<String, ResolvedInstrument>>>,
    shutdown: &mut watch::Receiver<bool>,
    tick_sender: &broadcast::Sender<Tick>,
    latest: &Arc<RwLock<HashMap<String, Tick>>>,
    latest_revision: &watch::Sender<u64>,
    rest_schedule: &mut Option<RestPollSchedule>,
) -> SessionOutcome {
    let mut subscribed = HashMap::<String, ResolvedInstrument>::new();
    let mut websocket_subscribed_at = HashMap::<String, Instant>::new();
    let mut last_websocket_tick = HashMap::<String, Instant>::new();
    // Drop the watch guard before awaiting. Holding `watch::Ref` across the
    // network write would make this worker future non-Send on Tokio's pool.
    let initial_desired = desired.borrow().clone();
    if sync_subscriptions(
        &mut websocket,
        &mut subscribed,
        &initial_desired,
        &mut websocket_subscribed_at,
        &mut last_websocket_tick,
    )
    .await
    .is_err()
    {
        return SessionOutcome::Reconnect;
    }

    let mut heartbeat = interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    heartbeat.tick().await;
    let mut last_server_activity = Instant::now();

    let rest_settings = config.rest_fallback.as_ref();
    let rest_poll_interval = rest_settings
        .map(|settings| settings.poll_interval)
        .unwrap_or(Duration::from_secs(24 * 60 * 60));
    let mut rest_timer = interval(rest_poll_interval);
    rest_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    rest_timer.tick().await;
    let mut rest_tasks = JoinSet::<RestPollReport>::new();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    rest_tasks.abort_all();
                    let _ = websocket.close(None).await;
                    return SessionOutcome::Shutdown;
                }
            }
            changed = desired.changed() => {
                if changed.is_err() {
                    rest_tasks.abort_all();
                    return SessionOutcome::Shutdown;
                }
                let current = desired.borrow().clone();
                if sync_subscriptions(
                    &mut websocket,
                    &mut subscribed,
                    &current,
                    &mut websocket_subscribed_at,
                    &mut last_websocket_tick,
                )
                .await
                .is_err()
                {
                    rest_tasks.abort_all();
                    return SessionOutcome::Reconnect;
                }
                prune_latest(latest, latest_revision, &current);
                if current.is_empty() {
                    rest_tasks.abort_all();
                    let _ = websocket.close(None).await;
                    return SessionOutcome::NoSubscriptions;
                }
            }
            message = websocket.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        last_server_activity = Instant::now();
                        if let Ok(Some(tick)) = parse_ltp_with_subscriptions(
                            text.as_ref(),
                            &subscribed,
                            Utc::now().timestamp_millis(),
                        ) {
                            let key = tick.instrument.key();
                            if desired.borrow().contains_key(&key) {
                                last_websocket_tick.insert(key, Instant::now());
                                publish_tick(tick, tick_sender, latest, latest_revision);
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        last_server_activity = Instant::now();
                        if websocket.send(Message::Pong(payload)).await.is_err() {
                            rest_tasks.abort_all();
                            return SessionOutcome::Reconnect;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_server_activity = Instant::now();
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                        rest_tasks.abort_all();
                        return SessionOutcome::Reconnect;
                    }
                    _ => {
                        last_server_activity = Instant::now();
                    }
                }
            }
            _ = heartbeat.tick() => {
                if last_server_activity.elapsed() > config.heartbeat_timeout
                    || websocket.send(Message::Ping(Vec::new().into())).await.is_err()
                {
                    rest_tasks.abort_all();
                    return SessionOutcome::Reconnect;
                }
            }
            _ = rest_timer.tick(), if rest_settings.is_some() && rest_tasks.is_empty() => {
                let settings = rest_settings.expect("guarded by is_some");
                let now = Instant::now();
                if rest_schedule
                    .as_ref()
                    .is_some_and(|schedule| schedule.is_ready(now))
                {
                    let due = rest_due_instruments(
                        &subscribed,
                        &websocket_subscribed_at,
                        &last_websocket_tick,
                        settings.websocket_stale_after,
                    );
                    if !due.is_empty()
                        && let Some(client) = rest_client
                    {
                        let report_client = client.clone();
                        let api_base = config.rest_api_base.clone();
                        let report_token = token.to_string();
                        rest_tasks.spawn(async move {
                            poll_rest_ltp(report_client, api_base, report_token, due).await
                        });
                    }
                }
            }
            completed = rest_tasks.join_next(), if !rest_tasks.is_empty() => {
                let Some(Ok(report)) = completed else {
                    rest_schedule
                        .as_mut()
                        .expect("REST task requires a schedule")
                        .record_failure(Instant::now());
                    continue;
                };
                match report.outcome {
                    RestPollOutcome::Ticks(ticks) => {
                        rest_schedule
                            .as_mut()
                            .expect("REST task requires a schedule")
                            .record_success(Instant::now());
                        for tick in ticks {
                            let key = tick.instrument.key();
                            let newer_websocket_tick = last_websocket_tick
                                .get(&key)
                                .is_some_and(|received| *received > report.started_at);
                            if !newer_websocket_tick && desired.borrow().contains_key(&key) {
                                publish_tick(tick, tick_sender, latest, latest_revision);
                            }
                        }
                    }
                    RestPollOutcome::Unauthorized => {
                        rest_schedule
                            .as_mut()
                            .expect("REST task requires a schedule")
                            .record_failure(Instant::now());
                        return SessionOutcome::Reconnect;
                    }
                    RestPollOutcome::RetryAfter(delay) => {
                        rest_schedule
                            .as_mut()
                            .expect("REST task requires a schedule")
                            .record_retry_after(Instant::now(), delay);
                    }
                    RestPollOutcome::Failed => {
                        rest_schedule
                            .as_mut()
                            .expect("REST task requires a schedule")
                            .record_failure(Instant::now());
                    }
                }
            }
        }
    }
}

async fn sync_subscriptions(
    websocket: &mut PriceSocket,
    subscribed: &mut HashMap<String, ResolvedInstrument>,
    desired: &HashMap<String, ResolvedInstrument>,
    subscribed_at: &mut HashMap<String, Instant>,
    last_websocket_tick: &mut HashMap<String, Instant>,
) -> Result<()> {
    let unsubscribe = subscribed
        .iter()
        .filter(|(key, _)| !desired.contains_key(*key))
        .map(|(_, instrument)| instrument.websocket_code.clone())
        .collect::<Vec<_>>();
    if !unsubscribe.is_empty() {
        send_subscription_message(websocket, "unsubscribe", unsubscribe).await?;
    }

    let subscribe = desired
        .iter()
        .filter(|(key, _)| !subscribed.contains_key(*key))
        .map(|(_, instrument)| instrument.websocket_code.clone())
        .collect::<Vec<_>>();
    if !subscribe.is_empty() {
        send_subscription_message(websocket, "subscribe", subscribe).await?;
    }

    let now = Instant::now();
    for key in desired.keys() {
        if !subscribed.contains_key(key) {
            subscribed_at.insert(key.clone(), now);
        }
    }
    subscribed_at.retain(|key, _| desired.contains_key(key));
    last_websocket_tick.retain(|key, _| desired.contains_key(key));
    subscribed.clone_from(desired);
    Ok(())
}

async fn send_subscription_message(
    websocket: &mut PriceSocket,
    action: &str,
    instruments: Vec<String>,
) -> Result<()> {
    let message = json!({
        "action": action,
        "mode": "ltp",
        "instruments": instruments,
    });
    websocket
        .send(Message::Text(message.to_string().into()))
        .await
        .context("market-feed subscription send failed")
}

fn rest_due_instruments(
    subscribed: &HashMap<String, ResolvedInstrument>,
    subscribed_at: &HashMap<String, Instant>,
    last_websocket_tick: &HashMap<String, Instant>,
    stale_after: Duration,
) -> Vec<ResolvedInstrument> {
    let now = Instant::now();
    subscribed
        .iter()
        .filter(|(key, _)| {
            let reference = last_websocket_tick
                .get(*key)
                .or_else(|| subscribed_at.get(*key));
            reference.is_none_or(|instant| now.duration_since(*instant) >= stale_after)
        })
        .take(REST_MAX_INSTRUMENTS)
        .map(|(_, instrument)| instrument.clone())
        .collect()
}

/// Select active instruments for REST while no WebSocket session exists. A
/// recently received WebSocket quote gets the configured grace period, but a
/// previous REST quote does not postpone the next scheduled REST poll.
fn disconnected_rest_due_instruments(
    desired: &HashMap<String, ResolvedInstrument>,
    latest: &Arc<RwLock<HashMap<String, Tick>>>,
    websocket_stale_after: Duration,
    now_timestamp_ms: i64,
) -> Vec<ResolvedInstrument> {
    let latest = read_lock(latest);
    desired
        .iter()
        .filter(|(key, _)| {
            !latest.get(*key).is_some_and(|tick| {
                tick.source == TickSource::WebSocket
                    && tick.age_at(now_timestamp_ms) < websocket_stale_after
            })
        })
        .take(REST_MAX_INSTRUMENTS)
        .map(|(_, instrument)| instrument.clone())
        .collect()
}

fn publish_tick(
    tick: Tick,
    tick_sender: &broadcast::Sender<Tick>,
    latest: &Arc<RwLock<HashMap<String, Tick>>>,
    latest_revision: &watch::Sender<u64>,
) {
    write_lock(latest).insert(tick.instrument.key(), tick.clone());
    let next_revision = latest_revision.borrow().wrapping_add(1);
    latest_revision.send_replace(next_revision);
    let _ = tick_sender.send(tick);
}

fn prune_latest(
    latest: &Arc<RwLock<HashMap<String, Tick>>>,
    latest_revision: &watch::Sender<u64>,
    desired: &HashMap<String, ResolvedInstrument>,
) {
    let changed = {
        let mut latest = write_lock(latest);
        let old_length = latest.len();
        latest.retain(|key, _| desired.contains_key(key));
        latest.len() != old_length
    };
    if changed {
        let next_revision = latest_revision.borrow().wrapping_add(1);
        latest_revision.send_replace(next_revision);
    }
}

struct RestPollReport {
    started_at: Instant,
    outcome: RestPollOutcome,
}

enum RestPollOutcome {
    Ticks(Vec<Tick>),
    Unauthorized,
    RetryAfter(Option<Duration>),
    Failed,
}

async fn poll_rest_ltp(
    client: Client,
    api_base: String,
    token: String,
    instruments: Vec<ResolvedInstrument>,
) -> RestPollReport {
    let started_at = Instant::now();
    let scrip_codes = instruments
        .iter()
        .map(ResolvedInstrument::rest_code)
        .collect::<Vec<_>>()
        .join(",");
    let response = client
        .get(format!(
            "{}/market/quotes/ltp",
            api_base.trim_end_matches('/')
        ))
        .header("Authorization", token)
        .query(&[("scrip-codes", scrip_codes)])
        .send()
        .await;

    let outcome = match response {
        Ok(response) if response.status().is_success() => match response.json::<Value>().await {
            Ok(body) => RestPollOutcome::Ticks(parse_rest_ltp_value(
                &body,
                &instruments,
                Utc::now().timestamp_millis(),
            )),
            Err(_) => RestPollOutcome::Failed,
        },
        Ok(response)
            if matches!(
                response.status(),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
            ) =>
        {
            RestPollOutcome::Unauthorized
        }
        Ok(response) if response.status() == StatusCode::TOO_MANY_REQUESTS => {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs);
            RestPollOutcome::RetryAfter(retry_after)
        }
        Ok(response) if response.status().is_server_error() => RestPollOutcome::RetryAfter(None),
        Ok(_) | Err(_) => RestPollOutcome::Failed,
    };

    RestPollReport {
        started_at,
        outcome,
    }
}

#[derive(Default)]
struct InstrumentResolver {
    by_websocket_code: HashMap<String, ResolvedInstrument>,
    by_security_id: HashMap<String, Option<ResolvedInstrument>>,
}

impl InstrumentResolver {
    fn from_instruments<'a>(
        instruments: impl IntoIterator<Item = &'a ResolvedInstrument>,
    ) -> Result<Self> {
        let mut resolver = Self::default();
        for instrument in instruments {
            let instrument = instrument.clone().normalized()?;
            resolver
                .by_websocket_code
                .insert(instrument.key(), instrument.clone());
            resolver
                .by_security_id
                .entry(instrument.security_id.clone())
                .and_modify(|existing| *existing = None)
                .or_insert_with(|| Some(instrument));
        }
        Ok(resolver)
    }

    fn resolve(&self, wire_instrument: &str) -> Option<ResolvedInstrument> {
        let wire_instrument = wire_instrument.trim();
        if wire_instrument.contains(':') {
            return self
                .by_websocket_code
                .get(&wire_instrument.to_ascii_uppercase())
                .cloned();
        }
        self.by_security_id.get(wire_instrument).cloned().flatten()
    }
}

/// Parse the documented INDstocks LTP WebSocket JSON. Heartbeat and unrelated
/// messages return `Ok(None)`. Bare and `SEGMENT:TOKEN` instrument forms are
/// both accepted; ambiguous bare tokens are ignored.
pub fn parse_ltp_text(
    text: &str,
    instruments: &[ResolvedInstrument],
    received_timestamp_ms: i64,
) -> Result<Option<Tick>> {
    let value: Value = serde_json::from_str(text).context("invalid market-feed JSON")?;
    let resolver = InstrumentResolver::from_instruments(instruments.iter())?;
    Ok(parse_ltp_value(&value, &resolver, received_timestamp_ms))
}

fn parse_ltp_with_subscriptions(
    text: &str,
    instruments: &HashMap<String, ResolvedInstrument>,
    received_timestamp_ms: i64,
) -> Result<Option<Tick>> {
    let value: Value = serde_json::from_str(text).context("invalid market-feed JSON")?;
    let resolver = InstrumentResolver::from_instruments(instruments.values())?;
    Ok(parse_ltp_value(&value, &resolver, received_timestamp_ms))
}

fn parse_ltp_value(
    value: &Value,
    resolver: &InstrumentResolver,
    received_timestamp_ms: i64,
) -> Option<Tick> {
    if value
        .get("mode")
        .and_then(Value::as_str)
        .is_some_and(|mode| !mode.eq_ignore_ascii_case("ltp"))
    {
        return None;
    }
    let wire_instrument = value
        .get("instrument")
        .or_else(|| value.pointer("/data/instrument"))
        .and_then(Value::as_str)?;
    let instrument = resolver.resolve(wire_instrument)?;
    let ltp = value
        .pointer("/data/ltp")
        .or_else(|| value.get("ltp"))
        .and_then(number_as_f64)?;
    if !ltp.is_finite() || ltp < 0.0 {
        return None;
    }
    let exchange_timestamp_ms = value
        .get("timestamp")
        .or_else(|| value.pointer("/data/timestamp"))
        .and_then(number_as_i64)
        .map(normalize_epoch_millis);
    Some(Tick {
        instrument,
        ltp,
        exchange_timestamp_ms,
        received_timestamp_ms,
        source: TickSource::WebSocket,
    })
}

/// Parse the documented REST `{ "data": { SCRIP: { "live_price": ... }}}`
/// response into fallback ticks. Unknown or malformed entries are skipped.
pub fn parse_rest_ltp_text(
    text: &str,
    instruments: &[ResolvedInstrument],
    received_timestamp_ms: i64,
) -> Result<Vec<Tick>> {
    let value: Value = serde_json::from_str(text).context("invalid REST LTP JSON")?;
    Ok(parse_rest_ltp_value(
        &value,
        instruments,
        received_timestamp_ms,
    ))
}

fn parse_rest_ltp_value(
    value: &Value,
    instruments: &[ResolvedInstrument],
    received_timestamp_ms: i64,
) -> Vec<Tick> {
    let by_rest_code = instruments
        .iter()
        .filter_map(|instrument| {
            instrument
                .clone()
                .normalized()
                .ok()
                .map(|instrument| (instrument.rest_code().to_ascii_uppercase(), instrument))
        })
        .collect::<HashMap<_, _>>();
    let Some(data) = value.get("data").and_then(Value::as_object) else {
        return Vec::new();
    };

    data.iter()
        .filter_map(|(rest_code, quote)| {
            let instrument = by_rest_code.get(&rest_code.to_ascii_uppercase())?;
            let ltp = quote.get("live_price").and_then(number_as_f64)?;
            if !ltp.is_finite() || ltp < 0.0 {
                return None;
            }
            Some(Tick {
                instrument: instrument.clone(),
                ltp,
                exchange_timestamp_ms: None,
                received_timestamp_ms,
                source: TickSource::RestFallback,
            })
        })
        .collect()
}

fn number_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|number| number as f64))
        .or_else(|| value.as_u64().map(|number| number as f64))
        .or_else(|| value.as_str()?.trim().parse::<f64>().ok())
}

fn number_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
        .or_else(|| value.as_str()?.trim().parse::<i64>().ok())
}

fn normalize_epoch_millis(timestamp: i64) -> i64 {
    if timestamp.abs() < 10_000_000_000 {
        timestamp.saturating_mul(1_000)
    } else {
        timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::get};

    fn sensex_put() -> ResolvedInstrument {
        ResolvedInstrument::new("BFO:847862", "847862", "SENSEX 78800 PE").unwrap()
    }

    #[test]
    fn parses_documented_bare_security_id_websocket_tick() {
        let tick = parse_ltp_text(
            r#"{"mode":"ltp","instrument":"847862","timestamp":1750138351089,"data":{"ltp":727.55}}"#,
            &[sensex_put()],
            1_750_138_351_100,
        )
        .unwrap()
        .unwrap();

        assert_eq!(tick.instrument, sensex_put());
        assert_eq!(tick.ltp, 727.55);
        assert_eq!(tick.exchange_timestamp_ms, Some(1_750_138_351_089));
        assert_eq!(tick.source, TickSource::WebSocket);
    }

    #[test]
    fn parses_prefixed_websocket_instrument_and_string_numbers() {
        let tick = parse_ltp_text(
            r#"{"mode":"ltp","instrument":"bfo:847862","timestamp":"1750138351","data":{"ltp":"728.25"}}"#,
            &[sensex_put()],
            1_750_138_352_000,
        )
        .unwrap()
        .unwrap();

        assert_eq!(tick.ltp, 728.25);
        assert_eq!(tick.exchange_timestamp_ms, Some(1_750_138_351_000));
    }

    #[test]
    fn ignores_ambiguous_bare_security_id_but_accepts_prefixed_form() {
        let bfo = sensex_put();
        let nfo = ResolvedInstrument::new("NFO:847862", "847862", "NIFTY OPTION").unwrap();
        let instruments = [bfo.clone(), nfo];

        assert!(
            parse_ltp_text(
                r#"{"mode":"ltp","instrument":"847862","data":{"ltp":100}}"#,
                &instruments,
                10,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            parse_ltp_text(
                r#"{"mode":"ltp","instrument":"BFO:847862","data":{"ltp":100}}"#,
                &instruments,
                10,
            )
            .unwrap()
            .unwrap()
            .instrument,
            bfo
        );
    }

    #[test]
    fn parses_batched_rest_fallback_ltp() {
        let ticks = parse_rest_ltp_text(
            r#"{"status":"success","data":{"BFO_847862":{"live_price":727.55},"NFO_999":{"live_price":123}}}"#,
            &[sensex_put()],
            42,
        )
        .unwrap();

        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].ltp, 727.55);
        assert_eq!(ticks[0].received_timestamp_ms, 42);
        assert_eq!(ticks[0].source, TickSource::RestFallback);
    }

    #[test]
    fn rest_schedule_preserves_cadence_and_caps_failure_backoff() {
        let settings = RestFallbackConfig {
            poll_interval: Duration::from_secs(1),
            websocket_stale_after: Duration::from_secs(2),
            request_timeout: Duration::from_secs(1),
            maximum_backoff: Duration::from_secs(4),
        };
        let start = Instant::now();
        let mut schedule = RestPollSchedule::new(&settings, start);

        assert!(schedule.is_ready(start));
        schedule.record_success(start);
        assert_eq!(schedule.next_allowed_at, start + Duration::from_secs(1));

        let first_failure = start + Duration::from_secs(1);
        schedule.record_failure(first_failure);
        assert_eq!(
            schedule.next_allowed_at,
            first_failure + Duration::from_secs(1)
        );

        let second_failure = start + Duration::from_secs(2);
        schedule.record_failure(second_failure);
        assert_eq!(
            schedule.next_allowed_at,
            second_failure + Duration::from_secs(2)
        );

        schedule.record_retry_after(
            start + Duration::from_secs(4),
            Some(Duration::from_secs(30)),
        );
        assert_eq!(schedule.next_allowed_at, start + Duration::from_secs(8));
        assert_eq!(schedule.failure_delay, Duration::from_secs(4));
    }

    #[test]
    fn disconnected_fallback_only_honors_fresh_websocket_grace() {
        let instrument = sensex_put();
        let desired = HashMap::from([(instrument.key(), instrument.clone())]);
        let latest = Arc::new(RwLock::new(HashMap::new()));
        let now_ms = 10_000;

        assert_eq!(
            disconnected_rest_due_instruments(&desired, &latest, Duration::from_secs(2), now_ms,),
            vec![instrument.clone()]
        );

        write_lock(&latest).insert(
            instrument.key(),
            Tick {
                instrument: instrument.clone(),
                ltp: 100.0,
                exchange_timestamp_ms: None,
                received_timestamp_ms: now_ms - 1_999,
                source: TickSource::WebSocket,
            },
        );
        assert!(
            disconnected_rest_due_instruments(&desired, &latest, Duration::from_secs(2), now_ms,)
                .is_empty()
        );

        write_lock(&latest)
            .get_mut(&instrument.key())
            .unwrap()
            .received_timestamp_ms = now_ms - 2_000;
        assert_eq!(
            disconnected_rest_due_instruments(&desired, &latest, Duration::from_secs(2), now_ms,),
            vec![instrument.clone()]
        );

        {
            let mut latest = write_lock(&latest);
            let fallback_tick = latest.get_mut(&instrument.key()).unwrap();
            fallback_tick.source = TickSource::RestFallback;
            fallback_tick.received_timestamp_ms = now_ms;
        }
        assert_eq!(
            disconnected_rest_due_instruments(&desired, &latest, Duration::from_secs(2), now_ms,),
            vec![instrument]
        );
    }

    #[tokio::test]
    async fn rest_fallback_publishes_while_websocket_handshake_is_backing_off() {
        let app = Router::new().route(
            "/market/quotes/ltp",
            get(|| async {
                Json(json!({
                    "status": "success",
                    "data": {"BFO_847862": {"live_price": 731.25}}
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let config = MarketFeedConfig {
            // This HTTP route deliberately rejects the WebSocket upgrade, so
            // the worker remains in its reconnect phase while REST is healthy.
            websocket_url: format!("ws://{address}/not-a-websocket"),
            rest_api_base: format!("http://{address}"),
            candidate_watch_ttl: Duration::from_secs(10),
            command_capacity: 8,
            tick_broadcast_capacity: 8,
            heartbeat_interval: Duration::from_secs(1),
            heartbeat_timeout: Duration::from_secs(2),
            reconnect_initial_delay: Duration::from_secs(2),
            reconnect_maximum_delay: Duration::from_secs(2),
            rest_fallback: Some(RestFallbackConfig {
                poll_interval: Duration::from_secs(1),
                websocket_stale_after: Duration::from_secs(1),
                request_timeout: Duration::from_secs(1),
                maximum_backoff: Duration::from_secs(2),
            }),
        };
        let runtime = spawn_market_feed(
            config,
            token_provider_fn(|| async { Ok("test-token".to_string()) }),
        )
        .unwrap();
        let mut ticks = runtime.handle.subscribe_ticks();
        let lease = runtime
            .handle
            .subscribe(sensex_put(), SubscriptionReason::PendingOrder)
            .await
            .unwrap();

        let tick = tokio::time::timeout(Duration::from_secs(3), ticks.recv())
            .await
            .expect("REST fallback did not publish during WebSocket backoff")
            .unwrap();
        assert_eq!(tick.instrument, sensex_put());
        assert_eq!(tick.ltp, 731.25);
        assert_eq!(tick.source, TickSource::RestFallback);

        lease.release().await.unwrap();
        runtime.shutdown().await.unwrap();
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn reference_counts_persistent_leases_before_unsubscribing() {
        let instrument = sensex_put();
        let now = Instant::now();
        let mut book = SubscriptionBook::default();
        let (pending, first_delta) = book
            .acquire(
                instrument.clone(),
                SubscriptionReason::PendingOrder,
                now,
                Duration::from_secs(10),
            )
            .unwrap();
        let (open, second_delta) = book
            .acquire(
                instrument.clone(),
                SubscriptionReason::OpenPosition,
                now,
                Duration::from_secs(10),
            )
            .unwrap();

        assert_eq!(first_delta, Some(BookDelta::Subscribe(instrument.clone())));
        assert_eq!(second_delta, None);
        assert_eq!(book.release(pending.lease_id), None);
        assert_eq!(
            book.reason_count(&instrument, SubscriptionReason::OpenPosition),
            1
        );
        assert_eq!(
            book.release(open.lease_id),
            Some(BookDelta::Unsubscribe(instrument))
        );
    }

    #[test]
    fn candidate_expiry_keeps_persistent_reference_active() {
        let instrument = sensex_put();
        let now = Instant::now();
        let mut book = SubscriptionBook::default();
        let (_candidate, first_delta) = book
            .acquire(
                instrument.clone(),
                SubscriptionReason::CandidateWatch,
                now,
                Duration::from_secs(10),
            )
            .unwrap();
        let (pending, second_delta) = book
            .acquire(
                instrument.clone(),
                SubscriptionReason::PendingOrder,
                now,
                Duration::from_secs(10),
            )
            .unwrap();

        assert!(first_delta.is_some());
        assert_eq!(second_delta, None);
        assert!(book.expire(now + Duration::from_secs(11)).is_empty());
        assert_eq!(
            book.reason_count(&instrument, SubscriptionReason::CandidateWatch),
            0
        );
        assert_eq!(
            book.reason_count(&instrument, SubscriptionReason::PendingOrder),
            1
        );
        assert_eq!(
            book.release(pending.lease_id),
            Some(BookDelta::Unsubscribe(instrument))
        );
    }
}
