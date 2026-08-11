//! Bounded, timestamped media capture for a live YouTube stream.
//!
//! The module owns one long-running FFmpeg ingest process. FFmpeg writes closed
//! five-second MPEG-TS segments; every four consecutive segments are remuxed
//! into one 20-second MP4. Callers must acknowledge segment and window events.
//! A segment is only removed after it has been acknowledged by the caller and
//! has also been consumed by the internal window assembler.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    fs::File,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    fs,
    process::{Child, Command},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{MissedTickBehavior, interval, timeout},
};

pub const SEGMENT_SECONDS: u64 = 5;
pub const WINDOW_SEGMENTS: usize = 4;
pub const WINDOW_SECONDS: u64 = SEGMENT_SECONDS * WINDOW_SEGMENTS as u64;
pub const DEFAULT_CLIP_RETENTION: usize = 3;
pub const GEMINI_INLINE_LIMIT_BYTES: u64 = 20 * 1024 * 1024;
const STALE_CAPTURE_GRACE: Duration = Duration::from_secs(WINDOW_SECONDS * 2);
const CAPTURE_LOCK_FILE: &str = ".capture.lock";

/// Runtime limits and executable locations for the capture worker.
#[derive(Clone, Debug)]
pub struct CaptureConfig {
    /// Root for the private segment sessions and published `clips` directory.
    pub output_dir: PathBuf,
    pub yt_dlp_path: PathBuf,
    pub ffmpeg_path: PathBuf,
    pub poll_interval: Duration,
    pub resolver_timeout: Duration,
    pub packaging_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub event_send_timeout: Duration,
    pub event_queue_capacity: usize,
    pub control_queue_capacity: usize,
    pub max_segments_per_poll: usize,
    pub max_inflight_segments: usize,
    pub max_inflight_windows: usize,
    pub clip_retention: usize,
    pub max_clip_bytes: u64,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("data/media"),
            yt_dlp_path: PathBuf::from("yt-dlp"),
            ffmpeg_path: PathBuf::from("ffmpeg"),
            poll_interval: Duration::from_millis(100),
            resolver_timeout: Duration::from_secs(30),
            packaging_timeout: Duration::from_secs(20),
            shutdown_timeout: Duration::from_secs(5),
            event_send_timeout: Duration::from_secs(2),
            event_queue_capacity: 8,
            control_queue_capacity: 32,
            max_segments_per_poll: 8,
            max_inflight_segments: 24,
            max_inflight_windows: 8,
            clip_retention: DEFAULT_CLIP_RETENTION,
            max_clip_bytes: GEMINI_INLINE_LIMIT_BYTES,
        }
    }
}

impl CaptureConfig {
    fn validate(&self) -> Result<()> {
        if self.output_dir.as_os_str().is_empty() {
            bail!("capture output directory cannot be empty");
        }
        if self.yt_dlp_path.as_os_str().is_empty() || self.ffmpeg_path.as_os_str().is_empty() {
            bail!("yt-dlp and ffmpeg executable paths cannot be empty");
        }
        if self.poll_interval.is_zero()
            || self.resolver_timeout.is_zero()
            || self.packaging_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
            || self.event_send_timeout.is_zero()
        {
            bail!("capture timeouts and polling interval must be non-zero");
        }
        if self.event_queue_capacity == 0
            || self.control_queue_capacity == 0
            || self.max_segments_per_poll == 0
            || self.max_inflight_segments < WINDOW_SEGMENTS
            || self.max_inflight_windows == 0
            || self.clip_retention == 0
            || self.max_clip_bytes == 0
        {
            bail!("capture queue, in-flight, retention, and size limits must be positive");
        }
        Ok(())
    }
}

/// A closed, immutable five-second audio/video segment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaSegment {
    pub id: String,
    pub sequence: u64,
    pub path: PathBuf,
    pub started_at_utc: DateTime<Utc>,
    pub ended_at_utc: DateTime<Utc>,
    pub duration_ms: u64,
    pub size_bytes: u64,
}

/// Four consecutive segments published as an optimized 20-second MP4.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaWindow {
    pub id: String,
    pub sequence: u64,
    pub path: PathBuf,
    pub segments: Vec<MediaSegment>,
    pub started_at_utc: DateTime<Utc>,
    pub ended_at_utc: DateTime<Utc>,
    pub created_at_utc: DateTime<Utc>,
    pub duration_ms: u64,
    pub size_bytes: u64,
    /// False means the fallback compression could not bring the clip below the
    /// configured inline-upload limit. The caller must not upload it inline.
    pub inline_upload_safe: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureStopReason {
    Requested,
    EndOfStream,
    ControllerDropped,
}

/// Bounded output stream from the capture worker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum CaptureEvent {
    SegmentReady(MediaSegment),
    WindowReady(MediaWindow),
    Fault {
        at_utc: DateTime<Utc>,
        message: String,
    },
    Stopped {
        at_utc: DateTime<Utc>,
        reason: CaptureStopReason,
    },
}

#[derive(Clone)]
pub struct CaptureController {
    commands: mpsc::Sender<CaptureCommand>,
}

impl fmt::Debug for CaptureController {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureController").finish_non_exhaustive()
    }
}

impl CaptureController {
    /// Acknowledge that all external users of this five-second segment (for
    /// example STT) have finished reading it.
    pub async fn acknowledge_segment(&self, segment_id: impl Into<String>) -> Result<()> {
        self.commands
            .send(CaptureCommand::AcknowledgeSegment(segment_id.into()))
            .await
            .map_err(|_| anyhow!("capture worker is no longer running"))
    }

    /// Acknowledge that downstream analysis has finished reading a 20-second
    /// MP4. Old acknowledged windows are eligible for latest-N rotation.
    pub async fn acknowledge_window(&self, window_id: impl Into<String>) -> Result<()> {
        self.commands
            .send(CaptureCommand::AcknowledgeWindow(window_id.into()))
            .await
            .map_err(|_| anyhow!("capture worker is no longer running"))
    }

    pub async fn request_shutdown(&self) -> Result<()> {
        self.commands
            .send(CaptureCommand::Shutdown(None))
            .await
            .map_err(|_| anyhow!("capture worker is no longer running"))
    }

    fn try_request_shutdown(&self) {
        let _ = self.commands.try_send(CaptureCommand::Shutdown(None));
    }
}

/// Owns the event receiver and worker lifetime. Dropping it aborts the worker;
/// FFmpeg has `kill_on_drop` enabled so a detached capture process is not left
/// behind.
pub struct CaptureSession {
    pub session_id: String,
    pub events: mpsc::Receiver<CaptureEvent>,
    controller: CaptureController,
    task: Option<JoinHandle<Result<()>>>,
    shutdown_timeout: Duration,
}

impl fmt::Debug for CaptureSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureSession")
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl CaptureSession {
    /// Resolve the public YouTube page without shell interpolation, then start
    /// a single long-running FFmpeg ingest process.
    pub async fn start(mut config: CaptureConfig, youtube_url: &str) -> Result<Self> {
        config.validate()?;
        validate_source_url(youtube_url)?;

        if config.output_dir.is_relative() {
            config.output_dir = std::env::current_dir()
                .context("cannot resolve current directory")?
                .join(&config.output_dir);
        }
        fs::create_dir_all(&config.output_dir)
            .await
            .with_context(|| {
                format!(
                    "cannot create capture directory {}",
                    config.output_dir.display()
                )
            })?;

        let resolved = resolve_stream(&config, youtube_url).await?;
        // All media below one output root is managed as one retention domain.
        // Holding this lock prevents another current capture worker from having
        // its writable segment or unacknowledged clip mistaken for stale data.
        let root_lock = acquire_capture_root_lock(&config.output_dir)?;
        prepare_capture_directories(&config.output_dir, config.clip_retention).await?;
        let capture_started_at_utc = Utc::now();
        let session_id = capture_started_at_utc
            .format("capture_%Y%m%dT%H%M%S%3fZ")
            .to_string();
        let segment_dir = config.output_dir.join("segments").join(&session_id);
        let clips_dir = config.output_dir.join("clips");
        fs::create_dir_all(&segment_dir).await.with_context(|| {
            format!("cannot create segment directory {}", segment_dir.display())
        })?;
        fs::create_dir_all(&clips_dir)
            .await
            .with_context(|| format!("cannot create clips directory {}", clips_dir.display()))?;

        let child = spawn_capture_process(&config, &resolved, &segment_dir)?;
        let (event_tx, events) = mpsc::channel(config.event_queue_capacity);
        let (command_tx, command_rx) = mpsc::channel(config.control_queue_capacity);
        let controller = CaptureController {
            commands: command_tx,
        };
        let worker_session_id = session_id.clone();
        let shutdown_timeout = config.shutdown_timeout;
        let task = tokio::spawn(run_capture_worker(
            config,
            worker_session_id,
            capture_started_at_utc,
            segment_dir,
            clips_dir,
            root_lock,
            child,
            event_tx,
            command_rx,
        ));

        Ok(Self {
            session_id,
            events,
            controller,
            task: Some(task),
            shutdown_timeout,
        })
    }

    pub fn controller(&self) -> CaptureController {
        self.controller.clone()
    }

    pub async fn next_event(&mut self) -> Option<CaptureEvent> {
        self.events.recv().await
    }

    /// Gracefully stop FFmpeg, then wait for the worker. If it fails to stop in
    /// time the task is aborted, which still kills the child through RAII.
    pub async fn shutdown(mut self) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        let _ = self
            .controller
            .commands
            .send(CaptureCommand::Shutdown(Some(done_tx)))
            .await;

        let _ = timeout(self.shutdown_timeout, done_rx).await;
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        match timeout(self.shutdown_timeout, &mut task).await {
            Ok(joined) => joined.context("capture worker task panicked")?,
            Err(_) => {
                task.abort();
                let _ = task.await;
                bail!("capture worker did not stop within the shutdown timeout");
            }
        }
    }
}

impl Drop for CaptureSession {
    fn drop(&mut self) {
        self.controller.try_request_shutdown();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Debug)]
enum CaptureCommand {
    AcknowledgeSegment(String),
    AcknowledgeWindow(String),
    Shutdown(Option<oneshot::Sender<()>>),
}

struct CaptureRootLock {
    _file: File,
    #[cfg(not(windows))]
    path: PathBuf,
}

#[cfg(windows)]
fn acquire_capture_root_lock(output_dir: &Path) -> Result<CaptureRootLock> {
    use std::os::windows::fs::OpenOptionsExt;

    let path = output_dir.join(CAPTURE_LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        // An active capture owns the full media tree. Denying every sharing
        // mode makes a second process fail closed before it can prune files.
        .share_mode(0)
        .open(&path)
        .with_context(|| {
            format!(
                "capture output {} is already owned by another worker",
                output_dir.display()
            )
        })?;
    Ok(CaptureRootLock { _file: file })
}

#[cfg(not(windows))]
fn acquire_capture_root_lock(output_dir: &Path) -> Result<CaptureRootLock> {
    let path = output_dir.join(CAPTURE_LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| {
            format!(
                "capture output {} is already owned by another worker",
                output_dir.display()
            )
        })?;
    Ok(CaptureRootLock { _file: file, path })
}

impl Drop for CaptureRootLock {
    fn drop(&mut self) {
        #[cfg(not(windows))]
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

struct ResolvedStream {
    media_url: String,
    user_agent: Option<String>,
    referer: Option<String>,
}

impl fmt::Debug for ResolvedStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedStream")
            .field("media_url", &"<redacted ephemeral URL>")
            .field("has_user_agent", &self.user_agent.is_some())
            .field("has_referer", &self.referer.is_some())
            .finish()
    }
}

fn validate_source_url(url: &str) -> Result<()> {
    let trimmed = url.trim();
    if trimmed != url || trimmed.contains(['\r', '\n']) {
        bail!("stream URL contains surrounding whitespace or a newline");
    }
    if !(trimmed.starts_with("https://") || trimmed.starts_with("http://")) {
        bail!("stream URL must use http or https");
    }
    Ok(())
}

async fn resolve_stream(config: &CaptureConfig, youtube_url: &str) -> Result<ResolvedStream> {
    let get_url_args = [
        "--no-config",
        "--no-playlist",
        "--no-warnings",
        "--format",
        "best[height<=720]/best",
        "--get-url",
        "--",
        youtube_url,
    ];
    if let Ok(output) = run_yt_dlp(config, &get_url_args).await
        && output.status.success()
        && let Some(url) = first_http_line(&output.stdout)
    {
        return Ok(ResolvedStream {
            media_url: url,
            user_agent: None,
            referer: None,
        });
    }

    // JSON fallback supplies the same selected URL plus the small set of
    // non-secret headers FFmpeg occasionally needs for a manifest.
    let json_args = [
        "--no-config",
        "--no-playlist",
        "--no-warnings",
        "--format",
        "best[height<=720]/best",
        "--dump-single-json",
        "--",
        youtube_url,
    ];
    let output = run_yt_dlp(config, &json_args).await?;
    if !output.status.success() {
        bail!(
            "yt-dlp could not resolve the livestream (status {})",
            output.status
        );
    }
    let value: Value =
        serde_json::from_slice(&output.stdout).context("yt-dlp returned invalid resolver JSON")?;
    let media_url = value
        .get("url")
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("requested_downloads")
                .and_then(Value::as_array)
                .and_then(|downloads| downloads.first())
                .and_then(|download| download.get("url"))
                .and_then(Value::as_str)
        })
        .filter(|url| url.starts_with("https://") || url.starts_with("http://"))
        .ok_or_else(|| anyhow!("yt-dlp resolver JSON contained no playable URL"))?
        .to_owned();
    let headers = value.get("http_headers").and_then(Value::as_object);
    let header = |name: &str| {
        headers.and_then(|map| {
            map.iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .and_then(|(_, value)| value.as_str())
                .map(str::to_owned)
        })
    };
    Ok(ResolvedStream {
        media_url,
        user_agent: header("user-agent"),
        referer: header("referer"),
    })
}

async fn run_yt_dlp(config: &CaptureConfig, args: &[&str]) -> Result<std::process::Output> {
    let mut command = Command::new(&config.yt_dlp_path);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        // Resolver errors can contain signed manifest URLs. Keep them out of
        // application logs and return a bounded generic failure instead.
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let child = command.spawn().with_context(|| {
        format!(
            "cannot start yt-dlp executable {}",
            config.yt_dlp_path.display()
        )
    })?;
    timeout(config.resolver_timeout, child.wait_with_output())
        .await
        .map_err(|_| anyhow!("yt-dlp resolution timed out"))?
        .context("cannot wait for yt-dlp resolver")
}

fn first_http_line(stdout: &[u8]) -> Option<String> {
    String::from_utf8_lossy(stdout)
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("https://") || line.starts_with("http://"))
        .map(str::to_owned)
}

fn spawn_capture_process(
    config: &CaptureConfig,
    stream: &ResolvedStream,
    segment_dir: &Path,
) -> Result<Child> {
    let output_pattern = segment_dir.join("segment_%09d.ts");
    let mut command = Command::new(&config.ffmpeg_path);
    command
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-nostdin")
        .arg("-y")
        .arg("-rw_timeout")
        .arg("15000000")
        .arg("-reconnect")
        .arg("1")
        .arg("-reconnect_streamed")
        .arg("1")
        .arg("-reconnect_delay_max")
        .arg("2");
    if let Some(user_agent) = &stream.user_agent {
        command.arg("-user_agent").arg(user_agent);
    }
    if let Some(referer) = &stream.referer {
        command.arg("-referer").arg(referer);
    }
    command
        .arg("-thread_queue_size")
        .arg("512")
        .arg("-i")
        .arg(&stream.media_url)
        .arg("-map")
        .arg("0:v:0")
        .arg("-map")
        .arg("0:a:0")
        .arg("-vf")
        .arg("fps=5,scale=-2:720:force_original_aspect_ratio=decrease:flags=fast_bilinear,format=yuv420p")
        .arg("-vsync")
        .arg("cfr")
        .arg("-c:v")
        .arg("libx264")
        .arg("-preset")
        .arg("veryfast")
        .arg("-tune")
        .arg("zerolatency")
        .arg("-crf")
        .arg("30")
        .arg("-maxrate")
        .arg("700k")
        .arg("-bufsize")
        .arg("1400k")
        .arg("-g")
        .arg("25")
        .arg("-keyint_min")
        .arg("25")
        .arg("-sc_threshold")
        .arg("0")
        .arg("-force_key_frames")
        .arg("expr:gte(t,n_forced*5)")
        .arg("-c:a")
        .arg("aac")
        // Supports the older FFmpeg build already present on this machine.
        .arg("-strict")
        .arg("-2")
        .arg("-b:a")
        .arg("48k")
        .arg("-ac")
        .arg("1")
        .arg("-ar")
        .arg("16000")
        .arg("-af")
        .arg("aresample=async=1:first_pts=0")
        .arg("-f")
        .arg("segment")
        .arg("-segment_format")
        .arg("mpegts")
        .arg("-segment_time")
        .arg(SEGMENT_SECONDS.to_string())
        .arg("-segment_time_delta")
        .arg("0.05")
        .arg("-reset_timestamps")
        .arg("1")
        .arg(output_pattern)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        // Never leak the signed direct media URL through FFmpeg diagnostics.
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command.spawn().with_context(|| {
        format!(
            "cannot start ffmpeg executable {}",
            config.ffmpeg_path.display()
        )
    })
}

struct SegmentLifecycle {
    media: MediaSegment,
    externally_acknowledged: bool,
    assembled: bool,
}

struct WindowLifecycle {
    media: MediaWindow,
    acknowledged: bool,
}

struct WorkerState {
    config: CaptureConfig,
    session_id: String,
    capture_started_at_utc: DateTime<Utc>,
    segment_dir: PathBuf,
    clips_dir: PathBuf,
    /// Kept open for the entire worker lifetime. On Windows the handle denies
    /// sharing, making the output tree a single-writer retention domain.
    _root_lock: CaptureRootLock,
    next_segment: u64,
    next_window: u64,
    grouper: SegmentGrouper,
    segments: HashMap<String, SegmentLifecycle>,
    windows: HashMap<String, WindowLifecycle>,
}

enum WorkerExit {
    Stopped {
        reason: CaptureStopReason,
        reply: Option<oneshot::Sender<()>>,
    },
}

async fn run_capture_worker(
    config: CaptureConfig,
    session_id: String,
    capture_started_at_utc: DateTime<Utc>,
    segment_dir: PathBuf,
    clips_dir: PathBuf,
    root_lock: CaptureRootLock,
    mut child: Child,
    events: mpsc::Sender<CaptureEvent>,
    mut commands: mpsc::Receiver<CaptureCommand>,
) -> Result<()> {
    let mut state = WorkerState {
        config,
        session_id,
        capture_started_at_utc,
        segment_dir,
        clips_dir,
        _root_lock: root_lock,
        next_segment: 0,
        next_window: 0,
        grouper: SegmentGrouper::default(),
        segments: HashMap::new(),
        windows: HashMap::new(),
    };
    let result = capture_loop(&mut state, &mut child, &events, &mut commands).await;
    let stop_result = stop_child(&mut child, state.config.shutdown_timeout).await;
    let teardown_result = match stop_result {
        Ok(()) => state.cleanup_after_stop().await,
        Err(error) => Err(error),
    };

    match result {
        Ok(WorkerExit::Stopped { reason, reply }) => {
            if let Some(reply) = reply {
                let _ = reply.send(());
            }
            let _ = send_event(
                &events,
                CaptureEvent::Stopped {
                    at_utc: Utc::now(),
                    reason,
                },
                state.config.event_send_timeout,
            )
            .await;
            teardown_result
        }
        Err(error) => {
            let _ = send_event(
                &events,
                CaptureEvent::Fault {
                    at_utc: Utc::now(),
                    message: format!("capture pipeline stopped: {error:#}"),
                },
                state.config.event_send_timeout,
            )
            .await;
            let _ = teardown_result;
            Err(error)
        }
    }
}

async fn capture_loop(
    state: &mut WorkerState,
    child: &mut Child,
    events: &mpsc::Sender<CaptureEvent>,
    commands: &mut mpsc::Receiver<CaptureCommand>,
) -> Result<WorkerExit> {
    let mut poller = interval(state.config.poll_interval);
    poller.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            command = commands.recv() => {
                match command {
                    Some(CaptureCommand::AcknowledgeSegment(id)) => {
                        state.acknowledge_segment(&id).await?;
                    }
                    Some(CaptureCommand::AcknowledgeWindow(id)) => {
                        state.acknowledge_window(&id).await?;
                    }
                    Some(CaptureCommand::Shutdown(reply)) => {
                        return Ok(WorkerExit::Stopped {
                            reason: CaptureStopReason::Requested,
                            reply,
                        });
                    }
                    None => {
                        return Ok(WorkerExit::Stopped {
                            reason: CaptureStopReason::ControllerDropped,
                            reply: None,
                        });
                    }
                }
            }
            _ = poller.tick() => {
                if let Some(status) = child.try_wait().context("cannot poll ffmpeg capture process")? {
                    state.publish_closed_segments(events).await?;
                    if status.success() {
                        return Ok(WorkerExit::Stopped {
                            reason: CaptureStopReason::EndOfStream,
                            reply: None,
                        });
                    }
                    bail!("ffmpeg capture process exited with status {status}");
                }
                state.publish_closed_segments(events).await?;
            }
        }
    }
}

impl WorkerState {
    async fn publish_closed_segments(&mut self, events: &mpsc::Sender<CaptureEvent>) -> Result<()> {
        for _ in 0..self.config.max_segments_per_poll {
            let current_path = segment_path(&self.segment_dir, self.next_segment);
            let following_path = segment_path(&self.segment_dir, self.next_segment + 1);
            // FFmpeg opens the next file only after closing the current muxer.
            // The newest file is intentionally never published while writable.
            if fs::metadata(&following_path).await.is_err() {
                break;
            }
            let metadata = fs::metadata(&current_path).await.with_context(|| {
                format!("ffmpeg skipped expected segment {}", current_path.display())
            })?;
            if metadata.len() == 0 {
                bail!("ffmpeg closed an empty media segment");
            }
            if self.segments.len() >= self.config.max_inflight_segments {
                bail!(
                    "capture reached the in-flight segment limit; downstream acknowledgements are too slow"
                );
            }

            let sequence = self.next_segment;
            // FFmpeg may spend time opening a live manifest after it is
            // spawned. Use the first output file's creation time as the UTC
            // media anchor when the filesystem exposes it, rather than
            // attributing that connection delay to the first segment.
            if sequence == 0
                && let Ok(created) = metadata.created()
            {
                self.capture_started_at_utc = DateTime::<Utc>::from(created);
            }
            let start_offset = i64::try_from(sequence)
                .ok()
                .and_then(|value| value.checked_mul(SEGMENT_SECONDS as i64))
                .ok_or_else(|| anyhow!("segment timestamp overflow"))?;
            let started_at_utc =
                self.capture_started_at_utc + ChronoDuration::seconds(start_offset);
            let ended_at_utc = started_at_utc + ChronoDuration::seconds(SEGMENT_SECONDS as i64);
            let id = format!("{}:{sequence:09}", self.session_id);
            let segment = MediaSegment {
                id: id.clone(),
                sequence,
                path: current_path,
                started_at_utc,
                ended_at_utc,
                duration_ms: SEGMENT_SECONDS * 1_000,
                size_bytes: metadata.len(),
            };
            self.segments.insert(
                id,
                SegmentLifecycle {
                    media: segment.clone(),
                    externally_acknowledged: false,
                    assembled: false,
                },
            );
            self.next_segment += 1;

            send_event(
                events,
                CaptureEvent::SegmentReady(segment.clone()),
                self.config.event_send_timeout,
            )
            .await?;

            let grouping = self.grouper.push(segment);
            for abandoned in grouping.abandoned {
                self.mark_assembled_and_maybe_delete(&abandoned.id).await?;
            }
            if let Some(group) = grouping.complete {
                self.publish_window(group, events).await?;
            }
        }
        Ok(())
    }

    async fn publish_window(
        &mut self,
        segments: Vec<MediaSegment>,
        events: &mpsc::Sender<CaptureEvent>,
    ) -> Result<()> {
        let inflight_windows = self
            .windows
            .values()
            .filter(|lifecycle| !lifecycle.acknowledged)
            .count();
        if inflight_windows >= self.config.max_inflight_windows {
            bail!(
                "capture reached the in-flight window limit; downstream acknowledgements are too slow"
            );
        }
        let window = build_window(
            &self.config,
            &self.session_id,
            self.next_window,
            &self.segment_dir,
            &self.clips_dir,
            &segments,
        )
        .await?;
        self.next_window += 1;
        for segment in &segments {
            self.mark_assembled_and_maybe_delete(&segment.id).await?;
        }
        self.windows.insert(
            window.id.clone(),
            WindowLifecycle {
                media: window.clone(),
                acknowledged: false,
            },
        );
        self.prune_clips().await?;
        send_event(
            events,
            CaptureEvent::WindowReady(window),
            self.config.event_send_timeout,
        )
        .await
    }

    async fn acknowledge_segment(&mut self, id: &str) -> Result<()> {
        if let Some(lifecycle) = self.segments.get_mut(id) {
            lifecycle.externally_acknowledged = true;
        }
        self.maybe_delete_segment(id).await
    }

    async fn acknowledge_window(&mut self, id: &str) -> Result<()> {
        if let Some(lifecycle) = self.windows.get_mut(id) {
            lifecycle.acknowledged = true;
        }
        self.prune_clips().await
    }

    async fn mark_assembled_and_maybe_delete(&mut self, id: &str) -> Result<()> {
        if let Some(lifecycle) = self.segments.get_mut(id) {
            lifecycle.assembled = true;
        }
        self.maybe_delete_segment(id).await
    }

    async fn maybe_delete_segment(&mut self, id: &str) -> Result<()> {
        let removable = self
            .segments
            .get(id)
            .filter(|lifecycle| lifecycle.assembled && lifecycle.externally_acknowledged)
            .map(|lifecycle| lifecycle.media.path.clone());
        let Some(path) = removable else {
            return Ok(());
        };
        ensure_direct_child(&path, &self.segment_dir)?;
        match fs::remove_file(&path).await {
            Ok(()) => {
                self.segments.remove(id);
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.segments.remove(id);
                Ok(())
            }
            Err(error) => Err(error)
                .with_context(|| format!("cannot remove acknowledged segment {}", path.display())),
        }
    }

    async fn cleanup_after_stop(&mut self) -> Result<()> {
        // Only unacknowledged segment events may still be held by downstream
        // readers. Everything else in this now-stopped session is disposable,
        // including FFmpeg's last never-published TS file.
        let protected_segments: HashSet<PathBuf> = self
            .segments
            .values()
            .filter(|lifecycle| !lifecycle.externally_acknowledged)
            .map(|lifecycle| lifecycle.media.path.clone())
            .collect();
        cleanup_stopped_session_directory(&self.segment_dir, &protected_segments).await?;

        // Acknowledged clips remain a global latest-N set. Unacknowledged
        // final clips are retained until their consumers acknowledge them (or
        // until a later startup establishes that this session is stale).
        self.prune_clips().await?;
        cleanup_stale_clip_artifacts(&self.clips_dir, STALE_CAPTURE_GRACE).await?;
        if let Some(segments_root) = self.segment_dir.parent() {
            cleanup_stale_segment_sessions(segments_root, STALE_CAPTURE_GRACE).await?;
        }
        Ok(())
    }

    async fn prune_clips(&mut self) -> Result<()> {
        let protected: HashSet<PathBuf> = self
            .windows
            .values()
            .filter(|lifecycle| !lifecycle.acknowledged)
            .map(|lifecycle| lifecycle.media.path.clone())
            .collect();
        let deleted =
            prune_acknowledged_clips(&self.clips_dir, &protected, self.config.clip_retention)
                .await?;
        if !deleted.is_empty() {
            self.windows
                .retain(|_, lifecycle| !deleted.contains(&lifecycle.media.path));
        }
        Ok(())
    }
}

async fn cleanup_stopped_session_directory(
    directory: &Path,
    protected_segments: &HashSet<PathBuf>,
) -> Result<()> {
    let mut entries = match fs::read_dir(directory).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("cannot scan stopped session {}", directory.display()));
        }
    };
    while let Some(entry) = entries
        .next_entry()
        .await
        .context("cannot enumerate stopped session artifacts")?
    {
        let file_type = entry
            .file_type()
            .await
            .context("cannot inspect stopped session artifact type")?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !file_type.is_file()
            || !is_managed_session_artifact(name)
            || protected_segments.contains(&entry.path())
        {
            continue;
        }
        ensure_direct_child(&entry.path(), directory)?;
        match fs::remove_file(entry.path()).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "cannot remove stopped-session artifact {}",
                        entry.path().display()
                    )
                });
            }
        }
    }
    drop(entries);

    // remove_dir is intentionally non-recursive: any protected or unexpected
    // file keeps the session directory in place for a future conservative
    // stale cleanup pass.
    match fs::remove_dir(directory).await {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error)
            .with_context(|| format!("cannot remove stopped session {}", directory.display())),
    }
}

async fn prepare_capture_directories(output_dir: &Path, clip_retention: usize) -> Result<()> {
    let segments_dir = output_dir.join("segments");
    let clips_dir = output_dir.join("clips");
    fs::create_dir_all(&segments_dir).await.with_context(|| {
        format!(
            "cannot create capture segments directory {}",
            segments_dir.display()
        )
    })?;
    fs::create_dir_all(&clips_dir).await.with_context(|| {
        format!(
            "cannot create capture clips directory {}",
            clips_dir.display()
        )
    })?;

    cleanup_stale_segment_sessions(&segments_dir, STALE_CAPTURE_GRACE).await?;
    cleanup_stale_clip_artifacts(&clips_dir, STALE_CAPTURE_GRACE).await?;
    prune_acknowledged_clips(&clips_dir, &HashSet::new(), clip_retention).await?;
    Ok(())
}

async fn cleanup_stale_segment_sessions(root: &Path, minimum_age: Duration) -> Result<()> {
    let mut entries = fs::read_dir(root)
        .await
        .with_context(|| format!("cannot scan segment sessions directory {}", root.display()))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .context("cannot enumerate segment sessions")?
    {
        let file_type = entry
            .file_type()
            .await
            .context("cannot inspect segment session type")?;
        if !file_type.is_dir() || file_type.is_symlink() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !is_managed_session_name(name)
            || !session_directory_is_stale_and_managed(&entry.path(), minimum_age).await?
        {
            continue;
        }
        ensure_direct_child(&entry.path(), root)?;
        fs::remove_dir_all(entry.path()).await.with_context(|| {
            format!(
                "cannot remove stale capture session {}",
                entry.path().display()
            )
        })?;
    }
    Ok(())
}

async fn session_directory_is_stale_and_managed(
    directory: &Path,
    minimum_age: Duration,
) -> Result<bool> {
    let directory_metadata = fs::metadata(directory)
        .await
        .with_context(|| format!("cannot inspect capture session {}", directory.display()))?;
    if !metadata_is_old_enough(&directory_metadata, minimum_age) {
        return Ok(false);
    }

    let mut entries = fs::read_dir(directory)
        .await
        .with_context(|| format!("cannot scan capture session {}", directory.display()))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .context("cannot enumerate capture session artifacts")?
    {
        let file_type = entry
            .file_type()
            .await
            .context("cannot inspect capture session artifact type")?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Ok(false);
        };
        // Refuse broad recursive deletion if anything not generated by this
        // module appears in the directory.
        if !file_type.is_file() || !is_managed_session_artifact(name) {
            return Ok(false);
        }
        let metadata = entry
            .metadata()
            .await
            .context("cannot inspect capture session artifact")?;
        if !metadata_is_old_enough(&metadata, minimum_age) {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn cleanup_stale_clip_artifacts(directory: &Path, minimum_age: Duration) -> Result<()> {
    let mut entries = fs::read_dir(directory)
        .await
        .with_context(|| format!("cannot scan clips directory {}", directory.display()))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .context("cannot enumerate clip artifacts")?
    {
        let file_type = entry
            .file_type()
            .await
            .context("cannot inspect clip artifact type")?;
        if !file_type.is_file() || file_type.is_symlink() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let metadata = entry
            .metadata()
            .await
            .context("cannot inspect clip artifact")?;
        let is_partial = is_partial_clip_name(name);
        let is_empty_completed = is_completed_clip_name(name) && metadata.len() == 0;
        if !(is_partial || is_empty_completed) || !metadata_is_old_enough(&metadata, minimum_age) {
            continue;
        }
        ensure_direct_child(&entry.path(), directory)?;
        fs::remove_file(entry.path()).await.with_context(|| {
            format!(
                "cannot remove stale clip artifact {}",
                entry.path().display()
            )
        })?;
    }
    Ok(())
}

fn metadata_is_old_enough(metadata: &std::fs::Metadata, minimum_age: Duration) -> bool {
    if minimum_age.is_zero() {
        return true;
    }
    metadata
        .modified()
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age >= minimum_age)
}

fn is_managed_session_name(name: &str) -> bool {
    name.strip_prefix("capture_")
        .is_some_and(is_capture_timestamp)
}

fn is_capture_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 19
        && bytes[8] == b'T'
        && bytes[18] == b'Z'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 8 | 18) || byte.is_ascii_digit())
}

fn is_managed_session_artifact(name: &str) -> bool {
    is_numbered_name(name, "segment_", 9, ".ts")
        || is_numbered_name(name, "window_", 6, ".ffconcat")
}

fn is_numbered_name(name: &str, prefix: &str, digits: usize, suffix: &str) -> bool {
    name.strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(suffix))
        .is_some_and(|number| {
            number.len() == digits && number.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn is_completed_clip_name(name: &str) -> bool {
    is_clip_name_with_suffix(name, ".mp4")
}

fn is_partial_clip_name(name: &str) -> bool {
    is_clip_name_with_suffix(name, "_partial.mp4") || is_clip_name_with_suffix(name, "_compact.mp4")
}

fn is_clip_name_with_suffix(name: &str, suffix: &str) -> bool {
    let Some(body) = name
        .strip_prefix("window_")
        .and_then(|value| value.strip_suffix(suffix))
    else {
        return false;
    };
    let Some((timestamp, sequence)) = body.split_once('_') else {
        return false;
    };
    is_capture_timestamp(timestamp)
        && sequence.len() == 6
        && sequence.bytes().all(|byte| byte.is_ascii_digit())
}

fn segment_path(directory: &Path, sequence: u64) -> PathBuf {
    directory.join(format!("segment_{sequence:09}.ts"))
}

fn ensure_direct_child(path: &Path, expected_parent: &Path) -> Result<()> {
    if path.parent() != Some(expected_parent) {
        bail!("refusing to remove a media file outside its managed directory");
    }
    Ok(())
}

#[derive(Default)]
struct SegmentGrouper {
    pending: VecDeque<MediaSegment>,
}

struct GroupingOutcome {
    abandoned: Vec<MediaSegment>,
    complete: Option<Vec<MediaSegment>>,
}

impl SegmentGrouper {
    fn push(&mut self, segment: MediaSegment) -> GroupingOutcome {
        let is_contiguous = self.pending.back().is_none_or(|previous| {
            previous.sequence.checked_add(1) == Some(segment.sequence)
                && previous.ended_at_utc == segment.started_at_utc
        });
        let abandoned = if is_contiguous {
            Vec::new()
        } else {
            self.pending.drain(..).collect()
        };
        self.pending.push_back(segment);
        let complete = if self.pending.len() == WINDOW_SEGMENTS {
            Some(self.pending.drain(..).collect())
        } else {
            None
        };
        GroupingOutcome {
            abandoned,
            complete,
        }
    }
}

async fn build_window(
    config: &CaptureConfig,
    session_id: &str,
    sequence: u64,
    segment_dir: &Path,
    clips_dir: &Path,
    segments: &[MediaSegment],
) -> Result<MediaWindow> {
    if segments.len() != WINDOW_SEGMENTS {
        bail!("a media window requires exactly four segments");
    }
    for pair in segments.windows(2) {
        if pair[0].sequence.checked_add(1) != Some(pair[1].sequence)
            || pair[0].ended_at_utc != pair[1].started_at_utc
        {
            bail!("cannot package non-consecutive media segments");
        }
    }
    for segment in segments {
        ensure_direct_child(&segment.path, segment_dir)?;
    }

    let started_at_utc = segments[0].started_at_utc;
    let ended_at_utc = segments[WINDOW_SEGMENTS - 1].ended_at_utc;
    let timestamp = started_at_utc.format("%Y%m%dT%H%M%S%3fZ");
    let id = format!("{session_id}:window:{sequence:06}");
    let final_path = clips_dir.join(format!("window_{timestamp}_{sequence:06}.mp4"));
    let partial_path = clips_dir.join(format!("window_{timestamp}_{sequence:06}_partial.mp4"));
    let compact_path = clips_dir.join(format!("window_{timestamp}_{sequence:06}_compact.mp4"));
    let manifest_path = segment_dir.join(format!("window_{sequence:06}.ffconcat"));

    let mut manifest = String::from("ffconcat version 1.0\n");
    for segment in segments {
        let file_name = segment
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("segment filename is not valid UTF-8"))?;
        // Segment names are generated internally and never contain quotes.
        manifest.push_str(&format!("file '{file_name}'\n"));
    }
    fs::write(&manifest_path, manifest)
        .await
        .with_context(|| format!("cannot write concat manifest {}", manifest_path.display()))?;

    let package_result = package_with_ffmpeg(config, &manifest_path, &partial_path).await;
    let _ = fs::remove_file(&manifest_path).await;
    if let Err(error) = package_result {
        let _ = fs::remove_file(&partial_path).await;
        return Err(error);
    }
    let mut size_bytes = fs::metadata(&partial_path)
        .await
        .with_context(|| format!("cannot inspect packaged clip {}", partial_path.display()))?
        .len();

    if size_bytes > config.max_clip_bytes
        && compact_with_ffmpeg(config, &partial_path, &compact_path)
            .await
            .is_ok()
    {
        let compact_size = fs::metadata(&compact_path)
            .await
            .with_context(|| format!("cannot inspect compact clip {}", compact_path.display()))?
            .len();
        if compact_size < size_bytes {
            fs::remove_file(&partial_path).await.with_context(|| {
                format!("cannot replace oversized clip {}", partial_path.display())
            })?;
            fs::rename(&compact_path, &partial_path)
                .await
                .with_context(|| {
                    format!("cannot publish compact clip {}", partial_path.display())
                })?;
            size_bytes = compact_size;
        } else {
            let _ = fs::remove_file(&compact_path).await;
        }
    }
    let _ = fs::remove_file(&compact_path).await;
    fs::rename(&partial_path, &final_path)
        .await
        .with_context(|| format!("cannot publish completed clip {}", final_path.display()))?;

    Ok(MediaWindow {
        id,
        sequence,
        path: final_path,
        segments: segments.to_vec(),
        started_at_utc,
        ended_at_utc,
        created_at_utc: Utc::now(),
        duration_ms: WINDOW_SECONDS * 1_000,
        size_bytes,
        inline_upload_safe: size_bytes <= config.max_clip_bytes,
    })
}

async fn package_with_ffmpeg(
    config: &CaptureConfig,
    manifest_path: &Path,
    output_path: &Path,
) -> Result<()> {
    let mut command = Command::new(&config.ffmpeg_path);
    command
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-nostdin")
        .arg("-y")
        .arg("-f")
        .arg("concat")
        .arg("-safe")
        .arg("0")
        .arg("-i")
        .arg(manifest_path)
        .arg("-map")
        .arg("0:v:0")
        .arg("-map")
        .arg("0:a:0")
        .arg("-c")
        .arg("copy")
        .arg("-bsf:a")
        .arg("aac_adtstoasc")
        .arg("-movflags")
        .arg("+faststart")
        .arg(output_path);
    run_bounded_ffmpeg(config, command, "20-second clip packaging").await
}

async fn compact_with_ffmpeg(
    config: &CaptureConfig,
    input_path: &Path,
    output_path: &Path,
) -> Result<()> {
    let mut command = Command::new(&config.ffmpeg_path);
    command
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-nostdin")
        .arg("-y")
        .arg("-i")
        .arg(input_path)
        .arg("-vf")
        .arg("fps=5,scale=-2:720:force_original_aspect_ratio=decrease:flags=fast_bilinear,format=yuv420p")
        .arg("-c:v")
        .arg("libx264")
        .arg("-preset")
        .arg("veryfast")
        .arg("-crf")
        .arg("32")
        .arg("-maxrate")
        .arg("500k")
        .arg("-bufsize")
        .arg("1000k")
        .arg("-c:a")
        .arg("aac")
        .arg("-strict")
        .arg("-2")
        .arg("-b:a")
        .arg("40k")
        .arg("-movflags")
        .arg("+faststart")
        .arg(output_path);
    run_bounded_ffmpeg(config, command, "oversized clip compression").await
}

async fn run_bounded_ffmpeg(
    config: &CaptureConfig,
    mut command: Command,
    operation: &str,
) -> Result<()> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command.spawn().with_context(|| {
        format!(
            "cannot start ffmpeg for {operation} using {}",
            config.ffmpeg_path.display()
        )
    })?;
    match timeout(config.packaging_timeout, child.wait()).await {
        Ok(status) => {
            let status = status.with_context(|| format!("cannot wait for ffmpeg {operation}"))?;
            if !status.success() {
                bail!("ffmpeg {operation} failed with status {status}");
            }
            Ok(())
        }
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            bail!("ffmpeg {operation} timed out");
        }
    }
}

async fn stop_child(child: &mut Child, wait: Duration) -> Result<()> {
    if child
        .try_wait()
        .context("cannot inspect ffmpeg process during shutdown")?
        .is_some()
    {
        return Ok(());
    }
    child
        .start_kill()
        .context("cannot request ffmpeg process shutdown")?;
    timeout(wait, child.wait())
        .await
        .map_err(|_| anyhow!("ffmpeg did not exit within the shutdown timeout"))?
        .context("cannot wait for ffmpeg shutdown")?;
    Ok(())
}

async fn send_event(
    sender: &mpsc::Sender<CaptureEvent>,
    event: CaptureEvent,
    wait: Duration,
) -> Result<()> {
    timeout(wait, sender.send(event))
        .await
        .map_err(|_| anyhow!("capture event queue remained full beyond its time limit"))?
        .map_err(|_| anyhow!("capture event receiver was dropped"))
}

async fn completed_clip_paths(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    let mut entries = fs::read_dir(directory)
        .await
        .with_context(|| format!("cannot scan clips directory {}", directory.display()))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .context("cannot enumerate completed clips")?
    {
        let file_type = entry
            .file_type()
            .await
            .context("cannot inspect completed clip type")?;
        if !file_type.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if is_completed_clip_name(&name) {
            paths.push(entry.path());
        }
    }
    Ok(paths)
}

async fn prune_acknowledged_clips(
    directory: &Path,
    unacknowledged: &HashSet<PathBuf>,
    keep_latest: usize,
) -> Result<HashSet<PathBuf>> {
    let completed = completed_clip_paths(directory).await?;
    let acknowledged: Vec<PathBuf> = completed
        .into_iter()
        .filter(|path| !unacknowledged.contains(path))
        .collect();
    let mut deleted = HashSet::new();
    for path in select_clip_retention_deletions(&acknowledged, keep_latest) {
        ensure_direct_child(&path, directory)?;
        match fs::remove_file(&path).await {
            Ok(()) => {
                deleted.insert(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                deleted.insert(path);
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("cannot rotate completed clip {}", path.display()));
            }
        }
    }
    Ok(deleted)
}

/// Return all completed clip paths older than the newest `keep_latest` paths.
/// Capture filenames begin with a fixed-width UTC timestamp, so lexical order
/// is chronological. The input is not mutated.
pub fn select_clip_retention_deletions(
    completed_paths: &[PathBuf],
    keep_latest: usize,
) -> Vec<PathBuf> {
    let mut sorted = completed_paths.to_vec();
    sorted.sort_by(|left, right| {
        left.file_name()
            .cmp(&right.file_name())
            .then_with(|| left.cmp(right))
    });
    let delete_count = sorted.len().saturating_sub(keep_latest);
    sorted.truncate(delete_count);
    sorted
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn test_directory(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "market_manager_capture_{label}_{}_{}",
            std::process::id(),
            NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn clip_path(directory: &Path, timestamp: &str, sequence: u64) -> PathBuf {
        directory.join(format!("window_{timestamp}_{sequence:06}.mp4"))
    }

    fn segment(sequence: u64, base: DateTime<Utc>) -> MediaSegment {
        let started_at_utc = base + ChronoDuration::seconds((sequence * SEGMENT_SECONDS) as i64);
        MediaSegment {
            id: format!("test:{sequence}"),
            sequence,
            path: PathBuf::from(format!("segment_{sequence:09}.ts")),
            started_at_utc,
            ended_at_utc: started_at_utc + ChronoDuration::seconds(SEGMENT_SECONDS as i64),
            duration_ms: SEGMENT_SECONDS * 1_000,
            size_bytes: 1,
        }
    }

    #[test]
    fn groups_exactly_four_consecutive_non_overlapping_segments() {
        let base = DateTime::parse_from_rfc3339("2026-08-11T03:16:40Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut grouper = SegmentGrouper::default();
        for sequence in 0..3 {
            let result = grouper.push(segment(sequence, base));
            assert!(result.complete.is_none());
            assert!(result.abandoned.is_empty());
        }
        let first = grouper.push(segment(3, base)).complete.unwrap();
        assert_eq!(
            first.iter().map(|item| item.sequence).collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(first[0].started_at_utc, base);
        assert_eq!(
            first[3].ended_at_utc,
            base + ChronoDuration::seconds(WINDOW_SECONDS as i64)
        );

        for sequence in 4..8 {
            let result = grouper.push(segment(sequence, base));
            if sequence == 7 {
                assert_eq!(
                    result
                        .complete
                        .unwrap()
                        .iter()
                        .map(|item| item.sequence)
                        .collect::<Vec<_>>(),
                    vec![4, 5, 6, 7]
                );
            } else {
                assert!(result.complete.is_none());
            }
        }
    }

    #[test]
    fn a_sequence_gap_drops_only_the_incomplete_group() {
        let base = DateTime::parse_from_rfc3339("2026-08-11T03:16:40Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut grouper = SegmentGrouper::default();
        assert!(grouper.push(segment(0, base)).complete.is_none());
        assert!(grouper.push(segment(1, base)).complete.is_none());

        let gap = grouper.push(segment(3, base));
        assert_eq!(
            gap.abandoned
                .iter()
                .map(|item| item.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(gap.complete.is_none());
        for sequence in 4..7 {
            let result = grouper.push(segment(sequence, base));
            if sequence == 6 {
                assert_eq!(
                    result
                        .complete
                        .unwrap()
                        .iter()
                        .map(|item| item.sequence)
                        .collect::<Vec<_>>(),
                    vec![3, 4, 5, 6]
                );
            }
        }
    }

    #[test]
    fn retention_selects_everything_except_latest_three() {
        let paths = vec![
            PathBuf::from("clips/window_20260811T031720000Z_000002.mp4"),
            PathBuf::from("clips/window_20260811T031640000Z_000000.mp4"),
            PathBuf::from("clips/window_20260811T031800000Z_000004.mp4"),
            PathBuf::from("clips/window_20260811T031700000Z_000001.mp4"),
            PathBuf::from("clips/window_20260811T031740000Z_000003.mp4"),
        ];
        assert_eq!(
            select_clip_retention_deletions(&paths, 3),
            vec![
                PathBuf::from("clips/window_20260811T031640000Z_000000.mp4"),
                PathBuf::from("clips/window_20260811T031700000Z_000001.mp4"),
            ]
        );
    }

    #[test]
    fn retention_handles_fewer_clips_and_zero_keep() {
        let paths = vec![PathBuf::from("clips/window_20260811T031640000Z_000000.mp4")];
        assert!(select_clip_retention_deletions(&paths, 3).is_empty());
        assert_eq!(select_clip_retention_deletions(&paths, 0), paths);
    }

    #[test]
    fn generated_media_names_are_matched_strictly() {
        assert!(is_managed_session_name("capture_20260811T031640000Z"));
        assert!(!is_managed_session_name("capture_latest"));
        assert!(is_managed_session_artifact("segment_000000123.ts"));
        assert!(is_managed_session_artifact("window_000123.ffconcat"));
        assert!(!is_managed_session_artifact("notes.txt"));
        assert!(is_completed_clip_name(
            "window_20260811T031640000Z_000123.mp4"
        ));
        assert!(is_partial_clip_name(
            "window_20260811T031640000Z_000123_partial.mp4"
        ));
        assert!(!is_completed_clip_name(
            "window_20260811T031640000Z_000123_partial.mp4"
        ));
        assert!(!is_partial_clip_name("unrelated_partial.mp4"));
    }

    #[tokio::test]
    async fn retention_counts_only_acknowledged_clips() {
        let directory = test_directory("acknowledged_retention");
        fs::create_dir_all(&directory).await.unwrap();
        let timestamps = [
            "20260811T031640000Z",
            "20260811T031700000Z",
            "20260811T031720000Z",
            "20260811T031740000Z",
            "20260811T031800000Z",
        ];
        let paths: Vec<PathBuf> = timestamps
            .iter()
            .enumerate()
            .map(|(sequence, timestamp)| clip_path(&directory, timestamp, sequence as u64))
            .collect();
        for path in &paths {
            fs::write(path, b"clip").await.unwrap();
        }

        let protected = HashSet::from([paths[4].clone()]);
        let deleted = prune_acknowledged_clips(&directory, &protected, 3)
            .await
            .unwrap();
        assert_eq!(deleted, HashSet::from([paths[0].clone()]));
        assert!(fs::try_exists(&paths[4]).await.unwrap());
        assert_eq!(completed_clip_paths(&directory).await.unwrap().len(), 4);

        let deleted = prune_acknowledged_clips(&directory, &HashSet::new(), 3)
            .await
            .unwrap();
        assert_eq!(deleted, HashSet::from([paths[1].clone()]));
        assert_eq!(completed_clip_paths(&directory).await.unwrap().len(), 3);
        fs::remove_dir_all(&directory).await.unwrap();
    }

    #[tokio::test]
    async fn stopped_session_cleanup_preserves_unacknowledged_segment() {
        let root = test_directory("stopped_session");
        let session = root.join("capture_20260811T031640000Z");
        fs::create_dir_all(&session).await.unwrap();
        let acknowledged = session.join("segment_000000000.ts");
        let unacknowledged = session.join("segment_000000001.ts");
        let unpublished = session.join("segment_000000002.ts");
        let manifest = session.join("window_000000.ffconcat");
        for path in [&acknowledged, &unacknowledged, &unpublished, &manifest] {
            fs::write(path, b"media").await.unwrap();
        }

        cleanup_stopped_session_directory(&session, &HashSet::from([unacknowledged.clone()]))
            .await
            .unwrap();
        assert!(!fs::try_exists(&acknowledged).await.unwrap());
        assert!(fs::try_exists(&unacknowledged).await.unwrap());
        assert!(!fs::try_exists(&unpublished).await.unwrap());
        assert!(!fs::try_exists(&manifest).await.unwrap());
        assert!(fs::try_exists(&session).await.unwrap());

        cleanup_stopped_session_directory(&session, &HashSet::new())
            .await
            .unwrap();
        assert!(!fs::try_exists(&session).await.unwrap());
        fs::remove_dir_all(&root).await.unwrap();
    }

    #[tokio::test]
    async fn acknowledgement_removes_an_already_assembled_ts_immediately() {
        let root = test_directory("segment_ack");
        let segment_dir = root.join("segments").join("capture_20260811T031640000Z");
        let clips_dir = root.join("clips");
        fs::create_dir_all(&segment_dir).await.unwrap();
        fs::create_dir_all(&clips_dir).await.unwrap();
        let root_lock = acquire_capture_root_lock(&root).unwrap();
        let base = DateTime::parse_from_rfc3339("2026-08-11T03:16:40Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut media = segment(0, base);
        media.id = "capture_20260811T031640000Z:000000000".to_owned();
        media.path = segment_dir.join("segment_000000000.ts");
        fs::write(&media.path, b"media").await.unwrap();
        let id = media.id.clone();
        let mut state = WorkerState {
            config: CaptureConfig {
                output_dir: root.clone(),
                ..CaptureConfig::default()
            },
            session_id: "capture_20260811T031640000Z".to_owned(),
            capture_started_at_utc: base,
            segment_dir,
            clips_dir,
            _root_lock: root_lock,
            next_segment: 1,
            next_window: 0,
            grouper: SegmentGrouper::default(),
            segments: HashMap::from([(
                id.clone(),
                SegmentLifecycle {
                    media: media.clone(),
                    externally_acknowledged: false,
                    assembled: true,
                },
            )]),
            windows: HashMap::new(),
        };

        state.acknowledge_segment(&id).await.unwrap();
        assert!(!fs::try_exists(&media.path).await.unwrap());
        assert!(!state.segments.contains_key(&id));
        drop(state);
        fs::remove_dir_all(&root).await.unwrap();
    }

    #[tokio::test]
    async fn stale_cleanup_is_scoped_to_generated_artifacts() {
        let root = test_directory("stale_cleanup");
        let segments = root.join("segments");
        let clips = root.join("clips");
        let stale_session = segments.join("capture_20260811T031640000Z");
        let foreign_session = segments.join("capture_20260811T031700000Z");
        fs::create_dir_all(&stale_session).await.unwrap();
        fs::create_dir_all(&foreign_session).await.unwrap();
        fs::create_dir_all(&clips).await.unwrap();
        fs::write(stale_session.join("segment_000000000.ts"), b"old")
            .await
            .unwrap();
        fs::write(foreign_session.join("do-not-delete.txt"), b"user")
            .await
            .unwrap();
        let partial = clips.join("window_20260811T031640000Z_000000_partial.mp4");
        let unrelated = clips.join("unrelated_partial.mp4");
        fs::write(&partial, b"partial").await.unwrap();
        fs::write(&unrelated, b"user").await.unwrap();

        cleanup_stale_segment_sessions(&segments, Duration::ZERO)
            .await
            .unwrap();
        cleanup_stale_clip_artifacts(&clips, Duration::ZERO)
            .await
            .unwrap();
        assert!(!fs::try_exists(&stale_session).await.unwrap());
        assert!(fs::try_exists(&foreign_session).await.unwrap());
        assert!(!fs::try_exists(&partial).await.unwrap());
        assert!(fs::try_exists(&unrelated).await.unwrap());
        fs::remove_dir_all(&root).await.unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn output_lock_rejects_a_second_live_owner() {
        let directory = test_directory("root_lock");
        std::fs::create_dir_all(&directory).unwrap();
        let first = acquire_capture_root_lock(&directory).unwrap();
        assert!(acquire_capture_root_lock(&directory).is_err());
        drop(first);
        let second = acquire_capture_root_lock(&directory).unwrap();
        drop(second);
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
