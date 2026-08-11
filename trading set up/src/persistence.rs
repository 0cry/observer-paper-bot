//! Crash-conscious persistence primitives for the paper engine and dashboard.
//!
//! The module deliberately stays independent of trading-domain types. Events,
//! snapshots, and closed trades only need to implement Serde's traits.

use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Serialize, de::DeserializeOwned};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Controls when an append writer flushes userspace buffers and asks the OS to
/// durably sync file data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Keep data buffered until [`JsonlEventWriter::flush`],
    /// [`JsonlEventWriter::sync`], or `finish` is called.
    Manual,
    /// Flush after every record so dashboard readers can see it immediately.
    FlushEveryEvent,
    /// Flush and sync file data after every record.
    SyncEveryEvent,
    /// Flush after every record and sync file data after each batch.
    SyncEveryEvents(NonZeroUsize),
}

impl SyncPolicy {
    fn flushes_each_event(self) -> bool {
        !matches!(self, Self::Manual)
    }

    fn should_sync(self, unsynced_events: usize) -> bool {
        match self {
            Self::SyncEveryEvent => true,
            Self::SyncEveryEvents(batch) => unsynced_events >= batch.get(),
            Self::Manual | Self::FlushEveryEvent => false,
        }
    }
}

/// Single-writer append-only JSON Lines event log.
///
/// Each value is serialized fully in memory before any bytes are appended, so
/// serialization failures cannot create half a record. An OS/process crash may
/// still leave one incomplete final write; [`replay_jsonl`] reports and ignores
/// exactly that final unterminated fragment.
pub struct JsonlEventWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    policy: SyncPolicy,
    unsynced_events: usize,
}

impl JsonlEventWriter {
    pub fn open(path: impl AsRef<Path>, policy: SyncPolicy) -> Result<Self> {
        let path = path.as_ref();
        ensure_parent(path)?;

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .with_context(|| format!("could not open JSONL event log {}", path.display()))?;

        validate_jsonl_append_boundary(&mut file, path)?;

        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            policy,
            unsynced_events: 0,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append<T: Serialize>(&mut self, event: &T) -> Result<()> {
        // Serialize first. `serde_json::to_writer(&mut self.writer, ...)` could
        // leave a partial line if a custom Serialize implementation fails.
        let encoded = serde_json::to_vec(event).context("could not serialize JSONL event")?;
        self.writer
            .write_all(&encoded)
            .and_then(|_| self.writer.write_all(b"\n"))
            .with_context(|| format!("could not append JSONL event to {}", self.path.display()))?;
        self.unsynced_events = self.unsynced_events.saturating_add(1);

        if self.policy.flushes_each_event() {
            self.flush()?;
        }
        if self.policy.should_sync(self.unsynced_events) {
            self.sync()?;
        }
        Ok(())
    }

    /// Flush userspace buffers without requesting durable storage.
    pub fn flush(&mut self) -> Result<()> {
        self.writer
            .flush()
            .with_context(|| format!("could not flush JSONL event log {}", self.path.display()))
    }

    /// Flush and sync all event-log file data accumulated so far.
    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        self.writer
            .get_ref()
            .sync_data()
            .with_context(|| format!("could not sync JSONL event log {}", self.path.display()))?;
        self.unsynced_events = 0;
        Ok(())
    }

    /// Finish the writer with a durable sync. Use this instead of relying on
    /// `Drop`, whose flush error cannot be reported.
    pub fn finish(mut self) -> Result<()> {
        self.sync()
    }
}

impl Drop for JsonlEventWriter {
    fn drop(&mut self) {
        // Best effort only. Call `finish` when errors must be observed.
        let _ = self.writer.flush();
    }
}

/// Details about an ignored, incomplete final JSONL record. Raw bytes are not
/// retained so logs cannot accidentally expose event payloads or credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncatedFinalLine {
    pub line_number: u64,
    pub byte_offset: u64,
    pub byte_len: usize,
    pub parse_error: String,
}

/// Metadata produced while replaying a JSONL log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplaySummary {
    pub records_replayed: u64,
    pub lines_seen: u64,
    pub bytes_read: u64,
    /// Offset immediately after the last valid record. This is informational;
    /// this module never truncates an event log automatically.
    pub last_valid_offset: u64,
    pub truncated_final_line: Option<TruncatedFinalLine>,
}

/// Collected replay result for callers that want all restored records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport<T> {
    pub records: Vec<T>,
    pub summary: ReplaySummary,
}

/// Replay valid JSONL records into a callback without retaining unbounded
/// history in memory.
pub fn replay_jsonl_with<T, F>(path: impl AsRef<Path>, mut on_record: F) -> Result<ReplaySummary>
where
    T: DeserializeOwned,
    F: FnMut(T) -> Result<()>,
{
    let path = path.as_ref();
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ReplaySummary::default());
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("could not open JSONL event log {}", path.display()));
        }
    };

    let mut reader = BufReader::new(file);
    let mut summary = ReplaySummary::default();
    let mut line = Vec::new();

    loop {
        line.clear();
        let byte_offset = summary.bytes_read;
        let bytes = reader
            .read_until(b'\n', &mut line)
            .with_context(|| format!("could not read JSONL event log {}", path.display()))?;
        if bytes == 0 {
            break;
        }

        summary.bytes_read = summary.bytes_read.saturating_add(bytes as u64);
        summary.lines_seen = summary.lines_seen.saturating_add(1);
        let terminated = line.last() == Some(&b'\n');
        if terminated {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }

        match serde_json::from_slice::<T>(&line) {
            Ok(record) => {
                on_record(record).with_context(|| {
                    format!(
                        "JSONL replay callback failed at line {} of {}",
                        summary.lines_seen,
                        path.display()
                    )
                })?;
                summary.records_replayed = summary.records_replayed.saturating_add(1);
                summary.last_valid_offset = summary.bytes_read;
            }
            Err(error) if !terminated => {
                summary.truncated_final_line = Some(TruncatedFinalLine {
                    line_number: summary.lines_seen,
                    byte_offset,
                    byte_len: bytes,
                    parse_error: error.to_string(),
                });
                break;
            }
            Err(error) => {
                bail!(
                    "invalid JSONL record at line {} of {}: {}",
                    summary.lines_seen,
                    path.display(),
                    error
                );
            }
        }
    }

    Ok(summary)
}

/// Replay and collect all valid JSONL records.
///
/// For large logs, prefer [`replay_jsonl_with`] and fold directly into engine
/// state so replay memory remains bounded.
pub fn replay_jsonl<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<ReplayReport<T>> {
    let mut records = Vec::new();
    let summary = replay_jsonl_with(path, |record| {
        records.push(record);
        Ok(())
    })?;
    Ok(ReplayReport { records, summary })
}

/// Atomically replace a JSON snapshot by writing and syncing a same-directory
/// temporary file before `rename`.
pub fn atomic_write_json_snapshot<T: Serialize>(
    path: impl AsRef<Path>,
    snapshot: &T,
) -> Result<()> {
    let path = path.as_ref();
    ensure_parent(path)?;

    let (temp_path, temp_file) = create_same_directory_temp(path)?;
    let mut pending = PendingTempFile::new(temp_path);
    let mut writer = BufWriter::new(temp_file);
    serde_json::to_writer(&mut writer, snapshot).with_context(|| {
        format!(
            "could not serialize temporary snapshot {}",
            pending.path().display()
        )
    })?;
    writer
        .write_all(b"\n")
        .and_then(|_| writer.flush())
        .with_context(|| {
            format!(
                "could not write temporary snapshot {}",
                pending.path().display()
            )
        })?;
    writer.get_ref().sync_all().with_context(|| {
        format!(
            "could not sync temporary snapshot {}",
            pending.path().display()
        )
    })?;
    drop(writer);

    fs::rename(pending.path(), path).with_context(|| {
        format!(
            "could not atomically replace snapshot {} with {}",
            path.display(),
            pending.path().display()
        )
    })?;
    pending.commit();
    sync_parent_directory(path)?;
    Ok(())
}

/// Load one canonical snapshot. Missing files are represented by `None`.
pub fn load_json_snapshot<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Option<T>> {
    let path = path.as_ref();
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("could not read JSON snapshot {}", path.display()));
        }
    };
    let value = serde_json::from_reader(BufReader::new(file))
        .with_context(|| format!("invalid JSON snapshot {}", path.display()))?;
    Ok(Some(value))
}

/// A candidate snapshot that could not be read or decoded while falling back
/// to an older generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidSnapshot {
    pub path: PathBuf,
    pub error: String,
}

/// Result of scanning a snapshot directory from newest to oldest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestSnapshot<T> {
    pub value: Option<T>,
    pub path: Option<PathBuf>,
    pub skipped_invalid: Vec<InvalidSnapshot>,
}

impl<T> LatestSnapshot<T> {
    fn empty() -> Self {
        Self {
            value: None,
            path: None,
            skipped_invalid: Vec::new(),
        }
    }
}

/// Load the newest valid top-level `.json` snapshot from a directory.
///
/// Candidates are sorted by modification time and then filename, newest first.
/// Unreadable/malformed generations are reported in `skipped_invalid`, allowing
/// callers to replay the event log when every snapshot is unusable.
pub fn load_latest_valid_snapshot<T: DeserializeOwned>(
    directory: impl AsRef<Path>,
) -> Result<LatestSnapshot<T>> {
    let directory = directory.as_ref();
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LatestSnapshot::empty());
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("could not read snapshot directory {}", directory.display())
            });
        }
    };

    let mut candidates = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| {
            format!(
                "could not enumerate snapshot directory {}",
                directory.display()
            )
        })?;
        let path = entry.path();
        if !entry
            .file_type()
            .with_context(|| format!("could not inspect snapshot candidate {}", path.display()))?
            .is_file()
            || path.extension().and_then(|value| value.to_str()) != Some("json")
        {
            continue;
        }

        let modified = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .unwrap_or(UNIX_EPOCH);
        let name = entry.file_name();
        candidates.push((modified, name, path));
    }

    candidates.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| right.1.cmp(&left.1)));

    let mut result = LatestSnapshot::empty();
    for (_, _, path) in candidates {
        match File::open(&path) {
            Ok(file) => match serde_json::from_reader::<_, T>(BufReader::new(file)) {
                Ok(value) => {
                    result.value = Some(value);
                    result.path = Some(path);
                    return Ok(result);
                }
                Err(error) => result.skipped_invalid.push(InvalidSnapshot {
                    path,
                    error: error.to_string(),
                }),
            },
            Err(error) => result.skipped_invalid.push(InvalidSnapshot {
                path,
                error: error.to_string(),
            }),
        }
    }

    Ok(result)
}

/// Append-only closed-trade CSV writer. It emits a header only when opening an
/// empty file and expects a single writer per path.
pub struct ClosedTradeCsvWriter {
    path: PathBuf,
    writer: csv::Writer<File>,
    policy: SyncPolicy,
    unsynced_events: usize,
}

impl ClosedTradeCsvWriter {
    pub fn open(path: impl AsRef<Path>, policy: SyncPolicy) -> Result<Self> {
        let path = path.as_ref();
        ensure_parent(path)?;

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .with_context(|| format!("could not open closed-trade CSV {}", path.display()))?;
        let is_empty = file
            .metadata()
            .with_context(|| format!("could not inspect closed-trade CSV {}", path.display()))?
            .len()
            == 0;
        validate_csv_append_boundary(&mut file, path)?;

        Ok(Self {
            path: path.to_path_buf(),
            writer: csv::WriterBuilder::new()
                .has_headers(is_empty)
                .from_writer(file),
            policy,
            unsynced_events: 0,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append<T: Serialize>(&mut self, trade: &T) -> Result<()> {
        self.writer
            .serialize(trade)
            .with_context(|| format!("could not append closed trade to {}", self.path.display()))?;
        self.unsynced_events = self.unsynced_events.saturating_add(1);

        if self.policy.flushes_each_event() {
            self.flush()?;
        }
        if self.policy.should_sync(self.unsynced_events) {
            self.sync()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer
            .flush()
            .with_context(|| format!("could not flush closed-trade CSV {}", self.path.display()))
    }

    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        self.writer
            .get_ref()
            .sync_data()
            .with_context(|| format!("could not sync closed-trade CSV {}", self.path.display()))?;
        self.unsynced_events = 0;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.sync()
    }
}

impl Drop for ClosedTradeCsvWriter {
    fn drop(&mut self) {
        let _ = self.writer.flush();
    }
}

/// Replace a complete CSV export atomically. This is suitable for dashboard
/// downloads while [`ClosedTradeCsvWriter`] remains the authoritative append
/// history.
pub fn atomic_export_csv<T, I>(path: impl AsRef<Path>, records: I) -> Result<usize>
where
    T: Serialize,
    I: IntoIterator<Item = T>,
{
    let path = path.as_ref();
    ensure_parent(path)?;
    let (temp_path, temp_file) = create_same_directory_temp(path)?;
    let mut pending = PendingTempFile::new(temp_path);
    let mut writer = csv::WriterBuilder::new()
        .has_headers(true)
        .from_writer(temp_file);
    let mut count = 0usize;

    for record in records {
        writer
            .serialize(record)
            .with_context(|| format!("could not serialize CSV export for {}", path.display()))?;
        count = count.saturating_add(1);
    }
    writer
        .flush()
        .with_context(|| format!("could not flush CSV export for {}", path.display()))?;
    writer
        .get_ref()
        .sync_all()
        .with_context(|| format!("could not sync CSV export for {}", path.display()))?;
    drop(writer);

    fs::rename(pending.path(), path).with_context(|| {
        format!(
            "could not atomically replace CSV export {} with {}",
            path.display(),
            pending.path().display()
        )
    })?;
    pending.commit();
    sync_parent_directory(path)?;
    Ok(count)
}

/// Load a closed-trade CSV for history views. Missing files return an empty
/// collection.
pub fn load_csv<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>> {
    let path = path.as_ref();
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("could not open CSV history {}", path.display()));
        }
    };
    let mut reader = csv::Reader::from_reader(file);
    reader
        .deserialize()
        .enumerate()
        .map(|(index, row)| {
            row.with_context(|| {
                format!(
                    "invalid CSV history row {} in {}",
                    index.saturating_add(2),
                    path.display()
                )
            })
        })
        .collect()
}

/// A FIFO history that retains at most `capacity` newest values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedHistory<T> {
    capacity: usize,
    items: VecDeque<T>,
}

impl<T> BoundedHistory<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            items: VecDeque::with_capacity(capacity),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Insert a newest item and return the oldest evicted item, if any. A zero
    /// capacity is valid and immediately returns the supplied value.
    pub fn push(&mut self, item: T) -> Option<T> {
        if self.capacity == 0 {
            return Some(item);
        }
        let evicted = if self.items.len() == self.capacity {
            self.items.pop_front()
        } else {
            None
        };
        self.items.push_back(item);
        evicted
    }

    pub fn oldest(&self) -> Option<&T> {
        self.items.front()
    }

    pub fn newest(&self) -> Option<&T> {
        self.items.back()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> + ExactSizeIterator {
        self.items.iter()
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }

    pub fn into_vec(self) -> Vec<T> {
        self.items.into_iter().collect()
    }
}

fn ensure_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "could not create persistence directory {}",
            parent.display()
        )
    })
}

fn validate_jsonl_append_boundary(file: &mut File, path: &Path) -> Result<()> {
    let len = file
        .metadata()
        .with_context(|| format!("could not inspect JSONL event log {}", path.display()))?
        .len();
    if len == 0 {
        return Ok(());
    }

    file.seek(SeekFrom::End(-1))?;
    let mut final_byte = [0u8; 1];
    file.read_exact(&mut final_byte)?;
    if final_byte[0] == b'\n' {
        return Ok(());
    }

    let line_start = find_last_line_start(file, len)?;
    let tail_len = len
        .checked_sub(line_start)
        .ok_or_else(|| anyhow!("invalid JSONL tail offset"))?;
    let tail_len: usize = tail_len
        .try_into()
        .context("unterminated JSONL record is too large to validate")?;
    let mut tail = vec![0u8; tail_len];
    file.seek(SeekFrom::Start(line_start))?;
    file.read_exact(&mut tail)?;

    if serde_json::from_slice::<serde_json::Value>(&tail).is_err() {
        bail!(
            "{} ends with a truncated/invalid JSONL record; replay or rotate it before appending",
            path.display()
        );
    }

    // A valid final JSON value merely missed its line terminator. Add the
    // delimiter durably before allowing later records to be appended.
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

fn validate_csv_append_boundary(file: &mut File, path: &Path) -> Result<()> {
    let len = file
        .metadata()
        .with_context(|| format!("could not inspect closed-trade CSV {}", path.display()))?
        .len();
    if len == 0 {
        return Ok(());
    }
    file.seek(SeekFrom::End(-1))?;
    let mut final_byte = [0u8; 1];
    file.read_exact(&mut final_byte)?;
    if final_byte[0] != b'\n' {
        bail!(
            "{} has an unterminated final CSV record; refusing to append",
            path.display()
        );
    }
    Ok(())
}

fn find_last_line_start(file: &mut File, len: u64) -> Result<u64> {
    const BLOCK_SIZE: u64 = 8 * 1024;
    let mut cursor = len;
    let mut block = Vec::new();

    while cursor > 0 {
        let start = cursor.saturating_sub(BLOCK_SIZE);
        let size: usize = (cursor - start)
            .try_into()
            .context("JSONL scan block length overflow")?;
        block.resize(size, 0);
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut block)?;
        if let Some(index) = block.iter().rposition(|byte| *byte == b'\n') {
            return Ok(start + index as u64 + 1);
        }
        cursor = start;
    }
    Ok(0)
}

fn create_same_directory_temp(target: &Path) -> Result<(PathBuf, File)> {
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = target
        .file_name()
        .ok_or_else(|| anyhow!("persistence target {} has no filename", target.display()))?
        .to_string_lossy();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    for _ in 0..32 {
        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp_path = parent.join(format!(
            ".{filename}.tmp-{}-{timestamp}-{sequence}",
            std::process::id()
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
        {
            Ok(file) => return Ok((temp_path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("could not create temporary file {}", temp_path.display())
                });
            }
        }
    }

    bail!(
        "could not allocate a unique temporary file beside {}",
        target.display()
    )
}

struct PendingTempFile {
    path: PathBuf,
    committed: bool,
}

impl PendingTempFile {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for PendingTempFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("could not sync persistence directory {}", parent.display()))
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<()> {
    // `sync_all` on directory handles is not portable. The same-directory
    // rename is still atomic; the temporary file itself was synced first.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    static TEST_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestEvent {
        id: u64,
        action: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestTrade {
        trade_id: String,
        entry: f64,
        exit: f64,
        pnl: f64,
    }

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let sequence = TEST_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "market-manager-persistence-test-{}-{}-{sequence}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ));
            fs::create_dir(&path).expect("create isolated test directory");
            Self { path }
        }

        fn join(&self, name: &str) -> PathBuf {
            self.path.join(name)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            // This exact path was uniquely created by this test process.
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn jsonl_round_trip_and_callback_replay() {
        let directory = TestDirectory::new();
        let path = directory.join("events.jsonl");
        let mut writer = JsonlEventWriter::open(
            &path,
            SyncPolicy::SyncEveryEvents(NonZeroUsize::new(2).unwrap()),
        )
        .unwrap();
        writer
            .append(&TestEvent {
                id: 1,
                action: "placed".into(),
            })
            .unwrap();
        writer
            .append(&TestEvent {
                id: 2,
                action: "filled".into(),
            })
            .unwrap();
        writer.finish().unwrap();

        let report = replay_jsonl::<TestEvent>(&path).unwrap();
        assert_eq!(report.summary.records_replayed, 2);
        assert_eq!(report.summary.truncated_final_line, None);
        assert_eq!(report.summary.bytes_read, report.summary.last_valid_offset);
        assert_eq!(report.records[1].action, "filled");

        let mut ids = Vec::new();
        let summary = replay_jsonl_with::<TestEvent, _>(&path, |event| {
            ids.push(event.id);
            Ok(())
        })
        .unwrap();
        assert_eq!(ids, vec![1, 2]);
        assert_eq!(summary.records_replayed, 2);
    }

    #[test]
    fn replay_tolerates_only_an_unterminated_invalid_final_line() {
        let directory = TestDirectory::new();
        let path = directory.join("events.jsonl");
        fs::write(
            &path,
            b"{\"id\":1,\"action\":\"ok\"}\n{\"id\":2,\"action\":",
        )
        .unwrap();

        let report = replay_jsonl::<TestEvent>(&path).unwrap();
        assert_eq!(report.records.len(), 1);
        let truncated = report.summary.truncated_final_line.unwrap();
        assert_eq!(truncated.line_number, 2);
        assert!(truncated.byte_len > 0);
        assert!(!truncated.parse_error.is_empty());

        fs::write(
            &path,
            b"{\"id\":1,\"action\":\"ok\"}\nnot-json\n{\"id\":2,\"action\":\"ok\"}\n",
        )
        .unwrap();
        let error = replay_jsonl::<TestEvent>(&path).unwrap_err().to_string();
        assert!(error.contains("line 2"));
    }

    #[test]
    fn writer_repairs_valid_missing_delimiter_but_rejects_truncated_tail() {
        let directory = TestDirectory::new();
        let path = directory.join("events.jsonl");
        fs::write(&path, b"{\"id\":1,\"action\":\"ok\"}").unwrap();
        let mut writer = JsonlEventWriter::open(&path, SyncPolicy::FlushEveryEvent).unwrap();
        writer
            .append(&TestEvent {
                id: 2,
                action: "next".into(),
            })
            .unwrap();
        writer.finish().unwrap();
        assert_eq!(replay_jsonl::<TestEvent>(&path).unwrap().records.len(), 2);

        fs::write(&path, b"{\"id\":1").unwrap();
        let error = JsonlEventWriter::open(&path, SyncPolicy::Manual)
            .err()
            .expect("truncated tail must be rejected")
            .to_string();
        assert!(error.contains("truncated/invalid"));
    }

    #[test]
    fn snapshot_replacement_and_latest_valid_fallback_work() {
        let directory = TestDirectory::new();
        let canonical = directory.join("canonical.json");
        atomic_write_json_snapshot(&canonical, &vec![1u64, 2]).unwrap();
        atomic_write_json_snapshot(&canonical, &vec![3u64, 4]).unwrap();
        assert_eq!(
            load_json_snapshot::<Vec<u64>>(&canonical).unwrap(),
            Some(vec![3, 4])
        );

        // Ensure this invalid generation is newest. The filename is also the
        // deterministic tie-breaker on coarse-mtime filesystems.
        std::thread::sleep(std::time::Duration::from_millis(5));
        let invalid = directory.join("zz-newest.json");
        fs::write(&invalid, b"{incomplete").unwrap();

        let latest = load_latest_valid_snapshot::<Vec<u64>>(&directory.path).unwrap();
        assert_eq!(latest.value, Some(vec![3, 4]));
        assert_eq!(latest.path, Some(canonical));
        assert_eq!(latest.skipped_invalid.len(), 1);
        assert_eq!(latest.skipped_invalid[0].path, invalid);
    }

    #[test]
    fn closed_trade_csv_supports_append_load_and_atomic_export() {
        let directory = TestDirectory::new();
        let history = directory.join("closed.csv");
        let first = TestTrade {
            trade_id: "t-1".into(),
            entry: 100.0,
            exit: 110.0,
            pnl: 610.0,
        };
        let second = TestTrade {
            trade_id: "t-2".into(),
            entry: 80.0,
            exit: 75.0,
            pnl: -345.0,
        };

        let mut writer = ClosedTradeCsvWriter::open(&history, SyncPolicy::SyncEveryEvent).unwrap();
        writer.append(&first).unwrap();
        writer.append(&second).unwrap();
        writer.finish().unwrap();
        assert_eq!(
            load_csv::<TestTrade>(&history).unwrap(),
            vec![first.clone(), second.clone()]
        );

        let export = directory.join("export.csv");
        assert_eq!(
            atomic_export_csv(&export, vec![second.clone(), first.clone()]).unwrap(),
            2
        );
        assert_eq!(load_csv::<TestTrade>(&export).unwrap(), vec![second, first]);
    }

    #[test]
    fn bounded_history_evicts_oldest_and_supports_zero_capacity() {
        let mut history = BoundedHistory::new(2);
        assert_eq!(history.push(1), None);
        assert_eq!(history.push(2), None);
        assert_eq!(history.push(3), Some(1));
        assert_eq!(history.oldest(), Some(&2));
        assert_eq!(history.newest(), Some(&3));
        assert_eq!(history.iter().copied().collect::<Vec<_>>(), vec![2, 3]);

        let mut disabled = BoundedHistory::new(0);
        assert_eq!(disabled.push(9), Some(9));
        assert!(disabled.is_empty());
    }
}
