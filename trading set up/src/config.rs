//! Validated runtime configuration loaded from an external secret file and
//! the process environment.
//!
//! The secret file path can be set with `OBSERVER_ENV_PATH`. On Windows it
//! otherwise defaults to `%LOCALAPPDATA%\observer-trading\.env`. Process
//! environment values take precedence over file values, except for
//! `GEMINI_API_KEY`: when that key is present in the secret file, the file
//! value is authoritative. Relative application paths are resolved against
//! the project directory, never against the caller's current working directory.

use std::{
    collections::{BTreeMap, HashSet},
    env, fmt, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize, Serializer};

pub const DEFAULT_GEMINI_MODEL: &str = "gemini-3.5-flash-lite";
pub const DEFAULT_PAPER_ACCOUNTS: &str =
    "account_1:5000,account_2:10000,account_3:2000,account_4:15000,account_5:20000";
pub const DEFAULT_DASHBOARD_BIND: &str = "127.0.0.1:8787";
pub const DEFAULT_GEMINI_KEY_NAMES: &str = "GEMINI_WORKING_01,GEMINI_WORKING_02,GEMINI_WORKING_03";
pub const DEFAULT_API_KEY_LIMIT: usize = 3;

/// A secret that is redacted both in debug output and Serde serialization.
///
/// Call [`SecretString::expose_secret`] only at the API boundary that needs the
/// actual credential.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            bail!("secret value must not be empty");
        }
        if value.contains('\r') || value.contains('\n') {
            bail!("secret value must not contain line breaks");
        }
        Ok(Self(value))
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString([REDACTED])")
    }
}

impl Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("[REDACTED]")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppConfig {
    pub gemini: GeminiConfig,
    pub elevenlabs: ElevenLabsConfig,
    pub database: DatabaseConfig,
    pub broker: BrokerConfig,
    pub accounts: Vec<AccountConfig>,
    pub lot_sizes: LotSizeConfig,
    pub trading: TradingConfig,
    pub dashboard: DashboardConfig,
    pub scheduler: SchedulerConfig,
    pub media: MediaConfig,
    pub paths: PathConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BrokerConfig {
    pub client_id: Option<SecretString>,
    pub mpin: Option<SecretString>,
    pub totp_secret: Option<SecretString>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ElevenLabsConfig {
    pub api_keys: Vec<SecretString>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DatabaseConfig {
    pub url: Option<SecretString>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GeminiConfig {
    pub api_key: SecretString,
    pub api_keys: Vec<SecretString>,
    pub model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountConfig {
    pub id: String,
    pub initial_capital_rupees: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct LotSizeConfig {
    pub nifty: u32,
    pub sensex: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TradingConfig {
    pub minimum_confidence_pct: f64,
    pub entry_buffer_points: f64,
    pub candidate_ttl_seconds: u64,
    pub charge_per_fill_rupees: f64,
    pub end_of_day_exit_ist: NaiveTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DashboardConfig {
    pub bind: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchedulerConfig {
    pub youtube_channel_url: Option<String>,
    pub poll_start_ist: NaiveTime,
    pub last_discovery_ist: NaiveTime,
    pub worker_stop_ist: NaiveTime,
    pub poll_interval_seconds: u64,
    pub market_holidays_ist: Vec<NaiveDate>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct MediaConfig {
    pub clips_to_keep: usize,
    pub stt_concurrency: usize,
    pub elevenlabs_key_limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PathConfig {
    pub project_dir: PathBuf,
    pub data_dir: PathBuf,
    pub media_dir: PathBuf,
    pub observer_token_path: PathBuf,
    pub observer_totp_path: PathBuf,
    pub yt_dlp_path: PathBuf,
    pub ffmpeg_path: PathBuf,
}

impl AppConfig {
    /// Load the external secret file, then overlay the process environment.
    ///
    /// `OBSERVER_ENV_PATH` selects an explicit file. Without it, Windows uses
    /// `%LOCALAPPDATA%\observer-trading\.env`; other platforms rely on process
    /// environment variables unless an explicit path is configured.
    ///
    /// An explicit `GEMINI_API_KEY` entry in the secret file is never replaced
    /// by an inherited process value. This prevents an unrelated shell/session
    /// credential from silently changing the account used by this project. A
    /// blank file entry remains authoritative and fails validation instead
    /// of falling back to the inherited credential.
    pub fn load(project_dir: impl AsRef<Path>) -> Result<Self> {
        let project_dir = project_dir.as_ref();
        let dotenv_path = external_env_path()?;

        let file_values =
            if let Some(dotenv_path) = dotenv_path.as_ref().filter(|path| path.exists()) {
                let contents = fs::read_to_string(&dotenv_path)
                    .with_context(|| format!("could not read {}", dotenv_path.display()))?;
                parse_dotenv(&contents)
                    .with_context(|| format!("could not parse {}", dotenv_path.display()))?
            } else {
                BTreeMap::new()
            };

        let mut values = merge_project_and_environment(file_values, env::vars());
        hydrate_gemini_credentials(project_dir, &mut values)?;
        Self::from_values(project_dir, values)
    }

    /// Build configuration from injected key/value pairs. This is the preferred
    /// entry point for tests because it never mutates global process state.
    pub fn from_values<I, K, V>(project_dir: impl AsRef<Path>, values: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let values = values
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<BTreeMap<_, _>>();
        Self::from_map(project_dir.as_ref(), &values)
    }

    fn from_map(project_dir: &Path, values: &BTreeMap<String, String>) -> Result<Self> {
        let gemini_keys = configured_gemini_keys(values)?;
        let elevenlabs_keys = configured_provider_keys(values, "ELEVENLABS_API_KEY")?;
        let gemini_model = get(values, "GEMINI_MODEL")
            .unwrap_or(DEFAULT_GEMINI_MODEL)
            .trim()
            .to_owned();
        if gemini_model.is_empty() || gemini_model.contains('\r') || gemini_model.contains('\n') {
            bail!("GEMINI_MODEL must be a non-empty single-line model name");
        }

        let accounts = parse_accounts(
            get(values, "PAPER_ACCOUNTS")
                .or_else(|| get(values, "ACCOUNT_CAPITALS"))
                .unwrap_or(DEFAULT_PAPER_ACCOUNTS),
        )?;

        let data_dir = resolve_path(
            project_dir,
            get(values, "DATA_DIR").unwrap_or("data"),
            "DATA_DIR",
        )?;
        let media_dir = match get(values, "MEDIA_DIR") {
            Some(path) => resolve_path(project_dir, path, "MEDIA_DIR")?,
            None => data_dir.join("media"),
        };

        let config = Self {
            gemini: GeminiConfig {
                api_key: gemini_keys[0].clone(),
                api_keys: gemini_keys,
                model: gemini_model,
            },
            elevenlabs: ElevenLabsConfig {
                api_keys: elevenlabs_keys,
            },
            database: DatabaseConfig {
                url: get(values, "DATABASE_URL")
                    .filter(|value| !value.trim().is_empty())
                    .map(SecretString::new)
                    .transpose()?,
            },
            broker: BrokerConfig {
                client_id: optional_secret(values, "BROKER_CLIENT_ID")?,
                mpin: optional_secret(values, "BROKER_MPIN")?,
                totp_secret: optional_secret(values, "BROKER_TOTP_SECRET")?,
            },
            accounts,
            lot_sizes: LotSizeConfig {
                nifty: parse_or(values, "NIFTY_LOT_SIZE", 65)?,
                sensex: parse_or(values, "SENSEX_LOT_SIZE", 20)?,
            },
            trading: TradingConfig {
                minimum_confidence_pct: parse_or(values, "MIN_TRADE_CONFIDENCE", 65.0)?,
                entry_buffer_points: parse_or(values, "ENTRY_BUFFER_POINTS", 2.0)?,
                candidate_ttl_seconds: parse_or(values, "CANDIDATE_TTL_SECONDS", 10)?,
                charge_per_fill_rupees: parse_or(values, "CHARGE_PER_FILL", 20.0)?,
                end_of_day_exit_ist: parse_time(
                    get(values, "EOD_EXIT_TIME_IST").unwrap_or("15:29:30"),
                )?,
            },
            dashboard: DashboardConfig {
                bind: dashboard_bind(values)?,
            },
            scheduler: SchedulerConfig {
                youtube_channel_url: get(values, "YOUTUBE_CHANNEL_URL")
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned),
                poll_start_ist: parse_time(get(values, "POLL_START_IST").unwrap_or("09:00:00"))?,
                last_discovery_ist: parse_time(
                    get(values, "LAST_DISCOVERY_IST").unwrap_or("15:30:00"),
                )?,
                worker_stop_ist: parse_time(get(values, "WORKER_STOP_IST").unwrap_or("16:00:00"))?,
                poll_interval_seconds: parse_or(values, "YOUTUBE_POLL_SECONDS", 60)?,
                market_holidays_ist: parse_market_holidays(
                    get(values, "MARKET_HOLIDAYS_IST").unwrap_or(DEFAULT_2026_FO_HOLIDAYS),
                )?,
            },
            media: MediaConfig {
                clips_to_keep: parse_or(values, "CLIPS_TO_KEEP", 3)?,
                stt_concurrency: parse_or(values, "STT_CONCURRENCY", 4)?,
                elevenlabs_key_limit: parse_or(
                    values,
                    "ELEVENLABS_KEY_LIMIT",
                    DEFAULT_API_KEY_LIMIT,
                )?,
            },
            paths: PathConfig {
                project_dir: project_dir.to_path_buf(),
                data_dir,
                media_dir,
                observer_token_path: resolve_path(
                    project_dir,
                    get(values, "OBSERVER_TOKEN_PATH").unwrap_or("../token.txt"),
                    "OBSERVER_TOKEN_PATH",
                )?,
                observer_totp_path: resolve_path(
                    project_dir,
                    get(values, "OBSERVER_TOTP_PATH").unwrap_or("../totp.txt"),
                    "OBSERVER_TOTP_PATH",
                )?,
                // Bare executable names intentionally remain bare so Windows
                // PATH lookup still works; explicit values may be absolute.
                yt_dlp_path: PathBuf::from(get(values, "YT_DLP_PATH").unwrap_or("yt-dlp")),
                ffmpeg_path: PathBuf::from(get(values, "FFMPEG_PATH").unwrap_or("ffmpeg")),
            },
        };

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.gemini.api_keys.is_empty()
            || self
                .gemini
                .api_keys
                .iter()
                .any(|key| key.expose_secret().trim().is_empty())
        {
            bail!("at least one non-empty Gemini API key is required");
        }
        if self.elevenlabs.api_keys.is_empty()
            || self
                .elevenlabs
                .api_keys
                .iter()
                .any(|key| key.expose_secret().trim().is_empty())
        {
            bail!("at least one non-empty ElevenLabs API key is required");
        }
        let broker_secret_count = [
            self.broker.client_id.is_some(),
            self.broker.mpin.is_some(),
            self.broker.totp_secret.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        if broker_secret_count != 0 && broker_secret_count != 3 {
            bail!("BROKER_CLIENT_ID, BROKER_MPIN, and BROKER_TOTP_SECRET must be set together");
        }
        if self.accounts.is_empty() {
            bail!("PAPER_ACCOUNTS must contain at least one account");
        }

        let mut account_ids = HashSet::new();
        for account in &self.accounts {
            validate_account_id(&account.id)?;
            if !account_ids.insert(account.id.to_ascii_lowercase()) {
                bail!(
                    "PAPER_ACCOUNTS contains duplicate account id '{}'",
                    account.id
                );
            }
            if !account.initial_capital_rupees.is_finite() || account.initial_capital_rupees <= 0.0
            {
                bail!(
                    "capital for account '{}' must be greater than zero",
                    account.id
                );
            }
        }

        if self.lot_sizes.nifty == 0 || self.lot_sizes.sensex == 0 {
            bail!("NIFTY_LOT_SIZE and SENSEX_LOT_SIZE must be greater than zero");
        }
        if self.lot_sizes.nifty > 10_000 || self.lot_sizes.sensex > 10_000 {
            bail!("lot sizes must not exceed 10000");
        }
        if !self.trading.minimum_confidence_pct.is_finite()
            || !(0.0..=100.0).contains(&self.trading.minimum_confidence_pct)
        {
            bail!("MIN_TRADE_CONFIDENCE must be between 0 and 100");
        }
        if !self.trading.entry_buffer_points.is_finite() || self.trading.entry_buffer_points < 0.0 {
            bail!("ENTRY_BUFFER_POINTS must be a finite non-negative number");
        }
        if self.trading.candidate_ttl_seconds == 0 || self.trading.candidate_ttl_seconds > 3_600 {
            bail!("CANDIDATE_TTL_SECONDS must be between 1 and 3600");
        }
        if !self.trading.charge_per_fill_rupees.is_finite()
            || self.trading.charge_per_fill_rupees < 0.0
        {
            bail!("CHARGE_PER_FILL must be a finite non-negative number");
        }
        if self.dashboard.bind.port() == 0 {
            bail!("DASHBOARD_BIND must use a non-zero port");
        }
        if self.scheduler.poll_start_ist >= self.scheduler.last_discovery_ist
            || self.scheduler.last_discovery_ist >= self.scheduler.worker_stop_ist
        {
            bail!("scheduler times must satisfy start < last discovery < worker stop");
        }
        if !(15..=3600).contains(&self.scheduler.poll_interval_seconds) {
            bail!("YOUTUBE_POLL_SECONDS must be between 15 and 3600");
        }
        if self.media.clips_to_keep == 0 {
            bail!("CLIPS_TO_KEEP must be greater than zero");
        }
        if !(1..=64).contains(&self.media.stt_concurrency) {
            bail!("STT_CONCURRENCY must be between 1 and 64");
        }
        if !(1..=16).contains(&self.media.elevenlabs_key_limit) {
            bail!("ELEVENLABS_KEY_LIMIT must be between 1 and 16");
        }
        if self.paths.yt_dlp_path.as_os_str().is_empty()
            || self.paths.ffmpeg_path.as_os_str().is_empty()
        {
            bail!("YT_DLP_PATH and FFMPEG_PATH must not be empty");
        }
        Ok(())
    }
}

fn optional_secret(values: &BTreeMap<String, String>, key: &str) -> Result<Option<SecretString>> {
    get(values, key)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(SecretString::new)
        .transpose()
}

const DEFAULT_2026_FO_HOLIDAYS: &str = "2026-01-26,2026-03-03,2026-03-26,2026-03-31,2026-04-03,2026-04-14,2026-05-01,2026-05-28,2026-06-26,2026-09-14,2026-10-02,2026-10-20,2026-11-10,2026-11-24,2026-12-25";

fn dashboard_bind(values: &BTreeMap<String, String>) -> Result<SocketAddr> {
    if let Some(port) = get(values, "PORT") {
        let port = port
            .parse::<u16>()
            .with_context(|| format!("PORT has invalid value '{port}'"))?;
        if port == 0 {
            bail!("PORT must be non-zero");
        }
        return Ok(SocketAddr::from(([0, 0, 0, 0], port)));
    }
    parse_or(
        values,
        "DASHBOARD_BIND",
        SocketAddr::from_str(DEFAULT_DASHBOARD_BIND).expect("valid default bind"),
    )
}

fn parse_market_holidays(value: &str) -> Result<Vec<NaiveDate>> {
    let mut dates = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .with_context(|| format!("invalid MARKET_HOLIDAYS_IST date '{value}'"))
        })
        .collect::<Result<Vec<_>>>()?;
    dates.sort_unstable();
    dates.dedup();
    Ok(dates)
}

fn configured_gemini_keys(values: &BTreeMap<String, String>) -> Result<Vec<SecretString>> {
    let mut raw_keys = (1..=16)
        .filter_map(|index| get(values, &format!("GEMINI_API_KEY_{index}")))
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    if raw_keys.is_empty()
        && let Some(legacy) = get(values, "GEMINI_API_KEY").filter(|value| !value.trim().is_empty())
    {
        raw_keys.push(legacy.to_owned());
    }
    if raw_keys.is_empty() {
        bail!("GEMINI_API_KEY is required and must not be empty (GEMINI_API_KEY_1 is preferred)");
    }

    let mut seen = HashSet::new();
    raw_keys
        .into_iter()
        .filter(|key| seen.insert(key.clone()))
        .map(SecretString::new)
        .collect()
}

fn configured_provider_keys(
    values: &BTreeMap<String, String>,
    prefix: &str,
) -> Result<Vec<SecretString>> {
    let mut raw_keys = (1..=16)
        .filter_map(|index| get(values, &format!("{prefix}_{index}")))
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if raw_keys.is_empty()
        && let Some(legacy) = get(values, prefix).filter(|value| !value.trim().is_empty())
    {
        raw_keys.push(legacy.to_owned());
    }
    if raw_keys.is_empty() {
        bail!("{prefix}_1 is required and must not be empty");
    }
    let mut seen = HashSet::new();
    raw_keys
        .into_iter()
        .filter(|key| seen.insert(key.clone()))
        .map(SecretString::new)
        .collect()
}

fn hydrate_gemini_credentials(
    project_dir: &Path,
    values: &mut BTreeMap<String, String>,
) -> Result<()> {
    if get(values, "GEMINI_VAULT_PATH").is_none() {
        return Ok(());
    }

    let vault_path = resolve_path(
        project_dir,
        required(values, "GEMINI_VAULT_PATH")?,
        "GEMINI_VAULT_PATH",
    )?;
    let vault_text = fs::read_to_string(&vault_path).with_context(|| {
        format!(
            "could not read Gemini credential vault {}",
            vault_path.display()
        )
    })?;
    let vault = parse_dotenv(&vault_text).with_context(|| {
        format!(
            "could not parse Gemini credential vault {}",
            vault_path.display()
        )
    })?;
    let requested_names = get(values, "GEMINI_KEY_NAMES")
        .unwrap_or(DEFAULT_GEMINI_KEY_NAMES)
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if requested_names.is_empty() || requested_names.len() > 16 {
        bail!("GEMINI_KEY_NAMES must select between 1 and 16 credential names");
    }

    for (offset, name) in requested_names.into_iter().enumerate() {
        let value = vault
            .get(&name)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("Gemini credential vault is missing selected entry {name}"))?;
        values
            .entry(format!("GEMINI_API_KEY_{}", offset + 1))
            .or_insert_with(|| value.clone());
    }
    Ok(())
}

fn external_env_path() -> Result<Option<PathBuf>> {
    if let Some(explicit) = env::var_os("OBSERVER_ENV_PATH") {
        if explicit.is_empty() {
            bail!("OBSERVER_ENV_PATH must not be empty");
        }
        return Ok(Some(PathBuf::from(explicit)));
    }

    #[cfg(windows)]
    {
        let local_app_data = env::var_os("LOCALAPPDATA")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("LOCALAPPDATA is unavailable; set OBSERVER_ENV_PATH"))?;
        return Ok(Some(
            PathBuf::from(local_app_data)
                .join("observer-trading")
                .join(".env"),
        ));
    }

    #[cfg(not(windows))]
    Ok(None)
}

/// Merge external-file and inherited settings without allowing an inherited
/// credential to replace a secret explicitly pinned by the file.
fn merge_project_and_environment<I, K, V>(
    mut project_values: BTreeMap<String, String>,
    environment_values: I,
) -> BTreeMap<String, String>
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    let pinned_secrets = project_values
        .keys()
        .filter(|key| is_secret_environment_key(key))
        .cloned()
        .collect::<HashSet<_>>();

    for (key, value) in environment_values {
        let key = key.into();
        if pinned_secrets.contains(&key) {
            continue;
        }
        project_values.insert(key, value.into());
    }

    project_values
}

fn is_secret_environment_key(key: &str) -> bool {
    key == "DATABASE_URL"
        || key == "BROKER_CLIENT_ID"
        || key == "BROKER_MPIN"
        || key == "BROKER_TOTP_SECRET"
        || key == "GEMINI_API_KEY"
        || key.starts_with("GEMINI_API_KEY_")
        || key == "ELEVENLABS_API_KEY"
        || key.starts_with("ELEVENLABS_API_KEY_")
}

fn get<'a>(values: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    values.get(key).map(String::as_str)
}

fn required<'a>(values: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str> {
    get(values, key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("{key} is required and must not be empty"))
}

fn parse_or<T>(values: &BTreeMap<String, String>, key: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    match get(values, key) {
        Some(value) => value
            .trim()
            .parse::<T>()
            .map_err(|error| anyhow!("invalid {key}: {error}")),
        None => Ok(default),
    }
}

fn parse_time(value: &str) -> Result<NaiveTime> {
    let value = value.trim();
    NaiveTime::parse_from_str(value, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M"))
        .with_context(|| format!("invalid EOD_EXIT_TIME_IST '{value}'; expected HH:MM:SS"))
}

fn parse_accounts(value: &str) -> Result<Vec<AccountConfig>> {
    let mut accounts = Vec::new();

    for (index, raw_entry) in value.split(',').enumerate() {
        let entry = raw_entry.trim();
        if entry.is_empty() {
            bail!("PAPER_ACCOUNTS contains an empty entry");
        }

        let (id, capital) = entry
            .split_once(':')
            .or_else(|| entry.split_once('='))
            .map(|(id, capital)| (id.trim().to_owned(), capital.trim()))
            .unwrap_or_else(|| (format!("account_{}", index + 1), entry));
        validate_account_id(&id)?;
        let initial_capital_rupees = capital
            .parse::<f64>()
            .with_context(|| format!("invalid capital for PAPER_ACCOUNTS account '{id}'"))?;
        accounts.push(AccountConfig {
            id,
            initial_capital_rupees,
        });
    }

    Ok(accounts)
}

fn validate_account_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
    {
        bail!("account id '{id}' must be 1-64 ASCII letters, digits, hyphens, or underscores");
    }
    Ok(())
}

fn resolve_path(project_dir: &Path, value: &str, key: &str) -> Result<PathBuf> {
    let value = value.trim();
    if value.is_empty() {
        bail!("{key} must not be empty");
    }
    let path = PathBuf::from(value);
    Ok(if path.is_absolute() {
        path
    } else {
        project_dir.join(path)
    })
}

/// Parse the small dotenv subset used by this project. Both quoted and
/// unquoted values are accepted; variable expansion is intentionally omitted.
fn parse_dotenv(contents: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();

    for (line_index, raw_line) in contents.lines().enumerate() {
        let line_number = line_index + 1;
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("export ") {
            line = rest.trim_start();
        }
        let (key, raw_value) = line
            .split_once('=')
            .ok_or_else(|| anyhow!("line {line_number} must contain '='"))?;
        let key = key.trim();
        validate_env_key(key)
            .with_context(|| format!("invalid variable on .env line {line_number}"))?;
        let value = parse_dotenv_value(raw_value.trim(), line_number)?;
        values.insert(key.to_owned(), value);
    }

    Ok(values)
}

fn validate_env_key(key: &str) -> Result<()> {
    let mut characters = key.chars();
    if !characters
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        || !characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        bail!("'{key}' is not a valid environment variable name");
    }
    Ok(())
}

fn parse_dotenv_value(raw: &str, line_number: usize) -> Result<String> {
    if let Some(quoted) = raw.strip_prefix('\'') {
        let closing = quoted
            .find('\'')
            .ok_or_else(|| anyhow!("unterminated single quote on .env line {line_number}"))?;
        ensure_only_comment(&quoted[closing + 1..], line_number)?;
        return Ok(quoted[..closing].to_owned());
    }

    if let Some(quoted) = raw.strip_prefix('"') {
        let mut value = String::new();
        let mut escaped = false;
        let mut closing_byte = None;
        for (byte_index, character) in quoted.char_indices() {
            if escaped {
                match character {
                    'n' => value.push('\n'),
                    'r' => value.push('\r'),
                    't' => value.push('\t'),
                    '\\' => value.push('\\'),
                    '"' => value.push('"'),
                    other => {
                        value.push('\\');
                        value.push(other);
                    }
                }
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                closing_byte = Some(byte_index);
                break;
            } else {
                value.push(character);
            }
        }
        if escaped {
            bail!("unterminated escape on .env line {line_number}");
        }
        let closing_byte = closing_byte
            .ok_or_else(|| anyhow!("unterminated double quote on .env line {line_number}"))?;
        ensure_only_comment(&quoted[closing_byte + 1..], line_number)?;
        return Ok(value);
    }

    let value = raw
        .find(" #")
        .map(|comment| &raw[..comment])
        .unwrap_or(raw)
        .trim();
    Ok(value.to_owned())
}

fn ensure_only_comment(suffix: &str, line_number: usize) -> Result<()> {
    let suffix = suffix.trim();
    if suffix.is_empty() || suffix.starts_with('#') {
        Ok(())
    } else {
        bail!("unexpected text after quoted value on .env line {line_number}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with(values: &[(&str, &str)]) -> Result<AppConfig> {
        let mut injected = vec![
            ("GEMINI_API_KEY", "test-secret-key"),
            ("ELEVENLABS_API_KEY", "test-elevenlabs-key"),
        ];
        injected.extend_from_slice(values);
        AppConfig::from_values("C:/project", injected)
    }

    #[test]
    fn defaults_match_the_paper_trading_contract() {
        let config = config_with(&[]).unwrap();

        assert_eq!(config.gemini.model, "gemini-3.5-flash-lite");
        assert_eq!(
            config
                .accounts
                .iter()
                .map(|account| account.initial_capital_rupees)
                .collect::<Vec<_>>(),
            vec![5_000.0, 10_000.0, 2_000.0, 15_000.0, 20_000.0]
        );
        assert_eq!(
            config.lot_sizes,
            LotSizeConfig {
                nifty: 65,
                sensex: 20
            }
        );
        assert_eq!(config.trading.minimum_confidence_pct, 65.0);
        assert_eq!(config.trading.entry_buffer_points, 2.0);
        assert_eq!(config.trading.candidate_ttl_seconds, 10);
        assert_eq!(config.trading.charge_per_fill_rupees, 20.0);
        assert_eq!(
            config.trading.end_of_day_exit_ist,
            NaiveTime::from_hms_opt(15, 29, 30).unwrap()
        );
        assert_eq!(config.media.clips_to_keep, 3);
        assert_eq!(config.dashboard.bind.to_string(), "127.0.0.1:8787");
    }

    #[test]
    fn injected_values_override_defaults_and_relative_paths_use_project_dir() {
        let config = config_with(&[
            ("PAPER_ACCOUNTS", "small:2500,large:12500"),
            ("DATA_DIR", "runtime-data"),
            ("STT_CONCURRENCY", "4"),
            ("DASHBOARD_BIND", "127.0.0.1:9000"),
        ])
        .unwrap();

        assert_eq!(config.accounts[0].id, "small");
        assert_eq!(config.accounts[1].initial_capital_rupees, 12_500.0);
        assert_eq!(
            config.paths.data_dir,
            PathBuf::from("C:/project").join("runtime-data")
        );
        assert_eq!(
            config.paths.media_dir,
            PathBuf::from("C:/project").join("runtime-data/media")
        );
        assert_eq!(config.media.stt_concurrency, 4);
        assert_eq!(config.dashboard.bind.port(), 9000);
    }

    #[test]
    fn rejects_unsafe_or_impossible_values() {
        assert!(config_with(&[("MIN_TRADE_CONFIDENCE", "101")]).is_err());
        assert!(config_with(&[("STT_CONCURRENCY", "0")]).is_err());
        assert!(config_with(&[("PAPER_ACCOUNTS", "same:5000,same:10000")]).is_err());
        assert!(config_with(&[("DASHBOARD_BIND", "127.0.0.1:0")]).is_err());
    }

    #[test]
    fn secret_is_never_exposed_by_debug_or_serialization() {
        let config = config_with(&[]).unwrap();
        let debug = format!("{config:?}");
        let json = serde_json::to_string(&config).unwrap();

        assert!(!debug.contains("test-secret-key"));
        assert!(!json.contains("test-secret-key"));
        assert!(debug.contains("[REDACTED]"));
        assert!(json.contains("[REDACTED]"));
        assert_eq!(config.gemini.api_key.expose_secret(), "test-secret-key");
    }

    #[test]
    fn project_gemini_key_wins_while_environment_overrides_non_secret_settings() {
        let project_values = BTreeMap::from([
            ("GEMINI_API_KEY".to_owned(), "project-secret".to_owned()),
            ("DASHBOARD_BIND".to_owned(), "127.0.0.1:8787".to_owned()),
        ]);
        let environment_values = [
            ("GEMINI_API_KEY", "inherited-secret"),
            ("DASHBOARD_BIND", "127.0.0.1:9000"),
        ];

        let merged = merge_project_and_environment(project_values, environment_values);

        assert_eq!(merged["GEMINI_API_KEY"], "project-secret");
        assert_eq!(merged["DASHBOARD_BIND"], "127.0.0.1:9000");
    }

    #[test]
    fn inherited_gemini_key_is_used_only_when_project_does_not_define_it() {
        let merged = merge_project_and_environment(
            BTreeMap::new(),
            [("GEMINI_API_KEY", "inherited-secret")],
        );

        assert_eq!(merged["GEMINI_API_KEY"], "inherited-secret");
    }

    #[test]
    fn blank_project_gemini_key_is_not_masked_by_inherited_secret() {
        let merged = merge_project_and_environment(
            BTreeMap::from([("GEMINI_API_KEY".to_owned(), String::new())]),
            [("GEMINI_API_KEY", "inherited-secret")],
        );
        let error = AppConfig::from_values("C:/project", merged).unwrap_err();
        let message = error.to_string();

        assert!(message.contains("GEMINI_API_KEY is required"));
        assert!(!message.contains("inherited-secret"));
    }

    #[test]
    fn invalid_secret_error_does_not_echo_the_secret() {
        let secret = "sensitive-first-line\nsensitive-second-line";
        let error = AppConfig::from_values("C:/project", [("GEMINI_API_KEY", secret)]).unwrap_err();
        let message = error.to_string();

        assert!(!message.contains("sensitive-first-line"));
        assert!(!message.contains("sensitive-second-line"));
        assert!(message.contains("secret value must not contain line breaks"));
    }

    #[test]
    fn dotenv_parser_handles_quotes_comments_and_windows_paths() {
        let values = parse_dotenv(
            r#"
                # comment
                export GEMINI_API_KEY="secret value" # comment
                DATA_DIR='C:\market data'
                STT_CONCURRENCY=3 # worker limit
            "#,
        )
        .unwrap();

        assert_eq!(values["GEMINI_API_KEY"], "secret value");
        assert_eq!(values["DATA_DIR"], r"C:\market data");
        assert_eq!(values["STT_CONCURRENCY"], "3");
    }
}
