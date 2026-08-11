mod capture;
mod config;
mod dashboard;
mod gemini;
mod market_feed;
mod neon;
mod paper;
mod paper_runtime;
mod persistence;
mod runtime_logs;
mod scheduler;
mod stt;
mod trailing;

use std::{
    collections::HashMap,
    fs,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Asia::Kolkata;
use clap::{ArgAction, Parser, Subcommand};
use csv::ReaderBuilder;
use data_encoding::BASE32_NOPAD;
use futures_util::{SinkExt, StreamExt, future::pending};
use hmac::{Hmac, Mac};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha1::Sha1;
use tokio::time::{Instant, interval, sleep, sleep_until};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

const API_BASE: &str = "https://api.indstocks.com";
const WS_PRICES: &str = "wss://ws-prices.indstocks.com/api/v1/ws/prices";
const USER_AGENT: &str = "observer-market-manager/0.1";

#[derive(Parser, Debug)]
#[command(name = "market-manager")]
#[command(about = "Read-only INDstocks market-data engine for NIFTY and SENSEX options")]
struct Cli {
    /// Observer directory containing token.txt and totp.txt.
    #[arg(long, global = true)]
    observer_dir: Option<PathBuf>,

    /// Directory used for timestamped live JSONL and backtest CSV files.
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Validate external configuration and initialize/ping durable storage without starting workers.
    Doctor,

    /// Run the IST market-day supervisor and discover the configured channel live stream.
    Daemon,

    /// Run the live-edge multimodal paper trader and real-time dashboard.
    Paper {
        /// Public YouTube livestream URL. Capture begins at the current live edge.
        #[arg(long)]
        stream_url: String,

        /// Optional finite runtime for diagnostics; omit for continuous operation.
        #[arg(long)]
        duration_seconds: Option<u64>,
    },

    /// Subscribe to one or more live NIFTY/SENSEX option contracts.
    Live {
        /// Example: --contract "sensex 13 aug 2026 78800 pe". Repeat for multiple contracts.
        #[arg(long, action = ArgAction::Append, required = true)]
        contract: Vec<String>,

        /// Output one timestamped LTP sample per contract at this interval.
        #[arg(long, default_value_t = 10)]
        interval_seconds: u64,

        /// Optional finite duration, useful for connection tests.
        #[arg(long)]
        duration_seconds: Option<u64>,
    },

    /// Download a full trading day of broker-provided 10-second candles.
    Backtest {
        /// Example: --contract "sensex 13 aug 2026 78800 pe".
        #[arg(long)]
        contract: String,

        /// Trading date whose 09:15-15:30 IST candles should be fetched.
        #[arg(long)]
        date: NaiveDate,

        /// Optional output CSV path.
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Validate the token, generating a fresh one only if missing or rejected.
    AuthCheck,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Underlying {
    Nifty,
    Sensex,
}

impl Underlying {
    fn label(self) -> &'static str {
        match self {
            Self::Nifty => "NIFTY",
            Self::Sensex => "SENSEX",
        }
    }
}

impl FromStr for Underlying {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "NIFTY" | "NIFTY50" | "INFTY" => Ok(Self::Nifty),
            "SENSEX" => Ok(Self::Sensex),
            _ => bail!("instrument must be NIFTY or SENSEX"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OptionType {
    Ce,
    Pe,
}

impl OptionType {
    fn label(self) -> &'static str {
        match self {
            Self::Ce => "CE",
            Self::Pe => "PE",
        }
    }
}

impl FromStr for OptionType {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            "CE" | "CALL" => Ok(Self::Ce),
            "PE" | "PUT" => Ok(Self::Pe),
            _ => bail!("option type must be CE or PE"),
        }
    }
}

#[derive(Debug, Clone)]
struct ContractRequest {
    underlying: Underlying,
    expiry: NaiveDate,
    strike: f64,
    option_type: OptionType,
}

impl ContractRequest {
    fn parse(input: &str) -> Result<Self> {
        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.len() < 4 {
            bail!("invalid contract '{input}'; expected instrument + expiry + strike + PE/CE");
        }

        let underlying = parts[0].parse()?;
        let option_type = parts[parts.len() - 1].parse()?;
        let strike = parts[parts.len() - 2]
            .replace(',', "")
            .parse::<f64>()
            .with_context(|| format!("invalid strike in '{input}'"))?;
        let expiry_text = parts[1..parts.len() - 2].join(" ");
        let expiry = parse_user_date(&expiry_text)
            .with_context(|| format!("invalid expiry date in '{input}'"))?;

        Ok(Self {
            underlying,
            expiry,
            strike,
            option_type,
        })
    }

    fn label(&self) -> String {
        format!(
            "{} {} {} {}",
            self.underlying.label(),
            self.expiry
                .format("%d %b %Y")
                .to_string()
                .to_ascii_uppercase(),
            format_strike(self.strike),
            self.option_type.label()
        )
    }
}

fn parse_user_date(value: &str) -> Result<NaiveDate> {
    let normalized = value.trim();
    let formats = ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d %b %Y", "%d %B %Y"];
    for format in formats {
        if let Ok(date) = NaiveDate::parse_from_str(normalized, format) {
            return Ok(date);
        }
        if let Ok(date) = NaiveDate::parse_from_str(&title_case_words(normalized), format) {
            return Ok(date);
        }
    }
    bail!("unsupported date '{value}'")
}

fn title_case_words(value: &str) -> String {
    value
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => {
                    first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase()
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn format_strike(strike: f64) -> String {
    if strike.fract().abs() < f64::EPSILON {
        format!("{strike:.0}")
    } else {
        strike.to_string()
    }
}

#[derive(Debug, Deserialize)]
struct InstrumentRow {
    #[serde(rename = "EXCH")]
    exchange: String,
    #[serde(rename = "SECURITY_ID")]
    security_id: String,
    #[serde(rename = "TRADING_SYMBOL")]
    trading_symbol: String,
    #[serde(rename = "EXPIRY_DATE")]
    expiry_date: String,
    #[serde(rename = "STRIKE_PRICE")]
    strike_price: String,
    #[serde(rename = "OPTION_TYPE")]
    option_type: String,
}

#[derive(Debug, Clone)]
struct ResolvedContract {
    request: ContractRequest,
    trading_symbol: String,
    security_id: String,
    rest_code: String,
    websocket_code: String,
}

impl ResolvedContract {
    fn safe_filename(&self) -> String {
        self.trading_symbol
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect()
    }
}

#[derive(Clone)]
struct TokenManager {
    client: Client,
    token_file: PathBuf,
    credentials_file: PathBuf,
    direct_credentials: Option<(String, String, String)>,
}

enum TokenValidation {
    Valid,
    Rejected,
    Unavailable(String),
}

impl TokenManager {
    fn new(client: Client, observer_dir: &Path) -> Self {
        Self {
            client,
            token_file: observer_dir.join("token.txt"),
            credentials_file: observer_dir.join("totp.txt"),
            direct_credentials: None,
        }
    }

    fn with_paths(
        client: Client,
        token_file: PathBuf,
        credentials_file: PathBuf,
        direct_credentials: Option<(String, String, String)>,
    ) -> Self {
        Self {
            client,
            token_file,
            credentials_file,
            direct_credentials,
        }
    }

    async fn ensure_valid_token(&self) -> Result<String> {
        if let Ok(token) = fs::read_to_string(&self.token_file) {
            let token = token.trim().to_string();
            if !token.is_empty() {
                match self.validate(&token).await? {
                    TokenValidation::Valid => {
                        println!("INDstocks access token is valid.");
                        return Ok(token);
                    }
                    TokenValidation::Rejected => {
                        println!("Stored access token was rejected; generating one fresh token.");
                    }
                    TokenValidation::Unavailable(reason) => {
                        bail!("token could not be validated ({reason}); no new token was generated")
                    }
                }
            }
        } else {
            println!("No stored access token found; generating one fresh token.");
        }

        let token = self.generate_once().await?;
        match self.validate(&token).await? {
            TokenValidation::Valid => {
                atomic_write_secret(&self.token_file, &token)?;
                println!("Fresh access token generated, validated, and saved.");
                Ok(token)
            }
            TokenValidation::Rejected => {
                bail!("newly generated token was rejected; nothing was saved")
            }
            TokenValidation::Unavailable(reason) => {
                bail!("new token could not be validated ({reason}); nothing was saved")
            }
        }
    }

    async fn validate(&self, token: &str) -> Result<TokenValidation> {
        let response = self
            .client
            .get(format!("{API_BASE}/user/profile"))
            .header("Authorization", token)
            .send()
            .await
            .context("INDstocks profile validation request failed")?;

        Ok(match response.status() {
            StatusCode::OK | StatusCode::CREATED => TokenValidation::Valid,
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => TokenValidation::Rejected,
            status => TokenValidation::Unavailable(format!("HTTP {}", status.as_u16())),
        })
    }

    async fn generate_once(&self) -> Result<String> {
        let (client_id, mpin, secret) = match self.direct_credentials.clone() {
            Some(credentials) => credentials,
            None => read_totp_credentials(&self.credentials_file)?,
        };
        let code = current_totp(&secret)?;
        let response = self
            .client
            .post(format!("{API_BASE}/generate/token"))
            .header("x-api-key", client_id)
            .json(&json!({"mpin": mpin, "totp": code}))
            .send()
            .await
            .context("INDstocks token-generation request failed")?;

        let status = response.status();
        if status != StatusCode::OK && status != StatusCode::CREATED {
            bail!(
                "token generation failed with HTTP {}; no retry attempted",
                status.as_u16()
            );
        }

        let body: Value = response
            .json()
            .await
            .context("token-generation response was not valid JSON")?;
        extract_token(&body).ok_or_else(|| anyhow!("token-generation response contained no token"))
    }
}

fn read_totp_credentials(path: &Path) -> Result<(String, String, String)> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("could not read credential file {}", path.display()))?;
    let mut client_id = None;
    let mut mpin = None;
    let mut secret = None;

    for line in text.lines() {
        let Some((label, value)) = line.split_once([':', '=']) else {
            continue;
        };
        let label = label.trim().to_ascii_lowercase();
        let value = value.trim().to_string();
        if label.contains("client id") {
            client_id = Some(value);
        } else if label == "mpin" {
            mpin = Some(value);
        } else if label == "totp secret" {
            secret = Some(value);
        }
    }

    Ok((
        client_id.ok_or_else(|| anyhow!("totp.txt is missing client ID"))?,
        mpin.ok_or_else(|| anyhow!("totp.txt is missing MPIN"))?,
        secret.ok_or_else(|| anyhow!("totp.txt is missing TOTP secret"))?,
    ))
}

fn current_totp(secret: &str) -> Result<String> {
    totp_at(secret, Utc::now().timestamp() as u64)
}

fn totp_at(secret: &str, unix_seconds: u64) -> Result<String> {
    let normalized = secret
        .chars()
        .filter(|c| !c.is_whitespace() && *c != '=')
        .collect::<String>()
        .to_ascii_uppercase();
    let key = BASE32_NOPAD
        .decode(normalized.as_bytes())
        .context("TOTP secret is not valid base32")?;
    let counter = unix_seconds / 30;
    let mut mac = Hmac::<Sha1>::new_from_slice(&key).context("could not initialize TOTP")?;
    mac.update(&counter.to_be_bytes());
    let digest = mac.finalize().into_bytes();
    let offset = (digest[19] & 0x0f) as usize;
    let binary = ((digest[offset] as u32 & 0x7f) << 24)
        | ((digest[offset + 1] as u32) << 16)
        | ((digest[offset + 2] as u32) << 8)
        | digest[offset + 3] as u32;
    Ok(format!("{:06}", binary % 1_000_000))
}

fn extract_token(body: &Value) -> Option<String> {
    [
        body.pointer("/data/token"),
        body.pointer("/data/access_token"),
        body.get("token"),
        body.get("access_token"),
    ]
    .into_iter()
    .flatten()
    .find_map(Value::as_str)
    .map(str::trim)
    .filter(|value| !value.is_empty())
    .map(ToOwned::to_owned)
}

fn atomic_write_secret(path: &Path, value: &str) -> Result<()> {
    let temporary = path.with_extension("txt.tmp");
    fs::write(&temporary, value).with_context(|| {
        format!(
            "could not write temporary token file {}",
            temporary.display()
        )
    })?;
    fs::rename(&temporary, path)
        .with_context(|| format!("could not replace token file {}", path.display()))?;
    Ok(())
}

async fn fetch_instruments(client: &Client, token: &str) -> Result<Vec<InstrumentRow>> {
    let response = client
        .get(format!("{API_BASE}/market/instruments"))
        .query(&[("source", "fno")])
        .header("Authorization", token)
        .send()
        .await
        .context("instrument-master request failed")?;
    let status = response.status();
    if !status.is_success() {
        bail!(
            "instrument-master request failed with HTTP {}",
            status.as_u16()
        );
    }
    let bytes = response
        .bytes()
        .await
        .context("instrument-master download failed")?;
    let mut reader = ReaderBuilder::new().from_reader(bytes.as_ref());
    let mut instruments = Vec::new();
    for row in reader.deserialize() {
        instruments.push(row.context("invalid row in INDstocks instrument master")?);
    }
    Ok(instruments)
}

fn resolve_contract(rows: &[InstrumentRow], request: ContractRequest) -> Result<ResolvedContract> {
    let prefix = format!("{}-", request.underlying.label());
    let expected_option = request.option_type.label();

    let matches = rows
        .iter()
        .filter(|row| row.trading_symbol.to_ascii_uppercase().starts_with(&prefix))
        .filter(|row| row.option_type.eq_ignore_ascii_case(expected_option))
        .filter(|row| {
            row.strike_price
                .parse::<f64>()
                .map(|strike| (strike - request.strike).abs() < 0.0001)
                .unwrap_or(false)
        })
        .filter(|row| parse_instrument_expiry(&row.expiry_date) == Some(request.expiry))
        .collect::<Vec<_>>();

    if matches.is_empty() {
        bail!(
            "contract not found in instrument master: {}",
            request.label()
        );
    }
    if matches.len() > 1 {
        bail!(
            "contract resolved to multiple instrument rows: {}",
            request.label()
        );
    }

    let row = matches[0];
    let segment = if row.exchange.eq_ignore_ascii_case("BSE") {
        "BFO"
    } else if row.exchange.eq_ignore_ascii_case("NSE") {
        "NFO"
    } else {
        bail!(
            "unsupported derivatives exchange '{}'; only NSE/BSE are allowed",
            row.exchange
        );
    };

    Ok(ResolvedContract {
        request,
        trading_symbol: row.trading_symbol.clone(),
        security_id: row.security_id.clone(),
        rest_code: format!("{segment}_{}", row.security_id),
        websocket_code: format!("{segment}:{}", row.security_id),
    })
}

fn parse_instrument_expiry(value: &str) -> Option<NaiveDate> {
    ["%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d"]
        .into_iter()
        .find_map(|format| {
            NaiveDateTime::parse_from_str(value, format)
                .map(|date_time| date_time.date())
                .or_else(|_| NaiveDate::parse_from_str(value, format))
                .ok()
        })
}

#[derive(Debug, Clone, Serialize)]
struct Candle {
    timestamp_epoch_seconds: i64,
    timestamp_ist: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

async fn run_backtest(
    client: &Client,
    token: &str,
    contract: &ResolvedContract,
    date: NaiveDate,
    data_dir: &Path,
    output: Option<PathBuf>,
) -> Result<()> {
    let start = Kolkata
        .from_local_datetime(&date.and_hms_opt(9, 15, 0).unwrap())
        .single()
        .ok_or_else(|| anyhow!("invalid market-open timestamp"))?;
    let end = Kolkata
        .from_local_datetime(&date.and_hms_opt(15, 30, 1).unwrap())
        .single()
        .ok_or_else(|| anyhow!("invalid market-close timestamp"))?;

    let response = client
        .get(format!("{API_BASE}/market/historical/10second"))
        .query(&[
            ("scrip-codes", contract.rest_code.clone()),
            ("start_time", start.timestamp_millis().to_string()),
            ("end_time", end.timestamp_millis().to_string()),
        ])
        .header("Authorization", token)
        .send()
        .await
        .context("10-second historical request failed")?;
    let status = response.status();
    if !status.is_success() {
        bail!(
            "10-second historical request failed with HTTP {}",
            status.as_u16()
        );
    }

    let body: Value = response
        .json()
        .await
        .context("historical response was not JSON")?;
    if body.get("success").and_then(Value::as_bool) == Some(false) {
        bail!("INDstocks reported that the historical request failed");
    }
    let raw_candles = body
        .pointer(&format!("/data/{}/candles", contract.rest_code))
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("historical response contained no candle array"))?;
    let mut candles = raw_candles
        .iter()
        .map(parse_candle)
        .collect::<Result<Vec<_>>>()?;
    candles.sort_by_key(|candle| candle.timestamp_epoch_seconds);

    let wrong_date = candles.iter().find(|candle| {
        DateTime::<Utc>::from_timestamp(candle.timestamp_epoch_seconds, 0)
            .map(|timestamp| timestamp.with_timezone(&Kolkata).date_naive() != date)
            .unwrap_or(true)
    });
    if let Some(candle) = wrong_date {
        bail!(
            "INDstocks returned {} data for requested date {}; refusing to save mislabeled backtest data",
            candle.timestamp_ist,
            date
        );
    }
    if let (Some(first), Some(last)) = (candles.first(), candles.last()) {
        let first_time = DateTime::<Utc>::from_timestamp(first.timestamp_epoch_seconds, 0)
            .unwrap()
            .with_timezone(&Kolkata)
            .time();
        let last_time = DateTime::<Utc>::from_timestamp(last.timestamp_epoch_seconds, 0)
            .unwrap()
            .with_timezone(&Kolkata)
            .time();
        if first_time > chrono::NaiveTime::from_hms_opt(9, 16, 0).unwrap()
            || last_time < chrono::NaiveTime::from_hms_opt(15, 29, 0).unwrap()
        {
            bail!(
                "INDstocks returned only a partial 10-second window ({} through {} IST); refusing to label it as a full-day backtest",
                first_time,
                last_time
            );
        }
    }

    let output = output.unwrap_or_else(|| {
        data_dir.join("backtest").join(format!(
            "{}_{}_10second.csv",
            contract.safe_filename(),
            date.format("%Y%m%d")
        ))
    });
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("could not create output directory {}", parent.display()))?;
    }
    let mut writer = csv::Writer::from_path(&output)
        .with_context(|| format!("could not create {}", output.display()))?;
    for candle in &candles {
        writer.serialize(candle)?;
    }
    writer.flush()?;

    println!(
        "Saved {} broker-provided 10-second candles for {} to {}.",
        candles.len(),
        contract.request.label(),
        output.display()
    );
    if candles.is_empty() {
        println!("The broker returned no trades/candles for this contract and date.");
    }
    Ok(())
}

fn parse_candle(value: &Value) -> Result<Candle> {
    let (timestamp, open, high, low, close, volume) = if let Some(object) = value.as_object() {
        (
            object.get("ts").and_then(Value::as_i64),
            object.get("o").and_then(Value::as_f64),
            object.get("h").and_then(Value::as_f64),
            object.get("l").and_then(Value::as_f64),
            object.get("c").and_then(Value::as_f64),
            object.get("v").and_then(number_as_f64),
        )
    } else if let Some(array) = value.as_array() {
        (
            array.first().and_then(Value::as_i64),
            array.get(1).and_then(number_as_f64),
            array.get(2).and_then(number_as_f64),
            array.get(3).and_then(number_as_f64),
            array.get(4).and_then(number_as_f64),
            array.get(5).and_then(number_as_f64),
        )
    } else {
        bail!("unexpected candle shape")
    };

    let mut timestamp = timestamp.ok_or_else(|| anyhow!("candle missing timestamp"))?;
    if timestamp > 10_000_000_000 {
        timestamp /= 1000;
    }
    let instant = DateTime::<Utc>::from_timestamp(timestamp, 0)
        .ok_or_else(|| anyhow!("invalid candle timestamp {timestamp}"))?
        .with_timezone(&Kolkata);

    Ok(Candle {
        timestamp_epoch_seconds: timestamp,
        timestamp_ist: instant.to_rfc3339(),
        open: open.ok_or_else(|| anyhow!("candle missing open"))?,
        high: high.ok_or_else(|| anyhow!("candle missing high"))?,
        low: low.ok_or_else(|| anyhow!("candle missing low"))?,
        close: close.ok_or_else(|| anyhow!("candle missing close"))?,
        volume: volume.unwrap_or(0.0),
    })
}

fn number_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|number| number as f64))
}

#[derive(Debug, Clone)]
struct LatestTick {
    exchange_timestamp_ms: i64,
    received_timestamp_ms: i64,
    ltp: f64,
}

#[derive(Serialize)]
struct LiveSample<'a> {
    sample_timestamp_ms: i64,
    sample_timestamp_ist: String,
    contract: String,
    trading_symbol: &'a str,
    scrip_code: &'a str,
    exchange_timestamp_ms: Option<i64>,
    received_timestamp_ms: Option<i64>,
    ltp: Option<f64>,
    stale: bool,
}

enum LiveOutcome {
    Complete,
    Reconnect,
}

struct LiveFileManager {
    session_writer: BufWriter<fs::File>,
    session_path: PathBuf,
    live_dir: PathBuf,
    daily_date: NaiveDate,
    daily_writer: BufWriter<fs::File>,
    daily_path: PathBuf,
}

impl LiveFileManager {
    fn new(live_dir: &Path) -> Result<Self> {
        fs::create_dir_all(live_dir)?;
        let session_path =
            live_dir.join(Utc::now().format("live_%Y%m%dT%H%M%SZ.jsonl").to_string());
        let session_writer = BufWriter::new(fs::File::create(&session_path)?);
        let daily_date = Utc::now().with_timezone(&Kolkata).date_naive();
        let (daily_path, daily_writer) = open_daily_live_file(live_dir, daily_date)?;
        Ok(Self {
            session_writer,
            session_path,
            live_dir: live_dir.to_path_buf(),
            daily_date,
            daily_writer,
            daily_path,
        })
    }

    fn write_sample(&mut self, sampled_at: DateTime<Utc>, sample: &LiveSample<'_>) -> Result<()> {
        self.roll_daily_file_if_needed(sampled_at)?;
        serde_json::to_writer(&mut self.session_writer, sample)?;
        self.session_writer.write_all(b"\n")?;
        serde_json::to_writer(&mut self.daily_writer, sample)?;
        self.daily_writer.write_all(b"\n")?;
        Ok(())
    }

    fn roll_daily_file_if_needed(&mut self, sampled_at: DateTime<Utc>) -> Result<()> {
        let date = sampled_at.with_timezone(&Kolkata).date_naive();
        if date != self.daily_date {
            self.daily_writer.flush()?;
            let (path, writer) = open_daily_live_file(&self.live_dir, date)?;
            self.daily_date = date;
            self.daily_path = path;
            self.daily_writer = writer;
            println!(
                "Daily live file rolled over to {}.",
                self.daily_path.display()
            );
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.session_writer.flush()?;
        self.daily_writer.flush()?;
        Ok(())
    }
}

fn open_daily_live_file(
    live_dir: &Path,
    date: NaiveDate,
) -> Result<(PathBuf, BufWriter<fs::File>)> {
    let path = live_dir.join(format!("live ({}).txt", date.format("%Y-%m-%d")));
    let file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("could not open daily live file {}", path.display()))?;
    Ok((path, BufWriter::new(file)))
}

async fn run_live(
    token_manager: &TokenManager,
    contracts: Vec<ResolvedContract>,
    data_dir: &Path,
    sample_seconds: u64,
    duration_seconds: Option<u64>,
) -> Result<()> {
    if sample_seconds == 0 {
        bail!("interval-seconds must be greater than zero");
    }
    let live_dir = data_dir.join("live");
    let mut writer = LiveFileManager::new(&live_dir)?;
    let started = Instant::now();
    let deadline = duration_seconds.map(|seconds| started + Duration::from_secs(seconds));
    let mut reconnect_delay = 1u64;

    println!(
        "Session live samples will be written to {}.",
        writer.session_path.display()
    );
    println!(
        "Daily live samples will continuously append to {}.",
        writer.daily_path.display()
    );
    loop {
        let token = token_manager.ensure_valid_token().await?;
        match live_session(
            token_manager,
            &token,
            &contracts,
            &mut writer,
            sample_seconds,
            deadline,
        )
        .await
        {
            Ok(LiveOutcome::Complete) => {
                writer.flush()?;
                return Ok(());
            }
            Ok(LiveOutcome::Reconnect) => {
                if deadline.is_some_and(|end| Instant::now() >= end) {
                    writer.flush()?;
                    return Ok(());
                }
                println!("Live feed disconnected; reconnecting in {reconnect_delay} seconds.");
                sleep(Duration::from_secs(reconnect_delay)).await;
                reconnect_delay = (reconnect_delay * 2).min(30);
            }
            Err(error) => {
                if deadline.is_some_and(|end| Instant::now() >= end) {
                    writer.flush()?;
                    return Ok(());
                }
                println!("Live feed error: {error:#}");
                println!("Reconnecting in {reconnect_delay} seconds.");
                sleep(Duration::from_secs(reconnect_delay)).await;
                reconnect_delay = (reconnect_delay * 2).min(30);
            }
        }
    }
}

async fn live_session(
    token_manager: &TokenManager,
    token: &str,
    contracts: &[ResolvedContract],
    writer: &mut LiveFileManager,
    sample_seconds: u64,
    deadline: Option<Instant>,
) -> Result<LiveOutcome> {
    let mut request = WS_PRICES.into_client_request()?;
    request.headers_mut().insert(
        "Authorization",
        token.parse().context("invalid token header")?,
    );
    request
        .headers_mut()
        .insert("User-Agent", USER_AGENT.parse().unwrap());

    let (mut websocket, _) = connect_async(request)
        .await
        .context("INDstocks WebSocket handshake failed")?;
    let subscription = json!({
        "action": "subscribe",
        "mode": "ltp",
        "instruments": contracts.iter().map(|contract| contract.websocket_code.clone()).collect::<Vec<_>>()
    });
    websocket
        .send(Message::Text(subscription.to_string().into()))
        .await?;
    println!(
        "Connected and subscribed to {} contract(s).",
        contracts.len()
    );

    let by_security_id = contracts
        .iter()
        .enumerate()
        .map(|(index, contract)| (contract.security_id.clone(), index))
        .collect::<HashMap<_, _>>();
    let mut latest: HashMap<usize, LatestTick> = HashMap::new();
    let mut sampler = interval(Duration::from_secs(sample_seconds));
    sampler.tick().await;
    let mut auth_check = interval(Duration::from_secs(15 * 60));
    auth_check.tick().await;
    let deadline_wait = async {
        match deadline {
            Some(deadline) => sleep_until(deadline).await,
            None => pending::<()>().await,
        }
    };
    tokio::pin!(deadline_wait);

    loop {
        tokio::select! {
            _ = &mut deadline_wait => return Ok(LiveOutcome::Complete),
            _ = auth_check.tick() => {
                match token_manager.validate(token).await? {
                    TokenValidation::Valid => {}
                    TokenValidation::Rejected => return Ok(LiveOutcome::Reconnect),
                    TokenValidation::Unavailable(reason) => {
                        println!("Periodic token check unavailable ({reason}); keeping the active feed.");
                    }
                }
            }
            _ = sampler.tick() => {
                let sampled_at = Utc::now();
                for (index, contract) in contracts.iter().enumerate() {
                    let tick = latest.get(&index);
                    let stale = tick
                        .map(|tick| sampled_at.timestamp_millis() - tick.received_timestamp_ms > (sample_seconds as i64 * 2_000))
                        .unwrap_or(true);
                    let sample = LiveSample {
                        sample_timestamp_ms: sampled_at.timestamp_millis(),
                        sample_timestamp_ist: sampled_at.with_timezone(&Kolkata).to_rfc3339(),
                        contract: contract.request.label(),
                        trading_symbol: &contract.trading_symbol,
                        scrip_code: &contract.rest_code,
                        exchange_timestamp_ms: tick.map(|value| value.exchange_timestamp_ms),
                        received_timestamp_ms: tick.map(|value| value.received_timestamp_ms),
                        ltp: tick.map(|value| value.ltp),
                        stale,
                    };
                    writer.write_sample(sampled_at, &sample)?;
                    println!(
                        "{} | {} | LTP {}{}",
                        sample.sample_timestamp_ist,
                        sample.contract,
                        sample.ltp.map(|ltp| ltp.to_string()).unwrap_or_else(|| "waiting".to_string()),
                        if sample.stale { " (stale/no tick)" } else { "" }
                    );
                }
                writer.flush()?;
            }
            message = websocket.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(value) = serde_json::from_str::<Value>(&text)
                            && let (Some(instrument), Some(ltp)) = (
                                value.get("instrument").and_then(Value::as_str),
                                value.pointer("/data/ltp").and_then(number_as_f64),
                            )
                            && let Some(index) = by_security_id.get(instrument)
                        {
                            latest.insert(*index, LatestTick {
                                exchange_timestamp_ms: value
                                    .get("timestamp")
                                    .and_then(Value::as_i64)
                                    .unwrap_or_default(),
                                received_timestamp_ms: Utc::now().timestamp_millis(),
                                ltp,
                            });
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => websocket.send(Message::Pong(payload)).await?,
                    Some(Ok(Message::Close(_))) | None => return Ok(LiveOutcome::Reconnect),
                    Some(Err(error)) => return Err(error.into()),
                    _ => {}
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let project_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let observer_dir = cli.observer_dir.unwrap_or_else(|| {
        project_dir
            .parent()
            .unwrap_or(project_dir.as_path())
            .to_path_buf()
    });
    let data_dir = cli.data_dir.unwrap_or_else(|| project_dir.join("data"));
    let client = Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(30))
        .build()?;
    let token_manager = TokenManager::new(client.clone(), &observer_dir);

    match cli.command {
        Command::Doctor => {
            let config = config::AppConfig::load(&project_dir)?;
            let database_url = config
                .database
                .url
                .as_ref()
                .ok_or_else(|| anyhow!("DATABASE_URL is required for Render readiness"))?;
            let store = neon::NeonStore::connect(database_url.expose_secret()).await?;
            store.ping().await?;
            println!(
                "Configuration ready: {} Gemini slot(s), {} ElevenLabs slot(s), Neon healthy, channel configured: {}.",
                config.gemini.api_keys.len(),
                config.elevenlabs.api_keys.len(),
                config.scheduler.youtube_channel_url.is_some(),
            );
        }
        Command::Daemon => {
            scheduler::run(&project_dir, client).await?;
        }
        Command::Paper {
            stream_url,
            duration_seconds,
        } => {
            paper_runtime::run(&project_dir, stream_url, client, duration_seconds).await?;
        }
        Command::AuthCheck => {
            token_manager.ensure_valid_token().await?;
        }
        Command::Backtest {
            contract,
            date,
            output,
        } => {
            let request = ContractRequest::parse(&contract)?;
            let token = token_manager.ensure_valid_token().await?;
            let instruments = fetch_instruments(&client, &token).await?;
            let resolved = resolve_contract(&instruments, request)?;
            println!(
                "Resolved {} as {}.",
                resolved.request.label(),
                resolved.rest_code
            );
            run_backtest(&client, &token, &resolved, date, &data_dir, output).await?;
        }
        Command::Live {
            contract,
            interval_seconds,
            duration_seconds,
        } => {
            let requests = contract
                .iter()
                .map(|value| ContractRequest::parse(value))
                .collect::<Result<Vec<_>>>()?;
            let token = token_manager.ensure_valid_token().await?;
            let instruments = fetch_instruments(&client, &token).await?;
            let contracts = requests
                .into_iter()
                .map(|request| resolve_contract(&instruments, request))
                .collect::<Result<Vec<_>>>()?;
            for contract in &contracts {
                println!(
                    "Resolved {} as {}.",
                    contract.request.label(),
                    contract.websocket_code
                );
            }
            run_live(
                &token_manager,
                contracts,
                &data_dir,
                interval_seconds,
                duration_seconds,
            )
            .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_contract_example() {
        let contract = ContractRequest::parse("sensex 13 aug 2026 78800 pe").unwrap();
        assert_eq!(contract.underlying, Underlying::Sensex);
        assert_eq!(
            contract.expiry,
            NaiveDate::from_ymd_opt(2026, 8, 13).unwrap()
        );
        assert_eq!(contract.strike, 78_800.0);
        assert_eq!(contract.option_type, OptionType::Pe);
    }

    #[test]
    fn totp_matches_rfc_vector_truncated_to_six_digits() {
        let code = totp_at("GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ", 59).unwrap();
        assert_eq!(code, "287082");
    }

    #[test]
    fn parses_indstocks_expiry() {
        assert_eq!(
            parse_instrument_expiry("08/13/2026 14:00"),
            NaiveDate::from_ymd_opt(2026, 8, 13)
        );
    }
}
