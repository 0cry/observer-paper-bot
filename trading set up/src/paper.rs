//! Deterministic, paper-only execution and portfolio accounting.
//!
//! All currency and option prices are represented as integer paise.  The
//! broker deliberately has no network or real-order API; callers feed it
//! validated market ticks and persist/serve its serializable state.

#![allow(dead_code)]

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

pub type Paise = i64;
pub type TimestampMs = i64;

pub const PAISE_PER_RUPEE: Paise = 100;
pub const STATE_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_PENDING_ENTRY_TTL_MS: TimestampMs = 60_000;

const fn default_pending_entry_ttl_ms() -> TimestampMs {
    DEFAULT_PENDING_ENTRY_TTL_MS
}

pub const fn rupees(value: i64) -> Paise {
    value * PAISE_PER_RUPEE
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ShadowMode {
    LlmExit,
    MovingSl,
}

impl ShadowMode {
    pub const ALL: [Self; 2] = [Self::LlmExit, Self::MovingSl];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Underlying {
    Nifty,
    Sensex,
}

impl Underlying {
    pub const fn lot_size(self) -> u32 {
        match self {
            Self::Nifty => 65,
            Self::Sensex => 20,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OptionKind {
    Ce,
    Pe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OptionContract {
    /// Stable key used by the live market-data subscription.
    pub instrument_id: String,
    pub trading_symbol: String,
    pub underlying: Underlying,
    /// ISO-8601 expiry (`YYYY-MM-DD`).
    pub expiry: String,
    pub strike_paise: Paise,
    pub option_kind: OptionKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeLevels {
    pub entry_paise: Paise,
    pub hard_sl_paise: Paise,
    pub t1_paise: Paise,
    pub t2_paise: Option<Paise>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradeSetup {
    /// Upstream may supply an ID.  If empty, the broker derives a stable FNV-1a
    /// ID from the contract, evidence timestamp, and levels.
    pub setup_id: String,
    pub contract: OptionContract,
    pub side: TradeSide,
    pub levels: TradeLevels,
    pub evidence_timestamp_ms: TimestampMs,
    pub received_timestamp_ms: TimestampMs,
}

impl TradeSetup {
    pub fn ensure_stable_id(&mut self) {
        if self.setup_id.trim().is_empty() {
            self.setup_id = self.derived_stable_id();
        } else {
            self.setup_id = self.setup_id.trim().to_string();
        }
    }

    pub fn derived_stable_id(&self) -> String {
        let canonical = format!(
            "{}|{:?}|{}|{}|{:?}|{:?}|{}|{}|{}|{}|{}",
            self.contract.instrument_id.trim(),
            self.contract.underlying,
            self.contract.expiry.trim(),
            self.contract.strike_paise,
            self.contract.option_kind,
            self.side,
            self.evidence_timestamp_ms,
            self.levels.entry_paise,
            self.levels.hard_sl_paise,
            self.levels.t1_paise,
            self.levels.t2_paise.unwrap_or(-1),
        );
        format!("setup-{:016x}", fnv1a_64(canonical.as_bytes()))
    }
}

fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    bytes.iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSpec {
    pub account_id: String,
    pub display_name: String,
    pub starting_capital_paise: Paise,
}

pub fn default_account_specs() -> Vec<AccountSpec> {
    [5_000_i64, 10_000, 2_000, 15_000, 20_000]
        .into_iter()
        .enumerate()
        .map(|(index, capital)| AccountSpec {
            account_id: format!("account-{:02}", index + 1),
            display_name: format!("Account {} (INR {capital})", index + 1),
            starting_capital_paise: rupees(capital),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperBrokerConfig {
    pub entry_buffer_paise: Paise,
    pub entry_charge_paise: Paise,
    pub exit_charge_paise: Paise,
    /// Maximum wall-clock lifetime of an unfilled entry order. The serde
    /// default keeps snapshots written before this setting was introduced
    /// restorable with the conservative 60-second lifetime.
    #[serde(default = "default_pending_entry_ttl_ms")]
    pub pending_entry_ttl_ms: TimestampMs,
    pub maximum_tick_age_ms: TimestampMs,
    pub maximum_future_skew_ms: TimestampMs,
    pub event_capacity: usize,
}

impl Default for PaperBrokerConfig {
    fn default() -> Self {
        Self {
            entry_buffer_paise: rupees(2),
            entry_charge_paise: rupees(20),
            exit_charge_paise: rupees(20),
            pending_entry_ttl_ms: DEFAULT_PENDING_ENTRY_TTL_MS,
            maximum_tick_age_ms: 5_000,
            maximum_future_skew_ms: 1_000,
            event_capacity: 4_096,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketTick {
    pub instrument_id: String,
    pub ltp_paise: Paise,
    pub exchange_timestamp_ms: TimestampMs,
    pub received_timestamp_ms: TimestampMs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExitReason {
    Llm,
    HardStop,
    MovingStop,
    EndOfDay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    SetupAccepted,
    SetupRejected,
    DuplicateSetupIgnored,
    EntryOrderPlaced,
    EntryOrderCancelled,
    EntryFilled,
    LevelsUpdated,
    LevelsUpdateRejected,
    LlmExitQueued,
    LlmExitRejected,
    StopUpdated,
    PositionClosed,
    EndOfDayStarted,
    TradingDayStarted,
    TickRejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerEvent {
    pub sequence: u64,
    pub timestamp_ms: TimestampMs,
    pub event_type: EventType,
    pub mode: Option<ShadowMode>,
    pub account_id: Option<String>,
    pub setup_id: Option<String>,
    pub instrument_id: Option<String>,
    pub quantity: Option<u32>,
    pub price_paise: Option<Paise>,
    pub amount_paise: Option<Paise>,
    pub exit_reason: Option<ExitReason>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingEntry {
    pub order_id: String,
    pub setup_id: String,
    pub contract: OptionContract,
    pub levels: TradeLevels,
    pub lots: u32,
    pub quantity: u32,
    pub trigger_cap_paise: Paise,
    pub reserved_paise: Paise,
    pub created_timestamp_ms: TimestampMs,
    pub evidence_timestamp_ms: TimestampMs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitRequest {
    pub requested_timestamp_ms: TimestampMs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenPosition {
    pub position_id: String,
    pub setup_id: String,
    pub contract: OptionContract,
    pub levels: TradeLevels,
    pub lots: u32,
    pub quantity: u32,
    pub entry_price_paise: Paise,
    pub entry_charge_paise: Paise,
    pub effective_sl_paise: Paise,
    pub last_ltp_paise: Paise,
    pub maximum_ltp_paise: Paise,
    pub minimum_ltp_paise: Paise,
    pub opened_timestamp_ms: TimestampMs,
    pub last_tick_timestamp_ms: TimestampMs,
    pub llm_exit_request: Option<ExitRequest>,
}

impl OpenPosition {
    pub fn gross_unrealized_pnl_paise(&self) -> Paise {
        (self.last_ltp_paise - self.entry_price_paise) * i64::from(self.quantity)
    }

    pub fn net_unrealized_pnl_paise(&self, exit_charge_paise: Paise) -> Paise {
        self.gross_unrealized_pnl_paise() - self.entry_charge_paise - exit_charge_paise
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedTrade {
    pub trade_id: String,
    pub mode: ShadowMode,
    pub account_id: String,
    pub setup_id: String,
    pub contract: OptionContract,
    pub lots: u32,
    pub quantity: u32,
    pub entry_price_paise: Paise,
    pub exit_price_paise: Paise,
    pub entry_charge_paise: Paise,
    pub exit_charge_paise: Paise,
    pub gross_pnl_paise: Paise,
    pub net_pnl_paise: Paise,
    pub opened_timestamp_ms: TimestampMs,
    pub closed_timestamp_ms: TimestampMs,
    pub exit_reason: ExitReason,
    pub maximum_ltp_paise: Paise,
    pub minimum_ltp_paise: Paise,
    pub final_sl_paise: Paise,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AccountState {
    account_id: String,
    display_name: String,
    starting_capital_paise: Paise,
    cash_balance_paise: Paise,
    realized_pnl_paise: Paise,
    pending_entries: BTreeMap<String, PendingEntry>,
    open_positions: BTreeMap<String, OpenPosition>,
    closed_trades: Vec<ClosedTrade>,
}

impl AccountState {
    fn reserved_pending_paise(&self) -> Paise {
        self.pending_entries
            .values()
            .map(|order| order.reserved_paise)
            .sum()
    }

    fn reserved_exit_paise(&self, exit_charge_paise: Paise) -> Paise {
        exit_charge_paise * self.open_positions.len() as i64
    }

    fn free_cash_paise(&self, exit_charge_paise: Paise) -> Paise {
        self.cash_balance_paise
            - self.reserved_pending_paise()
            - self.reserved_exit_paise(exit_charge_paise)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ShadowBook {
    mode: ShadowMode,
    accounts: BTreeMap<String, AccountState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperBroker {
    pub schema_version: u32,
    pub config: PaperBrokerConfig,
    setup_registry: BTreeMap<String, TradeSetup>,
    books: BTreeMap<ShadowMode, ShadowBook>,
    latest_ticks: BTreeMap<String, MarketTick>,
    events: VecDeque<BrokerEvent>,
    next_event_sequence: u64,
    end_of_day_timestamp_ms: Option<TimestampMs>,
    /// Exchange trading date in Asia/Kolkata (`YYYY-MM-DD`).  This field is
    /// optional so v1 snapshots written before trading-day tracking was added
    /// remain readable and can be annotated safely on their first restore.
    #[serde(default)]
    trading_date_ist: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaperError {
    InvalidConfig(String),
    InvalidAccount(String),
    InvalidState(String),
    ArithmeticOverflow,
}

impl fmt::Display for PaperError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(formatter, "invalid broker config: {message}"),
            Self::InvalidAccount(message) => write!(formatter, "invalid account: {message}"),
            Self::InvalidState(message) => write!(formatter, "invalid broker state: {message}"),
            Self::ArithmeticOverflow => write!(formatter, "paper broker arithmetic overflow"),
        }
    }
}

impl std::error::Error for PaperError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PlacementStatus {
    Accepted,
    Duplicate,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementResult {
    pub setup_id: String,
    pub status: PlacementStatus,
    pub orders_placed: usize,
    pub rejection_reason: Option<String>,
    pub events: Vec<BrokerEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TickRejection {
    EmptyInstrument,
    InvalidPrice,
    Stale,
    FromFuture,
    OutOfOrder,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickResult {
    pub accepted: bool,
    pub rejection: Option<TickRejection>,
    pub entries_filled: usize,
    pub positions_closed: usize,
    pub events: Vec<BrokerEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovingStopContext {
    pub mode: ShadowMode,
    pub account_id: String,
    pub setup_id: String,
    pub contract: OptionContract,
    pub levels: TradeLevels,
    pub quantity: u32,
    pub entry_price_paise: Paise,
    pub current_stop_paise: Paise,
    pub current_ltp_paise: Paise,
    pub maximum_ltp_paise: Paise,
    pub opened_timestamp_ms: TimestampMs,
    pub tick_timestamp_ms: TimestampMs,
}

/// Implemented by the independent moving-SL module.  Returned stops are
/// monotonic-clamped by the broker and never allowed below the hard stop.
pub trait MovingStopPolicy {
    fn next_stop_paise(&mut self, context: &MovingStopContext) -> Option<Paise>;
}

impl<F> MovingStopPolicy for F
where
    F: FnMut(&MovingStopContext) -> Option<Paise>,
{
    fn next_stop_paise(&mut self, context: &MovingStopContext) -> Option<Paise> {
        self(context)
    }
}

#[derive(Debug, Default)]
pub struct NoMovingStop;

impl MovingStopPolicy for NoMovingStop {
    fn next_stop_paise(&mut self, _context: &MovingStopContext) -> Option<Paise> {
        None
    }
}

#[derive(Debug, Clone)]
struct EventDraft {
    timestamp_ms: TimestampMs,
    event_type: EventType,
    mode: Option<ShadowMode>,
    account_id: Option<String>,
    setup_id: Option<String>,
    instrument_id: Option<String>,
    quantity: Option<u32>,
    price_paise: Option<Paise>,
    amount_paise: Option<Paise>,
    exit_reason: Option<ExitReason>,
    message: String,
}

impl EventDraft {
    fn simple(
        timestamp_ms: TimestampMs,
        event_type: EventType,
        message: impl Into<String>,
    ) -> Self {
        Self {
            timestamp_ms,
            event_type,
            mode: None,
            account_id: None,
            setup_id: None,
            instrument_id: None,
            quantity: None,
            price_paise: None,
            amount_paise: None,
            exit_reason: None,
            message: message.into(),
        }
    }
}

impl PaperBroker {
    pub fn new(config: PaperBrokerConfig) -> Result<Self, PaperError> {
        Self::with_accounts(config, default_account_specs())
    }

    pub fn with_accounts(
        config: PaperBrokerConfig,
        account_specs: Vec<AccountSpec>,
    ) -> Result<Self, PaperError> {
        validate_config(&config)?;
        if account_specs.is_empty() {
            return Err(PaperError::InvalidAccount(
                "at least one account is required".to_string(),
            ));
        }

        let mut account_template = BTreeMap::new();
        for spec in account_specs {
            let account_id = spec.account_id.trim().to_string();
            if account_id.is_empty() {
                return Err(PaperError::InvalidAccount(
                    "account_id cannot be empty".to_string(),
                ));
            }
            if spec.starting_capital_paise <= 0 {
                return Err(PaperError::InvalidAccount(format!(
                    "{account_id} must have positive starting capital"
                )));
            }
            if account_template.contains_key(&account_id) {
                return Err(PaperError::InvalidAccount(format!(
                    "duplicate account_id {account_id}"
                )));
            }
            account_template.insert(
                account_id.clone(),
                AccountState {
                    account_id,
                    display_name: spec.display_name,
                    starting_capital_paise: spec.starting_capital_paise,
                    cash_balance_paise: spec.starting_capital_paise,
                    realized_pnl_paise: 0,
                    pending_entries: BTreeMap::new(),
                    open_positions: BTreeMap::new(),
                    closed_trades: Vec::new(),
                },
            );
        }

        let books = ShadowMode::ALL
            .into_iter()
            .map(|mode| {
                (
                    mode,
                    ShadowBook {
                        mode,
                        accounts: account_template.clone(),
                    },
                )
            })
            .collect();

        Ok(Self {
            schema_version: STATE_SCHEMA_VERSION,
            config,
            setup_registry: BTreeMap::new(),
            books,
            latest_ticks: BTreeMap::new(),
            events: VecDeque::new(),
            next_event_sequence: 1,
            end_of_day_timestamp_ms: None,
            trading_date_ist: None,
        })
    }

    /// Restores a broker serialized as its full [`PaperBroker`] state.
    ///
    /// The caller must provide the currently configured accounts and broker
    /// settings.  Restoration fails closed when either differs from the state
    /// on disk; balances are never silently transplanted into another account
    /// layout or interpreted with different fees/risk settings.
    pub fn restore_from_persisted(
        persisted: Self,
        expected_config: PaperBrokerConfig,
        expected_accounts: Vec<AccountSpec>,
    ) -> Result<Self, PaperError> {
        let expected = Self::with_accounts(expected_config, expected_accounts)?;
        if persisted.config != expected.config {
            return Err(PaperError::InvalidState(
                "persisted broker config does not match the active config".to_string(),
            ));
        }

        persisted.validate_restored_state()?;
        for mode in ShadowMode::ALL {
            let restored_book = persisted
                .books
                .get(&mode)
                .expect("validated broker contains both books");
            let expected_book = expected
                .books
                .get(&mode)
                .expect("new broker contains both books");
            if restored_book.accounts.len() != expected_book.accounts.len() {
                return Err(PaperError::InvalidState(format!(
                    "persisted account set does not match active config for {mode:?}"
                )));
            }
            for (account_id, expected_account) in &expected_book.accounts {
                let restored_account = restored_book.accounts.get(account_id).ok_or_else(|| {
                    PaperError::InvalidState(format!(
                        "persisted state is missing configured account {mode:?}/{account_id}"
                    ))
                })?;
                if restored_account.display_name != expected_account.display_name
                    || restored_account.starting_capital_paise
                        != expected_account.starting_capital_paise
                {
                    return Err(PaperError::InvalidState(format!(
                        "persisted identity/capital differs for {mode:?}/{account_id}"
                    )));
                }
            }
        }
        Ok(persisted)
    }

    /// Verifies every persisted accounting and lifecycle invariant before a
    /// deserialized broker is admitted back into the live runtime.
    pub fn validate_restored_state(&self) -> Result<(), PaperError> {
        if self.schema_version != STATE_SCHEMA_VERSION {
            return Err(PaperError::InvalidState(format!(
                "unsupported schema version {}",
                self.schema_version
            )));
        }
        validate_config(&self.config)?;
        if self.books.len() != ShadowMode::ALL.len() {
            return Err(PaperError::InvalidState(
                "broker must contain exactly the two shadow books".to_string(),
            ));
        }
        if self.next_event_sequence == 0 || self.next_event_sequence == u64::MAX {
            return Err(PaperError::InvalidState(
                "event sequence is exhausted or invalid".to_string(),
            ));
        }
        if self.end_of_day_timestamp_ms.is_some_and(|value| value <= 0) {
            return Err(PaperError::InvalidState(
                "end-of-day timestamp must be positive".to_string(),
            ));
        }
        if let Some(date) = &self.trading_date_ist {
            validate_trading_date_ist(date)?;
        }

        validate_setup_registry(&self.setup_registry)?;
        validate_latest_ticks(&self.latest_ticks)?;
        validate_event_buffer(
            &self.events,
            self.next_event_sequence,
            self.config.event_capacity,
        )?;

        let mut reference_accounts: Option<BTreeMap<String, (String, Paise)>> = None;
        for mode in ShadowMode::ALL {
            let book = self
                .books
                .get(&mode)
                .ok_or_else(|| PaperError::InvalidState(format!("missing {mode:?} book")))?;
            if book.mode != mode {
                return Err(PaperError::InvalidState(format!(
                    "book key/mode mismatch for {mode:?}"
                )));
            }
            if book.accounts.is_empty() {
                return Err(PaperError::InvalidState(format!(
                    "{mode:?} book has no accounts"
                )));
            }
            let identities = book
                .accounts
                .iter()
                .map(|(account_id, account)| {
                    (
                        account_id.clone(),
                        (account.display_name.clone(), account.starting_capital_paise),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            if let Some(reference) = &reference_accounts {
                if reference != &identities {
                    return Err(PaperError::InvalidState(
                        "shadow books have mismatched account identities/capital".to_string(),
                    ));
                }
            } else {
                reference_accounts = Some(identities);
            }

            for (account_id, account) in &book.accounts {
                validate_account_state(
                    mode,
                    account_id,
                    account,
                    &self.setup_registry,
                    &self.latest_ticks,
                    &self.config,
                )?;
            }
        }

        if self.end_of_day_timestamp_ms.is_some()
            && self.books.values().any(|book| {
                book.accounts
                    .values()
                    .any(|account| !account.pending_entries.is_empty())
            })
        {
            return Err(PaperError::InvalidState(
                "end-of-day state still contains a pending entry".to_string(),
            ));
        }
        Ok(())
    }

    pub fn place_setups(
        &mut self,
        mut setups: Vec<TradeSetup>,
        now_ms: TimestampMs,
    ) -> Vec<PlacementResult> {
        for setup in &mut setups {
            setup.ensure_stable_id();
        }
        setups.sort_by(|left, right| {
            left.evidence_timestamp_ms
                .cmp(&right.evidence_timestamp_ms)
                .then_with(|| left.contract.cmp(&right.contract))
                .then_with(|| left.setup_id.cmp(&right.setup_id))
        });
        setups
            .into_iter()
            .map(|setup| self.place_setup(setup, now_ms))
            .collect()
    }

    pub fn place_setup(&mut self, mut setup: TradeSetup, now_ms: TimestampMs) -> PlacementResult {
        setup.ensure_stable_id();
        let setup_id = setup.setup_id.clone();

        if self.setup_registry.contains_key(&setup_id) {
            let event = self.commit_event(EventDraft {
                setup_id: Some(setup_id.clone()),
                instrument_id: Some(setup.contract.instrument_id.clone()),
                ..EventDraft::simple(
                    now_ms,
                    EventType::DuplicateSetupIgnored,
                    "duplicate setup ignored idempotently",
                )
            });
            return PlacementResult {
                setup_id,
                status: PlacementStatus::Duplicate,
                orders_placed: 0,
                rejection_reason: None,
                events: vec![event],
            };
        }

        if let Some(reason) = validate_setup(&setup) {
            let event = self.commit_event(EventDraft {
                setup_id: Some(setup_id.clone()),
                instrument_id: Some(setup.contract.instrument_id.clone()),
                ..EventDraft::simple(now_ms, EventType::SetupRejected, reason.clone())
            });
            return PlacementResult {
                setup_id,
                status: PlacementStatus::Rejected,
                orders_placed: 0,
                rejection_reason: Some(reason),
                events: vec![event],
            };
        }

        if self.end_of_day_timestamp_ms.is_some() {
            let reason = "session is in end-of-day closeout".to_string();
            let event = self.commit_event(EventDraft {
                setup_id: Some(setup_id.clone()),
                instrument_id: Some(setup.contract.instrument_id.clone()),
                ..EventDraft::simple(now_ms, EventType::SetupRejected, reason.clone())
            });
            return PlacementResult {
                setup_id,
                status: PlacementStatus::Rejected,
                orders_placed: 0,
                rejection_reason: Some(reason),
                events: vec![event],
            };
        }

        if let Some((active_setup_id, lifecycle)) =
            self.active_setup_for_instrument(&setup.contract.instrument_id, &setup_id)
        {
            let reason = format!(
                "instrument {} already has an active {lifecycle} for setup {active_setup_id}",
                setup.contract.instrument_id
            );
            let event = self.commit_event(EventDraft {
                setup_id: Some(setup_id.clone()),
                instrument_id: Some(setup.contract.instrument_id.clone()),
                ..EventDraft::simple(now_ms, EventType::SetupRejected, reason.clone())
            });
            return PlacementResult {
                setup_id,
                status: PlacementStatus::Rejected,
                orders_placed: 0,
                rejection_reason: Some(reason),
                events: vec![event],
            };
        }

        let trigger_cap_paise = match setup
            .levels
            .entry_paise
            .checked_add(self.config.entry_buffer_paise)
        {
            Some(value) => value,
            None => {
                let reason = "entry plus buffer overflows".to_string();
                let event = self.commit_event(EventDraft {
                    setup_id: Some(setup_id.clone()),
                    instrument_id: Some(setup.contract.instrument_id.clone()),
                    ..EventDraft::simple(now_ms, EventType::SetupRejected, reason.clone())
                });
                return PlacementResult {
                    setup_id,
                    status: PlacementStatus::Rejected,
                    orders_placed: 0,
                    rejection_reason: Some(reason),
                    events: vec![event],
                };
            }
        };

        let mut drafts = Vec::new();
        let mut orders_placed = 0;
        for mode in ShadowMode::ALL {
            let book = self.books.get_mut(&mode).expect("books created together");
            for account in book.accounts.values_mut() {
                let free_cash = account.free_cash_paise(self.config.exit_charge_paise);
                let Ok(Some((lots, quantity, reservation))) = size_order(
                    free_cash,
                    trigger_cap_paise,
                    setup.contract.underlying.lot_size(),
                    self.config.entry_charge_paise,
                    self.config.exit_charge_paise,
                ) else {
                    continue;
                };

                let order_id = format!("{:?}:{}:{}:ENTRY", mode, account.account_id, setup_id);
                let order = PendingEntry {
                    order_id,
                    setup_id: setup_id.clone(),
                    contract: setup.contract.clone(),
                    levels: setup.levels.clone(),
                    lots,
                    quantity,
                    trigger_cap_paise,
                    reserved_paise: reservation,
                    created_timestamp_ms: now_ms,
                    evidence_timestamp_ms: setup.evidence_timestamp_ms,
                };
                account.pending_entries.insert(setup_id.clone(), order);
                orders_placed += 1;
                drafts.push(EventDraft {
                    timestamp_ms: now_ms,
                    event_type: EventType::EntryOrderPlaced,
                    mode: Some(mode),
                    account_id: Some(account.account_id.clone()),
                    setup_id: Some(setup_id.clone()),
                    instrument_id: Some(setup.contract.instrument_id.clone()),
                    quantity: Some(quantity),
                    price_paise: Some(trigger_cap_paise),
                    amount_paise: Some(reservation),
                    exit_reason: None,
                    message: format!(
                        "reserved {} paise for {lots} lot(s) at entry cap {trigger_cap_paise}",
                        reservation
                    ),
                });
            }
        }

        self.setup_registry.insert(setup_id.clone(), setup.clone());
        drafts.push(EventDraft {
            setup_id: Some(setup_id.clone()),
            instrument_id: Some(setup.contract.instrument_id.clone()),
            quantity: u32::try_from(orders_placed).ok(),
            ..EventDraft::simple(
                now_ms,
                EventType::SetupAccepted,
                if orders_placed == 0 {
                    "setup accepted but no shadow account could afford one lot".to_string()
                } else {
                    format!("setup accepted with {orders_placed} shadow order(s)")
                },
            )
        });
        let events = self.commit_events(drafts);

        PlacementResult {
            setup_id,
            status: PlacementStatus::Accepted,
            orders_placed,
            rejection_reason: None,
            events,
        }
    }

    /// Finds another setup that still owns this live contract anywhere in the
    /// mirrored paper books. Closed trades and cancelled entries do not count,
    /// so a later, genuinely new setup may re-enter after the prior lifecycle
    /// has finished.
    fn active_setup_for_instrument(
        &self,
        instrument_id: &str,
        incoming_setup_id: &str,
    ) -> Option<(String, &'static str)> {
        for book in self.books.values() {
            for account in book.accounts.values() {
                if let Some(order) = account.pending_entries.values().find(|order| {
                    order.setup_id != incoming_setup_id
                        && order.contract.instrument_id == instrument_id
                }) {
                    return Some((order.setup_id.clone(), "pending entry"));
                }
                if let Some(position) = account.open_positions.values().find(|position| {
                    position.setup_id != incoming_setup_id
                        && position.contract.instrument_id == instrument_id
                }) {
                    return Some((position.setup_id.clone(), "open position"));
                }
            }
        }
        None
    }

    pub fn cancel_pending_setup(
        &mut self,
        setup_id: &str,
        now_ms: TimestampMs,
    ) -> Vec<BrokerEvent> {
        let mut drafts = Vec::new();
        for mode in ShadowMode::ALL {
            let book = self.books.get_mut(&mode).expect("books created together");
            for account in book.accounts.values_mut() {
                if let Some(order) = account.pending_entries.remove(setup_id) {
                    drafts.push(EventDraft {
                        timestamp_ms: now_ms,
                        event_type: EventType::EntryOrderCancelled,
                        mode: Some(mode),
                        account_id: Some(account.account_id.clone()),
                        setup_id: Some(setup_id.to_string()),
                        instrument_id: Some(order.contract.instrument_id),
                        quantity: Some(order.quantity),
                        price_paise: Some(order.trigger_cap_paise),
                        amount_paise: Some(order.reserved_paise),
                        exit_reason: None,
                        message: "pending entry cancelled; reservation released".to_string(),
                    });
                }
            }
        }
        self.commit_events(drafts)
    }

    /// Apply a qualified streamer level revision to an already-filled setup.
    /// Pending entries are rejected because changing their entry cap would
    /// require a fresh affordability/reservation decision. Active stops are
    /// monotonic: a later update can tighten but never loosen protection.
    pub fn update_open_levels(
        &mut self,
        setup_id: &str,
        levels: TradeLevels,
        now_ms: TimestampMs,
    ) -> Vec<BrokerEvent> {
        let invalid = levels.entry_paise <= 0
            || levels.hard_sl_paise <= 0
            || levels.t1_paise <= 0
            || levels.hard_sl_paise >= levels.entry_paise
            || levels.t1_paise <= levels.entry_paise
            || levels.t2_paise.is_some_and(|t2| t2 <= levels.t1_paise);
        let has_pending = self.books.values().any(|book| {
            book.accounts
                .values()
                .any(|account| account.pending_entries.contains_key(setup_id))
        });
        let known = self.setup_registry.contains_key(setup_id);
        if invalid || has_pending || !known {
            let reason = if invalid {
                "level update rejected: invalid BUY level ordering"
            } else if has_pending {
                "level update rejected while entry remains pending"
            } else {
                "level update rejected: setup is unknown"
            };
            return vec![self.commit_event(EventDraft {
                setup_id: Some(setup_id.to_string()),
                ..EventDraft::simple(now_ms, EventType::LevelsUpdateRejected, reason)
            })];
        }

        let mut changed = 0usize;
        for book in self.books.values_mut() {
            for account in book.accounts.values_mut() {
                if let Some(position) = account.open_positions.get_mut(setup_id) {
                    position.levels = levels.clone();
                    position.effective_sl_paise =
                        position.effective_sl_paise.max(levels.hard_sl_paise);
                    changed += 1;
                }
            }
        }
        if changed == 0 {
            return vec![self.commit_event(EventDraft {
                setup_id: Some(setup_id.to_string()),
                ..EventDraft::simple(
                    now_ms,
                    EventType::LevelsUpdateRejected,
                    "level update rejected: no open position exists",
                )
            })];
        }
        if let Some(setup) = self.setup_registry.get_mut(setup_id) {
            setup.levels = levels;
        }
        vec![self.commit_event(EventDraft {
            setup_id: Some(setup_id.to_string()),
            quantity: u32::try_from(changed).ok(),
            ..EventDraft::simple(
                now_ms,
                EventType::LevelsUpdated,
                format!("streamer levels updated across {changed} open shadow position(s)"),
            )
        })]
    }

    fn commit_events(&mut self, drafts: Vec<EventDraft>) -> Vec<BrokerEvent> {
        drafts
            .into_iter()
            .map(|draft| self.commit_event(draft))
            .collect()
    }

    fn commit_event(&mut self, draft: EventDraft) -> BrokerEvent {
        let event = BrokerEvent {
            sequence: self.next_event_sequence,
            timestamp_ms: draft.timestamp_ms,
            event_type: draft.event_type,
            mode: draft.mode,
            account_id: draft.account_id,
            setup_id: draft.setup_id,
            instrument_id: draft.instrument_id,
            quantity: draft.quantity,
            price_paise: draft.price_paise,
            amount_paise: draft.amount_paise,
            exit_reason: draft.exit_reason,
            message: draft.message,
        };
        self.next_event_sequence = self.next_event_sequence.saturating_add(1);
        if self.config.event_capacity > 0 {
            while self.events.len() >= self.config.event_capacity {
                self.events.pop_front();
            }
            self.events.push_back(event.clone());
        }
        event
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SessionStatus {
    Open,
    Closing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub position: OpenPosition,
    pub entry_notional_paise: Paise,
    pub market_value_paise: Paise,
    pub gross_unrealized_pnl_paise: Paise,
    /// Includes the paid entry charge and reserved future exit charge.
    pub net_unrealized_pnl_paise: Paise,
    pub distance_to_stop_paise: Paise,
    pub last_tick_age_ms: TimestampMs,
    pub last_tick_is_fresh: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PortfolioTotals {
    pub starting_capital_paise: Paise,
    pub cash_balance_paise: Paise,
    pub pending_reservation_paise: Paise,
    pub exit_charge_reservation_paise: Paise,
    pub total_reserved_paise: Paise,
    pub free_cash_paise: Paise,
    pub gross_market_value_paise: Paise,
    pub liquidation_equity_paise: Paise,
    pub realized_pnl_paise: Paise,
    pub gross_unrealized_pnl_paise: Paise,
    pub net_unrealized_pnl_paise: Paise,
    pub total_pnl_paise: Paise,
    pub charges_paid_paise: Paise,
    pub pending_order_count: usize,
    pub open_position_count: usize,
    pub closed_trade_count: usize,
}

impl PortfolioTotals {
    fn add_assign(&mut self, other: &Self) {
        self.starting_capital_paise += other.starting_capital_paise;
        self.cash_balance_paise += other.cash_balance_paise;
        self.pending_reservation_paise += other.pending_reservation_paise;
        self.exit_charge_reservation_paise += other.exit_charge_reservation_paise;
        self.total_reserved_paise += other.total_reserved_paise;
        self.free_cash_paise += other.free_cash_paise;
        self.gross_market_value_paise += other.gross_market_value_paise;
        self.liquidation_equity_paise += other.liquidation_equity_paise;
        self.realized_pnl_paise += other.realized_pnl_paise;
        self.gross_unrealized_pnl_paise += other.gross_unrealized_pnl_paise;
        self.net_unrealized_pnl_paise += other.net_unrealized_pnl_paise;
        self.total_pnl_paise += other.total_pnl_paise;
        self.charges_paid_paise += other.charges_paid_paise;
        self.pending_order_count += other.pending_order_count;
        self.open_position_count += other.open_position_count;
        self.closed_trade_count += other.closed_trade_count;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub account_id: String,
    pub display_name: String,
    pub totals: PortfolioTotals,
    pub pending_entries: Vec<PendingEntry>,
    pub open_positions: Vec<PositionSnapshot>,
    pub closed_trades: Vec<ClosedTrade>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowSnapshot {
    pub mode: ShadowMode,
    pub totals: PortfolioTotals,
    pub accounts: Vec<AccountSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaperBrokerSnapshot {
    pub schema_version: u32,
    pub as_of_timestamp_ms: TimestampMs,
    pub session_status: SessionStatus,
    pub end_of_day_timestamp_ms: Option<TimestampMs>,
    #[serde(default)]
    pub trading_date_ist: Option<String>,
    pub latest_event_sequence: u64,
    pub accepted_setup_count: usize,
    pub accepted_setups: Vec<TradeSetup>,
    pub latest_ticks: Vec<MarketTick>,
    /// Sum of both independent shadow books; never interpret this as one
    /// deployable wallet.
    pub combined_shadow_totals: PortfolioTotals,
    pub shadows: Vec<ShadowSnapshot>,
    pub closed_trade_history: Vec<ClosedTrade>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventPage {
    pub requested_after_sequence: u64,
    pub oldest_available_sequence: Option<u64>,
    pub latest_available_sequence: Option<u64>,
    /// True when bounded retention removed events the caller has not seen.
    pub retention_gap: bool,
    pub events: Vec<BrokerEvent>,
}

impl PaperBroker {
    pub fn snapshot(&self, as_of_timestamp_ms: TimestampMs) -> PaperBrokerSnapshot {
        let shadows: Vec<ShadowSnapshot> = ShadowMode::ALL
            .into_iter()
            .map(|mode| self.shadow_snapshot(mode, as_of_timestamp_ms))
            .collect();
        let mut combined_shadow_totals = PortfolioTotals::default();
        for shadow in &shadows {
            combined_shadow_totals.add_assign(&shadow.totals);
        }

        PaperBrokerSnapshot {
            schema_version: self.schema_version,
            as_of_timestamp_ms,
            session_status: if self.end_of_day_timestamp_ms.is_some() {
                SessionStatus::Closing
            } else {
                SessionStatus::Open
            },
            end_of_day_timestamp_ms: self.end_of_day_timestamp_ms,
            trading_date_ist: self.trading_date_ist.clone(),
            latest_event_sequence: self.next_event_sequence.saturating_sub(1),
            accepted_setup_count: self.setup_registry.len(),
            accepted_setups: self.setup_registry.values().cloned().collect(),
            latest_ticks: self.latest_ticks.values().cloned().collect(),
            combined_shadow_totals,
            shadows,
            closed_trade_history: self.closed_trade_history(),
        }
    }

    pub fn event_page_after(&self, sequence: u64) -> EventPage {
        let oldest = self.events.front().map(|event| event.sequence);
        let latest = self.events.back().map(|event| event.sequence);
        let first_requested = sequence.saturating_add(1);
        EventPage {
            requested_after_sequence: sequence,
            oldest_available_sequence: oldest,
            latest_available_sequence: latest,
            retention_gap: oldest.is_some_and(|oldest_sequence| first_requested < oldest_sequence),
            events: self
                .events
                .iter()
                .filter(|event| event.sequence > sequence)
                .cloned()
                .collect(),
        }
    }

    pub fn closed_trade_history(&self) -> Vec<ClosedTrade> {
        let mut history: Vec<ClosedTrade> = self
            .books
            .values()
            .flat_map(|book| book.accounts.values())
            .flat_map(|account| account.closed_trades.iter().cloned())
            .collect();
        history.sort_by(|left, right| {
            left.closed_timestamp_ms
                .cmp(&right.closed_timestamp_ms)
                .then_with(|| left.mode.cmp(&right.mode))
                .then_with(|| left.account_id.cmp(&right.account_id))
                .then_with(|| left.trade_id.cmp(&right.trade_id))
        });
        history
    }

    pub fn latest_tick(&self, instrument_id: &str) -> Option<&MarketTick> {
        self.latest_ticks.get(instrument_id)
    }

    fn shadow_snapshot(&self, mode: ShadowMode, as_of_timestamp_ms: TimestampMs) -> ShadowSnapshot {
        let book = self.books.get(&mode).expect("books created together");
        let accounts: Vec<AccountSnapshot> = book
            .accounts
            .values()
            .map(|account| self.account_snapshot(account, as_of_timestamp_ms))
            .collect();
        let mut totals = PortfolioTotals::default();
        for account in &accounts {
            totals.add_assign(&account.totals);
        }
        ShadowSnapshot {
            mode,
            totals,
            accounts,
        }
    }

    fn account_snapshot(
        &self,
        account: &AccountState,
        as_of_timestamp_ms: TimestampMs,
    ) -> AccountSnapshot {
        let position_snapshots: Vec<PositionSnapshot> = account
            .open_positions
            .values()
            .map(|position| {
                let tick_age = as_of_timestamp_ms
                    .saturating_sub(position.last_tick_timestamp_ms)
                    .max(0);
                PositionSnapshot {
                    entry_notional_paise: position.entry_price_paise * i64::from(position.quantity),
                    market_value_paise: position.last_ltp_paise * i64::from(position.quantity),
                    gross_unrealized_pnl_paise: position.gross_unrealized_pnl_paise(),
                    net_unrealized_pnl_paise: position
                        .net_unrealized_pnl_paise(self.config.exit_charge_paise),
                    distance_to_stop_paise: position.last_ltp_paise - position.effective_sl_paise,
                    last_tick_age_ms: tick_age,
                    last_tick_is_fresh: tick_age <= self.config.maximum_tick_age_ms,
                    position: position.clone(),
                }
            })
            .collect();
        let gross_market_value: Paise = position_snapshots
            .iter()
            .map(|position| position.market_value_paise)
            .sum();
        let gross_unrealized: Paise = position_snapshots
            .iter()
            .map(|position| position.gross_unrealized_pnl_paise)
            .sum();
        let net_unrealized: Paise = position_snapshots
            .iter()
            .map(|position| position.net_unrealized_pnl_paise)
            .sum();
        let pending_reservation = account.reserved_pending_paise();
        let exit_reservation = account.reserved_exit_paise(self.config.exit_charge_paise);
        let charges_paid: Paise = account
            .closed_trades
            .iter()
            .map(|trade| trade.entry_charge_paise + trade.exit_charge_paise)
            .sum::<Paise>()
            + account
                .open_positions
                .values()
                .map(|position| position.entry_charge_paise)
                .sum::<Paise>();
        let liquidation_equity = account.cash_balance_paise + gross_market_value - exit_reservation;
        let total_pnl = liquidation_equity - account.starting_capital_paise;
        let totals = PortfolioTotals {
            starting_capital_paise: account.starting_capital_paise,
            cash_balance_paise: account.cash_balance_paise,
            pending_reservation_paise: pending_reservation,
            exit_charge_reservation_paise: exit_reservation,
            total_reserved_paise: pending_reservation + exit_reservation,
            free_cash_paise: account.free_cash_paise(self.config.exit_charge_paise),
            gross_market_value_paise: gross_market_value,
            liquidation_equity_paise: liquidation_equity,
            realized_pnl_paise: account.realized_pnl_paise,
            gross_unrealized_pnl_paise: gross_unrealized,
            net_unrealized_pnl_paise: net_unrealized,
            total_pnl_paise: total_pnl,
            charges_paid_paise: charges_paid,
            pending_order_count: account.pending_entries.len(),
            open_position_count: account.open_positions.len(),
            closed_trade_count: account.closed_trades.len(),
        };

        debug_assert_eq!(
            totals.total_pnl_paise,
            totals.realized_pnl_paise + totals.net_unrealized_pnl_paise
        );

        AccountSnapshot {
            account_id: account.account_id.clone(),
            display_name: account.display_name.clone(),
            totals,
            pending_entries: account.pending_entries.values().cloned().collect(),
            open_positions: position_snapshots,
            closed_trades: account.closed_trades.clone(),
        }
    }
}

fn validate_config(config: &PaperBrokerConfig) -> Result<(), PaperError> {
    if config.entry_buffer_paise < 0 {
        return Err(PaperError::InvalidConfig(
            "entry buffer cannot be negative".to_string(),
        ));
    }
    if config.entry_charge_paise < 0 || config.exit_charge_paise < 0 {
        return Err(PaperError::InvalidConfig(
            "charges cannot be negative".to_string(),
        ));
    }
    if config.pending_entry_ttl_ms <= 0 {
        return Err(PaperError::InvalidConfig(
            "pending entry TTL must be positive".to_string(),
        ));
    }
    if config.maximum_tick_age_ms < 0 || config.maximum_future_skew_ms < 0 {
        return Err(PaperError::InvalidConfig(
            "tick-age limits cannot be negative".to_string(),
        ));
    }
    Ok(())
}

fn invalid_state(message: impl Into<String>) -> PaperError {
    PaperError::InvalidState(message.into())
}

fn checked_state_add(left: Paise, right: Paise, context: &str) -> Result<Paise, PaperError> {
    left.checked_add(right)
        .ok_or_else(|| invalid_state(format!("arithmetic overflow while validating {context}")))
}

fn checked_state_sub(left: Paise, right: Paise, context: &str) -> Result<Paise, PaperError> {
    left.checked_sub(right)
        .ok_or_else(|| invalid_state(format!("arithmetic overflow while validating {context}")))
}

fn checked_state_mul(value: Paise, quantity: u32, context: &str) -> Result<Paise, PaperError> {
    value
        .checked_mul(i64::from(quantity))
        .ok_or_else(|| invalid_state(format!("arithmetic overflow while validating {context}")))
}

fn validate_trading_date_ist(value: &str) -> Result<(), PaperError> {
    if value.len() != 10
        || NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .ok()
            .is_none_or(|date| date.format("%Y-%m-%d").to_string() != value)
    {
        return Err(invalid_state(format!(
            "invalid IST trading date {value:?}; expected YYYY-MM-DD"
        )));
    }
    Ok(())
}

fn validate_setup_registry(registry: &BTreeMap<String, TradeSetup>) -> Result<(), PaperError> {
    for (setup_id, setup) in registry {
        if setup_id.trim().is_empty() || setup_id != &setup.setup_id || setup_id != setup_id.trim()
        {
            return Err(invalid_state(format!(
                "setup registry key/id mismatch for {setup_id:?}"
            )));
        }
        if let Some(reason) = validate_setup(setup) {
            return Err(invalid_state(format!(
                "invalid accepted setup {setup_id}: {reason}"
            )));
        }
    }
    Ok(())
}

fn validate_latest_ticks(ticks: &BTreeMap<String, MarketTick>) -> Result<(), PaperError> {
    for (instrument_id, tick) in ticks {
        if instrument_id.trim().is_empty()
            || instrument_id != &tick.instrument_id
            || instrument_id != instrument_id.trim()
            || tick.ltp_paise <= 0
            || tick.exchange_timestamp_ms <= 0
            || tick.received_timestamp_ms <= 0
        {
            return Err(invalid_state(format!(
                "invalid latest tick for instrument {instrument_id:?}"
            )));
        }
    }
    Ok(())
}

fn validate_event_buffer(
    events: &VecDeque<BrokerEvent>,
    next_sequence: u64,
    capacity: usize,
) -> Result<(), PaperError> {
    if events.len() > capacity || (capacity == 0 && !events.is_empty()) {
        return Err(invalid_state("event buffer exceeds configured capacity"));
    }
    let mut previous = None;
    for event in events {
        if event.sequence == 0
            || previous.is_some_and(|prior| event.sequence != prior + 1)
            || event.sequence >= next_sequence
        {
            return Err(invalid_state("event sequence buffer is not contiguous"));
        }
        previous = Some(event.sequence);
    }
    if previous.is_some_and(|latest| latest + 1 != next_sequence) {
        return Err(invalid_state(
            "next event sequence does not follow the retained event tail",
        ));
    }
    Ok(())
}

fn validate_buy_levels(levels: &TradeLevels) -> bool {
    levels.entry_paise > 0
        && levels.hard_sl_paise > 0
        && levels.t1_paise > levels.entry_paise
        && levels.hard_sl_paise < levels.entry_paise
        && levels.t2_paise.is_none_or(|t2| t2 > levels.t1_paise)
}

fn validate_quantity(
    lots: u32,
    quantity: u32,
    underlying: Underlying,
    context: &str,
) -> Result<(), PaperError> {
    let expected = lots
        .checked_mul(underlying.lot_size())
        .ok_or_else(|| invalid_state(format!("quantity overflow in {context}")))?;
    if lots == 0 || quantity == 0 || quantity != expected {
        return Err(invalid_state(format!("lot/quantity mismatch in {context}")));
    }
    Ok(())
}

fn validate_account_state(
    mode: ShadowMode,
    account_key: &str,
    account: &AccountState,
    setups: &BTreeMap<String, TradeSetup>,
    latest_ticks: &BTreeMap<String, MarketTick>,
    config: &PaperBrokerConfig,
) -> Result<(), PaperError> {
    let location = format!("{mode:?}/{account_key}");
    if account_key.trim().is_empty()
        || account_key != account.account_id
        || account.starting_capital_paise <= 0
        || account.cash_balance_paise < 0
    {
        return Err(invalid_state(format!(
            "invalid account identity/balance in {location}"
        )));
    }

    let charges = checked_state_add(
        config.entry_charge_paise,
        config.exit_charge_paise,
        "pending-entry charges",
    )?;
    let mut pending_reservation = 0;
    for (setup_id, order) in &account.pending_entries {
        let context = format!("pending entry {location}/{setup_id}");
        let setup = setups
            .get(setup_id)
            .ok_or_else(|| invalid_state(format!("{context} references an unknown setup")))?;
        if setup_id != &order.setup_id
            || order.contract != setup.contract
            || order.levels != setup.levels
            || order.evidence_timestamp_ms != setup.evidence_timestamp_ms
            || order.created_timestamp_ms < order.evidence_timestamp_ms
            || order.created_timestamp_ms <= 0
        {
            return Err(invalid_state(format!("inconsistent {context}")));
        }
        validate_quantity(
            order.lots,
            order.quantity,
            order.contract.underlying,
            &context,
        )?;
        let expected_cap = setup
            .levels
            .entry_paise
            .checked_add(config.entry_buffer_paise)
            .ok_or_else(|| invalid_state(format!("entry cap overflow in {context}")))?;
        let expected_reservation = checked_state_add(
            checked_state_mul(expected_cap, order.quantity, &context)?,
            charges,
            &context,
        )?;
        if order.trigger_cap_paise != expected_cap || order.reserved_paise != expected_reservation {
            return Err(invalid_state(format!("reservation mismatch in {context}")));
        }
        pending_reservation =
            checked_state_add(pending_reservation, order.reserved_paise, &location)?;
    }

    let mut open_cost = 0;
    for (setup_id, position) in &account.open_positions {
        let context = format!("open position {location}/{setup_id}");
        let setup = setups
            .get(setup_id)
            .ok_or_else(|| invalid_state(format!("{context} references an unknown setup")))?;
        if setup_id != &position.setup_id
            || position.contract != setup.contract
            || position.levels != setup.levels
            || !validate_buy_levels(&position.levels)
            || position.entry_charge_paise != config.entry_charge_paise
            || position.entry_price_paise <= 0
            || position.last_ltp_paise <= 0
            || position.effective_sl_paise < position.levels.hard_sl_paise
            || position.opened_timestamp_ms <= 0
            || position.last_tick_timestamp_ms < position.opened_timestamp_ms
            || position.minimum_ltp_paise <= 0
            || position.minimum_ltp_paise > position.entry_price_paise
            || position.minimum_ltp_paise > position.last_ltp_paise
            || position.maximum_ltp_paise < position.entry_price_paise
            || position.maximum_ltp_paise < position.last_ltp_paise
        {
            return Err(invalid_state(format!("inconsistent {context}")));
        }
        validate_quantity(
            position.lots,
            position.quantity,
            position.contract.underlying,
            &context,
        )?;
        let latest = latest_ticks
            .get(&position.contract.instrument_id)
            .ok_or_else(|| invalid_state(format!("{context} has no corresponding latest tick")))?;
        if latest.ltp_paise != position.last_ltp_paise
            || latest.exchange_timestamp_ms != position.last_tick_timestamp_ms
        {
            return Err(invalid_state(format!(
                "latest tick disagrees with {context}"
            )));
        }
        if let Some(request) = &position.llm_exit_request {
            if mode != ShadowMode::LlmExit
                || request.requested_timestamp_ms < position.opened_timestamp_ms
            {
                return Err(invalid_state(format!(
                    "invalid LLM exit request in {context}"
                )));
            }
        }
        let cost = checked_state_add(
            checked_state_mul(position.entry_price_paise, position.quantity, &context)?,
            position.entry_charge_paise,
            &context,
        )?;
        open_cost = checked_state_add(open_cost, cost, &location)?;
    }

    let mut realized = 0;
    let mut closed_setup_ids = BTreeMap::<String, String>::new();
    let mut trade_ids = BTreeMap::<String, ()>::new();
    for trade in &account.closed_trades {
        let context = format!("closed trade {location}/{}", trade.trade_id);
        let setup = setups
            .get(&trade.setup_id)
            .ok_or_else(|| invalid_state(format!("{context} references an unknown setup")))?;
        if trade.trade_id.trim().is_empty()
            || trade.mode != mode
            || trade.account_id != account.account_id
            || trade.contract != setup.contract
            || trade.entry_price_paise <= 0
            || trade.exit_price_paise <= 0
            || trade.entry_charge_paise != config.entry_charge_paise
            || trade.exit_charge_paise != config.exit_charge_paise
            || trade.opened_timestamp_ms <= 0
            || trade.closed_timestamp_ms < trade.opened_timestamp_ms
            || trade.minimum_ltp_paise <= 0
            || trade.minimum_ltp_paise > trade.entry_price_paise
            || trade.minimum_ltp_paise > trade.exit_price_paise
            || trade.maximum_ltp_paise < trade.entry_price_paise
            || trade.maximum_ltp_paise < trade.exit_price_paise
            || trade.final_sl_paise <= 0
        {
            return Err(invalid_state(format!("inconsistent {context}")));
        }
        validate_quantity(
            trade.lots,
            trade.quantity,
            trade.contract.underlying,
            &context,
        )?;
        let gross = checked_state_mul(
            checked_state_sub(trade.exit_price_paise, trade.entry_price_paise, &context)?,
            trade.quantity,
            &context,
        )?;
        let net = checked_state_sub(
            checked_state_sub(gross, trade.entry_charge_paise, &context)?,
            trade.exit_charge_paise,
            &context,
        )?;
        if trade.gross_pnl_paise != gross || trade.net_pnl_paise != net {
            return Err(invalid_state(format!("P/L mismatch in {context}")));
        }
        if trade_ids.insert(trade.trade_id.clone(), ()).is_some()
            || closed_setup_ids
                .insert(trade.setup_id.clone(), trade.trade_id.clone())
                .is_some()
        {
            return Err(invalid_state(format!(
                "duplicate trade/setup lifecycle in {context}"
            )));
        }
        realized = checked_state_add(realized, net, &location)?;
    }

    for setup_id in account.pending_entries.keys() {
        if account.open_positions.contains_key(setup_id) || closed_setup_ids.contains_key(setup_id)
        {
            return Err(invalid_state(format!(
                "setup {setup_id} occupies multiple lifecycle states in {location}"
            )));
        }
    }
    for setup_id in account.open_positions.keys() {
        if closed_setup_ids.contains_key(setup_id) {
            return Err(invalid_state(format!(
                "setup {setup_id} is both open and closed in {location}"
            )));
        }
    }
    if account.realized_pnl_paise != realized {
        return Err(invalid_state(format!(
            "realized P/L mismatch in {location}"
        )));
    }
    let expected_cash = checked_state_sub(
        checked_state_add(account.starting_capital_paise, realized, &location)?,
        open_cost,
        &location,
    )?;
    if account.cash_balance_paise != expected_cash {
        return Err(invalid_state(format!(
            "cash accounting mismatch in {location}"
        )));
    }
    let exit_reservation = checked_state_mul(
        config.exit_charge_paise,
        u32::try_from(account.open_positions.len())
            .map_err(|_| invalid_state(format!("too many open positions in {location}")))?,
        &location,
    )?;
    let total_reserved = checked_state_add(pending_reservation, exit_reservation, &location)?;
    if checked_state_sub(account.cash_balance_paise, total_reserved, &location)? < 0 {
        return Err(invalid_state(format!("negative free cash in {location}")));
    }
    Ok(())
}

fn validate_setup(setup: &TradeSetup) -> Option<String> {
    if setup.contract.instrument_id.trim().is_empty() {
        return Some("instrument_id cannot be empty".to_string());
    }
    if setup.contract.trading_symbol.trim().is_empty() {
        return Some("trading_symbol cannot be empty".to_string());
    }
    if setup.contract.expiry.trim().is_empty() {
        return Some("expiry cannot be empty".to_string());
    }
    if setup.contract.strike_paise <= 0 {
        return Some("strike must be positive".to_string());
    }
    if setup.side != TradeSide::Buy {
        return Some("v1 accepts BUY option trades only".to_string());
    }
    if setup.levels.entry_paise <= 0
        || setup.levels.hard_sl_paise <= 0
        || setup.levels.t1_paise <= 0
    {
        return Some("entry, hard SL, and T1 must be positive".to_string());
    }
    if setup.levels.hard_sl_paise >= setup.levels.entry_paise {
        return Some("BUY hard SL must be below entry".to_string());
    }
    if setup.levels.t1_paise <= setup.levels.entry_paise {
        return Some("BUY T1 must be above entry".to_string());
    }
    if let Some(t2) = setup.levels.t2_paise {
        if t2 <= setup.levels.t1_paise {
            return Some("BUY T2 must be above T1".to_string());
        }
    }
    None
}

fn size_order(
    free_cash_paise: Paise,
    trigger_cap_paise: Paise,
    lot_size: u32,
    entry_charge_paise: Paise,
    exit_charge_paise: Paise,
) -> Result<Option<(u32, u32, Paise)>, PaperError> {
    if free_cash_paise <= 0 || trigger_cap_paise <= 0 || lot_size == 0 {
        return Ok(None);
    }
    let charges = entry_charge_paise
        .checked_add(exit_charge_paise)
        .ok_or(PaperError::ArithmeticOverflow)?;
    let spendable = free_cash_paise - charges;
    if spendable <= 0 {
        return Ok(None);
    }
    let one_lot_premium = trigger_cap_paise
        .checked_mul(i64::from(lot_size))
        .ok_or(PaperError::ArithmeticOverflow)?;
    let lots_i64 = spendable / one_lot_premium;
    if lots_i64 <= 0 {
        return Ok(None);
    }
    let lots = u32::try_from(lots_i64).map_err(|_| PaperError::ArithmeticOverflow)?;
    let quantity = lots
        .checked_mul(lot_size)
        .ok_or(PaperError::ArithmeticOverflow)?;
    let reservation = trigger_cap_paise
        .checked_mul(i64::from(quantity))
        .and_then(|premium| premium.checked_add(charges))
        .ok_or(PaperError::ArithmeticOverflow)?;
    Ok(Some((lots, quantity, reservation)))
}

impl PaperBroker {
    /// Queues an explicit LLM exit for the LLM-managed shadow only.  The
    /// moving-SL shadow intentionally ignores this request.  Execution occurs
    /// on the next accepted fresh tick for the instrument.
    pub fn request_llm_exit(&mut self, setup_id: &str, now_ms: TimestampMs) -> Vec<BrokerEvent> {
        let mut drafts = Vec::new();
        let book = self
            .books
            .get_mut(&ShadowMode::LlmExit)
            .expect("books created together");
        for account in book.accounts.values_mut() {
            if let Some(position) = account.open_positions.get_mut(setup_id) {
                position.llm_exit_request = Some(ExitRequest {
                    requested_timestamp_ms: now_ms,
                });
                drafts.push(EventDraft {
                    timestamp_ms: now_ms,
                    event_type: EventType::LlmExitQueued,
                    mode: Some(ShadowMode::LlmExit),
                    account_id: Some(account.account_id.clone()),
                    setup_id: Some(setup_id.to_string()),
                    instrument_id: Some(position.contract.instrument_id.clone()),
                    quantity: Some(position.quantity),
                    price_paise: None,
                    amount_paise: None,
                    exit_reason: Some(ExitReason::Llm),
                    message: "explicit LLM exit queued from current evidence".to_owned(),
                });
            }
        }

        if drafts.is_empty() {
            drafts.push(EventDraft {
                setup_id: Some(setup_id.to_string()),
                ..EventDraft::simple(
                    now_ms,
                    EventType::LlmExitRejected,
                    "no open LLM-shadow position exists for setup",
                )
            });
        }
        self.commit_events(drafts)
    }

    /// Direct integration point for a trailing module that computes stops
    /// outside the tick callback.  Stops are monotonic and hard-SL bounded.
    pub fn update_moving_stop(
        &mut self,
        setup_id: &str,
        proposed_stop_paise: Paise,
        now_ms: TimestampMs,
    ) -> Vec<BrokerEvent> {
        let mut drafts = Vec::new();
        let book = self
            .books
            .get_mut(&ShadowMode::MovingSl)
            .expect("books created together");
        for account in book.accounts.values_mut() {
            if let Some(position) = account.open_positions.get_mut(setup_id) {
                let bounded = proposed_stop_paise.max(position.levels.hard_sl_paise);
                if bounded > position.effective_sl_paise {
                    let old_stop = position.effective_sl_paise;
                    position.effective_sl_paise = bounded;
                    drafts.push(EventDraft {
                        timestamp_ms: now_ms,
                        event_type: EventType::StopUpdated,
                        mode: Some(ShadowMode::MovingSl),
                        account_id: Some(account.account_id.clone()),
                        setup_id: Some(setup_id.to_string()),
                        instrument_id: Some(position.contract.instrument_id.clone()),
                        quantity: Some(position.quantity),
                        price_paise: Some(bounded),
                        amount_paise: Some(bounded - old_stop),
                        exit_reason: None,
                        message: format!("moving stop raised from {old_stop} to {bounded} paise"),
                    });
                }
            }
        }
        self.commit_events(drafts)
    }

    /// Returns the exchange trading date currently attached to this broker.
    pub fn trading_date_ist(&self) -> Option<&str> {
        self.trading_date_ist.as_deref()
    }

    /// Opens or annotates an Asia/Kolkata trading date.
    ///
    /// Calling this repeatedly for the same date is idempotent.  Advancing to
    /// a later date clears only the previous EOD latch and stale quote cache.
    /// Cash, realized P/L, closed history, accepted setup IDs, and event
    /// counters are preserved.  A rollover is refused while any pending entry
    /// or open position remains, so an overnight date change can never make an
    /// unfinished closeout tradable again or orphan its setup metadata.
    pub fn start_trading_day_ist(
        &mut self,
        trading_date_ist: &str,
        now_ms: TimestampMs,
    ) -> Result<Vec<BrokerEvent>, PaperError> {
        validate_trading_date_ist(trading_date_ist)?;
        if now_ms <= 0 {
            return Err(invalid_state(
                "trading-day start timestamp must be positive",
            ));
        }
        if self.trading_date_ist.as_deref() == Some(trading_date_ist) {
            return Ok(Vec::new());
        }

        let requested_date = NaiveDate::parse_from_str(trading_date_ist, "%Y-%m-%d")
            .expect("date was validated above");
        let is_initial_annotation =
            self.trading_date_ist.is_none() && self.end_of_day_timestamp_ms.is_none();
        if let Some(current) = &self.trading_date_ist {
            let current_date = NaiveDate::parse_from_str(current, "%Y-%m-%d")
                .map_err(|_| invalid_state("stored IST trading date is invalid"))?;
            if requested_date < current_date {
                return Err(invalid_state(format!(
                    "cannot roll trading date backward from {current} to {trading_date_ist}"
                )));
            }
        }

        let has_live_state = self.books.values().any(|book| {
            book.accounts.values().any(|account| {
                !account.pending_entries.is_empty() || !account.open_positions.is_empty()
            })
        });
        if !is_initial_annotation && has_live_state {
            return Err(invalid_state(format!(
                "cannot start {trading_date_ist} while pending/open positions remain"
            )));
        }

        if !is_initial_annotation {
            self.end_of_day_timestamp_ms = None;
            self.latest_ticks.clear();
        }
        self.trading_date_ist = Some(trading_date_ist.to_string());
        let event = self.commit_event(EventDraft::simple(
            now_ms,
            EventType::TradingDayStarted,
            if is_initial_annotation {
                format!("IST trading date initialized to {trading_date_ist}")
            } else {
                format!("IST trading date advanced to {trading_date_ist}; EOD latch reset")
            },
        ));
        Ok(vec![event])
    }

    /// Cancels every pending entry.  Open positions remain subscribed and are
    /// closed on their first fresh tick at or after `at_ms`.
    pub fn trigger_end_of_day(&mut self, at_ms: TimestampMs) -> Vec<BrokerEvent> {
        if self.end_of_day_timestamp_ms.is_some() {
            return Vec::new();
        }
        self.end_of_day_timestamp_ms = Some(at_ms);

        let mut drafts = vec![EventDraft::simple(
            at_ms,
            EventType::EndOfDayStarted,
            "end-of-day closeout started; new entries disabled",
        )];
        for mode in ShadowMode::ALL {
            let book = self.books.get_mut(&mode).expect("books created together");
            for account in book.accounts.values_mut() {
                let orders = std::mem::take(&mut account.pending_entries);
                for (_, order) in orders {
                    drafts.push(EventDraft {
                        timestamp_ms: at_ms,
                        event_type: EventType::EntryOrderCancelled,
                        mode: Some(mode),
                        account_id: Some(account.account_id.clone()),
                        setup_id: Some(order.setup_id),
                        instrument_id: Some(order.contract.instrument_id),
                        quantity: Some(order.quantity),
                        price_paise: Some(order.trigger_cap_paise),
                        amount_paise: Some(order.reserved_paise),
                        exit_reason: Some(ExitReason::EndOfDay),
                        message: "pending entry cancelled for end of day".to_string(),
                    });
                }
            }
        }
        self.commit_events(drafts)
    }

    pub fn on_tick(&mut self, tick: MarketTick, now_ms: TimestampMs) -> TickResult {
        let mut no_moving_stop = NoMovingStop;
        self.on_tick_with_policy(tick, now_ms, &mut no_moving_stop)
    }

    pub fn on_tick_with_policy<P: MovingStopPolicy + ?Sized>(
        &mut self,
        tick: MarketTick,
        now_ms: TimestampMs,
        moving_stop_policy: &mut P,
    ) -> TickResult {
        if let Some(rejection) = self.validate_tick(&tick, now_ms) {
            let event = self.commit_event(EventDraft {
                instrument_id: Some(tick.instrument_id),
                price_paise: Some(tick.ltp_paise),
                ..EventDraft::simple(
                    now_ms,
                    EventType::TickRejected,
                    format!("market tick rejected: {rejection:?}"),
                )
            });
            return TickResult {
                accepted: false,
                rejection: Some(rejection),
                entries_filled: 0,
                positions_closed: 0,
                events: vec![event],
            };
        }

        self.latest_ticks
            .insert(tick.instrument_id.clone(), tick.clone());

        let mut drafts = Vec::new();
        let mut entries_filled = 0;
        let mut positions_closed = 0;
        let eod_at = self.end_of_day_timestamp_ms;
        let pending_entry_ttl_ms = self.config.pending_entry_ttl_ms;

        for mode in ShadowMode::ALL {
            let book = self.books.get_mut(&mode).expect("books created together");
            for account in book.accounts.values_mut() {
                // Expire every stale pending order before this tick can fill
                // anything. Use local processing time (the same clock used at
                // order creation), so a delayed/replayed quote cannot revive a
                // signal after its lifetime has elapsed.
                let expired_ids: Vec<String> = account
                    .pending_entries
                    .iter()
                    .filter(|(_, order)| {
                        now_ms.saturating_sub(order.created_timestamp_ms) >= pending_entry_ttl_ms
                    })
                    .map(|(setup_id, _)| setup_id.clone())
                    .collect();

                for setup_id in expired_ids {
                    let order = account
                        .pending_entries
                        .remove(&setup_id)
                        .expect("expiry key originated from map");
                    drafts.push(EventDraft {
                        timestamp_ms: now_ms,
                        event_type: EventType::EntryOrderCancelled,
                        mode: Some(mode),
                        account_id: Some(account.account_id.clone()),
                        setup_id: Some(order.setup_id),
                        instrument_id: Some(order.contract.instrument_id),
                        quantity: Some(order.quantity),
                        price_paise: Some(order.trigger_cap_paise),
                        amount_paise: Some(order.reserved_paise),
                        exit_reason: None,
                        message: format!(
                            "pending entry expired after {} ms; reservation released",
                            pending_entry_ttl_ms
                        ),
                    });
                }

                let fill_ids: Vec<String> = account
                    .pending_entries
                    .iter()
                    .filter(|(_, order)| {
                        order.contract.instrument_id == tick.instrument_id
                            // A quote received before this order existed may be
                            // fresh enough for valuation, but it is not a
                            // post-order market event and must never fill it.
                            && tick.received_timestamp_ms >= order.created_timestamp_ms
                            && tick.ltp_paise <= order.trigger_cap_paise
                    })
                    .map(|(setup_id, _)| setup_id.clone())
                    .collect();

                for setup_id in fill_ids {
                    let order = account
                        .pending_entries
                        .remove(&setup_id)
                        .expect("fill key originated from map");
                    let fill_notional = tick.ltp_paise * i64::from(order.quantity);
                    account.cash_balance_paise -= fill_notional + self.config.entry_charge_paise;
                    let position_id = format!(
                        "{:?}:{}:{}:POSITION",
                        mode, account.account_id, order.setup_id
                    );
                    let position = OpenPosition {
                        position_id,
                        setup_id: order.setup_id.clone(),
                        contract: order.contract.clone(),
                        levels: order.levels.clone(),
                        lots: order.lots,
                        quantity: order.quantity,
                        entry_price_paise: tick.ltp_paise,
                        entry_charge_paise: self.config.entry_charge_paise,
                        effective_sl_paise: order.levels.hard_sl_paise,
                        last_ltp_paise: tick.ltp_paise,
                        maximum_ltp_paise: tick.ltp_paise,
                        minimum_ltp_paise: tick.ltp_paise,
                        opened_timestamp_ms: tick.exchange_timestamp_ms,
                        last_tick_timestamp_ms: tick.exchange_timestamp_ms,
                        llm_exit_request: None,
                    };
                    account
                        .open_positions
                        .insert(order.setup_id.clone(), position);
                    entries_filled += 1;
                    drafts.push(EventDraft {
                        timestamp_ms: tick.exchange_timestamp_ms,
                        event_type: EventType::EntryFilled,
                        mode: Some(mode),
                        account_id: Some(account.account_id.clone()),
                        setup_id: Some(order.setup_id),
                        instrument_id: Some(order.contract.instrument_id),
                        quantity: Some(order.quantity),
                        price_paise: Some(tick.ltp_paise),
                        amount_paise: Some(fill_notional + self.config.entry_charge_paise),
                        exit_reason: None,
                        message: format!(
                            "BUY filled fully at {} paise with {} paise entry charge",
                            tick.ltp_paise, self.config.entry_charge_paise
                        ),
                    });
                }

                let position_ids: Vec<String> = account
                    .open_positions
                    .iter()
                    .filter(|(_, position)| position.contract.instrument_id == tick.instrument_id)
                    .map(|(setup_id, _)| setup_id.clone())
                    .collect();

                for setup_id in position_ids {
                    let mut stop_event = None;
                    let exit_reason = {
                        let position = account
                            .open_positions
                            .get_mut(&setup_id)
                            .expect("position key originated from map");
                        position.last_ltp_paise = tick.ltp_paise;
                        position.last_tick_timestamp_ms = tick.exchange_timestamp_ms;
                        position.maximum_ltp_paise = position.maximum_ltp_paise.max(tick.ltp_paise);
                        position.minimum_ltp_paise = position.minimum_ltp_paise.min(tick.ltp_paise);

                        if mode == ShadowMode::MovingSl {
                            let context = MovingStopContext {
                                mode,
                                account_id: account.account_id.clone(),
                                setup_id: position.setup_id.clone(),
                                contract: position.contract.clone(),
                                levels: position.levels.clone(),
                                quantity: position.quantity,
                                entry_price_paise: position.entry_price_paise,
                                current_stop_paise: position.effective_sl_paise,
                                current_ltp_paise: position.last_ltp_paise,
                                maximum_ltp_paise: position.maximum_ltp_paise,
                                opened_timestamp_ms: position.opened_timestamp_ms,
                                tick_timestamp_ms: tick.exchange_timestamp_ms,
                            };
                            if let Some(proposed) = moving_stop_policy.next_stop_paise(&context) {
                                let bounded = proposed.max(position.levels.hard_sl_paise);
                                if bounded > position.effective_sl_paise {
                                    let old_stop = position.effective_sl_paise;
                                    position.effective_sl_paise = bounded;
                                    stop_event = Some(EventDraft {
                                        timestamp_ms: tick.exchange_timestamp_ms,
                                        event_type: EventType::StopUpdated,
                                        mode: Some(mode),
                                        account_id: Some(account.account_id.clone()),
                                        setup_id: Some(position.setup_id.clone()),
                                        instrument_id: Some(
                                            position.contract.instrument_id.clone(),
                                        ),
                                        quantity: Some(position.quantity),
                                        price_paise: Some(bounded),
                                        amount_paise: Some(bounded - old_stop),
                                        exit_reason: None,
                                        message: format!(
                                            "moving stop raised from {old_stop} to {bounded} paise"
                                        ),
                                    });
                                }
                            }
                        }

                        if eod_at.is_some_and(|at| tick.exchange_timestamp_ms >= at) {
                            Some(ExitReason::EndOfDay)
                        } else if tick.ltp_paise <= position.effective_sl_paise {
                            if mode == ShadowMode::MovingSl
                                && position.effective_sl_paise > position.levels.hard_sl_paise
                            {
                                Some(ExitReason::MovingStop)
                            } else {
                                Some(ExitReason::HardStop)
                            }
                        } else if mode == ShadowMode::LlmExit {
                            match &position.llm_exit_request {
                                Some(request)
                                    if tick.received_timestamp_ms
                                        >= request.requested_timestamp_ms =>
                                {
                                    Some(ExitReason::Llm)
                                }
                                Some(_) | None => None,
                            }
                        } else {
                            None
                        }
                    };

                    if let Some(event) = stop_event {
                        drafts.push(event);
                    }
                    if let Some(reason) = exit_reason {
                        let position = account
                            .open_positions
                            .remove(&setup_id)
                            .expect("position exists until close");
                        let exit_notional = tick.ltp_paise * i64::from(position.quantity);
                        account.cash_balance_paise += exit_notional - self.config.exit_charge_paise;
                        let gross_pnl = (tick.ltp_paise - position.entry_price_paise)
                            * i64::from(position.quantity);
                        let net_pnl =
                            gross_pnl - position.entry_charge_paise - self.config.exit_charge_paise;
                        account.realized_pnl_paise += net_pnl;
                        let closed = ClosedTrade {
                            trade_id: format!(
                                "{}:{}",
                                position.position_id, tick.exchange_timestamp_ms
                            ),
                            mode,
                            account_id: account.account_id.clone(),
                            setup_id: position.setup_id.clone(),
                            contract: position.contract.clone(),
                            lots: position.lots,
                            quantity: position.quantity,
                            entry_price_paise: position.entry_price_paise,
                            exit_price_paise: tick.ltp_paise,
                            entry_charge_paise: position.entry_charge_paise,
                            exit_charge_paise: self.config.exit_charge_paise,
                            gross_pnl_paise: gross_pnl,
                            net_pnl_paise: net_pnl,
                            opened_timestamp_ms: position.opened_timestamp_ms,
                            closed_timestamp_ms: tick.exchange_timestamp_ms,
                            exit_reason: reason,
                            maximum_ltp_paise: position.maximum_ltp_paise,
                            minimum_ltp_paise: position.minimum_ltp_paise,
                            final_sl_paise: position.effective_sl_paise,
                        };
                        account.closed_trades.push(closed);
                        positions_closed += 1;
                        drafts.push(EventDraft {
                            timestamp_ms: tick.exchange_timestamp_ms,
                            event_type: EventType::PositionClosed,
                            mode: Some(mode),
                            account_id: Some(account.account_id.clone()),
                            setup_id: Some(position.setup_id),
                            instrument_id: Some(position.contract.instrument_id),
                            quantity: Some(position.quantity),
                            price_paise: Some(tick.ltp_paise),
                            amount_paise: Some(net_pnl),
                            exit_reason: Some(reason),
                            message: format!(
                                "position closed at {} paise; net P/L {} paise",
                                tick.ltp_paise, net_pnl
                            ),
                        });
                    }
                }
            }
        }

        let events = self.commit_events(drafts);
        TickResult {
            accepted: true,
            rejection: None,
            entries_filled,
            positions_closed,
            events,
        }
    }

    fn validate_tick(&self, tick: &MarketTick, now_ms: TimestampMs) -> Option<TickRejection> {
        if tick.instrument_id.trim().is_empty() {
            return Some(TickRejection::EmptyInstrument);
        }
        if tick.ltp_paise <= 0 {
            return Some(TickRejection::InvalidPrice);
        }
        let future_limit = now_ms.saturating_add(self.config.maximum_future_skew_ms);
        if tick.exchange_timestamp_ms > future_limit || tick.received_timestamp_ms > future_limit {
            return Some(TickRejection::FromFuture);
        }
        if now_ms.saturating_sub(tick.exchange_timestamp_ms) > self.config.maximum_tick_age_ms
            || now_ms.saturating_sub(tick.received_timestamp_ms) > self.config.maximum_tick_age_ms
        {
            return Some(TickRejection::Stale);
        }
        if let Some(previous) = self.latest_ticks.get(&tick.instrument_id) {
            if tick.exchange_timestamp_ms < previous.exchange_timestamp_ms
                || (tick.exchange_timestamp_ms == previous.exchange_timestamp_ms
                    && tick.received_timestamp_ms <= previous.received_timestamp_ms)
            {
                return Some(TickRejection::OutOfOrder);
            }
        }

        let mut maximum_quantity = 0_u32;
        for book in self.books.values() {
            for account in book.accounts.values() {
                for order in account.pending_entries.values() {
                    if order.contract.instrument_id == tick.instrument_id {
                        maximum_quantity = maximum_quantity.max(order.quantity);
                    }
                }
                for position in account.open_positions.values() {
                    if position.contract.instrument_id == tick.instrument_id {
                        maximum_quantity = maximum_quantity.max(position.quantity);
                    }
                }
            }
        }
        if maximum_quantity > 0
            && tick
                .ltp_paise
                .checked_mul(i64::from(maximum_quantity))
                .is_none()
        {
            return Some(TickRejection::InvalidPrice);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE_TIME: TimestampMs = 1_000_000;

    fn contract(id: &str, underlying: Underlying) -> OptionContract {
        OptionContract {
            instrument_id: id.to_string(),
            trading_symbol: format!("{id}-SYMBOL"),
            underlying,
            expiry: "2026-08-13".to_string(),
            strike_paise: rupees(24_000),
            option_kind: OptionKind::Ce,
        }
    }

    fn setup(
        id: &str,
        instrument_id: &str,
        underlying: Underlying,
        evidence_timestamp_ms: TimestampMs,
    ) -> TradeSetup {
        TradeSetup {
            setup_id: id.to_string(),
            contract: contract(instrument_id, underlying),
            side: TradeSide::Buy,
            levels: TradeLevels {
                entry_paise: rupees(10),
                hard_sl_paise: rupees(8),
                t1_paise: rupees(12),
                t2_paise: Some(rupees(15)),
            },
            evidence_timestamp_ms,
            received_timestamp_ms: evidence_timestamp_ms + 20_000,
        }
    }

    fn tick(instrument_id: &str, price_paise: Paise, timestamp_ms: TimestampMs) -> MarketTick {
        MarketTick {
            instrument_id: instrument_id.to_string(),
            ltp_paise: price_paise,
            exchange_timestamp_ms: timestamp_ms,
            received_timestamp_ms: timestamp_ms,
        }
    }

    #[test]
    fn new_paper_payloads_are_confidence_free() {
        let mut setup_json = serde_json::to_value(setup(
            "confidence-free",
            "NIFTY-A",
            Underlying::Nifty,
            BASE_TIME,
        ))
        .unwrap();
        let mut config_json = serde_json::to_value(PaperBrokerConfig::default()).unwrap();

        assert!(setup_json.get("confidence_pct").is_none());
        assert!(config_json.get("minimum_confidence_pct").is_none());

        setup_json["confidence_pct"] = serde_json::json!(65);
        config_json["minimum_confidence_pct"] = serde_json::json!(65);
        let restored_setup: TradeSetup = serde_json::from_value(setup_json).unwrap();
        let restored_config: PaperBrokerConfig = serde_json::from_value(config_json).unwrap();
        assert!(
            serde_json::to_value(restored_setup)
                .unwrap()
                .get("confidence_pct")
                .is_none()
        );
        assert!(
            serde_json::to_value(restored_config)
                .unwrap()
                .get("minimum_confidence_pct")
                .is_none()
        );
    }

    fn one_account_broker(capital_rupees: i64) -> PaperBroker {
        PaperBroker::with_accounts(
            PaperBrokerConfig::default(),
            vec![AccountSpec {
                account_id: "only".to_string(),
                display_name: "Only Account".to_string(),
                starting_capital_paise: rupees(capital_rupees),
            }],
        )
        .unwrap()
    }

    fn account_snapshot(
        broker: &PaperBroker,
        mode: ShadowMode,
        now_ms: TimestampMs,
    ) -> AccountSnapshot {
        broker
            .snapshot(now_ms)
            .shadows
            .into_iter()
            .find(|shadow| shadow.mode == mode)
            .unwrap()
            .accounts
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    fn creates_two_independent_default_shadow_wallet_sets() {
        let broker = PaperBroker::new(PaperBrokerConfig::default()).unwrap();
        let snapshot = broker.snapshot(BASE_TIME);
        assert_eq!(snapshot.shadows.len(), 2);
        for shadow in &snapshot.shadows {
            assert_eq!(shadow.accounts.len(), 5);
            let capitals: Vec<Paise> = shadow
                .accounts
                .iter()
                .map(|account| account.totals.starting_capital_paise)
                .collect();
            assert_eq!(
                capitals,
                vec![
                    rupees(5_000),
                    rupees(10_000),
                    rupees(2_000),
                    rupees(15_000),
                    rupees(20_000)
                ]
            );
            assert_eq!(shadow.totals.starting_capital_paise, rupees(52_000));
        }
        assert_eq!(
            snapshot.combined_shadow_totals.starting_capital_paise,
            rupees(104_000)
        );
    }

    #[test]
    fn stable_setup_id_is_repeatable_and_sensitive_to_trade_identity() {
        let mut first = setup("", "NIFTY-A", Underlying::Nifty, BASE_TIME);
        let mut same = first.clone();
        first.ensure_stable_id();
        same.ensure_stable_id();
        assert_eq!(first.setup_id, same.setup_id);
        assert!(first.setup_id.starts_with("setup-"));

        let mut changed = setup("", "NIFTY-A", Underlying::Nifty, BASE_TIME);
        changed.levels.t1_paise += 5;
        changed.ensure_stable_id();
        assert_ne!(first.setup_id, changed.setup_id);

        let mut supplied = setup("  upstream-id  ", "NIFTY-A", Underlying::Nifty, BASE_TIME);
        supplied.ensure_stable_id();
        assert_eq!(supplied.setup_id, "upstream-id");
    }

    #[test]
    fn sizes_maximum_whole_lots_and_reserves_buffer_and_both_charges() {
        let mut broker = one_account_broker(5_000);
        let result = broker.place_setup(
            setup("sizing", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        assert_eq!(result.status, PlacementStatus::Accepted);
        assert_eq!(result.orders_placed, 2);

        for mode in ShadowMode::ALL {
            let account = account_snapshot(&broker, mode, BASE_TIME);
            assert_eq!(account.pending_entries.len(), 1);
            let order = &account.pending_entries[0];
            assert_eq!(order.lots, 6);
            assert_eq!(order.quantity, 390);
            assert_eq!(order.trigger_cap_paise, rupees(12));
            assert_eq!(order.reserved_paise, rupees(4_720));
            assert_eq!(account.totals.free_cash_paise, rupees(280));
        }
    }

    #[test]
    fn sensex_uses_twenty_quantity_per_lot() {
        let mut broker = one_account_broker(2_000);
        let result = broker.place_setup(
            setup("sensex", "SENSEX-A", Underlying::Sensex, BASE_TIME),
            BASE_TIME,
        );
        assert_eq!(result.orders_placed, 2);
        let account = account_snapshot(&broker, ShadowMode::LlmExit, BASE_TIME);
        let order = &account.pending_entries[0];
        assert_eq!(order.lots, 8);
        assert_eq!(order.quantity, 160);
        assert_eq!(order.reserved_paise, rupees(1_960));
    }

    #[test]
    fn rejects_sell_and_invalid_buy_levels() {
        let mut broker = one_account_broker(5_000);

        let mut sell = setup("sell", "NIFTY-A", Underlying::Nifty, BASE_TIME);
        sell.side = TradeSide::Sell;
        assert_eq!(
            broker.place_setup(sell, BASE_TIME).status,
            PlacementStatus::Rejected
        );

        let mut invalid_levels = setup("levels", "NIFTY-A", Underlying::Nifty, BASE_TIME);
        invalid_levels.levels.hard_sl_paise = invalid_levels.levels.entry_paise;
        assert_eq!(
            broker.place_setup(invalid_levels, BASE_TIME).status,
            PlacementStatus::Rejected
        );
    }

    #[test]
    fn accepted_setup_is_idempotent_even_if_no_account_can_afford_it() {
        let mut broker = one_account_broker(100);
        let trade = setup("stable", "NIFTY-A", Underlying::Nifty, BASE_TIME);
        let first = broker.place_setup(trade.clone(), BASE_TIME);
        let second = broker.place_setup(trade, BASE_TIME + 1);
        assert_eq!(first.status, PlacementStatus::Accepted);
        assert_eq!(first.orders_placed, 0);
        assert_eq!(second.status, PlacementStatus::Duplicate);
        assert_eq!(broker.snapshot(BASE_TIME + 1).accepted_setup_count, 1);
    }

    #[test]
    fn different_setup_for_same_instrument_is_blocked_while_pending_after_restore() {
        let mut broker = one_account_broker(5_000);
        let first = broker.place_setup(
            setup("first-evidence", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        assert_eq!(first.status, PlacementStatus::Accepted);

        // Exercise the persisted lifecycle, not an in-memory-only cache: after
        // a crash/restart the active order must still suppress replayed trade
        // evidence that arrives under a new setup ID.
        let serialized = serde_json::to_string(&broker).unwrap();
        let mut restored: PaperBroker = serde_json::from_str(&serialized).unwrap();
        restored.validate_restored_state().unwrap();

        let exact_replay = restored.place_setup(
            setup("first-evidence", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME + 1,
        );
        assert_eq!(exact_replay.status, PlacementStatus::Duplicate);

        let replay = restored.place_setup(
            setup(
                "later-evidence",
                "NIFTY-A",
                Underlying::Nifty,
                BASE_TIME + 1,
            ),
            BASE_TIME + 2,
        );
        assert_eq!(replay.status, PlacementStatus::Rejected);
        assert_eq!(replay.orders_placed, 0);
        assert!(
            replay
                .rejection_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("pending entry for setup first-evidence"))
        );
        assert_eq!(replay.events[0].event_type, EventType::SetupRejected);
        assert!(!restored.setup_registry.contains_key("later-evidence"));
    }

    #[test]
    fn different_setup_for_same_instrument_is_blocked_while_position_is_open() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("open-first", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        let fill = broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );
        assert_eq!(fill.entries_filled, 2);

        let second = broker.place_setup(
            setup("open-second", "NIFTY-A", Underlying::Nifty, BASE_TIME + 101),
            BASE_TIME + 101,
        );
        assert_eq!(second.status, PlacementStatus::Rejected);
        assert!(
            second
                .rejection_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("open position for setup open-first"))
        );
    }

    #[test]
    fn same_instrument_reentry_is_allowed_after_pending_entry_is_cancelled() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("cancel-first", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        assert_eq!(
            broker
                .cancel_pending_setup("cancel-first", BASE_TIME + 1)
                .len(),
            2
        );

        let reentry = broker.place_setup(
            setup("after-cancel", "NIFTY-A", Underlying::Nifty, BASE_TIME + 2),
            BASE_TIME + 2,
        );
        assert_eq!(reentry.status, PlacementStatus::Accepted);
        assert_eq!(reentry.orders_placed, 2);
    }

    #[test]
    fn same_instrument_reentry_is_allowed_after_all_positions_are_closed() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("closed-first", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );
        let stopped = broker.on_tick(tick("NIFTY-A", rupees(7), BASE_TIME + 200), BASE_TIME + 200);
        assert_eq!(stopped.positions_closed, 2);

        let reentry = broker.place_setup(
            setup("after-close", "NIFTY-A", Underlying::Nifty, BASE_TIME + 201),
            BASE_TIME + 201,
        );
        assert_eq!(reentry.status, PlacementStatus::Accepted);
        assert_eq!(reentry.orders_placed, 2);
    }

    #[test]
    fn active_setup_does_not_block_a_different_instrument() {
        let mut broker = one_account_broker(5_000);
        let first = broker.place_setup(
            setup("nifty", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        assert_eq!(first.orders_placed, 2);

        // The first Nifty reservation leaves exactly enough for one default
        // Sensex lot in each shadow account. This asserts actual placement,
        // not merely acceptance followed by an affordability skip.
        let second = broker.place_setup(
            setup("sensex", "SENSEX-A", Underlying::Sensex, BASE_TIME + 1),
            BASE_TIME + 1,
        );
        assert_eq!(second.status, PlacementStatus::Accepted);
        assert_eq!(second.orders_placed, 2);
    }

    #[test]
    fn batch_processing_uses_evidence_time_before_input_order() {
        let mut broker = one_account_broker(1_000);
        let later = setup("later", "NIFTY-B", Underlying::Nifty, BASE_TIME + 1);
        let earlier = setup("earlier", "NIFTY-A", Underlying::Nifty, BASE_TIME);
        let results = broker.place_setups(vec![later, earlier], BASE_TIME + 10);
        assert_eq!(results[0].setup_id, "earlier");
        assert_eq!(results[0].orders_placed, 2);
        assert_eq!(results[1].setup_id, "later");
        assert_eq!(results[1].orders_placed, 0);
    }

    #[test]
    fn pending_entry_persists_until_first_fresh_tick_at_or_below_buffered_cap() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("entry", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );

        let above = broker.on_tick(
            tick("NIFTY-A", rupees(12) + 1, BASE_TIME + 100),
            BASE_TIME + 100,
        );
        assert!(above.accepted);
        assert_eq!(above.entries_filled, 0);
        assert_eq!(
            account_snapshot(&broker, ShadowMode::LlmExit, BASE_TIME + 100)
                .pending_entries
                .len(),
            1
        );

        let stale = broker.on_tick(
            tick("NIFTY-A", rupees(11), BASE_TIME - 10_000),
            BASE_TIME + 101,
        );
        assert_eq!(stale.rejection, Some(TickRejection::Stale));

        let fill = broker.on_tick(
            tick("NIFTY-A", rupees(11), BASE_TIME + 200),
            BASE_TIME + 200,
        );
        assert_eq!(fill.entries_filled, 2);
        for mode in ShadowMode::ALL {
            let account = account_snapshot(&broker, mode, BASE_TIME + 200);
            assert!(account.pending_entries.is_empty());
            assert_eq!(account.open_positions.len(), 1);
            assert_eq!(
                account.open_positions[0].position.entry_price_paise,
                rupees(11)
            );
        }
    }

    #[test]
    fn pending_entry_cannot_fill_after_its_ttl() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("expired-entry", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );

        let after_ttl = BASE_TIME + DEFAULT_PENDING_ENTRY_TTL_MS + 1;
        let result = broker.on_tick(tick("NIFTY-A", rupees(10), after_ttl), after_ttl);

        assert!(result.accepted);
        assert_eq!(result.entries_filled, 0);
        assert_eq!(
            result
                .events
                .iter()
                .filter(|event| event.event_type == EventType::EntryOrderCancelled)
                .count(),
            2
        );
        assert!(result.events.iter().all(|event| {
            event.event_type != EventType::EntryOrderCancelled
                || event.message.contains("expired after 60000 ms")
        }));
        for mode in ShadowMode::ALL {
            let account = account_snapshot(&broker, mode, after_ttl);
            assert!(account.pending_entries.is_empty());
            assert!(account.open_positions.is_empty());
            assert_eq!(account.totals.cash_balance_paise, rupees(5_000));
            assert_eq!(account.totals.free_cash_paise, rupees(5_000));
        }
    }

    #[test]
    fn pending_entry_still_fills_inside_its_ttl() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("live-entry", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );

        let inside_ttl = BASE_TIME + DEFAULT_PENDING_ENTRY_TTL_MS - 1;
        let result = broker.on_tick(tick("NIFTY-A", rupees(10), inside_ttl), inside_ttl);

        assert!(result.accepted);
        assert_eq!(result.entries_filled, 2);
        assert!(
            result
                .events
                .iter()
                .all(|event| event.event_type != EventType::EntryOrderCancelled)
        );
        for mode in ShadowMode::ALL {
            let account = account_snapshot(&broker, mode, inside_ttl);
            assert!(account.pending_entries.is_empty());
            assert_eq!(account.open_positions.len(), 1);
        }
    }

    #[test]
    fn pre_ttl_config_snapshot_restores_pending_order_with_safe_default_expiry() {
        let config = PaperBrokerConfig::default();
        let accounts = vec![AccountSpec {
            account_id: "only".to_string(),
            display_name: "Only Account".to_string(),
            starting_capital_paise: rupees(5_000),
        }];
        let mut broker = PaperBroker::with_accounts(config.clone(), accounts.clone()).unwrap();
        broker.place_setup(
            setup("restored-expiry", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );

        // Simulate a state file written before pending_entry_ttl_ms was added.
        let mut legacy_value = serde_json::to_value(&broker).unwrap();
        legacy_value
            .get_mut("config")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .remove("pending_entry_ttl_ms");
        let persisted: PaperBroker = serde_json::from_value(legacy_value).unwrap();
        assert_eq!(
            persisted.config.pending_entry_ttl_ms,
            DEFAULT_PENDING_ENTRY_TTL_MS
        );

        let mut restored =
            PaperBroker::restore_from_persisted(persisted, config, accounts).unwrap();
        let after_ttl = BASE_TIME + DEFAULT_PENDING_ENTRY_TTL_MS + 1;
        let result = restored.on_tick(tick("NIFTY-A", rupees(10), after_ttl), after_ttl);
        assert_eq!(result.entries_filled, 0);
        for mode in ShadowMode::ALL {
            let account = account_snapshot(&restored, mode, after_ttl);
            assert!(account.pending_entries.is_empty());
            assert!(account.open_positions.is_empty());
        }
        restored.validate_restored_state().unwrap();
    }

    #[test]
    fn fresh_replayed_quote_from_before_order_creation_cannot_fill_entry() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("entry-time-gate", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );

        let replayed = MarketTick {
            instrument_id: "NIFTY-A".to_string(),
            ltp_paise: rupees(10),
            exchange_timestamp_ms: BASE_TIME - 2,
            received_timestamp_ms: BASE_TIME - 1,
        };
        let replay_result = broker.on_tick(replayed, BASE_TIME);
        assert!(replay_result.accepted);
        assert_eq!(replay_result.entries_filled, 0);
        for mode in ShadowMode::ALL {
            assert_eq!(
                account_snapshot(&broker, mode, BASE_TIME)
                    .pending_entries
                    .len(),
                1
            );
        }

        let live_result = broker.on_tick(tick("NIFTY-A", rupees(10), BASE_TIME + 1), BASE_TIME + 1);
        assert_eq!(live_result.entries_filled, 2);
    }

    #[test]
    fn fill_updates_cash_free_cash_equity_and_live_pnl_exactly() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("accounting", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(11), BASE_TIME + 100),
            BASE_TIME + 100,
        );

        let account = account_snapshot(&broker, ShadowMode::LlmExit, BASE_TIME + 100);
        assert_eq!(account.totals.cash_balance_paise, rupees(690));
        assert_eq!(account.totals.exit_charge_reservation_paise, rupees(20));
        assert_eq!(account.totals.free_cash_paise, rupees(670));
        assert_eq!(account.totals.gross_market_value_paise, rupees(4_290));
        assert_eq!(account.totals.liquidation_equity_paise, rupees(4_960));
        assert_eq!(account.totals.gross_unrealized_pnl_paise, 0);
        assert_eq!(account.totals.net_unrealized_pnl_paise, -rupees(40));
        assert_eq!(account.totals.total_pnl_paise, -rupees(40));
        assert_eq!(account.totals.charges_paid_paise, rupees(20));
    }

    #[test]
    fn hard_stop_closes_both_shadows_tick_by_tick_with_both_charges() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("hard-stop", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );
        let close = broker.on_tick(tick("NIFTY-A", rupees(8), BASE_TIME + 200), BASE_TIME + 200);
        assert_eq!(close.positions_closed, 2);

        for mode in ShadowMode::ALL {
            let account = account_snapshot(&broker, mode, BASE_TIME + 200);
            assert!(account.open_positions.is_empty());
            assert_eq!(account.closed_trades.len(), 1);
            assert_eq!(account.closed_trades[0].exit_reason, ExitReason::HardStop);
            assert_eq!(account.closed_trades[0].gross_pnl_paise, -rupees(780));
            assert_eq!(account.closed_trades[0].net_pnl_paise, -rupees(820));
            assert_eq!(account.totals.realized_pnl_paise, -rupees(820));
            assert_eq!(account.totals.cash_balance_paise, rupees(4_180));
        }
    }

    #[test]
    fn qualified_llm_exit_closes_only_llm_shadow() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("llm-exit", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );

        broker.request_llm_exit("llm-exit", BASE_TIME + 130);
        let exit_tick = broker.on_tick(
            tick("NIFTY-A", rupees(11) + 50, BASE_TIME + 140),
            BASE_TIME + 140,
        );
        assert_eq!(exit_tick.positions_closed, 1);

        let llm = account_snapshot(&broker, ShadowMode::LlmExit, BASE_TIME + 140);
        let moving = account_snapshot(&broker, ShadowMode::MovingSl, BASE_TIME + 140);
        assert!(llm.open_positions.is_empty());
        assert_eq!(llm.closed_trades[0].exit_reason, ExitReason::Llm);
        assert_eq!(moving.open_positions.len(), 1);
        assert!(moving.closed_trades.is_empty());
    }

    #[test]
    fn quote_received_before_llm_exit_request_cannot_execute_that_request() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup(
                "llm-exit-time-gate",
                "NIFTY-A",
                Underlying::Nifty,
                BASE_TIME,
            ),
            BASE_TIME,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );
        broker.request_llm_exit("llm-exit-time-gate", BASE_TIME + 200);

        let replayed = MarketTick {
            instrument_id: "NIFTY-A".to_string(),
            ltp_paise: rupees(11),
            exchange_timestamp_ms: BASE_TIME + 150,
            received_timestamp_ms: BASE_TIME + 199,
        };
        let replay_result = broker.on_tick(replayed, BASE_TIME + 201);
        assert!(replay_result.accepted);
        assert_eq!(replay_result.positions_closed, 0);
        assert_eq!(
            account_snapshot(&broker, ShadowMode::LlmExit, BASE_TIME + 201)
                .open_positions
                .len(),
            1
        );

        let live_result = broker.on_tick(
            tick("NIFTY-A", rupees(11), BASE_TIME + 210),
            BASE_TIME + 210,
        );
        assert_eq!(live_result.positions_closed, 1);
        assert_eq!(
            account_snapshot(&broker, ShadowMode::LlmExit, BASE_TIME + 210).closed_trades[0]
                .exit_reason,
            ExitReason::Llm
        );
    }

    #[test]
    fn moving_policy_is_called_only_for_moving_shadow_and_never_lowers_stop() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("trail", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );

        let mut calls = 0;
        let mut policy = |context: &MovingStopContext| {
            calls += 1;
            assert_eq!(context.mode, ShadowMode::MovingSl);
            Some(rupees(11))
        };
        broker.on_tick_with_policy(
            tick("NIFTY-A", rupees(12), BASE_TIME + 200),
            BASE_TIME + 200,
            &mut policy,
        );
        assert_eq!(calls, 1);

        let raised = broker.update_moving_stop("trail", rupees(9), BASE_TIME + 210);
        assert!(raised.is_empty());
        let exit = broker.on_tick(
            tick("NIFTY-A", rupees(10) + 50, BASE_TIME + 220),
            BASE_TIME + 220,
        );
        assert_eq!(exit.positions_closed, 1);

        let moving = account_snapshot(&broker, ShadowMode::MovingSl, BASE_TIME + 220);
        let llm = account_snapshot(&broker, ShadowMode::LlmExit, BASE_TIME + 220);
        assert_eq!(moving.closed_trades[0].exit_reason, ExitReason::MovingStop);
        assert_eq!(llm.open_positions.len(), 1);
    }

    #[test]
    fn gap_below_original_hard_stop_is_still_a_moving_stop_after_trail_raised() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("moving-gap", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );

        broker.update_moving_stop("moving-gap", rupees(11), BASE_TIME + 150);
        let close = broker.on_tick(tick("NIFTY-A", rupees(7), BASE_TIME + 200), BASE_TIME + 200);
        assert_eq!(close.positions_closed, 2);

        let moving = account_snapshot(&broker, ShadowMode::MovingSl, BASE_TIME + 200);
        let llm = account_snapshot(&broker, ShadowMode::LlmExit, BASE_TIME + 200);
        assert_eq!(moving.closed_trades[0].exit_reason, ExitReason::MovingStop);
        assert_eq!(llm.closed_trades[0].exit_reason, ExitReason::HardStop);
    }

    #[test]
    fn cancel_releases_pending_reservation_without_changing_cash() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("cancel", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        let events = broker.cancel_pending_setup("cancel", BASE_TIME + 1);
        assert_eq!(events.len(), 2);
        for mode in ShadowMode::ALL {
            let account = account_snapshot(&broker, mode, BASE_TIME + 1);
            assert_eq!(account.totals.cash_balance_paise, rupees(5_000));
            assert_eq!(account.totals.free_cash_paise, rupees(5_000));
            assert!(account.pending_entries.is_empty());
        }
    }

    #[test]
    fn lower_fill_releases_capital_for_an_additional_simultaneous_trade() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("first", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );

        let second = broker.place_setup(
            setup("second", "NIFTY-B", Underlying::Nifty, BASE_TIME + 1),
            BASE_TIME + 110,
        );
        assert_eq!(second.orders_placed, 2);
        broker.on_tick(
            tick("NIFTY-B", rupees(10), BASE_TIME + 120),
            BASE_TIME + 120,
        );
        for mode in ShadowMode::ALL {
            let account = account_snapshot(&broker, mode, BASE_TIME + 120);
            assert_eq!(account.open_positions.len(), 2);
        }
    }

    #[test]
    fn end_of_day_cancels_pending_and_closes_open_positions_on_first_eligible_tick() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("open", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );
        broker.place_setup(
            setup("pending", "NIFTY-B", Underlying::Nifty, BASE_TIME + 1),
            BASE_TIME + 110,
        );

        let eod_at = BASE_TIME + 200;
        let eod_events = broker.trigger_end_of_day(eod_at);
        assert!(
            eod_events
                .iter()
                .any(|event| event.event_type == EventType::EntryOrderCancelled)
        );
        assert_eq!(
            broker.snapshot(eod_at).session_status,
            SessionStatus::Closing
        );

        let before = broker.on_tick(tick("NIFTY-A", rupees(11), eod_at - 1), eod_at - 1);
        assert_eq!(before.positions_closed, 0);
        let at_or_after = broker.on_tick(tick("NIFTY-A", rupees(11), eod_at), eod_at);
        assert_eq!(at_or_after.positions_closed, 2);
        for mode in ShadowMode::ALL {
            let account = account_snapshot(&broker, mode, eod_at);
            assert!(account.pending_entries.is_empty());
            assert!(account.open_positions.is_empty());
            assert_eq!(account.closed_trades[0].exit_reason, ExitReason::EndOfDay);
        }

        let after_eod = broker.place_setup(
            setup("too-late", "NIFTY-C", Underlying::Nifty, eod_at),
            eod_at,
        );
        assert_eq!(after_eod.status, PlacementStatus::Rejected);
    }

    #[test]
    fn out_of_order_tick_cannot_mutate_positions() {
        let mut broker = one_account_broker(5_000);
        broker.place_setup(
            setup("order", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );
        let rejected = broker.on_tick(tick("NIFTY-A", rupees(8), BASE_TIME + 99), BASE_TIME + 101);
        assert_eq!(rejected.rejection, Some(TickRejection::OutOfOrder));
        for mode in ShadowMode::ALL {
            assert_eq!(
                account_snapshot(&broker, mode, BASE_TIME + 101)
                    .open_positions
                    .len(),
                1
            );
        }
    }

    #[test]
    fn state_and_dashboard_snapshot_round_trip_through_json() {
        let mut broker = one_account_broker(5_000);
        let mut without_t2 = setup("persist", "NIFTY-A", Underlying::Nifty, BASE_TIME);
        without_t2.levels.t2_paise = None;
        broker.place_setup(without_t2, BASE_TIME);
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );

        let json = serde_json::to_string(&broker).unwrap();
        let restored: PaperBroker = serde_json::from_str(&json).unwrap();
        restored.validate_restored_state().unwrap();
        assert_eq!(restored, broker);

        let snapshot = restored.snapshot(BASE_TIME + 100);
        let snapshot_json = serde_json::to_string(&snapshot).unwrap();
        let round_trip: PaperBrokerSnapshot = serde_json::from_str(&snapshot_json).unwrap();
        assert_eq!(round_trip, snapshot);
    }

    #[test]
    fn validated_restore_resumes_pending_open_closed_cash_and_event_counter() {
        let config = PaperBrokerConfig::default();
        let accounts = vec![AccountSpec {
            account_id: "only".to_string(),
            display_name: "Only Account".to_string(),
            starting_capital_paise: rupees(50_000),
        }];
        let mut broker = PaperBroker::with_accounts(config.clone(), accounts.clone()).unwrap();
        broker
            .start_trading_day_ist("2026-08-11", BASE_TIME)
            .unwrap();

        broker.place_setup(
            setup("closed", "NIFTY-A", Underlying::Nifty, BASE_TIME + 1),
            BASE_TIME + 1,
        );
        broker.on_tick(tick("NIFTY-A", rupees(10), BASE_TIME + 10), BASE_TIME + 10);
        broker.on_tick(tick("NIFTY-A", rupees(8), BASE_TIME + 20), BASE_TIME + 20);
        broker.place_setup(
            setup("open", "NIFTY-B", Underlying::Nifty, BASE_TIME + 21),
            BASE_TIME + 21,
        );
        broker.on_tick(tick("NIFTY-B", rupees(10), BASE_TIME + 30), BASE_TIME + 30);
        let pending = broker.place_setup(
            setup("pending", "NIFTY-C", Underlying::Nifty, BASE_TIME + 31),
            BASE_TIME + 31,
        );
        assert_eq!(pending.orders_placed, 2);

        let before = broker.snapshot(BASE_TIME + 31);
        assert_eq!(before.latest_event_sequence > 0, true);
        for shadow in &before.shadows {
            let account = &shadow.accounts[0];
            assert_eq!(account.pending_entries.len(), 1);
            assert_eq!(account.open_positions.len(), 1);
            assert_eq!(account.closed_trades.len(), 1);
        }

        let json = serde_json::to_string(&broker).unwrap();
        let persisted: PaperBroker = serde_json::from_str(&json).unwrap();
        let mut restored =
            PaperBroker::restore_from_persisted(persisted, config, accounts).unwrap();
        assert_eq!(restored, broker);
        assert_eq!(restored.snapshot(BASE_TIME + 31), before);

        let events = restored.cancel_pending_setup("pending", BASE_TIME + 32);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, before.latest_event_sequence + 1);
        restored.validate_restored_state().unwrap();
    }

    #[test]
    fn restore_fails_closed_on_config_account_or_accounting_mismatch() {
        let config = PaperBrokerConfig::default();
        let accounts = vec![AccountSpec {
            account_id: "only".to_string(),
            display_name: "Only Account".to_string(),
            starting_capital_paise: rupees(5_000),
        }];
        let broker = PaperBroker::with_accounts(config.clone(), accounts.clone()).unwrap();

        let mut changed_config = config.clone();
        changed_config.exit_charge_paise += 1;
        assert!(matches!(
            PaperBroker::restore_from_persisted(broker.clone(), changed_config, accounts.clone()),
            Err(PaperError::InvalidState(_))
        ));

        let mut changed_accounts = accounts.clone();
        changed_accounts[0].starting_capital_paise += 1;
        assert!(matches!(
            PaperBroker::restore_from_persisted(broker.clone(), config.clone(), changed_accounts),
            Err(PaperError::InvalidState(_))
        ));

        let mut corrupted = broker;
        corrupted
            .books
            .get_mut(&ShadowMode::LlmExit)
            .unwrap()
            .accounts
            .get_mut("only")
            .unwrap()
            .cash_balance_paise -= 1;
        assert!(matches!(
            PaperBroker::restore_from_persisted(corrupted, config, accounts),
            Err(PaperError::InvalidState(_))
        ));
    }

    #[test]
    fn restored_eod_closeout_must_finish_before_safe_new_ist_day_reset() {
        let config = PaperBrokerConfig::default();
        let accounts = vec![AccountSpec {
            account_id: "only".to_string(),
            display_name: "Only Account".to_string(),
            starting_capital_paise: rupees(5_000),
        }];
        let mut broker = PaperBroker::with_accounts(config.clone(), accounts.clone()).unwrap();
        broker
            .start_trading_day_ist("2026-08-11", BASE_TIME)
            .unwrap();
        broker.place_setup(
            setup("overnight", "NIFTY-A", Underlying::Nifty, BASE_TIME + 1),
            BASE_TIME + 1,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );
        let eod_at = BASE_TIME + 200;
        broker.trigger_end_of_day(eod_at);

        let persisted: PaperBroker =
            serde_json::from_str(&serde_json::to_string(&broker).unwrap()).unwrap();
        let mut restored =
            PaperBroker::restore_from_persisted(persisted, config, accounts).unwrap();
        let before_failed_roll = restored.clone();
        assert!(
            restored
                .start_trading_day_ist("2026-08-12", eod_at + 1)
                .is_err()
        );
        assert_eq!(restored, before_failed_roll);

        let close = restored.on_tick(tick("NIFTY-A", rupees(11), eod_at + 2), eod_at + 2);
        assert_eq!(close.positions_closed, 2);
        let before_roll = restored.snapshot(eod_at + 2);
        let roll_events = restored
            .start_trading_day_ist("2026-08-12", eod_at + 3)
            .unwrap();
        assert_eq!(roll_events.len(), 1);
        assert_eq!(roll_events[0].event_type, EventType::TradingDayStarted);
        assert_eq!(restored.trading_date_ist(), Some("2026-08-12"));
        let after_roll = restored.snapshot(eod_at + 3);
        assert_eq!(after_roll.session_status, SessionStatus::Open);
        assert_eq!(after_roll.end_of_day_timestamp_ms, None);
        assert!(after_roll.latest_ticks.is_empty());
        assert_eq!(
            after_roll.combined_shadow_totals,
            before_roll.combined_shadow_totals
        );
        assert_eq!(
            after_roll.closed_trade_history,
            before_roll.closed_trade_history
        );
        assert_eq!(
            after_roll.accepted_setup_count,
            before_roll.accepted_setup_count
        );
        restored.validate_restored_state().unwrap();
    }

    #[test]
    fn trading_day_annotation_is_idempotent_and_rollover_never_mutates_open_state() {
        let mut broker = one_account_broker(5_000);
        let first = broker
            .start_trading_day_ist("2026-08-11", BASE_TIME)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert!(
            broker
                .start_trading_day_ist("2026-08-11", BASE_TIME + 1)
                .unwrap()
                .is_empty()
        );
        broker.place_setup(
            setup("open-roll", "NIFTY-A", Underlying::Nifty, BASE_TIME + 2),
            BASE_TIME + 2,
        );
        broker.on_tick(tick("NIFTY-A", rupees(10), BASE_TIME + 3), BASE_TIME + 3);
        let unchanged = broker.clone();
        assert!(
            broker
                .start_trading_day_ist("2026-08-12", BASE_TIME + 4)
                .is_err()
        );
        assert_eq!(broker, unchanged);
        assert!(
            broker
                .start_trading_day_ist("2026-08-10", BASE_TIME + 4)
                .is_err()
        );
        assert_eq!(broker, unchanged);
    }

    #[test]
    fn additive_trading_date_field_keeps_pre_tracking_v1_json_restorable() {
        let config = PaperBrokerConfig::default();
        let accounts = vec![AccountSpec {
            account_id: "only".to_string(),
            display_name: "Only Account".to_string(),
            starting_capital_paise: rupees(5_000),
        }];
        let broker = PaperBroker::with_accounts(config.clone(), accounts.clone()).unwrap();
        let mut value = serde_json::to_value(&broker).unwrap();
        value.as_object_mut().unwrap().remove("trading_date_ist");
        let old_v1: PaperBroker = serde_json::from_value(value).unwrap();
        let restored = PaperBroker::restore_from_persisted(old_v1, config, accounts).unwrap();
        assert_eq!(restored.trading_date_ist(), None);
    }

    #[test]
    fn bounded_event_page_reports_retention_gap_for_realtime_dashboard() {
        let mut config = PaperBrokerConfig::default();
        config.event_capacity = 3;
        let mut broker = PaperBroker::with_accounts(
            config,
            vec![AccountSpec {
                account_id: "only".to_string(),
                display_name: "Only".to_string(),
                starting_capital_paise: rupees(5_000),
            }],
        )
        .unwrap();
        broker.place_setup(
            setup("events", "NIFTY-A", Underlying::Nifty, BASE_TIME),
            BASE_TIME,
        );
        broker.on_tick(
            tick("NIFTY-A", rupees(10), BASE_TIME + 100),
            BASE_TIME + 100,
        );
        broker.request_llm_exit("events", BASE_TIME + 110);
        broker.on_tick(
            tick("NIFTY-A", rupees(11), BASE_TIME + 120),
            BASE_TIME + 120,
        );

        let page = broker.event_page_after(0);
        assert!(page.retention_gap);
        assert_eq!(page.events.len(), 3);
        assert!(page.oldest_available_sequence.unwrap() > 1);
        let latest = page.latest_available_sequence.unwrap();
        assert!(broker.event_page_after(latest).events.is_empty());
    }
}
