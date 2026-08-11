//! Deterministic moving-stop logic for long (BUY) option positions.
//!
//! The caller is expected to pass only accepted market ticks to
//! [`TrailState::update_on_tick`]. Exit checks carry their own freshness flag so
//! a delayed price cannot close a paper position.

use std::{error::Error, fmt};

/// Index whose option contract is being managed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Underlying {
    Nifty,
    Sensex,
}

impl Underlying {
    fn runner_stride(self) -> f64 {
        match self {
            Self::Nifty => 5.0,
            Self::Sensex => 8.0,
        }
    }

    fn runner_sl_increment(self) -> f64 {
        match self {
            Self::Nifty => 4.0,
            Self::Sensex => 6.0,
        }
    }

    fn t2_sl_offset(self) -> f64 {
        match self {
            Self::Nifty => 5.0,
            Self::Sensex => 10.0,
        }
    }
}

/// Streamer-provided levels for a long option trade.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TrailLevels {
    pub entry: f64,
    pub hard_sl: f64,
    pub t1: f64,
    pub t2: Option<f64>,
}

impl TrailLevels {
    pub fn new(entry: f64, hard_sl: f64, t1: f64, t2: Option<f64>) -> Result<Self, TrailError> {
        let levels = Self {
            entry,
            hard_sl,
            t1,
            t2,
        };
        levels.validate()?;
        Ok(levels)
    }

    /// Checks that every supplied level is finite and ordered for a BUY trade.
    pub fn validate(&self) -> Result<(), TrailError> {
        validate_finite("entry", self.entry)?;
        validate_finite("hard_sl", self.hard_sl)?;
        validate_finite("t1", self.t1)?;
        if let Some(t2) = self.t2 {
            validate_finite("t2", t2)?;
        }

        if self.hard_sl >= self.entry {
            return Err(TrailError::InvalidOrdering(
                "hard_sl must be strictly below entry",
            ));
        }
        if self.entry >= self.t1 {
            return Err(TrailError::InvalidOrdering(
                "entry must be strictly below t1",
            ));
        }
        if let Some(t2) = self.t2 {
            if self.t1 >= t2 {
                return Err(TrailError::InvalidOrdering("t1 must be strictly below t2"));
            }
        }

        Ok(())
    }
}

/// Highest moving-stop phase reached by the trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TrailPhase {
    /// Entry filled; the streamer's hard stop is active.
    Phase0,
    /// Price reached halfway from entry to T1.
    Phase1,
    /// Price reached T1.
    Phase2,
    /// Price reached halfway from T1 to T2.
    Phase3,
    /// Price reached T2.
    Phase4,
    /// At least one runner step beyond T2, or beyond T1 when T2 is absent.
    Phase5,
}

/// Stateful moving stop for one long option position.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TrailState {
    pub phase: TrailPhase,
    pub effective_sl: f64,
    pub runner_steps: u64,
    underlying: Underlying,
    levels: TrailLevels,
}

impl TrailState {
    pub fn new(underlying: Underlying, levels: TrailLevels) -> Result<Self, TrailError> {
        levels.validate()?;
        Ok(Self {
            phase: TrailPhase::Phase0,
            effective_sl: levels.hard_sl,
            runner_steps: 0,
            underlying,
            levels,
        })
    }

    pub fn underlying(&self) -> Underlying {
        self.underlying
    }

    pub fn levels(&self) -> TrailLevels {
        self.levels
    }

    /// Applies every newly crossed phase in order and returns whether state changed.
    ///
    /// A single gap-up tick can cross multiple phases. Falling or repeated ticks
    /// never reduce the phase, runner-step count, or effective stop.
    pub fn update_on_tick(&mut self, ltp: f64) -> Result<bool, TrailError> {
        validate_finite("ltp", ltp)?;

        let before = (self.phase, self.effective_sl, self.runner_steps);
        let d1 = self.levels.t1 - self.levels.entry;

        let phase1_trigger = self.levels.entry + 0.5 * d1;
        if self.phase < TrailPhase::Phase1 && reached(ltp, phase1_trigger) {
            self.raise_sl_to(self.levels.entry + 0.3 * d1);
            self.phase = TrailPhase::Phase1;
        }

        if self.phase < TrailPhase::Phase2 && reached(ltp, self.levels.t1) {
            self.raise_sl_to(self.levels.entry + 0.5 * d1);
            self.phase = TrailPhase::Phase2;
        }

        match self.levels.t2 {
            Some(t2) => self.update_with_t2(ltp, t2),
            None => self.update_runner(ltp, self.levels.t1),
        }

        Ok(before != (self.phase, self.effective_sl, self.runner_steps))
    }

    /// Returns true only when a fresh, finite tick is at or below the active stop.
    pub fn should_exit(&self, ltp: f64, is_fresh: bool) -> bool {
        is_fresh
            && ltp.is_finite()
            && (ltp <= self.effective_sl
                || (ltp - self.effective_sl).abs() <= comparison_tolerance(ltp, self.effective_sl))
    }

    fn update_with_t2(&mut self, ltp: f64, t2: f64) {
        let d2 = t2 - self.levels.t1;
        let phase3_trigger = self.levels.t1 + 0.5 * d2;

        if self.phase < TrailPhase::Phase3 && reached(ltp, phase3_trigger) {
            // Phase 3 is defined relative to the previously active stop.
            self.effective_sl += 0.3 * d2;
            self.phase = TrailPhase::Phase3;
        }

        if self.phase < TrailPhase::Phase4 && reached(ltp, t2) {
            self.raise_sl_to(t2 - self.underlying.t2_sl_offset());
            self.phase = TrailPhase::Phase4;
        }

        self.update_runner(ltp, t2);
    }

    fn update_runner(&mut self, ltp: f64, anchor: f64) {
        // With no T2 the runner begins only after T1/Phase 2 has been reached.
        // With T2 it begins only after T2/Phase 4 has been reached.
        let required_phase = if self.levels.t2.is_some() {
            TrailPhase::Phase4
        } else {
            TrailPhase::Phase2
        };
        if self.phase < required_phase {
            return;
        }

        let completed = completed_runner_steps(ltp, anchor, self.underlying.runner_stride());
        if completed > self.runner_steps {
            let new_steps = completed - self.runner_steps;
            self.effective_sl += new_steps as f64 * self.underlying.runner_sl_increment();
            self.runner_steps = completed;
            self.phase = TrailPhase::Phase5;
        }
    }

    fn raise_sl_to(&mut self, candidate: f64) {
        if candidate > self.effective_sl {
            self.effective_sl = candidate;
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TrailError {
    NonFiniteLevel { field: &'static str, value: f64 },
    InvalidOrdering(&'static str),
}

impl fmt::Display for TrailError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonFiniteLevel { field, value } => {
                write!(f, "{field} must be finite, got {value}")
            }
            Self::InvalidOrdering(message) => f.write_str(message),
        }
    }
}

impl Error for TrailError {}

fn validate_finite(field: &'static str, value: f64) -> Result<(), TrailError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(TrailError::NonFiniteLevel { field, value })
    }
}

fn reached(value: f64, threshold: f64) -> bool {
    value >= threshold || (value - threshold).abs() <= comparison_tolerance(value, threshold)
}

fn comparison_tolerance(left: f64, right: f64) -> f64 {
    32.0 * f64::EPSILON * left.abs().max(right.abs()).max(1.0)
}

fn completed_runner_steps(ltp: f64, anchor: f64, stride: f64) -> u64 {
    let raw_steps = (ltp - anchor) / stride;
    if raw_steps < 1.0 && !reached(raw_steps, 1.0) {
        return 0;
    }

    let nearest_integer = raw_steps.round();
    let normalized = if (raw_steps - nearest_integer).abs()
        <= comparison_tolerance(raw_steps, nearest_integer)
    {
        nearest_integer
    } else {
        raw_steps.floor()
    };
    normalized.max(0.0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1e-9;

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() <= EPS,
            "expected {expected}, got {actual}"
        );
    }

    fn levels_with_t2() -> TrailLevels {
        TrailLevels::new(100.0, 90.0, 120.0, Some(160.0)).unwrap()
    }

    fn levels_without_t2() -> TrailLevels {
        TrailLevels::new(100.0, 90.0, 120.0, None).unwrap()
    }

    #[test]
    fn validates_all_finite_levels() {
        for levels in [
            TrailLevels {
                entry: f64::NAN,
                hard_sl: 90.0,
                t1: 120.0,
                t2: None,
            },
            TrailLevels {
                entry: 100.0,
                hard_sl: f64::NEG_INFINITY,
                t1: 120.0,
                t2: None,
            },
            TrailLevels {
                entry: 100.0,
                hard_sl: 90.0,
                t1: f64::INFINITY,
                t2: None,
            },
            TrailLevels {
                entry: 100.0,
                hard_sl: 90.0,
                t1: 120.0,
                t2: Some(f64::NAN),
            },
        ] {
            assert!(matches!(
                levels.validate(),
                Err(TrailError::NonFiniteLevel { .. })
            ));
        }
    }

    #[test]
    fn validates_buy_level_ordering() {
        assert!(TrailLevels::new(100.0, 100.0, 120.0, None).is_err());
        assert!(TrailLevels::new(100.0, 90.0, 100.0, None).is_err());
        assert!(TrailLevels::new(100.0, 90.0, 120.0, Some(120.0)).is_err());

        assert!(TrailLevels::new(100.0, 99.99, 100.01, Some(100.02)).is_ok());
    }

    #[test]
    fn starts_in_phase_zero_at_hard_stop() {
        let state = TrailState::new(Underlying::Nifty, levels_with_t2()).unwrap();
        assert_eq!(state.phase, TrailPhase::Phase0);
        assert_close(state.effective_sl, 90.0);
        assert_eq!(state.runner_steps, 0);
        assert_eq!(state.underlying(), Underlying::Nifty);
        assert_eq!(state.levels(), levels_with_t2());
    }

    #[test]
    fn phase_one_uses_thirty_percent_of_entry_to_t1_distance() {
        let mut state = TrailState::new(Underlying::Nifty, levels_with_t2()).unwrap();
        assert!(!state.update_on_tick(109.99).unwrap());
        assert!(state.update_on_tick(110.0).unwrap());
        assert_eq!(state.phase, TrailPhase::Phase1);
        assert_close(state.effective_sl, 106.0);
    }

    #[test]
    fn phase_two_uses_fifty_percent_of_entry_to_t1_distance() {
        let mut state = TrailState::new(Underlying::Nifty, levels_with_t2()).unwrap();
        state.update_on_tick(110.0).unwrap();
        assert!(!state.update_on_tick(119.99).unwrap());
        assert!(state.update_on_tick(120.0).unwrap());
        assert_eq!(state.phase, TrailPhase::Phase2);
        assert_close(state.effective_sl, 110.0);
    }

    #[test]
    fn phase_three_adds_thirty_percent_of_t1_to_t2_distance() {
        let mut state = TrailState::new(Underlying::Nifty, levels_with_t2()).unwrap();
        state.update_on_tick(120.0).unwrap();
        assert!(!state.update_on_tick(139.99).unwrap());
        assert!(state.update_on_tick(140.0).unwrap());
        assert_eq!(state.phase, TrailPhase::Phase3);
        assert_close(state.effective_sl, 122.0);
    }

    #[test]
    fn phase_four_uses_underlying_specific_t2_offset() {
        let mut nifty = TrailState::new(Underlying::Nifty, levels_with_t2()).unwrap();
        nifty.update_on_tick(160.0).unwrap();
        assert_eq!(nifty.phase, TrailPhase::Phase4);
        assert_close(nifty.effective_sl, 155.0);

        let mut sensex = TrailState::new(Underlying::Sensex, levels_with_t2()).unwrap();
        sensex.update_on_tick(160.0).unwrap();
        assert_eq!(sensex.phase, TrailPhase::Phase4);
        assert_close(sensex.effective_sl, 150.0);
    }

    #[test]
    fn nifty_runner_counts_only_complete_five_point_steps() {
        let mut state = TrailState::new(Underlying::Nifty, levels_with_t2()).unwrap();
        state.update_on_tick(160.0).unwrap();

        assert!(!state.update_on_tick(164.999).unwrap());
        assert!(state.update_on_tick(165.0).unwrap());
        assert_eq!(state.phase, TrailPhase::Phase5);
        assert_eq!(state.runner_steps, 1);
        assert_close(state.effective_sl, 159.0);

        assert!(state.update_on_tick(176.0).unwrap());
        assert_eq!(state.runner_steps, 3);
        assert_close(state.effective_sl, 167.0);
    }

    #[test]
    fn sensex_runner_counts_only_complete_eight_point_steps() {
        let mut state = TrailState::new(Underlying::Sensex, levels_with_t2()).unwrap();
        state.update_on_tick(160.0).unwrap();

        assert!(!state.update_on_tick(167.999).unwrap());
        assert!(state.update_on_tick(168.0).unwrap());
        assert_eq!(state.runner_steps, 1);
        assert_close(state.effective_sl, 156.0);

        assert!(state.update_on_tick(184.0).unwrap());
        assert_eq!(state.runner_steps, 3);
        assert_close(state.effective_sl, 168.0);
    }

    #[test]
    fn no_t2_jumps_from_phase_two_to_runner_anchored_at_t1() {
        let mut state = TrailState::new(Underlying::Nifty, levels_without_t2()).unwrap();

        state.update_on_tick(120.0).unwrap();
        assert_eq!(state.phase, TrailPhase::Phase2);
        assert_close(state.effective_sl, 110.0);

        assert!(!state.update_on_tick(124.999).unwrap());
        assert!(state.update_on_tick(125.0).unwrap());
        assert_eq!(state.phase, TrailPhase::Phase5);
        assert_eq!(state.runner_steps, 1);
        assert_close(state.effective_sl, 114.0);
    }

    #[test]
    fn gap_tick_applies_all_crossed_phases_sequentially() {
        let mut state = TrailState::new(Underlying::Nifty, levels_with_t2()).unwrap();
        assert!(state.update_on_tick(175.0).unwrap());

        assert_eq!(state.phase, TrailPhase::Phase5);
        assert_eq!(state.runner_steps, 3);
        assert_close(state.effective_sl, 167.0);
    }

    #[test]
    fn falling_and_repeated_ticks_never_loosen_or_double_count() {
        let mut state = TrailState::new(Underlying::Nifty, levels_with_t2()).unwrap();
        state.update_on_tick(175.0).unwrap();
        let high_water_state = state;

        assert!(!state.update_on_tick(161.0).unwrap());
        assert!(!state.update_on_tick(175.0).unwrap());
        assert_eq!(state, high_water_state);

        assert!(state.update_on_tick(180.0).unwrap());
        assert_eq!(state.runner_steps, 4);
        assert_close(state.effective_sl, high_water_state.effective_sl + 4.0);
    }

    #[test]
    fn phase_four_never_lowers_a_tighter_previous_stop() {
        let levels = TrailLevels::new(100.0, 99.0, 102.0, Some(104.0)).unwrap();
        let mut state = TrailState::new(Underlying::Nifty, levels).unwrap();

        state.update_on_tick(103.0).unwrap();
        let phase3_sl = state.effective_sl;
        state.update_on_tick(104.0).unwrap();

        assert_eq!(state.phase, TrailPhase::Phase4);
        assert_close(state.effective_sl, phase3_sl);
        assert!(state.effective_sl > 104.0 - 5.0);
    }

    #[test]
    fn should_exit_requires_a_fresh_finite_tick_at_or_below_stop() {
        let state = TrailState::new(Underlying::Nifty, levels_with_t2()).unwrap();

        assert!(state.should_exit(90.0, true));
        assert!(state.should_exit(89.99, true));
        assert!(!state.should_exit(90.01, true));
        assert!(!state.should_exit(89.0, false));
        assert!(!state.should_exit(f64::NAN, true));
        assert!(!state.should_exit(f64::NEG_INFINITY, true));
    }

    #[test]
    fn invalid_tick_is_rejected_without_mutating_state() {
        let mut state = TrailState::new(Underlying::Nifty, levels_with_t2()).unwrap();
        let before = state;

        assert!(matches!(
            state.update_on_tick(f64::NAN),
            Err(TrailError::NonFiniteLevel { field: "ltp", .. })
        ));
        assert_eq!(state, before);
    }

    #[test]
    fn decimal_runner_boundaries_are_stable() {
        let levels = TrailLevels::new(100.1, 90.1, 120.1, Some(160.1)).unwrap();
        let mut state = TrailState::new(Underlying::Nifty, levels).unwrap();

        state.update_on_tick(165.1).unwrap();
        assert_eq!(state.runner_steps, 1);
        assert_eq!(state.phase, TrailPhase::Phase5);
    }
}
