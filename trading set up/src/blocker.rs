//! Pure, deterministic transcript filtering used by the local blocker prototype.
//!
//! This module is intentionally not wired into the live runtime. Its replay binary
//! is opt-in and reads a transcript file supplied on the command line.

const BLOCK_TERMS: &[&str] = &[
    "telegram",
    "tele gram",
    "whatsapp",
    "whats app",
    "watsapp",
    "vip",
    "v i p",
    "premium",
    "youtube",
    "twitter",
    "description",
    "description box",
    "link",
    "like",
    "share",
    "comment",
    "subscribe",
    "join",
    "welcome",
    "hello",
    "thank you",
    "take care",
    "weekend",
    "lemon",
    "lemonn",
    "coinswitch",
    "coin switch",
    "broker",
    "referral",
    "good morning",
    "good afternoon",
    "good evening",
    "forex",
    "gold",
    "silver",
    "xauusd",
    "xagusd",
    "crypto",
    "cryptocurrency",
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "solana",
    "crude oil",
    "commodities",
];

const MUST_PASS_TERMS: &[&str] = &[
    "risk",
    "channel",
    "market",
    "analysis",
    "trade",
    "call",
    "target",
    "entry",
    "sl",
    "stop loss",
    "buy",
    "sell",
    "exit",
    "book",
    "booked",
    "booking",
    "trail",
    "trailing",
    "hold",
    "square off",
    "option",
    "nifty",
    "banknifty",
    "bank nifty",
    "sensex",
];

const SILENCE_MARKERS: &[&str] = &[
    "",
    "no speech",
    "no speech detected",
    "silence",
    "background music",
    "music",
    "noise",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputClip {
    pub sequence: u64,
    pub start_ms: i64,
    pub duration_ms: u64,
    pub text: String,
}

impl InputClip {
    pub fn new(sequence: u64, start_ms: i64, duration_ms: u64, text: impl Into<String>) -> Self {
        Self {
            sequence,
            start_ms,
            duration_ms,
            text: text.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetainedClip {
    pub sequence: u64,
    pub start_ms: i64,
    pub duration_ms: u64,
    pub text: String,
    pub must_terms: Vec<String>,
}

impl RetainedClip {
    fn from_input(input: InputClip, must_terms: Vec<String>) -> Self {
        Self {
            sequence: input.sequence,
            start_ms: input.start_ms,
            duration_ms: input.duration_ms,
            text: input.text,
            must_terms,
        }
    }

    fn is_must_pass(&self) -> bool {
        !self.must_terms.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClipAudit {
    pub sequence: u64,
    pub start_ms: i64,
    pub text: String,
}

impl From<&InputClip> for ClipAudit {
    fn from(input: &InputClip) -> Self {
        Self {
            sequence: input.sequence,
            start_ms: input.start_ms,
            text: input.text.clone(),
        }
    }
}

impl From<&RetainedClip> for ClipAudit {
    fn from(clip: &RetainedClip) -> Self {
        Self {
            sequence: clip.sequence,
            start_ms: clip.start_ms,
            text: clip.text.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BlockReason {
    Silence,
    Keyword(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClipClass {
    Blocked { reason: BlockReason },
    MustPass { terms: Vec<String> },
    Pass,
}

#[derive(Clone, Debug, Default)]
pub struct ClipClassifier;

impl ClipClassifier {
    pub fn classify(&self, text: &str) -> ClipClass {
        let normalized = normalize(text);
        if SILENCE_MARKERS.contains(&normalized.as_str()) {
            return ClipClass::Blocked {
                reason: BlockReason::Silence,
            };
        }

        let must_terms = matching_terms(&normalized, MUST_PASS_TERMS);
        if !must_terms.is_empty() {
            return ClipClass::MustPass { terms: must_terms };
        }

        if let Some(keyword) = matching_terms(&normalized, BLOCK_TERMS).into_iter().next() {
            return ClipClass::Blocked {
                reason: BlockReason::Keyword(keyword),
            };
        }

        ClipClass::Pass
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatchEvent {
    Blocked {
        audit: ClipAudit,
        reason: BlockReason,
    },
    FullSet {
        clips: Vec<RetainedClip>,
    },
    MustSolo {
        clip: RetainedClip,
    },
    ExpiredNormal {
        audit: ClipAudit,
    },
}

impl DispatchEvent {
    pub fn clips(&self) -> Vec<&RetainedClip> {
        match self {
            Self::FullSet { clips } => clips.iter().collect(),
            Self::MustSolo { clip } => vec![clip],
            Self::Blocked { .. } | Self::ExpiredNormal { .. } => Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatcherConfig {
    pub full_set_size: usize,
    pub must_deadline_ms: i64,
    pub normal_expiry_ms: i64,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            full_set_size: 5,
            must_deadline_ms: 15_000,
            normal_expiry_ms: 30_000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Dispatcher {
    classifier: ClipClassifier,
    config: DispatcherConfig,
    pending: Vec<RetainedClip>,
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new(ClipClassifier, DispatcherConfig::default())
    }
}

impl Dispatcher {
    pub fn new(classifier: ClipClassifier, config: DispatcherConfig) -> Self {
        assert!(config.full_set_size > 0, "full_set_size must be positive");
        assert!(
            config.must_deadline_ms > 0,
            "must_deadline_ms must be positive"
        );
        assert!(config.normal_expiry_ms >= config.must_deadline_ms);
        Self {
            classifier,
            config,
            pending: Vec::new(),
        }
    }

    pub fn ingest(&mut self, clip: InputClip) -> Vec<DispatchEvent> {
        let mut events = self.advance_to(clip.start_ms);
        match self.classifier.classify(&clip.text) {
            ClipClass::Blocked { reason } => events.push(DispatchEvent::Blocked {
                audit: ClipAudit::from(&clip),
                reason,
            }),
            ClipClass::MustPass { terms } => {
                self.pending.push(RetainedClip::from_input(clip, terms));
                events.extend(self.dispatch_full_set());
            }
            ClipClass::Pass => {
                self.pending
                    .push(RetainedClip::from_input(clip, Vec::new()));
                events.extend(self.dispatch_full_set());
            }
        }
        events
    }

    pub fn advance_to(&mut self, now_ms: i64) -> Vec<DispatchEvent> {
        let mut events = Vec::new();
        let mut index = 0;
        while index < self.pending.len() {
            let clip = &self.pending[index];
            if clip.is_must_pass() && now_ms >= clip.start_ms + self.config.must_deadline_ms {
                events.push(DispatchEvent::MustSolo {
                    clip: self.pending.remove(index),
                });
                continue;
            }
            if now_ms >= clip.start_ms + self.config.normal_expiry_ms {
                let expired = self.pending.remove(index);
                if expired.is_must_pass() {
                    events.push(DispatchEvent::MustSolo { clip: expired });
                } else {
                    events.push(DispatchEvent::ExpiredNormal {
                        audit: ClipAudit::from(&expired),
                    });
                }
                continue;
            }
            index += 1;
        }
        events
    }

    pub fn finish(&mut self, now_ms: i64) -> Vec<DispatchEvent> {
        self.advance_to(now_ms)
    }

    fn dispatch_full_set(&mut self) -> Option<DispatchEvent> {
        if self.pending.len() < self.config.full_set_size {
            return None;
        }
        let clips = self.pending.drain(..self.config.full_set_size).collect();
        Some(DispatchEvent::FullSet { clips })
    }
}

fn normalize(text: &str) -> String {
    let mut result = String::new();
    let mut previous_space = true;
    for character in text.chars() {
        if character.is_alphanumeric() {
            for lower in character.to_lowercase() {
                result.push(lower);
            }
            previous_space = false;
        } else if !previous_space {
            result.push(' ');
            previous_space = true;
        }
    }
    result.trim().to_owned()
}

fn matching_terms(normalized: &str, terms: &[&str]) -> Vec<String> {
    let words = normalized.split_whitespace().collect::<Vec<_>>();
    terms
        .iter()
        .filter(|term| {
            let needle = term.split_whitespace().collect::<Vec<_>>();
            words.windows(needle.len()).any(|window| window == needle)
        })
        .map(|term| (*term).to_owned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clip(sequence: u64, text: &str) -> InputClip {
        InputClip::new(sequence, (sequence as i64 - 1) * 3_000, 3_000, text)
    }

    #[test]
    fn silence_is_blocked() {
        let classifier = ClipClassifier::default();

        assert!(matches!(
            classifier.classify("[no speech detected]"),
            ClipClass::Blocked {
                reason: BlockReason::Silence
            }
        ));
    }

    #[test]
    fn hindi_speech_is_not_mistaken_for_silence() {
        let classifier = ClipClassifier::default();

        assert!(matches!(
            classifier.classify("आज बाजार खुलने का इंतजार है"),
            ClipClass::Pass
        ));
    }

    #[test]
    fn must_pass_overrides_a_blocker_match() {
        let classifier = ClipClassifier::default();

        assert!(matches!(
            classifier.classify("Telegram Nifty call"),
            ClipClass::MustPass { .. }
        ));
    }

    #[test]
    fn blocker_matches_whole_words_only() {
        let classifier = ClipClassifier::default();

        assert!(matches!(
            classifier.classify("gold update"),
            ClipClass::Blocked { .. }
        ));
        assert!(matches!(
            classifier.classify("golden ratio"),
            ClipClass::Pass
        ));
    }

    #[test]
    fn five_nonconsecutive_retained_clips_dispatch_as_one_full_set() {
        let mut dispatcher = Dispatcher::default();
        let mut events = Vec::new();
        for (sequence, &text) in ["one", "", "Telegram", "four", "five", "six", "seven"]
            .iter()
            .enumerate()
        {
            events.extend(dispatcher.ingest(clip(sequence as u64 + 1, text)));
        }

        let full = events
            .iter()
            .find_map(|event| match event {
                DispatchEvent::FullSet { clips } => Some(clips),
                _ => None,
            })
            .expect("a full set must be emitted");
        assert_eq!(
            full.iter().map(|clip| clip.sequence).collect::<Vec<_>>(),
            vec![1, 4, 5, 6, 7]
        );
    }

    #[test]
    fn must_pass_dispatches_solo_after_fifteen_seconds() {
        let mut dispatcher = Dispatcher::default();
        let mut events = dispatcher.ingest(clip(1, "Nifty analysis"));
        events.extend(dispatcher.advance_to(14_999));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, DispatchEvent::MustSolo { .. }))
        );

        events.extend(dispatcher.advance_to(15_000));

        assert!(events.iter().any(|event| matches!(
            event,
            DispatchEvent::MustSolo { clip } if clip.sequence == 1
        )));
    }

    #[test]
    fn normal_pass_expires_after_thirty_seconds() {
        let mut dispatcher = Dispatcher::default();
        let _ = dispatcher.ingest(clip(1, "general commentary"));
        assert!(dispatcher.advance_to(20_000).is_empty());
        let events = dispatcher.advance_to(30_000);

        assert!(events.iter().any(|event| matches!(
            event,
            DispatchEvent::ExpiredNormal { audit } if audit.sequence == 1
        )));
    }

    #[test]
    fn action_synonyms_are_must_pass() {
        let classifier = ClipClassifier::default();

        for text in [
            "stop loss 127",
            "trail to 123",
            "part booking now",
            "Bank Nifty sell",
        ] {
            assert!(
                matches!(classifier.classify(text), ClipClass::MustPass { .. }),
                "{}",
                text
            );
        }
    }

    #[test]
    fn consumed_clips_are_never_emitted_twice() {
        let mut dispatcher = Dispatcher::default();
        let mut events = Vec::new();
        for sequence in 1..=5 {
            events.extend(dispatcher.ingest(clip(sequence, "regular commentary")));
        }
        events.extend(dispatcher.advance_to(30_000));

        let mut emitted = events
            .iter()
            .flat_map(DispatchEvent::clips)
            .map(|clip| clip.sequence)
            .collect::<Vec<_>>();
        emitted.sort_unstable();
        emitted.dedup();
        assert_eq!(emitted, vec![1, 2, 3, 4, 5]);
    }
}
