#[path = "../blocker.rs"]
mod blocker;

use std::{collections::BTreeSet, env, fs, path::PathBuf, process::ExitCode};

use blocker::{BlockReason, ClipClass, ClipClassifier, DispatchEvent, Dispatcher, InputClip};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("blocker replay failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let (path, logical_limit) = parse_args(env::args().skip(1))?;
    let content = fs::read_to_string(&path)
        .map_err(|error| format!("could not read {}: {error}", path.display()))?;
    let classifier = ClipClassifier::default();
    let mut dispatcher = Dispatcher::default();
    let mut stats = ReplayStats::default();
    let mut last_end_ms = None;

    for line in content.lines().filter(|line| line.starts_with('[')) {
        if logical_limit.is_some_and(|limit| stats.input_clips >= limit) {
            break;
        }
        let sequence = stats.input_clips as u64 + 1;
        let clip = parse_transcript_line(sequence, line)?;
        if matches!(classifier.classify(&clip.text), ClipClass::MustPass { .. }) {
            stats.must_pass_total += 1;
        }
        last_end_ms = Some(clip.start_ms + clip.duration_ms as i64);
        stats.record(dispatcher.ingest(clip), &classifier);
        stats.input_clips += 1;
    }

    let finish_at =
        last_end_ms.ok_or_else(|| "transcript contains no timestamped clips".to_owned())? + 20_000;
    stats.record(dispatcher.finish(finish_at), &classifier);

    println!("input_clips={}", stats.input_clips);
    println!("blocked_silence={}", stats.blocked_silence);
    println!("blocked_keyword={}", stats.blocked_keyword);
    println!("full_set_calls={}", stats.full_set_calls);
    println!("must_solo_calls={}", stats.must_solo_calls);
    println!("expired_normal={}", stats.expired_normal);
    println!("must_pass_total={}", stats.must_pass_total);
    println!("must_pass_lost={}", stats.must_pass_lost());
    println!(
        "expired_normal_with_must_term={}",
        stats.expired_normal_with_must_term
    );
    println!("duplicate_dispatch_ids={}", stats.duplicate_dispatch_ids);

    if stats.must_pass_lost() != 0
        || stats.expired_normal_with_must_term != 0
        || stats.duplicate_dispatch_ids != 0
    {
        return Err("prototype acceptance checks failed".to_owned());
    }
    Ok(())
}

fn parse_args(
    arguments: impl IntoIterator<Item = String>,
) -> Result<(PathBuf, Option<usize>), String> {
    let mut values = arguments.into_iter();
    let path = values
        .next()
        .ok_or_else(|| "usage: blocker_replay <transcript.txt> [--logical-clips N]".to_owned())?;
    let mut logical_limit = None;
    while let Some(argument) = values.next() {
        if argument != "--logical-clips" {
            return Err(format!("unknown argument: {argument}"));
        }
        let raw_limit = values
            .next()
            .ok_or_else(|| "--logical-clips requires a positive integer".to_owned())?;
        let limit = raw_limit
            .parse::<usize>()
            .map_err(|_| "--logical-clips requires a positive integer".to_owned())?;
        if limit == 0 {
            return Err("--logical-clips requires a positive integer".to_owned());
        }
        logical_limit = Some(limit);
    }
    Ok((PathBuf::from(path), logical_limit))
}

fn parse_transcript_line(sequence: u64, line: &str) -> Result<InputClip, String> {
    let closing = line
        .find(']')
        .ok_or_else(|| format!("missing timestamp close bracket: {line}"))?;
    let timestamps = line
        .strip_prefix('[')
        .ok_or_else(|| format!("missing timestamp open bracket: {line}"))?
        .get(..closing - 1)
        .ok_or_else(|| format!("invalid timestamp bracket: {line}"))?;
    let (start, end) = timestamps
        .split_once(" - ")
        .ok_or_else(|| format!("invalid timestamp range: {line}"))?;
    let start_ms = parse_clock_ms(start)?;
    let mut end_ms = parse_clock_ms(end)?;
    if end_ms <= start_ms {
        end_ms += 24 * 60 * 60 * 1_000;
    }
    let mut text = line[closing + 1..].trim().to_owned();
    if text.starts_with("[speaker_") {
        let speaker_end = text
            .find(']')
            .ok_or_else(|| format!("invalid speaker marker: {line}"))?;
        text = text[speaker_end + 1..].trim().to_owned();
    }
    if text.eq_ignore_ascii_case("[no speech detected]") {
        text.clear();
    }
    Ok(InputClip::new(
        sequence,
        start_ms,
        (end_ms - start_ms) as u64,
        text,
    ))
}

fn parse_clock_ms(raw: &str) -> Result<i64, String> {
    let lower = raw.trim().to_ascii_lowercase();
    let (clock, is_pm) = if let Some(clock) = lower.strip_suffix("am") {
        (clock, false)
    } else if let Some(clock) = lower.strip_suffix("pm") {
        (clock, true)
    } else {
        return Err(format!("clock must end with am or pm: {raw}"));
    };
    let fields = clock.trim().split(':').collect::<Vec<_>>();
    if fields.len() != 3 {
        return Err(format!("clock must have h:m:s fields: {raw}"));
    }
    let hour = fields[0]
        .parse::<i64>()
        .map_err(|_| format!("invalid hour: {raw}"))?;
    let minute = fields[1]
        .parse::<i64>()
        .map_err(|_| format!("invalid minute: {raw}"))?;
    let second = fields[2]
        .parse::<i64>()
        .map_err(|_| format!("invalid second: {raw}"))?;
    if !(1..=12).contains(&hour) || !(0..60).contains(&minute) || !(0..60).contains(&second) {
        return Err(format!("clock field out of range: {raw}"));
    }
    let hour_24 = match (hour, is_pm) {
        (12, false) => 0,
        (12, true) => 12,
        (hour, true) => hour + 12,
        (hour, false) => hour,
    };
    Ok(((hour_24 * 60 * 60) + (minute * 60) + second) * 1_000)
}

#[derive(Default)]
struct ReplayStats {
    input_clips: usize,
    blocked_silence: usize,
    blocked_keyword: usize,
    full_set_calls: usize,
    must_solo_calls: usize,
    expired_normal: usize,
    must_pass_total: usize,
    must_pass_dispatched: usize,
    expired_normal_with_must_term: usize,
    dispatched_ids: BTreeSet<u64>,
    duplicate_dispatch_ids: usize,
}

impl ReplayStats {
    fn record(&mut self, events: Vec<DispatchEvent>, classifier: &ClipClassifier) {
        for event in events {
            match &event {
                DispatchEvent::Blocked { reason, .. } => match reason {
                    BlockReason::Silence => self.blocked_silence += 1,
                    BlockReason::Keyword(_) => self.blocked_keyword += 1,
                },
                DispatchEvent::FullSet { clips } => {
                    self.full_set_calls += 1;
                    for clip in clips {
                        if !clip.must_terms.is_empty() {
                            self.must_pass_dispatched += 1;
                        }
                    }
                }
                DispatchEvent::MustSolo { clip } => {
                    self.must_solo_calls += 1;
                    if !clip.must_terms.is_empty() {
                        self.must_pass_dispatched += 1;
                    }
                }
                DispatchEvent::ExpiredNormal { audit } => {
                    self.expired_normal += 1;
                    if matches!(classifier.classify(&audit.text), ClipClass::MustPass { .. }) {
                        self.expired_normal_with_must_term += 1;
                    }
                }
            }
            for clip in event.clips() {
                if !self.dispatched_ids.insert(clip.sequence) {
                    self.duplicate_dispatch_ids += 1;
                }
            }
        }
    }

    fn must_pass_lost(&self) -> usize {
        self.must_pass_total
            .saturating_sub(self.must_pass_dispatched)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_timestamped_speaker_line() {
        let clip = parse_transcript_line(1, "[9:01:42am - 9:01:45am] [speaker_0] Nifty call")
            .expect("valid transcript line");

        assert_eq!(clip.sequence, 1);
        assert_eq!(clip.start_ms, 32_502_000);
        assert_eq!(clip.duration_ms, 3_000);
        assert_eq!(clip.text, "Nifty call");
    }

    #[test]
    fn parser_turns_no_speech_marker_into_empty_text() {
        let clip = parse_transcript_line(2, "[9:01:45am - 9:01:48am] [no speech detected]")
            .expect("valid transcript line");

        assert_eq!(clip.text, "");
    }
}
