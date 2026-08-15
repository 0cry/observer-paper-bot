//! RAM-only bounded material retained solely to recover a collapsed analysis
//! dispatch. Normal provider prompts never include this buffer.

use std::collections::VecDeque;

use crate::stt::TranscriptChunk;

pub const RECOVERY_TRANSCRIPT_WINDOW_SECONDS: f64 = 60.0;
pub const RECOVERY_IMAGE_LIMIT: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryImage {
    pub source_sequence: u64,
    pub jpeg: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct RecoveryBuffer {
    transcripts: VecDeque<TranscriptChunk>,
    images: VecDeque<RecoveryImage>,
}

impl RecoveryBuffer {
    pub fn push_transcript(&mut self, chunk: TranscriptChunk) {
        let newest_end = chunk.end_sec;
        self.transcripts.push_back(chunk);
        let cutoff = newest_end - RECOVERY_TRANSCRIPT_WINDOW_SECONDS;
        while self
            .transcripts
            .front()
            .is_some_and(|oldest| oldest.end_sec <= cutoff)
        {
            self.transcripts.pop_front();
        }
    }

    pub fn push_image(&mut self, image: RecoveryImage) {
        self.images.push_back(image);
        while self.images.len() > RECOVERY_IMAGE_LIMIT {
            self.images.pop_front();
        }
    }

    pub fn transcripts(&self) -> &VecDeque<TranscriptChunk> {
        &self.transcripts
    }

    pub fn images(&self) -> &VecDeque<RecoveryImage> {
        &self.images
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stt::{TranscriptFailure, TranscriptStatus};

    fn chunk(index: u64, start_sec: f64, end_sec: f64) -> TranscriptChunk {
        TranscriptChunk {
            index,
            start_sec,
            end_sec,
            status: TranscriptStatus::Complete,
            failure: None::<TranscriptFailure>,
            text: format!("segment-{index}"),
            word_timestamps: Vec::new(),
            speakers: Vec::new(),
            language_code: None,
        }
    }

    #[test]
    fn keeps_only_the_latest_sixty_seconds_of_transcript() {
        let mut buffer = RecoveryBuffer::default();
        for index in 0..=21 {
            let start = index as f64 * 3.0;
            buffer.push_transcript(chunk(index, start, start + 3.0));
        }
        assert_eq!(buffer.transcripts().front().unwrap().index, 2);
        assert_eq!(buffer.transcripts().back().unwrap().index, 21);
        assert!(
            buffer.transcripts().back().unwrap().end_sec
                - buffer.transcripts().front().unwrap().end_sec
                < 60.0
        );
    }

    #[test]
    fn keeps_only_the_latest_three_images() {
        let mut buffer = RecoveryBuffer::default();
        for sequence in 1..=5 {
            buffer.push_image(RecoveryImage {
                source_sequence: sequence,
                jpeg: vec![sequence as u8],
            });
        }
        assert_eq!(
            buffer
                .images()
                .iter()
                .map(|image| image.source_sequence)
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
    }
}
