//! Newest position snapshot only, held pending until the writer takes it — including at run end.

use std::time::Duration;

use rtrb::Producer;

use crate::exposure::ExposureSnapshot;
use crate::warn;

/// Output ring: replace pending, retry cadence, destructor drains final.
pub struct ExposureSink {
    producer: Producer<ExposureSnapshot>,
    pending: Option<ExposureSnapshot>,
    superseded: u64,
}

/// The writer drains its ring on a 100ms poll; 2ms x 250 attempts spans five polls, long enough
/// to survive a brief writer hiccup without hanging shutdown.
const FLUSH_ATTEMPT_INTERVAL: Duration = Duration::from_millis(2);
const FLUSH_ATTEMPTS: u32 = 250;

/// Destructor flushes final snapshot (covers drain + fatal + panic unwind). Blocking safe at thread end, bounded.
impl Drop for ExposureSink {
    fn drop(&mut self) {
        for _ in 0..FLUSH_ATTEMPTS {
            self.retry_pending();
            // Abandoned ring -> writer gone, nothing to do.
            if !self.is_pending() || self.producer.is_abandoned() {
                return;
            }
            std::thread::sleep(FLUSH_ATTEMPT_INTERVAL);
        }
        warn!(
            "exposure: the run's final position never reached the writer after {} ms — the file on disk is one change stale",
            FLUSH_ATTEMPT_INTERVAL.as_millis() * u128::from(FLUSH_ATTEMPTS)
        );
    }
}

impl ExposureSink {
    pub fn new(producer: Producer<ExposureSnapshot>) -> Self {
        ExposureSink {
            producer,
            pending: None,
            superseded: 0,
        }
    }

    pub fn push(&mut self, snapshot: ExposureSnapshot) {
        if self.pending.replace(snapshot).is_some() {
            self.superseded += 1;
        }
        self.retry_pending();
    }

    #[inline]
    pub fn retry_pending(&mut self) {
        let Some(snapshot) = self.pending else { return };
        if self.producer.push(snapshot).is_ok() {
            self.pending = None;
        }
    }

    #[inline]
    pub fn is_pending(&self) -> bool {
        self.pending.is_some()
    }

    /// Nonzero means the writer is falling behind the hot side.
    pub fn superseded(&self) -> u64 {
        self.superseded
    }
}
