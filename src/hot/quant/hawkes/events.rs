//! The rolling window of event times that the fitter and the live evaluator both read. Its push
//! clamps a late stamp to the one before it, because every recursion here assumes time never runs
//! backwards.

use crate::hot::series::FastQueue;
use crate::time::{DurationUs, TsUs};

/// Event times as seconds since the first event; a full window evicts the oldest.
#[derive(Debug, Clone, PartialEq)]
pub struct HawkesEvents {
    times: FastQueue<f64>,
    origin: Option<TsUs>,
    last_secs: f64,
    out_of_order: u64,
}

impl HawkesEvents {
    /// # Panics
    /// `max_events == 0` (config bug).
    pub fn new(max_events: usize) -> Self {
        assert!(max_events != 0, "hawkes event window must be non-zero");
        Self {
            times: FastQueue::new(max_events, 2),
            origin: None,
            last_secs: 0.0,
            out_of_order: 0,
        }
    }

    /// An earlier stamp than its predecessor is clamped to that predecessor so every recursion still
    /// sees `dt >= 0`, and is counted in [`HawkesEvents::out_of_order_count`].
    pub fn push(&mut self, ts: TsUs) {
        let origin = *self.origin.get_or_insert(ts);
        let secs = ts.diff(origin).to_secs();
        if secs < self.last_secs {
            self.out_of_order += 1;
        } else {
            self.last_secs = secs;
        }
        self.times.push(self.last_secs);
    }

    /// Wipes the window and its time origin; the lifetime out-of-order count survives.
    pub fn clear(&mut self) {
        self.times.clear();
        self.origin = None;
        self.last_secs = 0.0;
    }

    pub fn len(&self) -> usize {
        self.times.len()
    }

    pub fn is_empty(&self) -> bool {
        self.times.is_empty()
    }

    /// Events whose stamp preceded their predecessor and were clamped, over the whole lifetime.
    pub fn out_of_order_count(&self) -> u64 {
        self.out_of_order
    }

    /// Oldest-first relative seconds — always one contiguous slice.
    pub fn times_secs(&self) -> &[f64] {
        self.times.as_slice()
    }

    /// `now` as relative seconds, clamped to at least the last event; `None` before any event.
    pub(super) fn end_secs(&self, now: TsUs) -> Option<f64> {
        let last = self.times.last()?;
        let origin = self.origin?;
        Some(now.diff(origin).to_secs().max(last))
    }

    /// The absolute stamp of the newest retained event, reconstructed from the origin — exact,
    /// because the stored seconds came from an integral µs difference.
    pub(super) fn last_ts(&self) -> Option<TsUs> {
        let last = self.times.last()?;
        let origin = self.origin?;
        Some(origin + DurationUs::from_micros((last * 1e6).round() as i64))
    }
}
