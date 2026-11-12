//! Lanes whose consumer wants only the freshest state, so a full ring costs a gap, never a stall.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rtrb::Producer;

use crate::hot::metrics::MetricsSnapshot;
use crate::link::OutboundLink;
use crate::log::LogRecord;
use crate::msg::ui::{UiBookSnapshot, UiEvent};

pub struct DropCountingSink<T> {
    producer: Producer<T>,
    dropped: u64,
}

impl<T> DropCountingSink<T> {
    pub fn new(producer: Producer<T>) -> Self {
        Self {
            producer,
            dropped: 0,
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, message: T) {
        if self.producer.push(message).is_err() {
            self.count_drop();
        }
    }

    /// Reached only once a consumer has fallen a whole ring behind — off the steady-state path.
    #[cold]
    fn count_drop(&mut self) {
        self.dropped += 1;
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

pub type UiBookSink = DropCountingSink<UiBookSnapshot>;

pub type UiEventSink = DropCountingSink<UiEvent>;

pub type LinkSink = DropCountingSink<OutboundLink>;

pub type MetricsSink = DropCountingSink<MetricsSnapshot>;

impl DropCountingSink<UiEvent> {
    /// The sequence advances even when the ring is full, so a dropped event leaves a visible gap
    /// in the numbering rather than silently renumbering the events that follow it.
    #[inline]
    pub(crate) fn push_stamped(&mut self, seq: &mut u64, build: impl FnOnce(u64) -> UiEvent) {
        let current = *seq;
        *seq += 1;
        self.push(build(current));
    }
}

/// A dedicated ring so that a strategy telemetry flood fills its own lane instead of displacing
/// engine warnings and errors. The counter is shared rather than plain because the log drain must
/// hold a live handle to it, and that atomic is a cost the other sinks in this file decline to pay.
pub struct StrategyLogSink {
    producer: Producer<LogRecord>,
    drops: Arc<AtomicU64>,
}

impl StrategyLogSink {
    pub fn new(producer: Producer<LogRecord>) -> Self {
        Self {
            producer,
            drops: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn drops_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.drops)
    }

    pub(crate) fn push(&mut self, record: LogRecord) {
        if self.producer.push(record).is_err() {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }
}
