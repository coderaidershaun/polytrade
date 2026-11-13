//! Lanes where a full ring costs a counted gap, never a stall: their consumers want only the
//! freshest state, or, for latency samples, the count of what they missed.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rtrb::Producer;

use crate::link::OutboundLink;
use crate::log::HotRecord;
use crate::msg::failure::FailureClass;
use crate::msg::ui::{UiBookSnapshot, UiEvent};

pub struct DropCountingSink<T> {
    producer: Producer<T>,
    dropped: u64,
}

impl<T> DropCountingSink<T> {
    /// A full ring drops the message and counts it.
    pub const WHEN_FULL: FailureClass = FailureClass::Reject;

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
    producer: Producer<HotRecord>,
    drops: Arc<AtomicU64>,
}

impl StrategyLogSink {
    /// A full ring drops the record and counts it.
    pub const WHEN_FULL: FailureClass = FailureClass::Reject;

    pub fn new(producer: Producer<HotRecord>) -> Self {
        Self {
            producer,
            drops: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn drops_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.drops)
    }

    pub(crate) fn push(&mut self, record: HotRecord) {
        if self.producer.push(record).is_err() {
            self.count_drop();
        }
    }

    /// The hot thread is the one writer of this counter, so a plain store does what an atomic add
    /// would, with no locked instruction.
    #[cold]
    #[inline(never)]
    fn count_drop(&self) {
        let dropped = self.drops.load(Ordering::Relaxed);
        self.drops.store(dropped + 1, Ordering::Relaxed);
    }
}

/// The write end of one ring of latency samples. The metrics thread reports the drops beside the
/// samples it did get, so the counter is shared.
pub struct SampleSink<T> {
    producer: Producer<T>,
    dropped: Arc<AtomicU64>,
}

impl<T> SampleSink<T> {
    /// A full ring drops the sample and counts it.
    pub const WHEN_FULL: FailureClass = FailureClass::Reject;

    pub(crate) fn new(producer: Producer<T>) -> Self {
        Self {
            producer,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn drops_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.dropped)
    }

    /// Never waits: a full ring drops `sample` and counts it.
    #[inline]
    pub fn push(&mut self, sample: T) {
        if self.producer.push(sample).is_err() {
            self.count_drop();
        }
    }

    /// The producer is the counter's only writer, so a load and a store add one exactly, and the
    /// metrics thread reads the total with no lock.
    #[cold]
    fn count_drop(&self) {
        let dropped = self.dropped.load(Ordering::Relaxed);
        self.dropped.store(dropped + 1, Ordering::Relaxed);
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
