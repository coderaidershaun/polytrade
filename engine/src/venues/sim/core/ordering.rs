//! Effective-time ordering for the buffered inputs and the emissions of the simulator.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::msg::time::LocalTs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Phase {
    DepthReset,
    SnapshotRebuild,
    DeltaCommit,
    Trade,
    OrderAction,
    Reconcile,
    /// Every emission takes this one phase, so emissions at one time leave in the order of their
    /// push.
    Emission,
}

pub(super) type EventKey = (LocalTs, Phase, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct Sequenced<T> {
    pub at_ts_us: LocalTs,
    phase: Phase,
    sequence: u64,
    pub item: T,
}

impl<T> Sequenced<T> {
    fn key(&self) -> EventKey {
        (self.at_ts_us, self.phase, self.sequence)
    }
}

/// An entry compared by its whole key alone. The key holds the sequence, so no two entries tie,
/// and the heap pops them in the one order of their keys.
#[derive(Debug, Clone)]
struct ByKey<T>(Sequenced<T>);

impl<T> PartialEq for ByKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.key() == other.0.key()
    }
}

impl<T> Eq for ByKey<T> {}

impl<T> PartialOrd for ByKey<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for ByKey<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.key().cmp(&other.0.key())
    }
}

/// Every buffered simulator input is ordered by the same key, so the rule lives here once: two
/// copies of it could drift apart and only one of them would fail a test.
#[derive(Debug, Clone)]
pub(super) struct SequencedByKey<T> {
    entries: BinaryHeap<Reverse<ByKey<T>>>,
    sequence: u64,
}

impl<T> SequencedByKey<T> {
    /// Reserves room for `capacity` entries at start, so a queue that stays within its bound never
    /// allocates again.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: BinaryHeap::with_capacity(capacity),
            sequence: 0,
        }
    }

    pub fn push(&mut self, at_ts_us: LocalTs, phase: Phase, item: T) {
        let entry = Sequenced {
            at_ts_us,
            phase,
            sequence: self.sequence,
            item,
        };
        self.sequence += 1;
        self.entries.push(Reverse(ByKey(entry)));
    }

    pub fn peek(&self) -> Option<EventKey> {
        self.entries.peek().map(|Reverse(ByKey(entry))| entry.key())
    }

    pub fn pop(&mut self) -> Option<Sequenced<T>> {
        self.entries.pop().map(|Reverse(ByKey(entry))| entry)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

impl<T> Default for SequencedByKey<T> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}
