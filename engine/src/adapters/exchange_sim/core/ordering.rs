//! Effective-time ordering for buffered simulator inputs.

use crate::time::TsUs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Phase {
    DepthReset,
    SnapshotRebuild,
    DeltaCommit,
    Trade,
    OrderAction,
    Reconcile,
}

pub(super) type EventKey = (TsUs, Phase, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct Sequenced<T> {
    pub at_ts_us: TsUs,
    phase: Phase,
    sequence: u64,
    pub item: T,
}

impl<T> Sequenced<T> {
    fn key(&self) -> EventKey {
        (self.at_ts_us, self.phase, self.sequence)
    }
}

/// Every buffered simulator input is ordered by the same key, so the rule lives here once: two
/// copies of it could drift apart and only one of them would fail a test.
#[derive(Debug, Clone)]
pub(super) struct SequencedByKey<T> {
    entries: Vec<Sequenced<T>>,
    sequence: u64,
}

impl<T> SequencedByKey<T> {
    pub fn push(&mut self, at_ts_us: TsUs, phase: Phase, item: T) {
        let entry = Sequenced {
            at_ts_us,
            phase,
            sequence: self.sequence,
            item,
        };
        self.sequence += 1;
        let at = self
            .entries
            .partition_point(|held| held.key() <= entry.key());
        self.entries.insert(at, entry);
    }

    pub fn peek(&self) -> Option<EventKey> {
        self.entries.first().map(Sequenced::key)
    }

    pub fn pop(&mut self) -> Option<Sequenced<T>> {
        (!self.entries.is_empty()).then(|| self.entries.remove(0))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

impl<T> Default for SequencedByKey<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            sequence: 0,
        }
    }
}
