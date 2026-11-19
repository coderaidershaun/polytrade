//! When the fold seals the tape: each time a spin crosses a multiple of the seal interval, so the
//! file borders of a run depend on message time alone and a replay seals at the same spins.

use std::num::NonZeroU64;

use crate::hot::book::Book;
use crate::hot::digest::{Digest, Digester};
use crate::hot::sink::{AuditSink, ResearchSink};

use super::{HotEngine, HotState};

/// The count of the seal interval that the latest seal closed. A late timer skips slots, so the
/// seal goes at the first spin past each multiple, not only at the multiple itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SealBorders {
    interval_spins: NonZeroU64,
    sealed_intervals: u64,
}

impl SealBorders {
    pub(super) fn every(interval_spins: NonZeroU64) -> Self {
        Self {
            interval_spins,
            sealed_intervals: 0,
        }
    }

    /// True once for each multiple of the interval that `spin_seq` reaches.
    #[inline]
    pub(super) fn is_crossed_by(&mut self, spin_seq: u64) -> bool {
        let intervals = spin_seq / self.interval_spins.get();
        if intervals <= self.sealed_intervals {
            return false;
        }
        self.sealed_intervals = intervals;
        true
    }

    /// What [`Self::is_crossed_by`] would say, without taking the border.
    #[inline]
    fn would_be_crossed_by(&self, spin_seq: u64) -> bool {
        spin_seq / self.interval_spins.get() > self.sealed_intervals
    }
}

impl HotEngine {
    /// Whether the fold seals its files at this spin. The tape seals at the same point of the
    /// input, so a file border of each table is one point of a replay too.
    #[inline]
    pub(crate) fn seals_at(&self, spin_seq: u64) -> bool {
        self.state
            .seal_borders
            .is_some_and(|borders| borders.would_be_crossed_by(spin_seq))
    }

    /// The books as the fold holds them, which the tape snapshots at each file border.
    #[inline]
    pub(crate) fn books(&self) -> &[Book] {
        &self.state.books
    }
}

impl Digest for SealBorders {
    fn digest(&self, digester: &mut Digester) {
        let Self {
            interval_spins,
            sealed_intervals,
        } = self;
        interval_spins.get().digest(digester);
        sealed_intervals.digest(digester);
    }
}

impl HotState {
    /// Each message drains what it banked, so each row before this spin is already in its ring or
    /// the audit bank, and the seal closes the files ahead of the rows of this spin.
    pub(super) fn seal_at_border(&mut self, spin_seq: u64) {
        let Some(borders) = self.seal_borders.as_mut() else {
            return;
        };
        if borders.is_crossed_by(spin_seq) {
            self.request_seal();
        }
    }

    /// Seals both rings: research and audit files close at the same point of the input.
    pub(super) fn request_seal(&mut self) {
        if let Some(sink) = self.sink.as_mut() {
            sink.request_seal();
        }
        if let Some(audit) = self.actions.audit_mut() {
            audit.request_seal();
        }
    }

    /// The count of tries, at the one spin where a seal of either ring is first past its cap.
    pub(super) fn retry_pending_seal(&mut self) -> Option<u32> {
        let research = self
            .sink
            .as_mut()
            .and_then(ResearchSink::retry_pending_seal);
        let audit = self
            .actions
            .audit_mut()
            .and_then(AuditSink::retry_pending_seal);
        research.or(audit)
    }
}
