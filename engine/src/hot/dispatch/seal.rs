//! When the fold seals the tape: each time a spin crosses a multiple of the seal interval, so the
//! file borders of a run depend on message time alone and a replay seals at the same spins.

use std::num::NonZeroU64;

use crate::hot::digest::{Digest, Digester};

use super::HotState;

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
    /// Each message drains what it banked, so each row before this spin is already in the ring and
    /// the seal closes the files ahead of the rows of this spin.
    pub(super) fn seal_at_border(&mut self, spin_seq: u64) {
        let (Some(sink), Some(borders)) = (self.sink.as_mut(), self.seal_borders.as_mut()) else {
            return;
        };
        if borders.is_crossed_by(spin_seq) {
            sink.request_seal();
        }
    }
}
