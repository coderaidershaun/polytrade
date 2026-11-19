//! Where the rows of each closed interval go: the tape of the run and the OTLP export, each when
//! the run has it, so the metrics thread never knows which outputs it feeds.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::tape::RollupRows;

use super::rows::{RollupRow, RollupSink};

#[derive(Default)]
pub struct RollupOutputs {
    /// `None` with no `persistence:` block.
    pub tape: Option<RollupRows>,
    /// `None` with no `otlp:` block.
    #[cfg(feature = "otlp")]
    pub otlp: Option<crate::runtime::otlp::OtlpRows>,
}

impl RollupOutputs {
    /// The count of rows that the full tape ring dropped, which the metrics thread reports beside
    /// the drops of each other ring.
    pub(super) fn tape_drops(&self) -> Option<Arc<AtomicU64>> {
        self.tape.as_ref().map(RollupRows::drops_handle)
    }
}

impl RollupSink for RollupOutputs {
    fn write(&mut self, row: RollupRow) {
        if let Some(tape) = self.tape.as_mut() {
            tape.write(row);
        }
        #[cfg(feature = "otlp")]
        if let Some(otlp) = self.otlp.as_mut() {
            otlp.write(row);
        }
    }
}
