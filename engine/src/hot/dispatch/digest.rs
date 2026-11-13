//! The digest of the whole fold: which fields of the engine are its state, and why each field that
//! is not state is left out.

use crate::hot::digest::{Digester, digest_fields};

use super::{HotEngine, HotState};

/// The fold state after some message, as one number. Two folds of the same messages from the same
/// start give the same digest after each message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StateDigest(u64);

impl HotEngine {
    /// A fixed hash of each field of the fold state, floats by their bits. The fold never calls it.
    /// It leaves out the strategy, whose state shows in the rows that it writes, and each count
    /// that depends on how fast a reader drains a ring.
    pub fn state_digest(&self) -> StateDigest {
        StateDigest(Digester::of(self))
    }
}

digest_fields! {
    HotEngine { state, run, unretained_volume_bars, feature_names, ui }
    not_state {
        // Its own state shows in the rows that it writes, and the replay tests compare those.
        strategy,
        // Output rings: their drop counts depend on how fast their readers drain them.
        strategy_log_sink,
        link_sink,
        // A copy of `run` for the link actor to read.
        run_report,
    }
}

digest_fields! {
    HotState {
        books, derived, windows, ledger, published_exposure, spin_seq, fold_time, gates,
        exec, desired, actions, declared_link_topics,
    }
    not_state {
        // Its pending snapshot and its count of superseded ones depend on how fast the writer
        // reads.
        exposure,
        // Its drop count and its pending seal depend on how fast the writer reads.
        sink,
    }
}
