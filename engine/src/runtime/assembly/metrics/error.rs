//! What the metrics thread refuses. Each is counted and the run goes on: a fault of measurement
//! never changes what the engine trades.

use crate::msg::latency::{Category, Stage};

use super::ring::SampleRing;

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricsError {
    #[error(
        "the {} ring dropped {dropped} samples in one interval, so the tail of each stage that it feeds reads low - enlarge latency.sample_ring",
        ring.label()
    )]
    SampleRingFull { ring: SampleRing, dropped: u64 },
    #[error(
        "{count} {} {} pairs ended before they started and were skipped - a thread moved core, or the counter is not invariant",
        category.label(),
        stage.label()
    )]
    Backward {
        category: Category,
        stage: Stage,
        count: u64,
    },
    /// The venue stamped an event after our own receive of it, so its clock runs ahead of ours.
    #[error(
        "{count} {} {} samples came out negative and were skipped - the venue's clock runs ahead of this host's",
        category.label(),
        stage.label()
    )]
    NegativeVenueStage {
        category: Category,
        stage: Stage,
        count: u64,
    },
    #[error(
        "the two counter reads of an anchor were {spread_ns} ns apart, more than {limit_ns} ns - kept the last good anchor"
    )]
    AnchorSpread { spread_ns: u64, limit_ns: u64 },
}
