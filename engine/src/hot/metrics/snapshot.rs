//! What leaves the hot thread: the per-spin rollup and the counters folded into it. Separate from
//! the accumulators so the report's shape — the one thing every downstream reader parses — reads
//! without the recording machinery around it.

use crate::hot::strategy::LaneDrops;
use crate::time::TsUs;

use super::{CATEGORIES, Category, MAX_QUEUES, QueueDepthStat, STAGES, Stage, StageStat};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetricsSnapshot {
    pub taken_at: TsUs,
    pub stages: [[StageStat; STAGES]; CATEGORIES],
    pub occupancy: [QueueDepthStat; MAX_QUEUES],
    /// One-minute half-life EWMA of the messages waiting across every input ring, folded once per
    /// spin on the tick's own event time. A keeping-up engine reads 0.0; `None` until the first spin,
    /// which is absence of a reading rather than an idle engine.
    pub backlog_ema: Option<f64>,
    pub queue_count: u8,
    pub counters: EngineCounters,
}

impl MetricsSnapshot {
    pub fn stage(&self, category: Category, stage: Stage) -> StageStat {
        self.stages[category as usize][stage as usize]
    }

    pub fn is_active(&self, category: Category) -> bool {
        self.stages[category as usize]
            .iter()
            .any(|stat| stat.count > 0)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineCounters {
    pub persist_dropped: u64,
    pub book_trimmed: u64,
    pub book_remove_missing: u64,
    pub klines_unconfigured: u64,
    pub intensity_inside_spread: u64,
    pub intensity_without_book: u64,
    pub snapshots_dropped: u64,
    pub orders_submitted: u64,
    pub bank_drops: LaneDrops,
    pub ui_books_dropped: u64,
    pub ui_events_dropped: u64,
    pub link_dropped: u64,
    /// Position snapshots replaced before the exposure writer took them. Nonzero means the file on
    /// disk trails the hot side, so a hard kill would restore a stale position.
    pub exposure_superseded: u64,
}
