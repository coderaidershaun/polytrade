//! What leaves the hot thread: the per-spin rollup and the counters folded into it. Separate from
//! the accumulators so the report's shape — the one thing every downstream reader parses — reads
//! without the recording machinery around it.

use crate::msg::latency::EngineCounters;
use crate::msg::time::TsUs;

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
