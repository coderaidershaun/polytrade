//! The rows of each closed interval, and the interface that takes them. The table that keeps them
//! is written elsewhere; the metrics thread only hands rows over.

use crate::msg::ids::QueueId;
use crate::msg::labelled_enum::labelled_enum;
use crate::msg::latency::{Category, Stage};
use crate::msg::time::{ClockSource, TsUs};
use crate::msg::ui::LatencyCell;

use super::ring::SampleRing;

labelled_enum! {
    /// Each ring and lane whose drops a run reports.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum SinkKind {
        StageSamples = "stage_samples",
        CounterSamples = "counter_samples",
        WireSamples = "wire_samples",
        LatencyReports = "latency_reports",
        Persist = "persist",
        UiBooks = "ui_books",
        UiEvents = "ui_events",
        Link = "link",
        BankFeatures = "bank_features",
        BankPersist = "bank_persist",
        BankLogs = "bank_logs",
        BankLink = "bank_link",
        ExposureSuperseded = "exposure_superseded",
    }
    pub fn label;
    pub const ALL;
}

impl SinkKind {
    pub(super) const fn of_ring(ring: SampleRing) -> SinkKind {
        match ring {
            SampleRing::Stages => SinkKind::StageSamples,
            SampleRing::Counters => SinkKind::CounterSamples,
            SampleRing::Wires => SinkKind::WireSamples,
        }
    }
}

/// The numbers of one stage of one category over one interval. Percentiles hold within one
/// percent; the maximum is exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LatencyRow {
    pub interval_start_ts_us: TsUs,
    pub interval_end_ts_us: TsUs,
    pub category: Category,
    pub stage: Stage,
    pub count: u64,
    /// Samples lost on the ring that feeds this stage during the interval. Above zero, the tail
    /// reads low.
    pub dropped: u64,
    pub backward: u64,
    pub negative: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
    /// `None` when the stage has no budget for that percentile.
    pub budget_p99_ns: Option<u64>,
    pub budget_p999_ns: Option<u64>,
    /// Whether the 99th or the 99.9th percentile of the window that ends with this interval read
    /// above the budget. It only reports: nothing that trades reads it.
    pub is_over_budget: bool,
    pub clock_source: ClockSource,
}

impl LatencyRow {
    pub fn cell(&self) -> LatencyCell {
        LatencyCell {
            count: self.count,
            p50_ns: self.p50_ns,
            p99_ns: self.p99_ns,
            p999_ns: self.p999_ns,
            max_ns: self.max_ns,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DropRow {
    pub interval_end_ts_us: TsUs,
    pub sink: SinkKind,
    /// Since the start of the run.
    pub dropped_total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BacklogRow {
    pub interval_end_ts_us: TsUs,
    pub queue: QueueId,
    pub max: u32,
    /// One-minute half-life average over spins, in message time.
    pub ema: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RollupRow {
    Latency(LatencyRow),
    Drop(DropRow),
    Backlog(BacklogRow),
}

/// Where the rows of each closed interval go.
pub trait RollupSink {
    fn write(&mut self, row: RollupRow);
}
