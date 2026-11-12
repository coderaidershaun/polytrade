//! Engine self-timing for display. The latency report carries percentiles of each stage from the
//! metrics thread, never a mean. The older summary of the hot thread is still sent until that
//! thread stops timing itself.

use crate::hot::metrics::{Category as HotCategory, MetricsSnapshot, Stage as HotStage};
use crate::msg::latency::{Category, Stage};
use crate::msg::time::{DurationUs, TsUs};

/// Count and sum, never a pre-divided mean: folding two categories together has to add before it
/// divides, and a mean cannot be added.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct UiLatencyCell {
    pub count: u64,
    pub sum_us: i64,
}

impl UiLatencyCell {
    /// `None` when nothing was measured — a stage with no samples is not a stage that ran in zero.
    pub fn mean_us(self) -> Option<f64> {
        (self.count > 0).then(|| self.sum_us as f64 / self.count as f64)
    }
}

/// One display row: the stages a reader compares side by side. `ExchangeInternal` is deliberately
/// absent — it stays a log-only diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct UiLatencyRow {
    pub exchange_to_received: UiLatencyCell,
    pub received_to_queued: UiLatencyCell,
    pub queue_wait: UiLatencyCell,
    pub processing: UiLatencyCell,
    pub end_to_end: UiLatencyCell,
    pub order_round_trip: UiLatencyCell,
}

impl UiLatencyRow {
    fn fold(snapshot: &MetricsSnapshot, categories: &[HotCategory]) -> Self {
        Self {
            exchange_to_received: stage_sum(snapshot, categories, HotStage::ExchangeToReceived),
            received_to_queued: stage_sum(snapshot, categories, HotStage::ReceivedToQueued),
            queue_wait: stage_sum(snapshot, categories, HotStage::QueueWait),
            processing: stage_sum(snapshot, categories, HotStage::Processing),
            end_to_end: stage_sum(snapshot, categories, HotStage::EndToEnd),
            order_round_trip: stage_sum(snapshot, categories, HotStage::OrderRoundTrip),
        }
    }

    /// The engine's own two stages across every category. The venue-facing four stay empty: summing
    /// exch->recv over categories the engine never received from venues answers no question.
    fn fold_engine_stages(snapshot: &MetricsSnapshot) -> Self {
        Self {
            queue_wait: stage_sum(snapshot, &HotCategory::ALL, HotStage::QueueWait),
            processing: stage_sum(snapshot, &HotCategory::ALL, HotStage::Processing),
            ..Self::default()
        }
    }
}

fn stage_sum(
    snapshot: &MetricsSnapshot,
    categories: &[HotCategory],
    stage: HotStage,
) -> UiLatencyCell {
    let mut cell = UiLatencyCell::default();
    for &category in categories {
        let stat = snapshot.stage(category, stage);
        cell.count += stat.count;
        cell.sum_us = cell.sum_us.saturating_add(stat.sum_us);
    }
    cell
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct UiLatencySummary {
    pub market_data: UiLatencyRow,
    pub execution: UiLatencyRow,
    pub hot_path: UiLatencyRow,
    /// One-minute half-life EWMA of the messages waiting across every input ring — a count, not a
    /// rate. `None` only before the first spin has been sampled; a keeping-up engine reads 0.0.
    pub backlog_ema: Option<f64>,
}

const MARKET_DATA: [HotCategory; 4] = [
    HotCategory::Trade,
    HotCategory::BookDelta,
    HotCategory::BookSnapshot,
    HotCategory::Kline,
];

impl UiLatencySummary {
    pub fn from_snapshot(snapshot: &MetricsSnapshot) -> Self {
        Self {
            market_data: UiLatencyRow::fold(snapshot, &MARKET_DATA),
            execution: UiLatencyRow::fold(snapshot, &[HotCategory::Exec]),
            hot_path: UiLatencyRow::fold_engine_stages(snapshot),
            backlog_ema: snapshot.backlog_ema,
        }
    }
}

/// A percentile that a latency cell can report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Percentile {
    P50,
    P99,
    P999,
}

impl Percentile {
    pub const ALL: [Percentile; 3] = [Percentile::P50, Percentile::P99, Percentile::P999];

    pub const fn quantile(self) -> f64 {
        match self {
            Percentile::P50 => 0.5,
            Percentile::P99 => 0.99,
            Percentile::P999 => 0.999,
        }
    }

    /// The fewest samples that put a hundred of them beyond the percentile. With fewer, the value
    /// is set by a handful of samples and says nothing about the tail.
    pub const fn sample_floor(self) -> u64 {
        match self {
            Percentile::P50 => 200,
            Percentile::P99 => 10_000,
            Percentile::P999 => 100_000,
        }
    }
}

/// What a cell says about one statistic.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LatencyReading {
    /// The stage took no sample in the window.
    Absent,
    /// Too few samples for the statistic to mean anything.
    Thin {
        count: u64,
    },
    Micros(f64),
}

/// One stage over a window, in nanoseconds: three percentiles, the maximum and the sample count.
/// The percentiles hold within one percent; the maximum is exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LatencyCell {
    pub count: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

impl LatencyCell {
    pub const EMPTY: LatencyCell = LatencyCell {
        count: 0,
        p50_ns: 0,
        p99_ns: 0,
        p999_ns: 0,
        max_ns: 0,
    };

    pub fn reading(self, percentile: Percentile) -> LatencyReading {
        if self.count == 0 {
            return LatencyReading::Absent;
        }
        if self.count < percentile.sample_floor() {
            return LatencyReading::Thin { count: self.count };
        }
        let nanos = match percentile {
            Percentile::P50 => self.p50_ns,
            Percentile::P99 => self.p99_ns,
            Percentile::P999 => self.p999_ns,
        };
        LatencyReading::Micros(nanos as f64 / 1_000.0)
    }

    /// The maximum means something from the first sample on, so it is never thin.
    pub fn max_reading(self) -> LatencyReading {
        if self.count == 0 {
            return LatencyReading::Absent;
        }
        LatencyReading::Micros(self.max_ns as f64 / 1_000.0)
    }
}

/// One row of the latency panel: the categories it folds together and the stages it shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LatencyFlow {
    MarketData,
    /// Answers of the venue and the commands that went to it.
    Execution,
    /// The engine's own stages over every message that it folds.
    HotPath,
}

impl LatencyFlow {
    pub const ALL: [LatencyFlow; 3] = [
        LatencyFlow::MarketData,
        LatencyFlow::Execution,
        LatencyFlow::HotPath,
    ];

    pub const fn categories(self) -> &'static [Category] {
        match self {
            LatencyFlow::MarketData => &[
                Category::Trade,
                Category::BookDelta,
                Category::BookSnapshot,
                Category::Kline,
            ],
            LatencyFlow::Execution => &[Category::Exec, Category::Command],
            LatencyFlow::HotPath => &[
                Category::Trade,
                Category::BookDelta,
                Category::BookSnapshot,
                Category::Kline,
                Category::Spin,
                Category::Exec,
            ],
        }
    }

    pub const fn stages(self) -> &'static [Stage] {
        match self {
            LatencyFlow::MarketData => &[
                Stage::VenueToEngine,
                Stage::VenueInternal,
                Stage::WireToMessage,
                Stage::QueueWait,
                Stage::Processing,
                Stage::MessageToDecision,
            ],
            LatencyFlow::Execution => &[
                Stage::VenueToEngine,
                Stage::WireToMessage,
                Stage::QueueWait,
                Stage::Processing,
                Stage::MessageToDecision,
                Stage::OrderRoundTrip,
                Stage::DecisionToWire,
                Stage::BankAndLaneWait,
                Stage::Send,
            ],
            LatencyFlow::HotPath => &[
                Stage::QueueWait,
                Stage::Processing,
                Stage::MessageToDecision,
            ],
        }
    }

    const fn first_cell(self) -> usize {
        match self {
            LatencyFlow::MarketData => 0,
            LatencyFlow::Execution => LatencyFlow::MarketData.stages().len(),
            LatencyFlow::HotPath => {
                LatencyFlow::MarketData.stages().len() + LatencyFlow::Execution.stages().len()
            }
        }
    }
}

/// One cell for each stage of each flow, flow by flow in panel order.
pub const LATENCY_REPORT_CELLS: usize =
    LatencyFlow::HotPath.first_cell() + LatencyFlow::HotPath.stages().len();

/// What the metrics thread sends the workstation about once a second: each stage of each flow over
/// the window, the backlog of the input queues, and enough time stamps to tell a stale reading.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LatencyReport {
    /// Engine time when the metrics thread built the report. A reader that sees newer engine time
    /// on the feed knows how old the report is, even when the metrics thread has stopped.
    pub built_ts_us: TsUs,
    /// Time since each flow took its last sample, in [`LatencyFlow::ALL`] order. `None` until the
    /// flow takes its first.
    pub ages: [Option<DurationUs>; LatencyFlow::ALL.len()],
    /// Read them through [`LatencyReport::cell`].
    pub cells: [LatencyCell; LATENCY_REPORT_CELLS],
    /// One-minute half-life average of the messages that wait across every input queue, folded on
    /// message time. `None` until the first spin is counted; an engine that keeps up reads 0.0.
    pub backlog_ema: Option<f64>,
}

impl LatencyReport {
    /// `None` when the flow does not show the stage.
    pub fn cell(&self, flow: LatencyFlow, stage: Stage) -> Option<LatencyCell> {
        let position = flow.stages().iter().position(|shown| *shown == stage)?;
        Some(self.cells[flow.first_cell() + position])
    }

    pub(crate) fn cell_mut(&mut self, flow: LatencyFlow, stage: Stage) -> Option<&mut LatencyCell> {
        let position = flow.stages().iter().position(|shown| *shown == stage)?;
        Some(&mut self.cells[flow.first_cell() + position])
    }

    pub fn age(&self, flow: LatencyFlow) -> Option<DurationUs> {
        self.ages[flow as usize]
    }
}
