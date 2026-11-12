//! Engine self-timing for display. The latency report carries percentiles of each stage from the
//! metrics thread, never a mean.

use crate::msg::latency::{Category, Stage};
use crate::msg::time::{DurationUs, TsUs};

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
