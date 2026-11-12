//! Engine self-timing distilled for display: the ten-minute stage rollups and the input-ring
//! backlog, folded to count+sum so a panel can render a mean without carrying every percentile.

use crate::hot::metrics::{Category, MetricsSnapshot, Stage};

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
    fn fold(snapshot: &MetricsSnapshot, categories: &[Category]) -> Self {
        Self {
            exchange_to_received: stage_sum(snapshot, categories, Stage::ExchangeToReceived),
            received_to_queued: stage_sum(snapshot, categories, Stage::ReceivedToQueued),
            queue_wait: stage_sum(snapshot, categories, Stage::QueueWait),
            processing: stage_sum(snapshot, categories, Stage::Processing),
            end_to_end: stage_sum(snapshot, categories, Stage::EndToEnd),
            order_round_trip: stage_sum(snapshot, categories, Stage::OrderRoundTrip),
        }
    }

    /// The engine's own two stages across every category. The venue-facing four stay empty: summing
    /// exch->recv over categories the engine never received from venues answers no question.
    fn fold_engine_stages(snapshot: &MetricsSnapshot) -> Self {
        Self {
            queue_wait: stage_sum(snapshot, &Category::ALL, Stage::QueueWait),
            processing: stage_sum(snapshot, &Category::ALL, Stage::Processing),
            ..Self::default()
        }
    }
}

fn stage_sum(snapshot: &MetricsSnapshot, categories: &[Category], stage: Stage) -> UiLatencyCell {
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

const MARKET_DATA: [Category; 4] = [
    Category::Trade,
    Category::BookDelta,
    Category::BookSnapshot,
    Category::Kline,
];

impl UiLatencySummary {
    pub fn from_snapshot(snapshot: &MetricsSnapshot) -> Self {
        Self {
            market_data: UiLatencyRow::fold(snapshot, &MARKET_DATA),
            execution: UiLatencyRow::fold(snapshot, &[Category::Exec]),
            hot_path: UiLatencyRow::fold_engine_stages(snapshot),
            backlog_ema: snapshot.backlog_ema,
        }
    }
}
