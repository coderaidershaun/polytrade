//! The latency grid's words and roles: the header, one row per flow, and each cell's reading.

use crate::msg::ui::{UiLatencyRow, UiLatencySummary};
use crate::workstation::chart::pane::LATENCY_ROW_COUNT;
use crate::workstation::format;

use super::tone::Tone;

pub(crate) const LATENCY_TITLE: &str = "LATENCY  us";

pub(crate) const LATENCY_SHORT_TITLE: &str = "us";

/// One column of the grid. Each column owns both its header and its reading, so a reading can never
/// land under another column's header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Column {
    ExchangeToReceived,
    ReceivedToQueued,
    QueueWait,
    Processing,
    EndToEnd,
    RoundTrip,
    Slots,
}

impl Column {
    /// Paint order. The last column holds the slot count rather than a latency.
    pub(crate) const ALL: [Self; 7] = [
        Self::ExchangeToReceived,
        Self::ReceivedToQueued,
        Self::QueueWait,
        Self::Processing,
        Self::EndToEnd,
        Self::RoundTrip,
        Self::Slots,
    ];

    /// Which columns to keep as the width shrinks, first to last: the readings an operator needs
    /// before the stages that explain them.
    const SURVIVAL_ORDER: [Self; Self::ALL.len()] = [
        Self::EndToEnd,
        Self::RoundTrip,
        Self::Slots,
        Self::Processing,
        Self::QueueWait,
        Self::ExchangeToReceived,
        Self::ReceivedToQueued,
    ];

    /// The columns that survive when only `count` fit, in paint order.
    pub(crate) fn shown(count: usize) -> impl Iterator<Item = Self> {
        Self::ALL.into_iter().filter(move |column| {
            Self::SURVIVAL_ORDER
                .iter()
                .take(count)
                .any(|kept| kept == column)
        })
    }

    pub(crate) fn header(self) -> &'static str {
        match self {
            Self::ExchangeToReceived => "EXCH>RECV",
            Self::ReceivedToQueued => "RECV>QUEUE",
            Self::QueueWait => "QUEUE WAIT",
            Self::Processing => "PROCESS",
            Self::EndToEnd => "END-TO-END",
            Self::RoundTrip => "RND TRIP",
            Self::Slots => "SLOTS",
        }
    }

    pub(crate) fn cell(self, flow: Flow, summary: Option<&UiLatencySummary>) -> (String, Tone) {
        let value = summary.and_then(|summary| self.reading(flow, summary));
        let mut text = String::new();
        match self {
            Self::Slots => format::write_opt_slots(&mut text, value),
            _ => format::write_opt_latency_micros(&mut text, value),
        };
        (text, value_tone(value))
    }

    fn reading(self, flow: Flow, summary: &UiLatencySummary) -> Option<f64> {
        let row = flow.row(summary);
        match self {
            Self::ExchangeToReceived => row.exchange_to_received.mean_us(),
            Self::ReceivedToQueued => row.received_to_queued.mean_us(),
            Self::QueueWait => row.queue_wait.mean_us(),
            Self::Processing => row.processing.mean_us(),
            Self::EndToEnd => row.end_to_end.mean_us(),
            Self::RoundTrip => row.order_round_trip.mean_us(),
            Self::Slots => flow.slots(summary),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LatencyRow {
    pub label: &'static str,
    pub short_label: &'static str,
    pub flow: Flow,
}

pub(crate) const LATENCY_ROWS: [LatencyRow; LATENCY_ROW_COUNT] = [
    LatencyRow {
        label: "MARKET DATA",
        short_label: "MKT",
        flow: Flow::MarketData,
    },
    LatencyRow {
        label: "ORDER TRIP",
        short_label: "ORD",
        flow: Flow::Execution,
    },
    LatencyRow {
        label: "HOT PATH",
        short_label: "HOT",
        flow: Flow::HotPath,
    },
];

#[derive(Debug, Clone, Copy)]
pub(crate) enum Flow {
    MarketData,
    Execution,
    HotPath,
}

impl Flow {
    fn row(self, summary: &UiLatencySummary) -> &UiLatencyRow {
        match self {
            Flow::MarketData => &summary.market_data,
            Flow::Execution => &summary.execution,
            Flow::HotPath => &summary.hot_path,
        }
    }

    /// Backlog is one figure across every input queue, so it reads on the hot path's row alone.
    fn slots(self, summary: &UiLatencySummary) -> Option<f64> {
        match self {
            Flow::HotPath => summary.backlog_ema,
            _ => None,
        }
    }
}

fn value_tone(value: Option<f64>) -> Tone {
    match value {
        Some(_) => Tone::Text,
        None => Tone::Text2,
    }
}

/// The three readings a one-line summary keeps, each with its label: market data end to end, the
/// order round trip and the hot path's slot backlog.
pub(crate) fn summary_cells(
    summary: Option<&UiLatencySummary>,
) -> [(&'static str, String, Tone); 3] {
    [
        ("E2E", Flow::MarketData, Column::EndToEnd),
        ("RTT", Flow::Execution, Column::RoundTrip),
        ("SLOTS", Flow::HotPath, Column::Slots),
    ]
    .map(|(label, flow, column)| {
        let (text, tone) = column.cell(flow, summary);
        (label, text, tone)
    })
}
