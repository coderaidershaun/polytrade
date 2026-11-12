//! The latency grid's words and roles: the header, one row per flow, and each cell's reading. A
//! cell shows the 99th percentile of its stage, or says `thin` while too few samples back it.

use crate::msg::latency::Stage;
use crate::msg::time::DurationUs;
use crate::msg::ui::{LatencyFlow, LatencyReading, LatencyReport, Percentile};
use crate::operator::workstation::chart::pane::LATENCY_ROW_COUNT;
use crate::operator::workstation::format;

use super::tone::Tone;

pub(crate) const LATENCY_TITLE: &str = "LATENCY p99  us";

pub(crate) const LATENCY_SHORT_TITLE: &str = "p99";

/// The percentile that each cell of the grid shows.
const SHOWN: Percentile = Percentile::P99;

const THIN: &str = "thin";

/// One column of the grid. Each column owns both its header and its reading, so a reading can never
/// land under another column's header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Column {
    VenueToEngine,
    WireToMessage,
    QueueWait,
    Processing,
    MessageToDecision,
    RoundTrip,
    DecisionToWire,
    Slots,
}

impl Column {
    /// Paint order. The last column holds the slot count rather than a latency.
    pub(crate) const ALL: [Self; 8] = [
        Self::VenueToEngine,
        Self::WireToMessage,
        Self::QueueWait,
        Self::Processing,
        Self::MessageToDecision,
        Self::RoundTrip,
        Self::DecisionToWire,
        Self::Slots,
    ];

    /// Which columns to keep as the width shrinks, first to last: the readings an operator needs
    /// before the stages that explain them.
    const SURVIVAL_ORDER: [Self; Self::ALL.len()] = [
        Self::MessageToDecision,
        Self::RoundTrip,
        Self::DecisionToWire,
        Self::Slots,
        Self::Processing,
        Self::QueueWait,
        Self::VenueToEngine,
        Self::WireToMessage,
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
            Self::VenueToEngine => "EXCH>RECV",
            Self::WireToMessage => "RECV>QUEUE",
            Self::QueueWait => "QUEUE WAIT",
            Self::Processing => "PROCESS",
            Self::MessageToDecision => "MSG>DECIDE",
            Self::RoundTrip => "RND TRIP",
            Self::DecisionToWire => "DECIDE>WIRE",
            Self::Slots => "SLOTS",
        }
    }

    fn stage(self) -> Option<Stage> {
        match self {
            Self::VenueToEngine => Some(Stage::VenueToEngine),
            Self::WireToMessage => Some(Stage::WireToMessage),
            Self::QueueWait => Some(Stage::QueueWait),
            Self::Processing => Some(Stage::Processing),
            Self::MessageToDecision => Some(Stage::MessageToDecision),
            Self::RoundTrip => Some(Stage::OrderRoundTrip),
            Self::DecisionToWire => Some(Stage::DecisionToWire),
            Self::Slots => None,
        }
    }

    pub(crate) fn cell(self, flow: LatencyFlow, report: Option<&LatencyReport>) -> (String, Tone) {
        let mut text = String::new();
        let Some(stage) = self.stage() else {
            let slots = report.and_then(|report| slots(flow, report));
            format::write_opt_slots(&mut text, slots);
            return (text, value_tone(slots.is_some()));
        };
        let reading = report
            .and_then(|report| report.cell(flow, stage))
            .map_or(LatencyReading::Absent, |cell| cell.reading(SHOWN));
        match reading {
            LatencyReading::Micros(micros) => {
                format::write_opt_latency_micros(&mut text, Some(micros));
                (text, value_tone(true))
            }
            LatencyReading::Thin { .. } => (THIN.to_owned(), value_tone(false)),
            LatencyReading::Absent => {
                format::write_opt_latency_micros(&mut text, None);
                (text, value_tone(false))
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LatencyRow {
    pub label: &'static str,
    pub short_label: &'static str,
    pub flow: LatencyFlow,
}

pub(crate) const LATENCY_ROWS: [LatencyRow; LATENCY_ROW_COUNT] = [
    LatencyRow {
        label: "MARKET DATA",
        short_label: "MKT",
        flow: LatencyFlow::MarketData,
    },
    LatencyRow {
        label: "ORDER TRIP",
        short_label: "ORD",
        flow: LatencyFlow::Execution,
    },
    LatencyRow {
        label: "HOT PATH",
        short_label: "HOT",
        flow: LatencyFlow::HotPath,
    },
];

/// Backlog is one figure across every input queue, so it reads on the hot path's row alone.
fn slots(flow: LatencyFlow, report: &LatencyReport) -> Option<f64> {
    match flow {
        LatencyFlow::HotPath => report.backlog_ema,
        LatencyFlow::MarketData | LatencyFlow::Execution => None,
    }
}

fn value_tone(is_number: bool) -> Tone {
    if is_number { Tone::Text } else { Tone::Text2 }
}

/// The title, with the age of the report once the report is stale.
pub(crate) fn latency_title(title: &str, staleness: Option<DurationUs>) -> String {
    match staleness {
        Some(age) => format!("{title}  STALE {:.0}s", age.to_secs()),
        None => title.to_owned(),
    }
}

/// The three readings a one-line summary keeps, each with its label: market data from message to
/// decision, the order round trip and the hot path's slot backlog.
pub(crate) fn summary_cells(report: Option<&LatencyReport>) -> [(&'static str, String, Tone); 3] {
    [
        ("DECIDE", LatencyFlow::MarketData, Column::MessageToDecision),
        ("RTT", LatencyFlow::Execution, Column::RoundTrip),
        ("SLOTS", LatencyFlow::HotPath, Column::Slots),
    ]
    .map(|(label, flow, column)| {
        let (text, tone) = column.cell(flow, report);
        (label, text, tone)
    })
}
