//! The charts' words: axis labels, the notes that stand in for a plot with nothing to draw, and
//! the role of a signed readout.

use crate::ids::Price;
use crate::workstation::chart::position_view::QuoteBounds;
use crate::workstation::chart::view::{ChartBounds, ChartDomain};
use crate::workstation::format::{self, AxisTicks};

use super::tone::Tone;

#[derive(Debug, Clone, Copy)]
pub(crate) enum AxisLabels {
    VenueMid(Price),
    QuoteAmount,
}

/// The labelled ticks of a value axis, bottom to top. The ticks decide which values show, and this
/// decides how each one reads.
pub(crate) fn axis_labels(ticks: AxisTicks, labels: AxisLabels) -> Vec<(i64, String)> {
    let decimals = format::quote_axis_decimals(ticks.step());
    ticks
        .map(|value| {
            let label = format::render(|out| match labels {
                AxisLabels::VenueMid(tick) => format::write_venue_mid(out, value, tick),
                AxisLabels::QuoteAmount => format::write_quote_amount(out, value, decimals),
            });
            (value, label)
        })
        .collect()
}

/// What the mid chart says when it has no plot. A missing tick grid outranks a missing window.
pub(crate) fn mid_note(
    tick: Price,
    domain: Option<ChartDomain>,
    bounds: Option<ChartBounds>,
) -> Option<&'static str> {
    if tick.0 <= 0 {
        return Some("no tick grid for this instrument");
    }
    if domain.is_none() || bounds.is_none() {
        return Some("no mid samples yet");
    }
    None
}

/// What the risk chart says when it has no plot. A missing window outranks missing samples.
pub(crate) fn risk_note(
    domain: Option<ChartDomain>,
    bounds: Option<QuoteBounds>,
) -> Option<&'static str> {
    if domain.is_none() {
        return Some("no chart window");
    }
    if bounds.is_none() {
        return Some("no position samples yet");
    }
    None
}

pub(crate) fn sign_tone(value: i64) -> Tone {
    match value {
        value if value > 0 => Tone::Positive,
        value if value < 0 => Tone::Negative,
        _ => Tone::Text2,
    }
}
