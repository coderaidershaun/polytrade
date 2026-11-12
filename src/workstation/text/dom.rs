//! The ladder's words and roles: the groupings it offers, the status pill, the order and quote
//! rings, the mid's role and a row's price.

use crate::ids::Price;
use crate::time::DurationUs;
use crate::workstation::dom_view::{
    DomGrouping, DomRow, DomStatus, DomUnit, QuotePlacement, price_for_row,
};
use crate::workstation::exec_model::OrderStatus;
use crate::workstation::format;

use super::tone::Tone;

/// How a ladder row marks its price cell: an order the venue holds, one still in flight, one lost,
/// or only the quote the strategy wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ring {
    Confirmed,
    InFlight,
    Lost,
    Desired,
}

pub(crate) fn ring(row: &DomRow, status: DomStatus) -> Option<Ring> {
    if status != DomStatus::Live {
        return None;
    }
    match row.order_status {
        Some(OrderStatus::Confirmed) => Some(Ring::Confirmed),
        Some(OrderStatus::InFlight) => Some(Ring::InFlight),
        Some(OrderStatus::Lost) => Some(Ring::Lost),
        None if row.is_quoted => Some(Ring::Desired),
        None => None,
    }
}

pub(crate) fn status_pill(
    status: DomStatus,
    stale_age: Option<DurationUs>,
) -> Option<(String, Tone)> {
    match status {
        DomStatus::Live => None,
        DomStatus::Stale => Some((stale_label(stale_age), Tone::Stale)),
        DomStatus::AwaitingBook => Some(("AWAITING BOOK".to_owned(), Tone::Warning)),
        DomStatus::Disconnected => Some(("DISCONNECTED".to_owned(), Tone::Invalid)),
    }
}

pub(crate) fn mid_tone(mid_half_ticks: Option<i64>) -> Tone {
    match mid_half_ticks {
        Some(_) => Tone::Text,
        None => Tone::Invalid,
    }
}

pub(crate) fn row_price(buf: &mut String, tick: Price, price_decimals: usize, tick_index: i64) {
    format::write_opt(buf, price_for_row(tick, tick_index), |buf, price| {
        format::write_venue_price(buf, price, price_decimals)
    });
}

/// The groupings offered per unit, in dropdown order: tick multiples here, fractions of the mid
/// in [`BPS_GROUPINGS`].
const TICK_GROUPINGS: [(DomGrouping, &str); 7] = [
    (DomGrouping::Ticks { per_bucket: 1 }, "x1"),
    (DomGrouping::Ticks { per_bucket: 2 }, "x2"),
    (DomGrouping::Ticks { per_bucket: 5 }, "x5"),
    (DomGrouping::Ticks { per_bucket: 10 }, "x10"),
    (DomGrouping::Ticks { per_bucket: 25 }, "x25"),
    (DomGrouping::Ticks { per_bucket: 50 }, "x50"),
    (DomGrouping::Ticks { per_bucket: 100 }, "x100"),
];

const BPS_GROUPINGS: [(DomGrouping, &str); 8] = [
    (bps(1, 100), "0.01 bp"),
    (bps(1, 20), "0.05 bp"),
    (bps(1, 10), "0.1 bp"),
    (bps(1, 4), "0.25 bp"),
    (bps(1, 2), "0.5 bp"),
    (bps(1, 1), "1 bp"),
    (bps(2, 1), "2 bp"),
    (bps(5, 1), "5 bp"),
];

const fn bps(numerator: i64, denominator: i64) -> DomGrouping {
    DomGrouping::Bps {
        numerator,
        denominator,
    }
}

pub(crate) fn grouping_table(unit: DomUnit) -> &'static [(DomGrouping, &'static str)] {
    match unit {
        DomUnit::Ticks => &TICK_GROUPINGS,
        DomUnit::Bps => &BPS_GROUPINGS,
    }
}

pub(crate) fn grouping_label(grouping: DomGrouping) -> &'static str {
    grouping_table(grouping.unit())
        .iter()
        .find(|(candidate, _)| *candidate == grouping)
        .map_or(format::MISSING, |(_, label)| *label)
}

pub(crate) fn offscreen_of(order: QuotePlacement, desired: QuotePlacement) -> QuotePlacement {
    match order {
        QuotePlacement::OffScreenAbove { .. } | QuotePlacement::OffScreenBelow { .. } => order,
        _ => desired,
    }
}

fn stale_label(age: Option<DurationUs>) -> String {
    let Some(ms) = age.map(|age| age.micros() / 1_000) else {
        return "STALE".to_owned();
    };
    if ms < 1_000 {
        return format!("STALE {ms} ms");
    }
    format!("STALE {}.{} s", ms / 1_000, (ms % 1_000) / 100)
}

pub(crate) fn order_tone(status: OrderStatus) -> Tone {
    match status {
        OrderStatus::Confirmed => Tone::Selected,
        OrderStatus::InFlight => Tone::Warning,
        OrderStatus::Lost => Tone::Invalid,
    }
}
