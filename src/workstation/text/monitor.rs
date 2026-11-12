//! The center panel's words and roles: the quote summary band, the account band and the feature
//! list. The painters draw these as given, so a refusal or a missing reading reads the same wherever
//! it appears.

use crate::hot::exec::RejectOrigin;
use crate::ids::Side;
use crate::msg::exec::RejectClass;
use crate::workstation::dom_view::DomUnit;
use crate::workstation::format::{self, Wrote};
use crate::workstation::monitor::view::{AccountView, AssetRole, AssetRowView, FeatureRowView};

use super::tone::Tone;

/// Five decimals, the size of Binance's smallest BTC step. Eight would only add noise.
pub(crate) const BALANCE_DECIMALS: usize = 5;

/// A notional is money, and money reads at two places whatever precision its asset carries.
pub(crate) const VALUE_DECIMALS: usize = 2;

#[derive(Clone, Copy)]
pub(crate) enum Lane {
    Delta(Side),
    Mid,
}

/// Caption carries the unit because the number alone lies: the same offset is 2 ticks or 0.0017 bp.
pub(crate) fn summary_caption(lane: Lane, unit: DomUnit) -> &'static str {
    match (lane, unit) {
        (Lane::Mid, _) => "MID",
        (Lane::Delta(Side::Buy), DomUnit::Ticks) => "BID delta ticks",
        (Lane::Delta(Side::Buy), DomUnit::Bps) => "BID delta bps",
        (Lane::Delta(Side::Sell), DomUnit::Ticks) => "ASK delta ticks",
        (Lane::Delta(Side::Sell), DomUnit::Bps) => "ASK delta bps",
    }
}

pub(crate) fn delta_hover(unit: DomUnit) -> &'static str {
    match unit {
        DomUnit::Ticks => "delta in ticks from mid, not a price",
        DomUnit::Bps => "delta in basis points from mid, not a price",
    }
}

/// Bps divides the two half-tick counts, so the tick cancels and no tick size is needed.
pub(crate) fn write_delta(
    buf: &mut String,
    delta_half_ticks: Option<i64>,
    mid_half_ticks: Option<i64>,
    unit: DomUnit,
) -> Wrote {
    match unit {
        DomUnit::Bps => format::write_opt_bps_delta(buf, delta_half_ticks, mid_half_ticks),
        DomUnit::Ticks => format::write_opt(buf, delta_half_ticks, format::write_mid),
    }
}

/// A bound is a real reading the cell is too narrow to spell out, so it wears neither the live
/// side colour nor the absent grey.
pub(crate) fn cell_tone(wrote: Wrote, live: Tone) -> Tone {
    match wrote {
        Wrote::Value => live,
        Wrote::Absent => Tone::Text2,
        Wrote::Bound => Tone::Warning,
    }
}

/// The band's small notes: balances in assets the catalog does not name, and exposure frames an
/// engine sent that could not be used. They sit beside the title because the captions fill the
/// right.
pub(crate) fn account_notes(view: &AccountView<'_>) -> Vec<(String, Tone)> {
    [
        (view.unknown_asset_balances, "untracked"),
        (view.rejected_position_frames, "exposure dropped"),
    ]
    .into_iter()
    .filter(|(count, _)| *count != 0)
    .map(|(count, noun)| (format!("+{count} {noun}"), Tone::Text2))
    .collect()
}

/// The asset columns' captions, left to right.
pub(crate) fn account_captions(view: &AccountView<'_>) -> [String; 3] {
    let mut value_caption = String::from("value");
    if !view.quote.label.is_empty() {
        value_caption.push(' ');
        value_caption.push_str(view.quote.label);
    }
    ["free".to_owned(), "locked".to_owned(), value_caption]
}

/// An older engine's catalog names no assets, so the row then shows the asset's role rather than a
/// bare index.
pub(crate) fn asset_label(asset: AssetRowView<'_>) -> &str {
    if !asset.label.is_empty() {
        return asset.label;
    }
    match asset.role {
        AssetRole::Base => "BASE",
        AssetRole::Quote => "QUOTE",
    }
}

pub(crate) fn write_amount(scratch: &mut String, mantissa: Option<i64>, decimals: usize) {
    format::write_opt(scratch, mantissa, |buf, mantissa| {
        format::write_quote_amount(buf, mantissa, decimals)
    });
}

/// The last refusal the account band shows. None yet reads as no reading.
pub(crate) fn reject_display(origin: Option<RejectOrigin>) -> (String, Tone) {
    match origin {
        Some(RejectOrigin::Local(reason)) => (format!("gate {}", reason.label()), Tone::Warning),
        Some(RejectOrigin::Venue { class, code }) => {
            (format!("{code}  {}", class.as_str()), reject_tone(class))
        }
        None => (format::MISSING.to_owned(), Tone::Text2),
    }
}

/// How alarming a venue refusal is, stated once. The account band and the Orders channel report the
/// same rejection at the same moment, so a severity that lived in two tables could disagree on screen.
/// A post-only cross is an ordinary cost of quoting as maker, and `Gone` is the reconciler working.
pub(crate) fn reject_tone(class: RejectClass) -> Tone {
    match class {
        RejectClass::Refused | RejectClass::Gone => Tone::Text2,
        RejectClass::StillLive | RejectClass::Ambiguous => Tone::Warning,
        RejectClass::Fatal => Tone::Invalid,
    }
}

/// An id past the catalog's name table renders as no reading, the same as every other absent value
/// in the panel — including this row's own value cell.
pub(crate) fn feature_name<'a>(row: &FeatureRowView, names: &'a [Box<str>]) -> &'a str {
    names
        .get(row.feature.0 as usize)
        .map_or(format::MISSING, |name| name.as_ref())
}

pub(crate) fn feature_hover(row: &FeatureRowView, names: &[Box<str>]) -> String {
    let mut hover = String::from(feature_name(row, names));
    hover.push_str("  =  ");
    hover.push_str(&format::render(|out| write_feature_cell(out, row)));
    if row.stale {
        hover.push_str("   (stale)");
    }
    hover
}

pub(crate) fn write_feature_cell(buf: &mut String, row: &FeatureRowView) {
    format::write_opt(buf, row.value, format::write_feature_value);
}
