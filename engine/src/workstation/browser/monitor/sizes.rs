//! The monitor's sizes in points, each built in one place from the theme and the widths below, so
//! the painters and the fit rules always read the same numbers.

use eframe::egui;
use egui::emath::GuiRounding as _;

use crate::workstation::browser::theme::{self, METRICS};
use crate::workstation::monitor::fit::{self, BandSizes, ColumnWidths};

/// Eight glyphs of `HH:MM:SS` at the channel font, plus a little spare room.
const SHORT_TIME_W: f32 = 58.0;

/// Ten digits of a tick count at the channel font, plus a little spare room.
const PRICE_W: f32 = 72.0;

/// Seventeen glyphs at the channel font: `fee 0.009440 a1` with room for one more digit in the fee
/// and one in the asset number.
const FEE_W: f32 = 113.0;

/// About twenty glyphs of text at the channel font: enough to read a lifecycle or refusal note
/// before its ellipsis.
const SYSTEM_DESCRIPTION_W: f32 = 120.0;

/// About thirty-six glyphs of text at the channel font: the state, id, fill and price of an order
/// update.
const ORDER_DESCRIPTION_W: f32 = 200.0;

/// Header, base, quote, bid counts and ask counts. A fixed count keeps the band's height fixed.
const ACCOUNT_ROWS: usize = 5;

/// The gap under each feature row. The fit rules count it into the row pitch, so the list must
/// use exactly this and not whatever spacing the page has.
pub(super) const FEATURE_ROW_SPACING: f32 = METRICS.space_1;

pub fn column_widths() -> ColumnWidths {
    ColumnWidths {
        pad: METRICS.space_2,
        time: METRICS.monitor_time_col_w,
        short_time: SHORT_TIME_W,
        liquidity: METRICS.monitor_fill_tag_w,
        side: METRICS.monitor_intent_tag_w,
        price: PRICE_W,
        qty: METRICS.monitor_qty_col_w,
        fee: FEE_W,
        system_description: SYSTEM_DESCRIPTION_W,
        order_description: ORDER_DESCRIPTION_W,
    }
}

pub fn band_sizes() -> BandSizes {
    BandSizes {
        status: METRICS.monitor_row_height + 2.0 * METRICS.space_1,
        // Built from the same row height and padding the painter uses, so a size change cannot
        // leave the band too short for its rows.
        account: ACCOUNT_ROWS as f32 * METRICS.monitor_row_height + 2.0 * METRICS.space_1,
        summary: METRICS.monitor_summary_height,
        tabs: METRICS.monitor_tab_height,
        feature_pitch: METRICS.monitor_row_height + FEATURE_ROW_SPACING,
        channel_pitch: METRICS.monitor_row_height,
        feature_fraction: METRICS.monitor_feature_fraction,
    }
}

/// Returns the status strip along the top of `monitor` and the scroll viewport under it.
pub(super) fn split_status(monitor: egui::Rect) -> (egui::Rect, egui::Rect) {
    let strip = monitor.with_max_y((monitor.top() + band_sizes().status).min(monitor.bottom()));
    // Round the viewport the way the scroll area rounds its own, so a fractional pane edge cannot
    // leave the content a hair taller than the viewport and show a scroll bar on a pane that fits.
    let viewport = monitor.with_min_y(strip.bottom()).round_ui();
    (strip, viewport)
}

/// The pane height the band fit rules work from: the status strip plus the rounded scroll viewport
/// under it.
pub fn pane_height(monitor: egui::Rect) -> f32 {
    let (_, viewport) = split_status(monitor);
    viewport.height() + band_sizes().status
}

/// The centred column of `band` that holds the content. The band's background and rules use the
/// whole band, and everything else stays inside the column.
pub(super) fn content_column(band: egui::Rect) -> egui::Rect {
    theme::centred_column(band, fit::content_width(band.width()))
}
