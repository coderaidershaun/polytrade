//! The HUD's fit readouts: the sizes each pane's own fit rules chose for the rect it was given, so a
//! reviewer can compare them with what is painted.

use eframe::egui;

use polysim::workstation::browser::chart::mid::{self, Plot};
use polysim::workstation::browser::monitor::{band_sizes, column_widths, pane_height};
use polysim::workstation::chart::pane::{LatencyBand, StackBands};
use polysim::workstation::chart::view::{CandleGrouping, ChartBounds, ChartMode, value_ticks};
use polysim::workstation::monitor::fit::{self, BandHeights, ColumnPlan, TimeColumn};
use polysim::workstation::monitor::view::Channel;

/// What the latency band `band` paints, and how tall that is.
pub fn latency(band: egui::Rect) -> String {
    let fitted = LatencyBand::fitting(band.height());
    let name = match fitted {
        LatencyBand::Grid => "grid",
        LatencyBand::Summary => "summary",
    };
    format!("lat {name} {:.0}", fitted.height())
}

/// How the stack of charts under the latency band shares its `charts` rect.
pub fn stack(charts: egui::Rect) -> String {
    let bands = StackBands::for_height(charts.height());
    format!(
        "mid {:.0} strip {:.0} risk {:.0}",
        bands.mid, bands.strip, bands.risk
    )
}

/// The value ticks of the mid chart's plot, and in candle mode how many buckets make a candle.
pub fn mid_axis(plot: Option<&Plot<ChartBounds>>, mode: ChartMode) -> String {
    let Some(plot) = plot else { return "no plot".to_owned() };
    let ticks = value_ticks(plot.bounds, plot.rect.height(), mid::AXIS_FONT).count();
    match mode {
        ChartMode::Line => format!("ticks {ticks}"),
        ChartMode::Candles => {
            let grouping = CandleGrouping::for_plot(plot.domain, plot.rect.width());
            format!("ticks {ticks} | candle x{}", grouping.buckets_per_candle())
        }
    }
}

/// The content width, the columns the `channel` rows keep, and the six band heights, top to bottom.
pub fn monitor(rect: egui::Rect, channel: Channel, feature_count: usize) -> String {
    let heights = fit::fit_bands(pane_height(rect), feature_count, &band_sizes());
    let plan = fit::fit_columns(channel, rect.width(), &column_widths());
    format!(
        "mon {:.0} | {} | {}",
        fit::content_width(rect.width()),
        columns_text(plan),
        bands_text(&heights)
    )
}

fn columns_text(plan: ColumnPlan) -> String {
    let time = match plan.time {
        TimeColumn::Full => "full",
        TimeColumn::Short => "short",
        TimeColumn::Hidden => "hidden",
    };
    let liquidity = if plan.has_liquidity { " liq" } else { "" };
    let fee = if plan.has_fee { " fee" } else { "" };
    format!("time {time}{liquidity}{fee}")
}

fn bands_text(heights: &BandHeights) -> String {
    let scrolls = if heights.is_scrolled { " scrolls" } else { "" };
    format!(
        "{:.0} {:.0} {:.0} {:.0} {:.0} {:.0}{scrolls}",
        heights.status,
        heights.account,
        heights.summary,
        heights.features,
        heights.tabs,
        heights.channel
    )
}
