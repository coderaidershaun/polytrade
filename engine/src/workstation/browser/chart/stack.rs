//! The mid chart above the risk chart, sharing one window and one crosshair. The window is computed
//! once from the mid chart and handed to both, because deriving it twice would put the same pointer
//! position on different buckets.

use eframe::egui;

use crate::ids::{InstrumentId, Price};

use super::crosshair::{self, CrosshairFrame};
use super::mid::{self, ChartFrame};
use super::risk::{self, PositionFrame};
use crate::workstation::browser::controls;
use crate::workstation::browser::theme::{self, DARK, METRICS};
use crate::workstation::chart::model::ChartModel;
use crate::workstation::chart::pane::StackBands;
use crate::workstation::chart::position_model::PositionModel;
use crate::workstation::chart::position_view::RiskSeries;
use crate::workstation::chart::view::{self, ChartMode};
use crate::workstation::format;
use crate::workstation::text;

/// The risk selector's segments, in strip order. Exposure leads because it is the default series.
const RISK_SERIES: [(RiskSeries, &str); 2] =
    [(RiskSeries::Exposure, "EXPOSURE"), (RiskSeries::Pnl, "PNL")];

/// The selector's width inside the strip, leaving the rest of it to the readout.
const SELECTOR_WIDTH: f32 = 168.0;

pub struct ChartStackFrame<'a> {
    pub chart: &'a ChartModel,
    pub positions: &'a PositionModel,
    pub instrument: InstrumentId,
    pub tick: Price,
    pub mode: ChartMode,
    pub series: RiskSeries,
    /// The whole chart pane body around the stack, latency band included. A mouse click anywhere in
    /// it, or a tap off both plots, lets go of a pinned crosshair. A tap on a plot pins there.
    pub pane: egui::Rect,
}

pub fn paint(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    frame: &ChartStackFrame<'_>,
) -> Option<RiskSeries> {
    let bands = StackBands::for_height(rect.height());
    let (mid_rect, below_mid) = rect.split_top_bottom_at_y(rect.top() + bands.mid);
    let (strip_rect, lower_rect) = below_mid.split_top_bottom_at_y(below_mid.top() + bands.strip);

    let domain = view::domain(frame.chart, frame.instrument);

    let mid = mid::paint(
        ui,
        mid_rect,
        &ChartFrame {
            chart: frame.chart,
            instrument: frame.instrument,
            mode: frame.mode,
            tick: frame.tick,
            domain,
        },
    );

    let picked = paint_strip(ui, strip_rect, frame);

    let lower = risk::paint(
        ui,
        lower_rect,
        &PositionFrame {
            positions: frame.positions,
            instrument: frame.instrument,
            series: frame.series,
            domain,
        },
    );

    paint_strip_seams(ui, rect, strip_rect);

    crosshair::paint(
        ui,
        &CrosshairFrame {
            chart: frame.chart,
            positions: frame.positions,
            instrument: frame.instrument,
            tick: frame.tick,
            series: frame.series,
            mid: mid.geometry,
            lower: lower.geometry,
            stack: rect,
            pane: frame.pane,
            hovered: mid.response.hover_pos().or(lower.response.hover_pos()),
        },
    );
    picked
}

/// The strip's two edges, the only lines inside the stack: the panel's outer edges belong to the
/// page's frame, so neither chart outlines itself. The painter clips to the whole stack, not the
/// strip, because `crisp` can snap a line onto the pixel row just outside the strip, where a clip
/// at the strip would hide it.
fn paint_strip_seams(ui: &egui::Ui, stack: egui::Rect, strip: egui::Rect) {
    let painter = ui.painter_at(stack);
    theme::rules_h(&painter, [strip.top(), strip.bottom()], strip.x_range());
}

/// The selector fills the strip's whole height, so the strip is the touch target.
fn paint_strip(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    frame: &ChartStackFrame<'_>,
) -> Option<RiskSeries> {
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 0.0, DARK.panel);

    let selector = egui::Rect::from_min_size(
        egui::pos2(rect.left() + METRICS.space_2, rect.top()),
        egui::vec2(SELECTOR_WIDTH, rect.height()),
    );
    let picked =
        controls::paint_segmented_toggle(ui, selector, "risk-series", &RISK_SERIES, frame.series);
    paint_latest_value(&painter, rect, frame);
    picked
}

fn paint_latest_value(painter: &egui::Painter, rect: egui::Rect, frame: &ChartStackFrame<'_>) {
    let Some(latest) = frame.positions.latest(frame.instrument) else { return };
    let value = frame.series.value(&latest);
    let mut readout = String::new();
    format::write_quote_amount(&mut readout, value, RiskSeries::READOUT_DECIMALS);
    painter.text(
        egui::pos2(rect.right() - METRICS.space_2, rect.center().y),
        egui::Align2::RIGHT_CENTER,
        &readout,
        egui::FontId::monospace(METRICS.chart_axis_font),
        DARK.tone(text::sign_tone(value)),
    );
}
