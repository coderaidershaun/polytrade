//! The mid chart, painted by hand as a line or as candles with this engine's fills marked on it. It
//! also owns the frame and inset both stacked charts paint in, and hands its plot geometry back for
//! the crosshair.

use std::cmp::Ordering;
use std::mem;

use eframe::egui;

use crate::ids::{InstrumentId, Price, Side};

use crate::workstation::browser::theme::{self, DARK, METRICS};
use crate::workstation::chart::model::ChartModel;
use crate::workstation::chart::view::{
    self, Candle, CandleGrouping, ChartBounds, ChartDomain, ChartMode,
};
use crate::workstation::text::{self, AxisLabels};

pub const AXIS_FONT: f32 = METRICS.chart_axis_font;

pub struct ChartFrame<'a> {
    pub chart: &'a ChartModel,
    pub instrument: InstrumentId,
    pub mode: ChartMode,
    pub tick: Price,
    pub domain: Option<ChartDomain>,
}

/// Pixel transform for one plot of the stack. `B` names the value axis's unit, so the mid plot and
/// the risk plot below it cannot be handed to each other's painter.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Plot<B> {
    pub rect: egui::Rect,
    pub domain: ChartDomain,
    pub bounds: B,
}

impl<B: Copy + Into<ChartBounds>> Plot<B> {
    pub(crate) fn x(&self, index: u64) -> f32 {
        let fraction = view::x_fraction(index, self.domain);
        egui::lerp(self.rect.left()..=self.rect.right(), fraction)
    }

    /// The range is inverted because screen y grows downward.
    pub(crate) fn y(&self, value: i64) -> f32 {
        let fraction = view::y_fraction(value, self.bounds.into());
        egui::lerp(self.rect.bottom()..=self.rect.top(), fraction)
    }

    pub(super) fn point(&self, index: u64, value: i64) -> egui::Pos2 {
        egui::pos2(self.x(index), self.y(value))
    }
}

pub(crate) type PlotGeometry = Plot<ChartBounds>;

impl PlotGeometry {
    fn slot_width(&self) -> f32 {
        self.rect.width() / self.domain.width() as f32
    }
}

#[must_use]
pub struct ChartPaint<B> {
    pub response: egui::Response,
    pub geometry: Option<Plot<B>>,
}

pub fn paint(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    frame: &ChartFrame<'_>,
) -> ChartPaint<ChartBounds> {
    paint_frame(ui, rect, "polysim-chart", |painter, plot| {
        paint_plot(painter, plot, frame)
    })
}

/// Every plot of the stack gets the same frame and the same inset, so the stacked charts' x extents
/// match by construction and their axes line up.
pub(super) fn paint_frame<B>(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    id: &str,
    paint_plot: impl FnOnce(&egui::Painter, egui::Rect) -> Option<Plot<B>>,
) -> ChartPaint<B> {
    let response = ui.interact(rect, egui::Id::new(id), egui::Sense::hover());
    let painter = ui.painter().with_clip_rect(rect);

    painter.rect_filled(rect, 0.0, DARK.panel);
    let geometry = plot_rect(rect).and_then(|plot_rect| paint_plot(&painter, plot_rect));
    ChartPaint { response, geometry }
}

fn plot_rect(rect: egui::Rect) -> Option<egui::Rect> {
    let pad = egui::vec2(METRICS.chart_pad, METRICS.chart_pad);
    let gutter = egui::vec2(METRICS.chart_axis_gutter, 0.0);
    let plot = egui::Rect::from_min_max(rect.min + pad, rect.max - pad - gutter);
    plot.is_positive().then_some(plot)
}

fn paint_plot(
    painter: &egui::Painter,
    rect: egui::Rect,
    frame: &ChartFrame<'_>,
) -> Option<PlotGeometry> {
    let plot = project(rect, frame);
    let domain = plot.map(|plot| plot.domain);
    let bounds = plot.map(|plot| plot.bounds);
    if let Some(note) = text::mid_note(frame.tick, domain, bounds) {
        paint_note(painter, rect, note);
        return None;
    }
    let plot = plot?;
    match frame.mode {
        ChartMode::Line => paint_line(painter, &plot, frame),
        ChartMode::Candles => paint_candles(painter, &plot, frame),
    }
    paint_fills(painter, &plot, frame);
    paint_axis_labels(
        painter,
        plot.rect,
        plot.bounds,
        AxisLabels::VenueMid(frame.tick),
    );
    Some(plot)
}

fn project(rect: egui::Rect, frame: &ChartFrame<'_>) -> Option<PlotGeometry> {
    let domain = frame
        .domain
        .or_else(|| view::domain(frame.chart, frame.instrument))?;
    let bounds = view::bounds(frame.chart, frame.instrument, domain)?;
    Some(PlotGeometry {
        rect,
        domain,
        bounds,
    })
}

pub(crate) fn paint_axis_labels(
    painter: &egui::Painter,
    rect: egui::Rect,
    bounds: ChartBounds,
    labels: AxisLabels,
) {
    let ticks = view::value_ticks(bounds, rect.height(), AXIS_FONT);
    let font = egui::FontId::monospace(AXIS_FONT);
    let stroke = theme::hairline(painter, DARK.grid);
    let left = rect.right() + METRICS.chart_pad;
    for (value, label) in text::axis_labels(ticks, labels) {
        let y = egui::lerp(rect.bottom()..=rect.top(), view::y_fraction(value, bounds));
        painter.hline(rect.right()..=left, theme::crisp(painter, y), stroke);
        painter.text(
            egui::pos2(left, y),
            egui::Align2::LEFT_CENTER,
            &label,
            font.clone(),
            DARK.text_secondary,
        );
    }
}

fn paint_line(painter: &egui::Painter, plot: &PlotGeometry, frame: &ChartFrame<'_>) {
    let stroke = egui::Stroke::new(METRICS.chart_line_width, DARK.selected);
    let mut run: Vec<egui::Pos2> = Vec::new();
    for point in view::segment_points(frame.chart, frame.instrument, plot.domain) {
        if point.is_run_start {
            flush_run(painter, &mut run, stroke);
        }
        run.push(plot.point(point.bucket.index, point.bucket.close_half_ticks));
    }
    flush_run(painter, &mut run, stroke);
}

pub(crate) fn flush_run(painter: &egui::Painter, run: &mut Vec<egui::Pos2>, stroke: egui::Stroke) {
    match run.len() {
        0 => {}
        1 => {
            painter.circle_filled(run[0], METRICS.chart_line_width, stroke.color);
            run.clear();
        }
        _ => {
            painter.add(egui::Shape::line(mem::take(run), stroke));
        }
    }
}

fn paint_candles(painter: &egui::Painter, plot: &PlotGeometry, frame: &ChartFrame<'_>) {
    let grouping = CandleGrouping::for_plot(plot.domain, plot.rect.width());
    let candles = grouping.candles(frame.chart, frame.instrument);
    let mut bodies = egui::Mesh::default();
    bodies.reserve_vertices(candles.len() * 4);
    bodies.reserve_triangles(candles.len() * 2);
    let mut wicks = Vec::with_capacity(candles.len());
    let mut breaks = Vec::new();
    let thinnest = theme::one_physical_pixel(painter);

    for candle in &candles {
        let color = body_color(candle);
        let center_x = (plot.x(candle.first) + plot.x(candle.last)) / 2.0;
        let span = (candle.last - candle.first + 1) as f32 * plot.slot_width();
        if candle.has_break_before {
            breaks.push(egui::Shape::vline(
                theme::crisp(painter, center_x - span / 2.0),
                plot.rect.y_range(),
                theme::hairline(painter, DARK.border),
            ));
        }
        let half_body = (span - METRICS.chart_body_gap)
            .max(METRICS.chart_min_body_width)
            .max(thinnest)
            / 2.0;
        let body = egui::Rect::from_x_y_ranges(
            center_x - half_body..=center_x + half_body,
            body_y_range(plot, candle, thinnest),
        );
        bodies.add_colored_rect(body, color);
        wicks.push(egui::Shape::vline(
            theme::crisp(painter, center_x),
            egui::Rangef::new(
                plot.y(candle.high_half_ticks),
                plot.y(candle.low_half_ticks),
            ),
            egui::Stroke::new(METRICS.chart_wick_width, color),
        ));
    }
    painter.extend(breaks);
    painter.extend(wicks);
    painter.add(egui::Shape::mesh(bodies));
}

fn body_y_range(plot: &PlotGeometry, candle: &Candle, thinnest: f32) -> egui::Rangef {
    let top = plot.y(candle.open_half_ticks.max(candle.close_half_ticks));
    let bottom = plot
        .y(candle.open_half_ticks.min(candle.close_half_ticks))
        .max(top + thinnest);
    egui::Rangef::new(top, bottom)
}

fn body_color(candle: &Candle) -> egui::Color32 {
    match candle.close_half_ticks.cmp(&candle.open_half_ticks) {
        Ordering::Greater => DARK.bid,
        Ordering::Less => DARK.ask,
        Ordering::Equal => DARK.text_secondary,
    }
}

/// Real venue fills, each at its own bucket and price and never snapped to the mid line. They paint
/// after the series so no marker hides under a candle, and a marker off the mid shows where we
/// really traded.
fn paint_fills(painter: &egui::Painter, plot: &PlotGeometry, frame: &ChartFrame<'_>) {
    let mut fills = view::visible_fills(frame.chart, frame.instrument, plot.domain).peekable();
    if fills.peek().is_none() {
        return;
    }
    let outline = egui::Stroke::new(METRICS.chart_marker_outline, DARK.canvas);
    for fill in fills {
        let color = match fill.side {
            Side::Buy => DARK.bid,
            Side::Sell => DARK.sell_fill,
        };
        painter.circle(
            plot.point(fill.index, fill.half_ticks),
            METRICS.chart_marker_radius,
            color,
            outline,
        );
    }
    paint_fill_caption(painter, plot.rect);
}

fn paint_fill_caption(painter: &egui::Painter, rect: egui::Rect) {
    painter.text(
        rect.left_bottom(),
        egui::Align2::LEFT_BOTTOM,
        "fills",
        egui::FontId::proportional(METRICS.chart_caption_font),
        DARK.text_secondary,
    );
}

pub(crate) fn paint_note(painter: &egui::Painter, rect: egui::Rect, note: &str) {
    painter.text(
        rect.center(),
        egui::Align2::CENTER_CENTER,
        note,
        egui::FontId::proportional(METRICS.chart_note_font),
        DARK.text_secondary,
    );
}
