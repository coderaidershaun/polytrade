//! The crosshair over both plots: under a mouse, under a dragging finger, or where a finger lifted.
//! It skips the strip so it never crosses the series selector, and each pill stays inside its plot.
//! A finger on a plot scrubs, so a tap there always pins where it lifts, even over the pinned
//! bucket. Every other click or tap in the pane lets go of the pin.

use std::sync::Arc;

use eframe::egui;

use crate::ids::{InstrumentId, Price};

use super::mid::PlotGeometry;
use super::pin::{CrosshairPin, Touch};
use super::risk::PositionPlot;
use crate::workstation::browser::theme::{self, DARK, METRICS};
use crate::workstation::browser::touch::PaneClick;
use crate::workstation::chart::model::{ChartModel, bucket_open_ts};
use crate::workstation::chart::position_model::PositionModel;
use crate::workstation::chart::position_view::RiskSeries;
use crate::workstation::chart::view::{self, ChartDomain};
use crate::workstation::format;

/// Air around a pill's text.
const PILL_PAD: egui::Vec2 = egui::vec2(6.0, 3.0);

/// How far a pill sits from the hairline, so the line it explains stays visible beside it.
const PILL_OFFSET: f32 = 8.0;

/// Everything the crosshair needs to resolve and label one bucket. The two geometries carry the pixel
/// transforms; the two models carry the values, which a transform does not.
pub(super) struct CrosshairFrame<'a> {
    pub chart: &'a ChartModel,
    pub positions: &'a PositionModel,
    pub instrument: InstrumentId,
    pub tick: Price,
    pub series: RiskSeries,
    pub mid: Option<PlotGeometry>,
    pub lower: Option<PositionPlot>,
    /// Both charts together. The hairline clips to it, and a finger must land inside it to scrub.
    pub stack: egui::Rect,
    /// The whole pane body around the stack. A mouse click in it, or a tap off both plots, lets go
    /// of a pin.
    pub pane: egui::Rect,
    pub hovered: Option<egui::Pos2>,
}

/// The x axis both plots share, which turns a pointer position into a bucket.
#[derive(Clone, Copy)]
struct Axis {
    rect: egui::Rect,
    domain: ChartDomain,
}

impl Axis {
    fn of(frame: &CrosshairFrame<'_>) -> Option<Self> {
        let mid = frame.mid.map(|mid| Self {
            rect: mid.rect,
            domain: mid.domain,
        });
        mid.or_else(|| {
            frame.lower.map(|lower| Self {
                rect: lower.rect,
                domain: lower.domain,
            })
        })
    }

    fn bucket_at(self, x: f32) -> u64 {
        let fraction = (x - self.rect.left()) / self.rect.width().max(f32::EPSILON);
        view::bucket_at_fraction(fraction, self.domain)
    }
}

/// A touch event the pin cares about, copied out of the frame's input so no lock is held while the
/// event is resolved.
#[derive(Clone, Copy)]
struct FingerEvent {
    finger: u64,
    phase: egui::TouchPhase,
    pos: egui::Pos2,
}

impl FingerEvent {
    fn from_event(event: &egui::Event) -> Option<Self> {
        match *event {
            egui::Event::Touch { id, phase, pos, .. } => Some(Self {
                finger: id.0,
                phase,
                pos,
            }),
            _ => None,
        }
    }
}

pub(super) fn paint(ui: &egui::Ui, frame: &CrosshairFrame<'_>) {
    let Some(axis) = Axis::of(frame) else { return };
    let pin_id = egui::Id::new("polysim-crosshair-pin");
    let mut pin: CrosshairPin = ui
        .ctx()
        .data(|data| data.get_temp(pin_id))
        .unwrap_or_default();
    if is_let_go_click(ui, frame) {
        pin.on_touch(Touch::LetGo);
    }
    for touch in touches(ui, frame, axis) {
        pin.on_touch(touch);
    }
    if !ui.input(|input| input.any_touches()) {
        pin.end_scrub();
    }
    pin.forget_outside(axis.domain);
    ui.ctx().data_mut(|data| data.insert_temp(pin_id, pin));

    let hovered = frame.hovered.map(|pointer| axis.bucket_at(pointer.x));
    let Some(bucket) = pin.bucket(hovered) else { return };
    let painter = ui.painter().with_clip_rect(frame.stack);
    paint_hairline(&painter, frame, bucket);
    paint_pills(&painter, frame, bucket);
}

/// Whether this frame's click lets go of a pin. A mouse click does, plots included. A tap does
/// unless it landed on a plot: a finger there scrubs, and the pin lands where it lifts. The click
/// is read before the touch events apply, so a tap that starts on a plot and lifts off it still
/// pins where it lifted.
fn is_let_go_click(ui: &egui::Ui, frame: &CrosshairFrame<'_>) -> bool {
    match PaneClick::read(ui.ctx(), frame.pane, ui.layer_id()) {
        Some(PaneClick::Mouse) => true,
        Some(PaneClick::Finger(at)) => !is_on_plot(frame, at),
        None => false,
    }
}

/// This frame's touch events in the order they arrived. The chart reads raw touches, not clicks,
/// because a finger drags across it to scrub. A tap can deliver its start and end in one frame, so
/// their order decides what the pin ends up holding.
fn touches(ui: &egui::Ui, frame: &CrosshairFrame<'_>, axis: Axis) -> Vec<Touch> {
    let events: Vec<FingerEvent> = ui.input(|input| {
        input
            .events
            .iter()
            .filter_map(FingerEvent::from_event)
            .collect()
    });
    events
        .into_iter()
        .filter_map(|event| resolve(ui, frame, axis, event))
        .collect()
}

fn resolve(
    ui: &egui::Ui,
    frame: &CrosshairFrame<'_>,
    axis: Axis,
    event: FingerEvent,
) -> Option<Touch> {
    let FingerEvent { finger, phase, pos } = event;
    match phase {
        egui::TouchPhase::Start => {
            (is_reachable(ui, frame, pos) && is_on_plot(frame, pos)).then(|| Touch::OnPlot {
                finger,
                bucket: axis.bucket_at(pos.x),
            })
        }
        // A finger that drags off the stack keeps scrubbing, clamped to the plot's edge.
        egui::TouchPhase::Move => Some(Touch::Moved {
            finger,
            bucket: axis.bucket_at(pos.x),
        }),
        egui::TouchPhase::End => Some(Touch::Lifted { finger }),
        egui::TouchPhase::Cancel => Some(Touch::Cancelled { finger }),
    }
}

/// Inside the stack, and not under a popup that sits above it.
fn is_reachable(ui: &egui::Ui, frame: &CrosshairFrame<'_>, pos: egui::Pos2) -> bool {
    frame.stack.contains(pos) && ui.ctx().layer_id_at(pos) == Some(ui.layer_id())
}

fn is_on_plot(frame: &CrosshairFrame<'_>, pos: egui::Pos2) -> bool {
    frame.mid.is_some_and(|mid| mid.rect.contains(pos))
        || frame.lower.is_some_and(|lower| lower.rect.contains(pos))
}

fn paint_hairline(painter: &egui::Painter, frame: &CrosshairFrame<'_>, bucket: u64) {
    let stroke = theme::hairline(painter, DARK.grid);
    let mid = frame.mid.map(|mid| (mid.x(bucket), mid.rect.y_range()));
    let lower = frame
        .lower
        .map(|lower| (lower.x(bucket), lower.rect.y_range()));
    for (x, span) in [mid, lower].into_iter().flatten() {
        painter.vline(theme::crisp(painter, x), span, stroke);
    }
}

/// One pill per plot. The bucket's time goes in the mid chart's pill, or in the risk chart's when
/// there is no mid chart.
fn paint_pills(painter: &egui::Painter, frame: &CrosshairFrame<'_>, bucket: u64) {
    let time = bucket_open_ts(bucket, frame.chart.spin_interval())
        .map(|at| format::render(|out| format::write_time_of_day(out, at)));

    if let Some(mid) = frame.mid {
        let mut lines = Vec::new();
        let mut anchor = egui::pos2(mid.x(bucket), mid.rect.top());
        if let Some(close) = mid_close(frame, bucket) {
            let price = format::render(|out| format::write_venue_mid(out, close, frame.tick));
            lines.push((price, DARK.text_primary));
            anchor = mid.point(bucket, close);
        }
        lines.extend(time.clone().map(|time| (time, DARK.text_secondary)));
        paint_pill(painter, mid.rect, anchor, lines);
    }
    if let Some(lower) = frame.lower
        && let Some(value) = position_value(frame, bucket)
    {
        let readout = format::render(|out| {
            format::write_quote_amount(out, value, RiskSeries::READOUT_DECIMALS);
        });
        let mut lines = vec![(readout, DARK.text_primary)];
        if frame.mid.is_none() {
            lines.extend(time.map(|time| (time, DARK.text_secondary)));
        }
        paint_pill(painter, lower.rect, lower.point(bucket, value), lines);
    }
}

/// The pill flips to the left of the hairline when the right side would run past the plot, because
/// clamping it inside would cover the line it explains. Lines that would make it taller than the
/// plot are dropped from the bottom.
fn paint_pill(
    painter: &egui::Painter,
    plot: egui::Rect,
    anchor: egui::Pos2,
    lines: Vec<(String, egui::Color32)>,
) {
    let font = egui::FontId::monospace(METRICS.chart_axis_font);
    let mut galleys: Vec<Arc<egui::Galley>> = lines
        .into_iter()
        .map(|(text, color)| painter.layout_no_wrap(text, font.clone(), color))
        .collect();
    let height = |galleys: &[Arc<egui::Galley>]| {
        galleys.iter().map(|galley| galley.size().y).sum::<f32>() + 2.0 * PILL_PAD.y
    };
    while !galleys.is_empty() && height(&galleys) > plot.height() {
        galleys.pop();
    }
    let Some(first) = galleys.first() else { return };
    let widest = galleys
        .iter()
        .map(|galley| galley.size().x)
        .fold(0.0, f32::max);
    let size = egui::vec2(widest + 2.0 * PILL_PAD.x, height(&galleys));
    let right_of_line = anchor.x + PILL_OFFSET;
    let left_of_line = anchor.x - PILL_OFFSET - size.x;
    let is_flipped = right_of_line + size.x > plot.right() && left_of_line >= plot.left();
    let left = if is_flipped { left_of_line } else { right_of_line };
    let top_left = egui::pos2(left, anchor.y - PILL_PAD.y - first.size().y / 2.0);
    let pill = theme::clamp_rect(plot, egui::Rect::from_min_size(top_left, size));

    theme::fill_pill(painter, pill, DARK.border);
    let mut y = pill.top() + PILL_PAD.y;
    for galley in galleys {
        let line_height = galley.size().y;
        painter.galley(
            egui::pos2(pill.left() + PILL_PAD.x, y),
            galley,
            DARK.text_primary,
        );
        y += line_height;
    }
}

fn mid_close(frame: &CrosshairFrame<'_>, bucket: u64) -> Option<i64> {
    frame
        .chart
        .buckets(frame.instrument)
        .find(|candidate| candidate.index == bucket)
        .map(|candidate| candidate.close_half_ticks)
}

fn position_value(frame: &CrosshairFrame<'_>, bucket: u64) -> Option<i64> {
    frame
        .positions
        .buckets(frame.instrument)
        .find(|candidate| candidate.index == bucket)
        .map(|candidate| frame.series.value(candidate))
}
