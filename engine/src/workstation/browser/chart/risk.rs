//! The risk chart under the mid chart: exposure or PnL as a line over the same bucket window the
//! mid chart drew, with the zero line always in view. It draws no candles, because a position has
//! one value per spin rather than four prices. The window comes from the caller, so both charts put
//! the pointer on the same bucket.

use eframe::egui;

use crate::ids::InstrumentId;

use super::mid::{self, ChartPaint, Plot, paint_note};
use crate::workstation::browser::theme::{self, DARK, METRICS};
use crate::workstation::chart::position_model::PositionModel;
use crate::workstation::chart::position_view::{self, QuoteBounds, RiskSeries};
use crate::workstation::chart::view::ChartDomain;
use crate::workstation::text::{self, AxisLabels};

/// `domain` is `None` when the mid chart has no window, and this chart then shows its empty note.
pub(super) struct PositionFrame<'a> {
    pub positions: &'a PositionModel,
    pub instrument: InstrumentId,
    pub series: RiskSeries,
    pub domain: Option<ChartDomain>,
}

/// The pixel transform on the quote axis. Its bounds type keeps it apart from the mid chart's plot.
pub(super) type PositionPlot = Plot<QuoteBounds>;

pub(super) fn paint(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    frame: &PositionFrame<'_>,
) -> ChartPaint<QuoteBounds> {
    mid::paint_frame(ui, rect, "polysim-position-chart", |painter, plot| {
        paint_plot(painter, plot, frame)
    })
}

fn paint_plot(
    painter: &egui::Painter,
    rect: egui::Rect,
    frame: &PositionFrame<'_>,
) -> Option<PositionPlot> {
    let bounds = frame.domain.and_then(|domain| {
        position_view::bounds(frame.positions, frame.instrument, frame.series, domain)
    });
    if let Some(note) = text::risk_note(frame.domain, bounds) {
        paint_note(painter, rect, note);
        return None;
    }
    let (domain, bounds) = frame.domain.zip(bounds)?;
    let plot = PositionPlot {
        rect,
        domain,
        bounds,
    };
    paint_zero_line(painter, &plot);
    paint_line(painter, &plot, frame);
    mid::paint_axis_labels(
        painter,
        plot.rect,
        plot.bounds.as_chart_bounds(),
        AxisLabels::QuoteAmount,
    );
    Some(plot)
}

/// Drawn under the series. The bounds always include zero, so this line is always in view.
fn paint_zero_line(painter: &egui::Painter, plot: &PositionPlot) {
    painter.hline(
        plot.rect.x_range(),
        theme::crisp(painter, plot.y(0)),
        theme::hairline(painter, DARK.grid),
    );
}

fn paint_line(painter: &egui::Painter, plot: &PositionPlot, frame: &PositionFrame<'_>) {
    let stroke = egui::Stroke::new(METRICS.chart_line_width, DARK.selected);
    let mut run: Vec<egui::Pos2> = Vec::new();
    let mut previous: Option<u64> = None;
    for bucket in position_view::visible_buckets(frame.positions, frame.instrument, plot.domain) {
        // A missing index is a spin where the engine held no mark, so the line breaks there.
        // Positions carry no lost-book flag, so the index alone decides.
        if previous.is_some_and(|previous| bucket.index != previous + 1) {
            mid::flush_run(painter, &mut run, stroke);
        }
        previous = Some(bucket.index);
        run.push(plot.point(bucket.index, frame.series.value(bucket)));
    }
    mid::flush_run(painter, &mut run, stroke);
}
