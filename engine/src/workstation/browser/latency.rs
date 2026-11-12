//! The latency grid at the top of the chart pane: mean stage times per flow and the input-queue
//! backlog. Every cell is a number to scan, so it only paints, folding to one line on a short pane.

use eframe::egui;

use crate::msg::ui::UiLatencySummary;

use super::theme::{self, DARK, METRICS};
use crate::workstation::chart::pane::{LATENCY_HEADER_HEIGHT, LATENCY_ROW_HEIGHT, LatencyBand};
use crate::workstation::text::{
    Column, Flow, LATENCY_ROWS, LATENCY_SHORT_TITLE, LATENCY_TITLE, summary_cells,
};

/// Room for a three-letter row label after its inset and no more, so a 600 point pane still holds
/// all seven columns.
const SHORT_LABEL_WIDTH: f32 = 40.0;

/// The widest value is nine characters of the 12 point monospace face, about 65 points. With the
/// 8 point right inset and 7 points of air, the gap between two neighbouring values is about two
/// characters wide, so they never read as one run of digits.
const MIN_COLUMN_WIDTH: f32 = 80.0;

/// Wider columns only push the numbers further apart and make a row harder to scan, so any width
/// past this stays blank.
const MAX_COLUMN_WIDTH: f32 = 128.0;

pub struct LatencyFrame {
    pub summary: Option<UiLatencySummary>,
}

/// Splits a chart pane's body into the latency band at the top and the charts below it.
pub fn split(body: egui::Rect) -> (egui::Rect, egui::Rect) {
    let band = LatencyBand::for_pane(body.height(), METRICS.latency_fraction);
    body.split_top_bottom_at_y(body.top() + band.height().min(body.height()))
}

pub fn paint(ui: &mut egui::Ui, rect: egui::Rect, frame: &LatencyFrame) {
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 0.0, DARK.panel);

    let summary = frame.summary.as_ref();
    match LatencyBand::fitting(rect.height()) {
        LatencyBand::Grid => paint_grid(&painter, rect, summary),
        LatencyBand::Summary => paint_summary(&painter, rect, summary),
    }

    painter.hline(
        rect.x_range(),
        theme::crisp_bottom_edge(&painter, rect.bottom()),
        theme::hairline(&painter, DARK.border),
    );
}

/// How many columns a width holds, and how wide the label column and each value column are. Short
/// labels take over when the full ones would leave room for fewer than every column.
struct Fit {
    label_width: f32,
    column_width: f32,
    count: usize,
    has_short_labels: bool,
}

impl Fit {
    fn for_width(width: f32) -> Self {
        let holds =
            |label_width: f32| ((width - label_width) / MIN_COLUMN_WIDTH).floor().max(0.0) as usize;
        let has_short_labels = holds(METRICS.latency_label_col_w) < Column::ALL.len();
        let label_width =
            if has_short_labels { SHORT_LABEL_WIDTH } else { METRICS.latency_label_col_w };
        let count = holds(label_width).min(Column::ALL.len());
        let column_width = ((width - label_width) / count.max(1) as f32).min(MAX_COLUMN_WIDTH);
        Self {
            label_width,
            column_width,
            count,
            has_short_labels,
        }
    }

    fn content_width(&self) -> f32 {
        self.label_width + self.count as f32 * self.column_width
    }

    /// The centred part of a full-width `band` that holds the labels and values. The band's own
    /// fill and rules reach both edges.
    fn column(&self, band: egui::Rect) -> egui::Rect {
        theme::centred_column(band, self.content_width())
    }

    fn cell(&self, row: egui::Rect, position: usize) -> egui::Rect {
        let left = row.left() + self.label_width + position as f32 * self.column_width;
        row.with_min_x(left).with_max_x(left + self.column_width)
    }
}

fn paint_grid(painter: &egui::Painter, rect: egui::Rect, summary: Option<&UiLatencySummary>) {
    let fit = Fit::for_width(rect.width());
    let grid = rect.with_max_y((rect.top() + LatencyBand::Grid.height()).min(rect.bottom()));

    let header = grid.with_max_y(grid.top() + LATENCY_HEADER_HEIGHT);
    paint_header(painter, header, &fit);
    paint_rows(painter, grid.with_min_y(header.bottom()), &fit, summary);

    // The slot column is a queue depth, not a latency, so a full-height divider stops a reader
    // comparing it with the microsecond columns beside it.
    if Column::shown(fit.count).last() == Some(Column::Slots) {
        let queue_column = fit.cell(fit.column(grid), fit.count - 1);
        theme::rules_v(painter, [queue_column.left()], grid.y_range());
    }
}

fn paint_header(painter: &egui::Painter, band: egui::Rect, fit: &Fit) {
    painter.rect_filled(band, 0.0, DARK.panel_raised);
    let column = fit.column(band);
    let font = egui::FontId::proportional(METRICS.latency_header_font);
    let title = if fit.has_short_labels { LATENCY_SHORT_TITLE } else { LATENCY_TITLE };
    paint_label(painter, column, title, font.clone());
    for (position, shown) in Column::shown(fit.count).enumerate() {
        let cell = fit.cell(column, position);
        paint_value(
            painter,
            cell,
            shown.header(),
            font.clone(),
            DARK.text_secondary,
        );
    }
    theme::rules_h(painter, [band.bottom()], band.x_range());
}

fn paint_rows(
    painter: &egui::Painter,
    band: egui::Rect,
    fit: &Fit,
    summary: Option<&UiLatencySummary>,
) {
    let separator = theme::hairline(painter, DARK.grid);
    let label_font = egui::FontId::proportional(METRICS.latency_row_font);
    for (index, line) in LATENCY_ROWS.iter().enumerate() {
        let top = band.top() + index as f32 * LATENCY_ROW_HEIGHT;
        let row_band = band.with_min_y(top).with_max_y(top + LATENCY_ROW_HEIGHT);
        if index > 0 {
            painter.hline(
                band.x_range(),
                theme::crisp(painter, row_band.top()),
                separator,
            );
        }
        let row = fit.column(row_band);
        let label = if fit.has_short_labels { line.short_label } else { line.label };
        paint_label(painter, row, label, label_font.clone());
        paint_values(painter, row, fit, line.flow, summary);
    }
}

fn paint_values(
    painter: &egui::Painter,
    row: egui::Rect,
    fit: &Fit,
    flow: Flow,
    summary: Option<&UiLatencySummary>,
) {
    let font = egui::FontId::monospace(METRICS.latency_value_font);
    for (position, column) in Column::shown(fit.count).enumerate() {
        let (text, tone) = column.cell(flow, summary);
        let cell = fit.cell(row, position);
        paint_value(painter, cell, &text, font.clone(), DARK.tone(tone));
    }
}

/// The folded band: the title, then as many of the three readings as the width holds, never
/// painting past the right edge. It stays left-aligned instead of centred: it is one line of words
/// that starts with its title, like the strip readout and the link bar.
fn paint_summary(painter: &egui::Painter, rect: egui::Rect, summary: Option<&UiLatencySummary>) {
    let band = rect.with_max_y((rect.top() + LatencyBand::Summary.height()).min(rect.bottom()));
    painter.rect_filled(band, 0.0, DARK.panel_raised);
    theme::rules_h(painter, [band.bottom()], band.x_range());

    let label_font = egui::FontId::proportional(METRICS.latency_header_font);
    let value_font = egui::FontId::monospace(METRICS.latency_value_font);
    let title = painter.layout_no_wrap(
        LATENCY_TITLE.to_owned(),
        label_font.clone(),
        DARK.text_secondary,
    );
    let mut left = band.left() + METRICS.space_2;
    let title_width = title.size().x;
    paint_galley(painter, band, left, title);
    left += title_width + METRICS.space_3;

    for (label, text, tone) in summary_cells(summary) {
        let label =
            painter.layout_no_wrap(label.to_owned(), label_font.clone(), DARK.text_secondary);
        let value = painter.layout_no_wrap(text, value_font.clone(), DARK.tone(tone));
        let value_left = left + label.size().x + METRICS.space_1;
        let right = value_left + value.size().x;
        if right + METRICS.space_2 > band.right() {
            return;
        }
        paint_galley(painter, band, left, label);
        paint_galley(painter, band, value_left, value);
        left = right + METRICS.space_3;
    }
}

fn paint_galley(
    painter: &egui::Painter,
    band: egui::Rect,
    left: f32,
    galley: std::sync::Arc<egui::Galley>,
) {
    let top = theme::crisp(painter, band.center().y - galley.size().y / 2.0);
    painter.galley(egui::pos2(left, top), galley, DARK.text_secondary);
}

fn paint_label(painter: &egui::Painter, cell: egui::Rect, text: &str, font: egui::FontId) {
    let anchor = egui::pos2(cell.left() + METRICS.space_2, cell.center().y);
    let align = egui::Align2::LEFT_CENTER;
    theme::text_at(painter, anchor, align, text, font, DARK.text_secondary);
}

fn paint_value(
    painter: &egui::Painter,
    cell: egui::Rect,
    text: &str,
    font: egui::FontId,
    color: egui::Color32,
) {
    let anchor = egui::pos2(cell.right() - METRICS.space_2, cell.center().y);
    let align = egui::Align2::RIGHT_CENTER;
    theme::text_at(painter, anchor, align, text, font, color);
}
