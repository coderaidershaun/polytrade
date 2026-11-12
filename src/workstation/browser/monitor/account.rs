//! The account band and the status strip above it. The strip holds the halt latch and the last
//! refusal and never scrolls, so they stay in view whatever the pane height. Each value the band
//! projects can be absent on its own.

use eframe::egui;

use crate::hot::exec::ExecHalt;

use super::sizes::content_column;
use crate::workstation::browser::theme::{self, DARK, METRICS};
use crate::workstation::format::MISSING;
use crate::workstation::monitor::view::{
    AccountView, AssetRowView, MonitorFrame, SideCountView, account,
};
use crate::workstation::text::{
    self, BALANCE_DECIMALS, VALUE_DECIMALS, account_captions, account_notes, asset_label,
    write_amount,
};

/// Draws into the centred column of `band`. The caller fills the band.
pub(super) fn paint_status(ui: &mut egui::Ui, band: egui::Rect, frame: &MonitorFrame<'_>) {
    let view = account(frame.model, frame.instrument, frame.tick);
    let column = content_column(band);
    let painter = ui.painter_at(band);
    let grid = Grid::new(&painter, column);
    let row = column.shrink2(egui::vec2(0.0, METRICS.space_1));
    paint_status_row(&painter, &grid, row, &view);
}

// SMELL: the band's fixed height counts the five rows drawn here, but that count lives with the
// monitor's other sizes and nothing here reads it, so a sixth row would be cut off with no warning.
/// Draws into the centred column of `band`, and its rules across the whole band. The caller fills
/// the band.
pub(super) fn paint(ui: &mut egui::Ui, band: egui::Rect, frame: &MonitorFrame<'_>) {
    let view = account(frame.model, frame.instrument, frame.tick);
    let column = content_column(band);
    let painter = ui.painter_at(band);

    let grid = Grid::new(&painter, column);
    let mut scratch = String::new();
    let mut row = RowCursor::new(column);

    let header = row.next();
    paint_header(&painter, &grid, header, &view);
    paint_asset(&painter, &grid, row.next(), view.base, &mut scratch);
    let assets_bottom = row.next();
    paint_asset(&painter, &grid, assets_bottom, view.quote, &mut scratch);
    paint_counts(&painter, &grid, row.next(), "BID", view.bid);
    paint_counts(&painter, &grid, row.next(), "ASK", view.ask);

    // The container separates its own rows; no cell wears a box. The first rule closes the captions,
    // the second closes the asset table so the counts below it read as a separate list.
    let rules = [header.bottom(), assets_bottom.bottom()];
    theme::rules_h(&painter, rules, band.x_range());
}

/// Hands out fixed-height rows in order, so no painter works out its own y.
struct RowCursor {
    rect: egui::Rect,
    index: usize,
}

impl RowCursor {
    fn new(rect: egui::Rect) -> Self {
        Self { rect, index: 0 }
    }

    fn next(&mut self) -> egui::Rect {
        let top =
            self.rect.top() + METRICS.space_1 + self.index as f32 * METRICS.monitor_row_height;
        self.index += 1;
        self.rect
            .with_min_y(top)
            .with_max_y(top + METRICS.monitor_row_height)
    }
}

/// Label column plus three equal numeric columns, so every row lands on the same x positions and the
/// captions describe what sits under them.
struct Grid {
    label_left: f32,
    numbers_left: f32,
    column_width: f32,
    /// The longest number, in glyphs, that still leaves one glyph of space between it and the
    /// column to its left.
    number_glyphs: usize,
}

impl Grid {
    const COLUMNS: usize = 3;

    fn new(painter: &egui::Painter, rect: egui::Rect) -> Self {
        let label_left = rect.left() + METRICS.space_2;
        let numbers_left = label_left + METRICS.monitor_asset_label_w;
        let width = (rect.right() - METRICS.space_2 - numbers_left).max(0.0);
        let column_width = width / Self::COLUMNS as f32;
        let glyph = painter
            .layout_no_wrap("0".to_owned(), number_font(), DARK.text_primary)
            .size()
            .x;
        Self {
            label_left,
            numbers_left,
            column_width,
            number_glyphs: ((column_width - glyph) / glyph).floor().max(0.0) as usize,
        }
    }

    fn label(&self, row: egui::Rect) -> egui::Rect {
        row.with_min_x(self.label_left)
            .with_max_x(self.numbers_left)
    }

    fn column(&self, row: egui::Rect, index: usize) -> egui::Rect {
        let left = self.numbers_left + index as f32 * self.column_width;
        row.with_min_x(left).with_max_x(left + self.column_width)
    }
}

/// The band title, its notes and the asset columns' captions.
fn paint_header(painter: &egui::Painter, grid: &Grid, rect: egui::Rect, view: &AccountView<'_>) {
    let y = theme::crisp(painter, rect.center().y);
    let title = painter.text(
        egui::pos2(rect.left() + METRICS.space_2, y),
        egui::Align2::LEFT_CENTER,
        "ACCOUNT",
        egui::FontId::proportional(METRICS.monitor_micro_font),
        DARK.text_secondary,
    );
    let mut notes_right = f32::INFINITY;
    for (index, caption) in account_captions(view).iter().enumerate() {
        let cell = grid.column(rect, index);
        let drawn = painter.text(
            egui::pos2(cell.right(), theme::crisp(painter, cell.center().y)),
            egui::Align2::RIGHT_CENTER,
            caption,
            egui::FontId::proportional(METRICS.monitor_micro_font),
            DARK.text_secondary,
        );
        if index == 0 {
            notes_right = drawn.left() - METRICS.space_2;
        }
    }

    // The notes get what is left between the title and the first caption, so one that does not fit
    // is cut short instead of running under the caption.
    let mut note_left = title.right() + METRICS.space_2;
    for (note, tone) in account_notes(view) {
        if note_left >= notes_right {
            break;
        }
        let used = theme::paint_ellipsized(
            painter,
            egui::Rect::from_x_y_ranges(note_left..=notes_right, y..=y),
            &note,
            egui::FontId::proportional(METRICS.monitor_micro_font),
            DARK.tone(tone),
        );
        note_left += used + METRICS.space_2;
    }
}

/// One row per asset: free, locked and what the whole holding is worth. The label paints FIRST so an
/// oversized balance overruns the asset name rather than a digit — losing "USDT" is recoverable.
fn paint_asset(
    painter: &egui::Painter,
    grid: &Grid,
    rect: egui::Rect,
    asset: AssetRowView<'_>,
    scratch: &mut String,
) {
    theme::text_at(
        painter,
        egui::pos2(grid.label(rect).left(), rect.center().y),
        egui::Align2::LEFT_CENTER,
        asset_label(asset),
        egui::FontId::monospace(METRICS.monitor_channel_font),
        DARK.text_primary,
    );

    let free = asset.balance.map(|balance| balance.free);
    let locked = asset.balance.map(|balance| balance.locked);
    let glyphs = grid.number_glyphs;
    write_fitted_amount(scratch, free, BALANCE_DECIMALS, glyphs);
    paint_number(painter, grid.column(rect, 0), scratch, DARK.text_primary);
    // Locked is dimmer: the venue holds it, so it cannot be spent.
    write_fitted_amount(scratch, locked, BALANCE_DECIMALS, glyphs);
    paint_number(painter, grid.column(rect, 1), scratch, DARK.text_secondary);
    // Two places against the balances' five: obviously the same money, obviously a total.
    write_fitted_amount(scratch, asset.value, VALUE_DECIMALS, glyphs);
    paint_number(painter, grid.column(rect, 2), scratch, DARK.text_primary);
}

/// An amount too wide for its column loses decimal places, cut and never rounded, before it would
/// run into the number beside it. The whole part is never cut.
fn write_fitted_amount(
    scratch: &mut String,
    mantissa: Option<i64>,
    decimals: usize,
    max_glyphs: usize,
) {
    for places in (0..=decimals).rev() {
        write_amount(scratch, mantissa, places);
        if scratch.len() <= max_glyphs {
            return;
        }
    }
}

fn number_font() -> egui::FontId {
    egui::FontId::monospace(METRICS.monitor_channel_font)
}

fn paint_number(painter: &egui::Painter, cell: egui::Rect, text: &str, color: egui::Color32) {
    painter.text(
        egui::pos2(cell.right(), theme::crisp(painter, cell.center().y)),
        egui::Align2::RIGHT_CENTER,
        text,
        number_font(),
        color,
    );
}

/// Side's working-order counts on the asset grid's columns, each cell tagged because no caption
/// describes them. `lost` renders even at zero — a real zero is not an absence — and colours only
/// when non-zero. Leaked orders qualify the side itself, so the marker rides beside its label.
fn paint_counts(
    painter: &egui::Painter,
    grid: &Grid,
    rect: egui::Rect,
    label: &str,
    counts: SideCountView,
) {
    let y = theme::crisp(painter, rect.center().y);
    let label_rect = painter.text(
        egui::pos2(grid.label(rect).left(), y),
        egui::Align2::LEFT_CENTER,
        label,
        egui::FontId::monospace(METRICS.monitor_channel_font),
        DARK.text_secondary,
    );
    if counts.leaked > 0 {
        painter.text(
            egui::pos2(label_rect.right() + METRICS.space_1, y),
            egui::Align2::LEFT_CENTER,
            format!("+{}", counts.leaked),
            egui::FontId::monospace(METRICS.monitor_channel_font),
            DARK.invalid,
        );
    }

    let lost_color = if counts.lost > 0 { DARK.invalid } else { DARK.text_secondary };
    let cells = [
        ("open", counts.open, DARK.text_primary),
        ("inflt", counts.in_flight, DARK.text_primary),
        ("lost", counts.lost, lost_color),
    ];
    for (index, (tag, count, color)) in cells.into_iter().enumerate() {
        let cell = grid.column(rect, index);
        // The gap keeps a tag off the number of the cell before it.
        painter.text(
            egui::pos2(cell.left() + METRICS.space_2, y),
            egui::Align2::LEFT_CENTER,
            tag,
            egui::FontId::proportional(METRICS.monitor_micro_font),
            DARK.text_secondary,
        );
        paint_number(painter, cell, &count.to_string(), color);
    }
}

/// The last refusal and the kill switch share the strip. A halt takes the whole strip instead:
/// `HALTED` beside a refusal would collide exactly when it matters, and every refusal is in the
/// Orders channel anyway. Before the engine's first execution frame the gate reads as missing,
/// never as armed.
fn paint_status_row(
    painter: &egui::Painter,
    grid: &Grid,
    rect: egui::Rect,
    view: &AccountView<'_>,
) {
    let y = theme::crisp(painter, rect.center().y);
    if let Some(ExecHalt::Halted { reason, .. }) = view.halt {
        // Filled, not outlined, so the halt is easy to find from the corner of the eye.
        painter.rect_filled(
            rect.shrink2(egui::vec2(METRICS.space_2, 1.0)),
            METRICS.corner_radius,
            DARK.halt_fill,
        );
        painter.text(
            egui::pos2(rect.right() - METRICS.space_2 - METRICS.space_1, y),
            egui::Align2::RIGHT_CENTER,
            format!("{}  {}", text::HALT_WORD, reason.label()),
            egui::FontId::monospace(METRICS.monitor_channel_font),
            DARK.invalid,
        );
        return;
    }

    painter.text(
        egui::pos2(grid.label(rect).left(), y),
        egui::Align2::LEFT_CENTER,
        "reject",
        egui::FontId::proportional(METRICS.monitor_micro_font),
        DARK.text_secondary,
    );
    let (reject, tone) = text::reject_display(view.last_reject.map(|reject| reject.origin));
    let reject_text_rect = grid.column(rect, 0).union(grid.column(rect, 1));
    theme::paint_ellipsized(
        painter,
        reject_text_rect,
        &reject,
        egui::FontId::monospace(METRICS.monitor_channel_font),
        DARK.tone(tone),
    );

    let (gate, gate_color) = match view.halt {
        Some(ExecHalt::Armed) => ("ARMED", DARK.positive),
        _ => (MISSING, DARK.text_secondary),
    };
    paint_number(painter, grid.column(rect, 2), gate, gate_color);
}
