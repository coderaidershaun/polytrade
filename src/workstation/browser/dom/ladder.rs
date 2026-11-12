//! The ladder, painted by hand in three mirrored columns: asks read strategy, price, public and bids
//! read public, price, strategy. The strategy column shows real orders or desired quotes and never
//! merges the two.

use std::sync::Arc;

use eframe::egui;

use super::DomFrame;
use super::geometry::{ASK_COLUMNS, BID_COLUMNS, DomLayout};
use super::overlay::OverlayPainter;
use super::pin;
use crate::ids::Qty;
use crate::workstation::browser::theme::{self, DARK, METRICS};
use crate::workstation::dom_view::{
    DomRow, DomStatus, DomView, DomViewInput, StrategyCell, build_dom_view,
};
use crate::workstation::exec_model::OrderStatus;
use crate::workstation::format;
use crate::workstation::text::{self, Ring};

/// Floor on the row-rule fade: below it the ladder loses its structure rather than its noise.
const MIN_GRID_FADE: f32 = 0.35;

/// Text that does not fit its cell shrinks in steps of this size. The font cache keeps separate
/// glyphs for every size, so fixed steps stop a resize from adding new glyphs on every frame.
const FONT_STEP: f32 = 0.5;

pub fn paint(ui: &mut egui::Ui, rect: egui::Rect, frame: &DomFrame<'_>) -> egui::Response {
    let response = ui.interact(rect, egui::Id::new("polysim-dom"), egui::Sense::hover());
    let painter = ui.painter().with_clip_rect(rect);
    let layout = DomLayout::new(rect, frame.levels);
    let view = build_dom_view(DomViewInput {
        snapshot: frame.snapshot,
        overlay: frame.overlay,
        tick: frame.tick,
        grouping: frame.grouping,
        rows_per_side: layout.rows_per_side,
        feed: frame.feed,
    });

    painter.rect_filled(rect, 0.0, DARK.panel);
    paint_grid(&painter, &layout, rect);
    let mut ladder = LadderPainter {
        painter: &painter,
        layout: &layout,
        frame,
        status: view.status,
        scratch: String::new(),
    };
    ladder.rows(&view);
    paint_quote_rings(&painter, &layout, &view);
    ladder.separator(&view);

    let overlay = OverlayPainter {
        painter: &painter,
        layout: &layout,
        view: &view,
        frame,
    };
    let top_chevron_bottom = overlay.paint_offscreen();
    let pinned = pin::pinned_point(&response, &layout, &view);
    overlay.paint_hover(response.hover_pos().or(pinned));
    overlay.paint_status(top_chevron_bottom);

    response
}

fn paint_grid(painter: &egui::Painter, layout: &DomLayout, body: egui::Rect) {
    // A ladder narrower than its body gets a rule on both outer edges, so it reads as one block.
    let is_inset = layout.rect.width() < body.width();
    let rules = if is_inset { 0..=3 } else { 1..=2 };
    let columns = rules.map(|column| layout.rect.left() + column as f32 * layout.column_width);
    theme::rules_v(painter, columns, layout.rect.y_range());
    // A ladder at the height floor rules twice the lines one at the reference row does; at full
    // strength that many read as hatching, so the row rules fade as the rows compress.
    let rows = theme::hairline(
        painter,
        DARK.border.gamma_multiply(layout.scale.max(MIN_GRID_FADE)),
    );
    for index in 0..=layout.rows_per_side {
        let above = layout.separator.top() - index as f32 * layout.row_height;
        let below = layout.separator.bottom() + index as f32 * layout.row_height;
        painter.hline(layout.rect.x_range(), theme::crisp(painter, above), rows);
        painter.hline(layout.rect.x_range(), theme::crisp(painter, below), rows);
    }
}

/// Everything that writes a number into the ladder shares a painter, a geometry and ONE string
/// buffer, so these are methods rather than free functions each re-taking the lot.
struct LadderPainter<'a> {
    painter: &'a egui::Painter,
    layout: &'a DomLayout,
    frame: &'a DomFrame<'a>,
    status: DomStatus,
    scratch: String,
}

impl LadderPainter<'_> {
    fn rows(&mut self, view: &DomView) {
        for (index, row) in view.ask_rows().iter().enumerate() {
            let rect = self.layout.ask_row(index);
            let public = self.layout.cell(rect, ASK_COLUMNS.public);
            let strategy = self.layout.cell(rect, ASK_COLUMNS.strategy);
            self.price(rect, row.tick_index);
            self.public(public, row, at_status(DARK.ask, self.status), Edge::Right);
            self.strategy(strategy, row, Edge::Left);
        }
        for (index, row) in view.bid_rows().iter().enumerate() {
            let rect = self.layout.bid_row(index);
            let public = self.layout.cell(rect, BID_COLUMNS.public);
            let strategy = self.layout.cell(rect, BID_COLUMNS.strategy);
            self.price(rect, row.tick_index);
            self.public(public, row, at_status(DARK.bid, self.status), Edge::Left);
            self.strategy(strategy, row, Edge::Right);
        }
    }

    fn price(&mut self, row_rect: egui::Rect, tick_index: i64) {
        self.frame.write_row_price(&mut self.scratch, tick_index);
        let cell = self.layout.cell(row_rect, 1);
        let color = at_status(DARK.text_primary, self.status);
        let galley = self.fitted(self.layout.fonts.price, cell, color);
        self.put(galley, cell, (cell.center().x, egui::Align2::CENTER_CENTER));
    }

    fn public(&mut self, cell: egui::Rect, row: &DomRow, color: egui::Color32, edge: Edge) {
        let Some(qty) = row.public_qty else { return };
        self.write_qty(qty);
        self.edge_qty(cell, color, edge);
    }

    fn strategy(&mut self, cell: egui::Rect, row: &DomRow, edge: Edge) {
        let (qty, word, color) = match row.strategy_cell() {
            Some(StrategyCell::Order { qty, status }) => {
                (qty, status.word(), DARK.tone(text::order_tone(status)))
            }
            Some(StrategyCell::Desired { qty }) => (qty, "want", DARK.selected.gamma_multiply(0.6)),
            None => return,
        };
        let color = at_status(color, self.status);
        self.write_qty(qty);
        let qty_width = self.edge_qty(cell, color, edge);
        let Some(size) = self.layout.fonts.tag else { return };
        let tag =
            self.painter
                .layout_no_wrap(word.to_owned(), egui::FontId::proportional(size), color);
        // The colour and the quote ring still show the status when the tag does not fit.
        if qty_width + METRICS.space_1 + tag.size().x > room_of(cell) {
            return;
        }
        // The tag sits at the cell's far edge, opposite its quantity.
        self.put(tag, cell, edge.flip().anchor(cell));
    }

    fn separator(&mut self, view: &DomView) {
        let separator = self.layout.separator;
        self.painter.rect_filled(separator, 0.0, DARK.panel_raised);
        let border = egui::Stroke::new(2.0, DARK.focus);
        for y in [separator.top(), separator.bottom()] {
            self.painter
                .hline(separator.x_range(), theme::crisp(self.painter, y), border);
        }

        let label = self.painter.layout_no_wrap(
            "MID".to_owned(),
            egui::FontId::proportional(METRICS.dom_tag_font),
            DARK.text_secondary,
        );
        // The mid is centred, so it leaves the label's width free on both sides.
        let mid_cell = separator.shrink2(egui::vec2(METRICS.dom_cell_pad + label.size().x, 0.0));
        format::write_opt_venue_mid(&mut self.scratch, view.mid_half_ticks, self.frame.tick);
        let color = DARK.tone(text::mid_tone(view.mid_half_ticks));
        let mid = self.fitted(METRICS.dom_mid_font, mid_cell, color);
        self.put(
            mid,
            mid_cell,
            (mid_cell.center().x, egui::Align2::CENTER_CENTER),
        );
        let label_anchor = (
            separator.left() + METRICS.dom_cell_pad,
            egui::Align2::LEFT_CENTER,
        );
        self.put(label, separator, label_anchor);
    }

    fn write_qty(&mut self, qty: Qty) {
        format::write_qty(
            &mut self.scratch,
            qty,
            self.frame.qty_scale,
            self.frame.qty_decimals,
        );
    }

    /// Paints the quantity in the scratch buffer at `edge` of `cell` and returns the width it took.
    fn edge_qty(&self, cell: egui::Rect, color: egui::Color32, edge: Edge) -> f32 {
        let galley = self.fitted(self.layout.fonts.qty, cell, color);
        let width = galley.size().x;
        self.put(galley, cell, edge.anchor(cell));
        width
    }

    /// Lays out the scratch text in monospace at `size`, or at the largest smaller step that fits
    /// between `cell`'s paddings. It never goes below the legibility floor, so the text it returns
    /// can still be too wide.
    fn fitted(&self, size: f32, cell: egui::Rect, color: egui::Color32) -> Arc<egui::Galley> {
        let room = room_of(cell);
        let lay_out = |size: f32| {
            self.painter
                .layout_no_wrap(self.scratch.clone(), egui::FontId::monospace(size), color)
        };
        let galley = lay_out(size);
        let width = galley.size().x;
        if width <= room {
            return galley;
        }
        let mut size = (size * room / width / FONT_STEP).floor() * FONT_STEP;
        loop {
            size = size.max(METRICS.dom_min_font);
            let galley = lay_out(size);
            if galley.size().x <= room || size <= METRICS.dom_min_font {
                return galley;
            }
            size -= FONT_STEP;
        }
    }

    /// Paints `galley` at `anchor`, centred vertically on `cell`.
    fn put(&self, galley: Arc<egui::Galley>, cell: egui::Rect, anchor: (f32, egui::Align2)) {
        let (x, align) = anchor;
        let y = theme::crisp(self.painter, cell.center().y);
        if galley.size().x <= room_of(cell) {
            let rect = align.anchor_size(egui::pos2(x, y), galley.size());
            self.painter
                .galley(rect.min, galley, egui::Color32::PLACEHOLDER);
            return;
        }
        // The text is too wide even at the smallest font. Keep the leading digits and cut the end
        // at the cell edge, because a number without its first digits reads as a much smaller one.
        let start = egui::pos2(
            cell.left() + METRICS.dom_cell_pad,
            y - galley.size().y / 2.0,
        );
        let clip = egui::Rect::from_x_y_ranges(cell.x_range(), self.painter.clip_rect().y_range());
        self.painter
            .with_clip_rect(clip)
            .galley(start, galley, egui::Color32::PLACEHOLDER);
    }
}

fn room_of(cell: egui::Rect) -> f32 {
    (cell.width() - 2.0 * METRICS.dom_cell_pad).max(0.0)
}

#[derive(Clone, Copy)]
enum Edge {
    Left,
    Right,
}

impl Edge {
    fn flip(self) -> Self {
        match self {
            Self::Left => Self::Right,
            Self::Right => Self::Left,
        }
    }

    fn anchor(self, cell: egui::Rect) -> (f32, egui::Align2) {
        match self {
            Self::Left => (
                cell.left() + METRICS.dom_cell_pad,
                egui::Align2::LEFT_CENTER,
            ),
            Self::Right => (
                cell.right() - METRICS.dom_cell_pad,
                egui::Align2::RIGHT_CENTER,
            ),
        }
    }
}

fn paint_quote_rings(painter: &egui::Painter, layout: &DomLayout, view: &DomView) {
    for (index, row) in view.ask_rows().iter().enumerate() {
        let cell = layout.cell(layout.ask_row(index), 1);
        ring_for(painter, layout, cell, row, view.status);
    }
    for (index, row) in view.bid_rows().iter().enumerate() {
        let cell = layout.cell(layout.bid_row(index), 1);
        ring_for(painter, layout, cell, row, view.status);
    }
}

fn ring_for(
    painter: &egui::Painter,
    layout: &DomLayout,
    cell: egui::Rect,
    row: &DomRow,
    status: DomStatus,
) {
    let (width, color) = match text::ring(row, status) {
        Some(Ring::Confirmed) => (2.0, DARK.selected),
        Some(Ring::InFlight) => (1.0, DARK.tone(text::order_tone(OrderStatus::InFlight))),
        Some(Ring::Lost) => (1.0, DARK.tone(text::order_tone(OrderStatus::Lost))),
        Some(Ring::Desired) => (1.0, DARK.selected.gamma_multiply(0.6)),
        None => return,
    };
    // The ring carries status alone once the tags drop out, so it thins with the row rather than
    // swallowing a compressed cell — never below the one pixel that keeps it on screen at all.
    let width = (width * layout.scale).max(theme::one_physical_pixel(painter));
    painter.rect_stroke(
        cell,
        0.0,
        egui::Stroke::new(width, color),
        egui::StrokeKind::Inside,
    );
}

/// A ladder that is not live paints muted, so an operator reading it in peripheral vision sees it
/// is not current before reading a single number.
fn at_status(color: egui::Color32, status: DomStatus) -> egui::Color32 {
    match status {
        DomStatus::Live => color,
        DomStatus::Stale | DomStatus::AwaitingBook | DomStatus::Disconnected => {
            color.gamma_multiply(0.42)
        }
    }
}
