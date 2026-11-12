//! What the ladder paints over its rows: the chevrons for orders off screen, the hover readout and
//! the status badge. None of it ever reorders the rows.

use eframe::egui;

use super::DomFrame;
use super::geometry::{ASK_COLUMNS, BID_COLUMNS, DomLayout};
use crate::workstation::browser::theme::{self, DARK, Direction, METRICS};
use crate::workstation::dom_view::{DomRow, DomView, QuotePlacement};
use crate::workstation::format;
use crate::workstation::text;

pub(super) struct OverlayPainter<'a> {
    pub(super) painter: &'a egui::Painter,
    pub(super) layout: &'a DomLayout,
    pub(super) view: &'a DomView,
    pub(super) frame: &'a DomFrame<'a>,
}

impl OverlayPainter<'_> {
    /// Returns the bottom edge of the chevron at the top of the ladder, if one was painted, so the
    /// status badge can sit below it instead of on it.
    pub(super) fn paint_offscreen(&self) -> Option<f32> {
        let view = self.view;
        let ask = text::offscreen_of(view.ask_order_placement, view.ask_placement);
        let bid = text::offscreen_of(view.bid_order_placement, view.bid_placement);
        let mut top_chevron_bottom = None;
        for placement in [ask, bid] {
            match placement {
                QuotePlacement::OffScreenAbove { delta_half_ticks } => {
                    let pill = chevron(self.painter, self.layout, Direction::Up, delta_half_ticks);
                    top_chevron_bottom = Some(pill.bottom());
                }
                QuotePlacement::OffScreenBelow { delta_half_ticks } => {
                    chevron(self.painter, self.layout, Direction::Down, delta_half_ticks);
                }
                QuotePlacement::Visible | QuotePlacement::None => {}
            }
        }
        top_chevron_bottom
    }

    pub(super) fn paint_hover(&self, pointer: Option<egui::Pos2>) {
        let Some(pointer) = pointer else { return };
        let Some((row_rect, row)) = self.layout.row_at(self.view, pointer) else { return };
        self.painter
            .rect_filled(row_rect, 0.0, DARK.selected.gamma_multiply(0.12));
        let detail = row_detail(row, self.frame);
        readout(self.painter, self.layout, pointer, &detail);
    }

    pub(super) fn paint_status(&self, top_chevron_bottom: Option<f32>) {
        let Some((label, tone)) = text::status_pill(self.view.status, self.frame.stale_age) else {
            return;
        };
        let color = DARK.tone(tone);
        let galley = self.painter.layout_no_wrap(
            label,
            egui::FontId::proportional(METRICS.dom_qty_font),
            color,
        );
        let pad = METRICS.dom_cell_pad;
        let size = egui::vec2(galley.size().x + 2.0 * pad, galley.size().y + pad);
        let top = top_chevron_bottom.map_or(self.layout.rect.top() + size.y / 2.0, |bottom| {
            bottom + METRICS.space_1
        });
        let left = self.layout.rect.center().x - size.x / 2.0;
        let pill = egui::Rect::from_min_size(egui::pos2(left, top), size);
        theme::fill_pill(self.painter, pill, color);
        self.painter
            .galley(pill.min + egui::vec2(pad, pad / 2.0), galley, color);
    }
}

fn chevron(
    painter: &egui::Painter,
    layout: &DomLayout,
    direction: Direction,
    delta_half_ticks: i64,
) -> egui::Rect {
    // The triangle beside the number is what says "delta": every `format::write_*` opens by clearing
    // the buffer, so a prefix written here would be discarded.
    let label = format::render(|out| format::write_mid(out, delta_half_ticks));
    let galley = painter.layout_no_wrap(
        label,
        egui::FontId::proportional(METRICS.dom_tag_font + 1.0),
        DARK.focus,
    );

    let arrow = 9.0;
    let pad = METRICS.dom_cell_pad;
    let content_width = arrow + METRICS.space_1 + galley.size().x + 2.0 * pad;
    let height = layout.row_height.clamp(16.0, 22.0);
    // The top chip covers the public size of the top ask row, and the bottom chip that of the
    // bottom bid row, so neither hides a price or the strategy's own orders.
    let column = match direction {
        Direction::Up => ASK_COLUMNS.public,
        Direction::Down => BID_COLUMNS.public,
    };
    let cell = layout.cell(layout.rect, column);
    // The pill covers that size completely, so no part of the number shows beside it.
    let width = content_width.max(cell.width() - 2.0 * METRICS.space_1);
    let center_x = cell.center().x;
    let center_y = match direction {
        Direction::Up => layout.rect.top() + height / 2.0 + METRICS.space_1,
        Direction::Down => layout.rect.bottom() - height / 2.0 - METRICS.space_1,
    };
    let pill = theme::clamp_rect(
        layout.rect,
        egui::Rect::from_center_size(egui::pos2(center_x, center_y), egui::vec2(width, height)),
    );
    theme::fill_pill(painter, pill, DARK.focus);

    let arrow_center = egui::pos2(pill.left() + pad + arrow / 2.0, pill.center().y);
    let arrow_shape = theme::triangle(arrow_center, arrow, direction, DARK.selected);
    painter.add(arrow_shape);
    painter.galley(
        egui::pos2(
            arrow_center.x + arrow / 2.0 + METRICS.space_1,
            pill.center().y - galley.size().y / 2.0,
        ),
        galley,
        DARK.focus,
    );
    pill
}

struct RowDetail {
    price: String,
    public: String,
    order: String,
    desired: String,
}

fn row_detail(row: &DomRow, frame: &DomFrame<'_>) -> RowDetail {
    let price = format::render(|out| frame.write_row_price(out, row.tick_index));
    let public = format::render(|out| {
        format::write_opt_qty(out, row.public_qty, frame.qty_scale, frame.qty_decimals);
    });
    let mut order = format::render(|out| {
        format::write_opt_qty(out, row.order_qty, frame.qty_scale, frame.qty_decimals);
    });
    if let Some(status) = row.order_status {
        order.push_str("  ");
        order.push_str(status.word());
    }
    let desired = format::render(|out| {
        format::write_opt_qty(out, row.strategy_qty, frame.qty_scale, frame.qty_decimals);
    });
    RowDetail {
        price,
        public,
        order,
        desired,
    }
}

fn readout(painter: &egui::Painter, layout: &DomLayout, pointer: egui::Pos2, detail: &RowDetail) {
    let font = egui::FontId::monospace(METRICS.dom_tag_font + 2.0);
    let lines = [
        format!("px    {}", detail.price),
        format!("pub   {}", detail.public),
        format!("order {}", detail.order),
        format!("want  {}", detail.desired),
    ];
    let galleys: Vec<_> = lines
        .into_iter()
        .map(|line| painter.layout_no_wrap(line, font.clone(), DARK.text_primary))
        .collect();

    let pad = METRICS.dom_cell_pad;
    let line_h = galleys[0].size().y;
    let width = galleys.iter().map(|g| g.size().x).fold(0.0_f32, f32::max) + 2.0 * pad;
    let height = line_h * galleys.len() as f32 + 2.0 * pad;
    let anchor = egui::pos2(pointer.x + METRICS.space_2, pointer.y + METRICS.space_2);
    let clamped = theme::clamp_rect(
        layout.rect,
        egui::Rect::from_min_size(anchor, egui::vec2(width, height)),
    );

    theme::fill_pill(painter, clamped, DARK.border);
    for (index, galley) in galleys.into_iter().enumerate() {
        painter.galley(
            egui::pos2(
                clamped.left() + pad,
                clamped.top() + pad + index as f32 * line_h,
            ),
            galley,
            DARK.text_primary,
        );
    }
}
