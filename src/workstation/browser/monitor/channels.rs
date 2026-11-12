//! The monitor's tab strip and the active channel's rows, newest first. The pane's width picks the
//! columns a row shows, and each sits at an anchor taken from the row's own rect, so the numbers
//! never shift while the rows scroll.

use eframe::egui;

use super::pin::Pins;
use super::row_list::{RowList, paint_row_list};
use super::sizes::{column_widths, content_column};
use crate::workstation::browser::controls;
use crate::workstation::browser::theme::{self, DARK, Direction, METRICS};
use crate::workstation::monitor::fit::{ColumnPlan, ColumnWidths, TimeColumn, fit_columns};
use crate::workstation::monitor::model::{FillRow, OrderRow, SystemRow, TradeRow};
use crate::workstation::monitor::view::{Channel, MonitorDetail, MonitorFrame, MonitorUiState};
use crate::workstation::shell::TOUCH_HEIGHT;
use crate::workstation::text::{Cell, ChannelRow, TagColumn, record_row};

/// A list scrolled less than this from the top still shows the newest row, which absorbs sub-pixel
/// jitter.
const FOLLOW_EPS: f32 = 1.0;

/// A finger cannot reliably hit the small pill, so on a touch screen the badge answers taps in a
/// target as tall as any touch target and this much wider than the pill on each side.
const BADGE_TOUCH_MARGIN: f32 = 8.0;

const SHORT_TIME_LEN: usize = "HH:MM:SS".len();

/// One equal cell per channel, drawn in the centred column of `band`, so the strip never wraps or
/// overflows. The caller fills the band.
pub(super) fn paint_tabs(ui: &mut egui::Ui, band: egui::Rect, state: &mut MonitorUiState) {
    let rect = content_column(band);
    let painter = ui.painter_at(rect);
    let cell_w = rect.width() / Channel::ALL.len() as f32;

    for (index, channel) in Channel::ALL.iter().enumerate() {
        let active = *channel == state.active_tab;
        let cell = egui::Rect::from_min_size(
            egui::pos2(rect.left() + index as f32 * cell_w, rect.top()),
            egui::vec2(cell_w, rect.height()),
        );
        let response = ui.interact(
            cell,
            egui::Id::new(("monitor-tab", index)),
            egui::Sense::click(),
        );

        if active {
            controls::fill_selected(&painter, cell);
        } else if response.hovered() {
            painter.rect_filled(cell, 0.0, DARK.panel);
        }
        painter.text(
            cell.center(),
            egui::Align2::CENTER_CENTER,
            channel.label(),
            egui::FontId::proportional(METRICS.monitor_tab_font),
            if active { DARK.text_primary } else { DARK.text_secondary },
        );

        if response.clicked() {
            state.active_tab = *channel;
        }
    }

    let dividers = (1..Channel::ALL.len()).map(|boundary| rect.left() + boundary as f32 * cell_w);
    theme::rules_v(&painter, dividers, rect.y_range());
}

/// The active channel's rows, newest first, in the centred column of `band`. Switching tabs clears
/// nothing.
pub(super) fn paint_channel(
    ui: &mut egui::Ui,
    band: egui::Rect,
    frame: &MonitorFrame<'_>,
    state: &mut MonitorUiState,
    pins: &mut Pins,
) {
    match state.active_tab {
        Channel::System => render_channel::<SystemRow>(ui, band, frame, state, pins),
        Channel::Orders => render_channel::<OrderRow>(ui, band, frame, state, pins),
        Channel::PubTrades => render_channel::<TradeRow>(ui, band, frame, state, pins),
        Channel::TradeFills => render_channel::<FillRow>(ui, band, frame, state, pins),
    }
}

/// A row's cells drive both the paint and the hover record, so the two cannot differ.
fn render_channel<R: ChannelRow>(
    ui: &mut egui::Ui,
    band: egui::Rect,
    frame: &MonitorFrame<'_>,
    state: &mut MonitorUiState,
    pins: &mut Pins,
) {
    let rect = content_column(band);
    let appended = R::appended(frame);
    let slot = state.channel(R::CHANNEL);
    let forced = slot.take_pending_offset();
    let unseen_count = if slot.is_following { 0 } else { appended.saturating_sub(slot.seen) };
    if slot.is_following {
        slot.seen = appended;
    }

    let widths = column_widths();
    let plan = fit_columns(R::CHANNEL, band.width(), &widths);
    let mut cells: Vec<Cell> = Vec::new();
    let offset = paint_row_list(
        ui,
        RowList {
            rect,
            pins,
            salt: ("monitor-channel", R::CHANNEL.index()),
            row_count: R::count(frame),
            empty_note: R::EMPTY_NOTE,
            forced_offset: forced,
            row_spacing: 0.0,
        },
        // A row is named by its place in the append order, not on screen, so it keeps its pin as
        // newer rows arrive above it.
        |index| MonitorDetail::Row {
            channel: R::CHANNEL,
            ordinal: appended.saturating_sub(index as u64 + 1),
        },
        |painter, row_rect, index| {
            let Some(row) = R::at(frame, index) else { return };
            cells.clear();
            row.cells(frame, &mut cells);
            let columns = RowColumns::new(painter, row_rect, plan, &widths);
            columns.paint_cells(painter, row_rect, &cells);
        },
        |index| R::at(frame, index).map_or_else(String::new, |row| record_row(&row, frame)),
    );
    state.channel(R::CHANNEL).is_following = offset <= FOLLOW_EPS;

    if unseen_count > 0 {
        paint_unseen_badge(ui, rect, R::CHANNEL, unseen_count, state);
    }
}

/// A pill with the count of rows not yet seen. Clicking it jumps to the newest row. New rows never
/// pull a scrolled-away list there on their own.
fn paint_unseen_badge(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    channel: Channel,
    count: u64,
    state: &mut MonitorUiState,
) {
    let text = format!("{count} new");
    let font = egui::FontId::proportional(METRICS.monitor_badge_font);
    let galley = ui.painter().layout_no_wrap(text, font, DARK.text_primary);
    let galley_size = galley.size();
    let marker_size = METRICS.monitor_badge_font;
    let content = egui::vec2(marker_size + METRICS.space_1 + galley_size.x, galley_size.y);
    let size = content + egui::vec2(METRICS.space_2 * 2.0, METRICS.space_1);
    let badge = egui::Rect::from_min_size(
        egui::pos2(rect.center().x - size.x / 2.0, rect.top() + METRICS.space_1),
        size,
    );

    let response = ui.interact(
        badge_target(ui, badge, rect),
        egui::Id::new(("monitor-unseen", channel.index())),
        egui::Sense::click(),
    );
    let painter = ui.painter_at(badge);
    let fill = if response.hovered() { DARK.selected } else { DARK.selected.gamma_multiply(0.85) };
    painter.rect_filled(badge, METRICS.corner_radius, fill);

    let marker_center = egui::pos2(
        badge.left() + METRICS.space_2 + marker_size / 2.0,
        badge.center().y,
    );
    let marker = theme::triangle(marker_center, marker_size, Direction::Up, DARK.text_primary);
    painter.add(marker);
    painter.galley(
        egui::pos2(
            marker_center.x + marker_size / 2.0 + METRICS.space_1,
            badge.center().y - galley_size.y / 2.0,
        ),
        galley,
        DARK.text_primary,
    );

    if response.on_hover_text("jump to newest").clicked() {
        state.channel(channel).resume_follow();
    }
}

/// With a mouse the badge answers on its own pill. On a touch screen its target starts at the
/// channel's top edge and never above it, so it cannot catch a tap meant for a tab.
fn badge_target(ui: &egui::Ui, badge: egui::Rect, channel: egui::Rect) -> egui::Rect {
    if !ui.input(|input| input.has_touch_screen()) {
        return badge;
    }
    egui::Rect::from_min_max(
        egui::pos2(badge.left() - BADGE_TOUCH_MARGIN, channel.top()),
        egui::pos2(
            badge.right() + BADGE_TOUCH_MARGIN,
            (channel.top() + TOUCH_HEIGHT).max(badge.bottom()),
        ),
    )
    .intersect(channel)
}

fn mono_font() -> egui::FontId {
    egui::FontId::monospace(METRICS.monitor_channel_font)
}

fn text_font() -> egui::FontId {
    egui::FontId::proportional(METRICS.monitor_channel_font)
}

/// The column anchors and the text baseline, taken from the row's rect and the plan so the columns
/// never move.
struct RowColumns {
    plan: ColumnPlan,
    widths: ColumnWidths,
    time_x: f32,
    tag_x: f32,
    price_x: f32,
    qty_x: f32,
    fee_x: f32,
    y: f32,
}

impl RowColumns {
    fn new(
        painter: &egui::Painter,
        rect: egui::Rect,
        plan: ColumnPlan,
        widths: &ColumnWidths,
    ) -> Self {
        let left = rect.left() + widths.pad;
        let right = rect.right() - widths.pad;
        let fee_w = plan.fee_width(widths);
        Self {
            plan,
            widths: *widths,
            time_x: left,
            tag_x: left + plan.time_width(widths),
            price_x: right - fee_w - widths.qty,
            qty_x: right - fee_w,
            fee_x: right,
            y: theme::crisp(painter, rect.center().y),
        }
    }

    fn paint_cells(&self, painter: &egui::Painter, rect: egui::Rect, cells: &[Cell]) {
        let mut description_offset = 0.0;
        for cell in cells {
            match cell {
                Cell::Time(text) => self.paint_time(painter, text),
                Cell::Tag(tag) => {
                    self.paint_tag(painter, tag);
                    description_offset = self.widths.side;
                }
                Cell::Description { text, tone } => {
                    let room = rect
                        .with_min_x(self.tag_x + description_offset)
                        .with_max_x(self.qty_x);
                    theme::paint_ellipsized(painter, room, text, text_font(), DARK.tone(*tone));
                }
                Cell::Price(text) => self.paint_number(painter, self.price_x, text),
                Cell::Qty(text) => self.paint_number(painter, self.qty_x, text),
                Cell::Detail(text) => self.paint_fee(painter, text),
            }
        }
    }

    /// Liquidity note first when the plan keeps it, and the side word one tag width right of it.
    fn paint_tag(&self, painter: &egui::Painter, tag: &TagColumn) {
        let mut x = self.tag_x;
        if let Some(liquidity) = tag.liquidity.filter(|_| self.plan.has_liquidity) {
            painter.text(
                egui::pos2(x, self.y),
                egui::Align2::LEFT_CENTER,
                liquidity,
                text_font(),
                DARK.text_secondary,
            );
            x += self.widths.liquidity;
        }
        painter.text(
            egui::pos2(x, self.y),
            egui::Align2::LEFT_CENTER,
            tag.side,
            mono_font(),
            DARK.tone(tag.tone),
        );
    }

    fn paint_time(&self, painter: &egui::Painter, text: &str) {
        let shown = match self.plan.time {
            TimeColumn::Full => text,
            TimeColumn::Short => text.get(..SHORT_TIME_LEN).unwrap_or(text),
            TimeColumn::Hidden => return,
        };
        painter.text(
            egui::pos2(self.time_x, self.y),
            egui::Align2::LEFT_CENTER,
            shown,
            mono_font(),
            DARK.text_secondary,
        );
    }

    fn paint_number(&self, painter: &egui::Painter, x: f32, text: &str) {
        painter.text(
            egui::pos2(x, self.y),
            egui::Align2::RIGHT_CENTER,
            text,
            mono_font(),
            DARK.text_primary,
        );
    }

    fn paint_fee(&self, painter: &egui::Painter, text: &str) {
        if !self.plan.has_fee {
            return;
        }
        painter.text(
            egui::pos2(self.fee_x, self.y),
            egui::Align2::RIGHT_CENTER,
            text,
            mono_font(),
            DARK.text_secondary,
        );
    }
}
