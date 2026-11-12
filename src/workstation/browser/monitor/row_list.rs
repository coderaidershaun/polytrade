//! One scrolling list of monitor rows, shared by the feature band and the channels, so every list
//! scrolls, pins and says it is empty the same way.

use eframe::egui;

use super::pin::Pins;
use crate::workstation::browser::theme::{DARK, METRICS};
use crate::workstation::monitor::view::MonitorDetail;

/// One scrolling band of rows, each with a record that shows on hover and pins on a tap, and a note
/// when there are none. `forced_offset` scrolls once, for a channel that starts following again.
/// The final scroll offset comes back, so the caller can tell whether the list still shows the
/// newest row.
///
/// Rows arrive by index rather than as a slice, so a band that shows twenty rows of a 256-row
/// history builds twenty, not the whole history every frame.
pub(super) struct RowList<'a, S> {
    pub rect: egui::Rect,
    pub pins: &'a mut Pins,
    pub salt: S,
    pub row_count: usize,
    pub empty_note: &'a str,
    pub forced_offset: Option<f32>,
    pub row_spacing: f32,
}

pub(super) fn paint_row_list<S: egui::AsIdSalt>(
    ui: &mut egui::Ui,
    list: RowList<'_, S>,
    detail_of: impl Fn(usize) -> MonitorDetail,
    mut paint_row: impl FnMut(&egui::Painter, egui::Rect, usize),
    mut record_of: impl FnMut(usize) -> String,
) -> f32 {
    let row_h = METRICS.monitor_row_height;
    let RowList {
        rect,
        pins,
        salt,
        row_count,
        empty_note,
        forced_offset,
        row_spacing,
    } = list;
    let builder = egui::UiBuilder::new()
        .max_rect(rect)
        .layout(egui::Layout::top_down(egui::Align::Min));
    ui.scope_builder(builder, |ui| {
        if row_count == 0 {
            paint_empty_note(&ui.painter_at(rect), rect, empty_note);
            return 0.0;
        }
        ui.spacing_mut().item_spacing.y = row_spacing;
        let mut area = egui::ScrollArea::vertical()
            .id_salt(salt)
            .auto_shrink([false, false])
            // Without this a one-row list would still claim a tall area and overhang the band below.
            .min_scrolled_height(0.0);
        if let Some(offset) = forced_offset {
            area = area.vertical_scroll_offset(offset);
        }
        let output = area.show_rows(ui, row_h, row_count, |ui, range| {
            let width = ui.available_width();
            for index in range {
                let (_, row_rect) = ui.allocate_space(egui::vec2(width, row_h));
                pins.interact(ui, row_rect, detail_of(index), || record_of(index));
                paint_row(ui.painter(), row_rect, index);
            }
        });
        output.state.offset.y
    })
    .inner
}

/// An empty band says so in a muted note, rather than showing a made-up row.
fn paint_empty_note(painter: &egui::Painter, rect: egui::Rect, note: &str) {
    painter.text(
        rect.center(),
        egui::Align2::CENTER_CENTER,
        note,
        egui::FontId::proportional(METRICS.monitor_channel_font),
        DARK.text_secondary,
    );
}
