//! The center panel's bands: the status strip, then the account, quote summary, features, channel
//! tabs and the active channel. The strip never scrolls. Below it the bands scroll when the pane is
//! too short to show them whole, and the caller owns the tab, scroll and pin state.

use eframe::egui;

use super::pin::Pins;
use super::row_list::{RowList, paint_row_list};
use super::sizes::{self, FEATURE_ROW_SPACING};
use super::{account, channels, summary};
use crate::msg::persist::FeatureId;
use crate::workstation::browser::theme::{self, DARK, METRICS};
use crate::workstation::monitor::fit::{BandHeights, fit_bands};
use crate::workstation::monitor::view::{
    FeatureRowView, MonitorDetail, MonitorFrame, MonitorUiState, feature_rows,
};
use crate::workstation::text::{feature_hover, feature_name, write_feature_cell};

/// Paints the bands and applies their clicks and taps to `state`.
pub fn paint(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    frame: &MonitorFrame<'_>,
    state: &mut MonitorUiState,
) {
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 0.0, DARK.panel);

    let (strip, viewport) = sizes::split_status(rect);
    painter.rect_filled(strip, 0.0, DARK.panel_raised);
    account::paint_status(ui, strip, frame);

    let heights = fit_bands(
        sizes::pane_height(rect),
        frame.model.monitor().feature_count(),
        &sizes::band_sizes(),
    );
    let mut pins = Pins::new(rect, state);
    let builder = egui::UiBuilder::new()
        .max_rect(viewport)
        .layout(egui::Layout::top_down(egui::Align::Min));
    ui.scope_builder(builder, |ui| {
        // One scroll area in both modes, so the lists below keep their ids and scroll positions
        // when the pane crosses the height at which the bands stop fitting.
        egui::ScrollArea::vertical()
            .id_salt("monitor-bands")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                // When the bands fit, the content is exactly as tall as the viewport, so nothing
                // scrolls.
                let height =
                    if heights.is_scrolled { heights.content_height() } else { viewport.height() };
                let (content, _) = ui.allocate_exact_size(
                    egui::vec2(viewport.width(), height),
                    egui::Sense::hover(),
                );
                let bands = Bands::new(content, &heights);
                bands.paint(ui, frame, state, &mut pins);
            });
    });

    theme::rules_h(&painter, [strip.bottom()], rect.x_range());
    pins.resolve(ui, state);
}

/// The bands under the status strip, top to bottom, cut from one content rect.
struct Bands {
    content: egui::Rect,
    account: egui::Rect,
    summary: egui::Rect,
    features: egui::Rect,
    tabs: egui::Rect,
    channel: egui::Rect,
}

impl Bands {
    fn new(content: egui::Rect, heights: &BandHeights) -> Self {
        let band = |top: f32, height: f32| {
            egui::Rect::from_min_size(
                egui::pos2(content.left(), top),
                egui::vec2(content.width(), height),
            )
        };
        let account = band(content.top(), heights.account);
        let summary = band(account.bottom(), heights.summary);
        let features = band(summary.bottom(), heights.features);
        let tabs = band(features.bottom(), heights.tabs);
        Self {
            content,
            account,
            summary,
            features,
            tabs,
            // The channel runs to the bottom of the content, so rounding never leaves a gap under
            // the last band.
            channel: content.with_min_y(tabs.bottom()),
        }
    }

    fn paint(
        &self,
        ui: &mut egui::Ui,
        frame: &MonitorFrame<'_>,
        state: &mut MonitorUiState,
        pins: &mut Pins,
    ) {
        let painter = ui.painter().clone();
        for raised in [self.account, self.summary, self.tabs] {
            painter.rect_filled(raised, 0.0, DARK.panel_raised);
        }

        account::paint(ui, self.account, frame);
        summary::paint(ui, self.summary, frame, pins);
        paint_feature_list(ui, self.features, frame, pins);
        channels::paint_tabs(ui, self.tabs, state);
        channels::paint_channel(ui, self.channel, frame, state, pins);

        let seams = [
            self.account.bottom(),
            self.summary.bottom(),
            self.features.bottom(),
            self.tabs.bottom(),
        ];
        theme::rules_h(&painter, seams, self.content.x_range());
    }
}

/// The feature list scrolls on its own. A long name is cut with an ellipsis rather than running
/// into its value.
fn paint_feature_list(
    ui: &mut egui::Ui,
    band: egui::Rect,
    frame: &MonitorFrame<'_>,
    pins: &mut Pins,
) {
    let names = frame.feature_names;
    let mut scratch = String::new();
    let row_at = |index: usize| feature_rows(frame.model, frame.instrument).nth(index);
    paint_row_list(
        ui,
        RowList {
            rect: sizes::content_column(band),
            pins,
            salt: "monitor-features",
            row_count: frame.model.monitor().feature_count(),
            empty_note: "no features yet",
            forced_offset: None,
            row_spacing: FEATURE_ROW_SPACING,
        },
        |index| MonitorDetail::Feature(FeatureId(index as u16)),
        |painter, row_rect, index| {
            if let Some(row) = row_at(index) {
                paint_feature_row(painter, row_rect, &row, names, &mut scratch);
            }
        },
        |index| row_at(index).map_or_else(String::new, |row| feature_hover(&row, names)),
    );
}

fn paint_feature_row(
    painter: &egui::Painter,
    rect: egui::Rect,
    row: &FeatureRowView,
    names: &[Box<str>],
    scratch: &mut String,
) {
    if row.changed {
        // A thin bar on the left marks a change without flashing the whole row.
        let width = METRICS.accent_thickness;
        let bar = egui::Rect::from_min_max(
            egui::pos2(rect.left(), rect.top() + width),
            egui::pos2(rect.left() + width, rect.bottom() - width),
        );
        painter.rect_filled(bar, 0.0, DARK.selected);
    }

    let (name_color, value_color) = if row.stale {
        (
            DARK.text_secondary.gamma_multiply(0.65),
            DARK.text_secondary,
        )
    } else {
        (DARK.text_secondary, DARK.text_primary)
    };

    // Room for the widest value, a long negative with four decimals, so a name never runs into it.
    let value_col_w = METRICS.monitor_value_col_w;
    let pad = METRICS.space_2;
    let name_rect = rect
        .with_min_x(rect.left() + pad)
        .with_max_x(rect.right() - value_col_w - pad);
    theme::paint_ellipsized(
        painter,
        name_rect,
        feature_name(row, names),
        egui::FontId::proportional(METRICS.monitor_feature_font),
        name_color,
    );

    write_feature_cell(scratch, row);
    painter.text(
        egui::pos2(rect.right() - pad, theme::crisp(painter, rect.center().y)),
        egui::Align2::RIGHT_CENTER,
        scratch.as_str(),
        egui::FontId::monospace(METRICS.monitor_feature_font),
        if row.value.is_some() { value_color } else { DARK.text_secondary },
    );
}
