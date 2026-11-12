//! Touch pins for the monitor. A tap pins a row, a feature or a delta so its record stays on screen,
//! as hovering shows it with a mouse. Every pinnable widget goes through here, so they all behave
//! alike.

use eframe::egui;

use crate::workstation::browser::theme::{DARK, METRICS};
use crate::workstation::browser::touch::{self, PaneClick};
use crate::workstation::monitor::view::{MonitorDetail, MonitorUiState};

/// How far the popup sits from what it describes.
const POPUP_GAP: f32 = 4.0;

/// One frame's pin work. The widgets report their taps while the bands paint, and the state
/// changes once at the end, so every widget in a frame sees the same pin.
pub(super) struct Pins {
    monitor: egui::Rect,
    pinned: Option<MonitorDetail>,
    tapped: Option<MonitorDetail>,
    popup: Option<egui::Rect>,
}

impl Pins {
    pub(super) fn new(monitor: egui::Rect, state: &MonitorUiState) -> Self {
        Self {
            monitor,
            pinned: state.pinned(),
            tapped: None,
            popup: None,
        }
    }

    /// Makes `rect` a tap target for `detail`. Hovering shows `record` as a tooltip. While the
    /// detail is pinned, a tint marks the widget and the record stays in a popup instead. The
    /// record is built only when it shows, not every frame. Call this before painting the widget's
    /// own content, so the tint sits behind it.
    pub(super) fn interact(
        &mut self,
        ui: &egui::Ui,
        rect: egui::Rect,
        detail: MonitorDetail,
        record: impl FnOnce() -> String,
    ) {
        let response = ui.interact(rect, widget_id(detail), egui::Sense::CLICK);
        if response.clicked() && ui.input(touch::is_finger) {
            self.tapped = Some(detail);
        }
        if self.pinned != Some(detail) {
            response.on_hover_ui(|ui| {
                ui.label(record());
            });
            return;
        }
        // A pinned row that its list or the page has scrolled out of view must not float a popup
        // over whatever is there now.
        if !ui.is_rect_visible(rect) {
            return;
        }
        ui.painter()
            .rect_filled(rect, 0.0, DARK.selected.gamma_multiply(0.18));
        self.popup = self.show_record(ui, &response, &record());
    }

    /// The popup sits on the tooltip layer but is not the tooltip itself, so it leaves the one
    /// tooltip slot free for the hover readouts of other panes. A tap on the record lands on the
    /// record and clears the pin, like any tap elsewhere in the monitor.
    fn show_record(
        &self,
        ui: &egui::Ui,
        response: &egui::Response,
        record: &str,
    ) -> Option<egui::Rect> {
        let width = ui
            .spacing()
            .tooltip_width
            .min(self.monitor.width() - 2.0 * METRICS.space_2);
        let popup = egui::Popup::from_response(response)
            .kind(egui::PopupKind::Tooltip)
            .gap(POPUP_GAP)
            .sense(egui::Sense::hover())
            .width(width);
        // Pick the side against the monitor rather than the whole page, so the popup never covers
        // the ladder or the tab bar beside it and swallows their taps.
        let align = egui::RectAlign::find_best_align(
            [
                egui::RectAlign::BOTTOM_START,
                egui::RectAlign::BOTTOM_END,
                egui::RectAlign::TOP_START,
                egui::RectAlign::TOP_END,
            ]
            .into_iter(),
            self.monitor,
            response.interact_rect,
            POPUP_GAP,
            popup.get_expected_size().unwrap_or(egui::vec2(width, 0.0)),
        )
        .unwrap_or_else(|| self.roomier_side(response.interact_rect));
        popup
            .align(align)
            .align_alternatives(&[])
            .show(|ui| {
                ui.style_mut().interaction.selectable_labels = false;
                ui.set_max_width(width);
                ui.label(record);
            })
            .map(|shown| shown.response.rect)
    }

    /// When no side fits the popup inside the monitor, it still spills over whatever sits beside the
    /// monitor, so it takes the side with more room and spills less.
    fn roomier_side(&self, anchor: egui::Rect) -> egui::RectAlign {
        let below = self.monitor.bottom() - anchor.bottom();
        let above = anchor.top() - self.monitor.top();
        if above > below { egui::RectAlign::TOP_START } else { egui::RectAlign::BOTTOM_START }
    }

    /// Applies the frame's taps. A tap on a pinnable widget pins it, or moves the pin there, or lets
    /// go when it was already pinned. Any other click or tap in the monitor, on its own popup
    /// included, lets go of the pin. A click on a window drawn over the monitor leaves it alone.
    pub(super) fn resolve(self, ui: &egui::Ui, state: &mut MonitorUiState) {
        let before = state.pinned();
        match self.tapped {
            Some(detail) => state.tap(Some(detail)),
            None if self.is_click_inside(ui) => state.tap(None),
            None => {}
        }
        if state.pinned() != before {
            ui.ctx().request_repaint();
        }
    }

    fn is_click_inside(&self, ui: &egui::Ui) -> bool {
        PaneClick::read(ui.ctx(), self.monitor, ui.layer_id()).is_some()
            || ui.input(|input| {
                input.pointer.primary_clicked()
                    && input
                        .pointer
                        .interact_pos()
                        .is_some_and(|at| self.popup.is_some_and(|popup| popup.contains(at)))
            })
    }
}

/// Keyed by what the widget is, not by its place on screen, so a row keeps its id while newer rows
/// push it down and its popup does not blink.
fn widget_id(detail: MonitorDetail) -> egui::Id {
    egui::Id::new(("monitor-pin", detail))
}
