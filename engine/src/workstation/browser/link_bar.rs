//! The one strip about the CONNECTION rather than the market. Spans the full width below the
//! panels because it belongs to the whole workstation, and paints on the waiting screen too — that
//! is the moment an operator most needs to know whether the engine is answering at all.

use core::time::Duration;

use eframe::egui::{self, emath::GuiRounding};

use crate::link::RunState;
use crate::time::DurationUs;

use super::theme::{self, DARK};
use crate::workstation::link_model::LinkStatus;
use crate::workstation::link_view::{ChipKind, LEFT_PAD, LinkBarLayout, RunConfirm};
use crate::workstation::text::{self, Tone};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LinkAction {
    #[default]
    None,
    Assert(RunState),
    NextPeer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkBarFrame<'a> {
    pub status: Option<&'a LinkStatus>,
    /// The page's own clock: time elapsed since it started, never a wall time.
    pub ui_time: DurationUs,
}

const FONT_SIZE: f32 = 11.0;
const CONFIRM_KEY: &str = "run-confirm";
const RUN_BUTTON_KEY: &str = "run-button";
const PEER_CHIP_KEY: &str = "peer-chip";
const PEER_HIT_PAD: usize = 1;

/// Keeps the run confirm in egui's temporary memory under the caller's `ui.id()`, so two bars on
/// one page each need their own `ui.push_id` scope.
pub fn paint(ui: &mut egui::Ui, rect: egui::Rect, status: Option<&LinkStatus>) -> LinkAction {
    let confirm_id = ui.id().with(CONFIRM_KEY);
    let mut confirm = ui
        .data_mut(|data| data.get_temp::<RunConfirm>(confirm_id))
        .unwrap_or_default();
    let frame = LinkBarFrame {
        status,
        ui_time: ui_clock(ui),
    };
    let action = paint_with_confirm(ui, rect, &frame, &mut confirm);
    ui.data_mut(|data| data.insert_temp(confirm_id, confirm));
    action
}

/// Like [`paint`], but the caller holds the confirm and names the clock reading, so a page can show
/// the armed button at a fixed time.
pub fn paint_with_confirm(
    ui: &mut egui::Ui,
    rect: egui::Rect,
    frame: &LinkBarFrame,
    confirm: &mut RunConfirm,
) -> LinkAction {
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 0.0, DARK.panel_raised);
    let action = match frame.status {
        Some(status) => Strip::measure(ui, rect, status).paint(ui, frame.ui_time, confirm),
        None => {
            // A new link session counts from session 0 again, so an arming kept across it could
            // match the new one.
            *confirm = RunConfirm::default();
            paint_no_engine(ui, &painter, rect);
            LinkAction::None
        }
    };
    // Drawn last, because the hover fills of the run button and the peer chip span the full height
    // and would otherwise cover it.
    theme::rules_h(&painter, [rect.top()], rect.x_range());
    action
}

fn ui_clock(ui: &egui::Ui) -> DurationUs {
    let seconds = ui.input(|input| input.time);
    DurationUs::from_micros((seconds * 1e6).round() as i64)
}

fn font() -> egui::FontId {
    egui::FontId::monospace(FONT_SIZE)
}

fn column_advance(ui: &egui::Ui) -> f32 {
    ui.fonts_mut(|fonts| fonts.glyph_width(&font(), '0'))
}

fn paint_no_engine(ui: &egui::Ui, painter: &egui::Painter, rect: egui::Rect) {
    let left = rect.left() + LEFT_PAD as f32 * column_advance(ui);
    theme::text_at(
        painter,
        egui::pos2(left, rect.center().y),
        egui::Align2::LEFT_CENTER,
        text::NO_ENGINE,
        font(),
        DARK.text_secondary,
    );
}

/// A live bar laid out in whole character cells, which is exact because the font is monospace.
struct Strip<'a> {
    rect: egui::Rect,
    advance: f32,
    pixels_per_point: f32,
    status: &'a LinkStatus,
    layout: LinkBarLayout,
}

impl<'a> Strip<'a> {
    fn measure(ui: &egui::Ui, rect: egui::Rect, status: &'a LinkStatus) -> Self {
        let advance = column_advance(ui);
        let columns = (rect.width() / advance).floor() as usize;
        Self {
            rect,
            advance,
            pixels_per_point: ui.pixels_per_point(),
            status,
            layout: LinkBarLayout::fit(status, columns),
        }
    }

    fn x_at(&self, column: usize) -> f32 {
        self.rect.left() + column as f32 * self.advance
    }

    /// Spans the full bar height, so a tap or a click anywhere up the bar lands on it.
    fn button(&self) -> egui::Rect {
        // Rounded to a whole pixel, because a painter clipped to a fractional edge rounds that edge
        // and can cut off the rule drawn on it.
        let edge = self.x_at(self.layout.button_column);
        let left = edge.round_to_pixels(self.pixels_per_point);
        self.rect.with_min_x(left)
    }

    fn paint(&self, ui: &egui::Ui, ui_time: DurationUs, confirm: &mut RunConfirm) -> LinkAction {
        self.paint_chips(ui);
        let run = self.paint_run_button(ui, ui_time, confirm);
        let peer = self.paint_peer_picker(ui);
        if run == LinkAction::None { peer } else { run }
    }

    fn paint_chips(&self, ui: &egui::Ui) {
        let painter = ui.painter_at(self.rect.with_max_x(self.button().left()));
        for chip in &self.layout.chips {
            theme::text_at(
                &painter,
                egui::pos2(self.x_at(chip.column), self.rect.center().y),
                egui::Align2::LEFT_CENTER,
                &chip.text,
                font(),
                DARK.tone(chip.tone),
            );
        }
    }

    fn paint_run_button(
        &self,
        ui: &egui::Ui,
        ui_time: DurationUs,
        confirm: &mut RunConfirm,
    ) -> LinkAction {
        let button = self.button();
        let response = ui.interact(button, ui.id().with(RUN_BUTTON_KEY), egui::Sense::click());
        let sent = if response.clicked() { confirm.press(self.status, ui_time) } else { None };
        let target = self.status.next_assertion();
        let time_left = confirm.refresh(self.status, ui_time);
        let (label, color) = match time_left {
            Some(_) => (text::confirm_label(target), DARK.tone(Tone::Warning)),
            None => (text::button_label(target), DARK.text_primary),
        };
        let painter = ui.painter_at(button);
        painter.rect_filled(
            button,
            0.0,
            if response.hovered() { DARK.panel } else { DARK.panel_raised },
        );
        // Half a physical pixel inside the edge, so the rule lands on the button's first column of
        // pixels and is never clipped away.
        let rule_x = button.left() + theme::one_physical_pixel(&painter) / 2.0;
        theme::rules_v(&painter, [rule_x], button.y_range());
        theme::text_at(
            &painter,
            button.center(),
            egui::Align2::CENTER_CENTER,
            label,
            font(),
            color,
        );
        if let Some(remaining) = time_left {
            let delay = Duration::from_micros(u64::try_from(remaining.micros()).unwrap_or(0));
            ui.ctx().request_repaint_after(delay);
        }
        sent.map_or(LinkAction::None, LinkAction::Assert)
    }

    fn paint_peer_picker(&self, ui: &egui::Ui) -> LinkAction {
        if !text::is_peer_clickable(self.status) {
            return LinkAction::None;
        }
        let Some(peer) = self
            .layout
            .chips
            .iter()
            .find(|chip| chip.kind == ChipKind::Peer)
        else {
            return LinkAction::None;
        };
        let left = self.x_at(peer.column.saturating_sub(PEER_HIT_PAD));
        let right = self
            .x_at(peer.column + peer.text.chars().count() + PEER_HIT_PAD)
            .min(self.button().left());
        let hit = egui::Rect::from_min_max(
            egui::pos2(left, self.rect.top()),
            egui::pos2(right, self.rect.bottom()),
        );
        let response = ui
            .interact(hit, ui.id().with(PEER_CHIP_KEY), egui::Sense::click())
            .on_hover_text(text::PEER_HOVER);
        if response.hovered() {
            // The chips were painted before this fill, so paint the label again on top of it. No
            // accent bar: that bar means selected, and this chip can only be hovered.
            let painter = ui.painter_at(hit);
            painter.rect_filled(hit, 0.0, DARK.panel);
            theme::text_at(
                &painter,
                egui::pos2(self.x_at(peer.column), hit.center().y),
                egui::Align2::LEFT_CENTER,
                &peer.text,
                font(),
                DARK.text_primary,
            );
        }
        if response.clicked() { LinkAction::NextPeer } else { LinkAction::None }
    }
}
