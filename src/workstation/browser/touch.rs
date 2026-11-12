//! Telling a finger from a mouse. Only a finger's tap pins a readout, because a mouse already
//! shows every readout on hover, and any mouse click in a pane lets go of that pane's pin.

use eframe::egui;
use js_sys::Reflect;

/// A primary click that landed in one pane this frame, and what made it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum PaneClick {
    Finger(egui::Pos2),
    Mouse,
}

impl PaneClick {
    /// This frame's primary click when it landed inside `pane` and on `layer`. A click on a popup
    /// or a window drawn over the pane belongs to that window, so it gives `None`.
    pub(super) fn read(
        ctx: &egui::Context,
        pane: egui::Rect,
        layer: egui::LayerId,
    ) -> Option<Self> {
        let (at, is_finger) = ctx.input(|input| {
            if !input.pointer.primary_clicked() {
                return None;
            }
            Some((input.pointer.interact_pos()?, is_finger(input)))
        })?;
        if !pane.contains(at) || ctx.layer_id_at(at) != Some(layer) {
            return None;
        }
        Some(if is_finger { Self::Finger(at) } else { Self::Mouse })
    }
}

/// The browser registers a tap's click in the frame of its pointer release, which still holds the
/// touch. A mouse click's frame holds none. A tap that starts and ends inside one frame still
/// leaves its touch events in that frame.
pub(super) fn is_finger(input: &egui::InputState) -> bool {
    input.any_touches()
        || input
            .events
            .iter()
            .any(|event| matches!(event, egui::Event::Touch { .. }))
}

/// Tells egui, on its first pass, that this device has a touch screen. egui scrolls a list by
/// dragging only once it has seen a touch event, so without this the first drag after load does
/// nothing. A device with no touch points is left alone, and a mouse there never drags a list.
/// The announcement goes before the pass's own events, so it cannot end a real first touch that
/// shares its id.
pub fn announce_touch_screen(ctx: &egui::Context, raw_input: &mut egui::RawInput) {
    if ctx.cumulative_pass_nr() != 0 || !has_touch_points() {
        return;
    }
    let announcement = egui::Event::Touch {
        device_id: egui::TouchDeviceId(0),
        id: egui::TouchId(0),
        phase: egui::TouchPhase::Cancel,
        pos: egui::Pos2::ZERO,
        force: None,
    };
    raw_input.events.insert(0, announcement);
}

fn has_touch_points() -> bool {
    let Some(window) = web_sys::window() else { return false };
    let points = Reflect::get(&window, &"navigator".into())
        .and_then(|navigator| Reflect::get(&navigator, &"maxTouchPoints".into()));
    points
        .ok()
        .and_then(|points| points.as_f64())
        .is_some_and(|points| points > 0.0)
}
