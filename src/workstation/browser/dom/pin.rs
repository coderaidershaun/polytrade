//! Touch has no hover, so a finger tap on a row pins the readout there. While a pin is set, a tap on
//! another row moves it, and a second tap on the same row, a tap on no row, or a mouse click in the
//! ladder lets go. A mouse click never pins: hovering already shows the readout.

use eframe::egui;

use super::geometry::DomLayout;
use crate::workstation::browser::touch::PaneClick;
use crate::workstation::dom_view::DomView;

/// A pin marks a spot on the screen, not a price level. As the book moves, the readout shows
/// whatever row is under that spot, as it would under a resting mouse. A resize, a moved pane or a
/// single frame without the ladder drops the pin.
#[derive(Clone, Copy)]
struct Pin {
    body: egui::Rect,
    at: egui::Pos2,
    last_painted: u64,
}

/// Applies this frame's click and returns where the readout is pinned, if anywhere. `ladder` is the
/// response of the ladder's whole body.
pub(super) fn pinned_point(
    ladder: &egui::Response,
    layout: &DomLayout,
    view: &DomView,
) -> Option<egui::Pos2> {
    let id = ladder.id.with("pin");
    let body = ladder.rect;
    let this_frame = ladder.ctx.cumulative_frame_nr();
    let pin = ladder
        .ctx
        .data(|data| data.get_temp::<Option<Pin>>(id))
        .flatten()
        .filter(|pin| pin.body == body && pin.last_painted + 1 >= this_frame);
    let next = match PaneClick::read(&ladder.ctx, body, ladder.layer_id) {
        Some(PaneClick::Finger(at)) => {
            let tapped_row = layout.row_at(view, at).map(|(row, _)| row);
            is_pinned_by_tap(pin, tapped_row).then_some(Pin {
                body,
                at,
                last_painted: this_frame,
            })
        }
        Some(PaneClick::Mouse) => None,
        None => pin.map(|pin| Pin {
            last_painted: this_frame,
            ..pin
        }),
    };
    ladder.ctx.data_mut(|data| data.insert_temp(id, next));
    next.map(|pin| pin.at)
}

/// A tap on a row pins it, or moves the pin there from another row. A second tap on the pinned row,
/// or a tap on no row, lets go.
fn is_pinned_by_tap(pin: Option<Pin>, tapped_row: Option<egui::Rect>) -> bool {
    tapped_row.is_some_and(|row| !pin.is_some_and(|pin| row.contains(pin.at)))
}
