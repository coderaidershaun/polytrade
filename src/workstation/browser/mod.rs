//! The workstation in the browser: egui painters over the toolkit-free workstation, and the socket
//! that feeds them. Trunk builds the page and `polysim-web` serves it.

mod app;
pub mod chart;
pub mod chrome;
mod controls;
pub mod dom;
pub mod latency;
mod layout;
pub mod link_bar;
pub mod monitor;
mod socket;
mod theme;
mod touch;

use eframe::{AppCreator, WebOptions, WebRunner};
use web_sys::wasm_bindgen::JsCast;
use web_sys::wasm_bindgen::closure::Closure;
use web_sys::{AddEventListenerOptions, Document, HtmlCanvasElement};

use crate::error;
use app::WorkstationApp;
pub use touch::announce_touch_screen;

/// The page must hold a canvas with id `workstation`.
pub fn start() {
    start_in_canvas(
        "workstation",
        Box::new(|creation| {
            theme::install_style(&creation.egui_ctx);
            Ok(Box::new(WorkstationApp::start(creation.egui_ctx.clone())?))
        }),
    );
}

/// Runs the app `creator` builds on the canvas with id `canvas_id`. Nothing waits on the result,
/// so a failure goes to the console.
pub fn start_in_canvas(canvas_id: &str, creator: AppCreator<'static>) {
    let Some(document) = web_sys::window().and_then(|window| window.document()) else {
        error!("this page has no document");
        return;
    };
    let Some(canvas) = find_canvas(&document, canvas_id) else {
        error!("the page has no canvas with id {canvas_id:?}");
        return;
    };
    let canvas_id = canvas_id.to_owned();
    run_when_visible(&document, move || {
        wasm_bindgen_futures::spawn_local(async move {
            if let Err(error) = WebRunner::new()
                .start(canvas, WebOptions::default(), creator)
                .await
            {
                error!("cannot start the app on canvas {canvas_id:?}: {error:?}");
            }
        });
    });
}

/// eframe throws away the texture uploads of a frame drawn while the page is hidden, and the
/// canvas then stays black for good. So the first frame must be drawn visible, and a page opened
/// in a hidden tab starts, link included, when it is first shown. Warning: a page hidden again
/// before eframe draws that first frame still draws it hidden, so this narrows the gap rather than
/// closing it.
fn run_when_visible(document: &Document, run: impl FnOnce() + 'static) {
    if !document.hidden() {
        run();
        return;
    }
    let options = AddEventListenerOptions::new();
    options.set_once(true);
    let on_visible = Closure::once_into_js(run);
    if let Err(error) = document.add_event_listener_with_callback_and_add_event_listener_options(
        "visibilitychange",
        on_visible.unchecked_ref(),
        &options,
    ) {
        error!("cannot wait for the page to be shown: {error:?}");
    }
}

fn find_canvas(document: &Document, canvas_id: &str) -> Option<HtmlCanvasElement> {
    document.get_element_by_id(canvas_id)?.dyn_into().ok()
}
