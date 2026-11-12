//! Composition only: the workstation as a browser page. Trunk builds this bin to wasm32, and
//! `polysim-web` serves the result. Closing the tab stops nothing on the engines.

#[cfg(target_arch = "wasm32")]
fn main() {
    polysim::workstation::browser::start();
}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> std::process::ExitCode {
    eprintln!(
        "polysim-ui is the browser workstation: build it with `trunk build` in browser/ and serve it with polysim-web"
    );
    std::process::ExitCode::from(2)
}
