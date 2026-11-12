//! Engines seen from outside: the browser workstation that shows them, the server that relays them
//! to a browser tab, and the server that starts and stops them for an agent.

/// The painters and the socket exist only in the browser build, so a native build never links
/// eframe.
// Only the painters read some of the display text and model methods, so a native build sees them,
// and their re-exports, as unused. Each target lints the half it runs, which makes the browser
// build the one that finds dead workstation code.
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code, unused_imports))]
#[cfg_attr(target_arch = "wasm32", warn(dead_code))]
pub mod workstation;

/// Feature-gated, so a headless engine never builds the agent protocol stack.
#[cfg(feature = "mcp")]
pub mod fleet;

/// Feature-gated, so a headless engine never builds the browser workstation's HTTP server.
#[cfg(feature = "web")]
pub mod relay;
