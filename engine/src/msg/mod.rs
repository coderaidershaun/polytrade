//! The plain data that crosses from one domain into another, one file per crossing: `inbound` is
//! everything entering the hot thread, `exec` the order lifecycle out to a venue and back,
//! `persist` the records leaving for disk, `ui` the feed a workstation renders, and `relay` the
//! hello a browser tab reads from the web server.

pub mod exec;
pub mod inbound;
pub mod persist;
#[cfg(any(feature = "web", target_arch = "wasm32"))]
pub(crate) mod relay;
pub mod ui;
