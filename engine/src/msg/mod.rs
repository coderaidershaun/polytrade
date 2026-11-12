//! The plain data that crosses from one domain into another, one file per crossing: `inbound` is
//! everything entering the hot thread, `exec` the order lifecycle out to a venue and back,
//! `persist` the records leaving for disk, `ui` the feed a workstation renders, and `relay` the
//! hello a browser tab reads from the web server.

// macros that declare types, with no runtime code of their own
pub(crate) mod labelled_enum;

pub mod exec;
pub mod ids;
pub mod inbound;
pub mod persist;
#[cfg(any(feature = "web", target_arch = "wasm32"))]
pub(crate) mod relay;
pub mod time;
pub mod ui;
