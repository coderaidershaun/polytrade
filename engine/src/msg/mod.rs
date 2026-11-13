//! The plain data that one domain hands another, with one file for each crossing, and the ids,
//! times and run state that every crossing is written in.

// macros that declare types, with no runtime code of their own
pub(crate) mod labelled_enum;

pub mod exec;
pub mod failure;
pub mod ids;
pub mod inbound;
pub mod latency;
pub mod persist;
#[cfg(any(feature = "web", target_arch = "wasm32"))]
pub(crate) mod relay;
pub mod run_state;
pub mod time;
pub mod ui;
