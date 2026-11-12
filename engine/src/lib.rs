//! A market-data and quant engine. All the substance lives in this one library, and the binaries
//! and examples only compose it.
//!
//! The groups below are in dependency order. The vocabulary is the plain data that crosses from one
//! domain into another, and nothing else crosses.
//!
//! A submodule that only splits up an implementation stays private behind a `pub use`. A submodule
//! that is a concept in its own right stays `pub mod`, and callers name it.
//!
//! Every ring the hot thread writes to is fed through a `*Sink` in [`sink`], with one type for each
//! policy on a full ring.

#![forbid(unsafe_code)]
// The browser build compiles the whole vocabulary but runs only the workstation.
// SMELL: the browser build also compiles engine code that it must never run, such as the log drain
// thread and the hot thread's spawner. A page that called one would fail when it ran, not when it
// compiled.
#![cfg_attr(target_arch = "wasm32", allow(dead_code))]

// macros that declare types, with no runtime code of their own
mod labelled_enum;

// vocabulary
pub mod ids;
pub mod msg;
pub mod time;

// ring producers
pub mod sink;

// hot path
pub mod hot;

// async edge
#[cfg(not(target_arch = "wasm32"))]
pub mod adapters;
pub mod exposure;
pub mod link;
pub mod log;
#[cfg(not(target_arch = "wasm32"))]
pub mod persist;

// assembly
pub mod config;
pub mod registry;
#[cfg(not(target_arch = "wasm32"))]
pub mod runtime;
pub mod secrets;
pub mod shutdown;

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
pub mod mcp;

/// Feature-gated, so a headless engine never builds the browser workstation's HTTP server.
#[cfg(feature = "web")]
pub mod web;
