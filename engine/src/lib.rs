//! A market-data and quant engine. All the substance lives in this one library, and the binaries
//! and examples only compose it.
//!
//! The groups below are in dependency order. The vocabulary is the plain data that crosses from one
//! domain into another, and nothing else crosses.
//!
//! A submodule that only splits up an implementation stays private behind a `pub use`. A submodule
//! that is a concept in its own right stays `pub mod`, and callers name it.
//!
//! Every ring the hot thread writes to is fed through a `*Sink` in [`hot::sink`], with one type for each
//! policy on a full ring.

#![forbid(unsafe_code)]
// The browser build compiles the whole vocabulary but runs only the workstation.
// SMELL: the browser build also compiles engine code that it must never run, such as the log drain
// thread and the hot thread's spawner. A page that called one would fail when it ran, not when it
// compiled.
#![cfg_attr(target_arch = "wasm32", allow(dead_code))]

// vocabulary
pub mod msg;

pub mod log;

// hot path
pub mod hot;

// async edge
#[cfg(not(target_arch = "wasm32"))]
pub mod venues;

pub mod link;

#[cfg(not(target_arch = "wasm32"))]
pub mod tape;

// assembly
pub mod runtime;

pub mod operator;

/// Feature-gated, so a headless engine never builds the agent protocol stack.
#[cfg(feature = "mcp")]
pub mod mcp;
