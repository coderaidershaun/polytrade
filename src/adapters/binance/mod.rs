//! Spot + perp adapters. Transport (bytes) + state machines (parse/depth/kline deterministic replay)
//! + driver (async/REST/resync). Recorded fixtures drive sequencing without socket.

pub mod actor;
pub mod exec;
pub mod rest;

// `pub` for the fitness suite, not for a caller. Parsing and sequencing against recorded venue
// payloads is a standing contract of this crate, and widening a module later just to let a test
// compile is forbidden — so narrowing these would strand those suites for good. Written down
// because an unrecorded `pub` is how `ws` hid a dead read loop until someone finally narrowed it.
pub mod depth;
pub mod kline;
pub mod parse;
pub mod preflight;

pub(crate) mod ws;
