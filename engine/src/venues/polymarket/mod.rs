//! Three-layer law: TRANSPORT (`rest`, `ws`) → bytes; PURE MACHINES (`discovery`, `rotation`,
//! `shadow`, `teardown`, `parse`, `book`) → JSON→actions (deterministic replay); DRIVER (`actor`) →
//! async + rotation FSMs.

pub mod actor;
pub mod exec;
pub mod rest;

// `pub` for the fitness suite, not for a caller: adapter parse and sequencing against recorded
// payloads is a standing test contract, and widening a module later just to let a test compile is
// forbidden — narrowing these now would strand the parked suites for good. Written down because
// an unrecorded `pub` is how `ws` hid a dead read loop until someone finally narrowed it.
pub mod book;
pub mod discovery;
pub mod parse;
pub mod preflight;
pub mod rotation;
pub mod shadow;
pub mod teardown;

pub(crate) mod ws;
