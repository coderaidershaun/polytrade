//! One process from start to stop: the data and latches that each part of the engine shares, and
//! the assembly that wires those parts at start and tears them down.

pub mod config;
pub mod exposure;
pub(crate) mod pin;
pub mod registry;
pub mod secrets;
pub mod shutdown;

pub use pin::PinRefused;

#[cfg(not(target_arch = "wasm32"))]
mod assembly;

#[cfg(not(target_arch = "wasm32"))]
pub use assembly::{
    DrainDeadline, DrainFailure, DrainOutcome, DrainWatchdog, ExecutionLease, Exit, ForcedExit,
    PinnedSenders, Preflight, StartError, StartPlan, Trigger, check_symbol_order_capacity, metrics,
    run_trading_engine,
};
