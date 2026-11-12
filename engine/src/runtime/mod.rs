//! One process from start to stop: the data and latches that each part of the engine shares, and
//! the assembly that wires those parts at start and tears them down.

pub mod config;
pub mod exposure;
pub mod registry;
pub mod secrets;
pub mod shutdown;

#[cfg(not(target_arch = "wasm32"))]
mod assembly;

#[cfg(not(target_arch = "wasm32"))]
pub use assembly::{
    EngineError, ExecutionLease, ExitReport, check_symbol_order_capacity, decide_exit,
    run_trading_engine,
};
