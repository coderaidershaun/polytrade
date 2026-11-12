//! One process from start to stop: the data that each box reads at start, and the assembly that
//! wires the boxes and tears them down.

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
