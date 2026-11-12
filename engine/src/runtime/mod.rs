//! One process from start to stop: the data that each box reads at start, and the assembly that
//! wires the boxes and tears them down.

#[cfg(not(target_arch = "wasm32"))]
mod assembly;

#[cfg(not(target_arch = "wasm32"))]
pub use assembly::{
    EngineError, ExecutionLease, ExitReport, check_symbol_order_capacity, decide_exit,
    run_trading_engine,
};
