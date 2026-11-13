//! Tests of one engine process from start to stop: clocks, config, the lease, exposure, metrics,
//! pins, the exit, and the timing harness's refusal of a live engine.

mod clock;
mod config_guards;
mod exit;
mod exposure;
mod exposure_state;
mod identity;
mod latency_probe;
pub(crate) mod metrics;
mod pin;
mod preflight;
mod start_order;
mod timer;
