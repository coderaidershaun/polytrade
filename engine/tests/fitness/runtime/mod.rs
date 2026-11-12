//! Tests of one engine process from start to stop: the clock check, config guards, the execution
//! lease, exposure at boot, the metrics thread, the pins of its threads, the exit report, and the
//! timing harness's refusal of a live engine.

mod clock;
mod config_guards;
mod execution_lease;
mod exposure_boot;
mod exposure_state;
mod latency_probe;
pub(crate) mod metrics;
mod pin;
mod runtime_exit;
