//! Tests of one engine process from start to stop: the clock check, config guards, the execution
//! lease, exposure at boot, the metrics thread, and the exit report.

mod clock;
mod config_guards;
mod execution_lease;
mod exposure_boot;
mod exposure_state;
mod metrics;
mod runtime_exit;
