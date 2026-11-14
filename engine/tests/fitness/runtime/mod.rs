//! Tests of one engine process from start to stop: clocks, config, the lease, exposure, metrics,
//! the OTLP export, pins, the exit, and the timing harness's refusal of a live engine.

mod clock;
mod config_guards;
mod exit;
mod exposure;
mod exposure_state;
mod identity;
mod latency_probe;
pub(crate) mod metrics;
#[cfg(feature = "otlp")]
mod otlp;
mod pin;
mod preflight;
mod sources;
mod start_order;
mod timer;
