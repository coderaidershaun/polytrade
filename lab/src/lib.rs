//! Scores strategies on recorded history and gates research.
//!
//! Priorities, in order: determinism, self-evident architecture, adaptation (tiers, signals, sessions), throughput (sweeps).
//! Never: show the research agent a test fold or the holdout slice.
//! Never: reach a live engine.
//! Never: let a replay cross a tape gap.

#![forbid(unsafe_code)]
