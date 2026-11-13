//! Tests of the order in which the hot thread hands messages to the strategy, across replay,
//! warmup, and resume.

mod dispatch_replay;
mod spin_steps;
mod state_digest;
mod strategy_resume;
