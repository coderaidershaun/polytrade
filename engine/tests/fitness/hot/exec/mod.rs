//! Tests of the execution engine on the hot thread: orders, budgets, gates, and reconciliation.

mod capacity;
mod command_lane;
mod exec_audit;
mod exec_flatten;
mod exec_ladder;
mod exec_resync;
mod exec_window;
mod fills;
mod loss;
mod meter;
mod order_table;
mod prior_run;
mod probes;
mod quoting;
mod risk_gate;
mod silence;
mod venues;
