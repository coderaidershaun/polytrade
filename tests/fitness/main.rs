//! Fitness suite: the only CI tests. Proptest properties over recorded invariants, plus a
//! counting global allocator so the zero-allocation guarantees can be asserted rather than
//! trusted. Grouped as one test target so plain `cargo test` runs every area.
//!
//! Every module on disk is declared here. A suite that compiles but is not listed runs nowhere, so
//! absence from this file is the one way coverage can be lost without a diff showing it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

mod binance_depth_sequencing;
mod binance_kline_sequencing;
mod binance_parse;
mod binance_rest_plan;
mod binance_sign;
mod book_reconstruction;
mod cargo_bins;
mod chart_axis;
mod chart_pane;
mod chart_projection;
mod config_guards;
mod dispatch_replay;
mod dom_grouping;
mod dom_projection;
mod e2e_replay;
mod e2e_scenario;
mod egarch_seed;
mod engine_support;
mod exec_audit;
mod exec_budget;
mod exec_codec;
mod exec_core;
mod exec_flatten;
mod exec_ladder;
mod exec_no_clock;
mod exec_order;
mod exec_prior_run;
mod exec_reconcile;
mod exec_resync;
mod exec_session;
mod exec_support;
mod exec_sweep;
mod exec_window;
mod execution_lease;
mod exposure_boot;
mod exposure_state;
/// The deterministic Binance Spot matching model. It is where every fill, partial fill,
/// post-only rejection and ack/report race is exercised, because production is the only other
/// venue this engine will ever reach.
mod fake_venue;
mod fastqueue;
mod feature_contract;
mod hawkes_alloc;
mod ingress_order;
mod kyles_lambda_alloc;
mod latency_metrics;
mod link_bar;
mod link_control;
mod link_ingress;
mod link_session;
mod link_wire;
mod log_lane_ownership;
mod loss_gate;
/// These drive `polysim::mcp` and its binary, which exist only under feature `mcp`.
#[cfg(feature = "mcp")]
mod mcp_fleet;
#[cfg(feature = "mcp")]
mod mcp_http;
#[cfg(feature = "mcp")]
mod mcp_support;
#[cfg(feature = "mcp")]
mod mcp_tools;
/// The shipped strategy, included from its folder so the fitness suite drives the real thing. One
/// include point only — a second would mint a distinct `MicroRecorder` type.
#[path = "../../strategies/strat-micro-recorder/te-binance-spot-btcusdt/strategy.rs"]
mod micro_strategy;
mod monitor_fit;
mod monitor_pin;
mod monitor_projection;
mod parquet_readback;
mod persist_drain;
mod persist_exec;
mod persist_retention;
mod persist_tables;
mod poly_discovery;
mod poly_driver;
mod poly_exec_codec;
mod poly_exec_policy;
mod poly_exec_rows;
mod poly_publisher;
mod poly_rotation;
mod poly_shadow;
mod poly_sign;
/// The second trading engine of the same strategy, included from its folder for the same reason
/// `micro_strategy` is: the suite drives the real publisher, not a copy of it.
///
/// Both engines `#[path]`-include the strategy-level `common.rs`, so compiling the two of them
/// into this one binary loads that file twice. That is the point rather than an accident — the
/// duplicate IS the shared link schema, and `poly_publisher` exists to prove the two agree — but
/// each engine is its own crate in every other build, so neither can `use` the other's copy.
#[allow(clippy::duplicate_mod)]
#[path = "../../strategies/strat-micro-recorder/te-polymarket-btc-updown-5m/strategy.rs"]
mod poly_strategy;
mod polymarket_fixtures;
mod polymarket_gamma;
mod polymarket_parse;
mod position_ledger;
mod position_projection;
/// The quant calculator pins. They live here rather than beside the calculators because `src/`
/// carries production code only. One module per calculator, named for the concept rather than the
/// source path.
mod quant;
mod raw_recorder;
mod reconnect_backoff;
mod recorder_fixture;
mod recorder_min_distance;
mod recorder_quotes;
mod rest_quiet;
mod risk_gate;
mod runtime_exit;
mod scale_preflight;
mod sim_wire;
mod spin_sampling;
mod strategy_resume;
mod suite_integrity;
mod tracker;
mod ui_feed;
mod volume_bars;
/// These drive `polysim::web` and its binary, which exist only under feature `web`.
#[cfg(feature = "web")]
mod web_http;
mod workstation_format;
mod workstation_shell;
mod workstation_support;
mod zero_alloc;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

#[global_allocator]
static COUNTING_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

// SAFETY: every call forwards unchanged to the system allocator. The only added work is
// bumping a const-initialised thread-local counter, which never itself allocates.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        bump_allocation_count();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        bump_allocation_count();
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        bump_allocation_count();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn bump_allocation_count() {
    ALLOCATIONS
        .try_with(|count| count.set(count.get() + 1))
        .ok();
}

/// Allocator calls on this thread so far. Steady state must show zero allocations AND zero
/// deallocations, so both are counted.
/// Take the delta around a scoped region to assert that region never touched the allocator.
fn alloc_count() -> u64 {
    ALLOCATIONS.with(Cell::get)
}
