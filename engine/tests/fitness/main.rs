//! Fitness suite: the only CI tests. Proptest properties over recorded invariants, plus a
//! counting global allocator so the zero-allocation guarantees can be asserted rather than
//! trusted. Grouped as one test target so plain `cargo test` runs every area.
//!
//! Every module on disk is declared here. A suite that compiles but is not listed runs nowhere, so
//! absence from this file is the one way coverage can be lost without a diff showing it.

// The counting allocator implements an unsafe trait, and the workspace denies unsafe code.
#![allow(unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

mod chart_axis;
mod chart_pane;
mod chart_projection;
mod config_guards;
mod dom_grouping;
mod dom_projection;
mod engine_support;
mod exec_support;
mod execution_lease;
mod exposure_boot;
mod exposure_state;
mod hot;
mod link;
mod link_bar;
mod link_session;
/// These drive `engine::operator::fleet` and its binary, which exist only under feature `mcp`.
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
#[path = "../../../strategies/strat-micro-recorder/te-binance-spot-btcusdt/strategy.rs"]
mod micro_strategy;
mod monitor_fit;
mod monitor_pin;
mod monitor_projection;
mod parquet_readback;
/// The second trading engine of the same strategy, included from its folder for the same reason
/// `micro_strategy` is: the suite drives the real publisher, not a copy of it.
///
/// Both engines `#[path]`-include the strategy-level `link_schema.rs`, so compiling the two of them
/// into this one binary loads that file twice. That is the point rather than an accident — the
/// duplicate IS the shared link schema, and `poly_publisher` exists to prove the two agree — but
/// each engine is its own crate in every other build, so neither can `use` the other's copy.
#[allow(clippy::duplicate_mod)]
#[path = "../../../strategies/strat-micro-recorder/te-polymarket-btc-updown-5m/strategy.rs"]
mod poly_strategy;
mod position_projection;
mod raw_recorder;
mod runtime_exit;
mod scans;
mod suite_integrity;
mod tape;
mod venues;
/// These drive `engine::operator::relay` and its binary, which exist only under feature `web`.
#[cfg(feature = "web")]
mod web_http;
mod workstation_format;
mod workstation_shell;
mod workstation_support;

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
