//! The synchronous hot path: one pinned thread owns all market state, fed only by SPSC rings. No
//! async, no locks, no allocation in steady state.
//!
//! Everything below hangs off one loop. [`spawn`] builds the thread and the ring producers — the
//! only seam the async edge may touch. [`ingress`] pops the oldest message across the input rings,
//! and [`dispatch`] is the body every message runs through: apply to state, call the strategy back,
//! drain the action bank, stamp metrics. State is therefore a pure function of the ordered message
//! sequence, which is what makes a replay reproduce it exactly.
//!
//! Single-writer is enforced by ownership, one owner per file: [`book`] holds the per-instrument
//! order book, [`tracker`] the derived series over it, [`ledger`] position and cost basis, [`exec`]
//! every order and balance. A strategy reads all four through a ctx and can write to none of them —
//! it writes into the [`strategy`] action bank, which [`dispatch`] drains to the output rings.

// intake
pub mod ingress;
pub mod spawn;

// state, single writer each
pub mod book;
pub mod exec;
pub(crate) mod ledger;
pub mod tracker;

// building blocks the owners above hold
pub mod quant;
pub mod series;

// loop body + the customer seam it calls
pub mod dispatch;
pub mod strategy;

// output lanes
pub mod metrics;
pub(crate) mod ui_emit;
