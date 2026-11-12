//! Quant: pure calculators and fixed-state residents fed by the tracker. Dependency is one-way —
//! strategy depends on quant; quant never imports strategy or tracker state, only borrowed values.
//!
//! One vocabulary, because a strategy author meets every calculator here as the same kind of object:
//! `push` stores a datum verbatim in a rolling window (`FastQueue`'s own verb); `on_<datum>` folds
//! an observation into derived state, named for what it consumes and never for the caller's cadence,
//! since the same calculator may be driven from a spin or a book commit; `fit` runs an
//! optimisation or regression over a window and returns `Option<…Estimate>`; every cheap read is a
//! noun naming what it returns (`intensity`, `volatility`, `quote`), never `value`. Teardown splits
//! the same way — `clear` wipes a stored window, `reset` a recursion that stores none, and
//! `reset_continuity` drops only the chain link, keeping the accumulation.

/// Floors a rate before its `ln` so an underflowed one can't feed `-inf` into an objective.
pub(crate) const MIN_RATE: f64 = 1e-300;

pub(crate) const BPS: f64 = 1e4;

pub mod hawkes;
pub mod intensity;
pub mod liquidity;
pub mod micro;
pub mod optimise;
pub mod pricing;
pub mod toxicity;
pub mod volatility;
