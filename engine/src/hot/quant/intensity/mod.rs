//! Estimates how often trades reach each depth (A per second, k per tick) for the quoting model:
//! the hot thread counts in place, and the strategy fits by Poisson likelihood.

mod counts;
mod fit;

pub use counts::IntensityCounts;
pub(crate) use counts::{IntensityShape, TopOfBook, TradePrint};
pub use fit::{IntensityEstimate, IntensityFit, PacedIntensity, SideEstimate};
