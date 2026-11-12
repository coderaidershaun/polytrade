//! Simulated Binance Spot execution behind the same edge contract as the live adapter.

mod actor;
mod bringup;
/// Public by decision: the matching model here is the corpus the live Binance codec is tested
/// against, so its state types are a committed surface rather than an internal detail.
pub mod core;
mod driver;
mod error;
mod lanes;
mod readiness;
mod tap;
/// Public by decision: these are the Binance-shaped payloads the live codec must keep parsing, so
/// the surface is the contract itself and shrinking it would put the codec's evidence out of reach.
pub mod wire;

/// The only way to stand this venue up: a caller hands over its inputs here and reaches nothing
/// deeper.
pub(crate) use bringup::bring_up;
pub use error::SimVenueError;

use crate::adapters::exec::{LeaseNamespace, VenueCapabilities};
use crate::hot::exec::OrderBudget;

/// Stated by delegation rather than by copy: this venue synthesises Binance Spot, and a run whose
/// physics drifted from the venue it stands in for would be simulating something else.
///
/// The placement budget is the one fact that cannot be inherited. A real account spends a granted
/// order count; nothing meters a venue that exists inside this process, so there is none to spend.
pub(crate) fn capabilities() -> VenueCapabilities {
    crate::adapters::binance::exec::capabilities(OrderBudget::NONE)
}

/// A synthesised venue holds no account, so the trading engine's own identity is the whole history.
/// Its own history all the same: ids minted against a simulation must never continue a live one's.
pub fn lease_namespace() -> LeaseNamespace<'static> {
    LeaseNamespace {
        venue: "sim",
        account: None,
    }
}
