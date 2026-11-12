//! Self-exciting trade-arrival model: one exponential kernel whose intensity updates in constant
//! time per event, fitted by a warm-started simplex over a rolling window of event times.

mod events;
mod fit;
mod kernel;
mod monitor;

pub use events::HawkesEvents;
pub use fit::{HawkesEstimate, HawkesMle};
pub use kernel::{HawkesParams, UnivariateHawkes};
pub use monitor::{HawkesChoice, HawkesSide};
