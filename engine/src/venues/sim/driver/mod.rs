//! Adapter between the simulated venue and the production execution lifecycle.

mod account;
mod correlation;
mod handshake;
// SMELL: the driver's methods are spread over this folder, and its files read each other's fields
// and helpers, so no one of them can change alone.
mod session;
mod summary;

use session::{EffectStamp, asset_name, venue_millis};
pub use session::{SimExecDriver, SimExecDriverSetup};
