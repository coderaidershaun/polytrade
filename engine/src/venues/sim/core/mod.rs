//! Pure simulated-venue state transitions.

pub(crate) mod latency;
// SMELL: no test pins the fill model in `market` or the horizon of the latency lanes, so a change
// to how the simulator fills an order passes every gate.
pub(crate) mod market;
pub(crate) mod ordering;
/// Public by decision: an order is what the wire contract describes, so anyone holding that
/// contract to account needs to name one.
pub mod orders;
pub(crate) mod queue;
pub(crate) mod request;
pub(crate) mod resting;
mod revise;
pub(crate) mod schedule;
// SMELL: the venue and the request fold import each other, because the fold writes into the
// venue's emission book. The book and its events in a file of their own would end the loop.
mod venue;
/// Public by decision: balances and fee settlement are the other half of what the wire contract
/// carries, and a fill payload cannot be checked without the numbers behind it.
pub mod wallet;

pub(crate) use revise::ForcedOrderExit;
pub(crate) use venue::{EmissionBook, SimEmission, SimVenue, SimVenueSetup, VenueEvent};
