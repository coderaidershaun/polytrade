//! Venue adapters: async actors at the I/O edge that normalise exchange payloads into the crate's
//! inbound messages. Venue quirks never cross this boundary — `hot/` sees only normalised messages.

use std::time::Duration;

pub mod binance;
pub mod edge;
pub mod exec;
pub mod polymarket;
pub mod sim;
pub(crate) mod venue;

pub use venue::VenueError;

/// How often a parked adapter looks up to see whether the run resumed. Slow enough to cost nothing
/// while idle, quick enough that a resume is not felt.
pub(crate) const IDLE_POLL: Duration = Duration::from_millis(200);
