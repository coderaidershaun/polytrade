//! Venue adapters: async actors at the I/O edge that normalise exchange payloads into the crate's
//! inbound messages. Venue quirks never cross this boundary — `hot/` sees only normalised messages.

use std::time::Duration;

pub mod backoff;
pub mod binance;
pub(crate) mod chunk;
pub mod decode;
pub(crate) mod edge;
pub mod exchange_sim;
pub mod exec;
pub mod http;
pub(crate) mod lane;
pub mod polymarket;
pub mod rest_quiet;
pub(crate) mod socket;
pub(crate) mod venue;
pub mod venue_clock;

pub use venue::VenueError;

/// How often a parked adapter looks up to see whether the run resumed. Slow enough to cost nothing
/// while idle, quick enough that a resume is not felt.
pub(crate) const IDLE_POLL: Duration = Duration::from_millis(200);
