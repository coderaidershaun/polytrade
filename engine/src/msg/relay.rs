//! The first message the web server sends a browser tab, so the tab can build the link session the
//! engine will accept. It carries the token's hash, never the token. That hash is the only secret
//! the engine's frame guard checks, so whoever can read the hello can send frames the engine
//! accepts.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// Relative to the page, so the page reaches the relay both at the root and behind a path prefix.
pub(crate) const RELAY_PATH: &str = "api/link";

/// The query key whose value is the engine's index in [`RelayHello::peers`].
pub(crate) const PEER_KEY: &str = "peer";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RelayHello {
    pub(crate) strategy: String,
    pub(crate) peers: Vec<SocketAddr>,
    /// A plain `u64` because the link's own hash type is a hot-path type and never derives serde.
    pub(crate) token_hash: u64,
}
