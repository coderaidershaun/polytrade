//! The hashes of the token and the strategy that each frame of a link carries. They are derived
//! only here, so the relay's hello and the engine's frame guard cannot drift apart.

use crate::msg::time::VenueTs;

use super::envelope::{FrameGuard, LinkHash, LinkIdentity};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkCredential {
    token_hash: LinkHash,
    strategy_hash: LinkHash,
}

impl LinkCredential {
    /// A link with no token checks the hash of the empty token, so no token and an empty token are
    /// one credential.
    pub const fn new(token: Option<&str>, strategy_id: &str) -> Self {
        let token = match token {
            Some(token) => token,
            None => "",
        };
        Self {
            token_hash: LinkHash::of_name(token),
            strategy_hash: LinkHash::of_name(strategy_id),
        }
    }

    /// The credential of a workstation, which reads the hash of the token from the relay's hello
    /// and never sees the token itself.
    pub const fn of_token_hash(token_hash: LinkHash, strategy_id: &str) -> Self {
        Self {
            token_hash,
            strategy_hash: LinkHash::of_name(strategy_id),
        }
    }

    /// The only secret that a frame guard checks, so whoever reads it can send frames that the
    /// engine accepts.
    pub const fn token_hash(&self) -> LinkHash {
        self.token_hash
    }

    pub const fn guard(&self, schema_hash: LinkHash) -> FrameGuard {
        FrameGuard {
            token_hash: self.token_hash,
            strategy_hash: self.strategy_hash,
            schema_hash,
        }
    }

    pub const fn identity(&self, sender_te_hash: LinkHash, boot_ts_us: VenueTs) -> LinkIdentity {
        LinkIdentity {
            token_hash: self.token_hash,
            strategy_hash: self.strategy_hash,
            sender_te_hash,
            boot_ts_us,
        }
    }
}
