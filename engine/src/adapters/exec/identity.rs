//! Who minted an order id and on which run. Each venue codec encodes this into its own wire
//! format; the ownership verdict it yields is the same everywhere. The nonce history a run draws
//! those ids from is named here too — each venue declares its own, the runtime stores it.

use ring::digest::{SHA256, digest};

use crate::config::RunIdentity;
use crate::hot::exec::ClientIdLayout;
use crate::ids::ClientOrderId;
use crate::msg::exec::Provenance;

/// Which trading engine minted an order id — a stable digest of the run identity, so two engines on
/// one API key can each tell its own orders from the other's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TeTag(u32);

impl TeTag {
    /// FNV-1a over the run identity. Hand-rolled because std's hasher is not stable across
    /// toolchains, and a digest that moved under a toolchain bump would orphan the engine's own
    /// resting orders at the next restart.
    pub fn of(identity: &RunIdentity) -> Self {
        const OFFSET: u32 = 0x811c_9dc5;
        const PRIME: u32 = 0x0100_0193;
        let mut hash = OFFSET;
        // Separator prevents ("ab", "c") and ("a", "bc") hash collision.
        for byte in identity
            .strategy_id
            .as_str()
            .bytes()
            .chain(std::iter::once(0))
            .chain(identity.te_id.as_str().bytes())
        {
            hash = (hash ^ u32::from(byte)).wrapping_mul(PRIME);
        }
        Self(hash)
    }

    pub(crate) const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Which nonce history a run advances, declared by the venue it trades. Venue and account together
/// name a file that outlives every process on the host: two runs sharing one would mint client order
/// ids under a spent nonce, so this is a permanent commitment rather than a label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaseNamespace<'a> {
    pub venue: &'a str,
    /// The account this history belongs to, hashed rather than written into the name. `None` where
    /// the venue has no account to tell apart.
    pub account: Option<&'a [u8]>,
}

impl LeaseNamespace<'_> {
    pub(crate) fn nonce_file_stem(&self, te_tag: TeTag) -> String {
        let te = te_tag.get();
        match self.account {
            Some(account) => format!(".exec-{}-{te:08x}-{}", self.venue, fingerprint(account)),
            None => format!(".exec-{}-{te:08x}", self.venue),
        }
    }
}

/// Names the account in a path without putting the credential in one.
fn fingerprint(credential: &[u8]) -> String {
    digest(&SHA256, credential)
        .as_ref()
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A process's identity: a te_tag naming the trading engine, plus a run_nonce that rides the
/// high 32 bits of every client id this process mints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EngineIdentity {
    pub te_tag: TeTag,
    pub run_nonce: u32,
}

/// Who owns an order, paired with the client id that identifies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderOwnership {
    pub provenance: Provenance,
    /// Zero for a Foreign order, which is safe because nonce 0 is never minted for a real one.
    pub client_id: ClientOrderId,
}

impl OrderOwnership {
    pub(crate) const FOREIGN: Self = Self {
        provenance: Provenance::Foreign,
        client_id: ClientOrderId(0),
    };

    /// Verdict on an id the venue named back to us, once the venue codec has parsed it.
    pub(crate) fn of(tag: TeTag, client_id: ClientOrderId, identity: EngineIdentity) -> Self {
        if tag != identity.te_tag {
            return Self::FOREIGN;
        }
        let provenance = match ClientIdLayout::nonce_of(client_id) == identity.run_nonce {
            true => Provenance::Mine,
            false => Provenance::PriorRun,
        };
        Self {
            provenance,
            client_id,
        }
    }
}
