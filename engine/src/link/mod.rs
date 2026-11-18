//! The UDP link between engines and workstations. Every frame shares one envelope that carries the
//! digest of the wire layout, so either end refuses a frame of another layout before it reads the body.

#[cfg(not(target_arch = "wasm32"))]
mod actor;
mod control;
mod credential;
mod envelope;
mod feed;
mod frame;
mod senders;
mod subscribers;
mod wire;

#[cfg(not(target_arch = "wasm32"))]
pub use actor::{
    AssertionVerdict, ControlPlane, LinkActor, LinkActorSetup, LinkCounts, LinkCountsHandle,
    PeerFeed, RefusedAssertions,
};

pub use control::{
    CatalogFeature, CatalogInstrument, LINK_MAX_TOPICS, Lifecycle, RunPhase, Subscribe, TopicSet,
    TopicSetError,
};
pub use credential::LinkCredential;
pub use envelope::{
    Envelope, FrameGuard, LINK_MAGIC, LINK_MAX_DATAGRAM, LINK_NAME_LEN, LinkDecodeError, LinkHash,
    LinkIdentity, TopicId, WireName,
};
pub use frame::{
    LINK_LAYOUT, LINK_MAX_FIELDS, LinkBody, LinkDatagram, LinkFrame, LinkOrigin, LinkPayload,
    OutboundLink,
};
pub use senders::{LINK_MAX_SENDERS, SenderCount, SenderCounts};
pub use subscribers::{
    GateCounts, GateVerdict, LINK_MAX_GATE_KEYS, LINK_MAX_SUBSCRIBERS, LINK_SUBSCRIPTION_TTL,
    RefreshOutcome, SequenceGate, SubscriberTable,
};

/// Checks every name's length before hashing, because a truncated name would label the wrong field.
///
/// # Panics
/// When a name is longer than [`LINK_NAME_LEN`] bytes.
pub fn schema_hash_of_fields(names: &[&str]) -> LinkHash {
    for name in names {
        assert!(
            name.len() <= LINK_NAME_LEN,
            "link field {name:?} is {} bytes, capacity {LINK_NAME_LEN}",
            name.len()
        );
    }
    LinkHash::of_fields(names)
}
