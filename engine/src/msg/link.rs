//! The frame that the strategy of one engine sends another, as the fold reads it: who sent it, on
//! which topic, and its values. The link writes it to the wire and reads it back.

use crate::msg::time::VenueTs;

/// The most slots that fit the 384-byte input message. Its size assert fails on a 39th.
pub const LINK_MAX_FIELDS: usize = 38;

pub(crate) const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a digest; no strings on wire. Not security (sniffable); separates runs only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkHash(pub u64);

impl LinkHash {
    pub const fn of_name(name: &str) -> Self {
        Self(fnv_fold(FNV_OFFSET_BASIS, name.as_bytes()))
    }

    /// Ordered field-name digest; order+count both in digest -> read slot 7 as 7, not 8.
    pub const fn of_fields(names: &[&str]) -> Self {
        let mut hash = FNV_OFFSET_BASIS;
        let mut index = 0;
        while index < names.len() {
            hash = fnv_fold(hash, names[index].as_bytes());
            hash = fnv_fold(hash, &[0]);
            index += 1;
        }
        Self(hash)
    }
}

pub(crate) const fn fnv_fold(mut hash: u64, bytes: &[u8]) -> u64 {
    let mut index = 0;
    while index < bytes.len() {
        hash ^= bytes[index] as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
        index += 1;
    }
    hash
}

/// Topic <-> body kind one-to-one; below FIRST_STRATEGY reserved (strategy claim = config error).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicId(pub u16);

impl TopicId {
    pub const SUBSCRIBE: TopicId = TopicId(0);
    pub const BOOKS: TopicId = TopicId(1);
    pub const EVENTS: TopicId = TopicId(2);
    pub const CATALOG_INSTRUMENTS: TopicId = TopicId(3);
    pub const CATALOG_FEATURES: TopicId = TopicId(4);
    pub const LIFECYCLE: TopicId = TopicId(5);
    pub const LATENCY: TopicId = TopicId(6);
    /// Recent event frames, sent once to a subscriber that names this topic.
    pub const HISTORY: TopicId = TopicId(7);
    pub const FIRST_STRATEGY: TopicId = TopicId(16);

    /// Id of the strategy topic declared at `index`. The numbering lives here because the sites
    /// that resolve a topic name, stamp ids on outbound frames, and size the per-topic sequence
    /// array must agree — a disagreement surfaces as an out-of-bounds index on the link send path
    /// rather than as a startup rejection.
    ///
    /// # Panics
    /// Index past the u16 topic space (a strategy declaring tens of thousands of topics).
    #[inline]
    pub fn strategy(index: usize) -> Self {
        let raw = usize::from(Self::FIRST_STRATEGY.0) + index;
        TopicId(u16::try_from(raw).expect("strategy topic index overflows the u16 topic space"))
    }

    /// Width of the id space once `strategy_topic_count` strategy topics exist — the length an
    /// array indexed by raw topic id must have.
    #[inline]
    pub fn space_len(strategy_topic_count: usize) -> usize {
        usize::from(Self::FIRST_STRATEGY.0) + strategy_topic_count
    }

    #[inline]
    pub const fn is_strategy_topic(self) -> bool {
        self.0 >= Self::FIRST_STRATEGY.0
    }
}

/// `schema_hash` is a digest of the sender's ordered `link_fields()`. A mismatch drops the frame
/// and counts the drop.
///
/// The count and its slot array stay private: they are one invariant, and a frame handed to
/// strategy code with a count past the array would slice out of bounds on the hot thread.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkPayload {
    pub schema_hash: LinkHash,
    pub event_ts_us: VenueTs,
    count: u8,
    values: [f64; LINK_MAX_FIELDS],
}

impl LinkPayload {
    /// # Panics
    /// When `values` is longer than `LINK_MAX_FIELDS`. The field list is a code-level fact, so
    /// exceeding it is a bug rather than a runtime condition.
    pub fn new(schema_hash: LinkHash, event_ts_us: VenueTs, values: &[f64]) -> Self {
        assert!(
            values.len() <= LINK_MAX_FIELDS,
            "link payload carries {} values, capacity {LINK_MAX_FIELDS}",
            values.len()
        );
        let mut slots = [0.0; LINK_MAX_FIELDS];
        slots[..values.len()].copy_from_slice(values);
        Self {
            schema_hash,
            event_ts_us,
            count: values.len() as u8,
            values: slots,
        }
    }

    #[inline]
    pub fn values(&self) -> &[f64] {
        debug_assert!(self.count as usize <= LINK_MAX_FIELDS);
        &self.values[..self.count as usize]
    }

    /// The count and every slot, as the wire carries them.
    pub(crate) fn wire_parts(&self) -> (u8, &[f64; LINK_MAX_FIELDS]) {
        (self.count, &self.values)
    }

    /// A payload read from the wire. `None` when `count` is past the slots, so a frame handed to
    /// strategy code never slices out of bounds.
    pub(crate) fn from_wire(
        schema_hash: LinkHash,
        event_ts_us: VenueTs,
        count: u8,
        values: [f64; LINK_MAX_FIELDS],
    ) -> Option<Self> {
        (usize::from(count) <= LINK_MAX_FIELDS).then_some(Self {
            schema_hash,
            event_ts_us,
            count,
            values,
        })
    }
}

/// Sender identity + position in stream (envelope residue after FrameGuard::check). Omit magic/layout/hashes/mac to save budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkOrigin {
    pub sender_te_hash: LinkHash,
    pub boot_ts_us: VenueTs,
    pub topic: TopicId,
    pub seq: u64,
}

/// The sender's `event_ts_us` stays data and never orders the fold, because clock skew between
/// regions would reorder the stream.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkFrame {
    pub origin: LinkOrigin,
    pub payload: LinkPayload,
}
