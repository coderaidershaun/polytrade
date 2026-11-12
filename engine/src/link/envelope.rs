//! The envelope every datagram opens with, checked in a fixed order so version skew is caught before
//! a book body is misread, and the byte cursors every record is written and read through.

use crate::time::TsUs;

use super::wire::{WireField, wire_struct};

/// `"PLNK"` — a foreign datagram or a mistyped port is rejected on the first four bytes.
pub const LINK_MAGIC: u32 = 0x504C_4E4B;

/// Bump on ANY layout change (UI_BOOK_LEVELS, Level, etc). Only thing catching cross-commit skew.
pub const LINK_VERSION: u16 = 10;

/// Over-length panics (never truncates); margin real but thin (max 30 chars today).
pub const LINK_NAME_LEN: usize = 32;

/// Below IPv6 1280-byte MTU; no fragmentation.
pub const LINK_MAX_DATAGRAM: usize = 1200;

/// Reserved+zero for MAC retrofit. Link security: private network boundary.
const LINK_MAC_LEN: usize = 32;

pub(super) const ENVELOPE_LEN: usize = 4 + 2 + 2 + 8 + 8 + 8 + 8 + 8 + LINK_MAC_LEN;
pub(super) const NAME_LEN: usize = 1 + LINK_NAME_LEN;

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a digest; no strings on wire. Not security (sniffable); separates runs only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkHash(pub u64);

impl LinkHash {
    pub const fn of_name(name: &str) -> Self {
        Self(fold(FNV_OFFSET_BASIS, name.as_bytes()))
    }

    /// Ordered field-name digest; order+count both in digest -> read slot 7 as 7, not 8.
    pub const fn of_fields(names: &[&str]) -> Self {
        let mut hash = FNV_OFFSET_BASIS;
        let mut index = 0;
        while index < names.len() {
            hash = fold(hash, names[index].as_bytes());
            hash = fold(hash, &[0]);
            index += 1;
        }
        Self(hash)
    }
}

const fn fold(mut hash: u64, bytes: &[u8]) -> u64 {
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

    /// The topic a config file names: an engine topic by its fixed name, else the strategy topic
    /// declared under that name.
    pub(crate) fn from_name(name: &str, strategy_topics: &[&'static str]) -> Option<Self> {
        match name {
            "books" => Some(TopicId::BOOKS),
            "events" => Some(TopicId::EVENTS),
            "catalog_instruments" => Some(TopicId::CATALOG_INSTRUMENTS),
            "catalog_features" => Some(TopicId::CATALOG_FEATURES),
            "lifecycle" => Some(TopicId::LIFECYCLE),
            _ => strategy_topics
                .iter()
                .position(|declared| *declared == name)
                .map(TopicId::strategy),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkIdentity {
    pub token_hash: LinkHash,
    pub strategy_hash: LinkHash,
    pub sender_te_hash: LinkHash,
    pub boot_ts_us: TsUs,
}

/// boot_ts_us load-bearing: restart sets seq=0; (sender,topic)-only gate reads as stale until seq passes old HWM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Envelope {
    pub magic: u32,
    pub version: u16,
    pub topic: TopicId,
    pub token_hash: LinkHash,
    pub strategy_hash: LinkHash,
    pub sender_te_hash: LinkHash,
    pub boot_ts_us: TsUs,
    pub seq: u64,
    pub mac: [u8; LINK_MAC_LEN],
}

impl Envelope {
    pub fn new(identity: LinkIdentity, topic: TopicId, seq: u64) -> Self {
        Self {
            magic: LINK_MAGIC,
            version: LINK_VERSION,
            topic,
            token_hash: identity.token_hash,
            strategy_hash: identity.strategy_hash,
            sender_te_hash: identity.sender_te_hash,
            boot_ts_us: identity.boot_ts_us,
            seq,
            mac: [0; LINK_MAC_LEN],
        }
    }
}

wire_struct! {
    Envelope {
        magic,
        version,
        topic,
        token_hash,
        strategy_hash,
        sender_te_hash,
        boot_ts_us,
        seq,
        mac,
    }
}

/// The reserved MAC moves as one block rather than field-by-field: it is the only run of bytes here
/// that carries no structure.
impl WireField for [u8; LINK_MAC_LEN] {
    fn write(&self, writer: &mut ByteWriter<'_>) {
        writer.write_bytes(self);
    }

    fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
        Ok(reader.take())
    }
}

/// schema_hash gates strategy payloads only (engine topics carry no schema).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameGuard {
    pub token_hash: LinkHash,
    pub strategy_hash: LinkHash,
    pub schema_hash: LinkHash,
}

impl FrameGuard {
    pub(super) fn check(&self, envelope: &Envelope) -> Result<(), LinkDecodeError> {
        if envelope.magic != LINK_MAGIC {
            return Err(LinkDecodeError::MagicMismatch {
                found: envelope.magic,
            });
        }
        if envelope.version != LINK_VERSION {
            return Err(LinkDecodeError::VersionMismatch {
                found: envelope.version,
            });
        }
        if envelope.token_hash != self.token_hash {
            return Err(LinkDecodeError::TokenMismatch {
                found: envelope.token_hash,
                expected: self.token_hash,
            });
        }
        if envelope.strategy_hash != self.strategy_hash {
            return Err(LinkDecodeError::StrategyMismatch {
                found: envelope.strategy_hash,
                expected: self.strategy_hash,
            });
        }
        Ok(())
    }
}

/// Fixed bytes; validated UTF-8 by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WireName {
    bytes: [u8; LINK_NAME_LEN],
    len: u8,
}

impl WireName {
    /// # Panics
    /// Over LINK_NAME_LEN bytes (startup code-level fact; truncate -> silent column mislabel).
    pub fn new(name: &str) -> Self {
        assert!(
            name.len() <= LINK_NAME_LEN,
            "link name {name:?} is {} bytes, capacity {LINK_NAME_LEN}",
            name.len()
        );
        let mut bytes = [0; LINK_NAME_LEN];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        Self {
            bytes,
            len: name.len() as u8,
        }
    }

    pub fn as_str(&self) -> &str {
        str::from_utf8(&self.bytes[..self.len as usize])
            .expect("WireName holds validated utf8 by construction")
    }
}

impl WireField for WireName {
    fn write(&self, writer: &mut ByteWriter<'_>) {
        self.len.write(writer);
        writer.write_bytes(&self.bytes);
    }

    fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
        let len = u8::read(reader)?;
        let mut bytes: [u8; LINK_NAME_LEN] = reader.take();
        if len as usize > LINK_NAME_LEN {
            return Err(LinkDecodeError::NameTooLong { found: len });
        }
        // Zero every byte past `len`. A garbage tail would display the same name yet compare unequal,
        // so a catalog upsert would miss the entry it should replace.
        bytes[len as usize..].fill(0);
        str::from_utf8(&bytes[..len as usize]).map_err(|_| LinkDecodeError::NameNotUtf8)?;
        Ok(Self { bytes, len })
    }
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkDecodeError {
    #[error("datagram is {found} bytes, shorter than the {ENVELOPE_LEN}-byte envelope")]
    Truncated { found: usize },
    #[error("magic {found:#010x} is not the link magic {LINK_MAGIC:#010x}")]
    MagicMismatch { found: u32 },
    #[error("wire version {found} is not {LINK_VERSION} — peer built from another commit")]
    VersionMismatch { found: u16 },
    #[error("token hash {found:?} is not the configured {expected:?}")]
    TokenMismatch { found: LinkHash, expected: LinkHash },
    #[error("strategy hash {found:?} is not the configured {expected:?}")]
    StrategyMismatch { found: LinkHash, expected: LinkHash },
    #[error("schema hash {found:?} is not the declared {expected:?} — link_fields drifted")]
    SchemaMismatch { found: LinkHash, expected: LinkHash },
    #[error("topic {topic:?} is engine-reserved and unknown to this version")]
    UnknownTopic { topic: TopicId },
    #[error("topic {topic:?} frames are {expected} bytes, datagram is {found}")]
    LengthMismatch {
        topic: TopicId,
        expected: usize,
        found: usize,
    },
    #[error("payload declares {count} values, capacity {capacity}")]
    FieldCountExceeded { count: u8, capacity: usize },
    #[error("book declares {count} levels, capacity {capacity}")]
    BookLevelsExceeded { count: u16, capacity: usize },
    #[error(
        "order snapshot declares detail_len={detail_len}, total_working={total_working}; capacities are detail={detail_capacity}, total={total_capacity}"
    )]
    OrderSnapshotCountsInvalid {
        detail_len: u8,
        total_working: u16,
        detail_capacity: usize,
        total_capacity: u16,
    },
    #[error("order snapshot detail contains terminal state {state:?}")]
    OrderSnapshotTerminalState { state: crate::hot::exec::OrderState },
    #[error("order snapshot detail repeats client id {client_id:?}")]
    OrderSnapshotDuplicate {
        client_id: crate::ids::ClientOrderId,
    },
    #[error("subscription names {count} topics, capacity {capacity}")]
    TopicCountExceeded { count: u8, capacity: usize },
    #[error("name is {found} bytes, capacity {LINK_NAME_LEN}")]
    NameTooLong { found: u8 },
    #[error("name bytes are not utf8")]
    NameNotUtf8,
    #[error("unknown {field} discriminant {value}")]
    UnknownDiscriminant { field: &'static str, value: u8 },
}

impl LinkDecodeError {
    pub(super) fn unknown(field: &'static str, value: u8) -> Self {
        Self::UnknownDiscriminant { field, value }
    }
}

pub(super) struct ByteWriter<'a> {
    bytes: &'a mut [u8],
    at: usize,
}

impl<'a> ByteWriter<'a> {
    pub(super) fn new(bytes: &'a mut [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    pub(super) fn written(&self) -> usize {
        self.at
    }

    pub(super) fn write_bytes(&mut self, value: &[u8]) {
        self.bytes[self.at..self.at + value.len()].copy_from_slice(value);
        self.at += value.len();
    }

    /// Reused buffer must not leak previous datagram; future MAC covers these bytes.
    pub(super) fn pad_to(&mut self, end: usize) {
        self.bytes[self.at..end].fill(0);
        self.at = end;
    }
}

pub(super) struct ByteReader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> ByteReader<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    /// OOB read here = invariant violation (not bad input); frame length checked before body read.
    pub(super) fn take<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0; N];
        out.copy_from_slice(&self.bytes[self.at..self.at + N]);
        self.at += N;
        out
    }
}
