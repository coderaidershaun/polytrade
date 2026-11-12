//! The strategy payload frame and its datagram. A slot's meaning comes from the sender's
//! `link_fields()` declaration and never from the wire itself, so two senders whose field lists
//! differ do not agree about what a given slot index holds.

use crate::msg::ui::{UiBookSnapshot, UiEvent};
use crate::time::TsUs;

use super::control::{
    CATALOG_FEATURE_BODY_LEN, CATALOG_INSTRUMENT_BODY_LEN, CatalogFeature, CatalogInstrument,
    LIFECYCLE_BODY_LEN, Lifecycle, SUBSCRIBE_BODY_LEN, Subscribe,
};
use super::envelope::{
    ByteReader, ByteWriter, ENVELOPE_LEN, Envelope, FrameGuard, LINK_MAX_DATAGRAM, LinkDecodeError,
    LinkHash, TopicId,
};
use super::feed::{BOOK_BODY_LEN, EVENT_BODY_LEN};
use super::wire::{WireField, wire_array};

/// 38 measured maximum (see InboundLink budget assert).
pub const LINK_MAX_FIELDS: usize = 38;

const PAYLOAD_BODY_LEN: usize = 8 + 8 + 1 + 8 * LINK_MAX_FIELDS;

// Envelope+payload lands on InboundLink's 384-byte budget exactly.
const _: () = assert!(ENVELOPE_LEN + PAYLOAD_BODY_LEN <= LINK_MAX_DATAGRAM);

/// `schema_hash` is a digest of the sender's ordered `link_fields()`. A mismatch drops the frame
/// and counts the drop.
///
/// The count and its slot array stay private: they are one invariant, and a frame handed to
/// strategy code with a count past the array would slice out of bounds on the hot thread.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkPayload {
    pub schema_hash: LinkHash,
    pub event_ts_us: TsUs,
    count: u8,
    values: [f64; LINK_MAX_FIELDS],
}

impl LinkPayload {
    /// # Panics
    /// When `values` is longer than `LINK_MAX_FIELDS`. The field list is a code-level fact, so
    /// exceeding it is a bug rather than a runtime condition.
    pub fn new(schema_hash: LinkHash, event_ts_us: TsUs, values: &[f64]) -> Self {
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

    fn write(&self, writer: &mut ByteWriter<'_>) {
        self.schema_hash.write(writer);
        self.event_ts_us.write(writer);
        self.count.write(writer);
        self.values.write(writer);
    }

    fn read(reader: &mut ByteReader<'_>, guard: &FrameGuard) -> Result<Self, LinkDecodeError> {
        let schema_hash = LinkHash::read(reader)?;
        if schema_hash != guard.schema_hash {
            return Err(LinkDecodeError::SchemaMismatch {
                found: schema_hash,
                expected: guard.schema_hash,
            });
        }
        let event_ts_us = TsUs::read(reader)?;
        let count = u8::read(reader)?;
        if count as usize > LINK_MAX_FIELDS {
            return Err(LinkDecodeError::FieldCountExceeded {
                count,
                capacity: LINK_MAX_FIELDS,
            });
        }
        let values = WireField::read(reader)?;
        Ok(Self {
            schema_hash,
            event_ts_us,
            count,
            values,
        })
    }
}

wire_array! { f64; LINK_MAX_FIELDS; 0.0 }

/// Sender identity + position in stream (envelope residue after FrameGuard::check). Omit magic/version/hashes/mac to save budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkOrigin {
    pub sender_te_hash: LinkHash,
    pub boot_ts_us: TsUs,
    pub topic: TopicId,
    pub seq: u64,
}

impl From<&Envelope> for LinkOrigin {
    fn from(envelope: &Envelope) -> Self {
        Self {
            sender_te_hash: envelope.sender_te_hash,
            boot_ts_us: envelope.boot_ts_us,
            topic: envelope.topic,
            seq: envelope.seq,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkFrame {
    pub origin: LinkOrigin,
    pub payload: LinkPayload,
}

/// A decoded frame together with the local ingress stamps. The sender's `event_ts_us` stays data
/// and is never the ordering key, because clock skew between regions would reorder the stream.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InboundLink {
    pub frame: LinkFrame,
    pub received_ts_us: TsUs,
    pub queued_ts_us: TsUs,
}

/// 38 slots land on InboundMessage's 384-byte budget EXACTLY, where BookChunk takes 352. A 39th
/// slot or a new field trips this assert; the budget never moves.
const _: () = assert!(size_of::<InboundLink>() + align_of::<InboundLink>() <= 384);

/// Carries no envelope, because the actor owns the sequence and the identity. A replay therefore
/// produces byte-identical output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OutboundLink {
    pub topic: TopicId,
    pub payload: LinkPayload,
}

/// Topic determines body kind one-to-one; dispatch on topic alone.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinkDatagram {
    pub envelope: Envelope,
    pub body: LinkBody,
}

// The enum is large, but boxing it would cost a heap allocation per datagram and lose `Copy`, and
// the bodies are already fixed-size PODs.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LinkBody {
    Subscribe(Subscribe),
    Book(UiBookSnapshot),
    Event(UiEvent),
    CatalogInstrument(CatalogInstrument),
    CatalogFeature(CatalogFeature),
    Lifecycle(Lifecycle),
    Payload(LinkPayload),
}

impl LinkDatagram {
    /// # Panics
    /// Topic-body mismatch (code-level fact; receiver would decode as wrong kind).
    pub fn encode(&self, buffer: &mut [u8; LINK_MAX_DATAGRAM]) -> usize {
        let topic = self.envelope.topic;
        match expected_topic(&self.body) {
            Some(expected) => assert_eq!(
                topic, expected,
                "link body belongs on topic {expected:?}, envelope says {topic:?}"
            ),
            None => assert!(
                topic.is_strategy_topic(),
                "a strategy payload needs a topic at or above {:?}, envelope says {topic:?}",
                TopicId::FIRST_STRATEGY
            ),
        }
        let mut writer = ByteWriter::new(buffer.as_mut_slice());
        self.envelope.write(&mut writer);
        match &self.body {
            LinkBody::Subscribe(subscribe) => subscribe.write(&mut writer),
            LinkBody::Book(snapshot) => snapshot.write(&mut writer),
            LinkBody::Event(event) => event.write(&mut writer),
            LinkBody::CatalogInstrument(instrument) => instrument.write(&mut writer),
            LinkBody::CatalogFeature(feature) => feature.write(&mut writer),
            LinkBody::Lifecycle(lifecycle) => lifecycle.write(&mut writer),
            LinkBody::Payload(payload) => payload.write(&mut writer),
        }
        writer.written()
    }

    /// # Errors
    /// LinkDecodeError: one variant per cause for distinct counting. Body read only after all guards pass.
    pub fn decode(bytes: &[u8], guard: &FrameGuard) -> Result<Self, LinkDecodeError> {
        if bytes.len() < ENVELOPE_LEN {
            return Err(LinkDecodeError::Truncated { found: bytes.len() });
        }
        let mut reader = ByteReader::new(bytes);
        let envelope = Envelope::read(&mut reader)?;
        guard.check(&envelope)?;
        let topic = envelope.topic;
        let body_len = topic_body_len(topic).ok_or(LinkDecodeError::UnknownTopic { topic })?;
        let expected = ENVELOPE_LEN + body_len;
        if bytes.len() != expected {
            return Err(LinkDecodeError::LengthMismatch {
                topic,
                expected,
                found: bytes.len(),
            });
        }
        let body = match topic {
            TopicId::SUBSCRIBE => LinkBody::Subscribe(Subscribe::read(&mut reader)?),
            TopicId::BOOKS => LinkBody::Book(UiBookSnapshot::read(&mut reader)?),
            TopicId::EVENTS => LinkBody::Event(UiEvent::read(&mut reader)?),
            TopicId::CATALOG_INSTRUMENTS => {
                LinkBody::CatalogInstrument(CatalogInstrument::read(&mut reader)?)
            }
            TopicId::CATALOG_FEATURES => {
                LinkBody::CatalogFeature(CatalogFeature::read(&mut reader)?)
            }
            TopicId::LIFECYCLE => LinkBody::Lifecycle(Lifecycle::read(&mut reader)?),
            _ => LinkBody::Payload(LinkPayload::read(&mut reader, guard)?),
        };
        Ok(Self { envelope, body })
    }
}

fn expected_topic(body: &LinkBody) -> Option<TopicId> {
    match body {
        LinkBody::Subscribe(_) => Some(TopicId::SUBSCRIBE),
        LinkBody::Book(_) => Some(TopicId::BOOKS),
        LinkBody::Event(_) => Some(TopicId::EVENTS),
        LinkBody::CatalogInstrument(_) => Some(TopicId::CATALOG_INSTRUMENTS),
        LinkBody::CatalogFeature(_) => Some(TopicId::CATALOG_FEATURES),
        LinkBody::Lifecycle(_) => Some(TopicId::LIFECYCLE),
        LinkBody::Payload(_) => None,
    }
}

fn topic_body_len(topic: TopicId) -> Option<usize> {
    match topic {
        TopicId::SUBSCRIBE => Some(SUBSCRIBE_BODY_LEN),
        TopicId::BOOKS => Some(BOOK_BODY_LEN),
        TopicId::EVENTS => Some(EVENT_BODY_LEN),
        TopicId::CATALOG_INSTRUMENTS => Some(CATALOG_INSTRUMENT_BODY_LEN),
        TopicId::CATALOG_FEATURES => Some(CATALOG_FEATURE_BODY_LEN),
        TopicId::LIFECYCLE => Some(LIFECYCLE_BODY_LEN),
        _ if topic.is_strategy_topic() => Some(PAYLOAD_BODY_LEN),
        _ => None,
    }
}
