//! The strategy payload frame, the datagram, and the table of topics with the body of each. The
//! digest of the wire layout folds that table, so each topic and each body width is in it.

use crate::msg::link::LINK_MAX_FIELDS;
use crate::msg::time::VenueTs;

pub use crate::msg::link::{LinkFrame, LinkOrigin, LinkPayload};
use crate::msg::ui::{LatencyReport, UiBookSnapshot, UiEvent};

use super::control::{CatalogFeature, CatalogInstrument, Lifecycle, Subscribe};
use super::envelope::{
    ByteReader, ByteWriter, ENVELOPE_LEN, Envelope, FrameGuard, LINK_MAX_DATAGRAM, LinkDecodeError,
    LinkHash, TopicId,
};
use super::wire::{LayoutDigest, WireField, wire_array};

/// One engine topic: the name a config gives it, its id, and the width and digest of its body.
#[derive(Clone, Copy)]
pub(super) struct TopicLayout {
    pub(super) name: &'static str,
    pub(super) topic: TopicId,
    width: usize,
    digest: u64,
}

impl TopicLayout {
    const fn of<T: WireField>(name: &'static str, topic: TopicId) -> Self {
        Self {
            name,
            topic,
            width: T::WIDTH,
            digest: T::DIGEST,
        }
    }
}

/// Each engine topic and its body. Topics 8 to 15 are reserved for the engine; a strategy topic
/// carries a payload.
pub(super) const ENGINE_TOPICS: [TopicLayout; 8] = [
    TopicLayout::of::<Subscribe>("subscribe", TopicId::SUBSCRIBE),
    TopicLayout::of::<UiBookSnapshot>("books", TopicId::BOOKS),
    TopicLayout::of::<UiEvent>("events", TopicId::EVENTS),
    TopicLayout::of::<CatalogInstrument>("catalog_instruments", TopicId::CATALOG_INSTRUMENTS),
    TopicLayout::of::<CatalogFeature>("catalog_features", TopicId::CATALOG_FEATURES),
    TopicLayout::of::<Lifecycle>("lifecycle", TopicId::LIFECYCLE),
    TopicLayout::of::<LatencyReport>("latency", TopicId::LATENCY),
    TopicLayout::of::<UiEvent>("history", TopicId::HISTORY),
];

/// The digest of the whole wire layout: the envelope, each engine topic with its body, and the
/// payload of a strategy topic. The envelope carries it, so two builds of different layouts refuse
/// each other instead of misreading a body.
pub const LINK_LAYOUT: LinkHash = LinkHash(layout_digest());

const fn layout_digest() -> u64 {
    let mut digest = LayoutDigest::START
        .text("link")
        .text("envelope")
        .part(Envelope::WIDTH, Envelope::DIGEST);
    let mut index = 0;
    while index < ENGINE_TOPICS.len() {
        let row = ENGINE_TOPICS[index];
        digest = digest
            .text(row.name)
            .number(row.topic.0 as u64)
            .part(row.width, row.digest);
        index += 1;
    }
    digest
        .text("strategy payload")
        .number(TopicId::FIRST_STRATEGY.0 as u64)
        .part(LinkPayload::WIDTH, LinkPayload::DIGEST)
        .finish()
}

const fn fits_one_datagram() -> bool {
    let mut index = 0;
    while index < ENGINE_TOPICS.len() {
        if ENVELOPE_LEN + ENGINE_TOPICS[index].width > LINK_MAX_DATAGRAM {
            return false;
        }
        index += 1;
    }
    ENVELOPE_LEN + LinkPayload::WIDTH <= LINK_MAX_DATAGRAM
}

// Each frame, a payload with every slot filled included, fits one datagram.
const _: () = assert!(fits_one_datagram());

impl LinkPayload {
    const WIDTH: usize =
        LinkHash::WIDTH + VenueTs::WIDTH + u8::WIDTH + <[f64; LINK_MAX_FIELDS]>::WIDTH;
    const DIGEST: u64 = LayoutDigest::START
        .text("payload")
        .text("schema_hash")
        .part(LinkHash::WIDTH, LinkHash::DIGEST)
        .text("event_ts_us")
        .part(VenueTs::WIDTH, VenueTs::DIGEST)
        .text("count")
        .part(u8::WIDTH, u8::DIGEST)
        .text("values")
        .part(
            <[f64; LINK_MAX_FIELDS]>::WIDTH,
            <[f64; LINK_MAX_FIELDS]>::DIGEST,
        )
        .finish();

    fn write(&self, writer: &mut ByteWriter<'_>) {
        let (count, values) = self.wire_parts();
        self.schema_hash.write(writer);
        self.event_ts_us.write(writer);
        count.write(writer);
        values.write(writer);
    }

    fn read(reader: &mut ByteReader<'_>, guard: &FrameGuard) -> Result<Self, LinkDecodeError> {
        let schema_hash = LinkHash::read(reader)?;
        if schema_hash != guard.schema_hash {
            return Err(LinkDecodeError::SchemaMismatch {
                found: schema_hash,
                expected: guard.schema_hash,
            });
        }
        let event_ts_us = VenueTs::read(reader)?;
        let count = u8::read(reader)?;
        if count as usize > LINK_MAX_FIELDS {
            return Err(LinkDecodeError::FieldCountExceeded {
                count,
                capacity: LINK_MAX_FIELDS,
            });
        }
        let values = WireField::read(reader)?;
        Ok(Self::from_wire(schema_hash, event_ts_us, count, values)
            .expect("the count was checked against the slots above"))
    }
}

wire_array! { f64; LINK_MAX_FIELDS; 0.0 }

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
    Latency(LatencyReport),
    /// An event frame recorded before the subscriber came, with the body of a live one.
    History(UiEvent),
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
            LinkBody::Latency(report) => report.write(&mut writer),
            LinkBody::History(event) => event.write(&mut writer),
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
            TopicId::LATENCY => LinkBody::Latency(LatencyReport::read(&mut reader)?),
            TopicId::HISTORY => LinkBody::History(UiEvent::read(&mut reader)?),
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
        LinkBody::Latency(_) => Some(TopicId::LATENCY),
        LinkBody::History(_) => Some(TopicId::HISTORY),
        LinkBody::Payload(_) => None,
    }
}

fn topic_body_len(topic: TopicId) -> Option<usize> {
    if topic.is_strategy_topic() {
        return Some(LinkPayload::WIDTH);
    }
    ENGINE_TOPICS
        .iter()
        .find(|row| row.topic == topic)
        .map(|row| row.width)
}
