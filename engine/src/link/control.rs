//! The link's control and discovery frames: subscribe, lifecycle and catalog. The catalog travels
//! one item per frame, so a receiver never has to reassemble it.

use crate::config::ExecutionMode;
use crate::ids::{AssetId, InstrumentId, Price, Qty};
use crate::msg::persist::FeatureId;
use crate::time::{DurationUs, TsUs};

use super::envelope::{
    ByteReader, ByteWriter, ENVELOPE_LEN, LINK_MAX_DATAGRAM, LinkDecodeError, NAME_LEN, TopicId,
    WireName,
};
use super::wire::{OPTIONAL_MANTISSA_LEN, WireField, wire_enum, wire_struct};

/// Topics ONE subscription can name (not total topics that exist).
pub const LINK_MAX_TOPICS: usize = 16;

pub(super) const SUBSCRIBE_BODY_LEN: usize = 1 + 2 * LINK_MAX_TOPICS + 1 + 8;
pub(super) const CATALOG_INSTRUMENT_BODY_LEN: usize =
    8 + 2 + 2 + NAME_LEN + 2 * OPTIONAL_MANTISSA_LEN + 8 + 2 + 2 + 2 * NAME_LEN;
pub(super) const CATALOG_FEATURE_BODY_LEN: usize = 8 + 2 + 2 + NAME_LEN;
pub(super) const LIFECYCLE_BODY_LEN: usize = 1 + 1 + 1 + 8 + 8 + 2;

const _: () = assert!(ENVELOPE_LEN + SUBSCRIBE_BODY_LEN <= LINK_MAX_DATAGRAM);
const _: () = assert!(ENVELOPE_LEN + CATALOG_INSTRUMENT_BODY_LEN <= LINK_MAX_DATAGRAM);
const _: () = assert!(ENVELOPE_LEN + CATALOG_FEATURE_BODY_LEN <= LINK_MAX_DATAGRAM);
const _: () = assert!(ENVELOPE_LEN + LIFECYCLE_BODY_LEN <= LINK_MAX_DATAGRAM);

/// Topics subscriber wants. Empty = all topics (matches absent `topics:` in config).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TopicSet {
    topics: [TopicId; LINK_MAX_TOPICS],
    len: u8,
}

impl TopicSet {
    pub const ALL: TopicSet = TopicSet {
        topics: [TopicId(0); LINK_MAX_TOPICS],
        len: 0,
    };

    /// # Errors
    /// [`TopicSetError::TooManyTopics`] if count > [`LINK_MAX_TOPICS`].
    pub fn new(topics: &[TopicId]) -> Result<Self, TopicSetError> {
        if topics.len() > LINK_MAX_TOPICS {
            return Err(TopicSetError::TooManyTopics {
                count: topics.len(),
            });
        }
        let mut set = Self::ALL;
        set.topics[..topics.len()].copy_from_slice(topics);
        set.len = topics.len() as u8;
        Ok(set)
    }

    #[inline]
    pub fn is_wanted(&self, topic: TopicId) -> bool {
        self.len == 0 || self.topics().contains(&topic)
    }

    #[inline]
    pub fn topics(&self) -> &[TopicId] {
        &self.topics[..self.len as usize]
    }
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicSetError {
    #[error("subscription names {count} topics, capacity {LINK_MAX_TOPICS}")]
    TooManyTopics { count: usize },
}

/// Trading or parked (assertion vs report = one comparison).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunState {
    Running,
    Idle,
}

/// Sender lifecycle (independent of RunState; draining engine reports Running until stop).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunPhase {
    Starting,
    Ready,
    Draining,
    Stopped,
}

// 1-based discriminants so all-zero datagram can't decode as valid.
wire_enum! {
    RunState, "run state";
    (RunState::Running) = 1,
    (RunState::Idle) = 2,
}

wire_enum! {
    RunPhase, "run phase";
    (RunPhase::Starting) = 1,
    (RunPhase::Ready) = 2,
    (RunPhase::Draining) = 3,
    (RunPhase::Stopped) = 4,
}

wire_enum! {
    Option<ExecutionMode>, "execution mode";
    (None) = 1,
    (Some(ExecutionMode::Off)) = 2,
    (Some(ExecutionMode::Sim)) = 3,
    (Some(ExecutionMode::Live)) = 4,
}

/// Subscriber's standing assertion (topics + desired run state). desired_epoch = monotonic max wins (no oscillation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Subscribe {
    pub topics: TopicSet,
    pub desired_state: RunState,
    pub desired_epoch: u64,
}

/// The named topics ride a fixed-width slot array, so the count is a claim about the prefix rather
/// than the frame length — hence the one bespoke pair in this module.
impl WireField for Subscribe {
    fn write(&self, writer: &mut ByteWriter<'_>) {
        let wanted = self.topics.topics();
        self.topics.len.write(writer);
        for index in 0..LINK_MAX_TOPICS {
            wanted.get(index).map_or(0, |topic| topic.0).write(writer);
        }
        self.desired_state.write(writer);
        self.desired_epoch.write(writer);
    }

    fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
        let count = u8::read(reader)?;
        if count as usize > LINK_MAX_TOPICS {
            return Err(LinkDecodeError::TopicCountExceeded {
                count,
                capacity: LINK_MAX_TOPICS,
            });
        }
        let mut wanted = [TopicId(0); LINK_MAX_TOPICS];
        for slot in &mut wanted {
            *slot = TopicId::read(reader)?;
        }
        wanted[count as usize..].fill(TopicId(0));
        Ok(Self {
            topics: TopicSet {
                topics: wanted,
                len: count,
            },
            desired_state: WireField::read(reader)?,
            desired_epoch: WireField::read(reader)?,
        })
    }
}

/// Lifecycle (acknowledged_epoch detects lossy race; spin_interval for time scaling; feature_count before catalog frames).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Lifecycle {
    pub phase: RunPhase,
    pub run_state: RunState,
    pub execution_mode: Option<ExecutionMode>,
    pub acknowledged_epoch: u64,
    pub spin_interval_us: DurationUs,
    pub feature_count: u16,
}

wire_struct! {
    Lifecycle {
        phase,
        run_state,
        execution_mode,
        acknowledged_epoch,
        spin_interval_us,
        feature_count,
    }
}

/// One instrument. Subscriber knows catalog complete = total_count distinct instruments at current epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CatalogInstrument {
    pub catalog_ts_us: TsUs,
    pub total_count: u16,
    pub instrument: InstrumentId,
    pub display: WireName,
    pub tick_size: Option<Price>,
    pub lot_size: Option<Qty>,
    pub qty_scale: i64,
    /// Two assets. Index balance lane (frame-only workstation -> resolve to account balances). Names for operator readability.
    pub base_asset: AssetId,
    pub quote_asset: AssetId,
    pub base: WireName,
    pub quote: WireName,
}

wire_struct! {
    CatalogInstrument {
        catalog_ts_us,
        total_count,
        instrument,
        display,
        tick_size,
        lot_size,
        qty_scale,
        base_asset,
        quote_asset,
        base,
        quote,
    }
}

/// One feature (keyed by FeatureId so subscriber labels rows without reading strategy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CatalogFeature {
    pub catalog_ts_us: TsUs,
    pub total_count: u16,
    pub feature: FeatureId,
    pub name: WireName,
}

wire_struct! {
    CatalogFeature {
        catalog_ts_us,
        total_count,
        feature,
        name,
    }
}
