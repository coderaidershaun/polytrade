//! The link's control and discovery frames: subscribe, lifecycle and catalog. The catalog travels
//! one item per frame, so a receiver never has to reassemble it.

use crate::msg::ids::{AssetId, InstrumentId, Price, Qty};
use crate::msg::persist::FeatureId;
use crate::msg::run_state::{ControllerEpoch, RunState};
use crate::msg::time::{DurationUs, VenueTs};
use crate::runtime::config::ExecutionMode;

use super::envelope::{ByteReader, ByteWriter, LinkDecodeError, TopicId, WireName};
use super::wire::{LayoutDigest, WireField, wire_enum, wire_struct};

/// The bytes of a `u64`, so the layout is the one of a plain epoch. The read refuses 2^63 and above
/// with the value seen, so the subscribe and the lifecycle frame refuse alike.
impl WireField for ControllerEpoch {
    const WIDTH: usize = u64::WIDTH;
    const DIGEST: u64 = u64::DIGEST;

    fn write(&self, writer: &mut ByteWriter<'_>) {
        self.get().write(writer);
    }

    fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
        let epoch = u64::read(reader)?;
        ControllerEpoch::try_from(epoch).map_err(|refused| LinkDecodeError::EpochOutOfRange {
            found: refused.found,
        })
    }
}

/// Topics ONE subscription can name (not total topics that exist).
pub const LINK_MAX_TOPICS: usize = 16;

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
    pub desired_epoch: ControllerEpoch,
}

/// The named topics ride a fixed-width slot array, so the count is a claim about the prefix rather
/// than the frame length — hence the one bespoke pair in this module.
impl WireField for Subscribe {
    const WIDTH: usize =
        u8::WIDTH + TopicId::WIDTH * LINK_MAX_TOPICS + RunState::WIDTH + ControllerEpoch::WIDTH;
    const DIGEST: u64 = LayoutDigest::START
        .text("subscribe")
        .text("count")
        .part(u8::WIDTH, u8::DIGEST)
        .text("topics")
        .number(LINK_MAX_TOPICS as u64)
        .part(TopicId::WIDTH, TopicId::DIGEST)
        .text("desired_state")
        .part(RunState::WIDTH, RunState::DIGEST)
        .text("desired_epoch")
        .part(ControllerEpoch::WIDTH, ControllerEpoch::DIGEST)
        .finish();

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
    pub acknowledged_epoch: ControllerEpoch,
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
    pub catalog_ts_us: VenueTs,
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
    pub catalog_ts_us: VenueTs,
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
