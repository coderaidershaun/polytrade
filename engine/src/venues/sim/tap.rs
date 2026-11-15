//! Market-data intake for the simulated venue: the taps it attaches to the live market lanes, and
//! the lanes it drains them through.

use std::sync::Arc;

use rtrb::{Consumer, RingBuffer};

use super::error::SimVenueError;
use super::lanes::SimLane;
use crate::hot::spawn::{QueueProducer, SimTapGate};
use crate::info;
use crate::msg::ids::QueueId;
use crate::msg::inbound::{
    BookChunkKind, InboundMessage, MarketTapItem, MessageBody, TappedMessage, VenueMeta,
};
use crate::msg::time::LocalTs;
use crate::runtime::config::{BinanceMarket, VenueMarket};
use crate::runtime::registry::{ConnectionCategory, Registry};

pub struct MarketTapLane {
    consumer: Consumer<MarketTapItem>,
    drained: Vec<TappedMessage>,
    watermark_ts_us: LocalTs,
    lane: SimLane,
}

impl MarketTapLane {
    pub fn new(consumer: Consumer<MarketTapItem>, lane: SimLane) -> Self {
        Self {
            consumer,
            drained: Vec::new(),
            watermark_ts_us: LocalTs::from_micros(i64::MIN),
            lane,
        }
    }

    /// The batch never escapes: leaving it with the caller is how the reusable buffer gets lost and
    /// every later drain allocates a fresh one.
    pub(super) fn drain(&mut self, visit: impl FnOnce(&[TappedMessage])) {
        self.drained.clear();
        while let Ok(item) = self.consumer.pop() {
            let proof = match item {
                MarketTapItem::Watermark { received_ts_us } => received_ts_us,
                MarketTapItem::Event(tapped) => {
                    let TappedMessage {
                        message,
                        venue_meta,
                    } = tapped;
                    assert!(
                        has_required_evidence(&message, venue_meta),
                        "{} lane event {message:?} arrived with venue metadata {venue_meta:?} — \
                         its continuity evidence was lost",
                        self.lane
                    );
                    self.drained.push(tapped);
                    message.header.received_ts_us
                }
            };
            assert!(
                proof >= self.watermark_ts_us,
                "{} lane rewound from {} to {} — the producer clamps, so this is a bug",
                self.lane,
                self.watermark_ts_us.micros(),
                proof.micros()
            );
            self.watermark_ts_us = proof;
        }
        visit(&self.drained);
    }

    pub fn proven_watermark_ts_us(&self) -> Option<LocalTs> {
        (self.watermark_ts_us != LocalTs::from_micros(i64::MIN)).then_some(self.watermark_ts_us)
    }

    pub fn is_producer_gone(&self) -> bool {
        self.consumer.is_abandoned()
    }
}

fn has_required_evidence(message: &InboundMessage, venue_meta: VenueMeta) -> bool {
    match &message.body {
        MessageBody::Trade(_) => matches!(venue_meta, VenueMeta::Trade { .. }),
        MessageBody::BookReset(_) => matches!(venue_meta, VenueMeta::DepthReset { .. }),
        MessageBody::Book(chunk) => match chunk.kind {
            BookChunkKind::Delta => matches!(venue_meta, VenueMeta::DepthDelta { .. }),
            BookChunkKind::Snapshot => venue_meta == VenueMeta::None,
        },
        _ => venue_meta == VenueMeta::None,
    }
}

pub(super) struct MarketTaps {
    pub(super) trades: Consumer<MarketTapItem>,
    pub(super) depth: Consumer<MarketTapItem>,
    pub(super) gate: Arc<SimTapGate>,
}

/// The input queues of the Binance spot trades and depth lanes, found in preflight, so the taps
/// attach at the start with nothing left to find.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct TapLanes {
    trades: QueueId,
    depth: QueueId,
}

impl TapLanes {
    /// # Errors
    /// [`SimVenueError::LaneMissing`] when the registry holds no Binance spot producer group for a
    /// lane.
    pub(super) fn find(registry: &Registry) -> Result<Self, SimVenueError> {
        let queue_of = |lane: SimLane| {
            registry
                .producer_groups()
                .iter()
                .find(|group| tapped_lane(group.market, group.category) == Some(lane))
                .map(|group| group.queue_id)
                .ok_or(SimVenueError::LaneMissing { lane })
        };
        Ok(Self {
            trades: queue_of(SimLane::Trade)?,
            depth: queue_of(SimLane::Depth)?,
        })
    }
}

/// # Panics
/// If a Binance spot lane appears twice in the producer groups.
pub(super) fn attach_market_taps(
    registry: &Registry,
    capacity: usize,
    groups: Vec<QueueProducer>,
    lanes: TapLanes,
) -> (Vec<QueueProducer>, MarketTaps) {
    assert_eq!(
        groups.len(),
        registry.producer_groups().len(),
        "the zip below would silently drop a hot producer"
    );
    let gate = Arc::new(SimTapGate::new());
    let (trades_tap, trades) = RingBuffer::<MarketTapItem>::new(capacity);
    let (depth_tap, depth) = RingBuffer::<MarketTapItem>::new(capacity);
    let mut taps = [
        (lanes.trades, Some(trades_tap)),
        (lanes.depth, Some(depth_tap)),
    ];
    let mut tapped = Vec::with_capacity(groups.len());

    for (group, producer) in registry.producer_groups().iter().zip(groups) {
        if tapped_lane(group.market, group.category).is_none() {
            tapped.push(producer);
            continue;
        }
        let tap = taps
            .iter_mut()
            .find(|(queue, _)| *queue == group.queue_id)
            .and_then(|(_, tap)| tap.take());
        let Some(tap) = tap else {
            panic!(
                "one simulator tap per lane: {} appears twice in the producer groups",
                group.category.as_str()
            );
        };
        info!(
            "market tap for input queue {} ({}) allocated {} slots of {} bytes",
            group.queue_id.index(),
            group.category.as_str(),
            capacity,
            size_of::<MarketTapItem>()
        );
        tapped.push(producer.with_tap(tap, Arc::clone(&gate)));
    }

    let taps = MarketTaps {
        trades,
        depth,
        gate,
    };
    (tapped, taps)
}

fn tapped_lane(market: VenueMarket, category: ConnectionCategory) -> Option<SimLane> {
    if market != VenueMarket::Binance(BinanceMarket::Spot) {
        return None;
    }
    match category {
        ConnectionCategory::Trades => Some(SimLane::Trade),
        ConnectionCategory::Depth => Some(SimLane::Depth),
        ConnectionCategory::Klines | ConnectionCategory::Market => None,
    }
}
