//! Market-data intake for the simulated venue: the taps it attaches to the live market lanes, and
//! the lanes it drains them through.

use std::sync::Arc;

use rtrb::{Consumer, RingBuffer};

use super::lanes::SimLane;
use crate::config::{BinanceMarket, VenueMarket};
use crate::hot::spawn::{QueueProducer, SimTapGate};
use crate::info;
use crate::msg::inbound::{BookChunkKind, InboundMessage, MarketTapItem, TappedMessage, VenueMeta};
use crate::registry::{ConnectionCategory, Registry};
use crate::time::TsUs;

pub struct MarketTapLane {
    consumer: Consumer<MarketTapItem>,
    drained: Vec<TappedMessage>,
    watermark_ts_us: TsUs,
    lane: SimLane,
}

impl MarketTapLane {
    pub fn new(consumer: Consumer<MarketTapItem>, lane: SimLane) -> Self {
        Self {
            consumer,
            drained: Vec::new(),
            watermark_ts_us: TsUs::from_micros(i64::MIN),
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
                    message.received_ts_us()
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

    pub fn proven_watermark_ts_us(&self) -> Option<TsUs> {
        (self.watermark_ts_us != TsUs::from_micros(i64::MIN)).then_some(self.watermark_ts_us)
    }

    pub fn is_producer_gone(&self) -> bool {
        self.consumer.is_abandoned()
    }
}

fn has_required_evidence(message: &InboundMessage, venue_meta: VenueMeta) -> bool {
    match message {
        InboundMessage::Trade(_) => matches!(venue_meta, VenueMeta::Trade { .. }),
        InboundMessage::BookReset(_) => matches!(venue_meta, VenueMeta::DepthReset { .. }),
        InboundMessage::Book(chunk) => match chunk.kind {
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

/// # Panics
/// If either Binance Spot lane is missing or duplicated.
pub(super) fn attach_market_taps(
    registry: &Registry,
    capacity: usize,
    groups: Vec<QueueProducer>,
) -> (Vec<QueueProducer>, MarketTaps) {
    assert_eq!(
        groups.len(),
        registry.producer_groups().len(),
        "the zip below would silently drop a hot producer"
    );
    let gate = Arc::new(SimTapGate::new());
    let mut trades = None;
    let mut depth = None;
    let mut tapped = Vec::with_capacity(groups.len());

    for (group, producer) in registry.producer_groups().iter().zip(groups) {
        let Some(slot) = tapped_slot(group.market, group.category, &mut trades, &mut depth) else {
            tapped.push(producer);
            continue;
        };
        let (tap_producer, tap_consumer) = RingBuffer::<MarketTapItem>::new(capacity);
        info!(
            "market tap for input queue {} ({}) allocated {} slots of {} bytes",
            group.queue_id.0,
            group.category.as_str(),
            capacity,
            size_of::<MarketTapItem>()
        );
        *slot = Some(tap_consumer);
        tapped.push(producer.with_tap(tap_producer, Arc::clone(&gate)));
    }

    let taps = MarketTaps {
        trades: trades.expect("simulator requires a binance spot trades producer group"),
        depth: depth.expect("simulator requires a binance spot depth producer group"),
        gate,
    };
    (tapped, taps)
}

fn tapped_slot<'a>(
    market: VenueMarket,
    category: ConnectionCategory,
    trades: &'a mut Option<Consumer<MarketTapItem>>,
    depth: &'a mut Option<Consumer<MarketTapItem>>,
) -> Option<&'a mut Option<Consumer<MarketTapItem>>> {
    if market != VenueMarket::Binance(BinanceMarket::Spot) {
        return None;
    }
    let slot = match category {
        ConnectionCategory::Trades => trades,
        ConnectionCategory::Depth => depth,
        ConnectionCategory::Klines | ConnectionCategory::Market => return None,
    };
    assert!(
        slot.is_none(),
        "one simulator tap per lane: {} appears twice in the producer groups",
        category.as_str()
    );
    Some(slot)
}
