//! The fill model of the simulated venue: an own order fills only from print quantity that reached
//! it behind the public queue, and only while the market that it matches against is whole.

use engine::msg::exec::OrderStyle;
use engine::msg::ids::{
    AggregateTradeId, AssetId, ClientOrderId, InstrumentId, Notional, Price, Qty, RawTradeId, Side,
    StreamEpoch,
};
use engine::msg::inbound::{
    BookChunk, BookChunkKind, InboundMessage, Level, MessageBody, TappedMessage, TradeEvent,
    VenueMeta,
};
use engine::msg::time::{DurationUs, LocalTs, VenueTs};
use engine::runtime::config::FeeBps;
use engine::venues::binance::parse::{
    DepthDiff, ParseContext, parse_agg_trade, parse_combined_frame, parse_spot_depth_diff,
};
use engine::venues::exec::ExecRequest;
use engine::venues::sim::core::wallet::SimWalletSetup;
use engine::venues::sim::core::{
    InstrumentLimits, LatencyBudget, QueueAhead, ResetReason, SimEmission, SimVenue, SimVenueSetup,
    VenueEvent,
};
use proptest::prelude::*;

const SPOT_DEPTH: &str = include_str!("../../../../fixtures/binance/spot_depth.jsonl");
const SPOT_TRADES: &str = include_str!("../../../../fixtures/binance/spot_aggTrade.jsonl");
const INSTRUMENT: InstrumentId = InstrumentId(0);
const TICK: i64 = 1_000_000;
/// The best bid after the first two recorded depth frames, and the public quantity resting there.
const BEST_BID: Price = Price::from_mantissa(5_821_436_000_000);
const PUBLIC_AT_BEST_BID: i64 = 418_000;
/// Below the lowest bid of those frames, so the book cannot see what rests there.
const BEYOND_DEPTH: Price = Price::from_mantissa(5_000_000_000_000);
const OWN_QTY: Qty = Qty::from_mantissa(100_000);
const NO_LATENCY: LatencyBudget = LatencyBudget {
    order_entry: DurationUs::ZERO,
    ack: DurationUs::ZERO,
    max_market_data_delay: DurationUs::ZERO,
};

/// A simulated venue brought live on the recorded spot depth and trades. Each later event takes the
/// next received stamp, the next trade ids, and a venue stamp after the last commit.
struct LiveVenue {
    venue: SimVenue,
    received_us: i64,
    venue_us: i64,
    update_id: u64,
    aggregate_id: u64,
    trade_id: u64,
    epoch: StreamEpoch,
}

impl LiveVenue {
    /// The first recorded depth frame is the snapshot and the second the delta that bridges it.
    /// The recorded trades seed the trade stream, and none of them reaches an own bid.
    fn start() -> Self {
        let venue = SimVenue::new(SimVenueSetup {
            instrument: INSTRUMENT,
            book_capacity: 20,
            market_inbox_capacity: 1_024,
            verdict_retention: DurationUs::from_secs(60),
            limits: InstrumentLimits {
                tick: Price::from_mantissa(TICK),
                step: Qty::from_mantissa(1_000),
                min_qty: Qty::from_mantissa(1_000),
                min_notional: Notional::from_mantissa(500_000_000),
                max_orders_per_side: 4,
                max_amends: 4,
            },
            wallet: SimWalletSetup {
                base_asset: AssetId(0),
                quote_asset: AssetId(1),
                opening_base: Qty::from_mantissa(0),
                opening_quote: Notional::from_mantissa(100_000_000_000),
                maker_fee_bps: FeeBps::from_bps(0.0).expect("no fee is a rate"),
            },
        })
        .expect("two distinct assets and non-negative balances");
        let mut frames = SPOT_DEPTH.lines().take(2).map(recorded_depth);
        let snapshot = frames.next().expect("the depth file holds a first frame");
        let bridge = frames.next().expect("the depth file holds a second frame");
        let mut live = LiveVenue {
            venue,
            received_us: snapshot.exchange_ts_us.micros(),
            venue_us: 0,
            update_id: bridge.final_update_id,
            aggregate_id: 0,
            trade_id: 0,
            epoch: StreamEpoch::default(),
        };
        live.snapshot(&levels_of(&snapshot.bids), &levels_of(&snapshot.asks));
        live.delta(&bridge.bids, &bridge.asks, bridge.exchange_ts_us);
        live.venue
            .restore_matching(LocalTs::from_micros(live.received_us));
        for line in SPOT_TRADES.lines() {
            let frame = parse_combined_frame(line).expect("each recorded line is a combined frame");
            let recorded = parse_agg_trade(
                &frame.data,
                ParseContext {
                    instrument: INSTRUMENT,
                    received_ts_us: LocalTs::from_micros(live.received_us),
                },
            )
            .expect("each recorded line is an aggregate trade");
            live.aggregate_id = recorded.aggregate_id.0;
            live.trade_id = recorded.last_trade_id.0;
            live.venue_us = live.venue_us.max(recorded.trade.exchange_ts_us.micros());
            let fills = own_fill(&live.trade(
                recorded.trade,
                (
                    recorded.aggregate_id,
                    recorded.first_trade_id,
                    recorded.last_trade_id,
                ),
            ));
            assert_eq!(
                fills, 0,
                "no recorded print reaches an order before one rests"
            );
        }
        live
    }

    fn place_buy(&mut self, client: u64, price: Price, qty: Qty) -> Vec<SimEmission> {
        self.received_us += 1_000;
        self.venue.on_request(
            &ExecRequest::Place {
                instrument: INSTRUMENT,
                client_id: ClientOrderId(client),
                side: Side::Buy,
                price,
                qty,
                style: OrderStyle::PostOnly,
            },
            LocalTs::from_micros(self.received_us),
        );
        self.advance()
    }

    /// A print with the next trade ids, stamped after each depth commit so far.
    fn print(&mut self, side: Side, price: Price, qty: Qty) -> Vec<SimEmission> {
        self.venue_us += 1_000;
        let aggregate_id = self.aggregate_id + 1;
        let trade_id = self.trade_id + 1;
        self.print_with(side, price, qty, (aggregate_id, trade_id), self.venue_us)
    }

    /// A print with its own ids and venue stamp, for prints that break continuity or are late.
    fn print_with(
        &mut self,
        side: Side,
        price: Price,
        qty: Qty,
        (aggregate_id, trade_id): (u64, u64),
        venue_us: i64,
    ) -> Vec<SimEmission> {
        self.aggregate_id = self.aggregate_id.max(aggregate_id);
        self.trade_id = self.trade_id.max(trade_id);
        let event = TradeEvent {
            instrument: INSTRUMENT,
            price,
            qty,
            side,
            exchange_ts_us: VenueTs::from_micros(venue_us),
            exchange_sent_ts_us: None,
        };
        let ids = (
            AggregateTradeId(aggregate_id),
            RawTradeId(trade_id),
            RawTradeId(trade_id),
        );
        self.trade(event, ids)
    }

    fn trade(
        &mut self,
        event: TradeEvent,
        (aggregate_id, first_trade_id, last_trade_id): (AggregateTradeId, RawTradeId, RawTradeId),
    ) -> Vec<SimEmission> {
        let meta = VenueMeta::Trade {
            aggregate_id,
            first_trade_id,
            last_trade_id,
            stream_epoch: self.epoch,
        };
        self.deliver(&[(MessageBody::Trade(event), meta)])
    }

    /// A delta that sets one level, committed at a venue stamp after the last one.
    fn set_level(&mut self, side: Side, price: Price, qty: Qty) -> Vec<SimEmission> {
        self.venue_us += 1_000;
        let level = [Level { price, qty }];
        let (bids, asks): (&[Level], &[Level]) = match side {
            Side::Buy => (&level, &[]),
            Side::Sell => (&[], &level),
        };
        self.delta(bids, asks, VenueTs::from_micros(self.venue_us))
    }

    fn delta(&mut self, bids: &[Level], asks: &[Level], venue: VenueTs) -> Vec<SimEmission> {
        self.update_id += 1;
        let chunks = chunks(
            BookChunkKind::Delta,
            bids,
            asks,
            self.update_id,
            Some(venue),
        );
        let meta = VenueMeta::DepthDelta {
            exchange_ts_us: venue,
        };
        let tapped: Vec<_> = chunks
            .into_iter()
            .map(|chunk| (MessageBody::Book(chunk), meta))
            .collect();
        self.deliver(&tapped)
    }

    fn snapshot(&mut self, bids: &[Level], asks: &[Level]) -> Vec<SimEmission> {
        let chunks = chunks(BookChunkKind::Snapshot, bids, asks, self.update_id, None);
        let tapped: Vec<_> = chunks
            .into_iter()
            .map(|chunk| (MessageBody::Book(chunk), VenueMeta::None))
            .collect();
        self.deliver(&tapped)
    }

    fn deliver(&mut self, messages: &[(MessageBody, VenueMeta)]) -> Vec<SimEmission> {
        self.received_us += 1_000;
        let received = LocalTs::from_micros(self.received_us);
        let tapped: Vec<TappedMessage> = messages
            .iter()
            .map(|&(body, venue_meta)| TappedMessage {
                message: InboundMessage::new(received, body),
                venue_meta,
            })
            .collect();
        self.venue.on_market_batch(&tapped, NO_LATENCY);
        self.advance()
    }

    fn advance(&mut self) -> Vec<SimEmission> {
        let mut due = Vec::new();
        self.venue
            .advance_to_watermark(LocalTs::from_micros(self.received_us), &mut due);
        due
    }
}

fn recorded_depth(line: &str) -> DepthDiff {
    let frame = parse_combined_frame(line).expect("each recorded line is a combined frame");
    parse_spot_depth_diff(
        &frame.data,
        ParseContext {
            instrument: INSTRUMENT,
            received_ts_us: LocalTs::from_micros(1_784_410_629_415_000),
        },
    )
    .expect("each recorded line is a spot depth update")
}

/// A snapshot holds only the levels that rest, and a delta of zero removes a level.
fn levels_of(levels: &[Level]) -> Vec<Level> {
    levels
        .iter()
        .copied()
        .filter(|level| level.qty.mantissa() > 0)
        .collect()
}

fn chunks(
    kind: BookChunkKind,
    bids: &[Level],
    asks: &[Level],
    update_id: u64,
    venue: Option<VenueTs>,
) -> Vec<BookChunk> {
    let mut chunks = Vec::new();
    for (side, levels) in [(Side::Buy, bids), (Side::Sell, asks)] {
        if levels.is_empty() {
            continue;
        }
        let mut chunk = BookChunk::new(INSTRUMENT, kind, side);
        chunk.push_levels(levels);
        chunk.update_id = update_id;
        chunk.exchange_ts_us = venue;
        chunks.push(chunk);
    }
    if let Some(last) = chunks.last_mut() {
        last.is_last_chunk = true;
    }
    chunks
}

fn own_fill(emissions: &[SimEmission]) -> i64 {
    emissions
        .iter()
        .map(|emission| match emission.event {
            VenueEvent::Filled { last_qty, .. } => last_qty.mantissa(),
            _ => 0,
        })
        .sum()
}

fn queue_ahead(emissions: &[SimEmission]) -> Option<QueueAhead> {
    emissions.iter().find_map(|emission| match emission.event {
        VenueEvent::Rested { queue_ahead, .. } => Some(queue_ahead),
        _ => None,
    })
}

fn market_resets(emissions: &[SimEmission]) -> Vec<ResetReason> {
    emissions
        .iter()
        .filter_map(|emission| match emission.event {
            VenueEvent::MarketReset { reason } => Some(reason),
            _ => None,
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
enum AtTheBid {
    Print(i64),
    Level(i64),
}

fn at_the_bid() -> impl Strategy<Value = AtTheBid> {
    prop_oneof![
        (1i64..=300_000).prop_map(AtTheBid::Print),
        (0i64..=1_000_000).prop_map(AtTheBid::Level),
    ]
}

/// The public quantity at the own price, split at the own order, as the venue may know it: a print
/// takes from the front, a lower level takes from the back, and a higher level joins the back.
struct ExpectedQueue {
    ahead: i64,
    behind: i64,
    own_left: i64,
}

impl ExpectedQueue {
    fn set_visible(&mut self, visible: i64) {
        let known = self.ahead + self.behind;
        if visible >= known {
            self.behind += visible - known;
            return;
        }
        let mut excess = known - visible;
        let from_behind = excess.min(self.behind);
        self.behind -= from_behind;
        excess -= from_behind;
        self.ahead -= excess;
    }

    fn print(&mut self, qty: i64, own_fill: i64) {
        let from_ahead = qty.min(self.ahead);
        self.ahead -= from_ahead;
        self.own_left -= own_fill;
        self.behind -= (qty - from_ahead - own_fill).min(self.behind);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// A fill that exceeds its print, or that comes while public quantity rests ahead of the own
    /// order, would make each simulated run report fills that its market could not give.
    #[test]
    fn own_fill_never_exceeds_print_and_never_jumps_the_queue(
        events in prop::collection::vec(at_the_bid(), 1..40),
    ) {
        let mut live = LiveVenue::start();
        let rested = live.place_buy(1, BEST_BID, OWN_QTY);
        prop_assert_eq!(
            queue_ahead(&rested),
            Some(QueueAhead::Known(Qty::from_mantissa(PUBLIC_AT_BEST_BID))),
            "the own bid rests behind the public quantity the book shows at its price"
        );
        let mut queue = ExpectedQueue {
            ahead: PUBLIC_AT_BEST_BID,
            behind: 0,
            own_left: OWN_QTY.mantissa(),
        };
        let mut unseen = LiveVenue::start();
        let rested_unseen = unseen.place_buy(2, BEYOND_DEPTH, OWN_QTY);
        prop_assert_eq!(
            queue_ahead(&rested_unseen),
            Some(QueueAhead::Unobservable),
            "the book cannot see what rests below its lowest bid"
        );

        let (mut filled, mut reached) = (0i64, 0i64);
        for event in events {
            match event {
                AtTheBid::Level(visible) => {
                    let emissions = live.set_level(Side::Buy, BEST_BID, Qty::from_mantissa(visible));
                    prop_assert_eq!(own_fill(&emissions), 0, "a depth delta fills nothing");
                    queue.set_visible(visible);
                    let unseen_emissions =
                        unseen.set_level(Side::Buy, BEYOND_DEPTH, Qty::from_mantissa(visible));
                    prop_assert_eq!(own_fill(&unseen_emissions), 0, "a depth delta fills nothing");
                }
                AtTheBid::Print(qty) => {
                    let ahead = queue.ahead;
                    let fill = own_fill(&live.print(Side::Sell, BEST_BID, Qty::from_mantissa(qty)));
                    let reaching = (qty - ahead).max(0);
                    prop_assert!(
                        fill <= reaching,
                        "a print of {} with {} known ahead filled {}: the fill jumped the queue",
                        qty, ahead, fill
                    );
                    prop_assert!(fill <= queue.own_left, "the fill {} overfills the order", fill);
                    prop_assert_eq!(
                        fill,
                        reaching.min(queue.own_left),
                        "the fill is what reached the order, up to what the order has left"
                    );
                    queue.print(qty, fill);
                    filled += fill;
                    reached += reaching;

                    let unseen_fill =
                        own_fill(&unseen.print(Side::Sell, BEYOND_DEPTH, Qty::from_mantissa(qty)));
                    prop_assert_eq!(
                        unseen_fill, 0,
                        "quantity ahead that the book cannot see blocks each fill behind it"
                    );
                }
            }
        }
        prop_assert!(filled <= reached, "fills {} against {} that reached the order", filled, reached);
    }
}

/// A gap in the trade ids means a print was missed, so the venue cannot know which orders it
/// filled. Matching stops until the book is whole again, and an order fills nothing in between.
#[test]
fn trade_id_gap_suspends_matching_until_snapshot() {
    let mut live = LiveVenue::start();
    live.place_buy(1, BEST_BID, OWN_QTY);
    let through = Price::from_mantissa(BEST_BID.mantissa() - 100 * TICK);
    let print_qty = Qty::from_mantissa(10_000);

    live.venue_us += 1_000;
    let after_gap = (live.aggregate_id + 1, live.trade_id + 2);
    let gap = live.print_with(Side::Sell, through, print_qty, after_gap, live.venue_us);
    assert_eq!(
        market_resets(&gap),
        vec![ResetReason::TradeGap],
        "the print after a gap of ids resets the market once"
    );
    assert_eq!(
        own_fill(&gap),
        0,
        "the print that shows the gap fills nothing"
    );

    let mut between = Vec::new();
    for _ in 0..3 {
        between.extend(live.print(Side::Sell, through, print_qty));
    }
    let snapshot = recorded_depth(SPOT_DEPTH.lines().next().expect("a first depth frame"));
    between.extend(live.snapshot(&levels_of(&snapshot.bids), &levels_of(&snapshot.asks)));
    between.extend(live.set_level(
        Side::Buy,
        Price::from_mantissa(5_174_610_000_000),
        Qty::from_mantissa(38_000),
    ));
    assert_eq!(
        own_fill(&between),
        0,
        "no print fills an order while matching is suspended"
    );
    assert!(
        market_resets(&between).is_empty(),
        "one market reset for the episode, however the book is rebuilt"
    );

    live.venue
        .restore_matching(LocalTs::from_micros(live.received_us));
    let after = live.print(Side::Sell, through, print_qty);
    assert_eq!(
        own_fill(&after),
        print_qty.mantissa(),
        "once matching is restored, a print through the bid fills it again"
    );
}
