//! The order of the simulator's three lanes: the venue applies each event in the order of its key,
//! never past the horizon, so no arrival order of the lanes changes a fill.

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
use engine::venues::exec::ExecRequest;
use engine::venues::sim::core::wallet::SimWalletSetup;
use engine::venues::sim::core::{
    InstrumentLimits, LatencyBudget, SimEmission, SimVenue, SimVenueSetup, VenueEvent,
};
use engine::venues::sim::lanes::{SimHorizon, SimLane};

const INSTRUMENT: InstrumentId = InstrumentId(0);
const BASE_US: i64 = 1_800_000_000_000_000;
const LATENCY: LatencyBudget = LatencyBudget {
    order_entry: DurationUs::from_millis(15),
    ack: DurationUs::from_millis(15),
    max_market_data_delay: DurationUs::from_millis(100),
};

const fn at_ms(ms: i64) -> i64 {
    BASE_US + ms * 1_000
}

const fn cents(cents: i64) -> Price {
    Price::from_mantissa(cents * 1_000_000)
}

const fn milli_btc(milli: i64) -> Qty {
    Qty::from_mantissa(milli * 100_000)
}

fn stamp(us: i64) -> LocalTs {
    LocalTs::from_micros(us)
}

/// A venue with one instrument, live on a small book: bids at 100.00 and 99.00, an ask at 101.00,
/// and a trade stream whose first print has seeded its ids.
fn live_venue(inbox_capacity: usize) -> SimVenue {
    let mut venue = SimVenue::new(SimVenueSetup {
        instrument: INSTRUMENT,
        book_capacity: 20,
        market_inbox_capacity: inbox_capacity,
        verdict_retention: DurationUs::from_secs(60),
        limits: InstrumentLimits {
            tick: cents(1),
            step: Qty::from_mantissa(1_000),
            min_qty: Qty::from_mantissa(1_000),
            min_notional: Notional::from_mantissa(1_000_000),
            max_orders_per_side: 4,
            max_amends: 4,
        },
        wallet: SimWalletSetup {
            base_asset: AssetId(0),
            quote_asset: AssetId(1),
            opening_base: Qty::from_mantissa(0),
            opening_quote: Notional::from_mantissa(1_000_000_000_000),
            maker_fee_bps: FeeBps::from_bps(0.0).expect("no fee is a rate"),
        },
    })
    .expect("two distinct assets and non-negative balances");
    let bids = [
        Level {
            price: cents(10_000),
            qty: milli_btc(5),
        },
        Level {
            price: cents(9_900),
            qty: milli_btc(3),
        },
    ];
    let asks = [Level {
        price: cents(10_100),
        qty: milli_btc(4),
    }];
    let mut snapshot = vec![
        book_chunk(BookChunkKind::Snapshot, Side::Buy, &bids, None),
        book_chunk(BookChunkKind::Snapshot, Side::Sell, &asks, None),
    ];
    snapshot[1].is_last_chunk = true;
    let mut tapped: Vec<TappedMessage> = snapshot
        .into_iter()
        .map(|chunk| tap(at_ms(0), MessageBody::Book(chunk), VenueMeta::None))
        .collect();
    let mut bridge = book_chunk(BookChunkKind::Delta, Side::Buy, &bids[1..], Some(at_ms(1)));
    bridge.is_last_chunk = true;
    tapped.push(tap(
        at_ms(1),
        MessageBody::Book(bridge),
        depth_meta(at_ms(1)),
    ));
    tapped.push(print(at_ms(2), 0, Side::Buy, cents(10_100), milli_btc(1)));
    venue.on_market_batch(&tapped, LATENCY);
    let live_through = LATENCY.market_effective(stamp(at_ms(1)));
    let mut due = Vec::new();
    venue.advance_to_watermark(live_through, &mut due);
    venue.restore_matching(live_through);
    venue.advance_to_watermark(LATENCY.market_effective(stamp(at_ms(3))), &mut due);
    assert!(due.is_empty(), "bringing the venue live emits nothing");
    venue
}

fn book_chunk(
    kind: BookChunkKind,
    side: Side,
    levels: &[Level],
    venue_us: Option<i64>,
) -> BookChunk {
    let mut chunk = BookChunk::new(INSTRUMENT, kind, side);
    chunk.push_levels(levels);
    chunk.exchange_ts_us = venue_us.map(VenueTs::from_micros);
    chunk
}

fn depth_meta(venue_us: i64) -> VenueMeta {
    VenueMeta::DepthDelta {
        exchange_ts_us: VenueTs::from_micros(venue_us),
    }
}

fn tap(received_us: i64, body: MessageBody, venue_meta: VenueMeta) -> TappedMessage {
    TappedMessage {
        message: InboundMessage::new(stamp(received_us), body),
        venue_meta,
    }
}

/// A print with trade ids `id`, stamped by the venue at its receipt.
fn print(received_us: i64, id: u64, side: Side, price: Price, qty: Qty) -> TappedMessage {
    let trade = TradeEvent {
        instrument: INSTRUMENT,
        price,
        qty,
        side,
        exchange_ts_us: VenueTs::from_micros(received_us),
        exchange_sent_ts_us: None,
    };
    let meta = VenueMeta::Trade {
        aggregate_id: AggregateTradeId(1_000 + id),
        first_trade_id: RawTradeId(5_000 + id),
        last_trade_id: RawTradeId(5_000 + id),
        stream_epoch: StreamEpoch::default(),
    };
    tap(received_us, MessageBody::Trade(trade), meta)
}

fn level_delta(received_us: i64, side: Side, price: Price, qty: Qty) -> TappedMessage {
    let mut chunk = book_chunk(
        BookChunkKind::Delta,
        side,
        &[Level { price, qty }],
        Some(received_us),
    );
    chunk.is_last_chunk = true;
    tap(
        received_us,
        MessageBody::Book(chunk),
        depth_meta(received_us),
    )
}

fn place_buy(client: u64, price: Price, qty: Qty) -> ExecRequest {
    ExecRequest::Place {
        instrument: INSTRUMENT,
        client_id: ClientOrderId(client),
        side: Side::Buy,
        price,
        qty,
        style: OrderStyle::PostOnly,
    }
}

/// One event of one lane, at the stamp that is also the watermark of its lane.
#[derive(Debug, Clone)]
enum LaneEvent {
    Command {
        issued_us: i64,
        request: ExecRequest,
    },
    Market {
        lane: SimLane,
        tapped: Box<TappedMessage>,
    },
    Watermark {
        lane: SimLane,
        at_us: i64,
    },
}

fn commands() -> Vec<LaneEvent> {
    let command = |issued_ms: i64, request: ExecRequest| LaneEvent::Command {
        issued_us: at_ms(issued_ms),
        request,
    };
    vec![
        command(5, place_buy(1, cents(10_000), milli_btc(3))),
        command(45, place_buy(2, cents(9_950), milli_btc(1))),
        command(
            80,
            ExecRequest::Cancel {
                instrument: INSTRUMENT,
                client_id: ClientOrderId(1),
            },
        ),
        LaneEvent::Watermark {
            lane: SimLane::Command,
            at_us: at_ms(400),
        },
    ]
}

fn prints() -> Vec<LaneEvent> {
    let sell = |received_ms: i64, id: u64, price: Price, qty: Qty| LaneEvent::Market {
        lane: SimLane::Trade,
        tapped: Box::new(print(at_ms(received_ms), id, Side::Sell, price, qty)),
    };
    vec![
        sell(125, 1, cents(10_000), milli_btc(3)),
        sell(165, 2, cents(10_000), milli_btc(5)),
        sell(205, 3, cents(9_950), milli_btc(2)),
        sell(245, 4, cents(10_000), milli_btc(4)),
        sell(265, 5, cents(9_900), milli_btc(6)),
        LaneEvent::Watermark {
            lane: SimLane::Trade,
            at_us: at_ms(500),
        },
    ]
}

fn depth() -> Vec<LaneEvent> {
    let delta = |received_ms: i64, side: Side, price: Price, qty: Qty| LaneEvent::Market {
        lane: SimLane::Depth,
        tapped: Box::new(level_delta(at_ms(received_ms), side, price, qty)),
    };
    vec![
        delta(110, Side::Buy, cents(10_000), milli_btc(6)),
        delta(130, Side::Buy, cents(9_950), milli_btc(2)),
        delta(150, Side::Buy, cents(10_000), milli_btc(4)),
        delta(170, Side::Sell, cents(10_100), milli_btc(7)),
        delta(190, Side::Buy, cents(9_950), milli_btc(0)),
        delta(210, Side::Buy, cents(10_000), milli_btc(9)),
        delta(230, Side::Buy, cents(9_800), milli_btc(1)),
        delta(250, Side::Sell, cents(10_050), milli_btc(2)),
        LaneEvent::Watermark {
            lane: SimLane::Depth,
            at_us: at_ms(500),
        },
    ]
}

fn stamp_of(event: &LaneEvent) -> i64 {
    match event {
        LaneEvent::Command { issued_us, .. } => *issued_us,
        LaneEvent::Market { tapped, .. } => tapped.message.header.received_ts_us.micros(),
        LaneEvent::Watermark { at_us, .. } => *at_us,
    }
}

/// Feeds the lanes in `arrivals` as the actor would: each event lifts the watermark of its lane,
/// and the venue advances to the horizon after each. Each emission is held to that horizon.
fn replay(arrivals: &[LaneEvent]) -> Vec<SimEmission> {
    let mut venue = live_venue(1_024);
    let mut horizon = SimHorizon::unseeded(LATENCY);
    let mut emitted = Vec::new();
    for event in arrivals {
        match event {
            LaneEvent::Command { issued_us, request } => {
                horizon.observe(SimLane::Command, stamp(*issued_us));
                venue.on_request(request, LATENCY.arrival(stamp(*issued_us)));
            }
            LaneEvent::Market { lane, tapped } => {
                venue.on_market_batch(std::slice::from_ref(tapped.as_ref()), LATENCY);
                horizon.observe(*lane, tapped.message.header.received_ts_us);
            }
            LaneEvent::Watermark { lane, at_us } => horizon.observe(*lane, stamp(*at_us)),
        }
        if !horizon.has_all_watermarks() {
            continue;
        }
        let through = horizon.safe_venue_horizon();
        let mut due = Vec::new();
        venue.advance_to_watermark(through, &mut due);
        for emission in &due {
            assert!(
                emission.at_ts_us <= through,
                "an emission at {} came out past the horizon {}",
                emission.at_ts_us.micros(),
                through.micros()
            );
        }
        emitted.extend(due);
    }
    emitted
}

/// What each emission says, with its time as milliseconds after the base.
fn described(emissions: &[SimEmission]) -> Vec<(i64, &'static str, u64, i64)> {
    emissions
        .iter()
        .map(|emission| {
            let ms = (emission.at_ts_us.micros() - BASE_US) / 1_000;
            match &emission.event {
                VenueEvent::Rested { snapshot, .. } => (
                    ms,
                    "rested",
                    snapshot.order.client_id.0,
                    snapshot.order.qty.mantissa(),
                ),
                VenueEvent::Filled {
                    snapshot, last_qty, ..
                } => (
                    ms,
                    "filled",
                    snapshot.order.client_id.0,
                    last_qty.mantissa(),
                ),
                VenueEvent::Canceled { snapshot } => (
                    ms,
                    "canceled",
                    snapshot.order.client_id.0,
                    snapshot.order.filled.mantissa(),
                ),
                other => panic!("an emission that the streams do not cause: {other:?}"),
            }
        })
        .collect()
}

/// The emissions of the streams below, as the venue gave them when it kept its inbox and timeline
/// in a sorted vector.
const EXPECTED: [(i64, &str, u64, i64); 5] = [
    (20, "rested", 1, 300_000),
    (60, "rested", 2, 100_000),
    (65, "filled", 1, 200_000),
    (95, "canceled", 1, 200_000),
    (105, "filled", 2, 100_000),
];

/// A fill that changes with the order in which three lanes reach the simulator would make each
/// simulated run a function of thread timing, and two runs of one tape would disagree.
#[test]
fn lane_arrival_order_never_changes_fills() {
    let lanes_one_by_one: Vec<LaneEvent> = [commands(), depth(), prints()].concat();
    let mut interleaved = lanes_one_by_one.clone();
    interleaved.sort_by_key(|event| {
        (
            matches!(event, LaneEvent::Watermark { .. }),
            stamp_of(event),
        )
    });

    let one_by_one = replay(&lanes_one_by_one);
    let merged = replay(&interleaved);
    assert_eq!(
        one_by_one, merged,
        "the same three lanes in another arrival order gave other emissions"
    );
    assert_eq!(
        described(&one_by_one),
        EXPECTED,
        "the emissions of the streams, in the order of their keys"
    );
}

/// An event applied past the horizon could be overtaken by an event of another lane with an
/// earlier key that has not arrived yet, so the venue never applies one.
#[test]
fn nothing_is_applied_past_the_horizon() {
    let lowest = |command_ms: i64, trade_ms: i64, depth_ms: i64| {
        let mut horizon = SimHorizon::unseeded(LATENCY);
        horizon.observe(SimLane::Command, stamp(at_ms(command_ms)));
        assert!(!horizon.has_all_watermarks(), "one lane has a watermark");
        horizon.observe(SimLane::Trade, stamp(at_ms(trade_ms)));
        assert!(!horizon.has_all_watermarks(), "two lanes have a watermark");
        horizon.observe(SimLane::Depth, stamp(at_ms(depth_ms)));
        assert!(horizon.has_all_watermarks(), "each lane has a watermark");
        (horizon.safe_venue_horizon().micros() - BASE_US) / 1_000
    };
    assert_eq!(
        lowest(0, 500, 500),
        15,
        "the command watermark plus the entry latency"
    );
    assert_eq!(
        lowest(500, 50, 500),
        -50,
        "the trade watermark less the delay bound"
    );
    assert_eq!(
        lowest(500, 500, 60),
        -40,
        "the depth watermark less the delay bound"
    );

    let mut venue = live_venue(1_024);
    let mut horizon = SimHorizon::unseeded(LATENCY);
    horizon.observe(SimLane::Command, stamp(at_ms(5)));
    venue.on_request(
        &place_buy(1, cents(9_950), milli_btc(2)),
        LATENCY.arrival(stamp(at_ms(5))),
    );
    horizon.observe(SimLane::Trade, stamp(at_ms(300)));
    horizon.observe(SimLane::Depth, stamp(at_ms(150)));
    let mut due = Vec::new();
    venue.advance_to_watermark(horizon.safe_venue_horizon(), &mut due);
    assert_eq!(
        described(&due),
        [(20, "rested", 1, milli_btc(2).mantissa())],
        "the order rests at its effective time, once the horizon reaches it"
    );

    venue.on_market_batch(
        &[print(at_ms(300), 1, Side::Sell, cents(9_950), milli_btc(1))],
        LATENCY,
    );
    horizon.observe(SimLane::Command, stamp(at_ms(400)));
    horizon.observe(SimLane::Depth, stamp(at_ms(300) - 1));
    let short = horizon.safe_venue_horizon();
    assert_eq!(
        short.micros(),
        at_ms(200) - 1,
        "the depth lane holds the horizon a µs short of the print"
    );
    let mut due = Vec::new();
    venue.advance_to_watermark(short, &mut due);
    assert!(
        due.is_empty(),
        "a print past the horizon is not applied: {due:?}"
    );

    horizon.observe(SimLane::Depth, stamp(at_ms(300)));
    let mut due = Vec::new();
    venue.advance_to_watermark(horizon.safe_venue_horizon(), &mut due);
    assert_eq!(
        described(&due),
        [(200, "filled", 1, milli_btc(1).mantissa())],
        "the print applies once the horizon reaches its effective time"
    );
}
