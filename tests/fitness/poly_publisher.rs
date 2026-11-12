//! The polymarket publisher trading engine: one frame per spin carrying both up legs, each written
//! into the block named for the ROLE it plays — `cur` hosts the open window, `next` the one not yet
//! open.
//!
//! Worth pinning because every way of getting it wrong is SILENT. Window N's close IS window N+1's
//! open, so an inclusive comparison puts two legs in the current block for one microsecond; a `>`
//! instead of `>=` keeps publishing through the post-close resolution tail, where the market is only
//! settling to 0 or 1. A block written at the wrong offset mislabels a column that still looks live.
//! And the volume fields' cumulative reading is law rather than taste, because link frames may be
//! dropped: a per-spin delta would lose a spin permanently on the first dropped frame.
//!
//! The second half of the file is the same engine's MAKER, driven against the real execution engine
//! rather than the link, and it is here rather than in a file of its own because the two halves
//! share a calendar: which leg is `cur` on the wire is the same question as which leg may be quoted,
//! and a change to the role rule that this file caught on one side and not the other would be a
//! strategy publishing one market while trading another.

use polysim::adapters::exec::open_orders_snapshot_end;
use polysim::config::{
    Instruments, IntensitySpec, PolySeries, RecordedTables, StrategySpec, Subscriptions, TableKind,
    TrackerSpec, VenueMarket,
};
use polysim::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use polysim::hot::exec::{
    ExecLimits, ExecSettings, FeeModel, OrderBudget, RejectOrigin, RejectReason,
};
use polysim::hot::strategy::{Strategy, StrategyConfig};
use polysim::ids::{AssetId, ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty, Side};
use polysim::link::{
    Envelope, FrameGuard, InboundLink, LINK_MAX_DATAGRAM, LinkBody, LinkDatagram, LinkFrame,
    LinkHash, LinkIdentity, LinkOrigin, OutboundLink, schema_hash_of_fields,
};
use polysim::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, ExecLaneItem, OrderStyle, VenueOrderStatus,
};
use polysim::msg::inbound::{BookChunk, InboundMessage};
use polysim::msg::persist::{FeatureId, PersistRecord};
use polysim::msg::ui::{DomQuote, UiEvent};
use polysim::registry::InstrumentRow;
use polysim::sink::ExecSink;
use polysim::time::DurationUs;
use proptest::prelude::*;
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    FillPen, LinkedSetup, account_chunk, detached_setup, engine_view, engine_with_link, exec_event,
    instrument_row, persist_ring, pop, recorder_spec, rotation, snapshot_pair, spin, stream_ready,
    tracker_spec_all, trade, ts, ui_event_ring,
};
use crate::micro_strategy::{MicroRecorder, MicroRecorderParams};
/// The publisher's own copy of the shared schema — the same file the peer includes, so the frame
/// this suite builds expectations in is the one the engines run on.
use crate::poly_strategy::{PolyUpParams, PolyUpPublisher, common};

/// This strategy's two engines. The ids are what the link hashes into every envelope, so a peer of
/// another strategy — or another run — is rejected before its body is read.
const STRATEGY_ID: &str = "strat-micro-recorder";
const PUBLISHER_TE_ID: &str = "strat-micro-recorder-te-polymarket-btc-updown-5m";
const TOKEN: &str = "";

/// The btc-updown-5m cadence, in µs.
const WINDOW: i64 = 300_000_000;

/// A grid boundary far from zero, so a sign or origin slip cannot pass by accident.
const FIRST_OPEN: i64 = 1_000_000_000_000;

/// Slot a hosts the first window and slot b the second, each leg opening with its window.
const TILING: [(u16, i64); 4] = [
    (A_UP, FIRST_OPEN),
    (A_DOWN, FIRST_OPEN),
    (B_UP, FIRST_OPEN + WINDOW),
    (B_DOWN, FIRST_OPEN + WINDOW),
];

/// [`PolySeries::slot_symbols`] order: the `{a,b}` window slots crossed with `{up,down}`.
const SLOT_SYMBOLS: [&str; 4] = [
    "btc-updown-5m-a-up",
    "btc-updown-5m-a-down",
    "btc-updown-5m-b-up",
    "btc-updown-5m-b-down",
];

const A_UP: u16 = 0;
const A_DOWN: u16 = 1;
const B_UP: u16 = 2;
const B_DOWN: u16 = 3;

/// The shipped poly grid.
const TICK: i64 = FIXED_SCALE / 100;

/// The shipped `tracker.intensity` block, so the cadence under test is the deployed one.
const HALF_LIFE_SECS: f64 = 120.0;
const MIN_EVENTS: f64 = 3.0;
const REFIT_INTERVAL: i64 = 12_000_000;

/// Comfortably clear of [`MIN_EVENTS`] once decay has taken its cut — exactly three would sit under
/// the floor the instant any time passed, and the pin would read as a gate that never opens.
const TOUCHES: i64 = 6;

/// Distinct per slot so an assertion names WHICH slot leaked, not merely that one did. Complementary
/// up/down pairs, as the venue's own quotes are.
const QUOTES: [(i64, i64); 4] = [
    (cents(40), cents(42)),
    (cents(58), cents(60)),
    (cents(30), cents(32)),
    (cents(68), cents(70)),
];

/// Distinct per slot AND per side, so a bid/ask swap names itself as loudly as a slot leak.
const DEPTHS: [(i64, i64); 4] = [
    (shares(11), shares(12)),
    (shares(21), shares(22)),
    (shares(31), shares(32)),
    (shares(41), shares(42)),
];

const fn cents(value: i64) -> i64 {
    value * FIXED_SCALE / 100
}

const fn shares(value: i64) -> i64 {
    value * FIXED_SCALE
}

fn poly_tracker() -> TrackerSpec {
    TrackerSpec {
        intensity: Some(IntensitySpec {
            max_depth_ticks: 32,
            half_life_secs: HALF_LIFE_SECS,
            min_events: MIN_EVENTS,
        }),
        ..TrackerSpec::default()
    }
}

fn poly_rows() -> Vec<InstrumentRow> {
    SLOT_SYMBOLS
        .iter()
        .enumerate()
        .map(|(index, symbol)| InstrumentRow {
            instrument_id: InstrumentId(index as u16),
            market: VenueMarket::Polymarket(PolySeries::BtcUpDown5m),
            venue_symbol: (*symbol).into(),
            display: (*symbol).into(),
            base: "BTC".into(),
            quote: "USDC".into(),
            base_asset: AssetId(0),
            quote_asset: AssetId(1),
            // Stamped by the poly preflight on the live path; the reach histogram buckets by it, and
            // `MicroTracker::new` refuses to build one without it.
            tick_size: Some(Price(TICK)),
            lot_size: None,
            min_qty: None,
            min_notional: None,
            max_num_orders: None,
            max_num_order_amends: None,
            max_price: None,
            qty_scale: FIXED_SCALE,
            subscriptions: Subscriptions {
                klines: false,
                ..Subscriptions::default()
            },
            kline_intervals: Vec::new(),
            book_capacity: 128,
            max_exposure_quote: 500 * FIXED_SCALE,
            tracker: poly_tracker(),
        })
        .collect()
}

fn publisher() -> crate::engine_support::LinkedEngine {
    let rows = poly_rows();
    // Default params leave the maker half off, so every wire assertion below measures the publisher
    // alone — which is what it did before this engine could quote at all.
    let strategy = PolyUpPublisher::from_spec(
        &recorder_spec::<PolyUpParams>(Vec::new()),
        engine_view(DurationUs::from_secs(1)),
    );
    engine_with_link(LinkedSetup {
        instruments: &rows,
        strategy: Box::new(strategy),
        tables: RecordedTables::new(&[]),
        warmup: DurationUs::ZERO,
    })
}

/// A publisher whose four slots are priced and whose two window slots tile `[FIRST_OPEN, +2 WINDOW)`
/// — slot a hosts the first window, slot b the second. Rotations land before the books because a
/// rotation resets the slot's derived state, exactly as the live path does.
fn publisher_at_the_first_two_windows() -> crate::engine_support::LinkedEngine {
    let mut linked = publisher();
    for (instrument, open) in TILING {
        dispatch(
            &mut linked,
            InboundMessage::MarketRotation(rotation(
                instrument,
                open,
                open + WINDOW,
                FIRST_OPEN - 1_000,
            )),
        );
    }
    price_every_slot(&mut linked, FIRST_OPEN - 500);
    linked
}

fn price_every_slot(linked: &mut crate::engine_support::LinkedEngine, when: i64) {
    for slot in 0..SLOT_SYMBOLS.len() {
        price_slot(linked, slot as u16, when);
    }
}

fn price_slot(linked: &mut crate::engine_support::LinkedEngine, slot: u16, when: i64) {
    let (bid, ask) = QUOTES[usize::from(slot)];
    let (bids, asks) = snapshot_chunks(slot, bid, ask, when);
    dispatch(linked, InboundMessage::Book(bids));
    dispatch(linked, InboundMessage::Book(asks));
}

/// A venue snapshot arrives bids-then-asks and only the second chunk completes it; a fresh one over a
/// valid book clears both sides first. So between the two, one side holds prices and the other holds
/// nothing.
fn snapshot_chunks(slot: u16, bid: i64, ask: i64, when: i64) -> (BookChunk, BookChunk) {
    let (bid_qty, ask_qty) = DEPTHS[usize::from(slot)];
    snapshot_pair(slot, &[(bid, bid_qty)], &[(ask, ask_qty)], when)
}

fn dispatch(linked: &mut crate::engine_support::LinkedEngine, message: InboundMessage) {
    linked.engine.dispatch(pop(0, 0), &message);
}

/// The one frame the publisher banked on this spin.
fn spin_and_take_frame(
    linked: &mut crate::engine_support::LinkedEngine,
    now: i64,
) -> Option<OutboundLink> {
    while linked.outbound.pop().is_ok() {}
    dispatch(linked, InboundMessage::SpinTick(spin(1, now)));
    linked.outbound.pop().ok()
}

/// Every value the strategy banked for the link on one spin. Empty when it sent nothing, which is
/// distinct from a frame whose every slot reads absent.
fn spin_and_collect(linked: &mut crate::engine_support::LinkedEngine, now: i64) -> Wire {
    while linked.outbound.pop().is_ok() {}
    dispatch(linked, InboundMessage::SpinTick(spin(1, now)));
    let mut sent = Vec::new();
    while let Ok(outbound) = linked.outbound.pop() {
        sent.extend(as_wire(outbound.payload.values()));
    }
    sent
}

/// NaN is the wire's word for absent and never equals itself, so a frame compares by PRESENCE.
type Wire = Vec<Option<f64>>;

fn as_wire(values: &[f64]) -> Wire {
    values
        .iter()
        .map(|value| value.is_finite().then_some(*value))
        .collect()
}

/// An expectation is authored as the frame the publisher would have built, then flattened by the
/// publisher's own `to_array` — so a test names the slots it fills instead of the offsets they sit at.
fn wire_of(frame: common::UpFrame) -> Wire {
    as_wire(&frame.to_array())
}

/// Every expectation below starts from `ABSENT`, which would make a 0.0 seed there an expectation and
/// a bug at once: both sides would carry the zero and agree, while the peer read it as a live price.
#[test]
fn an_untouched_frame_is_absent_in_every_slot() {
    assert_eq!(
        wire_of(common::UpFrame::ABSENT),
        vec![None; common::LINK_FIELDS.len()]
    );
}

/// What a priced, untraded, unfitted slot owes in its role's block.
fn quoted_block(block: &mut common::UpRole, slot: usize) {
    let (bid, ask) = QUOTES[slot];
    let (bid_qty, ask_qty) = DEPTHS[slot];
    block.bid = Price(bid).to_f64();
    block.ask = Price(ask).to_f64();
    block.bid_qty = Qty(bid_qty).to_f64();
    block.ask_qty = Qty(ask_qty).to_f64();
    block.buy_vol = 0.0;
    block.sell_vol = 0.0;
}

/// Resolved by NAME so the assertion also pins the ordered name list the `schema_hash` digests.
fn slot_of(name: &str) -> usize {
    common::LINK_FIELDS
        .iter()
        .position(|field| *field == name)
        .unwrap_or_else(|| panic!("{name} is not a declared link field"))
}

/// The frame the two tiling windows owe `elapsed` µs past the first open, with neither leg traded
/// nor fitted.
fn expected_at(elapsed: i64) -> Wire {
    let mut expected = common::UpFrame::ABSENT;
    match elapsed {
        // Slot a is open, slot b has rotated but not opened.
        _ if elapsed < WINDOW => {
            quoted_block(&mut expected.cur, A_UP as usize);
            quoted_block(&mut expected.next, B_UP as usize);
        }
        // Slot a's window has closed and no third slot exists, so no leg is next.
        _ if elapsed < 2 * WINDOW => quoted_block(&mut expected.cur, B_UP as usize),
        // The resolution tail belongs to no role: after close the market only settles to 0 or 1, and
        // those prices answer a different question.
        _ => return Vec::new(),
    }
    wire_of(expected)
}

proptest! {
    /// Both legs, each in the block for the role it plays at `now`, for every instant the two windows
    /// tile — and silence once both have closed. Distinct quotes per slot mean a block written from
    /// the wrong leg names the leg it took.
    #[test]
    fn each_up_leg_lands_in_the_block_for_the_role_it_plays(
        elapsed in 0i64..(3 * WINDOW),
    ) {
        let mut linked = publisher_at_the_first_two_windows();
        let sent = spin_and_collect(&mut linked, FIRST_OPEN + elapsed);
        prop_assert_eq!(sent, expected_at(elapsed), "elapsed {}us into the tiling", elapsed);
    }
}

/// Window N's close IS window N+1's open, and the instants either side of that shared microsecond are
/// the only ones the role test can get wrong. Named rather than left to the proptest above: over a
/// 900-second range a generator reaches an exact boundary almost never, so the case that matters
/// would be decided by the seed. An inclusive close puts both legs in the current block for one
/// microsecond; an exclusive open leaves the instant owned by nobody.
#[test]
fn the_shared_microsecond_belongs_to_the_window_that_is_opening() {
    for elapsed in [0, WINDOW - 1, WINDOW, 2 * WINDOW - 1, 2 * WINDOW] {
        let mut linked = publisher_at_the_first_two_windows();
        assert_eq!(
            spin_and_collect(&mut linked, FIRST_OPEN + elapsed),
            expected_at(elapsed),
            "{elapsed}us past the first open"
        );
    }
}

/// A slot's window is unknown until its first rotation, and a publisher that guessed would publish
/// a price for a market it cannot place in time.
#[test]
fn nothing_crosses_the_link_before_the_first_rotation() {
    let mut linked = publisher();
    price_every_slot(&mut linked, FIRST_OPEN);

    assert_eq!(spin_and_collect(&mut linked, FIRST_OPEN + 1), Wire::new());
}

/// A book that is not whole is the resnapshot gap polymarket produces routinely, roughly once every
/// few seconds per market. The role still resolves — the leg is genuinely current — so the frame goes,
/// but with its book slots absent.
///
/// The dangerous instant is not the empty book, which has nothing to leak; it is the half-rebuilt one.
/// A fresh snapshot over a valid book clears it and refills bids before asks, so between those two
/// chunks a real bid sits in a book with no ask. Published, it is a one-sided market that reads on the
/// far side exactly like a live two-sided one.
#[test]
fn book_slots_read_absent_until_the_snapshot_is_whole() {
    let mut linked = publisher();
    dispatch(
        &mut linked,
        InboundMessage::MarketRotation(rotation(A_UP, FIRST_OPEN, FIRST_OPEN + WINDOW, FIRST_OPEN)),
    );

    let mut unpriced = common::UpFrame::ABSENT;
    unpriced.cur.buy_vol = 0.0;
    unpriced.cur.sell_vol = 0.0;
    let unpriced = wire_of(unpriced);
    assert_eq!(
        spin_and_collect(&mut linked, FIRST_OPEN + 1),
        unpriced,
        "a slot that has never been priced"
    );

    price_slot(&mut linked, A_UP, FIRST_OPEN + 2);
    let mut priced = common::UpFrame::ABSENT;
    quoted_block(&mut priced.cur, A_UP as usize);
    assert_eq!(
        spin_and_collect(&mut linked, FIRST_OPEN + 3),
        wire_of(priced)
    );

    // Distinct from this slot's standing quotes, so a leak names which book it came from.
    let (bids, asks) = snapshot_chunks(A_UP, cents(37), cents(39), FIRST_OPEN + 4);
    dispatch(&mut linked, InboundMessage::Book(bids));
    assert_eq!(
        spin_and_collect(&mut linked, FIRST_OPEN + 5),
        unpriced,
        "the resnapshot's bids have landed but its asks have not"
    );

    dispatch(&mut linked, InboundMessage::Book(asks));
    let mut resnapshotted = common::UpFrame::ABSENT;
    quoted_block(&mut resnapshotted.cur, A_UP as usize);
    resnapshotted.cur.bid = Price(cents(37)).to_f64();
    resnapshotted.cur.ask = Price(cents(39)).to_f64();
    assert_eq!(
        spin_and_collect(&mut linked, FIRST_OPEN + 6),
        wire_of(resnapshotted)
    );
}

/// The priced slots read by NAME, which is the one thing an expectation built as an `UpFrame` cannot
/// say: it and the publisher fill the same field, so a `poly_cur_up_bid => ask` line in the schema
/// would satisfy both. Distinct values per side and per role make a crossed frame name what it took —
/// and every way of crossing these leaves a frame that still decodes and still reads as a market.
///
/// The two prints are sized apart from each other and from every depth above, so a buy/sell crossing
/// cannot borrow a quantity that happens to match. They move no book, so the eight quote rows below
/// are the same ones an untraded leg owes.
#[test]
fn each_priced_slot_carries_the_side_and_the_quantity_its_name_claims() {
    let mut linked = publisher_at_the_first_two_windows();
    let (cur_bid, cur_ask) = QUOTES[A_UP as usize];
    for (side, price, qty) in [
        (Side::Buy, cur_ask, shares(3)),
        (Side::Sell, cur_bid, shares(1)),
    ] {
        dispatch(
            &mut linked,
            InboundMessage::Trade(trade(A_UP, price, qty, side, FIRST_OPEN)),
        );
    }
    let sent = spin_and_collect(&mut linked, FIRST_OPEN + 1);

    let (cur_bid_qty, cur_ask_qty) = DEPTHS[A_UP as usize];
    let (next_bid, next_ask) = QUOTES[B_UP as usize];
    let (next_bid_qty, next_ask_qty) = DEPTHS[B_UP as usize];
    for (field, expected) in [
        ("poly_cur_up_bid", Price(cur_bid).to_f64()),
        ("poly_cur_up_ask", Price(cur_ask).to_f64()),
        ("poly_cur_up_bid_qty", Qty(cur_bid_qty).to_f64()),
        ("poly_cur_up_ask_qty", Qty(cur_ask_qty).to_f64()),
        ("poly_cur_up_buy_vol", Qty(shares(3)).to_f64()),
        ("poly_cur_up_sell_vol", Qty(shares(1)).to_f64()),
        ("poly_next_up_bid", Price(next_bid).to_f64()),
        ("poly_next_up_ask", Price(next_ask).to_f64()),
        ("poly_next_up_bid_qty", Qty(next_bid_qty).to_f64()),
        ("poly_next_up_ask_qty", Qty(next_ask_qty).to_f64()),
    ] {
        assert_eq!(sent[slot_of(field)], Some(expected), "{field}");
    }
}

/// The cumulative-volume rule in executable form. Link topics carry STATE, so the volume slots are
/// the running total since the leg's rotation, not that spin's flow: a quiet spin repeats the total
/// rather than reporting zero, and a dropped frame therefore costs nothing. Rotation is the only
/// thing that lowers them.
///
/// Slot b carries this because it holds one role — `next` — on both sides of its own rotation, so a
/// reset is read from the same block that showed the total.
#[test]
fn traded_volume_is_cumulative_since_rotation_not_per_spin() {
    let mut linked = publisher_at_the_first_two_windows();
    let buy_slot = slot_of("poly_next_up_buy_vol");
    let sell_slot = slot_of("poly_next_up_sell_vol");
    let (bid, ask) = QUOTES[B_UP as usize];

    for (index, (side, price)) in [(Side::Buy, ask), (Side::Buy, ask), (Side::Sell, bid)]
        .into_iter()
        .enumerate()
    {
        dispatch(
            &mut linked,
            InboundMessage::Trade(trade(
                B_UP,
                price,
                shares(2),
                side,
                FIRST_OPEN + 10 + index as i64,
            )),
        );
    }

    let traded = spin_and_collect(&mut linked, FIRST_OPEN + 100);
    assert_eq!(traded[buy_slot], Some(4.0));
    assert_eq!(traded[sell_slot], Some(2.0));

    let quiet = spin_and_collect(&mut linked, FIRST_OPEN + 200);
    assert_eq!(
        (quiet[buy_slot], quiet[sell_slot]),
        (traded[buy_slot], traded[sell_slot]),
        "a spin with no prints repeats the running total; reporting this spin's flow would be a delta"
    );

    dispatch(
        &mut linked,
        InboundMessage::MarketRotation(rotation(
            B_UP,
            FIRST_OPEN + 3 * WINDOW,
            FIRST_OPEN + 4 * WINDOW,
            FIRST_OPEN + 300,
        )),
    );
    let rotated = spin_and_collect(&mut linked, FIRST_OPEN + 400);
    assert_eq!(
        (rotated[buy_slot], rotated[sell_slot]),
        (Some(0.0), Some(0.0)),
        "the new window's volume starts at nothing"
    );
}

/// The (A, k) pair is published per side and only once its own side has been fitted: sell aggressors
/// hit the bid, so a run of sells says nothing about ask liquidity. Publishing the pooled or opposite
/// fit there would fabricate depth on a side nobody traded.
#[test]
fn intensity_is_published_per_side_and_only_once_that_side_is_fitted() {
    let mut linked = publisher_at_the_first_two_windows();
    let a_bid = slot_of("poly_cur_up_intensity_a_bid");
    let k_bid = slot_of("poly_cur_up_intensity_k_bid");
    let a_ask = slot_of("poly_cur_up_intensity_a_ask");
    let k_ask = slot_of("poly_cur_up_intensity_k_ask");

    let cold = spin_and_collect(&mut linked, FIRST_OPEN + 1);
    assert_eq!(
        (cold[a_bid], cold[k_bid]),
        (None, None),
        "an empty histogram is not a fit"
    );

    drive_sells_into_the_current_bid(&mut linked);

    let too_soon = spin_and_collect(&mut linked, FIRST_OPEN + REFIT_INTERVAL / 2);
    assert_eq!(
        (too_soon[a_bid], too_soon[k_bid]),
        (None, None),
        "the fit in force is still the empty one until the refit falls due"
    );

    let refitted = spin_and_collect(&mut linked, FIRST_OPEN + REFIT_INTERVAL + 1);
    assert!(
        refitted[a_bid].is_some_and(|a| a > 0.0) && refitted[k_bid].is_some_and(|k| k > 0.0),
        "sells reach the bid, so the bid side fits: {:?}",
        (refitted[a_bid], refitted[k_bid])
    );
    assert_eq!(
        (refitted[a_ask], refitted[k_ask]),
        (None, None),
        "nobody lifted the offer, so the ask side has nothing to publish"
    );
}

/// A published (A, k) must not outlive the prints behind it, and both ways it can are silent.
///
/// Within a window the estimator re-dates its own last answer once decay drops the touch count under
/// the event floor, flagging it stale for exactly this reason; forwarding it would report a market
/// that has gone quiet as one still being hit. At rotation the engine wipes the reach histogram
/// outright, so an estimate cached across it would describe a market that has already resolved — and
/// its warm start would seed the next fit in the wrong basin.
#[test]
fn a_published_fit_never_outlives_the_prints_behind_it() {
    let a_bid = slot_of("poly_cur_up_intensity_a_bid");
    let k_bid = slot_of("poly_cur_up_intensity_k_bid");

    let mut decaying = publisher_at_the_first_two_windows();
    drive_sells_into_the_current_bid(&mut decaying);
    let fitted = spin_and_collect(&mut decaying, FIRST_OPEN + REFIT_INTERVAL + 1);
    assert!(fitted[a_bid].is_some(), "the fit this test is about to age");

    // A half life and a quarter past the last print: six touches decay to under the floor of three.
    let cold = spin_and_collect(&mut decaying, FIRST_OPEN + 150_000_000);
    assert_eq!(
        (cold[a_bid], cold[k_bid]),
        (None, None),
        "a fit the estimator itself marks stale is not a fit"
    );

    let mut rotated = publisher_at_the_first_two_windows();
    drive_sells_into_the_current_bid(&mut rotated);
    assert!(
        spin_and_collect(&mut rotated, FIRST_OPEN + REFIT_INTERVAL + 1)[a_bid].is_some(),
        "the fit this test is about to invalidate"
    );
    // The venue re-announces a slot's window on resubscribe, so the same bounds arriving again is a
    // real event — and it keeps this leg current, so the reset is read from the block it was fitted in.
    dispatch(
        &mut rotated,
        InboundMessage::MarketRotation(rotation(
            A_UP,
            FIRST_OPEN,
            FIRST_OPEN + WINDOW,
            FIRST_OPEN + REFIT_INTERVAL + 2,
        )),
    );
    let wiped = spin_and_collect(&mut rotated, FIRST_OPEN + REFIT_INTERVAL + 3);
    assert_eq!(
        (wiped[a_bid], wiped[k_bid]),
        (None, None),
        "the histogram behind that fit is gone, so the fit goes with it"
    );
}

/// Enough same-side prints to lift one leg's histogram clear of its event floor on the side where
/// the aggressor's counterparty RESTS — a sell prints at the standing bid, a buy at the standing
/// ask. The engine's convention, and the reason each (A, k) pair is named for the resting side
/// rather than the side that crossed. The touch count is the caller's so two drives can leave
/// distinct totals: equal values would let a swapped pair of columns pass the round trip.
fn drive_prints(
    linked: &mut crate::engine_support::LinkedEngine,
    slot: u16,
    side: Side,
    touches: i64,
) {
    let (bid, ask) = QUOTES[usize::from(slot)];
    let price = match side {
        Side::Buy => ask,
        Side::Sell => bid,
    };
    for index in 0..touches {
        dispatch(
            linked,
            InboundMessage::Trade(trade(slot, price, shares(1), side, FIRST_OPEN + 10 + index)),
        );
    }
}

fn drive_sells_into_the_current_bid(linked: &mut crate::engine_support::LinkedEngine) {
    drive_prints(linked, A_UP, Side::Sell, TOUCHES);
}

/// The whole point of the pair: what the publisher banks survives the wire and lands as feature rows
/// on its peer — one row per slot the publisher could fill, and none for the slots it could not.
///
/// This is where a divergence between the two `link_fields()` lists surfaces, and where an off-by-one
/// in the receiver's index map surfaces as a value under the wrong column name. Nothing else would
/// catch either — the engines are separate binaries, a mismatched `schema_hash` rejects every frame
/// at decode, and the operator sees a link that is merely quiet.
#[test]
fn every_finite_slot_decodes_on_the_peer_as_the_feature_column_of_the_same_name() {
    let mut publisher = publisher_at_the_first_two_windows();
    // Three drives with distinct touch counts fill eighteen of the twenty slots, exercising the
    // receiver's map across both blocks and both sides — a mapping only ever tested absent would
    // mislabel a live column the first time it turned finite. The next leg's ask pair stays absent,
    // keeping the other half of the claim — no row at all for an absent slot — in the same frame.
    drive_sells_into_the_current_bid(&mut publisher);
    drive_prints(&mut publisher, A_UP, Side::Buy, TOUCHES - 1);
    drive_prints(&mut publisher, B_UP, Side::Sell, TOUCHES - 2);
    let published = spin_and_take_frame(&mut publisher, FIRST_OPEN + REFIT_INTERVAL + 1)
        .expect("publisher sent");

    let rows = [instrument_row(0, tracker_spec_all(600), 128)];
    let consumer = MicroRecorder::from_spec(
        &recorder_spec::<MicroRecorderParams>(vec![TableKind::Features]),
        engine_view(DurationUs::from_secs(1)),
    );
    let mut expected: Vec<(FeatureId, f64)> = published
        .payload
        .values()
        .iter()
        .enumerate()
        .filter(|(_, value)| value.is_finite())
        .map(|(index, value)| {
            (
                feature_id(consumer.features(), common::LINK_FIELDS[index]),
                *value,
            )
        })
        .collect();
    assert_eq!(
        expected.len(),
        common::LINK_FIELDS.len() - 2,
        "the frame under test must carry every slot the drives above promised, absent pair included,
         else this proves less than it claims"
    );
    let guard = FrameGuard {
        token_hash: LinkHash::of_name(TOKEN),
        strategy_hash: LinkHash::of_name(STRATEGY_ID),
        schema_hash: schema_hash_of_fields(consumer.link_fields()),
    };
    let mut linked = engine_with_link(LinkedSetup {
        instruments: &rows,
        strategy: Box::new(consumer),
        tables: RecordedTables::new(&[TableKind::Features]),
        warmup: DurationUs::ZERO,
    });

    dispatch(
        &mut linked,
        over_the_wire(published, &guard, FIRST_OPEN + REFIT_INTERVAL + 2),
    );

    let mut landed = drain_features(&mut linked);
    landed.sort_by_key(|(feature, _)| feature.0);
    expected.sort_by_key(|(feature, _)| feature.0);
    assert_eq!(landed, expected);
}

/// Encodes as the publisher's link actor would and decodes under the consumer's own guard, so the
/// frame under test crossed the real codec rather than being handed sideways as a struct.
fn over_the_wire(published: OutboundLink, guard: &FrameGuard, when: i64) -> InboundMessage {
    let identity = LinkIdentity {
        token_hash: LinkHash::of_name(TOKEN),
        strategy_hash: LinkHash::of_name(STRATEGY_ID),
        sender_te_hash: LinkHash::of_name(PUBLISHER_TE_ID),
        boot_ts_us: ts(FIRST_OPEN),
    };
    let datagram = LinkDatagram {
        envelope: Envelope::new(identity, published.topic, 1),
        body: LinkBody::Payload(published.payload),
    };
    let mut buffer = [0u8; LINK_MAX_DATAGRAM];
    let len = datagram.encode(&mut buffer);
    let decoded = LinkDatagram::decode(&buffer[..len], guard)
        .expect("the two trading engines of one strategy agree on the link schema");
    let LinkBody::Payload(payload) = decoded.body else {
        panic!(
            "a strategy topic decodes as a payload, got {:?}",
            decoded.body
        );
    };
    InboundMessage::Link(InboundLink {
        frame: LinkFrame {
            origin: LinkOrigin::from(&decoded.envelope),
            payload,
        },
        received_ts_us: ts(when),
        queued_ts_us: ts(when),
    })
}

/// Resolved by NAME: ids are assigned in declaration order, so this also pins that the peer's columns
/// were APPENDED — a mid-list insert would move whatever index they hold.
fn feature_id(names: &[&str], wanted: &str) -> FeatureId {
    let index = names
        .iter()
        .position(|name| *name == wanted)
        .unwrap_or_else(|| panic!("{wanted} is not a declared feature column"));
    FeatureId(index as u16)
}

fn drain_features(linked: &mut crate::engine_support::LinkedEngine) -> Vec<(FeatureId, f64)> {
    let mut values = Vec::new();
    while let Ok(record) = linked.persist.pop() {
        if let PersistRecord::Feature(row) = record {
            values.push((row.feature, row.value));
        }
    }
    values
}

// ---------------------------------------------------------------------------------------------
// The maker half. Everything above is about the wire; everything below is about the orders.
// ---------------------------------------------------------------------------------------------

/// The shipped params, in the units the strategy reads them.
const ORDER_SHARES: f64 = 5.0;
const EDGE_TICKS: u32 = 2;
const QUOTE_STOP_LEAD_MS: u32 = 3_500;
const QUOTE_STOP_LEAD_US: i64 = QUOTE_STOP_LEAD_MS as i64 * 1_000;

/// Deliberately SHORTER than the strategy's own lead. The engine refuses a quote inside its margin
/// and sweeps what is resting, so a strategy stopping only at the margin would look identical from
/// the outside; making the engine's gate strictly later means a quote that stops early stopped
/// because the STRATEGY stopped it.
const ENGINE_MARGIN_MS: i64 = 3_000;

/// The polymarket grid as the preflight stamps it: a two-decimal share step, the venue's five-share
/// order floor, and a ceiling one tick below certainty.
const SHARE_STEP: i64 = FIXED_SCALE / 100;
const MIN_ORDER_SHARES: i64 = 5 * FIXED_SCALE;
const MAX_PRICE: i64 = FIXED_SCALE - TICK;

fn maker_params() -> PolyUpParams {
    PolyUpParams {
        enabled: true,
        order_shares: ORDER_SHARES,
        edge_ticks: EDGE_TICKS,
        quote_stop_lead_ms: QUOTE_STOP_LEAD_MS,
    }
}

/// [`poly_rows`] with the execution stamps the polymarket preflight adds, which the wire tests have
/// no use for and the order tests cannot work without — an unstamped row reads a size floor of zero,
/// and every claim below about the venue's minimum would pass for the wrong reason.
fn maker_rows() -> Vec<InstrumentRow> {
    poly_rows()
        .into_iter()
        .map(|row| InstrumentRow {
            lot_size: Some(Qty(SHARE_STEP)),
            min_qty: Some(Qty(MIN_ORDER_SHARES)),
            max_price: Some(Price(MAX_PRICE)),
            max_num_orders: Some(32),
            ..row
        })
        .collect()
}

/// Every limit wide open except the ones under test, for the reason `recorder_quotes` gives: a quote
/// refused by the price band or a stale book reads here as a strategy that declared nothing, and
/// these tests are about what it DID declare.
fn maker_settings() -> ExecSettings {
    ExecSettings {
        limits: ExecLimits {
            requote_threshold_ticks: 1,
            max_quote_distance_centi_bps: 100_000_000,
            max_book_age: DurationUs::from_secs(3_600),
            max_order_notional_quote: 1_000 * FIXED_SCALE,
        },
        max_orders_per_side: 1,
        // Zero, as the shipped config states it: the base asset IS the position here, so any
        // reserve at all is taken out of what may be sold and a holding of exactly the venue
        // minimum stops being sellable by either route.
        min_base_balance: 0,
        min_quote_balance: 0,
        max_consecutive_rejects: 5,
        max_session_loss_quote: 1_000 * FIXED_SCALE,
        inflight_timeout: DurationUs::from_secs(3_600),
        // No silence sweep: a reconciliation request in the command stream is noise these tests
        // would have to filter rather than a behaviour they are about.
        exec_silence_spins: u32::MAX,
        order_reap_window: DurationUs::from_secs(3_600),
        quote_stop_margin: DurationUs::from_millis(ENGINE_MARGIN_MS),
        flatten_slack_ticks: 2,
        order_budget: OrderBudget::NONE,
        fee_model: FeeModel::BinaryOutcome,
        taker_fee_rate: 0,
        // Polymarket does not lock an open order's funds, so a working slot's reservation stands
        // until the slot terminates and a zero-fill cancel frees it ungated.
        holds_reservations_until_settled: false,
    }
}

/// The same strategy driven against the REAL execution engine, so a declaration is followed all the
/// way to the command that leaves for the venue.
struct Maker {
    engine: HotEngine,
    events: Consumer<UiEvent>,
    commands: Consumer<ExecLaneItem>,
    spin_seq: u64,
}

/// What the strategy declared on one side of one rung, absent meaning it withdrew.
type DeclaredLevel = Option<(Price, Qty)>;

/// One spin's worth of what the engine said: what the strategy declared, what the engine decided to
/// send, and what it refused.
struct SpinReport {
    declared: Vec<(InstrumentId, DomQuote)>,
    commands: Vec<ExecCommand>,
    refusals: Vec<(InstrumentId, Side, RejectOrigin)>,
}

impl SpinReport {
    /// The ladder the strategy asked for on this instrument, as `(bid, ask)` at the only level a
    /// single-order-per-side config has.
    fn ladder(&self, instrument: u16) -> (DeclaredLevel, DeclaredLevel) {
        let quote = self
            .declared
            .iter()
            .rev()
            .find(|(id, _)| id.0 == instrument)
            .map(|(_, quote)| *quote)
            .unwrap_or_else(|| panic!("every instrument's declaration is teed every spin"));
        (quote.bids[0], quote.asks[0])
    }

    fn places(&self) -> Vec<(Side, Price, Qty, OrderStyle)> {
        self.commands
            .iter()
            .filter_map(|command| match command {
                ExecCommand::Place {
                    side,
                    price,
                    qty,
                    style,
                    ..
                } => Some((*side, *price, *qty, *style)),
                _ => None,
            })
            .collect()
    }

    /// Which orders were cancelled, by id rather than by count — a side carrying an inherited order
    /// as well as a quote would let a count agree for the wrong reason.
    fn cancelled_ids(&self) -> Vec<ClientOrderId> {
        self.commands
            .iter()
            .filter_map(|command| match command {
                ExecCommand::Cancel { client_id, .. } => Some(*client_id),
                _ => None,
            })
            .collect()
    }

    fn placed_ids(&self) -> Vec<ClientOrderId> {
        self.commands
            .iter()
            .filter_map(|command| match command {
                ExecCommand::Place { client_id, .. } => Some(*client_id),
                _ => None,
            })
            .collect()
    }
}

impl Maker {
    fn new(params: PolyUpParams) -> Self {
        let rows = maker_rows();
        let strategy = PolyUpPublisher::from_spec(
            &StrategySpec {
                instruments: Instruments::All,
                tables: Vec::new(),
                params,
            },
            engine_view(DurationUs::from_millis(100)),
        );
        let (persistence, _persist) = persist_ring(1_024);
        let (ui_event_sink, events) = ui_event_ring(4_096);
        let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(1_024);
        let engine = HotEngine::new(HotEngineSetup {
            exec: Some(ExecWiring {
                sink: ExecSink::new(commands_producer),
                settings: maker_settings(),
                // Zero because [`FillPen`] addresses this engine's slots with that constant.
                run_nonce: 0,
            }),
            persistence: Some(persistence),
            ui_event_sink,
            ..detached_setup(&rows, Box::new(strategy))
        });
        Self {
            engine,
            events,
            commands,
            spin_seq: 0,
        }
    }

    fn dispatch(&mut self, message: InboundMessage) {
        self.engine.dispatch(pop(0, 0), &message);
    }

    /// One spin at `now`, reporting only what that spin produced. The sequence number advances every
    /// spin as the timer actor's does: a declaration is only live on the spin it was made, so a
    /// repeated seq would let a stale one read as current.
    fn spin(&mut self, now: i64) -> SpinReport {
        while self.events.pop().is_ok() {}
        while self.commands.pop().is_ok() {}
        self.spin_seq += 1;
        self.dispatch(InboundMessage::SpinTick(spin(self.spin_seq, now)));

        let mut report = SpinReport {
            declared: Vec::new(),
            commands: Vec::new(),
            refusals: Vec::new(),
        };
        while let Ok(event) = self.events.pop() {
            match event {
                UiEvent::Quote {
                    instrument, quote, ..
                } => report.declared.push((instrument, quote)),
                UiEvent::Reject {
                    instrument,
                    side,
                    origin,
                    ..
                } => report.refusals.push((instrument, side, origin)),
                _ => {}
            }
        }
        while let Ok(ExecLaneItem::Command(stamped)) = self.commands.pop() {
            report.commands.push(stamped.command);
        }
        report
    }

    /// Stream up, open orders known, balances known. Without all three nothing is ever placed and
    /// every order-level assertion below would pass for the wrong reason.
    fn make_ready(&mut self, when: i64) {
        self.dispatch(stream_ready(InstrumentId(A_UP), when));
        self.dispatch(InboundMessage::Exec(open_orders_snapshot_end(
            InstrumentId(A_UP),
            ts(when),
        )));
        // One base and one quote asset across all four slots, as `poly_rows` declares them.
        self.dispatch(InboundMessage::Account(account_chunk(
            &[
                (AssetId(0), 1_000 * FIXED_SCALE),
                (AssetId(1), 1_000 * FIXED_SCALE),
            ],
            when,
        )));
    }

    /// The venue's half of a placement, so the order the engine banked becomes one it believes is
    /// RESTING — which is what makes a later withdrawal produce a cancel rather than nothing.
    fn ack_placed(&mut self, commands: &[ExecCommand], when: i64) {
        for command in commands {
            let ExecCommand::Place {
                instrument,
                client_id,
                side,
                price,
                ..
            } = command
            else {
                continue;
            };
            self.dispatch(InboundMessage::Exec(ExecEvent {
                kind: ExecKind::AckPlaced,
                status: Some(VenueOrderStatus::New),
                ..exec_event(*instrument, *client_id, *side, price.0, when)
            }));
        }
    }

    fn ack_cancelled(&mut self, commands: &[ExecCommand], when: i64) {
        for command in commands {
            let ExecCommand::Cancel {
                instrument,
                client_id,
            } = command
            else {
                continue;
            };
            self.dispatch(InboundMessage::Exec(ExecEvent {
                kind: ExecKind::ReportCanceled,
                status: Some(VenueOrderStatus::Canceled),
                ..exec_event(*instrument, *client_id, Side::Sell, 0, when)
            }));
        }
    }
}

/// A ready maker whose two window slots tile `[FIRST_OPEN, +2 WINDOW)` and whose four books are
/// priced, exactly as [`publisher_at_the_first_two_windows`] arranges them.
fn armed_maker() -> Maker {
    let mut maker = Maker::new(maker_params());
    for (instrument, open) in TILING {
        maker.dispatch(InboundMessage::MarketRotation(rotation(
            instrument,
            open,
            open + WINDOW,
            FIRST_OPEN - 1_000,
        )));
    }
    for (slot, &(bid, ask)) in QUOTES.iter().enumerate() {
        let (bids, asks) = snapshot_chunks(slot as u16, bid, ask, FIRST_OPEN - 500);
        maker.dispatch(InboundMessage::Book(bids));
        maker.dispatch(InboundMessage::Book(asks));
    }
    maker.make_ready(FIRST_OPEN - 400);
    maker
}

/// A holding deliberately UNEQUAL to `order_shares`, and not a whole number of them: an offer sized
/// off the order size instead of off the ledger is the mistake these tests exist to catch, and at a
/// position of exactly one order's worth the two numbers agree and the pin proves nothing. Reachable
/// — the ledger is seeded at boot from persisted cost basis, so a position can be any size a
/// previous run left behind.
const HELD_SHARES: i64 = 65 * FIXED_SCALE / 10;

/// Opens a long of `shares` on the current up leg through the real fill path.
fn hold(maker: &mut Maker, shares: i64, when: i64) {
    let mut pen = FillPen::new(A_UP);
    for message in pen.fill(Side::Buy, QUOTES[A_UP as usize].0, shares, when) {
        maker.dispatch(message);
    }
}

/// A leg that is merely SUBSCRIBED is not a market this engine may trade, and the venue hands the
/// next window over about a minute early — so there is a slot sitting pre-open at essentially all
/// times. The engine refuses a pre-open quote anyway; declaring one would only fill the refusal
/// stream with an answer the strategy already had.
#[test]
fn a_leg_is_not_quoted_until_the_window_it_hosts_is_open() {
    let mut maker = armed_maker();

    let pre_open = maker.spin(FIRST_OPEN - 1);
    for slot in 0..SLOT_SYMBOLS.len() {
        assert_eq!(
            pre_open.ladder(slot as u16),
            (None, None),
            "slot {slot} has no open window at this instant"
        );
    }
    assert!(
        pre_open.places().is_empty(),
        "and nothing was sent: {:?}",
        pre_open.commands
    );

    let open = maker.spin(FIRST_OPEN + 1);
    assert!(open.ladder(A_UP).0.is_some(), "slot a's window is now open");
    assert_eq!(
        open.ladder(B_UP),
        (None, None),
        "slot b's window is still ahead of it"
    );
    assert_eq!(
        open.ladder(A_DOWN),
        (None, None),
        "the down leg of the open window is published but never traded"
    );
}

/// What the strategy declares and what leaves for the venue are the same order: a post-only bid
/// `edge_ticks` behind the touch, sized in outcome shares.
///
/// Both halves matter. The price is the strategy's, and the size is `order_shares` converted to the
/// venue's share step — a units slip there prices right and trades wrong.
#[test]
fn the_open_leg_is_bid_for_behind_the_touch_and_that_bid_is_what_gets_placed() {
    let mut maker = armed_maker();
    let report = maker.spin(FIRST_OPEN + 1);

    let wanted = (
        Price(QUOTES[A_UP as usize].0 - i64::from(EDGE_TICKS) * TICK),
        Qty((ORDER_SHARES * FIXED_SCALE as f64) as i64),
    );
    assert_eq!(
        report.ladder(A_UP),
        (Some(wanted), None),
        "one bid, {EDGE_TICKS} ticks behind the {} touch, and no offer against a position it does \
         not hold",
        Price(QUOTES[A_UP as usize].0).to_f64()
    );
    assert_eq!(
        report.places(),
        vec![(Side::Buy, wanted.0, wanted.1, OrderStyle::PostOnly)],
        "the declared quote is the order that goes out, post-only — this venue's market orders are \
         the flatten's business and nothing else's"
    );
}

/// The strategy's own stop, proven to be the binding one.
///
/// The engine refuses a quote inside `quote_stop_margin` of the close and sweeps what is resting,
/// so a strategy that simply quoted until the engine said no would look identical from outside. It
/// is not identical: the refusal is edge-triggered per side, but a market that keeps re-declaring
/// through the last seconds of every five-minute window pays for it in cancel/replace churn and in
/// a log that says the same thing all day. Here the engine's margin is SHORTER than the strategy's
/// lead, so the instant quoting stops is the strategy's alone — and no `OutsideWindow` refusal is
/// ever produced, which is the whole point of stopping first.
#[test]
fn quoting_stops_before_the_engine_gate_would_ever_refuse_it() {
    let mut maker = armed_maker();
    let close = FIRST_OPEN + WINDOW;
    let stop = close - QUOTE_STOP_LEAD_US;
    let mut last_quoting = FIRST_OPEN;

    for now in (FIRST_OPEN + 1..close).step_by(100_000) {
        let report = maker.spin(now);
        match report.ladder(A_UP).0.is_some() {
            true => last_quoting = now,
            false => assert!(
                now >= stop,
                "quoting stopped at {now}, which is {}us before the close — earlier than the \
                 {QUOTE_STOP_LEAD_US}us lead asks for",
                close - now
            ),
        }
        for (instrument, side, origin) in &report.refusals {
            assert_ne!(
                *origin,
                RejectOrigin::Local(RejectReason::OutsideWindow),
                "instrument {} {side:?} declared a quote the engine's own window gate refused, at \
                 {now}",
                instrument.0
            );
        }
    }
    assert!(
        last_quoting < stop && last_quoting >= stop - 100_000,
        "the last quoting spin was {last_quoting}, and the stop is {stop}: quoting must run up to \
         the stop and not past it"
    );
}

/// Inventory leaves the way it arrived — passively — and the bid does not come back while it is
/// held. One position at a time is the whole of this strategy's risk model: an engine that never
/// adds to a fill cannot lose more than one order's worth in a window it has to be flat by.
#[test]
fn a_position_is_offered_back_and_never_added_to() {
    let mut maker = armed_maker();
    hold(&mut maker, HELD_SHARES, FIRST_OPEN + 1);

    let report = maker.spin(FIRST_OPEN + 2);
    assert_eq!(
        report.ladder(A_UP),
        (
            None,
            Some((
                Price(QUOTES[A_UP as usize].1 + i64::from(EDGE_TICKS) * TICK),
                Qty(HELD_SHARES)
            ))
        ),
        "the offer is sized to what is actually held, and the bid is gone"
    );
}

/// A holding under the venue's five-share minimum cannot be sold by ANY order, so the strategy does
/// not ask. Left to the engine it would be one refusal per requote, saying nothing an operator can
/// act on: the residue rides to the market's resolution by rule, and the flatten is where that gets
/// said out loud.
#[test]
fn a_residue_below_the_venue_minimum_is_not_offered() {
    let mut maker = armed_maker();
    hold(&mut maker, 3 * FIXED_SCALE, FIRST_OPEN + 1);

    let report = maker.spin(FIRST_OPEN + 2);
    assert_eq!(
        report.ladder(A_UP),
        (None, None),
        "three shares is under the five the venue will accept, and it is still a position — so \
         neither side is quoted"
    );

    let past_stop = maker.spin(FIRST_OPEN + WINDOW - QUOTE_STOP_LEAD_US);
    assert!(
        past_stop.places().is_empty(),
        "and the flatten cannot shed it either: {:?}",
        past_stop.commands
    );
}

/// The ordering the shared order budget makes load-bearing: `max_orders_per_side` is 1, so the
/// resting offer and the marketable order that closes the position compete for the same slot.
///
/// The strategy stops declaring at its stop, which is what puts the cancel out; only once the side
/// is free does the flatten it declares every spin actually become an order. Declaring both at once
/// and hoping is how a position rides through a rotation while a stale offer sits on the book.
#[test]
fn the_offer_comes_down_before_the_flatten_goes_out() {
    let mut maker = armed_maker();
    hold(&mut maker, HELD_SHARES, FIRST_OPEN + 1);

    let quoting = maker.spin(FIRST_OPEN + 2);
    let [offer] = quoting.placed_ids()[..] else {
        panic!("one offer against the position, got {:?}", quoting.commands)
    };
    maker.ack_placed(&quoting.commands, FIRST_OPEN + 3);

    let close = FIRST_OPEN + WINDOW;
    let withdrawing = maker.spin(close - QUOTE_STOP_LEAD_US);
    assert_eq!(
        withdrawing.ladder(A_UP),
        (None, None),
        "past its stop the strategy declares no quote at all"
    );
    assert_eq!(
        withdrawing.cancelled_ids(),
        vec![offer],
        "which pulls that offer specifically"
    );
    assert!(
        withdrawing.places().is_empty(),
        "and nothing marketable goes out while that offer still occupies the side: {:?}",
        withdrawing.commands
    );
    maker.ack_cancelled(&withdrawing.commands, close - QUOTE_STOP_LEAD_US + 1);

    let flattening = maker.spin(close - QUOTE_STOP_LEAD_US + 100_000);
    assert_eq!(
        flattening.places(),
        vec![(
            Side::Sell,
            // Through the bid by the configured slack, which is what a venue holding marketable
            // orders for 250ms before matching them needs.
            Price(QUOTES[A_UP as usize].0 - 2 * TICK),
            Qty(HELD_SHARES),
            OrderStyle::Immediate
        )],
        "the side is free, so the position goes out at market"
    );
}

/// Quoting carries ACROSS a rotation, because what is traded is the role and not a slot.
///
/// The series alternates two slots, so a strategy that resolved its instrument once — at
/// registration, or on the first rotation it saw — would quote a single five-minute window and then
/// go silent for the rest of the run while still publishing both legs perfectly. Nothing else here
/// would notice: every other maker test lives inside one window, and the wire tests do not place
/// orders. Window A's close IS window B's open, so the handover needs no message to arrive for it to
/// happen, and the leg being quoted has to change at that same instant.
#[test]
fn the_market_moves_to_whichever_leg_hosts_the_next_window() {
    let mut maker = armed_maker();

    let first = maker.spin(FIRST_OPEN + 1);
    assert!(
        first.ladder(A_UP).0.is_some(),
        "slot a hosts the open window"
    );
    assert_eq!(
        first.ladder(B_UP),
        (None, None),
        "slot b is still only subscribed"
    );

    let second = maker.spin(FIRST_OPEN + WINDOW + 1);
    assert_eq!(
        second.ladder(A_UP),
        (None, None),
        "slot a's market has closed and only settles now"
    );
    assert_eq!(
        second.ladder(B_UP).0,
        Some((
            Price(QUOTES[B_UP as usize].0 - i64::from(EDGE_TICKS) * TICK),
            Qty((ORDER_SHARES * FIXED_SCALE as f64) as i64)
        )),
        "and the bid has moved to the leg that now hosts the open window, at ITS touch"
    );
}

/// The end state of every window: quoting has stopped, there is nothing to shed, and the engine goes
/// quiet rather than sending a market order looking for a position.
///
/// Worth stating that this pins the PAIR and not the strategy alone. The strategy declares a flatten
/// only while it holds something, but a flatten declared unconditionally would produce exactly this
/// same silence — the engine sizes the order off the ledger and skips a flat instrument. The
/// declaration itself has no observation seam: the desired LADDER is teed to the UI every spin, the
/// desired flatten is not, so nothing outside the hot thread can tell the two strategies apart.
#[test]
fn nothing_marketable_goes_out_while_the_position_is_flat() {
    let mut maker = armed_maker();
    let close = FIRST_OPEN + WINDOW;

    for step in 0..5 {
        let report = maker.spin(close - QUOTE_STOP_LEAD_US + step * 100_000);
        assert_eq!(
            report.ladder(A_UP),
            (None, None),
            "past the stop, nothing is quoted"
        );
        assert!(
            report.places().is_empty(),
            "and nothing is sent: {:?}",
            report.commands
        );
    }
}
