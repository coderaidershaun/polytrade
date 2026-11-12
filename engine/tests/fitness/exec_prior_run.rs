//! FITNESS: an order an EARLIER run of this trading engine left resting is still this account's
//! money, and every execution on it lands in the ledger exactly once with the rows that explain it.
//!
//! A run dies with a post-only quote on the book. The next boot cancels it, but the cancel takes
//! hundreds of milliseconds to reach the venue and whatever is resting fills in the meantime — and
//! the myTrades repair surfaces such a fill later still, through the same path. Discarding it leaves
//! the position permanently short by the fill with no row in the `orders` or `fills` tables, so
//! every risk gate prices inventory the account does not hold and the tape cannot explain the gap
//! afterwards. A dropped BUY under-counts real exposure, which is the direction that goes on to
//! quote itself deeper in.
//!
//! What such a fill must NOT do is join this run's quoting. A prior-run order takes no slot the
//! reconciler counts, because the side it rests on is one this run still has to be able to quote —
//! and the second test here is what keeps the two tables apart.

use std::sync::{Arc, Mutex};

use polysim::config::{RecordedTables, TableKind, TrackerSpec};
use polysim::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use polysim::hot::exec::{ClientIdLayout, ExecSettings, MAX_ORDER_SLOTS, side_base};
use polysim::hot::metrics::MetricsSnapshot;
use polysim::hot::strategy::{Strategy, StrategyCtx};
use polysim::ids::{InstrumentId, Price, Qty, Side};
use polysim::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, ExecLaneItem, Provenance, VenueOrderStatus,
};
use polysim::msg::inbound::{InboundMessage, SpinTick};
use polysim::msg::persist::PersistRecord;
use polysim::registry::InstrumentRow;
use polysim::sink::ExecSink;
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    ONE, detached_setup, drain_orders_and_fills, exec_event, instrument_row, metrics_ring,
    persist_ring_for, pop, spin,
};
use crate::exec_support::drain_commands;

const INSTRUMENT: InstrumentId = InstrumentId(0);

/// The nonce of the run that DIED. Every fitness engine is built with nonce 0, so an id carrying
/// this one addresses a slot that belongs to nobody here — which is exactly what makes the venue's
/// report about it `Provenance::PriorRun`.
const PRIOR_NONCE: u32 = 0xABCD;

/// What the probe read on the spin it ran on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Reading {
    position_base: Qty,
    pnl_quote: i64,
    resting_buy: bool,
}

type Readings = Arc<Mutex<Option<Reading>>>;

/// Reads the engine's own ledger and order view each spin. It declares no quote and banks nothing:
/// the position under test arrives entirely from the venue.
struct LedgerProbe {
    latest: Readings,
}

impl Strategy for LedgerProbe {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        *self.latest.lock().expect("probe mutex poisoned") = Some(Reading {
            position_base: ctx.position_base(INSTRUMENT),
            pnl_quote: ctx.pnl_quote(INSTRUMENT),
            resting_buy: ctx
                .resting(INSTRUMENT, Side::Buy, polysim::hot::exec::QuoteLevel::ZERO)
                .is_some(),
        });
    }
}

struct Fixture {
    engine: HotEngine,
    records: Consumer<PersistRecord>,
    commands: Consumer<ExecLaneItem>,
    metrics: Consumer<MetricsSnapshot>,
    latest: Readings,
}

fn fixture() -> Fixture {
    fixture_with_lane(256)
}

fn fixture_with_lane(lane_capacity: usize) -> Fixture {
    let instruments: [InstrumentRow; 1] = [instrument_row(0, TrackerSpec::default(), 64)];
    let latest: Readings = Arc::new(Mutex::new(None));
    let (persistence, records) = persist_ring_for(
        1_024,
        RecordedTables::new(&[TableKind::Orders, TableKind::Fills]),
    );
    let (metrics_sink, metrics) = metrics_ring(64);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(lane_capacity);
    let strategy = Box::new(LedgerProbe {
        latest: Arc::clone(&latest),
    });
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        metrics_sink,
        exec: Some(ExecWiring {
            sink: ExecSink::new(commands_producer),
            settings: ExecSettings::disabled(),
            run_nonce: 0,
        }),
        ..detached_setup(&instruments, strategy)
    });
    Fixture {
        engine,
        records,
        commands,
        metrics,
        latest,
    }
}

/// One execution the venue reports on an order the previous run left resting. The totals are the
/// venue's ABSOLUTE ones, which is what makes redelivery detectable at all.
fn prior_run_trade(
    slot_index: usize,
    side: Side,
    price: i64,
    last: i64,
    cumulative: i64,
    when: i64,
) -> InboundMessage {
    let client_id = ClientIdLayout {
        run_nonce: PRIOR_NONCE,
    }
    .encode(slot_index, 1);
    InboundMessage::Exec(ExecEvent {
        kind: ExecKind::ReportTrade,
        provenance: Provenance::PriorRun,
        status: Some(VenueOrderStatus::PartiallyFilled),
        qty: Qty(4 * ONE),
        last_price: Price(price),
        last_qty: Qty(last),
        cumulative_qty: Qty(cumulative),
        cumulative_quote: Price(price).notional(Qty(cumulative)),
        ..exec_event(INSTRUMENT, client_id, side, price, when)
    })
}

/// An order of THIS run that the venue names once and then never answers about. It leaves the
/// engine something to chase every spin, which is what fills the bank while nothing drains it.
fn unanswered_order(when: i64) -> InboundMessage {
    let client_id = ClientIdLayout { run_nonce: 0 }.encode(side_base(INSTRUMENT, Side::Buy) + 1, 1);
    InboundMessage::Exec(ExecEvent {
        kind: ExecKind::SnapshotOrder,
        qty: Qty(4 * ONE),
        ..exec_event(INSTRUMENT, client_id, Side::Buy, 100 * ONE, when)
    })
}

fn read(fixture: &mut Fixture, seq: u64, when: i64) -> Reading {
    fixture
        .engine
        .dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(seq, when)));
    fixture
        .latest
        .lock()
        .expect("probe mutex poisoned")
        .expect("the spin ran, so the probe read the ledger")
}

/// FITNESS: a fill on a prior run's order moves the money exactly once however many times the venue
/// redelivers it, banks the rows that explain it, and is still cancelled on sight.
///
/// The three go together deliberately. Booking the fill without the rows leaves a position no tape
/// can reconcile against a venue statement; banking rows without the cancel would leave the order
/// resting; and folding a redelivery twice is a profit that never happened. Only the cumulative
/// totals separate a redelivery from a real second fill, which is why this drives both.
#[test]
fn a_prior_run_fill_moves_the_money_once_banks_its_rows_and_still_cancels() {
    let mut fixture = fixture();

    fixture.engine.dispatch(
        pop(0, 0),
        &prior_run_trade(1, Side::Buy, 100 * ONE, ONE, ONE, 10),
    );
    let first = read(&mut fixture, 0, 20);
    assert_eq!(
        first.position_base,
        Qty(ONE),
        "the venue filled an order this account owns and the position did not move"
    );
    assert_eq!(
        first.pnl_quote,
        -(100 * ONE),
        "no mark yet, so PnL is the bare cost basis — and the cash leg must have moved too"
    );

    let banked = drain_orders_and_fills(&mut fixture.records);
    assert_eq!(
        banked.fills.len(),
        1,
        "one execution, one line item to reconcile against the venue statement: {:?}",
        banked.fills
    );
    let fill = banked.fills[0];
    assert_eq!(fill.provenance, Provenance::PriorRun);
    assert_eq!(fill.booked_qty, Qty(ONE));
    assert_eq!(fill.side, Side::Buy);
    assert!(
        banked
            .orders
            .iter()
            .any(|row| row.client_id == fill.client_id),
        "a fill with no order-level counterpart is exactly the gap the orders table exists to \
         close: {:?}",
        banked.orders
    );

    let commands = drain_commands(&mut fixture.commands);
    assert!(
        commands.iter().any(|command| matches!(
            command,
            ExecCommand::CancelPriorRun {
                instrument: INSTRUMENT
            }
        )),
        "booking the fill must not have cost the cancel — the order is still resting: {commands:?}"
    );

    // The same report twice more, then a genuine second execution carrying a LARGER cumulative.
    for _ in 0..2 {
        fixture.engine.dispatch(
            pop(0, 0),
            &prior_run_trade(1, Side::Buy, 100 * ONE, ONE, ONE, 30),
        );
    }
    let redelivered = read(&mut fixture, 1, 40);
    assert_eq!(
        redelivered.position_base,
        Qty(ONE),
        "a redelivered report carries totals already folded and must move nothing"
    );
    assert_eq!(redelivered.pnl_quote, -(100 * ONE));
    let after_redelivery = drain_orders_and_fills(&mut fixture.records);
    assert!(
        after_redelivery.fills.is_empty() && after_redelivery.orders.is_empty(),
        "a redelivery moved neither state nor totals and must record nothing, got {} order rows \
         and {} fill rows",
        after_redelivery.orders.len(),
        after_redelivery.fills.len()
    );

    fixture.engine.dispatch(
        pop(0, 0),
        &prior_run_trade(1, Side::Buy, 100 * ONE, ONE, 2 * ONE, 50),
    );
    let second = read(&mut fixture, 2, 60);
    assert_eq!(
        second.position_base,
        Qty(2 * ONE),
        "the second execution is a real one — its cumulative advanced"
    );
    assert_eq!(second.pnl_quote, -(200 * ONE));
    assert_eq!(drain_orders_and_fills(&mut fixture.records).fills.len(), 1);
}

/// FITNESS: a prior run's order occupies no slot this run quotes from. It is somebody else's
/// leftover in every sense that matters to the reconciler — it holds no reservation, and the side it
/// rests on is one this engine still has to be able to place on once the cancel lands.
///
/// The client id is deliberately the one addressing this side's FIRST slot, which is also the first
/// slot a fresh claim takes: seating the prior-run order in the shared table would either evict a
/// live order or lose the fill to the orphan path, depending only on which arrived first.
#[test]
fn a_prior_run_order_takes_no_slot_this_run_quotes_from() {
    let mut fixture = fixture();

    fixture.engine.dispatch(
        pop(0, 0),
        &prior_run_trade(
            polysim::hot::exec::side_base(INSTRUMENT, Side::Buy),
            Side::Buy,
            100 * ONE,
            ONE,
            ONE,
            10,
        ),
    );

    let reading = read(&mut fixture, 0, 20);
    assert_eq!(
        reading.position_base,
        Qty(ONE),
        "the fill still has to land — this test is about WHERE the order lives, not whether it paid"
    );
    assert!(
        !reading.resting_buy,
        "an order this run never sent is resting in the slot this run's next buy would claim"
    );
}

/// A lane small enough that the engine outruns it in a few hundred spins, and wide enough that
/// emptying one a spin drains the bank behind it faster than the engine refills it.
const JAMMED_LANE: usize = 64;

/// Each spin chases the unanswered order with a reconciliation and asks for the open-order
/// snapshot, so one spin per slot in the order table banks several times over what any bank sized
/// to that table can hold.
const JAM_SPINS: usize = MAX_ORDER_SLOTS;

/// Commands the engine has managed to bank so far, from the newest snapshot on the metrics lane.
/// Drained every spin so the lane never fills and the reading is never a stale one.
fn banked_so_far(metrics: &mut Consumer<MetricsSnapshot>, previous: u64) -> u64 {
    let mut latest = previous;
    while let Ok(snapshot) = metrics.pop() {
        latest = snapshot.counters.orders_submitted;
    }
    latest
}

/// FITNESS: a prior-run cancel the command bank could not take is still sent once there is room.
///
/// The bank is full exactly when the edge has stopped draining, which is also when a restart is
/// most likely to find inherited orders on the venue — so the two coincide rather than being
/// independent misfortunes. An engine that recorded "this instrument has been cancelled" without
/// having sent anything would never speak of those orders again for the life of the process, and
/// they would rest there through every subsequent market move.
#[test]
fn a_prior_run_cancel_the_full_bank_refused_is_sent_once_there_is_room() {
    let mut fixture = fixture_with_lane(JAMMED_LANE);
    fixture.engine.dispatch(pop(0, 0), &unanswered_order(1_000));

    let mut banked = 0;
    for step in 0..JAM_SPINS {
        fixture.engine.dispatch(
            pop(0, 0),
            &InboundMessage::SpinTick(spin(step as u64, 1_010 + 10 * step as i64)),
        );
        banked = banked_so_far(&mut fixture.metrics, banked);
    }

    // A precondition, not decoration: with room left in the bank the cancel below would go straight
    // out and this test would pass without ever reaching the case it exists for.
    let jammed = banked;
    for step in JAM_SPINS..JAM_SPINS + 4 {
        fixture.engine.dispatch(
            pop(0, 0),
            &InboundMessage::SpinTick(spin(step as u64, 1_010 + 10 * step as i64)),
        );
        banked = banked_so_far(&mut fixture.metrics, banked);
    }
    assert_eq!(
        banked, jammed,
        "the bank still had room after {JAM_SPINS} undrained spins, so this test proves nothing"
    );

    fixture.engine.dispatch(
        pop(0, 0),
        &prior_run_trade(1, Side::Buy, 100 * ONE, ONE, ONE, 100_000),
    );

    // The edge catches up. Emptying a whole lane each spin drains the bank behind it.
    let mut sent = Vec::new();
    for step in 0..JAM_SPINS {
        sent.extend(drain_commands(&mut fixture.commands));
        fixture.engine.dispatch(
            pop(0, 0),
            &InboundMessage::SpinTick(spin(step as u64, 200_000 + 10 * step as i64)),
        );
        banked = banked_so_far(&mut fixture.metrics, banked);
    }
    assert!(
        banked > jammed,
        "the bank never drained, so the retry below had nowhere to go either"
    );

    // The venue names the inherited order again, exactly as its next reconciliation snapshot would.
    fixture.engine.dispatch(
        pop(0, 0),
        &prior_run_trade(1, Side::Buy, 100 * ONE, ONE, 2 * ONE, 400_000),
    );
    fixture
        .engine
        .dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(0, 400_010)));
    sent.extend(drain_commands(&mut fixture.commands));

    assert!(
        sent.iter().any(|command| matches!(
            command,
            ExecCommand::CancelPriorRun {
                instrument: INSTRUMENT
            }
        )),
        "the bank refused the cancel once and the engine never asked again — the inherited order \
         rests on the venue for the life of the process: {sent:?}"
    );
}
