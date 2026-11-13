//! The orders that an earlier run left resting: each of their fills reaches the ledger once, none
//! takes a slot of this run, a full table halts quoting, and a cancel the bank refused is asked again.

use std::sync::{Arc, Mutex};

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{
    ClientIdLayout, ExecSettings, HaltReason, MAX_ORDER_SLOTS, QuoteLevel, side_base,
};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, ExecLaneItem, Provenance, VenueOrderStatus,
};
use engine::msg::ids::{AssetId, InstrumentId, Price, Qty, Side, TradeId};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::persist::{IncidentKind, IncidentRow, PersistRecord};
use engine::msg::time::Stamper;
use engine::msg::ui::{UiEvent, UiHalt};
use engine::runtime::config::{RecordedTables, TableKind, TrackerSpec};
use engine::runtime::registry::InstrumentRow;
use engine::venues::exec::open_orders_snapshot_end;
use proptest::prelude::*;
use proptest::strategy::Strategy as _;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    ONE, account_chunk, detached_setup, drain_orders_and_fills, exec_event, instrument_row,
    persist_ring_for, spin, stream_ready, ts, ui_event_ring, venue_ts,
};
use crate::exec_support::drain_commands;

const INSTRUMENT: InstrumentId = InstrumentId(0);

/// The nonce of the run that DIED. Every fitness engine is built with nonce 0, so an id carrying
/// this one addresses a slot that belongs to nobody here — which is exactly what makes the venue's
/// report about it `Provenance::PriorRun`.
const PRIOR_NONCE: u32 = 0xABCD;

/// The run of the generated cases, and the run before it whose orders the venue still holds.
const RUN_NONCE: u32 = 7;
const EARLIER_NONCE: u32 = 6;

const TENTH: i64 = ONE / 10;
const HUNDREDTH: i64 = ONE / 100;

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
            pnl_quote: ctx.pnl_quote(INSTRUMENT).mantissa(),
            resting_buy: ctx
                .resting(INSTRUMENT, Side::Buy, QuoteLevel::ZERO)
                .is_some(),
        });
    }
}

struct Fixture {
    engine: HotEngine,
    records: Consumer<PersistRecord>,
    commands: Consumer<ExecLaneItem>,
    ui_events: Consumer<UiEvent>,
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
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(lane_capacity);
    let (ui_event_sink, ui_events) = ui_event_ring(4_096);
    let strategy = Box::new(LedgerProbe {
        latest: Arc::clone(&latest),
    });
    let engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        ui_event_sink,
        exec: Some(ExecWiring {
            sink: ExecSink::new(commands_producer, Stamper::Off),
            settings: ExecSettings::disabled(),
            run_nonce: 0,
        }),
        ..detached_setup(&instruments, strategy)
    });
    Fixture {
        engine,
        records,
        commands,
        ui_events,
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
    InboundMessage::new(
        ts(when),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::ReportTrade,
            provenance: Provenance::PriorRun,
            status: Some(VenueOrderStatus::PartiallyFilled),
            qty: Qty::from_mantissa(4 * ONE),
            last_price: Price::from_mantissa(price),
            last_qty: Qty::from_mantissa(last),
            cumulative_qty: Qty::from_mantissa(cumulative),
            cumulative_quote: Price::from_mantissa(price).notional(Qty::from_mantissa(cumulative)),
            ..exec_event(INSTRUMENT, client_id, side, price, when)
        }),
    )
}

/// An order of THIS run that the venue names once and then never answers about. It leaves the
/// engine something to chase every spin, which is what fills the bank while nothing drains it.
fn unanswered_order(when: i64) -> InboundMessage {
    let client_id = ClientIdLayout { run_nonce: 0 }.encode(side_base(INSTRUMENT, Side::Buy) + 1, 1);
    InboundMessage::new(
        ts(when),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::SnapshotOrder,
            qty: Qty::from_mantissa(4 * ONE),
            ..exec_event(INSTRUMENT, client_id, Side::Buy, 100 * ONE, when)
        }),
    )
}

fn read(fixture: &mut Fixture, seq: u64, when: i64) -> Reading {
    fixture.engine.dispatch(&spin(seq, when));
    fixture
        .latest
        .lock()
        .expect("probe mutex poisoned")
        .expect("the spin ran, so the probe read the ledger")
}

/// The halt that the last execution frame of the workstation shows.
fn halt_reason(ui_events: &mut Consumer<UiEvent>) -> Option<HaltReason> {
    let halt = std::iter::from_fn(|| ui_events.pop().ok())
        .filter_map(|event| match event {
            UiEvent::Execution { halt, .. } => Some(halt),
            _ => None,
        })
        .last();
    match halt {
        Some(UiHalt::Halted { reason, .. }) => Some(reason),
        _ => None,
    }
}

fn incidents(records: &mut Consumer<PersistRecord>) -> Vec<IncidentRow> {
    std::iter::from_fn(|| records.pop().ok())
        .filter_map(|record| match record {
            PersistRecord::Incident(row) => Some(row),
            _ => None,
        })
        .collect()
}

/// FITNESS: a report of an order of an earlier run that finds the prior-run table full halts
/// quoting, and the orders the table holds keep their fills. The ledger takes nothing from the
/// report that it could only place by a guess, and the tape keeps the fill. A table that dropped the
/// report would leave the position short by a fill the account holds, with nothing to show it.
#[test]
fn prior_run_overflow_halts() {
    let mut fixture = fixture();
    let earlier = ClientIdLayout {
        run_nonce: EARLIER_NONCE,
    };
    let report = |order: usize, cumulative: i64, when: i64| ExecEvent {
        kind: ExecKind::ReportTrade,
        provenance: Provenance::PriorRun,
        status: Some(VenueOrderStatus::PartiallyFilled),
        trade_id: Some(TradeId(order as i64 + 1)),
        qty: Qty::from_mantissa(ONE),
        last_price: Price::from_mantissa(100 * ONE),
        last_qty: Qty::from_mantissa(cumulative),
        cumulative_qty: Qty::from_mantissa(cumulative),
        cumulative_quote: Price::from_mantissa(100 * ONE).notional(Qty::from_mantissa(cumulative)),
        ..exec_event(
            INSTRUMENT,
            earlier.encode(order, 1),
            Side::Buy,
            100 * ONE,
            when,
        )
    };
    // One instrument gives the table 16 entries, and a partial fill keeps each order working, so no
    // entry is closed for the 17th to take.
    for order in 0..16 {
        let when = 10 + order as i64;
        fixture.engine.dispatch(&InboundMessage::new(
            ts(when),
            MessageBody::Exec(report(order, TENTH, when)),
        ));
    }
    let held = read(&mut fixture, 0, 100);
    assert_eq!(held.position_base, Qty::from_mantissa(16 * TENTH));
    assert_eq!(halt_reason(&mut fixture.ui_events), None);
    assert_eq!(incidents(&mut fixture.records), []);

    let seventeenth = report(16, 2 * TENTH, 200);
    fixture.engine.dispatch(&InboundMessage::new(
        ts(200),
        MessageBody::Exec(seventeenth),
    ));
    let after = read(&mut fixture, 1, 300);
    assert_eq!(
        halt_reason(&mut fixture.ui_events),
        Some(HaltReason::PriorRunOverflow),
        "a prior-run order with no free entry halts quoting"
    );
    assert_eq!(
        after.position_base,
        Qty::from_mantissa(16 * TENTH),
        "the 16 orders keep their fills, and the report with no entry moves nothing"
    );
    assert_eq!(
        incidents(&mut fixture.records),
        [IncidentRow {
            kind: IncidentKind::OrphanFill,
            instrument: INSTRUMENT,
            client_id: Some(seventeenth.client_id),
            trade_id: Some(TradeId(17)),
            qty: Qty::from_mantissa(2 * TENTH),
            amount: 0,
            window_close_ts_us: None,
            exchange_ts_us: venue_ts(200),
            received_ts_us: ts(200),
        }],
        "the fill that no entry holds stays on the tape"
    );
    assert_eq!(fixture.engine.counters().orphan_fills, 1);
    a_prior_run_fill_that_names_no_instrument_halts();
}

/// A prior-run report with a fill and no instrument has no entry to fold into, and no instrument
/// for a record. Quoting halts as for any fill that no slot holds, and the count keeps it.
fn a_prior_run_fill_that_names_no_instrument_halts() {
    let mut fixture = fixture();
    let MessageBody::Exec(mut report) = prior_run_trade(1, Side::Buy, 100 * ONE, ONE, ONE, 10).body
    else {
        unreachable!("a prior-run trade is an execution message");
    };
    report.instrument = None;
    fixture
        .engine
        .dispatch(&InboundMessage::new(ts(10), MessageBody::Exec(report)));
    let after = read(&mut fixture, 0, 20);
    assert_eq!(
        halt_reason(&mut fixture.ui_events),
        Some(HaltReason::OrphanFill),
        "a fill that names no instrument halts quoting, and never ends in a log line only"
    );
    assert_eq!(after.position_base, Qty::from_mantissa(0));
    assert_eq!(
        incidents(&mut fixture.records),
        [],
        "a record needs an instrument"
    );
    assert_eq!(fixture.engine.counters().orphan_fills, 1);
}

/// One order of the earlier run in a generated case: where it rests, its trades as steps of its
/// total, and whether the venue then cancels it.
#[derive(Debug, Clone)]
struct EarlierOrder {
    instrument: u16,
    side: Side,
    price: i64,
    steps: Vec<i64>,
    is_canceled: bool,
}

/// Each report the venue sends about `order`, number `index`: a trade report for each step, and a
/// cancel that carries the order's final total.
fn reports_of(index: usize, order: &EarlierOrder) -> Vec<ExecEvent> {
    let client_id = ClientIdLayout {
        run_nonce: EARLIER_NONCE,
    }
    .encode(index, 1);
    let price = Price::from_mantissa(order.price);
    let base = |kind, cumulative: i64| ExecEvent {
        kind,
        provenance: Provenance::PriorRun,
        qty: Qty::from_mantissa(4 * ONE),
        cumulative_qty: Qty::from_mantissa(cumulative),
        cumulative_quote: price.notional(Qty::from_mantissa(cumulative)),
        ..exec_event(
            InstrumentId(order.instrument),
            client_id,
            order.side,
            order.price,
            1,
        )
    };
    let mut reports = Vec::new();
    let mut cumulative = 0;
    for (trade, step) in (0..).zip(&order.steps) {
        cumulative += step;
        reports.push(ExecEvent {
            status: Some(VenueOrderStatus::PartiallyFilled),
            trade_id: Some(TradeId(1_000 * index as i64 + trade)),
            last_price: price,
            last_qty: Qty::from_mantissa(*step),
            ..base(ExecKind::ReportTrade, cumulative)
        });
    }
    if order.is_canceled {
        reports.push(ExecEvent {
            status: Some(VenueOrderStatus::Canceled),
            ..base(ExecKind::ReportCanceled, cumulative)
        });
    }
    reports
}

/// The position and the PnL of both instruments at a spin, and whether this run's table holds a
/// resting order at any level of either.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SpinReading {
    money: [(Qty, i64); 2],
    is_any_resting: bool,
}

#[derive(Default)]
struct TwoInstrumentProbe {
    latest: Arc<Mutex<Option<SpinReading>>>,
}

impl Strategy for TwoInstrumentProbe {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let instruments = [InstrumentId(0), InstrumentId(1)];
        let money = instruments.map(|instrument| {
            (
                ctx.position_base(instrument),
                ctx.pnl_quote(instrument).mantissa(),
            )
        });
        let is_any_resting = instruments.iter().any(|&instrument| {
            [Side::Buy, Side::Sell].iter().any(|&side| {
                QuoteLevel::ALL
                    .iter()
                    .any(|&level| ctx.resting(instrument, side, level).is_some())
            })
        });
        *self.latest.lock().expect("probe mutex poisoned") = Some(SpinReading {
            money,
            is_any_resting,
        });
    }
}

fn fold_earlier_orders(
    orders: &[EarlierOrder],
    delivered: &[ExecEvent],
) -> Result<(), TestCaseError> {
    let instruments: Vec<InstrumentRow> = (0..2)
        .map(|id| instrument_row(id, TrackerSpec::default(), 64))
        .collect();
    let (persistence, mut records) = persist_ring_for(
        4_096,
        RecordedTables::new(&[TableKind::Orders, TableKind::Fills]),
    );
    let (commands_producer, mut commands) = RingBuffer::<ExecLaneItem>::new(1_024);
    let probe = TwoInstrumentProbe::default();
    let latest = Arc::clone(&probe.latest);
    let mut engine = HotEngine::new(HotEngineSetup {
        persistence: Some(persistence),
        exec: Some(ExecWiring {
            sink: ExecSink::new(commands_producer, Stamper::Off),
            settings: ExecSettings::disabled(),
            run_nonce: RUN_NONCE,
        }),
        ..detached_setup(&instruments, Box::new(probe))
    });
    engine.dispatch(&stream_ready(InstrumentId(0), 1));
    engine.dispatch(&InboundMessage::new(
        ts(1),
        MessageBody::Account(account_chunk(&[(AssetId(0), ONE), (AssetId(1), ONE)], 1)),
    ));
    for instrument in [InstrumentId(0), InstrumentId(1)] {
        engine.dispatch(&InboundMessage::new(
            ts(1),
            MessageBody::Exec(open_orders_snapshot_end(instrument, ts(1))),
        ));
    }

    let mut held = vec![0; orders.len()];
    let mut steps = 0;
    for (step, report) in (0..).zip(delivered) {
        let order = ClientIdLayout::slot_of(report.client_id);
        if report.cumulative_qty.mantissa() > held[order] {
            held[order] = report.cumulative_qty.mantissa();
            steps += 1;
        }
        engine.dispatch(&InboundMessage::new(
            ts(1_000 * (step + 2)),
            MessageBody::Exec(*report),
        ));
    }
    engine.dispatch(&spin(0, 1_000 * (delivered.len() as i64 + 2)));

    let mut expected = [(0, 0); 2];
    for (order, total) in orders.iter().zip(&held) {
        let notional = Price::from_mantissa(order.price)
            .notional(Qty::from_mantissa(*total))
            .mantissa();
        let (position, cash) = &mut expected[usize::from(order.instrument)];
        match order.side {
            Side::Buy => (*position, *cash) = (*position + total, *cash - notional),
            Side::Sell => (*position, *cash) = (*position - total, *cash + notional),
        }
    }
    let SpinReading {
        money,
        is_any_resting,
    } = latest
        .lock()
        .expect("probe mutex poisoned")
        .expect("the spin ran the probe");
    prop_assert_eq!(
        money,
        expected.map(|(position, cash)| (Qty::from_mantissa(position), cash)),
        "each instrument holds the largest total of each of its orders, paid once"
    );
    prop_assert!(
        !is_any_resting,
        "an order of the earlier run took a slot that this run quotes from"
    );
    let fill_rows = std::iter::from_fn(|| records.pop().ok())
        .filter(|record| matches!(record, PersistRecord::Fill(_)))
        .count();
    prop_assert_eq!(fill_rows, steps, "one fill row for each step of a total");
    let cancels = drain_commands(&mut commands)
        .into_iter()
        .filter_map(|command| match command {
            ExecCommand::CancelPriorRun { instrument } => Some(instrument.0),
            _ => None,
        })
        .fold([0; 2], |mut counts, instrument| {
            counts[usize::from(instrument)] += 1;
            counts
        });
    let has_orders = [0, 1]
        .map(|instrument| usize::from(orders.iter().any(|order| order.instrument == instrument)));
    prop_assert_eq!(
        cancels,
        has_orders,
        "one cancel for each instrument that holds an order of the earlier run"
    );
    Ok(())
}

/// FITNESS: the fills of each order of an earlier run reach the ledger once, however the venue
/// repeats them, interleaves them across orders, or sends them after the order's cancel. Each
/// instrument that holds such an order gets one cancel, and no such order takes a slot of this run.
/// A report that moved money twice, or not at all, leaves a position the venue does not hold.
#[test]
fn prior_run_fills_reach_ledger_once() {
    a_prior_run_fill_moves_the_money_once_banks_its_rows_and_still_cancels();
    a_prior_run_order_takes_no_slot_this_run_quotes_from();
    let order = (
        0u16..2,
        prop_oneof![Just(Side::Buy), Just(Side::Sell)],
        90i64..110,
        prop::collection::vec(1i64..20, 1..=4),
        any::<bool>(),
    )
        .prop_map(
            |(instrument, side, price, steps, is_canceled)| EarlierOrder {
                instrument,
                side,
                price: price * ONE,
                steps: steps.into_iter().map(|step| step * HUNDREDTH).collect(),
                is_canceled,
            },
        );
    let cases = prop::collection::vec(order, 1..=6).prop_flat_map(|orders| {
        let reports: Vec<ExecEvent> = orders
            .iter()
            .enumerate()
            .flat_map(|(index, order)| reports_of(index, order))
            .collect();
        (
            Just(orders),
            Just(reports).prop_shuffle(),
            prop::collection::vec((0usize..64, 0usize..64), 0..8),
        )
    });
    let mut runner = TestRunner::new(Config {
        cases: 64,
        failure_persistence: None,
        ..Config::default()
    });
    runner
        .run(&cases, |(orders, shuffled, copies)| {
            let mut delivered = shuffled.clone();
            for (from, to) in copies {
                let copy = shuffled[from % shuffled.len()];
                delivered.insert(to % (delivered.len() + 1), copy);
            }
            fold_earlier_orders(&orders, &delivered)
        })
        .expect("each fill of an order of an earlier run reaches the ledger once");
}

/// FITNESS: a fill on a prior run's order moves the money exactly once however many times the venue
/// redelivers it, banks the rows that explain it, and is still cancelled on sight.
///
/// The three go together deliberately. Booking the fill without the rows leaves a position no tape
/// can reconcile against a venue statement; banking rows without the cancel would leave the order
/// resting; and folding a redelivery twice is a profit that never happened. Only the cumulative
/// totals separate a redelivery from a real second fill, which is why this drives both.
fn a_prior_run_fill_moves_the_money_once_banks_its_rows_and_still_cancels() {
    let mut fixture = fixture();

    fixture
        .engine
        .dispatch(&prior_run_trade(1, Side::Buy, 100 * ONE, ONE, ONE, 10));
    let first = read(&mut fixture, 0, 20);
    assert_eq!(
        first.position_base,
        Qty::from_mantissa(ONE),
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
    assert_eq!(fill.booked_qty, Qty::from_mantissa(ONE));
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
        fixture
            .engine
            .dispatch(&prior_run_trade(1, Side::Buy, 100 * ONE, ONE, ONE, 30));
    }
    let redelivered = read(&mut fixture, 1, 40);
    assert_eq!(
        redelivered.position_base,
        Qty::from_mantissa(ONE),
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

    fixture
        .engine
        .dispatch(&prior_run_trade(1, Side::Buy, 100 * ONE, ONE, 2 * ONE, 50));
    let second = read(&mut fixture, 2, 60);
    assert_eq!(
        second.position_base,
        Qty::from_mantissa(2 * ONE),
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
fn a_prior_run_order_takes_no_slot_this_run_quotes_from() {
    let mut fixture = fixture();

    fixture.engine.dispatch(&prior_run_trade(
        engine::hot::exec::side_base(INSTRUMENT, Side::Buy),
        Side::Buy,
        100 * ONE,
        ONE,
        ONE,
        10,
    ));

    let reading = read(&mut fixture, 0, 20);
    assert_eq!(
        reading.position_base,
        Qty::from_mantissa(ONE),
        "the fill still has to land — this test is about WHERE the order lives, not whether it paid"
    );
    assert!(
        !reading.resting_buy,
        "an order this run never sent is resting in the slot this run's next buy would claim"
    );
}

/// A lane small enough that the engine outruns it in a few spins, and wide enough that emptying one
/// a spin drains the bank behind it faster than the engine refills it.
const JAMMED_LANE: usize = 64;

/// Commands the engine has managed to bank so far: the count each counter sample of a spin carries.
fn banked_so_far(engine: &HotEngine) -> u64 {
    engine.counters().orders_submitted
}

/// CONTRACT: a cancel of an earlier run's orders that the full command bank refused is asked again
/// at the next report of such an order, and goes out once there is room. The bank is full exactly
/// when the edge has stopped draining, which is also when a restart is most likely to find inherited
/// orders on the venue. An engine that took the refused cancel for a sent one would never speak of
/// those orders again, and they would rest there through each later market move.
#[test]
fn prior_run_cancel_refused_by_a_full_bank_is_sent_later() {
    let mut fixture = fixture_with_lane(JAMMED_LANE);
    fixture.engine.dispatch(&unanswered_order(1_000));

    // The lane takes its 64 and nobody drains it, so the bank fills behind it. Full is when its
    // count stops rising for four spins in a row.
    let (mut banked, mut still_spins, mut seq) = (banked_so_far(&fixture.engine), 0, 0);
    while still_spins < 4 {
        assert!(
            seq < MAX_ORDER_SLOTS as u64,
            "the bank still had room after {seq} undrained spins"
        );
        fixture
            .engine
            .dispatch(&spin(seq, 1_010 + 10_000 * seq as i64));
        seq += 1;
        let now = banked_so_far(&fixture.engine);
        still_spins = if now == banked { still_spins + 1 } else { 0 };
        banked = now;
    }
    let jammed = banked;

    fixture
        .engine
        .dispatch(&prior_run_trade(1, Side::Buy, 100 * ONE, ONE, ONE, 100_000));

    // The edge catches up. Emptying a whole lane each spin drains the bank behind it.
    let mut sent = Vec::new();
    for step in 0..16 {
        sent.extend(drain_commands(&mut fixture.commands));
        fixture
            .engine
            .dispatch(&spin(seq + step, 200_000 + 10 * step as i64));
    }
    sent.extend(drain_commands(&mut fixture.commands));
    assert!(
        banked_so_far(&fixture.engine) > jammed,
        "the bank never drained, so the cancel below had nowhere to go either"
    );
    assert!(
        !sent
            .iter()
            .any(|command| matches!(command, ExecCommand::CancelPriorRun { .. })),
        "the full bank refused the cancel, so none can have gone out yet: {sent:?}"
    );

    // The venue names the inherited order again, exactly as its next report would.
    fixture.engine.dispatch(&prior_run_trade(
        1,
        Side::Buy,
        100 * ONE,
        ONE,
        2 * ONE,
        400_000,
    ));
    let reading = read(&mut fixture, seq + 16, 400_010);
    sent.extend(drain_commands(&mut fixture.commands));

    let cancels = sent
        .iter()
        .filter(|command| {
            matches!(
                command,
                ExecCommand::CancelPriorRun {
                    instrument: INSTRUMENT
                }
            )
        })
        .count();
    assert_eq!(
        cancels, 1,
        "the cancel the bank refused goes out once, at the next report of the inherited order: \
         {sent:?}"
    );
    assert_eq!(
        halt_reason(&mut fixture.ui_events),
        Some(HaltReason::CommandBankOverflow),
        "quoting stays halted until a person starts it again"
    );
    assert_eq!(
        reading.position_base,
        Qty::from_mantissa(2 * ONE),
        "the ledger moved once for each step of the inherited order"
    );
}
