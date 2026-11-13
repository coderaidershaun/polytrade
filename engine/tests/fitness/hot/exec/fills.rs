//! Each venue report moves money once: a quantity by the venue's running totals, a fee once for
//! each trade, and an event the engine cannot place either halts quoting or leaves a typed record.

use std::sync::{Arc, Mutex};

use engine::hot::dispatch::HotEngine;
use engine::hot::exec::{ClientIdLayout, side_base};
use engine::hot::strategy::{Fill, Strategy, StrategyCtx};
use engine::msg::exec::{ExecEvent, ExecKind, VenueOrderStatus};
use engine::msg::ids::{AssetId, InstrumentId, Notional, Price, Qty, Side, TradeId};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::persist::PersistRecord;
use engine::runtime::config::{RecordedTables, TableKind, TrackerSpec};
use proptest::prelude::*;
use proptest::strategy::Strategy as _;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::Consumer;

use crate::engine_support::{
    FillPen, ONE, engine_without_warmup, exec_event, instrument_row, persist_ring,
    persist_ring_for, spin, strategy_log_ring, ts,
};
use crate::hot::position_ledger::{
    Phase, bare_row, dispatch_all, probe_engine, reseat_book, spin_reading, take_readings,
};

/// The price of each fill of the generated runs, so the quote of a run is its quantity times it.
const PRICE: i64 = 100;

/// Keeps each fill a callback hands it, with the position that the ledger shows inside that
/// callback, and the position and cash of instrument 0 at the last spin.
#[derive(Default)]
struct FillProbe {
    fills: Arc<Mutex<Vec<(Qty, Qty)>>>,
    at_spin: Arc<Mutex<Option<(Qty, i64)>>>,
}

impl Strategy for FillProbe {
    fn on_fill(&mut self, ctx: &mut StrategyCtx<'_>, fill: &Fill) {
        let held = ctx.position_base(fill.instrument);
        self.fills
            .lock()
            .expect("fills mutex poisoned")
            .push((fill.qty, held));
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let instrument = InstrumentId(0);
        let cash = ctx.pnl_quote(instrument).mantissa() - ctx.exposure_quote(instrument).mantissa();
        *self.at_spin.lock().expect("spin mutex poisoned") =
            Some((ctx.position_base(instrument), cash));
    }
}

/// An engine over instrument 0 that records fill rows, with one resting buy of 1.0 at 100 that
/// the venue named.
fn engine_with_resting_buy(
    strategy: Box<dyn Strategy>,
) -> (HotEngine, Consumer<PersistRecord>, ExecEvent) {
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let (persistence, rows) = persist_ring_for(4_096, RecordedTables::new(&[TableKind::Fills]));
    let (log_sink, _logs) = strategy_log_ring(64);
    let mut engine = engine_without_warmup(&instruments, strategy, persistence, log_sink);
    dispatch_all(&mut engine, &reseat_book(0, 99 * ONE, 101 * ONE, 0));
    let client_id =
        ClientIdLayout { run_nonce: 0 }.encode(side_base(InstrumentId(0), Side::Buy), 1);
    let order = ExecEvent {
        kind: ExecKind::SnapshotOrder,
        status: Some(VenueOrderStatus::New),
        qty: Qty::from_mantissa(ONE),
        ..exec_event(InstrumentId(0), client_id, Side::Buy, PRICE * ONE, 1)
    };
    engine.dispatch(&InboundMessage::new(ts(1), MessageBody::Exec(order)));
    (engine, rows, order)
}

/// The k-th trade report of the run, with its own trade id, its step as its last quantity, and no
/// fee.
fn trade_report(order: ExecEvent, trade: i64, last: i64, cumulative: i64) -> ExecEvent {
    ExecEvent {
        kind: ExecKind::ReportTrade,
        status: Some(VenueOrderStatus::PartiallyFilled),
        trade_id: Some(TradeId(trade)),
        last_price: Price::from_mantissa(PRICE * ONE),
        last_qty: Qty::from_mantissa(last),
        cumulative_qty: Qty::from_mantissa(cumulative),
        cumulative_quote: Notional::from_mantissa(cumulative * PRICE),
        ..order
    }
}

fn fold_reports(steps: &[i64], copies: &[(usize, usize)]) -> Result<(), TestCaseError> {
    let (fills, at_spin) = (Arc::new(Mutex::new(Vec::new())), Arc::new(Mutex::new(None)));
    let (mut engine, mut rows, order) = engine_with_resting_buy(Box::new(FillProbe {
        fills: Arc::clone(&fills),
        at_spin: Arc::clone(&at_spin),
    }));
    let mut reports = Vec::new();
    let mut cumulative = 0;
    for (trade, step) in (1..).zip(steps) {
        cumulative += step;
        reports.push(trade_report(order, trade, *step, cumulative));
    }
    let mut delivered = reports.clone();
    for (from, to) in copies {
        let copy = reports[from % reports.len()];
        delivered.insert(to % (delivered.len() + 1), copy);
    }
    let mut held = 0;
    let mut expected_fills = Vec::new();
    for (step, report) in (0..).zip(&delivered) {
        engine.dispatch(&InboundMessage::new(
            ts(1_000 * (step + 2)),
            MessageBody::Exec(*report),
        ));
        let total = report.cumulative_qty.mantissa();
        if total > held {
            expected_fills.push((Qty::from_mantissa(total - held), Qty::from_mantissa(total)));
            held = total;
        }
    }
    let fill_rows = std::iter::from_fn(|| rows.pop().ok())
        .filter(|row| matches!(row, PersistRecord::Fill(_)))
        .count();
    prop_assert_eq!(
        fills.lock().expect("fills mutex poisoned").clone(),
        expected_fills.clone(),
        "each fill moves the position by its step once, and its callback sees it already moved"
    );
    prop_assert_eq!(
        fill_rows,
        expected_fills.len(),
        "one fill row for each step"
    );
    engine.dispatch(&spin(0, 1_000 * (delivered.len() as i64 + 2)));
    let read = *at_spin.lock().expect("spin mutex poisoned");
    prop_assert_eq!(
        read,
        Some((Qty::from_mantissa(cumulative), -(cumulative * PRICE))),
        "the position is the largest total and the cash pays only for it"
    );
    Ok(())
}

/// FITNESS: the venue's reports come again and out of order, and each one moves the money of its
/// trade once. The quantity folds the venue's running total, so a copy or an older report moves
/// nothing, and a strategy reads, in each fill's callback, a position that already holds that fill.
/// A fold that moved money for a copy would bank a trade that never happened.
#[test]
fn redelivered_and_reordered_reports_move_money_once() {
    a_fill_folds_before_its_callback_and_a_redelivery_moves_the_money_once();
    a_fill_moves_cash_the_way_its_side_and_its_commission_say();
    let runs = (
        prop::collection::vec(1i64..20, 1..=8).prop_map(|steps: Vec<i64>| {
            steps
                .into_iter()
                .map(|step| step * ONE / 1_000)
                .collect::<Vec<i64>>()
        }),
        prop::collection::vec((0usize..64, 0usize..64), 0..12),
    );
    let mut runner = TestRunner::new(Config {
        cases: 64,
        failure_persistence: None,
        ..Config::default()
    });
    runner
        .run(&runs, |(steps, copies): (Vec<i64>, Vec<(usize, usize)>)| {
            fold_reports(&steps, &copies)
        })
        .expect("each report moves the money of its trade once");
}
/// Reports the engine's own position and PnL each spin, so an assertion can be made against the
/// MONEY rather than against an enum.
#[derive(Default)]
struct MoneyProbe {
    latest: Arc<Mutex<Option<(i64, i64)>>>,
}

impl Strategy for MoneyProbe {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        *self.latest.lock().expect("money mutex poisoned") = Some((
            ctx.position_base(InstrumentId(0)).mantissa(),
            ctx.pnl_quote(InstrumentId(0)).mantissa(),
        ));
    }
}

/// FITNESS: a venue fill is folded BEFORE the `on_fill` that reports it, and a report the venue
/// redelivers moves the MONEY exactly once, whatever order the copies arrive in.
///
/// A strategy reading its position from `on_fill` must see the fill counted exactly once — adding it
/// again is the double count. A fill is its own message, which the engine has already applied by the
/// time anyone is told, never something a strategy banks mid-message for a later drain to fold.
/// Redelivery is not a hypothetical either — Binance really does it, and the second copy is
/// ordinary traffic that looks exactly like a real fill, with only its already-folded totals to tell
/// it apart; the order table's own fitness pins the same idempotence on the slot's totals, and this pins it where it
/// costs, on position and cash through the whole engine, because a fold idempotent about quantities
/// and not about cash would still bank a profit that never happened.
fn a_fill_folds_before_its_callback_and_a_redelivery_moves_the_money_once() {
    {
        let instruments = [bare_row(0)];
        let (mut engine, readings) = probe_engine(&instruments);
        // Mid of 100 and 102 is 101 — every number below is exact against it.
        dispatch_all(&mut engine, &reseat_book(0, 100 * ONE, 102 * ONE, 0));
        let mut pen = FillPen::new(0);

        dispatch_all(&mut engine, &pen.fill(Side::Buy, 101 * ONE, ONE, 10));
        engine.dispatch(&spin(0, 20));

        let recorded = take_readings(&readings);
        let in_fill = recorded
            .iter()
            .find(|reading| reading.phase == Phase::InFill)
            .expect("the fill earned an on_fill, and the probe read the ledger inside it");
        assert_eq!(
            in_fill.position_base,
            Qty::from_mantissa(ONE),
            "on_fill ran against a ledger that had not yet folded the fill it was reporting"
        );
        assert_eq!(
            in_fill.pnl_quote, 0,
            "bought at the mark, so nothing is made or lost yet"
        );

        let on_spin = recorded
            .iter()
            .find(|reading| reading.phase == Phase::OnSpin(0))
            .expect("the probe read the ledger on the following spin");
        assert_eq!(
            on_spin.position_base,
            Qty::from_mantissa(ONE),
            "the fill moved the position a second time between its callback and the next spin"
        );
        assert_eq!(on_spin.exposure_quote, 101 * ONE, "one unit marked at 101");
        assert_eq!(on_spin.pnl_quote, 0);

        dispatch_all(&mut engine, &pen.fill(Side::Buy, 100 * ONE, ONE, 30));
        engine.dispatch(&spin(1, 40));

        let after = spin_reading(&readings, 0, 1);
        assert_eq!(after.position_base, Qty::from_mantissa(2 * ONE));
        assert_eq!(after.exposure_quote, 202 * ONE);
        assert_eq!(
            after.pnl_quote, ONE,
            "cost 201 for two units now worth 202 — the one unit bought under the mark"
        );
    }
    {
        let instruments = [bare_row(0)];
        let (mut engine, readings) = probe_engine(&instruments);
        dispatch_all(&mut engine, &reseat_book(0, 100 * ONE, 102 * ONE, 0));

        let mut pen = FillPen::new(0);
        let first = pen.fill(Side::Buy, 100 * ONE, ONE, 10);
        let second = pen.fill(Side::Buy, 102 * ONE, 2 * ONE, 20);
        // The first copy of each, then both again out of order, then the earlier one a third time.
        for batch in [&first, &second, &second, &first, &first] {
            dispatch_all(&mut engine, batch);
        }
        engine.dispatch(&spin(0, 30));

        let after = spin_reading(&readings, 0, 0);
        let fills = take_readings(&readings)
            .iter()
            .filter(|reading| reading.phase == Phase::InFill)
            .count();
        assert_eq!(
            fills, 2,
            "a redelivered report earned its own on_fill, so a strategy counting fills counts ghosts"
        );

        assert_eq!(
            after.position_base,
            Qty::from_mantissa(3 * ONE),
            "the redeliveries moved the position again"
        );
        // Paid 100 for one unit and 204 for two; three units marked at 101 are worth 303.
        assert_eq!(after.exposure_quote, 303 * ONE);
        assert_eq!(
            after.pnl_quote, -ONE,
            "the redeliveries moved cash — a fold idempotent about size and not about money"
        );
    }
}

/// FITNESS: a fill on OUR BID increases the base position and decreases quote cash; a fill on OUR
/// ASK does the mirror.
///
/// This is the assertion the whole side vocabulary exists for, and it is deliberately made against
/// the money instead of against `Side`. `Side::Buy`/`Side::Sell`, `MarkoutSide::Bid`/`Ask` and the
/// DOM's `AnchorSide` all coexist in this codebase, and the compiler catches only the spellings that
/// do not exist. A mapping that is inverted CONSISTENTLY at every layer compiles, satisfies every
/// structural test, agrees with itself everywhere — and trades backwards. The only thing that cannot
/// be fooled by a consistent inversion is which way the cash went.
///
/// A commission is the third thing that moves the cash, and its sign inverts just as quietly: a
/// rebate booked as a fee, or a fee paid in another asset counted as quote, reports a profit or a
/// loss that the venue never paid.
fn a_fill_moves_cash_the_way_its_side_and_its_commission_say() {
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let latest = Arc::new(Mutex::new(None));
    let (persistence, _persist) = persist_ring(256);
    let (log_sink, _logs) = strategy_log_ring(64);
    let mut engine = engine_without_warmup(
        &instruments,
        Box::new(MoneyProbe {
            latest: Arc::clone(&latest),
        }),
        persistence,
        log_sink,
    );
    let mut pen = FillPen::new(0);

    // Buy one whole base unit at 100. No mark yet, so PnL is the bare cost basis — which is exactly
    // the number that has to be NEGATIVE: we spent quote to get base.
    for message in pen.fill(Side::Buy, 100 * ONE, ONE, 10) {
        engine.dispatch(&message);
    }
    engine.dispatch(&spin(0, 20));
    let (position, cash) = latest.lock().expect("poisoned").expect("the spin ran");
    assert_eq!(
        position, ONE,
        "a fill on our bid must BUY — base did not increase"
    );
    assert_eq!(
        cash,
        -(100 * ONE),
        "a fill on our bid must SPEND quote — cash did not decrease by the notional"
    );

    // Sell it back at 110. Base returns to flat and the round trip banks a profit; an inverted sell
    // would drive the position to -2 and the cash the wrong way.
    for message in pen.fill(Side::Sell, 110 * ONE, ONE, 30) {
        engine.dispatch(&message);
    }
    engine.dispatch(&spin(1, 40));
    let (position, cash) = latest.lock().expect("poisoned").expect("the spin ran");
    assert_eq!(
        position, 0,
        "a fill on our ask must SELL — the position did not come back to flat"
    );
    assert_eq!(
        cash,
        10 * ONE,
        "the round trip bought at 100 and sold at 110, so exactly 10 must be banked"
    );

    // Every row is the same buy of one unit at 100, so whatever the cash moves beyond that notional
    // is the commission alone.
    const COMMISSION: i64 = 23_599;
    let (base_asset, quote_asset) = (instruments[0].base_asset, instruments[0].quote_asset);
    let commissions = [
        (
            -COMMISSION,
            quote_asset,
            COMMISSION,
            "a rebate paid in quote is income — cash must rise by exactly the rebate",
        ),
        (
            COMMISSION,
            quote_asset,
            -COMMISSION,
            "a fee paid in quote is a cost — cash must fall by exactly the fee",
        ),
        (
            -COMMISSION,
            base_asset,
            0,
            "a rebate paid in base is not this instrument's quote cash",
        ),
        (
            COMMISSION,
            AssetId::UNKNOWN,
            0,
            "a fee in an asset no instrument names is not this instrument's quote cash",
        ),
    ];
    let mut cash_before = cash;
    for (step, (commission, commission_asset, cash_gain, why)) in
        commissions.into_iter().enumerate()
    {
        let when = 50 + 20 * step as i64;
        let mut charged = pen.report(Side::Buy, 100 * ONE, ONE, when);
        let MessageBody::Exec(report) = &mut charged.body else {
            unreachable!("the pen reports a fill as an Exec message");
        };
        report.commission = commission;
        report.commission_asset = commission_asset;
        engine.dispatch(&charged);
        engine.dispatch(&spin(2 + step as u64, when + 10));
        let (_, cash_after) = latest.lock().expect("poisoned").expect("the spin ran");
        assert_eq!(cash_after - cash_before, cash_gain - 100 * ONE, "{why}");
        cash_before = cash_after;
    }
}
