//! Each venue report moves money once: a quantity by the venue's running totals, a fee once for
//! each trade, and an event the engine cannot place either halts quoting or leaves a typed record.

use std::sync::{Arc, Mutex};

use engine::hot::dispatch::{ExecWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{ClientIdLayout, HaltReason, side_base};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{Fill, Strategy, StrategyCtx};
use engine::msg::exec::{ExecEvent, ExecKind, ExecLaneItem, VenueOrderStatus};
use engine::msg::ids::{AssetId, InstrumentId, Notional, Price, Qty, Side, TradeId};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::persist::{IncidentKind, IncidentRow, PersistRecord};
use engine::msg::time::Stamper;
use engine::msg::ui::{UiEvent, UiHalt};
use engine::runtime::config::{RecordedTables, TableKind, TrackerSpec};
use engine::runtime::registry::InstrumentRow;
use proptest::prelude::*;
use proptest::strategy::Strategy as _;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    FillPen, ONE, detached_setup, engine_without_warmup, exec_event, instrument_row, persist_ring,
    persist_ring_for, spin, strategy_log_ring, ts, ui_event_ring, venue_ts,
};
use crate::exec_support::{
    ASK, BID, INSTRUMENT, TwoSidedQuoter, drain_commands, exec_settings, is_placing, make_ready,
    spin_at,
};
use crate::hot::position_ledger::{
    Phase, bare_row, dispatch_all, probe_engine, reseat_book, spin_reading, take_readings,
};

/// The price of each fill of the generated runs, so the quote of a run is its quantity times it.
const PRICE: i64 = 100;

/// A tenth of a base unit, and the fee of 0.0001 that each case below counts in.
const TENTH: i64 = ONE / 10;
const FEE: i64 = ONE / 10_000;

/// The assets of [`instrument_row`]: its base and its quote.
const BASE: AssetId = AssetId(0);
const QUOTE: AssetId = AssetId(1);

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

/// An engine over `instruments` that records fill rows, with one resting buy of 1.0 at 100 on
/// instrument 0 that the venue named.
fn engine_with_resting_buy(
    instruments: &[InstrumentRow],
    strategy: Box<dyn Strategy>,
) -> (HotEngine, Consumer<PersistRecord>, ExecEvent) {
    let (persistence, rows) = persist_ring_for(4_096, RecordedTables::new(&[TableKind::Fills]));
    let (log_sink, _logs) = strategy_log_ring(64);
    let mut engine = engine_without_warmup(instruments, strategy, persistence, log_sink);
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
    let (mut engine, mut rows, order) = engine_with_resting_buy(
        &[instrument_row(0, TrackerSpec::default(), 64)],
        Box::new(FillProbe {
            fills: Arc::clone(&fills),
            at_spin: Arc::clone(&at_spin),
        }),
    );
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

/// One engine of the fee cases: the resting buy, the reports folded into it one stamp apart, and
/// the position and cash that a spin reads.
struct FeeRun {
    engine: HotEngine,
    rows: Consumer<PersistRecord>,
    order: ExecEvent,
    at_spin: Arc<Mutex<Option<(Qty, i64)>>>,
    when: i64,
    spins: u64,
}

impl FeeRun {
    fn new(instruments: &[InstrumentRow]) -> Self {
        let at_spin = Arc::new(Mutex::new(None));
        let (engine, rows, order) = engine_with_resting_buy(
            instruments,
            Box::new(FillProbe {
                fills: Arc::default(),
                at_spin: Arc::clone(&at_spin),
            }),
        );
        Self {
            engine,
            rows,
            order,
            at_spin,
            when: 1_000,
            spins: 0,
        }
    }

    /// A report of `trade` with its own quantity `last`, the order's total `cumulative`, and a fee
    /// of `fee` in `asset`.
    fn report(
        &self,
        trade: i64,
        (last, cumulative): (i64, i64),
        (asset, fee): (AssetId, i64),
    ) -> ExecEvent {
        ExecEvent {
            commission: fee,
            commission_asset: asset,
            ..trade_report(self.order, trade, last, cumulative)
        }
    }

    /// Folds `report` and answers its received stamp.
    fn fold(&mut self, report: ExecEvent) -> i64 {
        self.when += 1_000;
        self.engine.dispatch(&InboundMessage::new(
            ts(self.when),
            MessageBody::Exec(report),
        ));
        self.when
    }

    /// The position and the cash of instrument 0, read at a spin.
    fn read(&mut self) -> (Qty, i64) {
        self.when += 1_000;
        self.spins += 1;
        self.engine.dispatch(&spin(self.spins, self.when));
        self.at_spin
            .lock()
            .expect("spin mutex poisoned")
            .take()
            .expect("the spin ran the strategy")
    }

    fn incidents(&mut self) -> Vec<IncidentRow> {
        incidents(&mut self.rows)
    }
}

/// The incident rows on the tape since the last read. The other rows go.
fn incidents(rows: &mut Consumer<PersistRecord>) -> Vec<IncidentRow> {
    std::iter::from_fn(|| rows.pop().ok())
        .filter_map(|record| match record {
            PersistRecord::Incident(row) => Some(row),
            _ => None,
        })
        .collect()
}

/// The incident row that `report`, folded at `at`, should leave.
fn incident(kind: IncidentKind, report: &ExecEvent, qty: i64, amount: i64, at: i64) -> IncidentRow {
    IncidentRow {
        kind,
        instrument: InstrumentId(0),
        client_id: Some(report.client_id),
        trade_id: report.trade_id,
        qty: Qty::from_mantissa(qty),
        amount,
        window_close_ts_us: None,
        exchange_ts_us: report.exchange_ts_us,
        received_ts_us: ts(at),
    }
}

/// FITNESS: each fee folds once for each trade, whatever the venue repeats, reorders or loses. A
/// report whose quantity step its own trade does not explain leaves a fee-gap record, and the late
/// report of the lost trade books its fee and closes the gap. A fee that the engine cannot book, in
/// an asset with no row or of a trade it cannot tell from one booked before, leaves a record with
/// its amount. A fee booked twice, or lost with no record, is money that the ledger reports and the
/// venue never charged, or the reverse, and no later state shows it.
#[test]
fn two_trades_in_one_report_give_both_fees_or_a_gap_record() {
    a_lost_trade_leaves_a_gap_that_its_late_report_closes();
    a_repeat_that_the_full_memory_no_longer_holds_is_unbooked();
    a_fee_goes_to_the_row_of_its_asset();
}

/// R1 (trade 101, 0.4) and R3 (trade 103, 0.3, total 1.0) arrive, and R2 (trade 102, 0.3) is lost,
/// then comes late. A repeat of R1 comes last.
fn a_lost_trade_leaves_a_gap_that_its_late_report_closes() {
    let mut run = FeeRun::new(&[instrument_row(0, TrackerSpec::default(), 64)]);
    let r1 = run.report(101, (4 * TENTH, 4 * TENTH), (QUOTE, 4 * FEE));
    let r2 = run.report(102, (3 * TENTH, 7 * TENTH), (QUOTE, 3 * FEE));
    let r3 = run.report(103, (3 * TENTH, 10 * TENTH), (QUOTE, 3 * FEE));

    run.fold(r1);
    let r3_at = run.fold(r3);
    assert_eq!(
        run.read(),
        (Qty::from_mantissa(ONE), -(100 * ONE) - 7 * FEE),
        "R1 and R3 book the whole total, and only their own two fees"
    );
    assert_eq!(
        run.incidents(),
        [incident(IncidentKind::FeeGap, &r3, 3 * TENTH, 0, r3_at)],
        "R3 raised the total by 0.6 and its own trade explains 0.3 of it"
    );
    assert_eq!(run.engine.counters().fee_gaps, 1);

    run.fold(r2);
    run.fold(r1);
    assert_eq!(
        run.read(),
        (Qty::from_mantissa(ONE), -(100 * ONE) - 10 * FEE),
        "the late R2 books its fee once and moves no quantity, and the repeat of R1 books nothing"
    );
    assert_eq!(
        run.incidents(),
        [],
        "a late report and a repeat leave no record"
    );
    let counters = run.engine.counters();
    assert_eq!((counters.fee_gaps, counters.unbooked_fees), (1, 0));
}

/// Nine trades, each its own step with a fee of 0.0001, then a repeat of the first. The memory holds
/// the ids of eight trades, so the ninth pushes the first out.
fn a_repeat_that_the_full_memory_no_longer_holds_is_unbooked() {
    let mut run = FeeRun::new(&[instrument_row(0, TrackerSpec::default(), 64)]);
    let reports: Vec<ExecEvent> = (1..=9)
        .map(|trade| run.report(trade, (TENTH, trade * TENTH), (QUOTE, FEE)))
        .collect();
    for report in &reports {
        run.fold(*report);
    }
    let after_nine = (Qty::from_mantissa(9 * TENTH), -(90 * ONE) - 9 * FEE);
    assert_eq!(
        run.read(),
        after_nine,
        "each of nine trades books its own fee, the ninth with the memory full"
    );
    assert_eq!(run.incidents(), []);

    let repeat_at = run.fold(reports[0]);
    assert_eq!(
        run.read(),
        after_nine,
        "a repeat that the memory cannot tell from a new trade books nothing"
    );
    assert_eq!(
        run.incidents(),
        [incident(
            IncidentKind::UnbookedFee,
            &reports[0],
            TENTH,
            FEE,
            repeat_at
        )],
        "the fee that was not booked leaves its amount on the tape"
    );
    assert_eq!(run.engine.counters().unbooked_fees, 1);
}

/// One buy of 0.4 at 100 with a fee of 0.0004 in each asset that is not the quote asset.
fn a_fee_goes_to_the_row_of_its_asset() {
    let other = InstrumentRow {
        base_asset: AssetId(2),
        ..instrument_row(1, TrackerSpec::default(), 64)
    };
    let instruments = [instrument_row(0, TrackerSpec::default(), 64), other];
    let cases = [
        (
            BASE,
            4 * TENTH - 4 * FEE,
            false,
            "a fee in the base asset is base that the venue keeps: the position holds that much \
             less, and the cash pays only the notional",
        ),
        (
            AssetId(2),
            4 * TENTH,
            false,
            "a fee in another asset that the registry names goes to the row of that asset, never \
             to this instrument's position or cash",
        ),
        (
            AssetId::UNKNOWN,
            4 * TENTH,
            true,
            "a fee in an asset that the registry does not name moves nothing, and leaves a record \
             with its amount",
        ),
    ];
    for (asset, position, is_unbooked, why) in cases {
        let mut run = FeeRun::new(&instruments);
        let report = run.report(1, (4 * TENTH, 4 * TENTH), (asset, 4 * FEE));
        let at = run.fold(report);
        assert_eq!(
            run.read(),
            (Qty::from_mantissa(position), -(40 * ONE)),
            "{why}"
        );
        let unbooked: Vec<IncidentRow> = is_unbooked
            .then(|| incident(IncidentKind::UnbookedFee, &report, 4 * TENTH, 4 * FEE, at))
            .into_iter()
            .collect();
        assert_eq!(run.incidents(), unbooked, "{why}");
        assert_eq!(
            run.engine.counters().unbooked_fees,
            u64::from(is_unbooked),
            "{why}"
        );
    }
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

/// Quotes both sides at the touch, as the shared quoter does, and keeps the position that it reads
/// at each spin.
struct QuotingProbe {
    position: Arc<Mutex<Option<Qty>>>,
}

impl Strategy for QuotingProbe {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        TwoSidedQuoter.on_spin(ctx, tick);
        *self.position.lock().expect("position mutex poisoned") =
            Some(ctx.position_base(INSTRUMENT));
    }
}

/// CONTRACT: an event of one of this run's own orders that no slot holds halts quoting when it
/// carries a fill, and leaves one incident record with the order and its filled quantity, while the
/// ledger books nothing that it would have to place by a guess. With no fill the event is counted,
/// and quoting goes on. The tape takes the record with no table named for it.
#[test]
fn orphan_fill_halts_and_keeps_state() {
    const RUN_NONCE: u32 = 7;
    let client_id = ClientIdLayout {
        run_nonce: RUN_NONCE,
    }
    .encode(12, 3);
    for (cumulative, is_halted) in [(5 * TENTH, true), (0, false)] {
        let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
        let (persistence, mut rows) = persist_ring_for(1_024, RecordedTables::default());
        let (commands_producer, mut commands) = RingBuffer::<ExecLaneItem>::new(256);
        let (ui_event_sink, mut ui_events) = ui_event_ring(1_024);
        let position = Arc::new(Mutex::new(None));
        let probe = QuotingProbe {
            position: Arc::clone(&position),
        };
        let mut engine = HotEngine::new(HotEngineSetup {
            persistence: Some(persistence),
            ui_event_sink,
            exec: Some(ExecWiring {
                sink: ExecSink::new(commands_producer, Stamper::Off),
                settings: exec_settings(),
                run_nonce: RUN_NONCE,
            }),
            ..detached_setup(&instruments, Box::new(probe))
        });
        make_ready(&mut engine, 0);
        dispatch_all(&mut engine, &reseat_book(0, BID, ASK, 1));
        let filled = Qty::from_mantissa(cumulative);
        let orphan = ExecEvent {
            kind: ExecKind::ReportTrade,
            status: Some(VenueOrderStatus::PartiallyFilled),
            trade_id: Some(TradeId(77)),
            qty: Qty::from_mantissa(ONE),
            last_price: Price::from_mantissa(BID),
            last_qty: filled,
            cumulative_qty: filled,
            cumulative_quote: Price::from_mantissa(BID).notional(filled),
            ..exec_event(INSTRUMENT, client_id, Side::Buy, BID, 2)
        };
        engine.dispatch(&InboundMessage::new(ts(2), MessageBody::Exec(orphan)));
        spin_at(&mut engine, 1, 3);

        let halt = std::iter::from_fn(|| ui_events.pop().ok())
            .filter_map(|event| match event {
                UiEvent::Execution { halt, .. } => Some(halt),
                _ => None,
            })
            .last();
        let reason = match halt {
            Some(UiHalt::Halted { reason, .. }) => Some(reason),
            _ => None,
        };
        assert_eq!(
            reason,
            is_halted.then_some(HaltReason::OrphanFill),
            "a fill on an order with no slot halts quoting, and an event with no fill does not"
        );
        let sent = drain_commands(&mut commands);
        assert_eq!(
            is_placing(&sent, Side::Buy) || is_placing(&sent, Side::Sell),
            !is_halted,
            "no quote goes out after the halt, and quoting goes on after an event with no fill: \
             {sent:?}"
        );
        let orphan_fill = IncidentRow {
            kind: IncidentKind::OrphanFill,
            instrument: INSTRUMENT,
            client_id: Some(client_id),
            trade_id: Some(TradeId(77)),
            qty: filled,
            amount: 0,
            window_close_ts_us: None,
            exchange_ts_us: venue_ts(2),
            received_ts_us: ts(2),
        };
        let expected: Vec<IncidentRow> = is_halted.then_some(orphan_fill).into_iter().collect();
        assert_eq!(
            incidents(&mut rows),
            expected,
            "a fill on an order with no slot leaves one record with the order and its filled \
             quantity, whatever tables the config names"
        );
        assert_eq!(
            *position.lock().expect("position mutex poisoned"),
            Some(Qty::from_mantissa(0)),
            "the ledger books no fill that it would have to place by a guess"
        );
        let counters = engine.counters();
        assert_eq!(
            (counters.orphan_fills, counters.orphan_events_without_fill),
            if is_halted { (1, 0) } else { (0, 1) },
            "an orphan fill counts as an incident, and an event with no fill as a refusal"
        );
    }
}
