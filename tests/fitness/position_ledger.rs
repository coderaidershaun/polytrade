//! Position-ledger fitness: the engine folds every REAL venue fill into exact per-instrument
//! position, cash and mark-to-market, and hands it back through `ctx`. Three ways this breaks are
//! all silent. Arithmetic that drifts from the fills that produced it yields PnL nobody can audit
//! against the tape. A venue that redelivers a report — and Binance does — would move the money
//! twice, and the second move looks exactly like a real fill. And a mark invented from half a book,
//! or carried across a window rotation into a market whose prices mean something else, values a
//! position at a number no venue ever showed.
//!
//! Every position here opens through the real inbound path ([`FillPen`]), so a strategy cannot
//! assert a fill into existence. The fold therefore lands on the fill's OWN message, ahead of the
//! `on_fill` that reports it.

use std::sync::{Arc, Mutex};

use polysim::config::TrackerSpec;
use polysim::hot::dispatch::HotEngine;
use polysim::hot::strategy::{Fill, Registration, Strategy, StrategyCtx};
use polysim::ids::{AssetId, InstrumentId, Qty, Side};
use polysim::msg::exec::ExecEvent;
use polysim::msg::inbound::{InboundMessage, SpinTick};
use polysim::registry::InstrumentRow;
use proptest::prelude::*;
use proptest::strategy::Strategy as _;

use crate::engine_support::{
    FillPen, ONE, book_reset, delta_chunk, engine_without_warmup, idle_at, instrument_row,
    last_snapshot_chunk, metrics_ring, persist_ring, pop, rotation, run_control, running_at,
    snapshot_pair, spin, strategy_log_ring, trade,
};

/// One hundredth of a price unit — the grid a venue quotes on. Note it is EVEN in mantissas, so a
/// spread built from whole ticks alone never lands the mid between two mantissas; the generator
/// adds a single-mantissa jitter to reach the truncating case.
const TICK: i64 = ONE / 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Read inside the `on_fill` a venue fill earned, which the engine fires only after folding it.
    InFill,
    /// Read on a later message's spin. Carries the tick's sequence so a reader names the spin it
    /// means instead of taking whatever was most recent.
    OnSpin(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Reading {
    phase: Phase,
    instrument: InstrumentId,
    position_base: Qty,
    exposure_quote: i64,
    pnl_quote: i64,
    has_mark: bool,
}

type Readings = Arc<Mutex<Vec<Reading>>>;

/// Reads the ledger back through `ctx` on both callbacks it sees, so the tape alone decides the
/// ledger and both sides of the fold boundary are observed. It banks nothing: the position it
/// reports arrives from the venue, which is the only place a position can come from.
struct LedgerProbe {
    readings: Readings,
    instrument_count: usize,
}

impl LedgerProbe {
    fn new(readings: &Readings) -> Self {
        Self {
            readings: Arc::clone(readings),
            instrument_count: 0,
        }
    }

    fn record(&self, ctx: &StrategyCtx<'_>, phase: Phase, instrument: InstrumentId) {
        self.readings
            .lock()
            .expect("readings mutex poisoned")
            .push(Reading {
                phase,
                instrument,
                position_base: ctx.position_base(instrument),
                exposure_quote: ctx.exposure_quote(instrument),
                pnl_quote: ctx.pnl_quote(instrument),
                has_mark: ctx.has_mark(instrument),
            });
    }
}

impl Strategy for LedgerProbe {
    fn register(&mut self, registration: Registration<'_>) {
        self.instrument_count = registration.instruments.len();
    }

    fn on_fill(&mut self, ctx: &mut StrategyCtx<'_>, fill: &Fill) {
        self.record(ctx, Phase::InFill, fill.instrument);
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        for index in 0..self.instrument_count {
            self.record(ctx, Phase::OnSpin(tick.seq), InstrumentId(index as u16));
        }
    }
}

/// The fold every assertion is measured against, written from the definitions rather than from the
/// engine's expressions: buy pays cash and takes base, sell the reverse, exposure is the
/// mark times the position, PnL is the two added. A shared expression would agree with a bug.
#[derive(Debug, Clone, Copy, Default)]
struct Shadow {
    position_base: i64,
    cash_quote: i64,
    mark: Option<i64>,
}

impl Shadow {
    fn fill(&mut self, side: Side, price: i64, qty: i64) {
        let notional = scaled_product(price, qty);
        match side {
            Side::Buy => {
                self.position_base += qty;
                self.cash_quote -= notional;
            }
            Side::Sell => {
                self.position_base -= qty;
                self.cash_quote += notional;
            }
        }
    }

    fn exposure_quote(&self) -> i64 {
        self.mark
            .map_or(0, |mark| scaled_product(mark, self.position_base))
    }

    fn pnl_quote(&self) -> i64 {
        self.cash_quote + self.exposure_quote()
    }
}

fn scaled_product(left: i64, right: i64) -> i64 {
    let product = i128::from(left) * i128::from(right) / i128::from(ONE);
    i64::try_from(product).expect("fitness operands stay inside the i64 mantissa")
}

/// A tracker with every series switched off: the ledger reads none of it, and a light row keeps the
/// proptest below building a full estimator suite per case.
fn bare_row(id: u16) -> InstrumentRow {
    instrument_row(id, TrackerSpec::default(), 64)
}

fn probe_engine(instruments: &[InstrumentRow]) -> (HotEngine, Readings) {
    let readings: Readings = Arc::new(Mutex::new(Vec::new()));
    let (persistence, _persist) = persist_ring(256);
    let (log_sink, _logs) = strategy_log_ring(64);
    let (metrics, _metrics) = metrics_ring(64);
    let engine = engine_without_warmup(
        instruments,
        Box::new(LedgerProbe::new(&readings)),
        persistence,
        log_sink,
        metrics,
    );
    // The persist/log/metrics consumers are dropped on purpose: this suite reads the ledger through
    // ctx alone, and those lanes fill-then-drop harmlessly.
    (engine, readings)
}

fn take_readings(readings: &Readings) -> Vec<Reading> {
    std::mem::take(&mut *readings.lock().expect("readings mutex poisoned"))
}

/// The reading `instrument` produced on the spin with sequence `spin_seq`. Named rather than
/// "latest" on purpose: a scan for the most recent spin would quietly hand back the PREVIOUS one
/// when the expected spin recorded nothing, and every assertion below it would then run green
/// against stale numbers. Non-draining, so one spin can be read once per instrument.
fn spin_reading(readings: &Readings, instrument: u16, spin_seq: u64) -> Reading {
    let recorded = readings.lock().expect("readings mutex poisoned");
    recorded
        .iter()
        .rev()
        .find(|reading| {
            reading.phase == Phase::OnSpin(spin_seq)
                && reading.instrument == InstrumentId(instrument)
        })
        .copied()
        .unwrap_or_else(|| {
            panic!(
                "no reading for instrument {instrument} on spin {spin_seq} — the probe never ran"
            )
        })
}

/// What a reconnecting adapter sends, and the one way to move the top of book with no transient
/// crossing to reason about: drop it, then state it whole.
fn reseat_book(instrument: u16, bid: i64, ask: i64, when: i64) -> [InboundMessage; 3] {
    let (bids, asks) = snapshot_pair(instrument, &[(bid, ONE)], &[(ask, ONE)], when);
    [
        InboundMessage::BookReset(book_reset(instrument, when)),
        InboundMessage::Book(bids),
        InboundMessage::Book(asks),
    ]
}

fn dispatch_all(engine: &mut HotEngine, messages: &[InboundMessage]) {
    for message in messages {
        engine.dispatch(pop(0, 0), message);
    }
}

/// One step of a tape: either the book moves (which re-marks) or a print arrives (which the probe
/// turns into a fill).
#[derive(Debug, Clone, Copy)]
enum Op {
    Book { bid: i64, ask: i64 },
    Fill { side: Side, price: i64, qty: i64 },
}

/// The mark as `mark_ledger` states it: bid plus half the spread, truncated toward zero. Written
/// out here from the stated rule rather than shared with the engine, and deliberately NOT
/// `(bid + ask) / 2` — the two diverge by one mantissa on a crossed top with an odd spread, which
/// the generator below reaches.
fn stated_mid(bid: i64, ask: i64) -> i64 {
    bid + (ask - bid) / 2
}

fn arb_op() -> impl proptest::strategy::Strategy<Value = Op> {
    prop_oneof![
        // The spread spans negative (a crossed top), zero (locked) and positive: the book counts
        // and warns those but leaves them Valid, so they all reach the mark. The 0-or-1 mantissa
        // jitter makes the spread odd half the time, which is the only case where the engine's
        // guarded mid and the plain `(bid + ask) / 2` can disagree — and on a crossed top they do.
        (50i64..150, -9i64..=9, 0i64..=1).prop_map(|(units, spread_ticks, jitter)| Op::Book {
            bid: units * ONE,
            ask: units * ONE + spread_ticks * TICK + jitter,
        }),
        (
            prop_oneof![Just(Side::Buy), Just(Side::Sell)],
            50i64..150,
            1i64..32,
        )
            .prop_map(|(side, units, eighths)| Op::Fill {
                side,
                price: units * ONE,
                qty: eighths * ONE / 8,
            }),
    ]
}

proptest! {
    /// FITNESS: whatever order fills and re-marks arrive in, the ledger a strategy reads equals an
    /// independent fold of the same tape — exactly, as integers. Exposure is the mark times the
    /// position and PnL is cash plus exposure, at every single step and not merely at the end.
    #[test]
    fn fills_and_marks_fold_into_exact_exposure_and_pnl(
        ops in prop::collection::vec(arb_op(), 1..24),
    ) {
        let instruments = [bare_row(0)];
        let (mut engine, readings) = probe_engine(&instruments);
        let mut shadow = Shadow::default();
        let mut pen = FillPen::new(0);

        // Marked before the first op, so this test is about the arithmetic; the pre-mark half is
        // `a_mark_needs_both_sides_and_survives_a_book_reset`'s subject.
        dispatch_all(&mut engine, &reseat_book(0, 100 * ONE, 100 * ONE + TICK, 0));
        shadow.mark = Some(stated_mid(100 * ONE, 100 * ONE + TICK));

        for (index, op) in ops.iter().enumerate() {
            let when = 1_000 * (index as i64 + 1);
            match *op {
                Op::Book { bid, ask } => {
                    dispatch_all(&mut engine, &reseat_book(0, bid, ask, when));
                    let mid = stated_mid(bid, ask);
                    // On an uncrossed top the guarded form the engine uses and the plain mid are
                    // the same number. That equality is the whole reason the guarded form was
                    // allowed, so pin it wherever it is claimed to hold.
                    if bid <= ask {
                        prop_assert_eq!(mid, (bid + ask) / 2);
                    }
                    shadow.mark = Some(mid);
                }
                Op::Fill { side, price, qty } => {
                    dispatch_all(&mut engine, &pen.fill(side, price, qty, when));
                    shadow.fill(side, price, qty);
                }
            }
            let seq = index as u64;
            engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(seq, when + 1)));

            let reading = spin_reading(&readings, 0, seq);
            prop_assert_eq!(reading.position_base, Qty(shadow.position_base));
            prop_assert_eq!(reading.exposure_quote, shadow.exposure_quote());
            prop_assert_eq!(reading.pnl_quote, shadow.pnl_quote());
            prop_assert!(reading.has_mark);
        }
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
#[test]
fn a_fill_folds_before_its_callback_and_a_redelivery_moves_the_money_once() {
    {
        let instruments = [bare_row(0)];
        let (mut engine, readings) = probe_engine(&instruments);
        // Mid of 100 and 102 is 101 — every number below is exact against it.
        dispatch_all(&mut engine, &reseat_book(0, 100 * ONE, 102 * ONE, 0));
        let mut pen = FillPen::new(0);

        dispatch_all(&mut engine, &pen.fill(Side::Buy, 101 * ONE, ONE, 10));
        engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(0, 20)));

        let recorded = take_readings(&readings);
        let in_fill = recorded
            .iter()
            .find(|reading| reading.phase == Phase::InFill)
            .expect("the fill earned an on_fill, and the probe read the ledger inside it");
        assert_eq!(
            in_fill.position_base,
            Qty(ONE),
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
            Qty(ONE),
            "the fill moved the position a second time between its callback and the next spin"
        );
        assert_eq!(on_spin.exposure_quote, 101 * ONE, "one unit marked at 101");
        assert_eq!(on_spin.pnl_quote, 0);

        dispatch_all(&mut engine, &pen.fill(Side::Buy, 100 * ONE, ONE, 30));
        engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(1, 40)));

        let after = spin_reading(&readings, 0, 1);
        assert_eq!(after.position_base, Qty(2 * ONE));
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
        engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(0, 30)));

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
            Qty(3 * ONE),
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

/// FITNESS: a rotation flattens the slot it rotated and clears its mark, leaving every other slot
/// untouched; a park/resume flattens NOTHING. The rotation reset is message-driven, so a replay
/// performs it at exactly the same point.
///
/// The asymmetry is the whole point, and it is about money rather than about state hygiene. A
/// rotated window is a different market, so the position and the mark belonged to something that no
/// longer exists. A park is the same market with the engine's eyes shut: parking sells no coin, so
/// an engine that woke up flat would believe it held nothing while real inventory sat on the venue,
/// and would quote with no skew to unwind it. Zeroing also discards realised PnL, which lives in
/// cash on a row that may well read FLAT — so session PnL spans the whole process run, parks
/// included, which is what a session loss limit has to mean.
#[test]
fn a_rotation_flattens_its_own_slot_and_a_resume_preserves_every_one() {
    let instruments = [bare_row(0), bare_row(1)];
    let (mut engine, readings) = probe_engine(&instruments);
    for instrument in [0u16, 1] {
        dispatch_all(
            &mut engine,
            &reseat_book(instrument, 100 * ONE, 102 * ONE, 0),
        );
        let mut pen = FillPen::new(instrument);
        dispatch_all(&mut engine, &pen.fill(Side::Buy, 100 * ONE, ONE, 10));
    }
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(0, 20)));
    for instrument in [0u16, 1] {
        assert_eq!(spin_reading(&readings, instrument, 0).pnl_quote, ONE);
    }

    engine.dispatch(
        pop(0, 0),
        &InboundMessage::MarketRotation(rotation(0, 30, 300_000_030, 30)),
    );
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(1, 40)));
    let rotated = spin_reading(&readings, 0, 1);
    assert_eq!(
        rotated.position_base,
        Qty(0),
        "the position was the old window's"
    );
    assert_eq!(rotated.pnl_quote, 0);
    assert!(
        !rotated.has_mark,
        "the old window's mid is not a price in the new one"
    );
    let untouched = spin_reading(&readings, 1, 1);
    assert_eq!(untouched.position_base, Qty(ONE));
    assert_eq!(untouched.pnl_quote, ONE);

    engine.dispatch(pop(0, 0), &run_control(idle_at(1), 50));
    engine.dispatch(pop(0, 0), &run_control(running_at(2), 600_000_050));
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(2, 600_000_060)));
    let resumed = spin_reading(&readings, 1, 2);
    assert_eq!(
        resumed.position_base,
        Qty(ONE),
        "the park sold nothing, so the position it opened is still held"
    );
    // exposure and PnL together pin the cash leg: -100 paid against a 101 mark is the ONE the row
    // carried in before the park, so neither half of the money was quietly reset.
    assert_eq!(resumed.exposure_quote, 101 * ONE);
    assert_eq!(resumed.pnl_quote, ONE, "realised cash survives a park");
    assert!(
        resumed.has_mark,
        "the instrument is the same one it was before the park, so its mark stands"
    );
    let still_rotated = spin_reading(&readings, 0, 2);
    assert_eq!(
        still_rotated.position_base,
        Qty(0),
        "a resume does not undo the rotation's flatten either — it touches no row at all"
    );
    assert!(!still_rotated.has_mark);
}

/// FITNESS: no mark before a two-sided committed book, and once marked, the price is held through
/// gaps. A one-sided book would value a position at half a spread it invented; re-marking on nothing
/// would zero a live position's exposure the moment a feed hiccuped.
#[test]
fn a_mark_needs_both_sides_and_survives_a_book_reset() {
    let instruments = [bare_row(0)];
    let (mut engine, readings) = probe_engine(&instruments);

    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(0, 1)));
    assert!(
        !spin_reading(&readings, 0, 0).has_mark,
        "no book has committed at all"
    );

    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Book(last_snapshot_chunk(0, Side::Buy, &[(100 * ONE, ONE)], 10)),
    );
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(1, 11)));
    assert!(
        !spin_reading(&readings, 0, 1).has_mark,
        "a book with bids and no asks is not a valuation"
    );

    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Book(delta_chunk(0, Side::Sell, &[(102 * ONE, ONE)], 20)),
    );
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(2, 21)));
    assert!(
        spin_reading(&readings, 0, 2).has_mark,
        "both sides are present at a commit boundary"
    );

    let mut pen = FillPen::new(0);
    dispatch_all(&mut engine, &pen.fill(Side::Buy, 100 * ONE, ONE, 30));
    engine.dispatch(pop(0, 0), &InboundMessage::BookReset(book_reset(0, 40)));
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(3, 41)));
    let held = spin_reading(&readings, 0, 3);
    assert!(
        held.has_mark,
        "a reset is a data gap, not news about the price"
    );
    assert_eq!(held.exposure_quote, 101 * ONE);
    assert_eq!(held.pnl_quote, ONE);
}

/// FITNESS: the ledger is a pure function of the message sequence. Every reset it performs is
/// message-driven, so the same tape read twice gives the same numbers at the same points — the
/// property replay-based backtesting will rest on.
#[test]
fn replaying_one_tape_twice_reads_the_same_ledger() {
    let instruments = [bare_row(0)];
    let tape = mixed_tape();
    let first = read_back(&instruments, &tape);
    let second = read_back(&instruments, &tape);
    assert!(
        first.len() > 40,
        "the tape produced only {} readings — nothing meaningful is being compared",
        first.len()
    );
    assert_eq!(
        first, second,
        "the same tape produced different ledgers, so something outside the message sequence \
         reached the fold"
    );
}

fn read_back(instruments: &[InstrumentRow], tape: &[InboundMessage]) -> Vec<Reading> {
    let (mut engine, readings) = probe_engine(instruments);
    dispatch_all(&mut engine, tape);
    take_readings(&readings)
}

/// Fills on both sides, prints, re-seated books, a rotation and a park/resume — every path that
/// writes the ledger, in one sequence.
fn mixed_tape() -> Vec<InboundMessage> {
    let mut messages = Vec::new();
    let mut pen = FillPen::new(0);
    messages.extend(reseat_book(0, 100 * ONE, 102 * ONE, 0));
    for step in 0..24i64 {
        let when = 100 + step * 1_000;
        let side = if step % 3 == 0 { Side::Sell } else { Side::Buy };
        messages.push(InboundMessage::Trade(trade(
            0,
            (100 + step % 5) * ONE,
            ONE / 2,
            side,
            when,
        )));
        messages.extend(pen.fill(side, (100 + step % 5) * ONE, ONE / 2, when));
        if step % 7 == 3 {
            messages.extend(reseat_book(
                0,
                (99 + step % 4) * ONE,
                (101 + step % 4) * ONE + TICK,
                when + 1,
            ));
        }
        if step == 11 {
            messages.push(InboundMessage::MarketRotation(rotation(
                0,
                when,
                when + 300_000_000,
                when + 2,
            )));
        }
        if step == 17 {
            messages.push(run_control(idle_at(1), when + 2));
            messages.push(run_control(running_at(2), when + 3));
        }
        messages.push(InboundMessage::SpinTick(spin(step as u64, when + 5)));
    }
    messages
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
            ctx.position_base(InstrumentId(0)).0,
            ctx.pnl_quote(InstrumentId(0)),
        ));
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
#[test]
fn a_fill_moves_cash_the_way_its_side_and_its_commission_say() {
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let latest = Arc::new(Mutex::new(None));
    let (persistence, _persist) = persist_ring(256);
    let (log_sink, _logs) = strategy_log_ring(64);
    let (metrics, _metrics) = metrics_ring(64);
    let mut engine = engine_without_warmup(
        &instruments,
        Box::new(MoneyProbe {
            latest: Arc::clone(&latest),
        }),
        persistence,
        log_sink,
        metrics,
    );
    let mut pen = FillPen::new(0);

    // Buy one whole base unit at 100. No mark yet, so PnL is the bare cost basis — which is exactly
    // the number that has to be NEGATIVE: we spent quote to get base.
    for message in pen.fill(Side::Buy, 100 * ONE, ONE, 10) {
        engine.dispatch(pop(0, 0), &message);
    }
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(0, 20)));
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
        engine.dispatch(pop(0, 0), &message);
    }
    engine.dispatch(pop(0, 0), &InboundMessage::SpinTick(spin(1, 40)));
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
        let InboundMessage::Exec(report) = pen.report(Side::Buy, 100 * ONE, ONE, when) else {
            unreachable!("the pen reports a fill as an Exec message");
        };
        let charged = InboundMessage::Exec(ExecEvent {
            commission,
            commission_asset,
            ..report
        });
        engine.dispatch(pop(0, 0), &charged);
        engine.dispatch(
            pop(0, 0),
            &InboundMessage::SpinTick(spin(2 + step as u64, when + 10)),
        );
        let (_, cash_after) = latest.lock().expect("poisoned").expect("the spin ran");
        assert_eq!(cash_after - cash_before, cash_gain - 100 * ONE, "{why}");
        cash_before = cash_after;
    }
}
