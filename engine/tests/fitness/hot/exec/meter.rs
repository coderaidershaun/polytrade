//! Pins the placement budget of a venue. The engine counts what it places and stops quoting first,
//! so the budget that is left belongs to the order that gets a market flat. A venue whose budget is
//! spent refuses the next order, and that can be the one that closes a position.

use std::cell::Cell;

use engine::hot::dispatch::HotEngine;
use engine::hot::exec::{
    DesiredQuote, ExecSettings, OrderBudget, OrderBudgetWindow, QuoteLevel, RejectReason,
};
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{
    ExecCommand, ExecEvent, ExecKind, ExecLaneItem, OrderStyle, VenueOrderStatus,
};
use engine::msg::ids::{ClientOrderId, Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::{DurationUs, LocalTs};
use engine::runtime::config::RecordedTables;
use engine::runtime::exposure::InstrumentExposure;
use proptest::prelude::{prop, prop_assert, prop_oneof};
use proptest::strategy::Strategy as Generator;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use rtrb::Consumer;

use crate::engine_support::{ALL_TABLES, ONE, exec_event, local_refusals, ts};
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, QUOTE_QTY, QuotingEngine, QuotingSetup, TwoSidedQuoter,
    built_quoting_engine, drain_commands, exec_settings, make_ready, reseat_book, row_with_ceiling,
    spin_at,
};

/// Long enough that no named case rolls a window by accident: the one case that rolls one says so
/// in its own stamps.
const WINDOW: DurationUs = DurationUs::from_secs(3_600);

/// Two spins of a two-sided ladder plus one, so the budget runs out in the middle of a spin.
const GRANTED_PLACES: u32 = 3;

const SPINS: u64 = 4;

/// One spin each second, the shipped cadence.
fn at_spin(spin: u64) -> i64 {
    spin as i64 * 1_000_000
}

fn budget_of(max_places: u32) -> OrderBudget {
    OrderBudget::of(&[OrderBudgetWindow {
        window: WINDOW,
        max_places,
    }])
    .expect("one window is inside the model")
}

fn budgeted_engine(budget: OrderBudget, strategy: Box<dyn Strategy>) -> QuotingEngine {
    ready_engine(budget, strategy, &[], CEILING)
}

/// Ready at 0 with a book seated at 0, under `budget`, with nothing yet on the lane.
fn ready_engine(
    budget: OrderBudget,
    strategy: Box<dyn Strategy>,
    restored: &[InstrumentExposure],
    ceiling: i64,
) -> QuotingEngine {
    let mut built = built_quoting_engine(QuotingSetup {
        row: row_with_ceiling(ceiling),
        strategy,
        restored,
        settings: ExecSettings {
            order_budget: budget,
            ..exec_settings()
        },
        tables: RecordedTables::new(&ALL_TABLES),
        run_nonce: 0,
    });
    make_ready(&mut built.engine, 0);
    for message in reseat_book(BID, ASK, 0) {
        built.engine.dispatch(&message);
    }
    drain_commands(&mut built.commands);
    built
}

/// Retires an order that the engine placed, so its side is free to ask again in the next spin.
/// Without it the ladder places once, its own resting order blocks it, and the budget is never
/// reached.
fn report_canceled(engine: &mut HotEngine, command: ExecCommand, when: i64) {
    let ExecCommand::Place {
        client_id, side, ..
    } = command
    else {
        return;
    };
    engine.dispatch(&InboundMessage::new(
        ts(when),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::ReportCanceled,
            status: Some(VenueOrderStatus::Canceled),
            ..exec_event(INSTRUMENT, client_id, side, 0, when)
        }),
    ));
}

/// What one spin did: how many placements went out, and each refusal that the engine reported.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SpinOutcome {
    places: usize,
    refusals: Vec<RejectReason>,
}

fn run_spins(built: &mut QuotingEngine, spins: u64) -> Vec<SpinOutcome> {
    let mut transcript = Vec::new();
    for spin in 1..=spins {
        let when = at_spin(spin);
        spin_at(&mut built.engine, spin, when);
        let commands = drain_commands(&mut built.commands);
        let places = commands
            .iter()
            .filter(|command| matches!(command, ExecCommand::Place { .. }))
            .count();
        for command in commands {
            report_canceled(&mut built.engine, command, when + 1);
        }
        transcript.push(SpinOutcome {
            places,
            refusals: local_refusals(&mut built.ui_events),
        });
    }
    transcript
}

/// A spin with the book seated again first, so a spin far from the last one reads the budget and
/// not the age of the book.
fn quotable_spin(built: &mut QuotingEngine, spin: u64, when: i64) {
    for message in reseat_book(BID, ASK, when) {
        built.engine.dispatch(&message);
    }
    spin_at(&mut built.engine, spin, when);
}

fn total_places(transcript: &[SpinOutcome]) -> usize {
    transcript.iter().map(|spin| spin.places).sum()
}

/// FITNESS: in any window of a budget, the engine never places more than the budget grants. A
/// placement past the budget is refused by the venue, and that refusal falls on whatever places
/// next, the order that closes a position included.
///
/// Over generated budgets of one or two windows and generated spin stamps, late ones included: a
/// late spin is folded at the time already reached, which is the only way an earlier stamp reaches
/// the meter. A budget of N admits N and refuses the next, under the same spins that place more
/// with a budget out of reach or with no budget at all. A window that has passed returns its
/// budget, and one that has not keeps it.
#[test]
fn placements_in_any_window_never_exceed_budget() {
    a_budget_of_n_admits_n_and_refuses_the_next();
    a_window_returns_its_budget_only_once_it_has_passed();
    let mut runner = TestRunner::new(Config {
        cases: 96,
        failure_persistence: None,
        ..Config::default()
    });
    let (late_spins, refused_runs) = (Cell::new(0), Cell::new(0));
    let outcome = runner.run(&(budgets(), spin_stamps()), |(windows, stamps)| {
        let run = metered_run(&windows, &stamps);
        late_spins.set(late_spins.get() + run.late_spins);
        refused_runs.set(refused_runs.get() + u32::from(run.is_refused));
        for declared in &windows {
            within_window(&run.placed_at, *declared)?;
        }
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("the engine placed past its budget: {failure}");
    }
    assert!(
        late_spins.get() > 0 && refused_runs.get() > 0,
        "the generated runs must fold late spins and spend a budget: {} late spins, {} runs \
         refused",
        late_spins.get(),
        refused_runs.get()
    );
}

fn a_budget_of_n_admits_n_and_refuses_the_next() {
    let mut granted = budgeted_engine(budget_of(GRANTED_PLACES), Box::new(TwoSidedQuoter));
    let metered = run_spins(&mut granted, SPINS);
    assert_eq!(
        total_places(&metered),
        GRANTED_PLACES as usize,
        "the venue granted {GRANTED_PLACES} placements: {metered:?}"
    );

    let mut ample = budgeted_engine(budget_of(1_000), Box::new(TwoSidedQuoter));
    let unmetered = run_spins(&mut ample, SPINS);
    assert!(
        total_places(&unmetered) > GRANTED_PLACES as usize,
        "the same spins under a budget out of reach place more: {unmetered:?}"
    );

    let mut undeclared = budgeted_engine(OrderBudget::NONE, Box::new(TwoSidedQuoter));
    let unbudgeted = run_spins(&mut undeclared, SPINS);
    assert_eq!(
        unbudgeted, unmetered,
        "a venue that declares no budget places as one whose budget is out of reach"
    );
}

/// The meter counts slots that together cover at least the venue's window, so it frees a placement
/// later than the venue does, never earlier.
fn a_window_returns_its_budget_only_once_it_has_passed() {
    let mut granted = budgeted_engine(budget_of(GRANTED_PLACES), Box::new(TwoSidedQuoter));
    let spent = run_spins(&mut granted, SPINS);
    assert_eq!(total_places(&spent), GRANTED_PLACES as usize);

    let last_place_at = at_spin(SPINS);
    quotable_spin(&mut granted, SPINS + 1, last_place_at + WINDOW.micros());
    assert!(
        drain_commands(&mut granted.commands).is_empty(),
        "the budget came back at the very edge of the window that spent it"
    );

    quotable_spin(&mut granted, SPINS + 2, last_place_at + 2 * WINDOW.micros());
    let recovered = drain_commands(&mut granted.commands);
    assert!(
        recovered
            .iter()
            .any(|command| matches!(command, ExecCommand::Place { .. })),
        "a window that has wholly passed returns its budget: {recovered:?}"
    );
}

/// One or two windows of whole seconds, as a venue declares them, each with a small grant.
fn budgets() -> impl Generator<Value = Vec<OrderBudgetWindow>> {
    let window = (1i64..=8, 1u32..=6).prop_map(|(secs, max_places)| OrderBudgetWindow {
        window: DurationUs::from_secs(secs),
        max_places,
    });
    prop::collection::vec(window, 1..=2)
}

/// The received stamp of each spin, as steps from the last one. A step back is a late spin.
fn spin_stamps() -> impl Generator<Value = Vec<i64>> {
    let step = prop_oneof![
        4 => 0i64..=2_000_000,
        1 => -2_000_000i64..0,
    ];
    prop::collection::vec(step, 4..=40)
}

/// What a quoting engine placed under a generated budget and generated spins.
struct MeteredRun {
    /// The time each placement was banked at, in order.
    placed_at: Vec<LocalTs>,
    late_spins: u32,
    is_refused: bool,
}

/// Quotes both sides at each spin, and the venue cancels each placement at once, so each spin asks
/// for two more.
fn metered_run(windows: &[OrderBudgetWindow], steps: &[i64]) -> MeteredRun {
    let budget = OrderBudget::of(windows).expect("at most two windows");
    let mut built = budgeted_engine(budget, Box::new(TwoSidedQuoter));
    let mut run = MeteredRun {
        placed_at: Vec::new(),
        late_spins: 0,
        is_refused: false,
    };
    let (mut received, mut folded) = (10_000_000, 0);
    for (seq, step) in (1u64..).zip(steps) {
        received += step;
        run.late_spins += u32::from(received < folded);
        folded = folded.max(received);
        spin_at(&mut built.engine, seq, received);
        for (command, issued) in drain_issued(&mut built.commands) {
            if matches!(command, ExecCommand::Place { .. }) {
                run.placed_at.push(issued);
            }
            report_canceled(&mut built.engine, command, received + 1);
            folded = folded.max(received + 1);
        }
        let refusals = local_refusals(&mut built.ui_events);
        run.is_refused |= refusals.contains(&RejectReason::RateBudget);
    }
    run
}

/// Each command on the lane, with the time it was banked at.
fn drain_issued(commands: &mut Consumer<ExecLaneItem>) -> Vec<(ExecCommand, LocalTs)> {
    let mut drained = Vec::new();
    while let Ok(item) = commands.pop() {
        if let ExecLaneItem::Command(stamped) = item {
            drained.push((stamped.command, stamped.issued_ts_us));
        }
    }
    drained
}

/// For each placement, the placements of the window that ends at it, itself included.
fn within_window(placed_at: &[LocalTs], declared: OrderBudgetWindow) -> Result<(), TestCaseError> {
    for (index, end) in placed_at.iter().enumerate() {
        let in_window = placed_at[..=index]
            .iter()
            .filter(|at| end.diff(**at) < declared.window)
            .count();
        prop_assert!(
            in_window <= declared.max_places as usize,
            "{in_window} placements in the {:?} that ends at {end:?}, against a grant of {}",
            declared.window,
            declared.max_places
        );
    }
    Ok(())
}

/// Quotes a bid of half a unit in spin 1, gets flat in spin 2, and quotes the bid again in spin 3.
struct QuoteFlatQuote;

const HALF: Qty = Qty::from_mantissa(ONE / 2);

impl Strategy for QuoteFlatQuote {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        if tick.seq == 2 {
            ctx.flatten(INSTRUMENT);
            return;
        }
        let bid = DesiredQuote {
            price: Price::from_mantissa(BID),
            qty: HALF,
        };
        ctx.quote(INSTRUMENT, Side::Buy, QuoteLevel::ZERO, Some(bid));
    }
}

/// CONTRACT: a spent budget never refuses the order that gets a market flat, and that order still
/// spends the budget. A local refusal of the way out would be the engine choosing to stay exposed,
/// and a way out that the meter did not count would leave the next refusal to the venue.
#[test]
fn spent_budget_never_refuses_flatten() {
    let one_a_minute = OrderBudget::of(&[OrderBudgetWindow {
        window: DurationUs::from_secs(60),
        max_places: 1,
    }])
    .expect("one window is inside the model");
    let QuotingEngine {
        mut engine,
        mut commands,
        mut ui_events,
    } = ready_engine(one_a_minute, Box::new(QuoteFlatQuote), &[], CEILING);

    spin_at(&mut engine, 1, 1_000_000);
    let placed = drain_commands(&mut commands);
    let [ExecCommand::Place { client_id, .. }] = placed[..] else {
        panic!("spin 1 places the bid: {placed:?}");
    };
    fill_whole(&mut engine, client_id, 1_500_000);

    spin_at(&mut engine, 2, 2_000_000);
    let exit = drain_commands(&mut commands);
    let is_one_take = matches!(
        exit[..],
        [ExecCommand::Place {
            side: Side::Sell,
            qty,
            style: OrderStyle::Immediate,
            ..
        }] if qty == HALF
    );
    assert!(
        is_one_take,
        "with the budget spent, getting flat still sends its one marketable sell: {exit:?}"
    );

    local_refusals(&mut ui_events);
    spin_at(&mut engine, 3, 3_000_000);
    assert!(
        drain_commands(&mut commands).is_empty(),
        "no quote is placed while the budget is spent"
    );
    let refusals = local_refusals(&mut ui_events);
    let by_budget = refusals
        .iter()
        .filter(|reason| **reason == RejectReason::RateBudget)
        .count();
    assert_eq!(by_budget, 1, "one refusal by the budget: {refusals:?}");

    exits_spend_the_budget_they_are_never_refused_by();
}

/// The bid of spin 1 fills whole, and nothing of it rests.
fn fill_whole(engine: &mut HotEngine, client_id: ClientOrderId, when: i64) {
    let price = Price::from_mantissa(BID);
    engine.dispatch(&InboundMessage::new(
        ts(when),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::ReportTrade,
            status: Some(VenueOrderStatus::Filled),
            qty: HALF,
            last_price: price,
            last_qty: HALF,
            cumulative_qty: HALF,
            cumulative_quote: price.notional(HALF),
            ..exec_event(INSTRUMENT, client_id, Side::Buy, BID, when)
        }),
    ));
}

/// Two spins of a two-sided ladder spend the three granted placements, so the order that gets flat
/// meets an engine that already knows its budget is gone.
const FLATTEN_FROM: u64 = 3;

/// A grant of exactly one, so the exit either spends it or leaves it.
const SINGLE_PLACE: u32 = 1;

/// The spin the quotes begin on, and the last spin of the case.
const QUOTE_FROM: u64 = 2;

/// Quotes both sides until `flatten_from`, then gets flat.
struct QuoteThenFlatten {
    flatten_from: u64,
}

impl Strategy for QuoteThenFlatten {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        if tick.seq >= self.flatten_from {
            ctx.flatten(INSTRUMENT);
            return;
        }
        for (side, price) in [(Side::Buy, BID), (Side::Sell, ASK)] {
            let quote = DesiredQuote {
                price: Price::from_mantissa(price),
                qty: QUOTE_QTY,
            };
            ctx.quote(INSTRUMENT, side, QuoteLevel::ZERO, Some(quote));
        }
    }
}

/// Gets flat until `quote_from`, then quotes the bid, the side that the exit did not use, so only
/// the budget can refuse it.
struct FlattenThenQuote {
    quote_from: u64,
}

impl Strategy for FlattenThenQuote {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        if tick.seq < self.quote_from {
            ctx.flatten(INSTRUMENT);
            return;
        }
        let bid = DesiredQuote {
            price: Price::from_mantissa(BID),
            qty: QUOTE_QTY,
        };
        ctx.quote(INSTRUMENT, Side::Buy, QuoteLevel::ZERO, Some(bid));
    }
}

/// An engine long one unit, far under its exposure ceiling, so only the budget can stop a quote.
fn engine_holding_a_position(budget: OrderBudget, strategy: Box<dyn Strategy>) -> QuotingEngine {
    let restored = [InstrumentExposure {
        instrument: INSTRUMENT,
        position_base: Qty::from_mantissa(ONE),
        cash_quote: -100 * ONE,
        basis_quote: 100 * ONE,
    }];
    ready_engine(budget, strategy, &restored, 1_000_000 * ONE)
}

/// Quotes spend the budget, and the exit still goes out. Then an exit that spends the only grant
/// leaves the quote behind it refused by the budget.
fn exits_spend_the_budget_they_are_never_refused_by() {
    let mut built = engine_holding_a_position(
        budget_of(GRANTED_PLACES),
        Box::new(QuoteThenFlatten {
            flatten_from: FLATTEN_FROM,
        }),
    );
    let quoting = run_spins(&mut built, FLATTEN_FROM - 1);
    assert_eq!(
        total_places(&quoting),
        GRANTED_PLACES as usize,
        "the quotes spend the budget first: {quoting:?}"
    );
    assert!(
        quoting
            .iter()
            .any(|spin| spin.refusals.contains(&RejectReason::RateBudget)),
        "and the engine knows it is spent: {quoting:?}"
    );
    spin_at(&mut built.engine, FLATTEN_FROM, at_spin(FLATTEN_FROM));
    let exit = drain_commands(&mut built.commands);
    let is_marketable = exit.iter().any(|command| {
        matches!(
            command,
            ExecCommand::Place {
                style: OrderStyle::Immediate,
                side: Side::Sell,
                ..
            }
        )
    });
    assert!(
        is_marketable,
        "the exit went out with the budget spent: {exit:?}"
    );

    let mut spends = engine_holding_a_position(
        budget_of(SINGLE_PLACE),
        Box::new(FlattenThenQuote {
            quote_from: QUOTE_FROM,
        }),
    );
    let transcript = run_spins(&mut spends, QUOTE_FROM);
    assert_eq!(
        transcript[0].places, 1,
        "the exit goes first: {transcript:?}"
    );
    assert_eq!(
        transcript[1].places, 0,
        "the exit took the only grant: {transcript:?}"
    );
    assert!(
        transcript[1].refusals.contains(&RejectReason::RateBudget),
        "and the quote behind it is refused by the budget: {transcript:?}"
    );
}
