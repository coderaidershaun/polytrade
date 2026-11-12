//! FITNESS: the venue's placement budget is spent by the engine's own count, not discovered by
//! being refused.
//!
//! A venue that grants an account so many order placements per window stops accepting them when the
//! grant is gone, and it stops accepting the NEXT one — which may be the marketable order that
//! closes a position. The engine therefore meters what it places and stops quoting first, so the
//! headroom that is left belongs to the way out. That ordering is the whole point of the gate, and
//! the flatten case at the bottom is the one that would cost money if it inverted.
//!
//! Every number here comes from message stamps: the meter advances on the spin tick and counts the
//! placements the spin minted, so a replayed tape refuses at exactly the same point.

use polysim::config::RecordedTables;
use polysim::exposure::InstrumentExposure;
use polysim::hot::dispatch::HotEngine;
use polysim::hot::exec::{
    DesiredQuote, ExecSettings, OrderBudget, OrderBudgetWindow, QuoteLevel, RejectReason,
};
use polysim::hot::strategy::{Strategy, StrategyCtx};
use polysim::ids::{Price, Qty, Side};
use polysim::msg::exec::{ExecCommand, ExecEvent, ExecKind, OrderStyle, VenueOrderStatus};
use polysim::msg::inbound::{InboundMessage, SpinTick};
use polysim::time::DurationUs;

use crate::engine_support::{ALL_TABLES, ONE, exec_event, local_refusals, pop};
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, QUOTE_QTY, QuotingEngine, QuotingSetup, TwoSidedQuoter,
    built_quoting_engine, drain_commands, exec_settings, make_ready, reseat_book, row_with_ceiling,
    spin_at,
};

/// Long enough that no case below rolls a window by accident — the one case that means to roll one
/// says so in its own stamps.
const WINDOW: DurationUs = DurationUs::from_secs(3_600);

/// Three placements is two spins of a two-sided ladder plus one, so the budget runs out in the
/// MIDDLE of a spin rather than tidily between two.
const GRANTED_PLACES: u32 = 3;

/// One spin per second, which is the shipped cadence.
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
        built.engine.dispatch(pop(0, 0), &message);
    }
    drain_commands(&mut built.commands);
    built
}

/// Retires an order the venue is holding, so the side is free to ask for another next spin. Without
/// it the ladder places once and is then blocked by its own resting order, and the budget would
/// never be reached at all.
fn report_canceled(engine: &mut HotEngine, command: ExecCommand, when: i64) {
    let ExecCommand::Place {
        client_id, side, ..
    } = command
    else {
        return;
    };
    engine.dispatch(
        pop(0, 0),
        &InboundMessage::Exec(ExecEvent {
            kind: ExecKind::ReportCanceled,
            status: Some(VenueOrderStatus::Canceled),
            ..exec_event(INSTRUMENT, client_id, side, 0, when)
        }),
    );
}

/// What one spin did: how many placements went out, and every refusal the engine reported. Kept
/// together per spin because the property under test is WHEN the answer changes, not just that it
/// eventually did.
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

/// A spin far enough from the last one that the book would be stale. Restating the book is what the
/// market data actor does anyway; without it the engine refuses for staleness and a case about the
/// budget would be reading the book-age gate instead.
fn quotable_spin(built: &mut QuotingEngine, spin: u64, when: i64) {
    for message in reseat_book(BID, ASK, when) {
        built.engine.dispatch(pop(0, 0), &message);
    }
    spin_at(&mut built.engine, spin, when);
}

fn total_places(transcript: &[SpinOutcome]) -> usize {
    transcript.iter().map(|spin| spin.places).sum()
}

const SPINS: u64 = 4;

/// FITNESS: a budget of N admits exactly N placements and refuses the next one. The two runs beside
/// it drive the SAME message sequence under a budget that cannot bite and under no budget at all —
/// without them, an engine that had simply stopped quoting for some other reason would read exactly
/// like the meter working.
#[test]
fn a_budget_of_n_admits_exactly_n_placements_and_refuses_the_next() {
    let mut granted = budgeted_engine(budget_of(GRANTED_PLACES), Box::new(TwoSidedQuoter));
    let metered = run_spins(&mut granted, SPINS);
    assert_eq!(
        total_places(&metered),
        GRANTED_PLACES as usize,
        "the venue granted {GRANTED_PLACES} placements and the engine sent a different number: \
         {metered:?}"
    );

    let mut ample = budgeted_engine(budget_of(1_000), Box::new(TwoSidedQuoter));
    let unmetered = run_spins(&mut ample, SPINS);
    assert!(
        total_places(&unmetered) > GRANTED_PLACES as usize,
        "the same spins under a budget far away must place more, or the run above is reading a \
         fixture that stopped asking: {unmetered:?}"
    );

    let mut undeclared = budgeted_engine(OrderBudget::NONE, Box::new(TwoSidedQuoter));
    let unbudgeted = run_spins(&mut undeclared, SPINS);
    assert_eq!(
        unbudgeted, unmetered,
        "a venue that declares no placement budget must behave exactly as one whose budget is out \
         of reach — the gate is absent there, not permissive by luck"
    );
}

/// FITNESS: the meter frees a window that has PASSED and never one that has not. The counter keeps
/// coarse slots rather than a stamp per placement — a daily bucket of six figures rules out the
/// stamps — and the slots are deliberately biased: the span it counts is at least the venue's own
/// window, so it refuses early and can never admit a placement the venue still holds against us.
#[test]
fn the_meter_frees_a_window_that_has_passed_and_never_one_that_has_not() {
    let mut granted = budgeted_engine(budget_of(GRANTED_PLACES), Box::new(TwoSidedQuoter));
    let spent = run_spins(&mut granted, SPINS);
    assert_eq!(total_places(&spent), GRANTED_PLACES as usize);

    // Exactly one window after the last placement. The venue's own limiter may be releasing the
    // first placements about now; the engine must not assume it has.
    let last_place_at = at_spin(SPINS);
    let at_window_edge = last_place_at + WINDOW.micros();
    quotable_spin(&mut granted, SPINS + 1, at_window_edge);
    assert!(
        drain_commands(&mut granted.commands).is_empty(),
        "the meter released the budget at the very edge of the window it was spent in — it must \
         cover at least the venue's window, never less"
    );

    // Far enough past the window that no coarse slot can still hold the spending.
    quotable_spin(&mut granted, SPINS + 2, last_place_at + 2 * WINDOW.micros());
    let recovered = drain_commands(&mut granted.commands);
    assert!(
        recovered
            .iter()
            .any(|command| matches!(command, ExecCommand::Place { .. })),
        "a window that has wholly passed must return its budget, or the meter is a one-way latch \
         that stops the engine trading for the rest of the run: {recovered:?}"
    );
}

/// Two spins of a two-sided ladder spend the three granted placements and earn the refusal, so the
/// flatten arrives at an engine that already knows its budget is gone.
const FLATTEN_FROM: u64 = 3;

/// An engine holding a position it can be asked to shed, under a ceiling that position is nowhere
/// near — so both sides quote freely and the only thing left that can stop a placement is the
/// budget.
fn engine_holding_a_position(budget: OrderBudget, strategy: Box<dyn Strategy>) -> QuotingEngine {
    let restored = [InstrumentExposure {
        instrument: INSTRUMENT,
        position_base: Qty(ONE),
        cash_quote: -100 * ONE,
        basis_quote: 100 * ONE,
    }];
    ready_engine(budget, strategy, &restored, 1_000_000 * ONE)
}

/// Quotes both sides until `flatten_from`, then asks to get out. Two phases rather than two
/// strategies because the budget has to be spent by QUOTES before the flatten needs it gone.
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
            ctx.quote(
                INSTRUMENT,
                side,
                QuoteLevel::ZERO,
                Some(DesiredQuote {
                    price: Price(price),
                    qty: QUOTE_QTY,
                    style: OrderStyle::PostOnly,
                }),
            );
        }
    }
}

/// FITNESS: the exit and the meter's count of it, from both directions. An exhausted budget never
/// starves the order that reduces risk — the gate refuses QUOTES, and a quote costs a spread to skip
/// while a position the engine cannot shed costs whatever the market does next, so refusing quotes
/// early is precisely what leaves the venue-side headroom the exit needs. And the exit still SPENDS
/// the budget it can never be refused by: exemption from the gate is not exemption from the count, or
/// the meter reports headroom the venue has already given away and the next refusal comes from the
/// venue rather than from here — landing on whatever was placing at the time, which is the order this
/// gate exists to keep placeable.
#[test]
fn an_exhausted_budget_never_starves_the_flatten_and_the_flatten_still_spends_it() {
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
        "the quoting phase has to SPEND the budget or the case below proves nothing: {quoting:?}"
    );
    assert!(
        quoting
            .iter()
            .any(|spin| spin.refusals.contains(&RejectReason::RateBudget)),
        "and the engine has to know it is spent: {quoting:?}"
    );

    spin_at(&mut built.engine, FLATTEN_FROM, at_spin(FLATTEN_FROM));
    let exit = drain_commands(&mut built.commands);
    let marketable = exit.iter().any(|command| {
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
        marketable,
        "the engine held a position it wanted out of and refused its own exit for want of budget: \
         {exit:?}"
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
        "the exit had to go out first, or the quote behind it meets a budget nothing has spent: \
         {transcript:?}"
    );
    assert_eq!(
        transcript[1].places, 0,
        "the exit took the only granted placement, so the quote behind it had nothing left to \
         spend: {transcript:?}"
    );
    assert!(
        transcript[1].refusals.contains(&RejectReason::RateBudget),
        "and the quote must be refused BY THE BUDGET, naming what the exit spent: {transcript:?}"
    );
}

/// A grant of exactly one, so the exit either spends it or leaves it — there is no third outcome to
/// read the result two ways.
const SINGLE_PLACE: u32 = 1;

/// The spin the quote phase begins on, and the last spin the case runs: one spin of flatten is all
/// a single granted placement can pay for, so the quote is the final word.
const QUOTE_FROM: u64 = 2;

/// Asks to get out first, then quotes — the mirror of [`QuoteThenFlatten`]. Here the exit is what
/// spends the grant and the quote behind it is the only witness that it did. It quotes the side the
/// exit did NOT use, so nothing but the budget can be what refuses it.
struct FlattenThenQuote {
    quote_from: u64,
}

impl Strategy for FlattenThenQuote {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        if tick.seq < self.quote_from {
            ctx.flatten(INSTRUMENT);
            return;
        }
        ctx.quote(
            INSTRUMENT,
            Side::Buy,
            QuoteLevel::ZERO,
            Some(DesiredQuote {
                price: Price(BID),
                qty: QUOTE_QTY,
                style: OrderStyle::PostOnly,
            }),
        );
    }
}
