//! FITNESS: the session loss budget is a KILL SWITCH, and what decides whether it halts is whether
//! the loss has been REALISED — never whether the position happens to be exactly flat.
//!
//! The two verdicts differ because only one of them can still come back. A loss still marked to
//! market may be recovered by the position moving, so the engine withdraws the side that would ADD
//! to it and keeps the reducing side quoting: withdrawing both is a deadlock, since the only thing
//! that unwinds a position is a fill and the only thing that produces a fill is a quote
//! (a separate test covers that property). A loss booked into closed round trips and paid fees is
//! gone, and no price can hand it back — so it halts.
//!
//! Reading "realised" off an exactly-zero position is what this file exists to stop. One satoshi of
//! residue — the ordinary state after any partial fill — would turn the kill switch into a
//! permanent side restriction, and losses would keep growing on the side that was still admitted.

use polysim::config::{RecordedTables, TableKind, TrackerSpec};
use polysim::exposure::InstrumentExposure;
use polysim::hot::exec::{ExecSettings, LossVerdict, SessionPnl, assess_loss};
use polysim::ids::{InstrumentId, Qty, Side};
use polysim::msg::inbound::InboundMessage;

use crate::engine_support::{FillPen, Idle, ONE, instrument_row, pop, rotation};
use crate::exec_support::{
    QuotingEngine, QuotingSetup, built_quoting_engine, exec_settings, has_halted, reseat_book,
    spin_at,
};

/// The shipped config's budget, at the shipped scale.
const BUDGET: i64 = 5 * ONE;

/// One mantissa of base: the residue a partial fill leaves, and the smallest amount that could
/// turn a kill switch into a side restriction.
const DUST: Qty = Qty(1);

fn session(
    mark_to_market_quote: Option<i64>,
    realised_quote: i64,
    position_base: Qty,
) -> SessionPnl {
    SessionPnl {
        mark_to_market_quote,
        realised_quote,
        position_base,
    }
}

/// FITNESS: a breached REALISED loss halts whatever the position is. Dust, a whole unit, or nothing
/// at all — the money is banked and no mark can return it, so there is no state in which the engine
/// may go on quoting.
#[test]
fn a_realised_breach_halts_whatever_residue_the_position_holds() {
    for position in [Qty(0), DUST, Qty(ONE), Qty(-ONE)] {
        assert_eq!(
            assess_loss(session(Some(-6 * ONE), -6 * ONE, position), BUDGET),
            LossVerdict::Realised,
            "a realised loss past the budget while holding {} is still a realised loss",
            position.0
        );
    }
}

/// FITNESS: `assess_loss` verdicts match a named table spanning all three outcomes — an unrealised
/// loss only withdraws the side that would add to it, a realised number is honest even before any
/// mark exists, and no breach is ever invented from profit, from a zero budget, or from a budget with
/// nothing yet lost against it. Each case's assertion message carries its name.
#[test]
fn assess_loss_verdicts_match_named_cases() {
    struct Case {
        name: &'static str,
        mark_to_market_quote: Option<i64>,
        realised_quote: i64,
        position_base: i64,
        budget: i64,
        expected: LossVerdict,
    }
    let cases = [
        Case {
            name: "unrealised_long_recovers_by_selling",
            mark_to_market_quote: Some(-6 * ONE),
            realised_quote: 0,
            position_base: ONE,
            budget: BUDGET,
            expected: LossVerdict::MarkToMarket {
                reducing: Side::Sell,
            },
        },
        Case {
            name: "unrealised_short_recovers_by_buying",
            mark_to_market_quote: Some(-6 * ONE),
            realised_quote: 0,
            position_base: -ONE,
            budget: BUDGET,
            expected: LossVerdict::MarkToMarket {
                reducing: Side::Buy,
            },
        },
        Case {
            name: "within_budget_on_both_legs",
            mark_to_market_quote: Some(-4 * ONE),
            realised_quote: -4 * ONE,
            position_base: DUST.0,
            budget: BUDGET,
            expected: LossVerdict::Within,
        },
        Case {
            name: "realised_breach_judged_before_any_mark_exists",
            mark_to_market_quote: None,
            realised_quote: -6 * ONE,
            position_base: DUST.0,
            budget: BUDGET,
            expected: LossVerdict::Realised,
        },
        Case {
            name: "no_mark_means_no_invented_mark_to_market_breach",
            mark_to_market_quote: None,
            realised_quote: -4 * ONE,
            position_base: ONE,
            budget: BUDGET,
            expected: LossVerdict::Within,
        },
        Case {
            name: "flat_and_untraded_at_zero_budget",
            mark_to_market_quote: Some(0),
            realised_quote: 0,
            position_base: 0,
            budget: 0,
            expected: LossVerdict::Within,
        },
        Case {
            name: "flat_and_untraded_at_a_real_budget",
            mark_to_market_quote: Some(0),
            realised_quote: 0,
            position_base: 0,
            budget: BUDGET,
            expected: LossVerdict::Within,
        },
        Case {
            name: "profit_not_a_breach_at_zero_budget",
            mark_to_market_quote: Some(ONE),
            realised_quote: ONE,
            position_base: ONE,
            budget: 0,
            expected: LossVerdict::Within,
        },
        Case {
            name: "profit_not_a_breach_at_real_budget",
            mark_to_market_quote: Some(ONE),
            realised_quote: ONE,
            position_base: ONE,
            budget: BUDGET,
            expected: LossVerdict::Within,
        },
        Case {
            name: "one_mantissa_lost_still_breaches_a_zero_budget",
            mark_to_market_quote: Some(-1),
            realised_quote: -1,
            position_base: 0,
            budget: 0,
            expected: LossVerdict::Realised,
        },
    ];
    for case in cases {
        let got = assess_loss(
            session(
                case.mark_to_market_quote,
                case.realised_quote,
                Qty(case.position_base),
            ),
            case.budget,
        );
        assert_eq!(
            got, case.expected,
            "case {}: got {:?}, want {:?}",
            case.name, got, case.expected
        );
    }
}

fn fixture(max_session_loss_quote: i64) -> QuotingEngine {
    fixture_restoring(max_session_loss_quote, &[])
}

fn fixture_restoring(
    max_session_loss_quote: i64,
    restored: &[InstrumentExposure],
) -> QuotingEngine {
    built_quoting_engine(QuotingSetup {
        row: instrument_row(0, TrackerSpec::default(), 64),
        strategy: Box::new(Idle),
        restored,
        // Every limit except the session budget is set so wide it cannot be what stops a quote, so
        // a halt in the command stream can only have come from the loss gate.
        settings: ExecSettings {
            max_session_loss_quote,
            ..exec_settings()
        },
        tables: RecordedTables::new(&[TableKind::Orders]),
        run_nonce: 0,
    })
}

fn dispatch_all(fixture: &mut QuotingEngine, messages: &[InboundMessage]) {
    for message in messages {
        fixture.engine.dispatch(pop(0, 0), message);
    }
}

/// FITNESS: a round trip that loses more than the budget and leaves one mantissa behind HALTS. The
/// risk is a gate that tests `signum() == 0`: the residue would read as an open position, the
/// verdict would downgrade to a side restriction, and the budget would stop being a kill switch for
/// the rest of the run while losses kept growing on the side that was still admitted.
#[test]
fn a_round_trip_that_breaches_the_budget_halts_even_holding_dust() {
    let mut fixture = fixture(BUDGET);
    dispatch_all(&mut fixture, &reseat_book(99 * ONE, 101 * ONE, 0));
    spin_at(&mut fixture.engine, 0, 10);
    assert!(
        !has_halted(&mut fixture.commands),
        "nothing has traded yet, so nothing may have tripped"
    );

    // Buy a whole unit at 100, then sell all but one mantissa of it at 90: ten quote units realised
    // against a budget of five, with dust left over.
    let mut pen = FillPen::new(0);
    dispatch_all(&mut fixture, &pen.fill(Side::Buy, 100 * ONE, ONE, 20));
    dispatch_all(&mut fixture, &reseat_book(89 * ONE, 91 * ONE, 30));
    dispatch_all(&mut fixture, &pen.fill(Side::Sell, 90 * ONE, ONE - 1, 40));
    spin_at(&mut fixture.engine, 1, 50);

    assert!(
        has_halted(&mut fixture.commands),
        "ten quote units are gone against a budget of five and the engine is still quoting — one \
         mantissa of residue must not downgrade a kill switch to a side restriction"
    );
}

/// FITNESS: the same shaped loss that is NOT realised does not halt. Without this the test above
/// passes for a gate that halts on any breach at all, which would deadlock every position it was
/// meant to protect — the engine must keep the reducing side quoting or it can never get back to
/// flat.
#[test]
fn an_unrealised_loss_of_the_same_size_does_not_halt() {
    let mut fixture = fixture(BUDGET);
    dispatch_all(&mut fixture, &reseat_book(99 * ONE, 101 * ONE, 0));
    spin_at(&mut fixture.engine, 0, 10);

    let mut pen = FillPen::new(0);
    dispatch_all(&mut fixture, &pen.fill(Side::Buy, 100 * ONE, ONE, 20));
    // The mark alone falls to 90 — no second fill, so nothing is closed and nothing is banked.
    dispatch_all(&mut fixture, &reseat_book(89 * ONE, 91 * ONE, 30));
    spin_at(&mut fixture.engine, 1, 40);

    assert!(
        !has_halted(&mut fixture.commands),
        "a position whose mark fell is still recoverable by trading, and halting there traps it"
    );
}

/// One sell against `opened`, run with NO book at all so nothing has a mark: the mark-to-market leg
/// has no verdict to give and the halt can only have come from the realised one.
fn halts_after(opened: &[(i64, i64)], closed: &[(i64, i64)], budget: i64) -> bool {
    halts_after_restoring(&[], opened, closed, budget)
}

fn halts_after_restoring(
    restored: &[InstrumentExposure],
    opened: &[(i64, i64)],
    closed: &[(i64, i64)],
    budget: i64,
) -> bool {
    let mut fixture = fixture_restoring(budget, restored);
    let mut pen = FillPen::new(0);
    let mut when = 10;
    for (side, fills) in [(Side::Buy, opened), (Side::Sell, closed)] {
        for (price, qty) in fills {
            dispatch_all(&mut fixture, &pen.fill(side, *price, *qty, when));
            when += 10;
        }
    }
    spin_at(&mut fixture.engine, 0, when);
    has_halted(&mut fixture.commands)
}

/// FITNESS: what the kill switch measures is the AVERAGE COST realised loss, and this pins the
/// number rather than the direction — the same tape is run at a budget one unit inside it and one
/// unit outside, so the gate has to fire in exactly one of the two.
///
/// Two lot prices and a partial close is the case where the plausible alternatives separate. Buying
/// at 100 and 120 and selling one unit at 100 loses ten at average cost; FIFO would say zero,
/// because the lot it closes is the one bought at 100. A gate reading cash alone would say twenty,
/// which is the whole position's unrealised loss with nothing closed at all. The second pair drives
/// a sell straight THROUGH flat into a short, where the basis released and the basis opened come out
/// of one fill and a fold that took either for the whole would be wrong by the other.
#[test]
fn the_halt_fires_on_the_average_cost_realised_loss() {
    let two_lots = [(100 * ONE, ONE), (120 * ONE, ONE)];
    let partial_close = [(100 * ONE, ONE)];
    assert!(
        halts_after(&two_lots, &partial_close, 9 * ONE),
        "closing one of two units bought at 100 and 120 for 100 realises ten — FIFO would call it \
         nothing and go on quoting"
    );
    assert!(
        !halts_after(&two_lots, &partial_close, 11 * ONE),
        "ten realised is inside a budget of eleven, and a gate reading the whole position's cash \
         would have found twenty"
    );

    // Selling two units against a position of one closes it and opens a short with the remainder.
    let through_flat = [(100 * ONE, ONE), (90 * ONE, 2 * ONE)];
    assert!(
        halts_after(&two_lots, &through_flat, 29 * ONE),
        "ten from the partial close and twenty more selling the last unit at 90 against a basis \
         of 110"
    );
    assert!(
        !halts_after(&two_lots, &through_flat, 31 * ONE),
        "the short the flip opened must carry its own cost basis rather than realise on arrival"
    );
}

/// A row a previous run left: `cash` is every run's banked result so far, `basis` is what the
/// position still held actually cost. The two are independent, and that is the point — a flat row
/// with cash in it is an engine that has traded before and is holding nothing right now.
fn restored(position_base: i64, cash_quote: i64, basis_quote: i64) -> [InstrumentExposure; 1] {
    [InstrumentExposure {
        instrument: InstrumentId(0),
        position_base: Qty(position_base),
        cash_quote,
        basis_quote,
    }]
}

/// FITNESS: a restored ledger's cash and basis are independent facts, and the budget answers only to
/// what THIS session realises against them — never to a previous run's profit (fail-open: a ledger
/// seeded with `basis = -cash` would release the inherited fifty back into realised PnL and raise the
/// ceiling every run that ends ahead), a previous run's loss (fail-closed: an absolute reading would
/// trip on the first round trip of every run that follows), or the true cost basis of a position this
/// run inherited rather than opened. Named cases below span all three; each pins both the halt and
/// the adjacent within-budget non-halt so the boundary, not just the direction, is under test.
#[test]
fn restored_ledger_boundaries_match_named_cases() {
    struct Case {
        name: &'static str,
        restored: [InstrumentExposure; 1],
        opened: &'static [(i64, i64)],
        closed: &'static [(i64, i64)],
        budget: i64,
        expect_halt: bool,
    }
    let cases = [
        Case {
            name: "inherited_profit_does_not_cover_a_six_unit_session_loss",
            restored: restored(0, 50 * ONE, 0),
            opened: &[(100 * ONE, ONE)],
            closed: &[(94 * ONE, ONE)],
            budget: BUDGET,
            expect_halt: true,
        },
        Case {
            name: "inherited_profit_is_moot_once_the_session_loss_is_within_budget",
            restored: restored(0, 50 * ONE, 0),
            opened: &[(100 * ONE, ONE)],
            closed: &[(96 * ONE, ONE)],
            budget: BUDGET,
            expect_halt: false,
        },
        Case {
            name: "inherited_loss_does_not_halt_a_flat_first_round_trip",
            restored: restored(0, -50 * ONE, 0),
            opened: &[(100 * ONE, ONE)],
            closed: &[(100 * ONE, ONE)],
            budget: BUDGET,
            expect_halt: false,
        },
        Case {
            name: "closing_a_restored_position_at_its_real_basis_hits_the_budget",
            restored: restored(ONE, -70 * ONE, 100 * ONE),
            opened: &[],
            closed: &[(95 * ONE, ONE)],
            budget: BUDGET,
            expect_halt: true,
        },
        Case {
            name: "the_same_close_is_within_a_slightly_wider_budget",
            restored: restored(ONE, -70 * ONE, 100 * ONE),
            opened: &[],
            closed: &[(95 * ONE, ONE)],
            budget: 6 * ONE,
            expect_halt: false,
        },
    ];
    for case in cases {
        let halted = halts_after_restoring(&case.restored, case.opened, case.closed, case.budget);
        assert_eq!(
            halted, case.expect_halt,
            "case {}: halted={halted}",
            case.name
        );
    }
}

/// A rotation, far enough from its close that nothing in the quote-stop path fires — the only thing
/// under test here is what the ledger reset does to the budget.
fn rotate(fixture: &mut QuotingEngine, when: i64) {
    fixture.engine.dispatch(
        pop(0, 0),
        &InboundMessage::MarketRotation(rotation(0, when, when + 300 * 1_000_000, when)),
    );
}

/// FITNESS: the budget spans the RUN, not the market. A venue that rotates every few minutes hands
/// the ledger a new row each time, and the losses of the window just retired go with it — so a
/// switch reading only the live row re-baselines twelve times an hour and can never reach a budget
/// no single window spends.
///
/// Two identical round trips either side of one rotation, each inside the budget and together past
/// it. The pair is the point: the first assertion proves the fixture really is inside the budget
/// before the rotation, so the halt after it cannot be the gate firing early.
#[test]
fn losses_realised_before_a_rotation_still_count_against_the_budget() {
    let mut fixture = fixture(BUDGET);
    let mut pen = FillPen::new(0);

    // No book anywhere in this test, so nothing has a mark and the halt can only come from the
    // realised leg.
    dispatch_all(&mut fixture, &pen.fill(Side::Buy, 100 * ONE, ONE, 10));
    dispatch_all(&mut fixture, &pen.fill(Side::Sell, 97 * ONE, ONE, 20));
    spin_at(&mut fixture.engine, 0, 30);
    assert!(
        !has_halted(&mut fixture.commands),
        "three units lost against a budget of five is inside it"
    );

    rotate(&mut fixture, 40);
    dispatch_all(&mut fixture, &pen.fill(Side::Buy, 100 * ONE, ONE, 50));
    dispatch_all(&mut fixture, &pen.fill(Side::Sell, 97 * ONE, ONE, 60));
    spin_at(&mut fixture.engine, 1, 70);

    assert!(
        has_halted(&mut fixture.commands),
        "six units are gone against a budget of five and the engine is still quoting — the rotation \
         forgave the first three"
    );
}

/// FITNESS: a rotation must not manufacture a loss out of the profit it just erased.
///
/// The mark-to-market leg is measured against a baseline taken at the first honest valuation, and a
/// rotation zeroes the row that baseline was taken from. Left standing, the next spin reads the
/// whole baseline as this instant's loss — and reads it while FLAT, which is the one shape that
/// halts rather than merely restricting a side. The engine would kill itself for making money.
#[test]
fn a_rotation_does_not_leave_a_stale_baseline_reading_as_a_loss() {
    let mut fixture = fixture(BUDGET);
    let mut pen = FillPen::new(0);

    // Bought below where the book then marks it, and bought BEFORE any book exists so the baseline
    // cannot be taken until the position is already held: the baseline is +10, not zero.
    dispatch_all(&mut fixture, &pen.fill(Side::Buy, 90 * ONE, ONE, 10));
    dispatch_all(&mut fixture, &reseat_book(99 * ONE, 101 * ONE, 20));
    spin_at(&mut fixture.engine, 0, 30);
    assert!(
        !has_halted(&mut fixture.commands),
        "a position ten units in profit has breached nothing"
    );

    rotate(&mut fixture, 40);
    spin_at(&mut fixture.engine, 1, 50);
    assert!(
        !has_halted(&mut fixture.commands),
        "the position and its profit were erased together, so the difference between them is not a \
         ten-unit loss this run has to answer for"
    );

    // And the gate is silenced, not disarmed: a real loss on the new window still kills the run.
    dispatch_all(&mut fixture, &pen.fill(Side::Buy, 100 * ONE, ONE, 60));
    dispatch_all(&mut fixture, &pen.fill(Side::Sell, 94 * ONE, ONE, 70));
    spin_at(&mut fixture.engine, 2, 80);
    assert!(
        has_halted(&mut fixture.commands),
        "six units realised on the new window is past the budget however clean the slate was"
    );
}
