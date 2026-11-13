//! Pins the loss budget and the exposure ceiling, which decide which side of which instrument may
//! quote and when quoting halts. A budget judged for each instrument alone, or a gate that traps a
//! position, loses money in a way that no later record shows.

use engine::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup};
use engine::hot::exec::{
    ClientIdLayout, DesiredQuote, ExecSettings, ExposureCheck, LossVerdict, QuoteLevel,
    QuotePermission, SessionPnl, assess_exposure, assess_loss, side_base,
};
use engine::hot::sink::ExecSink;
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::msg::exec::{
    CancelReason, ExecCommand, ExecEvent, ExecKind, ExecLaneItem, VenueOrderStatus,
};
use engine::msg::ids::{AssetId, InstrumentId, Notional, Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::Stamper;
use engine::runtime::config::{RecordedTables, TableKind, TrackerSpec};
use engine::runtime::exposure::InstrumentExposure;
use engine::runtime::registry::InstrumentRow;
use proptest::prelude::{any, prop, prop_assert, prop_assert_eq};
use proptest::test_runner::{Config, TestRunner};
use rtrb::{Consumer, RingBuffer};

use crate::engine_support::{
    FillPen, Idle, ONE, book_reset, detached_setup, exec_event, exposure_ring, instrument_row,
    rotation, snapshot_pair, ts,
};
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, QUOTE_QTY, QuotingEngine, QuotingSetup, built_quoting_engine,
    drain_commands, exec_settings, has_halted, is_placing, make_ready, quoting_engine, reseat_book,
    spin_at,
};
use crate::micro_strategy::models::{RiskBudget, signed};

/// The shipped config's budget, at the shipped scale.
const BUDGET: i64 = 5 * ONE;

/// One mantissa of base: the residue a partial fill leaves, and the smallest amount that could
/// turn a kill switch into a side restriction.
const DUST: Qty = Qty::from_mantissa(1);

fn session(
    mark_to_market_quote: Option<i64>,
    realised_quote: i64,
    position_base: Qty,
) -> SessionPnl {
    SessionPnl {
        mark_to_market_quote: mark_to_market_quote.map(Notional::from_mantissa),
        realised_quote: Notional::from_mantissa(realised_quote),
        position_base,
    }
}

/// a breached REALISED loss halts whatever the position is. Dust, a whole unit, or nothing
/// at all — the money is banked and no mark can return it, so there is no state in which the engine
/// may go on quoting.
fn a_realised_breach_halts_whatever_residue_the_position_holds() {
    for position in [
        Qty::from_mantissa(0),
        DUST,
        Qty::from_mantissa(ONE),
        Qty::from_mantissa(-ONE),
    ] {
        assert_eq!(
            assess_loss(
                session(Some(-6 * ONE), -6 * ONE, position),
                Notional::from_mantissa(BUDGET)
            ),
            LossVerdict::Realised,
            "a realised loss past the budget while holding {} is still a realised loss",
            position.mantissa()
        );
    }
}

/// `assess_loss` verdicts match a named table spanning all three outcomes — an unrealised
/// loss only withdraws the side that would add to it, a realised number is honest even before any
/// mark exists, and no breach is ever invented from profit, from a zero budget, or from a budget with
/// nothing yet lost against it. Each case's assertion message carries its name.
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
            position_base: DUST.mantissa(),
            budget: BUDGET,
            expected: LossVerdict::Within,
        },
        Case {
            name: "realised_breach_judged_before_any_mark_exists",
            mark_to_market_quote: None,
            realised_quote: -6 * ONE,
            position_base: DUST.mantissa(),
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
                Qty::from_mantissa(case.position_base),
            ),
            Notional::from_mantissa(case.budget),
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
            max_session_loss_quote: Notional::from_mantissa(max_session_loss_quote),
            ..exec_settings()
        },
        tables: RecordedTables::new(&[TableKind::Orders]),
        run_nonce: 0,
    })
}

fn dispatch_all(fixture: &mut QuotingEngine, messages: &[InboundMessage]) {
    for message in messages {
        fixture.engine.dispatch(message);
    }
}

/// a round trip that loses more than the budget and leaves one mantissa behind HALTS. The
/// risk is a gate that tests `signum() == 0`: the residue would read as an open position, the
/// verdict would downgrade to a side restriction, and the budget would stop being a kill switch for
/// the rest of the run while losses kept growing on the side that was still admitted.
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

/// the same shaped loss that is NOT realised does not halt. Without this the test above
/// passes for a gate that halts on any breach at all, which would deadlock every position it was
/// meant to protect — the engine must keep the reducing side quoting or it can never get back to
/// flat.
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

/// what the kill switch measures is the AVERAGE COST realised loss, and this pins the
/// number rather than the direction — the same tape is run at a budget one unit inside it and one
/// unit outside, so the gate has to fire in exactly one of the two.
///
/// Two lot prices and a partial close is the case where the plausible alternatives separate. Buying
/// at 100 and 120 and selling one unit at 100 loses ten at average cost; FIFO would say zero,
/// because the lot it closes is the one bought at 100. A gate reading cash alone would say twenty,
/// which is the whole position's unrealised loss with nothing closed at all. The second pair drives
/// a sell straight THROUGH flat into a short, where the basis released and the basis opened come out
/// of one fill and a fold that took either for the whole would be wrong by the other.
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
        position_base: Qty::from_mantissa(position_base),
        cash_quote,
        basis_quote,
    }]
}

/// a restored ledger's cash and basis are independent facts, and the budget answers only to
/// what THIS session realises against them — never to a previous run's profit (fail-open: a ledger
/// seeded with `basis = -cash` would release the inherited fifty back into realised PnL and raise the
/// ceiling every run that ends ahead), a previous run's loss (fail-closed: an absolute reading would
/// trip on the first round trip of every run that follows), or the true cost basis of a position this
/// run inherited rather than opened. Named cases below span all three; each pins both the halt and
/// the adjacent within-budget non-halt so the boundary, not just the direction, is under test.
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
    fixture
        .engine
        .dispatch(&rotation(0, when, when + 300 * 1_000_000, when));
}

/// the budget spans the RUN, not the market. A venue that rotates every few minutes hands
/// the ledger a new row each time, and the losses of the window just retired go with it — so a
/// switch reading only the live row re-baselines twelve times an hour and can never reach a budget
/// no single window spends.
///
/// Two identical round trips either side of one rotation, each inside the budget and together past
/// it. The pair is the point: the first assertion proves the fixture really is inside the budget
/// before the rotation, so the halt after it cannot be the gate firing early.
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

/// a rotation must not manufacture a loss out of the profit it just erased.
///
/// The mark-to-market leg is measured against a baseline taken at the first honest valuation, and a
/// rotation zeroes the row that baseline was taken from. Left standing, the next spin reads the
/// whole baseline as this instant's loss — and reads it while FLAT, which is the one shape that
/// halts rather than merely restricting a side. The engine would kill itself for making money.
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

/// The mid of `BID` and `ASK`, which is what the ledger marks against.
const MARK: i64 = 100 * ONE;

fn ceiling_check(side: Side, position_base: i64, has_mark: bool) -> ExposureCheck {
    ExposureCheck {
        exposure_quote: if has_mark {
            Price::from_mantissa(MARK).notional(Qty::from_mantissa(position_base))
        } else {
            Notional::from_mantissa(0)
        },
        position_base: Qty::from_mantissa(position_base),
        has_mark,
        side,
        price: Price::from_mantissa(match side {
            Side::Buy => BID,
            Side::Sell => ASK,
        }),
        qty: QUOTE_QTY,
        ceiling_quote: Notional::from_mantissa(CEILING),
    }
}

/// `assess_exposure` verdicts at the ceiling match a named table. From over the ceiling the
/// engine withdraws the side that would ADD to the position and keeps the side that would reduce it —
/// the reducing side admitted by construction rather than by arithmetic, so no rounding or sign error
/// can reach it — and an UNMARKED position is not a flat one: the ledger's mark is `None` until the
/// first two-sided committed book, exposure reads 0 in that window too, and only `has_mark` tells the
/// two zeroes apart, so a restarted engine holding unvalued inventory must refuse to grow it rather
/// than read itself as flat.
fn assess_exposure_ceiling_verdicts_match_named_cases() {
    struct Case {
        name: &'static str,
        side: Side,
        position_base: i64,
        has_mark: bool,
        exposure_override: Option<i64>,
        expected: QuotePermission,
    }
    let cases = [
        Case {
            name: "long_at_the_ceiling_buy_is_withdrawn",
            side: Side::Buy,
            position_base: ONE,
            has_mark: true,
            exposure_override: None,
            expected: QuotePermission::ReducingOnly {
                reducing: Side::Sell,
            },
        },
        Case {
            name: "long_at_the_ceiling_sell_is_the_way_back",
            side: Side::Sell,
            position_base: ONE,
            has_mark: true,
            exposure_override: None,
            expected: QuotePermission::Both,
        },
        // Twice the ceiling — the regime this file was written for, since a price move alone can
        // carry exposure there with no fill at all. The unwinding side is not merely permitted, it
        // is unobjectionable: a gate answering "restricted, but not on this side" from deep over the
        // budget is reasoning about the position when it was asked about the way out of it.
        Case {
            name: "twice_the_ceiling_the_unwinding_side_is_unobjectionable",
            side: Side::Sell,
            position_base: 2 * ONE,
            has_mark: true,
            exposure_override: None,
            expected: QuotePermission::Both,
        },
        Case {
            name: "short_at_the_ceiling_sell_is_withdrawn",
            side: Side::Sell,
            position_base: -ONE,
            has_mark: true,
            exposure_override: None,
            expected: QuotePermission::ReducingOnly {
                reducing: Side::Buy,
            },
        },
        Case {
            name: "short_at_the_ceiling_buy_is_the_way_back",
            side: Side::Buy,
            position_base: -ONE,
            has_mark: true,
            exposure_override: None,
            expected: QuotePermission::Both,
        },
        Case {
            name: "an_unmarked_long_refuses_the_growing_side",
            side: Side::Buy,
            position_base: ONE,
            has_mark: false,
            exposure_override: None,
            expected: QuotePermission::ReducingOnly {
                reducing: Side::Sell,
            },
        },
        Case {
            name: "an_unmarked_long_stays_reducible",
            side: Side::Sell,
            position_base: ONE,
            has_mark: false,
            exposure_override: None,
            expected: QuotePermission::Both,
        },
        // Without this case the branch reading has_mark could be deleted and every other assertion
        // in this table would still pass: a mark of zero is an honest valuation, distinct from no
        // mark at all, and threatens the ceiling not at all.
        Case {
            name: "a_mark_of_zero_is_an_honest_valuation",
            side: Side::Buy,
            position_base: ONE,
            has_mark: true,
            exposure_override: Some(0),
            expected: QuotePermission::Both,
        },
    ];
    for case in cases {
        let mut check = ceiling_check(case.side, case.position_base, case.has_mark);
        if let Some(exposure_quote) = case.exposure_override {
            check.exposure_quote = Notional::from_mantissa(exposure_quote);
        }
        let got = assess_exposure(check);
        assert_eq!(
            got, case.expected,
            "case {}: got {:?}, want {:?}",
            case.name, got, case.expected
        );
    }
}

/// the projection is the order's REAL notional. The strategy must guess with a fixed
/// per-fill step because it has no price for a fill that has not happened; the engine holds the
/// price and size about to be sent, and two quotes that differ only in size must be judged
/// differently. A gate stepping by a constant cannot tell these two apart.
fn the_ceiling_projects_the_declared_notional_rather_than_a_fixed_step() {
    // Nine tenths of the way to the ceiling, so the two sizes below straddle it.
    let position = ONE * 9 / 10;
    let small = ExposureCheck {
        qty: Qty::from_mantissa(ONE / 10),
        ..ceiling_check(Side::Buy, position, true)
    };
    assert_eq!(
        small.exposure_quote + Price::from_mantissa(BID).notional(small.qty),
        Notional::from_mantissa(CEILING - ONE / 10),
        "the small quote lands just inside the ceiling"
    );
    assert_eq!(
        assess_exposure(small),
        QuotePermission::Both,
        "so it is admitted"
    );

    let large = ExposureCheck {
        qty: Qty::from_mantissa(ONE / 5),
        ..small
    };
    assert!(
        large.exposure_quote + Price::from_mantissa(BID).notional(large.qty)
            > Notional::from_mantissa(CEILING),
        "twice the size clears the ceiling — the scenario needs both sides of it"
    );
    assert_eq!(
        assess_exposure(large),
        QuotePermission::ReducingOnly {
            reducing: Side::Sell
        },
        "the same position and the same ceiling, refused only because the ORDER is bigger"
    );
}

/// One real fill through the inbound path: adopt the order from a reconciliation snapshot, then
/// report it FULLY filled. Closing it matters — a side still holding a working order is blocked by
/// the in-flight rule, and a test reading "no order placed" there would be reading that rule rather
/// than the ceiling.
fn fill_and_close(engine: &mut HotEngine, side: Side, price: i64, qty: i64, when: i64) {
    let client_id = ClientIdLayout { run_nonce: 0 }.encode(side_base(INSTRUMENT, side) + 1, 1);
    let base = ExecEvent {
        qty: Qty::from_mantissa(qty),
        ..exec_event(INSTRUMENT, client_id, side, price, when)
    };
    engine.dispatch(&InboundMessage::new(
        ts(when),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::SnapshotOrder,
            ..base
        }),
    ));
    engine.dispatch(&InboundMessage::new(
        ts(when),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::ReportTrade,
            status: Some(VenueOrderStatus::Filled),
            last_price: Price::from_mantissa(price),
            last_qty: Qty::from_mantissa(qty),
            cumulative_qty: Qty::from_mantissa(qty),
            cumulative_quote: Price::from_mantissa(price).notional(Qty::from_mantissa(qty)),
            ..base
        }),
    ));
}

/// The message sequence both engine-driven tests below share: come ready, take a long position by
/// the route the caller chose, then see the book that values it at exactly the ceiling.
fn quote_holding_one_unit(
    ceiling_quote: i64,
    restored: &[InstrumentExposure],
    fill: Option<Side>,
) -> Vec<ExecCommand> {
    let (mut engine, mut commands) = quoting_engine(ceiling_quote, restored);
    make_ready(&mut engine, 0);
    if let Some(side) = fill {
        fill_and_close(&mut engine, side, MARK, ONE, 5);
    }
    for message in reseat_book(BID, ASK, 10) {
        engine.dispatch(&message);
    }
    spin_at(&mut engine, 1, 20);
    drain_commands(&mut commands)
}

/// a position onto the ceiling withdraws the adding side at the ENGINE, whatever the
/// strategy keeps declaring — and leaves the reducing side quoting — whether the position arrived
/// through a real fill this run or was inherited whole from a restart.
///
/// The fill case is driven twice over one message sequence, differing only in the row's
/// `max_exposure_quote`. The loose run is what makes the tight one mean something: it proves this
/// fixture really does reach the quoting path, so "no order on the buy side" is the ceiling refusing
/// and not the fixture failing to ask. The restored case then proves the same gate reads a
/// cross-session ledger rather than starting the run believing itself flat.
fn a_position_at_the_ceiling_stops_the_engine_adding_whether_filled_or_restored() {
    let tight = quote_holding_one_unit(CEILING, &[], Some(Side::Buy));
    assert!(
        !is_placing(&tight, Side::Buy),
        "the position is marked at the ceiling, so nothing may grow it: {tight:?}"
    );
    assert!(
        is_placing(&tight, Side::Sell),
        "and the side that unwinds it must still quote — no quote means no fill, and no fill means \
         no way down: {tight:?}"
    );

    let loose = quote_holding_one_unit(1_000_000 * ONE, &[], Some(Side::Buy));
    assert!(
        is_placing(&loose, Side::Buy),
        "the same sequence under a ceiling far away must quote both sides, or the test above is \
         reading a fixture that never asked for an order: {loose:?}"
    );
    assert!(
        is_placing(&loose, Side::Sell),
        "and the other side: {loose:?}"
    );

    // An engine that BOOTS holding inventory is held to the same ceiling. The ledger is
    // cross-session, so a restart inherits a position no fill this run produced — and the gate must
    // read it off the restored cost basis rather than starting the run believing itself flat.
    let restored = [InstrumentExposure {
        instrument: INSTRUMENT,
        position_base: Qty::from_mantissa(ONE),
        cash_quote: -MARK,
        basis_quote: MARK,
    }];
    let inherited = quote_holding_one_unit(CEILING, &restored, None);
    assert!(
        !is_placing(&inherited, Side::Buy),
        "the inherited position is already at the ceiling: {inherited:?}"
    );
    assert!(
        is_placing(&inherited, Side::Sell),
        "and unwinding it is exactly what a restart holding inventory needs to do: {inherited:?}"
    );
}

/// The quote assets of the instruments of several markets.
const USDT: AssetId = AssetId(1);
const USDC: AssetId = AssetId(2);

/// The most instruments that a generated run quotes or trades.
const MAX_INSTRUMENTS: u16 = 1;

fn runner(cases: u32) -> TestRunner {
    TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    })
}

fn row(id: u16, quote_asset: AssetId) -> InstrumentRow {
    InstrumentRow {
        quote_asset,
        ..instrument_row(id, TrackerSpec::default(), 64)
    }
}

/// An engine over several instruments, with its command lane and one fill pen for each instrument.
struct Markets {
    engine: HotEngine,
    commands: Consumer<ExecLaneItem>,
    pens: Vec<FillPen>,
}

/// Each limit but the loss budget is out of reach, so a refusal or a halt comes from the budget.
fn markets(
    rows: &[InstrumentRow],
    restored: &[InstrumentExposure],
    budget: i64,
    strategy: Box<dyn Strategy>,
) -> Markets {
    let (producer, commands) = RingBuffer::new(1_024);
    let engine = HotEngine::new(HotEngineSetup {
        exec: Some(ExecWiring {
            sink: ExecSink::new(producer, Stamper::Off),
            settings: ExecSettings {
                max_session_loss_quote: Notional::from_mantissa(budget),
                ..exec_settings()
            },
            run_nonce: 0,
        }),
        exposure: ExposureWiring {
            restored,
            sink: exposure_ring(64).0,
        },
        ..detached_setup(rows, strategy)
    });
    let pens = rows
        .iter()
        .map(|row| FillPen::new(row.instrument_id.0))
        .collect();
    Markets {
        engine,
        commands,
        pens,
    }
}

/// Buys one unit at 100 and sells it at 100 plus `realised`, so the round trip realises `realised`.
fn round_trip(markets: &mut Markets, instrument: u16, realised: i64, when: i64) {
    let pen = &mut markets.pens[usize::from(instrument)];
    let mut messages = pen.fill(Side::Buy, 100 * ONE, ONE, when);
    messages.extend(pen.fill(Side::Sell, 100 * ONE + realised, ONE, when + 1));
    for message in &messages {
        markets.engine.dispatch(message);
    }
}

/// A book of one unit a side, one unit either side of `mid`.
fn seat(engine: &mut HotEngine, instrument: u16, mid: i64, when: i64) {
    let (bids, asks) = snapshot_pair(instrument, &[(mid - ONE, ONE)], &[(mid + ONE, ONE)], when);
    engine.dispatch(&book_reset(instrument, when));
    engine.dispatch(&InboundMessage::new(ts(when), MessageBody::Book(bids)));
    engine.dispatch(&InboundMessage::new(ts(when), MessageBody::Book(asks)));
}

/// Quotes a bid and an offer at the touch of each of its instruments, at each spin.
struct EachMarketQuoter {
    instruments: u16,
}

impl Strategy for EachMarketQuoter {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        for id in 0..self.instruments {
            let instrument = InstrumentId(id);
            let book = ctx.book(instrument);
            let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) else {
                continue;
            };
            for (side, price) in [(Side::Buy, bid.price), (Side::Sell, ask.price)] {
                let quote = DesiredQuote {
                    price,
                    qty: QUOTE_QTY,
                };
                ctx.quote(instrument, side, QuoteLevel::ZERO, Some(quote));
            }
        }
    }
}

/// Each side that the spin placed on, by instrument, and whether the spin halted quoting. The venue
/// cancels each placement at once, so each side is free again at the next spin.
fn spin_outcome(markets: &mut Markets, seq: u64, when: i64) -> (Vec<(u16, Side)>, bool) {
    spin_at(&mut markets.engine, seq, when);
    let commands = drain_commands(&mut markets.commands);
    let is_halted = commands.iter().any(|command| {
        matches!(
            command,
            ExecCommand::CancelOurs {
                reason: CancelReason::Halt,
                ..
            }
        )
    });
    let mut placed = Vec::new();
    for command in commands {
        let ExecCommand::Place {
            instrument,
            client_id,
            side,
            ..
        } = command
        else {
            continue;
        };
        placed.push((instrument.0, side));
        markets.engine.dispatch(&InboundMessage::new(
            ts(when + 1),
            MessageBody::Exec(ExecEvent {
                kind: ExecKind::ReportCanceled,
                status: Some(VenueOrderStatus::Canceled),
                ..exec_event(instrument, client_id, side, 0, when + 1)
            }),
        ));
    }
    placed.sort_by_key(|(instrument, side)| (*instrument, side.index()));
    (placed, is_halted)
}

/// FITNESS: quoting halts exactly when the realised loss of a quote asset, summed over each of its
/// instruments and each window, reaches the budget. A budget judged for each instrument alone lets
/// N instruments each lose most of it, and a sum across quote assets adds amounts that are not the
/// same money.
///
/// Over 1 to N instruments, each in one of two quote assets, and generated round trips of gains and
/// losses. The named cases hold the verdict table, a residue after a breach, the average-cost loss,
/// a restored ledger, a loss before a rotation, and no stale baseline after one.
#[test]
fn halt_exactly_when_summed_realised_loss_reaches_budget() {
    a_realised_breach_halts_whatever_residue_the_position_holds();
    assess_loss_verdicts_match_named_cases();
    a_round_trip_that_breaches_the_budget_halts_even_holding_dust();
    the_halt_fires_on_the_average_cost_realised_loss();
    restored_ledger_boundaries_match_named_cases();
    losses_realised_before_a_rotation_still_count_against_the_budget();
    a_rotation_does_not_leave_a_stale_baseline_reading_as_a_loss();

    let assets = prop::collection::vec(
        prop::sample::select(vec![USDT, USDC]),
        1..=usize::from(MAX_INSTRUMENTS),
    );
    let trips = prop::collection::vec((0u16..MAX_INSTRUMENTS, -15i64..=10), 1..=8);
    let outcome = runner(64).run(&(assets, 1i64..=20, trips), |(assets, budget, trips)| {
        let rows: Vec<InstrumentRow> = (0u16..)
            .zip(&assets)
            .map(|(id, asset)| row(id, *asset))
            .collect();
        let mut markets = markets(&rows, &[], budget * ONE, Box::new(Idle));
        let mut sums = [0i64; 3];
        let mut expected = None;
        let mut halted = None;
        for (seq, (instrument, units)) in (1u64..).zip(&trips) {
            let instrument = instrument % assets.len() as u16;
            let when = seq as i64 * 1_000;
            round_trip(&mut markets, instrument, units * ONE, when);
            sums[usize::from(assets[usize::from(instrument)].0)] += units;
            if expected.is_none() && sums.iter().any(|sum| *sum <= -budget) {
                expected = Some(seq);
            }
            let (_, is_halted) = spin_outcome(&mut markets, seq, when + 10);
            if is_halted && halted.is_none() {
                halted = Some(seq);
            }
        }
        prop_assert_eq!(
            halted,
            expected,
            "sums by asset {:?} against a budget of {}",
            sums,
            budget
        );
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("the halt did not follow the summed realised loss: {failure}");
    }
}

/// FITNESS: at each instrument some side can always quote, and the side that reduces a held position
/// is never withdrawn. A gate that withdraws both sides traps the position: no quote means no fill.
///
/// Over 1 to N instruments, each restored with a long or a short or nothing at a basis of 100, its
/// book then moved to a generated mark: at the first mark each side quotes, since the loss of the
/// restored basis is not this run's. At the next mark the reducing side quotes, and the side that
/// adds quotes unless that instrument's own mark-to-market loss reaches the budget. The properties
/// of the strategy's risk budget and of the exposure ceiling hold their ranges.
#[test]
fn some_side_is_always_quotable_and_the_reducing_side_is_never_withdrawn() {
    an_unrealised_loss_of_the_same_size_does_not_halt();
    ceiling_and_risk_budget_leave_a_side();

    let instruments =
        prop::collection::vec((-3i64..=3, 10i64..=200), 1..=usize::from(MAX_INSTRUMENTS));
    let outcome = runner(48).run(&(instruments, 1i64..=150), |(instruments, budget)| {
        let count = instruments.len() as u16;
        let rows: Vec<InstrumentRow> = (0..count).map(|id| row(id, USDT)).collect();
        let restored: Vec<InstrumentExposure> = (0u16..)
            .zip(&instruments)
            .filter(|(_, (units, _))| *units != 0)
            .map(|(id, (units, _))| InstrumentExposure {
                instrument: InstrumentId(id),
                position_base: Qty::from_mantissa(units * ONE),
                cash_quote: -100 * units * ONE,
                basis_quote: 100 * units.abs() * ONE,
            })
            .collect();
        let quoter = Box::new(EachMarketQuoter { instruments: count });
        let mut markets = markets(&rows, &restored, budget * ONE, quoter);
        make_ready(&mut markets.engine, 0);
        for id in 0..count {
            seat(&mut markets.engine, id, 100 * ONE, 1);
        }
        let (boot, _) = spin_outcome(&mut markets, 1, 10);
        let each_side: Vec<(u16, Side)> = (0..count)
            .flat_map(|id| [(id, Side::Buy), (id, Side::Sell)])
            .collect();
        prop_assert_eq!(
            &boot,
            &each_side,
            "the first mark is the baseline, so nothing is lost yet"
        );

        for (id, (_, mark)) in (0u16..).zip(&instruments) {
            seat(&mut markets.engine, id, mark * ONE, 20);
        }
        let (placed, is_halted) = spin_outcome(&mut markets, 2, 30);
        prop_assert!(!is_halted, "no loss was realised, so nothing may halt");
        for (id, (units, mark)) in (0u16..).zip(&instruments) {
            let open_loss = units * (mark - 100);
            for side in [Side::Buy, Side::Sell] {
                let is_reducing =
                    (*units > 0 && side == Side::Sell) || (*units < 0 && side == Side::Buy);
                let is_adding = *units != 0 && !is_reducing;
                let is_withdrawn = is_adding && open_loss <= -budget;
                prop_assert_eq!(
                    placed.contains(&(id, side)),
                    !is_withdrawn,
                    "instrument {} holding {} at {} against a budget of {}: {:?}",
                    id,
                    units,
                    mark,
                    budget,
                    placed
                );
            }
        }
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("a gate withdrew a side it must leave: {failure}");
    }
}

/// The strategy's risk budget and the engine's exposure ceiling, over the ranges of their old
/// properties: some side is always admitted, and the side that shrinks the position always is.
fn ceiling_and_risk_budget_leave_a_side() {
    let budgets = (-500i64..=500, 1i64..=50, 50i64..=200);
    let outcome = runner(256).run(&budgets, |(exposure_units, order_units, max_units)| {
        let budget = RiskBudget {
            order_notional: order_units * ONE,
            max_exposure_quote: max_units * ONE,
        };
        let exposure = exposure_units * ONE;
        prop_assert!(
            !budget.would_breach(exposure, Side::Buy) || !budget.would_breach(exposure, Side::Sell),
            "both sides withdrawn at exposure {} with order {} and ceiling {}",
            exposure,
            budget.order_notional,
            budget.max_exposure_quote
        );
        for side in [Side::Buy, Side::Sell] {
            let projected = exposure + signed(side, budget.order_notional);
            if projected.abs() < exposure.abs() {
                prop_assert!(
                    !budget.would_breach(exposure, side),
                    "{:?} shrinks {} yet is withdrawn",
                    side,
                    exposure
                );
            }
        }
        Ok(())
    });
    if let Err(failure) = outcome {
        panic!("the strategy's risk budget trapped a position: {failure}");
    }

    let ceilings = (
        -50i64..=50,
        1i64..=500,
        0i64..=500,
        1i64..=500,
        any::<bool>(),
    );
    let outcome = runner(256).run(
        &ceilings,
        |(position_units, mark_units, qty_hundredths, ceiling_units, has_mark)| {
            let position_base = Qty::from_mantissa(position_units * ONE);
            let mark = Price::from_mantissa(mark_units * ONE);
            let check = |side: Side| ExposureCheck {
                exposure_quote: if has_mark {
                    mark.notional(position_base)
                } else {
                    Notional::from_mantissa(0)
                },
                position_base,
                has_mark,
                side,
                price: mark,
                qty: Qty::from_mantissa(qty_hundredths * ONE / 100),
                ceiling_quote: Notional::from_mantissa(ceiling_units * ONE),
            };
            let (buy, sell) = (
                assess_exposure(check(Side::Buy)),
                assess_exposure(check(Side::Sell)),
            );
            prop_assert!(
                buy.admits(Side::Buy) || sell.admits(Side::Sell),
                "both sides withdrawn holding {}",
                position_units
            );
            let reducing = match position_units.signum() {
                1 => Side::Sell,
                -1 => Side::Buy,
                _ => return Ok(()),
            };
            prop_assert!(
                assess_exposure(check(reducing)).admits(reducing),
                "{:?} shrinks {} yet is withdrawn",
                reducing,
                position_units
            );
            Ok(())
        },
    );
    if let Err(failure) = outcome {
        panic!("the exposure ceiling trapped a position: {failure}");
    }
}

/// FITNESS: at the exposure ceiling the side that adds is refused and the side that reduces is not.
/// A position past the ceiling can only grow by the adding side, and the reducing side is the only
/// way back.
///
/// Long 10 at a mark of 10 under a ceiling of 100: the buy is refused and the sell admitted. Long 5:
/// of two buys that differ only in size, the one whose notional crosses the ceiling is refused. A
/// position with no mark is not flat. The named cases of the old verdict table, the declared
/// notional, and a position at the ceiling, filled or restored, hold their values.
#[test]
fn adding_side_is_refused_at_the_exposure_ceiling() {
    assess_exposure_ceiling_verdicts_match_named_cases();
    the_ceiling_projects_the_declared_notional_rather_than_a_fixed_step();
    a_position_at_the_ceiling_stops_the_engine_adding_whether_filled_or_restored();

    let at_ceiling = |side: Side, position: i64, qty: i64, has_mark: bool| {
        assess_exposure(ExposureCheck {
            exposure_quote: Notional::from_mantissa(if has_mark { 10 * position } else { 0 }),
            position_base: Qty::from_mantissa(position),
            has_mark,
            side,
            price: Price::from_mantissa(10 * ONE),
            qty: Qty::from_mantissa(qty),
            ceiling_quote: Notional::from_mantissa(100 * ONE),
        })
    };
    let refused = QuotePermission::ReducingOnly {
        reducing: Side::Sell,
    };
    assert_eq!(
        at_ceiling(Side::Buy, 10 * ONE, ONE, true),
        refused,
        "the buy adds past the ceiling"
    );
    assert_eq!(
        at_ceiling(Side::Sell, 10 * ONE, ONE, true),
        QuotePermission::Both,
        "the sell reduces"
    );
    assert_eq!(
        at_ceiling(Side::Buy, 5 * ONE, 4 * ONE, true),
        QuotePermission::Both,
        "90 of 100"
    );
    assert_eq!(
        at_ceiling(Side::Buy, 5 * ONE, 6 * ONE, true),
        refused,
        "110 of 100"
    );
    assert_eq!(
        at_ceiling(Side::Buy, 10 * ONE, ONE, false),
        refused,
        "no mark is not flat"
    );
}
