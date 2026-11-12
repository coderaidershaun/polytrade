//! FITNESS: the reconciler is where a strategy's wish becomes an order, so every property that
//! decides whether we quote, at what price and at what size is pinned here.
//!
//! It is a pure function, which is the whole reason these can be properties rather than scenarios:
//! no engine, no clock, no ring. The one that matters most is passive snapping, because a bid
//! snapped UP is not a wrong number on a chart, it is an order that crosses the spread and pays the
//! taker fee.
//!
//! The single-flight rule is NOT here. It is side-wide and the engine holds it, so it is pinned
//! where it lives — `exec_ladder`'s `ladder_serialises_and_requotes_only_after_terminal_confirmation`
//! drives a real engine and asserts no replacement goes out beside an unconfirmed cancel.
//!
//! The amend cases at the bottom reach across into the transition table, which nothing else here
//! does. They have to: a shrink CONVERGES only if the venue's answer lands the new size on the slot
//! that the next decision is taken against, so the property spans the fold and the decision and
//! belongs to neither alone.

use polysim::hot::exec::{
    BookTop, CloseReason, DesiredQuote, ExecLimits, OrderSlot, OrderState, PlaceIntent,
    ReconcileInput, ReconcileOutcome, RejectReason, RestingOrder, TickGrid, apply_exec_event,
    reconcile_side,
};
use polysim::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side, VenueOrderId};
use polysim::msg::exec::{
    ExecEvent, ExecKind, OrderStyle, Provenance, RejectClass, VenueOrderStatus,
};
use polysim::time::{DurationUs, TsUs};
use proptest::prelude::*;

use crate::engine_support::{rich, top};

/// A whole price unit in mantissas, matching the rest of the suite.
const ONE: i64 = 100_000_000;
const TICK: i64 = ONE / 100;
const STEP: i64 = ONE / 100_000;

fn grid() -> TickGrid {
    TickGrid {
        tick: TICK,
        step: STEP,
        min_qty: Qty(STEP),
        min_notional: 0,
        max_amends: 10,
        max_price: None,
    }
}

fn limits() -> ExecLimits {
    ExecLimits {
        requote_threshold_ticks: 1,
        // Wide enough that the band is not what any of these cases is testing; the band has its own.
        max_quote_distance_centi_bps: 100 * 100 * 100,
        max_book_age: DurationUs::from_secs(2),
        max_order_notional_quote: i64::MAX / 2,
    }
}

fn want(price: i64, qty: i64) -> Option<DesiredQuote> {
    Some(DesiredQuote {
        price: Price(price),
        qty: Qty(qty),
        style: OrderStyle::PostOnly,
    })
}

fn live(price: i64, qty: i64) -> Option<RestingOrder> {
    Some(RestingOrder {
        price: Price(price),
        qty: Qty(qty),
        filled: Qty(0),
        amends_used: 0,
    })
}

/// A buy order mid-amend, sized `qty` before the venue answers.
fn amending_slot(qty: i64) -> OrderSlot {
    OrderSlot {
        client_id: ClientOrderId(1),
        state: OrderState::AmendInFlight,
        side: Side::Buy,
        price: Price(passive(Side::Buy)),
        qty: Qty(qty),
        ..OrderSlot::EMPTY
    }
}

/// One venue answer about that order.
fn amend_answer(kind: ExecKind, qty: i64) -> ExecEvent {
    ExecEvent {
        instrument: InstrumentId(0),
        client_id: ClientOrderId(1),
        venue_order_id: Some(VenueOrderId(9)),
        trade_id: None,
        kind,
        status: Some(VenueOrderStatus::New),
        reject: None,
        provenance: Provenance::Mine,
        side: Side::Buy,
        liquidity: None,
        price: Price(passive(Side::Buy)),
        qty: Qty(qty),
        last_price: Price(0),
        last_qty: Qty(0),
        cumulative_qty: Qty(0),
        cumulative_quote: 0,
        commission: 0,
        commission_asset: AssetId::UNKNOWN,
        reject_code: 0,
        amends_remaining: ExecEvent::AMENDS_UNKNOWN,
        recon_seq: 0,
        exchange_ts_us: TsUs::from_micros(2_000),
        request_sent_ts_us: None,
        received_ts_us: TsUs::from_micros(2_000),
        queued_ts_us: TsUs::from_micros(2_000),
    }
}

/// The EDGE proving an amend never left the process — no venue heard of it, so it carries no status
/// and makes no claim about the amend budget.
fn amend_not_sent() -> ExecEvent {
    ExecEvent {
        status: None,
        ..amend_answer(ExecKind::AmendNotSent, 0)
    }
}

/// The venue REFUSING an amend, carrying whatever it says about what is left. `-2038` is the only
/// code that says anything, so this is the only shape a definitive zero ever arrives in.
fn amend_refused(amends_remaining: u8) -> ExecEvent {
    ExecEvent {
        reject: Some(RejectClass::StillLive),
        status: None,
        amends_remaining,
        ..amend_answer(ExecKind::AckFailed, 0)
    }
}

/// The projection the engine takes of a slot before deciding — `engine::resting_of`, which is
/// crate-internal, so the shape is restated rather than reached for.
fn resting_of(slot: &OrderSlot) -> Option<RestingOrder> {
    Some(RestingOrder {
        price: slot.price,
        qty: slot.qty,
        filled: slot.filled_base,
        amends_used: slot.amends_used,
    })
}

fn input(
    side: Side,
    desired: Option<DesiredQuote>,
    resting: Option<RestingOrder>,
) -> ReconcileInput {
    ReconcileInput {
        side,
        desired,
        resting,
        grid: grid(),
        top: top(100 * ONE, 100 * ONE + TICK),
        limits: limits(),
        funds: rich(),
    }
}

/// The order the venue would be holding after `outcome` is applied. `None` means nothing rests.
fn apply(resting: Option<RestingOrder>, outcome: ReconcileOutcome) -> Option<RestingOrder> {
    match outcome {
        ReconcileOutcome::Nothing => resting,
        ReconcileOutcome::Cancel => None,
        ReconcileOutcome::Place(intent) => Some(RestingOrder {
            price: intent.price,
            qty: intent.qty,
            filled: Qty(0),
            amends_used: 0,
        }),
        ReconcileOutcome::AmendQty(qty) => resting.map(|order| RestingOrder {
            qty,
            amends_used: order.amends_used + 1,
            ..order
        }),
        // A refusal changes nothing at the venue — that is what makes it safe to leave the resting
        // order standing.
        ReconcileOutcome::Reject(_) => resting,
    }
}

fn placed(outcome: ReconcileOutcome) -> Option<PlaceIntent> {
    match outcome {
        ReconcileOutcome::Place(intent) => Some(intent),
        _ => None,
    }
}

/// Whether this outcome puts a request on the wire. `Nothing` and every `Reject` do not, which is
/// what makes them the fixed points convergence is measured against.
fn emits_command(outcome: ReconcileOutcome) -> bool {
    !matches!(
        outcome,
        ReconcileOutcome::Nothing | ReconcileOutcome::Reject(_)
    )
}

/// A price that rests passively on `side` against the shared fixture book, so a case exercises the
/// path under test rather than being refused by the cross guard first.
fn passive(side: Side) -> i64 {
    match side {
        Side::Buy => 100 * ONE - TICK,
        Side::Sell => 100 * ONE + 2 * TICK,
    }
}

proptest! {
    /// FITNESS: no emitted order ever crosses. A buy at or above the best ask, or a sell at or below
    /// the best bid, is a taker order the strategy did not ask for and the fee tier cannot afford.
    #[test]
    fn an_emitted_order_never_crosses(
        want_ticks in -50i64..=50,
        qty_steps in 1i64..=1_000,
        side_is_buy in any::<bool>(),
    ) {
        let side = if side_is_buy { Side::Buy } else { Side::Sell };
        let outcome = reconcile_side(input(
            side,
            want(100 * ONE + want_ticks * TICK, qty_steps * STEP),
            None,
        ));
        if let Some(intent) = placed(outcome) {
            match side {
                Side::Buy => prop_assert!(
                    intent.price < Price(100 * ONE + TICK),
                    "emitted a buy at {:?} against a best ask of {:?}",
                    intent.price,
                    100 * ONE + TICK
                ),
                Side::Sell => prop_assert!(
                    intent.price > Price(100 * ONE),
                    "emitted a sell at {:?} against a best bid of {:?}",
                    intent.price,
                    100 * ONE
                ),
            }
        }
    }

    /// FITNESS: snapping is PASSIVE and size only ever rounds DOWN. A bid lands at or below what was
    /// asked for, an ask at or above it, both on the grid, and the size never exceeds the declaration.
    #[test]
    fn snapping_is_passive_and_size_only_shrinks(
        want_offset in -50_000i64..=50_000,
        qty in 1i64..=100 * 100_000,
        side_is_buy in any::<bool>(),
    ) {
        let side = if side_is_buy { Side::Buy } else { Side::Sell };
        let wanted_price = 100 * ONE + want_offset;
        let outcome = reconcile_side(ReconcileInput {
            // A band wide enough that only the snap is under test here.
            limits: ExecLimits { max_quote_distance_centi_bps: i64::MAX / 4, ..limits() },
            // A book far enough away that the cross guard never fires and every case reaches the snap.
            top: top(1, 1_000 * ONE),
            ..input(side, want(wanted_price, qty), None)
        });
        let Some(intent) = placed(outcome) else { return Ok(()); };
        prop_assert_eq!(intent.price.0 % TICK, 0, "price {:?} is off the tick grid", intent.price);
        prop_assert_eq!(intent.qty.0 % STEP, 0, "qty {:?} is off the step grid", intent.qty);
        prop_assert!(intent.qty <= Qty(qty), "size grew from {} to {:?}", qty, intent.qty);
        match side {
            Side::Buy => prop_assert!(
                intent.price <= Price(wanted_price),
                "a bid snapped UP, from {} to {:?}",
                wanted_price,
                intent.price
            ),
            Side::Sell => prop_assert!(
                intent.price >= Price(wanted_price),
                "an ask snapped DOWN, from {} to {:?}",
                wanted_price,
                intent.price
            ),
        }
    }

    /// FITNESS: the reconciler converges, and in at most two commands. This is the property that
    /// kills churn, oscillation and every "places twice" bug at once — an engine that keeps issuing
    /// commands against an unchanged declaration burns rate limit and queue priority forever.
    ///
    /// A refusal is a fixed point, not a failure to converge: it puts nothing on the wire and leaves
    /// the venue exactly as it was, so re-deciding must reach the same answer. Two commands rather
    /// than one because withdrawing an order that has filled past what is still wanted legitimately
    /// takes a cancel and then a place.
    #[test]
    fn the_reconciler_settles_within_two_commands(
        want_ticks in -20i64..=20,
        qty_steps in 1i64..=500,
        resting_ticks in -20i64..=20,
        resting_steps in 1i64..=500,
        filled_steps in 0i64..=500,
        side_is_buy in any::<bool>(),
    ) {
        let side = if side_is_buy { Side::Buy } else { Side::Sell };
        let desired = want(passive(side) + want_ticks * TICK, qty_steps * STEP);
        let mut resting = live(passive(side) + resting_ticks * TICK, resting_steps * STEP)
            .map(|order| RestingOrder {
                filled: Qty(filled_steps.min(resting_steps - 1) * STEP),
                ..order
            });
        let mut issued = Vec::new();
        loop {
            let outcome = reconcile_side(input(side, desired, resting));
            if !emits_command(outcome) {
                // The fixed point must be stable: deciding again over the same state repeats it.
                prop_assert_eq!(
                    reconcile_side(input(side, desired, resting)),
                    outcome,
                    "the settled answer is not stable"
                );
                break;
            }
            issued.push(outcome);
            prop_assert!(
                issued.len() <= 2,
                "still issuing commands after {:?} — the reconciler is oscillating",
                issued
            );
            resting = apply(resting, outcome);
        }
    }

    /// FITNESS: inside the hysteresis band the price never moves. Requoting a tick that is within
    /// the threshold is pure churn: it burns rate limit and surrenders queue priority for a price
    /// the strategy considers equivalent.
    #[test]
    fn a_price_inside_the_hysteresis_band_never_requotes(
        drift_ticks in -1i64..=1,
        qty_steps in 1i64..=500,
        side_is_buy in any::<bool>(),
    ) {
        let side = if side_is_buy { Side::Buy } else { Side::Sell };
        let resting_price = passive(side) + drift_ticks * TICK;
        let outcome = reconcile_side(input(
            side,
            want(passive(side), qty_steps * STEP),
            live(resting_price, qty_steps * STEP),
        ));
        if let Some(intent) = placed(outcome) {
            prop_assert_eq!(
                intent.price,
                Price(resting_price),
                "requoted from {} to {:?} for a {}-tick move inside the band",
                resting_price,
                intent.price,
                drift_ticks
            );
        }
    }

    /// FITNESS: a shrink past the amend budget cancels first. The latest desired replacement may
    /// only be placed by a later decision after terminal confirmation.
    #[test]
    fn a_shrink_past_the_amend_budget_cancels_first(
        amends_used in 0u8..=20,
        side_is_buy in any::<bool>(),
    ) {
        let side = if side_is_buy { Side::Buy } else { Side::Sell };
        let resting = Some(RestingOrder {
            amends_used,
            ..live(passive(side), 10 * STEP).expect("built")
        });
        let outcome = reconcile_side(input(side, want(passive(side), 5 * STEP), resting));
        if amends_used >= grid().max_amends {
            prop_assert!(
                outcome == ReconcileOutcome::Cancel,
                "budget exhausted at {} amends, yet the outcome was {:?}",
                amends_used,
                outcome
            );
        } else {
            prop_assert_eq!(
                outcome,
                ReconcileOutcome::AmendQty(Qty(5 * STEP)),
                "an amend was available at {} used, yet the outcome was {:?}",
                amends_used,
                outcome
            );
        }
    }
}

/// FITNESS: a quote at or beyond the touch is REFUSED, and the order already resting is left alone.
/// Cancelling into the moment we cannot price is churn; the stale passive order is not the risk.
#[test]
fn a_crossing_quote_is_refused_and_leaves_the_resting_order_alone() {
    let resting = live(100 * ONE - TICK, ONE);
    let at_the_ask = reconcile_side(input(Side::Buy, want(100 * ONE + TICK, ONE), resting));
    assert_eq!(
        at_the_ask,
        ReconcileOutcome::Reject(RejectReason::WouldCross),
        "a bid AT the best ask must be refused"
    );
    let through_the_ask =
        reconcile_side(input(Side::Buy, want(100 * ONE + 5 * TICK, ONE), resting));
    assert_eq!(
        through_the_ask,
        ReconcileOutcome::Reject(RejectReason::WouldCross),
        "a bid THROUGH the best ask must be refused"
    );
    let sell_at_the_bid = reconcile_side(input(Side::Sell, want(100 * ONE, ONE), resting));
    assert_eq!(
        sell_at_the_bid,
        ReconcileOutcome::Reject(RejectReason::WouldCross),
        "an ask AT the best bid must be refused"
    );
}

/// FITNESS: a book the engine cannot price withdraws the quote rather than leaving it resting.
/// Being blind is exactly when a stale quote gets picked off.
#[test]
fn an_unusable_book_withdraws_the_resting_quote() {
    let resting = live(100 * ONE - TICK, ONE);
    let desired = want(100 * ONE - TICK, ONE);

    let one_sided = ReconcileInput {
        top: BookTop {
            best_ask: None,
            ..top(100 * ONE, 100 * ONE + TICK)
        },
        ..input(Side::Buy, desired, resting)
    };
    assert_eq!(reconcile_side(one_sided), ReconcileOutcome::Cancel);

    let stale = ReconcileInput {
        top: BookTop {
            now_ts_us: TsUs::from_micros(1_000 + DurationUs::from_secs(3).micros()),
            ..top(100 * ONE, 100 * ONE + TICK)
        },
        ..input(Side::Buy, desired, resting)
    };
    assert_eq!(reconcile_side(stale), ReconcileOutcome::Cancel);

    let invalid = ReconcileInput {
        top: BookTop {
            is_valid: false,
            ..top(100 * ONE, 100 * ONE + TICK)
        },
        ..input(Side::Buy, desired, resting)
    };
    assert_eq!(reconcile_side(invalid), ReconcileOutcome::Cancel);

    // With nothing resting there is nothing to withdraw, and no command is the right answer.
    let nothing_resting = ReconcileInput {
        top: BookTop {
            is_valid: false,
            ..top(100 * ONE, 100 * ONE + TICK)
        },
        ..input(Side::Buy, desired, None)
    };
    assert_eq!(reconcile_side(nothing_resting), ReconcileOutcome::Nothing);
}

/// FITNESS: a side not re-declared this spin is withdrawn. That is what stops a strategy wedged
/// mid-logic from leaving the engine quoting on its behalf forever.
#[test]
fn an_undeclared_side_is_withdrawn() {
    assert_eq!(
        reconcile_side(input(Side::Buy, None, live(100 * ONE - TICK, ONE))),
        ReconcileOutcome::Cancel
    );
    assert_eq!(
        reconcile_side(input(Side::Buy, None, None)),
        ReconcileOutcome::Nothing
    );
}

/// FITNESS: an acknowledged amend lands the venue's new size on the slot so the shrink is decided
/// ONCE, and one amend COMMAND spends exactly one amend — however its answers arrive.
///
/// A slot still holding the pre-amend size decides the same shrink again on the next spin, and again
/// after that — spending the order's amend budget and eventually forcing a cancel. And the venue
/// publishes no per-order amend figure, so the local count is the only thing standing between the
/// engine and a rejected eleventh amend. Both properties are driven over every delivery pattern
/// because a single amend is answered TWICE — by the request's own ack and by an `executionReport` on
/// the account stream — and either may arrive first, may duplicate, or the other may go missing.
#[test]
fn an_amend_answer_lands_the_size_once_and_spends_exactly_one_amend_per_command() {
    let deliveries: [&[ExecKind]; 5] = [
        &[ExecKind::AckAmended],
        &[ExecKind::AckAmended, ExecKind::ReportAmended],
        &[ExecKind::ReportAmended, ExecKind::AckAmended],
        &[ExecKind::AckAmended, ExecKind::AckAmended],
        &[
            ExecKind::ReportAmended,
            ExecKind::AckAmended,
            ExecKind::ReportAmended,
        ],
    ];
    for answers in deliveries {
        let mut slot = amending_slot(10 * STEP);
        for kind in answers {
            apply_exec_event(&mut slot, &amend_answer(*kind, 4 * STEP));
        }
        assert_eq!(
            slot.qty,
            Qty(4 * STEP),
            "the slot did not settle on the venue's size after {answers:?}"
        );
        assert_eq!(
            slot.amends_used, 1,
            "one amend command spent {} amends against {answers:?}",
            slot.amends_used
        );
        let desired = want(passive(Side::Buy), 4 * STEP);
        assert_eq!(
            reconcile_side(input(Side::Buy, desired, resting_of(&slot))),
            ReconcileOutcome::Nothing,
            "the shrink was decided a second time after {answers:?}"
        );

        // The SECOND command counts too — a slot re-entering `AmendInFlight` is the engine sending
        // another amend, which is exactly what the budget is counting.
        slot.state = OrderState::AmendInFlight;
        for kind in answers {
            apply_exec_event(&mut slot, &amend_answer(*kind, 3 * STEP));
        }
        assert_eq!(
            slot.amends_used, 2,
            "a second amend command left the count at {} against {answers:?}",
            slot.amends_used
        );
    }
}

/// FITNESS: the venue's own statement retires the amend primitive whatever the local count says,
/// and an event that makes no claim leaves it alone. The local count is an ESTIMATE — an order
/// adopted mid-life counts from zero against a history nobody saw — so `-2038` is the only
/// authoritative word on the subject and has to overrule it. Conflating "no claim" with "none left"
/// runs the opposite way and retires the primitive on the first event that mentions nothing.
#[test]
fn a_definitive_zero_retires_the_amend_and_an_unknown_leaves_it_alone() {
    let shrink = want(passive(Side::Buy), 4 * STEP);

    let mut spent = OrderSlot {
        state: OrderState::Live,
        ..amending_slot(10 * STEP)
    };
    apply_exec_event(&mut spent, &amend_refused(0));
    let outcome = reconcile_side(input(Side::Buy, shrink, resting_of(&spent)));
    assert_eq!(
        outcome,
        ReconcileOutcome::Cancel,
        "the venue said this order gets no more amends, yet the engine answered {outcome:?}"
    );

    let mut unclaimed = OrderSlot {
        state: OrderState::Live,
        ..amending_slot(10 * STEP)
    };
    apply_exec_event(&mut unclaimed, &amend_refused(ExecEvent::AMENDS_UNKNOWN));
    assert_eq!(
        reconcile_side(input(Side::Buy, shrink, resting_of(&unclaimed))),
        ReconcileOutcome::AmendQty(Qty(4 * STEP)),
        "an event claiming nothing about the budget spent it anyway"
    );
}

/// FITNESS: a late answer never restores a size the venue has already left. `keepPriority` only
/// ever reduces, so the newest amend is the smallest one seen and the fold must be independent of
/// delivery order. Restoring the larger size has the engine amend to a quantity the order
/// already holds, which the venue refuses outright.
#[test]
fn a_late_amend_answer_never_restores_a_larger_size() {
    let mut slot = amending_slot(10 * STEP);
    apply_exec_event(&mut slot, &amend_answer(ExecKind::AckAmended, 6 * STEP));
    apply_exec_event(&mut slot, &amend_answer(ExecKind::AckAmended, 3 * STEP));
    apply_exec_event(&mut slot, &amend_answer(ExecKind::ReportAmended, 6 * STEP));
    assert_eq!(
        slot.qty,
        Qty(3 * STEP),
        "a stale amend answer restored a size the venue has left"
    );
}

/// FITNESS: an amend that never reached the venue releases the slot on the spot, and the shrink it
/// was going to make is decided again.
///
/// The engine moves a slot to `AmendInFlight` the moment it BANKS the command, before any of it has
/// left the process, and a side holding an unanswered order decides nothing further. So an amend the
/// edge then refuses — no socket to send it on, a request that would not build, a phase that closed
/// underneath it — leaves the strategy quoting around a size change that is never going to happen,
/// with the order resting at its original size, until the in-flight timeout fires seconds later.
///
/// Both halves belong to one property: releasing the slot is worth nothing if the released size is
/// wrong, because the next decision is taken against exactly that size.
#[test]
fn an_amend_that_never_left_releases_the_slot_and_the_shrink_is_decided_again() {
    let mut slot = amending_slot(10 * STEP);
    apply_exec_event(&mut slot, &amend_not_sent());
    assert_eq!(
        slot.state,
        OrderState::Live,
        "the slot is still waiting on an amend the venue was never asked for"
    );
    assert_eq!(
        slot.qty,
        Qty(10 * STEP),
        "an amend that never left moved the resting size anyway"
    );
    assert_eq!(
        slot.amends_used, 0,
        "an amend that never left spent one of the order's amends"
    );

    let shrink = want(passive(Side::Buy), 4 * STEP);
    assert_eq!(
        reconcile_side(input(Side::Buy, shrink, resting_of(&slot))),
        ReconcileOutcome::AmendQty(Qty(4 * STEP)),
        "the shrink was abandoned with the order still resting at its original size"
    );

    // An amend not leaving says nothing about whether the order survived, so a terminal answer that
    // raced it keeps the last word.
    let mut retired = amending_slot(10 * STEP);
    apply_exec_event(
        &mut retired,
        &amend_answer(ExecKind::ReportCanceled, 10 * STEP),
    );
    apply_exec_event(&mut retired, &amend_not_sent());
    assert_eq!(
        retired.state,
        OrderState::Closed(CloseReason::Canceled),
        "a refused amend resurrected an order the venue had already retired"
    );
}
