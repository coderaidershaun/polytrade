//! The order table answers each venue event no further than the venue confirmed it, names each
//! order by an id that addresses one slot and one generation only, and retires an order only when a
//! pass that postdates it did not name it.

use std::collections::HashSet;

use engine::hot::dispatch::HotEngine;
use engine::hot::exec::{
    AccountTable, ClaimRefusal, ClientIdLayout, CloseReason, DesiredQuote, ExecLimits, HaltReason,
    MAX_ORDER_SLOTS, OrderClaim, OrderSlot, OrderState, OrderTable, QuoteLevel, ReconcileInput,
    ReconcileOutcome, ReconcilePass, RejectReason, ReleaseOutcome, RestingOrder, TickGrid,
    reconcile_side, side_base,
};
use engine::msg::exec::{
    AccountChunk, AccountChunkKind, ExecCommand, ExecEvent, ExecKind, OrderStyle, RejectClass,
    VenueOrderStatus,
};
use engine::msg::ids::{AssetId, ClientOrderId, InstrumentId, Notional, Price, Qty, Side, Spend};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::time::{DurationUs, LocalTs, VenueTs};
use engine::msg::ui::{UiEvent, UiHalt};
use engine::runtime::config::{RecordedTables, TableKind};
use proptest::prelude::*;
use rtrb::Consumer;

use crate::engine_support::{
    ONE, account_chunk, exec_capacity, exec_event, local_refusals, rich, top, ts,
};
use crate::exec_support::{
    ASK, BID, CEILING, INSTRUMENT, QuotingEngine, QuotingSetup, TwoSidedQuoter,
    built_quoting_engine, drain_commands, exec_settings, make_ready, reseat_book, row_with_ceiling,
    spin_at,
};

const RUN_NONCE: u32 = 0x5EED_1234;
const TICK: i64 = 1_000_000;

/// The receipt stamp of every venue answer built here.
const RECEIVED: LocalTs = LocalTs::from_micros(2);

fn table(instruments: u16) -> OrderTable {
    OrderTable::new(&exec_capacity(instruments), RUN_NONCE)
}

fn buy_claim(level: QuoteLevel, price: i64, qty: i64) -> OrderClaim {
    OrderClaim {
        instrument: InstrumentId(0),
        side: Side::Buy,
        level,
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(qty),
        style: OrderStyle::PostOnly,
        claimed_ts_us: LocalTs::from_micros(1),
        recon_seq: 0,
    }
}

fn level(index: u8) -> QuoteLevel {
    QuoteLevel::new(index).expect("a quote level of the ladder")
}

fn event(kind: ExecKind, client_id: ClientOrderId) -> ExecEvent {
    ExecEvent {
        kind,
        qty: Qty::from_mantissa(10),
        ..exec_event(InstrumentId(0), client_id, Side::Buy, 100 * TICK, 1)
    }
}

fn trade(client_id: ClientOrderId, cumulative: i64) -> ExecEvent {
    ExecEvent {
        cumulative_qty: Qty::from_mantissa(cumulative),
        cumulative_quote: Notional::from_mantissa(cumulative * 100),
        ..event(ExecKind::ReportTrade, client_id)
    }
}

fn rejected(client_id: ClientOrderId, class: Option<RejectClass>) -> ExecEvent {
    ExecEvent {
        reject: class,
        ..event(ExecKind::AckFailed, client_id)
    }
}

/// A claimed order of `qty` on level `at`, moved to `state` by the transitions that the engine
/// itself uses: an acknowledgement, a banked cancel or amend, a timeout, or a cancel's answer.
fn order_in(table: &mut OrderTable, at: QuoteLevel, qty: i64, state: OrderState) -> usize {
    let (index, client_id) = table
        .claim(buy_claim(at, 100 * TICK, qty))
        .expect("a fresh level has a free slot");
    let placed = event(ExecKind::AckPlaced, client_id);
    match state {
        OrderState::PendingNew => {}
        OrderState::Live => {
            table.apply(index, &placed, RECEIVED);
        }
        OrderState::CancelInFlight => {
            table.apply(index, &placed, RECEIVED);
            table.mark_cancel_sent(index);
        }
        OrderState::AmendInFlight => {
            table.apply(index, &placed, RECEIVED);
            table.mark_amend_sent(index);
        }
        OrderState::Unknown => {
            table.mark_timed_out(index, RECEIVED);
        }
        OrderState::Closed(CloseReason::Canceled) => {
            table.apply(index, &placed, RECEIVED);
            table.apply(index, &event(ExecKind::AckCanceled, client_id), RECEIVED);
        }
        other => panic!("no transition of the engine reaches {other:?} from a claim"),
    }
    assert_eq!(table.slot(index).state(), state);
    index
}

/// FITNESS: a refused request moves its order by its reject class, and by nothing else. Each class
/// meets each state an order can be in when the answer lands, and the table below is the whole
/// answer. A refusal read as more certain than its class drops an order that can still fill, or
/// places a second one beside it, and both look like a working system.
#[test]
fn transition_has_one_arm_for_each_reject_class() {
    use OrderState::{AmendInFlight, CancelInFlight, Closed, Live, PendingNew, Unknown};
    let canceled = Closed(CloseReason::Canceled);
    let rejected_close = Closed(CloseReason::Rejected);
    let gone = Closed(CloseReason::ReconciledGone);
    let classes = [
        None,
        Some(RejectClass::StillLive),
        Some(RejectClass::Refused),
        Some(RejectClass::Gone),
        Some(RejectClass::Ambiguous),
        Some(RejectClass::Fatal),
    ];
    // One row for each state, one column for each class in the order of `classes`.
    let table_of_answers: [(OrderState, [OrderState; 6]); 6] = [
        (
            PendingNew,
            [
                Unknown,
                rejected_close,
                rejected_close,
                gone,
                Unknown,
                Unknown,
            ],
        ),
        (
            Live,
            [Unknown, Live, rejected_close, gone, Unknown, Unknown],
        ),
        (
            CancelInFlight,
            [Unknown, Live, rejected_close, gone, Unknown, Unknown],
        ),
        (
            AmendInFlight,
            [Unknown, Live, rejected_close, gone, Unknown, Unknown],
        ),
        (
            Unknown,
            [Unknown, Unknown, rejected_close, gone, Unknown, Unknown],
        ),
        (
            canceled,
            [canceled, canceled, canceled, canceled, canceled, canceled],
        ),
    ];
    for (start, answers) in table_of_answers {
        for (class, expected) in classes.into_iter().zip(answers) {
            let mut orders = table(1);
            let index = order_in(&mut orders, QuoteLevel::ZERO, 10, start);
            let client_id = orders.slot(index).client_id();
            let applied = orders.apply(index, &rejected(client_id, class), RECEIVED);
            assert_eq!(
                (applied.state, orders.slot(index).state()),
                (expected, expected),
                "a refusal of class {class:?} met an order in {start:?}"
            );
        }
    }
}

/// FITNESS: an answer is never read as more certain than the venue gave it. An ambiguous refusal, a
/// fill that races a cancel, a late fill on a closed order, a probe that answers "pending cancel",
/// the count of what a side may still hold, a reservation that waits for its proof, and each answer
/// to an amend: each one keeps the order working, or the money held, until a terminal fact says
/// otherwise.
///
/// Binance's -2011 on a cancel means "unknown order", which a fully filled order also returns, so
/// closing on it loses the fill. A fill mid-cancel that resolved the cancel would let the next spin
/// fire a second one. An amend answered twice, by its ack and by the stream, that spent two amends
/// or landed its size twice would cancel the ladder early.
#[test]
fn late_answers_never_resolve_past_the_venue() {
    ambiguous_and_late_fills();
    pending_cancel_probe();
    side_bookkeeping();
    reservation_until_proof();
    amend_answers();
}

fn ambiguous_and_late_fills() {
    let mut orders = table(1);
    let index = order_in(
        &mut orders,
        QuoteLevel::ZERO,
        10,
        OrderState::CancelInFlight,
    );
    let id = orders.slot(index).client_id();
    let applied = orders.apply(index, &rejected(id, Some(RejectClass::Ambiguous)), RECEIVED);
    assert_eq!(
        applied.state,
        OrderState::Unknown,
        "an ambiguous cancel rejection was read as a definite answer"
    );
    assert!(
        applied.state.is_in_flight(),
        "Unknown must block the side, or the reconciler places a second order beside one that may \
         still be resting"
    );

    let mut orders = table(1);
    let index = order_in(
        &mut orders,
        QuoteLevel::ZERO,
        10,
        OrderState::CancelInFlight,
    );
    let id = orders.slot(index).client_id();
    let applied = orders.apply(index, &trade(id, 4), RECEIVED);
    assert_eq!(
        applied.state,
        OrderState::CancelInFlight,
        "a fill resolved an outstanding cancel the venue has not yet answered"
    );
    assert_eq!(
        applied.fill.base,
        Qty::from_mantissa(4),
        "the partial fill was still applied"
    );

    let mut orders = table(1);
    let index = order_in(&mut orders, QuoteLevel::ZERO, 10, OrderState::Live);
    let id = orders.slot(index).client_id();
    let filled = ExecEvent {
        status: Some(VenueOrderStatus::Filled),
        ..trade(id, 6)
    };
    orders.apply(index, &filled, RECEIVED);
    assert_eq!(
        (
            orders.slot(index).state(),
            orders.slot(index).filled_base(),
            orders.slot(index).filled_quote()
        ),
        (
            OrderState::Closed(CloseReason::Filled),
            Qty::from_mantissa(6),
            Notional::from_mantissa(600)
        )
    );
    let late = ExecEvent {
        cumulative_quote: Notional::from_mantissa(1_000),
        ..trade(id, 10)
    };
    let applied = orders.apply(index, &late, RECEIVED);
    assert_eq!(
        applied.fill.base,
        Qty::from_mantissa(4),
        "a late fill on a closed order was lost"
    );
    assert_eq!(
        applied.state,
        OrderState::Closed(CloseReason::Filled),
        "the first terminal answer is the true one"
    );
}

/// `PendingCancel` is a venue saying it does not yet know. Reading the probe's answer as the
/// cancel's own frees the side beside an order that can still fill.
fn pending_cancel_probe() {
    let mut orders = table(1);
    let index = order_in(
        &mut orders,
        QuoteLevel::ZERO,
        10,
        OrderState::CancelInFlight,
    );
    let id = orders.slot(index).client_id();

    let probe = ExecEvent {
        status: Some(VenueOrderStatus::PendingCancel),
        recon_seq: 1,
        ..event(ExecKind::SnapshotOrder, id)
    };
    let applied = orders.apply(index, &probe, RECEIVED);
    assert_eq!(
        applied.state,
        OrderState::CancelInFlight,
        "a status probe answering \"pending cancel\" was read as the cancel's own answer"
    );
    assert!(
        orders.is_awaiting_answer(InstrumentId(0), Side::Buy),
        "the side reopened with its cancel still unanswered, so the ladder may quote beside an \
         order that can still fill"
    );

    let applied = orders.apply(index, &trade(id, 4), RECEIVED);
    assert_eq!(
        applied.fill.base,
        Qty::from_mantissa(4),
        "a fill arriving after the probe was lost"
    );
    assert_eq!(
        applied.state,
        OrderState::CancelInFlight,
        "a fill is no more an answer to the cancel than the probe was"
    );

    let restated = ExecEvent {
        status: Some(VenueOrderStatus::PendingCancel),
        recon_seq: 2,
        cumulative_qty: Qty::from_mantissa(4),
        cumulative_quote: Notional::from_mantissa(400),
        ..event(ExecKind::SnapshotOrder, id)
    };
    assert_eq!(
        orders.apply(index, &restated, RECEIVED).fill.base,
        Qty::from_mantissa(0),
        "a probe restating the venue's running totals paid the same fill a second time"
    );

    let canceled = ExecEvent {
        cumulative_qty: Qty::from_mantissa(4),
        cumulative_quote: Notional::from_mantissa(400),
        ..event(ExecKind::ReportCanceled, id)
    };
    let applied = orders.apply(index, &canceled, RECEIVED);
    assert_eq!(
        applied.state,
        OrderState::Closed(CloseReason::Canceled),
        "the terminal report is the answer the slot was waiting for"
    );
    assert!(
        !orders.is_awaiting_answer(InstrumentId(0), Side::Buy),
        "the side is still shut after finality arrived, so it can never quote again"
    );
    assert_eq!(
        orders.possibly_live_count(InstrumentId(0), Side::Buy),
        0,
        "a proven-cancelled order still counts against the side's capacity"
    );
    assert_eq!(
        orders.slot(index).filled_base(),
        Qty::from_mantissa(4),
        "the fill moved money more than once across the probe, the restatement and the cancel"
    );
}

/// "Is anything on this side awaiting an answer" and "how many orders on this side might still
/// exist" both stay conservative until a terminal fact proves otherwise.
fn side_bookkeeping() {
    let mut awaiting = table(1);
    let live = order_in(&mut awaiting, QuoteLevel::ZERO, 10, OrderState::Live);
    let cancelling = order_in(&mut awaiting, level(1), 10, OrderState::CancelInFlight);
    assert_eq!(
        awaiting
            .resting(InstrumentId(0), Side::Buy, QuoteLevel::ZERO)
            .map(OrderSlot::state),
        Some(OrderState::Live),
        "level zero remains independently addressable"
    );
    assert_eq!(awaiting.slot(live).state(), OrderState::Live);
    assert!(
        awaiting.is_awaiting_answer(InstrumentId(0), Side::Buy),
        "a cancel is outstanding on this side and nothing else may fire until it is answered"
    );
    let id = awaiting.slot(cancelling).client_id();
    awaiting.apply(cancelling, &event(ExecKind::AckCanceled, id), RECEIVED);
    assert!(
        !awaiting.is_awaiting_answer(InstrumentId(0), Side::Buy),
        "both legs are resolved, so the side may quote again"
    );

    let mut count = table(1);
    let claimed: Vec<usize> = (0..3)
        .map(|at| order_in(&mut count, level(at), 10, OrderState::PendingNew))
        .collect();
    assert_eq!(count.possibly_live_count(InstrumentId(0), Side::Buy), 3);
    for index in &claimed[..2] {
        let id = count.slot(*index).client_id();
        count.apply(*index, &event(ExecKind::AckPlaced, id), RECEIVED);
    }
    count.mark_cancel_sent(claimed[0]);
    count.mark_amend_sent(claimed[1]);
    count.mark_timed_out(claimed[2], RECEIVED);
    assert_eq!(
        count.possibly_live_count(InstrumentId(0), Side::Buy),
        3,
        "uncertainty released capacity before non-existence was proved"
    );
    let id = count.slot(claimed[0]).client_id();
    count.apply(claimed[0], &event(ExecKind::AckCanceled, id), RECEIVED);
    assert_eq!(count.possibly_live_count(InstrumentId(0), Side::Buy), 2);
}

fn balances(
    asset: AssetId,
    free: i64,
    venue_update_ts_ms: u64,
    kind: AccountChunkKind,
) -> AccountChunk {
    let mut chunk = account_chunk(&[(asset, free)], 0);
    chunk.kind = kind;
    chunk.venue_update_ts_ms = venue_update_ts_ms;
    chunk.exchange_ts_us = VenueTs::from_micros(1);
    chunk
}

/// A reservation releases on terminal proof only: a later balance stamp after the ack, or a proof
/// that the request never reached the venue.
fn reservation_until_proof() {
    let quote = AssetId(1);
    let mut account = AccountTable::new(&exec_capacity(1));
    account.apply(&balances(quote, 1_000, 1, AccountChunkKind::Snapshot));

    let four_hundred = Spend::Quote(Notional::from_mantissa(400));
    let taken_at = account.reserve(quote, four_hundred);
    assert_eq!(
        account.balance(quote).spendable(),
        600,
        "the reservation must shrink what the next order may spend"
    );
    assert_eq!(
        account.release(quote, four_hundred, taken_at),
        ReleaseOutcome::Held,
        "released on the ack alone — the same balance can now fund a second order"
    );
    account.apply(&balances(quote, 600, 2, AccountChunkKind::Update));
    assert_eq!(
        account.release(quote, four_hundred, taken_at),
        ReleaseOutcome::Released
    );
    assert_eq!(account.balance(quote).spendable(), 600);

    let mut unsent_account = AccountTable::new(&exec_capacity(1));
    unsent_account.apply(&balances(quote, 1_000, 1, AccountChunkKind::Snapshot));
    unsent_account.reserve(quote, four_hundred);
    let mut orders = table(1);
    let index = order_in(&mut orders, QuoteLevel::ZERO, 10, OrderState::PendingNew);
    let id = orders.slot(index).client_id();
    assert_eq!(
        orders
            .apply(index, &event(ExecKind::PlaceNotSent, id), RECEIVED)
            .state,
        OrderState::Closed(CloseReason::Rejected)
    );
    unsent_account.release_unsent(quote, four_hundred);
    assert_eq!(
        unsent_account.balance(quote).spendable(),
        1_000,
        "a request that never left the process stranded its reservation"
    );
}

/// The amend grid of the reconciler: a whole price unit, a tick of a hundredth, a lot of 1e-5.
const AMEND_TICK: i64 = ONE / 100;
const STEP: i64 = ONE / 100_000;

/// A buy price that rests passively against the book of the reconcile input below.
const PASSIVE_BUY: i64 = 100 * ONE - AMEND_TICK;

fn reconcile_input(desired: Option<DesiredQuote>, resting: RestingOrder) -> ReconcileInput {
    ReconcileInput {
        side: Side::Buy,
        desired,
        resting: Some(resting),
        grid: TickGrid {
            tick: AMEND_TICK,
            step: STEP,
            min_qty: Qty::from_mantissa(STEP),
            min_notional: Notional::from_mantissa(0),
            max_amends: 10,
            max_price: None,
        },
        top: top(100 * ONE, 100 * ONE + AMEND_TICK),
        limits: ExecLimits {
            requote_threshold_ticks: 1,
            max_quote_distance_centi_bps: 100 * 100 * 100,
            max_book_age: DurationUs::from_secs(2),
            max_order_notional_quote: Notional::from_mantissa(i64::MAX / 2),
        },
        funds: rich(),
    }
}

fn shrink_to(qty: i64) -> Option<DesiredQuote> {
    Some(DesiredQuote {
        price: Price::from_mantissa(PASSIVE_BUY),
        qty: Qty::from_mantissa(qty),
        style: OrderStyle::PostOnly,
    })
}

fn resting_of(slot: &OrderSlot) -> RestingOrder {
    RestingOrder {
        price: slot.price(),
        qty: slot.qty(),
        filled: slot.filled_base(),
        amends_used: slot.amends_used(),
    }
}

/// A buy of `qty` that rests at the passive price, moved to `state`.
fn amend_order(qty: i64, state: OrderState) -> (OrderTable, usize) {
    let mut orders = table(1);
    let (index, client_id) = orders
        .claim(buy_claim(QuoteLevel::ZERO, PASSIVE_BUY, qty))
        .expect("a fresh level has a free slot");
    orders.apply(index, &event(ExecKind::AckPlaced, client_id), RECEIVED);
    if state == OrderState::AmendInFlight {
        orders.mark_amend_sent(index);
    }
    assert_eq!(orders.slot(index).state(), state);
    (orders, index)
}

fn amend_answer(kind: ExecKind, qty: i64) -> ExecEvent {
    ExecEvent {
        status: Some(VenueOrderStatus::New),
        qty: Qty::from_mantissa(qty),
        ..event(kind, ClientOrderId(1))
    }
}

/// `-2038` is the only venue code that says anything about what is left, so this is the one shape a
/// definitive zero arrives in.
fn amend_refused(amends_remaining: u8) -> ExecEvent {
    ExecEvent {
        reject: Some(RejectClass::StillLive),
        status: None,
        amends_remaining,
        ..amend_answer(ExecKind::AckFailed, 0)
    }
}

fn amend_not_sent() -> ExecEvent {
    ExecEvent {
        status: None,
        ..amend_answer(ExecKind::AmendNotSent, 0)
    }
}

fn amend_answers() {
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
        let (mut orders, index) = amend_order(10 * STEP, OrderState::AmendInFlight);
        for kind in answers {
            orders.apply(index, &amend_answer(*kind, 4 * STEP), RECEIVED);
        }
        let slot = orders.slot(index);
        assert_eq!(
            slot.qty(),
            Qty::from_mantissa(4 * STEP),
            "the slot did not settle on the venue's size after {answers:?}"
        );
        assert_eq!(
            slot.amends_used(),
            1,
            "one amend command spent {} amends against {answers:?}",
            slot.amends_used()
        );
        assert_eq!(
            reconcile_side(reconcile_input(shrink_to(4 * STEP), resting_of(slot))),
            ReconcileOutcome::Nothing,
            "the shrink was decided a second time after {answers:?}"
        );
        orders.mark_amend_sent(index);
        for kind in answers {
            orders.apply(index, &amend_answer(*kind, 3 * STEP), RECEIVED);
        }
        assert_eq!(
            orders.slot(index).amends_used(),
            2,
            "a second amend command left the count at {} against {answers:?}",
            orders.slot(index).amends_used()
        );
    }

    let (mut spent, index) = amend_order(10 * STEP, OrderState::Live);
    spent.apply(index, &amend_refused(0), RECEIVED);
    let outcome = reconcile_side(reconcile_input(
        shrink_to(4 * STEP),
        resting_of(spent.slot(index)),
    ));
    assert_eq!(
        outcome,
        ReconcileOutcome::Cancel,
        "the venue said this order gets no more amends, yet the engine answered {outcome:?}"
    );
    let (mut unclaimed, index) = amend_order(10 * STEP, OrderState::Live);
    unclaimed.apply(index, &amend_refused(ExecEvent::AMENDS_UNKNOWN), RECEIVED);
    assert_eq!(
        reconcile_side(reconcile_input(
            shrink_to(4 * STEP),
            resting_of(unclaimed.slot(index))
        )),
        ReconcileOutcome::AmendQty(Qty::from_mantissa(4 * STEP)),
        "an event claiming nothing about the budget spent it anyway"
    );

    let (mut late, index) = amend_order(10 * STEP, OrderState::AmendInFlight);
    for (kind, qty) in [
        (ExecKind::AckAmended, 6 * STEP),
        (ExecKind::AckAmended, 3 * STEP),
        (ExecKind::ReportAmended, 6 * STEP),
    ] {
        late.apply(index, &amend_answer(kind, qty), RECEIVED);
    }
    assert_eq!(
        late.slot(index).qty(),
        Qty::from_mantissa(3 * STEP),
        "a stale amend answer restored a size the venue has left"
    );

    let (mut unsent, index) = amend_order(10 * STEP, OrderState::AmendInFlight);
    unsent.apply(index, &amend_not_sent(), RECEIVED);
    let slot = unsent.slot(index);
    assert_eq!(
        slot.state(),
        OrderState::Live,
        "the slot is still waiting on an amend the venue was never asked for"
    );
    assert_eq!(
        slot.qty(),
        Qty::from_mantissa(10 * STEP),
        "an amend that never left moved the resting size anyway"
    );
    assert_eq!(
        slot.amends_used(),
        0,
        "an amend that never left spent one of the order's amends"
    );
    assert_eq!(
        reconcile_side(reconcile_input(shrink_to(4 * STEP), resting_of(slot))),
        ReconcileOutcome::AmendQty(Qty::from_mantissa(4 * STEP)),
        "the shrink was abandoned with the order still resting at its original size"
    );
    let (mut retired, index) = amend_order(10 * STEP, OrderState::AmendInFlight);
    retired.apply(
        index,
        &amend_answer(ExecKind::ReportCanceled, 10 * STEP),
        RECEIVED,
    );
    retired.apply(index, &amend_not_sent(), RECEIVED);
    assert_eq!(
        retired.slot(index).state(),
        OrderState::Closed(CloseReason::Canceled),
        "a refused amend resurrected an order the venue had already retired"
    );
}

/// FITNESS: a client order id names one slot and one generation of it, across the whole space the
/// id can encode and across each reuse of a slot. A late report that addressed a slot now held by a
/// later order would fold someone else's fill into it.
#[test]
fn client_id_addresses_only_its_slot_and_generation() {
    let layout = ClientIdLayout {
        run_nonce: RUN_NONCE,
    };
    for slot_index in 0..MAX_ORDER_SLOTS {
        for generation in [0u16, 1, 7, u16::MAX] {
            let id = layout.encode(slot_index, generation);
            assert_eq!(ClientIdLayout::slot_of(id), slot_index);
            assert_eq!(ClientIdLayout::nonce_of(id), RUN_NONCE);
            assert_eq!(ClientIdLayout::generation_of(id), generation);
        }
    }

    let mut orders = table(1);
    let (index, first) = orders
        .claim(buy_claim(QuoteLevel::ZERO, 0, 0))
        .expect("a fresh side has free slots");
    assert_eq!(orders.find(first), Some(index));
    orders.apply(
        index,
        &event(ExecKind::AckCanceled, first),
        LocalTs::from_micros(10),
    );
    orders.reap(
        LocalTs::from_micros(10 + 60_000_000),
        DurationUs::from_micros(60_000_000),
    );
    let (reused, second) = orders
        .claim(OrderClaim {
            claimed_ts_us: LocalTs::from_micros(100),
            ..buy_claim(QuoteLevel::ZERO, 0, 0)
        })
        .expect("the reaped slot is free again");
    assert_eq!(reused, index, "the side reuses its own slots");
    assert_ne!(first, second, "the generation did not advance on reuse");
    assert_eq!(
        orders.find(first),
        None,
        "the reaped order's id still addresses its old slot, so a late report would corrupt the new tenant"
    );
    assert_eq!(orders.find(second), Some(index));

    let mut orders = table(1);
    let base = side_base(InstrumentId(0), Side::Buy);
    rest_and_reap(&mut orders, base, u16::MAX - 1);
    let (index, last) = orders
        .claim(buy_claim(QuoteLevel::ZERO, 0, 0))
        .expect("the reaped slot is free again");
    assert_eq!(
        (index, ClientIdLayout::generation_of(last)),
        (base, u16::MAX)
    );
    cancel_and_reap(&mut orders, index, last);
    let (next, id) = orders
        .claim(buy_claim(QuoteLevel::ZERO, 0, 0))
        .expect("the level holds three more slots");
    assert_ne!(
        next,
        base,
        "a slot at its last generation was claimed again, and its id wrapped to generation {}",
        ClientIdLayout::generation_of(id)
    );
    assert_eq!(next, base + 1);

    let mut orders = table(1);
    let mut minted = HashSet::new();
    let refusal = loop {
        match orders.claim(buy_claim(QuoteLevel::ZERO, 0, 0)) {
            Ok((index, id)) => {
                assert!(minted.insert(id), "client id {id:?} was minted twice");
                cancel_and_reap(&mut orders, index, id);
            }
            Err(refusal) => break refusal,
        }
    };
    assert_eq!(
        (minted.len(), refusal),
        (4 * usize::from(u16::MAX), ClaimRefusal::IdsSpent),
        "each slot of a level mints each generation once, then the level refuses"
    );

    let mut full = table(1);
    for _ in 0..4 {
        full.claim(buy_claim(QuoteLevel::ZERO, 0, 0))
            .expect("the level has four slots");
    }
    assert_eq!(
        full.claim(buy_claim(QuoteLevel::ZERO, 0, 0)),
        Err(ClaimRefusal::LevelFull),
        "four working orders on one level are a leak, never spent ids"
    );

    a_spent_level_refuses_and_the_engine_quotes_on();
}

/// The quote pass reports the refusal on the side whose level is spent, places on the other side,
/// and does not halt.
fn a_spent_level_refuses_and_the_engine_quotes_on() {
    let QuotingEngine {
        mut engine,
        mut commands,
        mut ui_events,
        ..
    } = built_quoting_engine(QuotingSetup {
        row: row_with_ceiling(CEILING),
        strategy: Box::new(TwoSidedQuoter),
        restored: &[],
        settings: exec_settings(),
        tables: RecordedTables::new(&[TableKind::Orders]),
        run_nonce: 0,
    });
    make_ready(&mut engine, 0);
    let base = side_base(INSTRUMENT, Side::Buy);
    for slot in base..base + 4 {
        let client_id = ClientIdLayout { run_nonce: 0 }.encode(slot, u16::MAX);
        for (kind, status) in [
            (ExecKind::SnapshotOrder, VenueOrderStatus::New),
            (ExecKind::ReportCanceled, VenueOrderStatus::Canceled),
        ] {
            engine.dispatch(&InboundMessage::new(
                ts(20),
                MessageBody::Exec(ExecEvent {
                    kind,
                    status: Some(status),
                    qty: Qty::from_mantissa(ONE / 10),
                    ..exec_event(INSTRUMENT, client_id, Side::Buy, BID, 20)
                }),
            ));
        }
    }
    let after_reap = 20 + exec_settings().order_reap_window.micros();
    for message in reseat_book(BID, ASK, after_reap) {
        engine.dispatch(&message);
    }
    drain_commands(&mut commands);
    local_refusals(&mut ui_events);
    spin_at(&mut engine, 1, after_reap + 10);

    let sent = drain_commands(&mut commands);
    assert!(
        matches!(
            sent.as_slice(),
            [ExecCommand::Place {
                side: Side::Sell,
                ..
            }]
        ),
        "the sell side still quotes, and the spent buy side sends nothing: {sent:?}"
    );
    assert_eq!(local_refusals(&mut ui_events), [RejectReason::IdsSpent]);
}

const REAP_WINDOW: DurationUs = DurationUs::from_micros(60_000_000);

/// An order of a slot at `generation` that the venue named, then cancelled, then reaped: the slot is
/// free again, and its generation stays.
fn rest_and_reap(orders: &mut OrderTable, slot: usize, generation: u16) {
    let id = ClientIdLayout {
        run_nonce: RUN_NONCE,
    }
    .encode(slot, generation);
    let index = orders
        .adopt(&event(ExecKind::SnapshotOrder, id), RECEIVED)
        .expect("the slot is free");
    cancel_and_reap(orders, index, id);
}

fn cancel_and_reap(orders: &mut OrderTable, index: usize, id: ClientOrderId) {
    let at = LocalTs::from_micros(10);
    orders.apply(index, &event(ExecKind::AckCanceled, id), at);
    orders.reap(LocalTs::from_micros(10 + REAP_WINDOW.micros()), REAP_WINDOW);
    assert_eq!(orders.slot(index).state(), OrderState::Free);
}

/// FITNESS: a reconciliation pass retires an order only when the order predates the question and
/// the answer did not name it, and only on the instrument that it asked about. A refused probe leaves
/// the doubt standing, and a cautious count past the cap halts and sweeps. A pass that retired an
/// order placed after its question would drop an order that is resting, and the next spin would
/// place a second one beside it.
#[test]
fn sweep_retires_only_orders_that_predate_its_question() {
    a_sweep_spares_what_is_newer_than_its_question();
    a_later_pass_retires_an_unknown_slot_inside_its_instrument();
    a_refused_probe_leaves_the_doubt();
    occupancy_above_the_cap_halts_and_sweeps();
}

fn claim_at(orders: &mut OrderTable, instrument: InstrumentId, at: QuoteLevel, seq: u64) -> usize {
    let (index, _) = orders
        .claim(OrderClaim {
            instrument,
            side: Side::Buy,
            level: at,
            price: Price::from_mantissa(100),
            qty: Qty::from_mantissa(1),
            style: OrderStyle::PostOnly,
            claimed_ts_us: LocalTs::from_micros(0),
            recon_seq: seq,
        })
        .expect("the keyed level has a generation slot");
    index
}

fn sweep(orders: &mut OrderTable, instrument: InstrumentId, recon_seq: u64) -> usize {
    let mut closed = 0;
    orders.sweep_unseen(
        ReconcilePass {
            instrument,
            recon_seq,
            recon_ts_us: LocalTs::from_micros(1_000),
        },
        &mut |_, _| closed += 1,
    );
    closed
}

fn a_sweep_spares_what_is_newer_than_its_question() {
    let mut orders = table(2);
    let placed_under = claim_at(&mut orders, INSTRUMENT, QuoteLevel::ZERO, 7);
    assert_eq!(sweep(&mut orders, INSTRUMENT, 7), 0);
    assert!(orders.slot(placed_under).state().is_working());

    let named = claim_at(&mut orders, INSTRUMENT, level(1), 0);
    let id = orders.slot(named).client_id();
    let seen = ExecEvent {
        kind: ExecKind::SnapshotOrder,
        status: Some(VenueOrderStatus::New),
        qty: Qty::from_mantissa(1),
        recon_seq: 8,
        ..exec_event(INSTRUMENT, id, Side::Buy, 100, 10)
    };
    orders.apply(named, &seen, ts(10));
    assert_eq!(sweep(&mut orders, INSTRUMENT, 8), 1);
    assert!(orders.slot(named).state().is_working());
    assert!(!orders.slot(placed_under).state().is_working());
}

/// An order the venue cancelled and then restated: the table holds it `Unknown`.
fn resurrected_slot(orders: &mut OrderTable, recon_seq: u64) -> usize {
    let index = claim_at(orders, INSTRUMENT, QuoteLevel::ZERO, recon_seq);
    let client_id = orders.slot(index).client_id();
    let base = ExecEvent {
        qty: Qty::from_mantissa(1),
        ..exec_event(INSTRUMENT, client_id, Side::Buy, 100, 10)
    };
    let canceled = ExecEvent {
        kind: ExecKind::ReportCanceled,
        status: Some(VenueOrderStatus::Canceled),
        ..base
    };
    orders.apply(index, &canceled, ts(10));
    let restated = ExecEvent {
        kind: ExecKind::SnapshotOrder,
        status: Some(VenueOrderStatus::New),
        recon_seq,
        ..base
    };
    orders.apply(index, &restated, ts(10));
    index
}

fn a_later_pass_retires_an_unknown_slot_inside_its_instrument() {
    let mut orders = table(2);
    let index = resurrected_slot(&mut orders, 5);
    assert_eq!(orders.slot(index).state(), OrderState::Unknown);
    assert_eq!(sweep(&mut orders, INSTRUMENT, 5), 0);
    assert_eq!(sweep(&mut orders, INSTRUMENT, 6), 1);
    assert!(!orders.slot(index).state().is_working());

    let elsewhere = claim_at(&mut orders, InstrumentId(1), QuoteLevel::ZERO, 0);
    assert_eq!(sweep(&mut orders, InstrumentId(0), 9), 0);
    assert!(orders.slot(elsewhere).state().is_working());
    assert_eq!(sweep(&mut orders, InstrumentId(1), 9), 1);
}

fn refused_at_the_gateway(client_id: ClientOrderId) -> ExecEvent {
    ExecEvent {
        kind: ExecKind::AckFailed,
        reject: Some(RejectClass::StillLive),
        reject_code: -1003,
        ..exec_event(INSTRUMENT, client_id, Side::Buy, 100, 20)
    }
}

fn a_refused_probe_leaves_the_doubt() {
    let mut orders = table(2);
    let index = resurrected_slot(&mut orders, 5);
    let client_id = orders.slot(index).client_id();
    orders.apply(index, &refused_at_the_gateway(client_id), ts(20));
    assert_eq!(orders.slot(index).state(), OrderState::Unknown);

    for (at, state) in [
        (level(1), OrderState::CancelInFlight),
        (level(2), OrderState::AmendInFlight),
    ] {
        let other = claim_at(&mut orders, INSTRUMENT, at, 5);
        let id = orders.slot(other).client_id();
        let placed = ExecEvent {
            kind: ExecKind::AckPlaced,
            ..exec_event(INSTRUMENT, id, Side::Buy, 100, 15)
        };
        orders.apply(other, &placed, ts(15));
        match state {
            OrderState::CancelInFlight => orders.mark_cancel_sent(other),
            _ => orders.mark_amend_sent(other),
        };
        orders.apply(other, &refused_at_the_gateway(id), ts(20));
        assert_eq!(orders.slot(other).state(), OrderState::Live);
    }
}

fn adopt_live(engine: &mut HotEngine, generation_offset: usize, side: Side, when: i64) {
    let slot = side_base(INSTRUMENT, side) + generation_offset;
    let client_id = ClientIdLayout { run_nonce: 0 }.encode(slot, 1);
    engine.dispatch(&InboundMessage::new(
        ts(when),
        MessageBody::Exec(ExecEvent {
            kind: ExecKind::SnapshotOrder,
            status: Some(VenueOrderStatus::New),
            qty: Qty::from_mantissa(ONE / 10),
            ..exec_event(INSTRUMENT, client_id, side, BID, when)
        }),
    ));
}

fn halt_of(events: &mut Consumer<UiEvent>) -> Option<UiHalt> {
    std::iter::from_fn(|| events.pop().ok())
        .filter_map(|event| match event {
            UiEvent::Execution { halt, .. } => Some(halt),
            _ => None,
        })
        .last()
}

fn occupancy_above_the_cap_halts_and_sweeps() {
    let QuotingEngine {
        mut engine,
        mut commands,
        mut ui_events,
        ..
    } = built_quoting_engine(QuotingSetup {
        row: row_with_ceiling(CEILING),
        strategy: Box::new(TwoSidedQuoter),
        restored: &[],
        settings: exec_settings(),
        tables: RecordedTables::new(&[TableKind::Orders]),
        run_nonce: 0,
    });
    make_ready(&mut engine, 0);
    for message in reseat_book(BID, ASK, 10) {
        engine.dispatch(&message);
    }

    adopt_live(&mut engine, 1, Side::Buy, 20);
    spin_at(&mut engine, 1, 30);
    assert!(!matches!(
        halt_of(&mut ui_events),
        Some(UiHalt::Halted { .. })
    ));
    drain_commands(&mut commands);

    adopt_live(&mut engine, 2, Side::Buy, 40);
    spin_at(&mut engine, 2, 50);
    assert!(matches!(
        halt_of(&mut ui_events),
        Some(UiHalt::Halted {
            reason: HaltReason::DuplicateResting,
            ..
        })
    ));
    assert!(
        drain_commands(&mut commands)
            .iter()
            .any(|command| matches!(command, ExecCommand::CancelOurs { .. }))
    );
}

/// One write of the order table, through its public transitions. A `pick` names the n-th slot in
/// use, so an answer or a mark lands on an order that exists.
#[derive(Debug, Clone)]
enum Operation {
    Claim { instrument: u16, side: Side, at: u8 },
    Answer { pick: usize, answer: usize },
    CancelSent { pick: usize },
    AmendSent { pick: usize },
    TimedOut { pick: usize },
    Adopt { slot: usize, generation: u16 },
    Sweep { instrument: u16, recon_seq: u64 },
    Invalidate,
    Reap { after_us: i64 },
}

/// Each kind of venue answer, with the status and the class that change what it does.
const ANSWERS: [(ExecKind, Option<VenueOrderStatus>, Option<RejectClass>); 17] = [
    (ExecKind::AckPlaced, None, None),
    (ExecKind::ReportNew, None, None),
    (ExecKind::PlaceNotSent, None, None),
    (ExecKind::AmendNotSent, None, None),
    (ExecKind::AckFailed, None, Some(RejectClass::StillLive)),
    (ExecKind::AckFailed, None, Some(RejectClass::Gone)),
    (ExecKind::AckFailed, None, Some(RejectClass::Ambiguous)),
    (ExecKind::ReportTrade, Some(VenueOrderStatus::Filled), None),
    (
        ExecKind::ReportTrade,
        Some(VenueOrderStatus::PartiallyFilled),
        None,
    ),
    (
        ExecKind::ReportCanceled,
        Some(VenueOrderStatus::Canceled),
        None,
    ),
    (ExecKind::AckCanceled, None, None),
    (
        ExecKind::ReportExpired,
        Some(VenueOrderStatus::Expired),
        None,
    ),
    (
        ExecKind::ReportRejected,
        Some(VenueOrderStatus::Rejected),
        None,
    ),
    (ExecKind::AckAmended, None, None),
    (ExecKind::SnapshotOrder, Some(VenueOrderStatus::New), None),
    (
        ExecKind::SnapshotOrder,
        Some(VenueOrderStatus::Canceled),
        None,
    ),
    (ExecKind::SnapshotEnd, None, None),
];

fn an_operation() -> impl Strategy<Value = Operation> {
    let side = prop_oneof![Just(Side::Buy), Just(Side::Sell)];
    prop_oneof![
        4 => (0u16..2, side, 0u8..8)
            .prop_map(|(instrument, side, at)| Operation::Claim { instrument, side, at }),
        4 => (any::<usize>(), 0..ANSWERS.len())
            .prop_map(|(pick, answer)| Operation::Answer { pick, answer }),
        1 => any::<usize>().prop_map(|pick| Operation::CancelSent { pick }),
        1 => any::<usize>().prop_map(|pick| Operation::AmendSent { pick }),
        1 => any::<usize>().prop_map(|pick| Operation::TimedOut { pick }),
        1 => (0..2 * 64usize, 1..u16::MAX)
            .prop_map(|(slot, generation)| Operation::Adopt { slot, generation }),
        1 => (0u16..2, 0u64..4)
            .prop_map(|(instrument, recon_seq)| Operation::Sweep { instrument, recon_seq }),
        1 => Just(Operation::Invalidate),
        2 => (0i64..2 * REAP_WINDOW.micros()).prop_map(|after_us| Operation::Reap { after_us }),
    ]
}

/// The slots that are not `Free`, read one by one through the public getters: the model the index
/// must equal.
fn scanned_in_use(orders: &OrderTable, slots: usize) -> Vec<usize> {
    (0..slots)
        .filter(|index| orders.slot(*index).state() != OrderState::Free)
        .collect()
}

fn operate(orders: &mut OrderTable, operation: &Operation, slots: usize, clock_us: i64) {
    let now = LocalTs::from_micros(clock_us);
    let in_use = scanned_in_use(orders, slots);
    let picked = |pick: usize| (!in_use.is_empty()).then(|| in_use[pick % in_use.len()]);
    match *operation {
        Operation::Claim {
            instrument,
            side,
            at,
        } => {
            // A refused claim is a legal outcome here: the index must hold either way.
            orders
                .claim(OrderClaim {
                    instrument: InstrumentId(instrument),
                    side,
                    claimed_ts_us: now,
                    ..buy_claim(level(at), 100, 1)
                })
                .ok();
        }
        Operation::Answer { pick, answer } => {
            let Some(index) = picked(pick) else { return };
            let (kind, status, reject) = ANSWERS[answer];
            let slot = orders.slot(index);
            let event = ExecEvent {
                kind,
                status,
                reject,
                qty: Qty::from_mantissa(1),
                recon_seq: 2,
                ..exec_event(
                    slot.instrument(),
                    slot.client_id(),
                    slot.side(),
                    100,
                    clock_us,
                )
            };
            orders.apply(index, &event, now);
        }
        Operation::CancelSent { pick } => {
            if let Some(index) = picked(pick) {
                orders.mark_cancel_sent(index);
            }
        }
        Operation::AmendSent { pick } => {
            if let Some(index) = picked(pick) {
                orders.mark_amend_sent(index);
            }
        }
        Operation::TimedOut { pick } => {
            if let Some(index) = picked(pick) {
                orders.mark_timed_out(index, now);
            }
        }
        Operation::Adopt { slot, generation } => {
            let id = ClientIdLayout {
                run_nonce: RUN_NONCE,
            }
            .encode(slot, generation);
            let instrument = InstrumentId(u16::try_from(slot / 64).expect("two instruments"));
            let restated = ExecEvent {
                kind: ExecKind::SnapshotOrder,
                status: Some(VenueOrderStatus::New),
                qty: Qty::from_mantissa(1),
                ..exec_event(instrument, id, Side::Buy, 100, clock_us)
            };
            orders.adopt(&restated, now);
        }
        Operation::Sweep {
            instrument,
            recon_seq,
        } => orders.sweep_unseen(
            ReconcilePass {
                instrument: InstrumentId(instrument),
                recon_seq,
                recon_ts_us: now,
            },
            &mut |_, _| {},
        ),
        Operation::Invalidate => orders.invalidate_all(&mut |_, _| {}),
        Operation::Reap { after_us } => {
            orders.reap(LocalTs::from_micros(clock_us + after_us), REAP_WINDOW);
        }
    }
}

proptest! {
    /// FITNESS: the index of slots in use lists exactly the slots that are not free, in ascending
    /// order, after each write of the table. The passes of a spin walk that index in place of every
    /// slot, so a slot that it missed would be an order that no timeout, reap or release ever visits
    /// again, and a slot out of order would change the order of the commands that a pass banks.
    #[test]
    fn working_index_equals_working_slots(
        operations in prop::collection::vec(an_operation(), 1..80),
    ) {
        let slots = exec_capacity(2).order_slots();
        let mut orders = table(2);
        for (step, operation) in operations.iter().enumerate() {
            let clock_us = 1_000 * (i64::try_from(step).expect("a short run") + 1);
            operate(&mut orders, operation, slots, clock_us);
            let indexed: Vec<usize> = orders.slots_in_use().collect();
            prop_assert_eq!(
                indexed,
                scanned_in_use(&orders, slots),
                "after step {} ({:?}) the index and the slots differ",
                step,
                operation
            );
        }
    }
}
