//! FITNESS: the order table's two money invariants — that a passive quote is never snapped into
//! aggression, and that a venue event stream which duplicates, reorders and arrives late can never
//! move our filled totals twice or lose a fill that was really paid.
//!
//! Both failures are silent by construction. An inverted snap still compiles, still produces a legal
//! order, and every layer downstream agrees with it; a double-folded fill produces a ledger that is
//! wrong in a direction no downstream reader can detect.

use polysim::hot::exec::{
    AccountTable, ClientIdLayout, CloseReason, MAX_ORDER_SLOTS, OrderClaim, OrderSlot, OrderState,
    OrderTable, QuoteLevel, ReleaseOutcome, apply_exec_event,
};
use polysim::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side, VenueOrderId};
use polysim::msg::exec::{
    AccountChunk, AccountChunkKind, ExecEvent, ExecKind, OrderStyle, Provenance, RejectClass,
    VenueOrderStatus,
};
use polysim::time::{DurationUs, TsUs};
use proptest::prelude::*;

use crate::engine_support::account_chunk;

const RUN_NONCE: u32 = 0x5EED_1234;
const TICK: i64 = 1_000_000;

fn event(kind: ExecKind, client_id: ClientOrderId) -> ExecEvent {
    ExecEvent {
        instrument: InstrumentId(0),
        client_id,
        venue_order_id: Some(VenueOrderId(77)),
        trade_id: None,
        kind,
        status: None,
        reject: None,
        provenance: Provenance::Mine,
        side: Side::Buy,
        liquidity: None,
        price: Price(100 * TICK),
        qty: Qty(10),
        last_price: Price(0),
        last_qty: Qty(0),
        cumulative_qty: Qty(0),
        cumulative_quote: 0,
        commission: 0,
        commission_asset: AssetId(1),
        reject_code: 0,
        amends_remaining: ExecEvent::AMENDS_UNKNOWN,
        recon_seq: 0,
        exchange_ts_us: TsUs::from_micros(1),
        request_sent_ts_us: None,
        received_ts_us: TsUs::from_micros(2),
        queued_ts_us: TsUs::from_micros(3),
    }
}

fn buy_claim(level: QuoteLevel, price: i64, qty: i64) -> OrderClaim {
    OrderClaim {
        instrument: InstrumentId(0),
        side: Side::Buy,
        level,
        price: Price(price),
        qty: Qty(qty),
        style: OrderStyle::PostOnly,
        claimed_ts_us: TsUs::from_micros(1),
        recon_seq: 0,
    }
}

fn live_slot() -> OrderSlot {
    OrderSlot {
        client_id: ClientOrderId(1),
        state: OrderState::Live,
        qty: Qty(10),
        ..OrderSlot::EMPTY
    }
}

proptest! {
    /// FITNESS: snapping is passive on both sides, at every price and grid. A buy that snapped UP or
    /// a sell that snapped DOWN would cross a spread the strategy meant to rest inside — and it
    /// would look like a working system, because the order is legal and every layer agrees.
    #[test]
    fn snapping_never_manufactures_aggression(
        mantissa in -1_000_000_000_000i64..1_000_000_000_000,
        increment in 1i64..1_000_000,
    ) {
        let raw = Price(mantissa);
        let bid = Side::Buy.snap_passive(raw, increment);
        let ask = Side::Sell.snap_passive(raw, increment);

        prop_assert!(bid.0 <= raw.0, "a buy snapped UP, from {} to {}", raw.0, bid.0);
        prop_assert!(ask.0 >= raw.0, "a sell snapped DOWN, from {} to {}", raw.0, ask.0);
        prop_assert_eq!(bid.0.rem_euclid(increment), 0, "a buy left the grid");
        prop_assert_eq!(ask.0.rem_euclid(increment), 0, "a sell left the grid");
        // Never further than one increment: rounding past a whole tick is a different price, not a
        // snap, and on a wide grid it would silently reprice the quote.
        prop_assert!(raw.0 - bid.0 < increment);
        prop_assert!(ask.0 - raw.0 < increment);
    }

    /// FITNESS: folding the venue's CUMULATIVE totals is idempotent under any redelivery order.
    /// Replaying the same reports in any order, with duplicates, must leave exactly the totals the
    /// highest report carried — which is what lets the engine hold no seen-set and allocate nothing.
    #[test]
    fn redelivered_fills_fold_once(
        steps in prop::collection::vec(1u8..20, 1..12),
        duplicates in prop::collection::vec(0usize..11, 0..12),
    ) {
        let mut cumulative = 0i64;
        let mut reports = Vec::new();
        for step in &steps {
            cumulative += i64::from(*step);
            let mut trade = event(ExecKind::ReportTrade, ClientOrderId(1));
            trade.cumulative_qty = Qty(cumulative);
            trade.cumulative_quote = cumulative * 100;
            reports.push(trade);
        }
        let highest = cumulative;
        for index in duplicates {
            if let Some(report) = reports.get(index % reports.len().max(1)).copied() {
                reports.push(report);
            }
        }

        let mut slot = OrderSlot { qty: Qty(highest), ..live_slot() };
        let mut applied_base = 0i64;
        for report in &reports {
            applied_base += apply_exec_event(&mut slot, report).fill.base.0;
        }

        prop_assert_eq!(slot.filled_base.0, highest, "the slot's total is not the venue's");
        prop_assert_eq!(applied_base, highest, "the deltas handed out do not sum to the total");
    }
}

/// FITNESS: three ways an answer must not be read as more certain than the venue actually gave it —
/// an ambiguous rejection, a fill racing an outstanding cancel, and a late fill on an already closed
/// order.
///
/// The three go together deliberately: each is the same failure shape — resolving in-flight or
/// terminal state past what the venue has confirmed — at the three sites where getting it wrong
/// either discards a fill the account was really paid or drives a second command at an order that may
/// still be resting. Binance's -2011 on a cancel reads as "unknown order", which is also what a FULLY
/// FILLED order returns, so closing on it loses the fill; a fill mid-cancel must not resolve the
/// cancel, or the next spin sees a Live order with no command out and fires a second cancel; and a
/// closed order must still fold a late redelivered fill instead of dropping it because the slot looks
/// done.
#[test]
fn ambiguous_or_late_answers_never_resolve_past_what_the_venue_confirmed() {
    let mut ambiguous = OrderSlot {
        state: OrderState::CancelInFlight,
        ..live_slot()
    };
    let mut ack = event(ExecKind::AckFailed, ClientOrderId(1));
    ack.reject = Some(RejectClass::Ambiguous);
    let applied = apply_exec_event(&mut ambiguous, &ack);
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

    let mut mid_cancel = OrderSlot {
        state: OrderState::CancelInFlight,
        ..live_slot()
    };
    let mut trade = event(ExecKind::ReportTrade, ClientOrderId(1));
    trade.cumulative_qty = Qty(4);
    trade.cumulative_quote = 400;
    let applied = apply_exec_event(&mut mid_cancel, &trade);
    assert_eq!(
        applied.state,
        OrderState::CancelInFlight,
        "a fill resolved an outstanding cancel the venue has not yet answered"
    );
    assert_eq!(
        applied.fill.base,
        Qty(4),
        "the partial fill was still applied"
    );

    let mut closed = OrderSlot {
        state: OrderState::Closed(CloseReason::Filled),
        qty: Qty(10),
        filled_base: Qty(6),
        filled_quote: 600,
        ..live_slot()
    };
    let mut late = event(ExecKind::ReportTrade, ClientOrderId(1));
    late.cumulative_qty = Qty(10);
    late.cumulative_quote = 1_000;
    let applied = apply_exec_event(&mut closed, &late);
    assert_eq!(
        applied.fill.base,
        Qty(4),
        "a late fill on a closed order was lost"
    );
    assert_eq!(
        applied.state,
        OrderState::Closed(CloseReason::Filled),
        "the first terminal answer is the true one"
    );
}

/// FITNESS: a client id addresses exactly the slot it was encoded for, both across the full
/// encode/decode space and across a slot's reuse by a later generation.
///
/// The two go together deliberately: one proves the layout round-trips for every slot and generation
/// the table can produce; the other proves a reaped order's OLD id stops resolving once the slot is
/// reused, which is the case the layout exists to defend — a late report addressing a slot now owned
/// by a different order would otherwise fold someone else's fill into it.
///
/// All three fields are read back, because production reads all three and each one decides
/// something different: the nonce separates this run's orders from a dead run's, the slot says where
/// a report folds, and the generation says whether it folds at all. Widening one field silently
/// steals bits from another, and a round trip over the whole space is what catches that.
#[test]
fn a_client_id_addresses_only_the_slot_and_generation_it_was_encoded_for() {
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

    let mut table = OrderTable::new(RUN_NONCE);
    let (index, first) = table
        .claim(buy_claim(QuoteLevel::ZERO, 0, 0))
        .expect("a fresh side has free slots");
    assert_eq!(table.find(first), Some(index));

    let mut canceled = event(ExecKind::AckCanceled, first);
    canceled.received_ts_us = TsUs::from_micros(10);
    apply_exec_event(table.slot_mut(index), &canceled);
    table.reap(
        TsUs::from_micros(10 + 60_000_000),
        DurationUs::from_micros(60_000_000),
    );

    let (reused, second) = table
        .claim(OrderClaim {
            claimed_ts_us: TsUs::from_micros(100),
            ..buy_claim(QuoteLevel::ZERO, 0, 0)
        })
        .expect("the reaped slot is free again");

    assert_eq!(reused, index, "the side reuses its own slots");
    assert_ne!(first, second, "the generation did not advance on reuse");
    assert_eq!(
        table.find(first),
        None,
        "the reaped order's id still addresses its old slot, so a late report would corrupt the new tenant"
    );
    assert_eq!(table.find(second), Some(index));
}

/// FITNESS: a side's in-flight bookkeeping is conservative in both directions the reconciler asks
/// about — "is anything on this side awaiting an answer" and "how many orders on this side might
/// still exist" — and both stay conservative until a TERMINAL venue fact proves otherwise.
///
/// The two go together deliberately: they are the same over-counting discipline read through the
/// reconciler's two queries, and either one under-counting lets a second command fire beside an order
/// that may still be resting or still be live.
#[test]
fn side_level_bookkeeping_stays_conservative_until_a_terminal_fact_proves_otherwise() {
    let mut awaiting_table = OrderTable::new(RUN_NONCE);
    let level_zero = buy_claim(QuoteLevel::ZERO, 100 * TICK, 10);
    let level_one = buy_claim(QuoteLevel::new(1).expect("level one"), 100 * TICK, 10);
    let (live, _) = awaiting_table
        .claim(level_zero)
        .expect("level zero has capacity");
    let (cancelling, _) = awaiting_table
        .claim(level_one)
        .expect("level one has capacity");
    awaiting_table.slot_mut(live).state = OrderState::Live;
    awaiting_table.slot_mut(cancelling).state = OrderState::CancelInFlight;
    assert_eq!(
        awaiting_table
            .resting(InstrumentId(0), Side::Buy, QuoteLevel::ZERO)
            .map(|slot| slot.state),
        Some(OrderState::Live),
        "level zero remains independently addressable"
    );
    assert!(
        awaiting_table.is_awaiting_answer(InstrumentId(0), Side::Buy),
        "a cancel is outstanding on this side and nothing else may fire until it is answered"
    );
    awaiting_table.slot_mut(cancelling).state = OrderState::Closed(CloseReason::Canceled);
    assert!(
        !awaiting_table.is_awaiting_answer(InstrumentId(0), Side::Buy),
        "both legs are resolved, so the side may quote again"
    );

    let mut count_table = OrderTable::new(RUN_NONCE);
    let mut indexes = Vec::new();
    for level_index in 0..3u8 {
        let (index, _) = count_table
            .claim(buy_claim(
                QuoteLevel::new(level_index).expect("test level"),
                (100 - i64::from(level_index)) * TICK,
                10,
            ))
            .expect("each keyed level has its own generation slots");
        indexes.push(index);
    }
    assert_eq!(
        count_table.possibly_live_count(InstrumentId(0), Side::Buy),
        3
    );
    for (index, state) in indexes.iter().copied().zip([
        OrderState::CancelInFlight,
        OrderState::AmendInFlight,
        OrderState::Unknown,
    ]) {
        count_table.slot_mut(index).state = state;
    }
    assert_eq!(
        count_table.possibly_live_count(InstrumentId(0), Side::Buy),
        3,
        "uncertainty released capacity before non-existence was proved"
    );
    count_table.slot_mut(indexes[0]).state = OrderState::Closed(CloseReason::Canceled);
    assert_eq!(
        count_table.possibly_live_count(InstrumentId(0), Side::Buy),
        2
    );
}

/// FITNESS: `PendingCancel` is a venue saying it does not yet know, and every layer must keep
/// treating it that way — the cancel stays outstanding, the side stays shut, and a fill landing
/// afterwards is still money the account was really paid.
///
/// This is the promise the shared vocabulary makes about the two answers either side of it.
/// `AckCanceled` and `ReportCanceled` mean "this order can no longer fill"; an adapter over a venue
/// whose cancels are best-effort cannot promise that, so it reports `PendingCancel` — deliberately
/// NON-terminal — and says nothing more until finality is known. Reading the probe's answer as the
/// cancel's own closes a slot over an order still resting, frees the side, and lets the ladder place
/// a replacement beside it; the fill below then arrives against an order the engine believes is
/// gone, and it is the venue, not the strategy, that decides how much got bought.
#[test]
fn a_pending_cancel_answer_never_resolves_the_cancel_it_was_asked_about() {
    let mut table = OrderTable::new(RUN_NONCE);
    let (index, client_id) = table
        .claim(buy_claim(QuoteLevel::ZERO, 100 * TICK, 10))
        .expect("a fresh side has free slots");
    apply_exec_event(
        table.slot_mut(index),
        &event(ExecKind::AckPlaced, client_id),
    );
    table.slot_mut(index).state = OrderState::CancelInFlight;

    let mut probe = event(ExecKind::SnapshotOrder, client_id);
    probe.status = Some(VenueOrderStatus::PendingCancel);
    probe.recon_seq = 1;
    let applied = apply_exec_event(table.slot_mut(index), &probe);
    assert_eq!(
        applied.state,
        OrderState::CancelInFlight,
        "a status probe answering \"pending cancel\" was read as the cancel's own answer"
    );
    assert!(
        table.is_awaiting_answer(InstrumentId(0), Side::Buy),
        "the side reopened with its cancel still unanswered, so the ladder may quote beside an \
         order that can still fill"
    );

    let mut trade = event(ExecKind::ReportTrade, client_id);
    trade.cumulative_qty = Qty(4);
    trade.cumulative_quote = 400;
    let applied = apply_exec_event(table.slot_mut(index), &trade);
    assert_eq!(
        applied.fill.base,
        Qty(4),
        "a fill arriving after the probe was lost"
    );
    assert_eq!(
        applied.state,
        OrderState::CancelInFlight,
        "a fill is no more an answer to the cancel than the probe was"
    );

    let mut restated = event(ExecKind::SnapshotOrder, client_id);
    restated.status = Some(VenueOrderStatus::PendingCancel);
    restated.recon_seq = 2;
    restated.cumulative_qty = Qty(4);
    restated.cumulative_quote = 400;
    assert_eq!(
        apply_exec_event(table.slot_mut(index), &restated).fill.base,
        Qty(0),
        "a probe restating the venue's running totals paid the same fill a second time"
    );

    let mut canceled = event(ExecKind::ReportCanceled, client_id);
    canceled.cumulative_qty = Qty(4);
    canceled.cumulative_quote = 400;
    let applied = apply_exec_event(table.slot_mut(index), &canceled);
    assert_eq!(
        applied.state,
        OrderState::Closed(CloseReason::Canceled),
        "the terminal report is the answer the slot was waiting for"
    );
    assert!(
        !table.is_awaiting_answer(InstrumentId(0), Side::Buy),
        "the side is still shut after finality arrived, so it can never quote again"
    );
    assert_eq!(
        table.possibly_live_count(InstrumentId(0), Side::Buy),
        0,
        "a proven-cancelled order still counts against the side's capacity"
    );
    assert_eq!(
        table.slot(index).filled_base,
        Qty(4),
        "the fill moved money more than once across the probe, the restatement and the cancel"
    );
}

/// FITNESS: a balance reservation releases only on terminal venue proof, whichever direction that
/// proof arrives from — a later balance snapshot confirming the ack already landed, or a proof the
/// request never reached the venue at all.
///
/// The two go together deliberately: both are the same reservation-lifecycle rule — hold until
/// proof, release exactly once proof arrives — read through its two entry points. Releasing on the
/// ack alone is a real double-spend, since the ack routinely beats the balance update; and a
/// placement proved unsent is not a venue rejection and must not strand either the reservation or the
/// existence capacity it took before dispatch.
#[test]
fn a_reservation_releases_only_on_terminal_proof_from_either_direction() {
    let quote = AssetId(1);
    let mut account = AccountTable::new();
    account.apply(&balances(quote, 1_000, 1, AccountChunkKind::Snapshot));

    let taken_at = account.reserve(quote, 400);
    assert_eq!(
        account.balance(quote).spendable(),
        600,
        "the reservation must shrink what the next order may spend"
    );
    assert_eq!(
        account.release(quote, 400, taken_at),
        ReleaseOutcome::Held,
        "released on the ack alone — the same balance can now fund a second order"
    );
    account.apply(&balances(quote, 600, 2, AccountChunkKind::Update));
    assert_eq!(
        account.release(quote, 400, taken_at),
        ReleaseOutcome::Released
    );
    assert_eq!(account.balance(quote).spendable(), 600);

    let mut unsent_account = AccountTable::new();
    unsent_account.apply(&balances(quote, 1_000, 1, AccountChunkKind::Snapshot));
    unsent_account.reserve(quote, 400);
    let mut slot = OrderSlot {
        state: OrderState::PendingNew,
        reserved_amount: 400,
        ..live_slot()
    };
    let unsent = event(ExecKind::PlaceNotSent, slot.client_id);
    assert_eq!(
        apply_exec_event(&mut slot, &unsent).state,
        OrderState::Closed(CloseReason::Rejected)
    );
    unsent_account.release_unsent(quote, 400);
    assert_eq!(
        unsent_account.balance(quote).spendable(),
        1_000,
        "a request that never left the process stranded its reservation"
    );
}

fn balances(
    asset: AssetId,
    free: i64,
    venue_update_ts_ms: u64,
    kind: AccountChunkKind,
) -> AccountChunk {
    AccountChunk {
        kind,
        venue_update_ts_ms,
        exchange_ts_us: TsUs::from_micros(1),
        received_ts_us: TsUs::from_micros(2),
        queued_ts_us: TsUs::from_micros(3),
        ..account_chunk(&[(asset, free)], 0)
    }
}
