//! Folding a venue event: a stream reset, the end of an open-orders snapshot, or the transition of
//! an order of this run or an earlier one, with its money, reservation release and audit rows.

use crate::hot::ledger::{FillFee, LedgerFill, PositionLedger};
use crate::hot::strategy::Actions;
use crate::log::{hot_error, hot_warn};
use crate::msg::exec::{ExecEvent, ExecKind, RejectClass};
use crate::msg::ids::{AssetId, Notional, Qty};
use crate::msg::persist::{IncidentKind, IncidentRow, OrderLifecycle, OrderTransition};
use crate::msg::time::LocalTs;

use super::account::ReleaseOutcome;
use super::audit::{bank_engine_driven, bank_event_rows, bank_incident};
use super::engine::{ExecEngine, FoldStamps, view_of};
use super::gates::HaltReason;
use super::order::{Applied, FeeFold, OrderSlot, OrderState, ReconcilePass, Unseated};
use super::view::{ExecCallback, Fill, OrderReject, RejectOrigin};

impl ExecEngine {
    #[cold]
    pub(super) fn on_stream_reset(&mut self, at: LocalTs, bank: &mut Actions) -> ExecCallback {
        hot_warn!(
            "execution stream reset — every open order is unknown until a reconciliation lands"
        );
        self.readiness.observe_stream_reset();
        // Every reason the pass last reported was derived from order state that is now a memory, so
        // the next refusal is news again rather than a repeat the latch would swallow.
        self.refusals.forget();
        self.orders.invalidate_all(&mut |slot, previous| {
            let transition = OrderTransition::StreamReset;
            bank_engine_driven(bank, slot, transition, previous.into(), at);
        });
        ExecCallback::None
    }

    #[cold]
    pub(super) fn on_snapshot_end(
        &mut self,
        event: &ExecEvent,
        stamps: FoldStamps,
        bank: &mut Actions,
    ) -> ExecCallback {
        let Some(instrument) = event.instrument else {
            return ExecCallback::None;
        };
        let pass = ReconcilePass {
            instrument,
            recon_seq: event.recon_seq,
            recon_ts_us: stamps.fold,
        };
        self.orders.sweep_unseen(pass, &mut |slot, previous| {
            swept_warning(slot, pass.recon_seq);
            let transition = OrderTransition::SweepClosed;
            bank_engine_driven(bank, slot, transition, previous.into(), stamps.received);
        });
        self.readiness.observe_open_orders_snapshot();
        ExecCallback::None
    }

    /// Retries each release that beat its account update. A free slot holds no reservation, so the
    /// pass walks the slots in use.
    pub(super) fn retry_reservation_releases(&mut self) {
        let mut next = self.orders.next_in_use(0);
        while let Some(index) = next {
            next = self.orders.next_in_use(index + 1);
            let slot = self.orders.slot(index);
            if slot.reserved_amount().is_none() || slot.state() == OrderState::PendingNew {
                continue;
            }
            self.release_reservation(index);
        }
    }

    pub(super) fn apply_to_slot(
        &mut self,
        index: usize,
        event: &ExecEvent,
        stamps: FoldStamps,
        ledger: &mut PositionLedger,
        bank: &mut Actions,
    ) -> ExecCallback {
        let (now, received) = (stamps.fold, stamps.received);
        let previous_state = OrderLifecycle::from(self.orders.slot(index).state());
        let applied = self.orders.apply(index, event, now);
        if applied.is_resurrection {
            self.resurrection_error(event);
        }
        let slot = *self.orders.slot(index);

        self.book_report(&slot, event, applied, ledger, bank, received);
        if event.kind == ExecKind::PlaceNotSent {
            self.release_unsent_reservation(index);
        } else {
            self.release_reservation(index);
        }
        self.record_outcome(event, applied.state, now);
        bank_event_rows(bank, &slot, event, previous_state, applied.fill, received);

        if !applied.fill.is_empty() {
            return ExecCallback::Fill(Fill {
                instrument: slot.instrument(),
                client_id: slot.client_id(),
                side: slot.side(),
                level: slot.level(),
                price: event.last_price,
                qty: applied.fill.base,
                notional: applied.fill.quote,
                commission: event.commission,
                commission_asset: event.commission_asset,
                liquidity: event.liquidity,
                trade_id: event.trade_id,
                state: applied.state,
                event_ts_us: received,
            });
        }
        if let Some(class) = event.reject {
            return ExecCallback::Reject(OrderReject {
                instrument: slot.instrument(),
                client_id: Some(slot.client_id()),
                side: slot.side(),
                level: Some(slot.level()),
                origin: RejectOrigin::Venue {
                    class,
                    code: event.reject_code,
                },
                event_ts_us: received,
            });
        }
        ExecCallback::Update(view_of(&slot).updated_at(received))
    }

    /// An order of an earlier run holds no reservation, adds to no reject streak and earns no
    /// callback. Its fills still move the ledger.
    #[cold]
    pub(super) fn apply_to_prior_run(
        &mut self,
        event: &ExecEvent,
        stamps: FoldStamps,
        ledger: &mut PositionLedger,
        bank: &mut Actions,
    ) {
        let now = stamps.fold;
        let index = match self.prior_orders.find_or_seat(event, now) {
            Ok(index) => index,
            Err(Unseated::TableFull) => return self.on_prior_run_overflow(event, stamps, bank),
            Err(Unseated::NoInstrument) => return self.prior_run_without_instrument_error(event),
        };
        let previous_state = OrderLifecycle::from(self.prior_orders.slot(index).state());
        let applied = self.prior_orders.apply(index, event, now);
        let slot = *self.prior_orders.slot(index);
        self.book_report(&slot, event, applied, ledger, bank, stamps.received);
        bank_event_rows(
            bank,
            &slot,
            event,
            previous_state,
            applied.fill,
            stamps.received,
        );
    }

    /// Ours by tag and nonce, but no slot holds it: the reap window was too short, or the order
    /// table leaked a slot. With a fill, the venue moved money that the ledger cannot place without
    /// a guess, so quoting halts and the tape keeps the fill. With none, the event is refused and
    /// counted.
    #[cold]
    pub(super) fn on_orphan(&mut self, event: &ExecEvent, stamps: FoldStamps, bank: &mut Actions) {
        if event.cumulative_qty.mantissa() <= 0 {
            orphan_warning(event);
            self.incidents.orphan_events_without_fill += 1;
            return;
        }
        orphan_fill_error(event);
        self.record_orphan_fill(event, stamps.received, bank);
        self.halt(HaltReason::OrphanFill, stamps.fold);
    }

    /// Each entry of the prior-run table holds a working order, so this one has nowhere to fold.
    /// The ledger would have to guess, so quoting halts with the table as it is, and the tape keeps
    /// the fill.
    #[cold]
    fn on_prior_run_overflow(&mut self, event: &ExecEvent, stamps: FoldStamps, bank: &mut Actions) {
        self.prior_run_overflow_error(event);
        self.record_orphan_fill(event, stamps.received, bank);
        self.halt(HaltReason::PriorRunOverflow, stamps.fold);
    }

    /// The fill of an own order that holds no slot, as the venue's running total.
    #[cold]
    fn record_orphan_fill(&mut self, event: &ExecEvent, received: LocalTs, bank: &mut Actions) {
        let Some(instrument) = event.instrument else { return };
        if event.cumulative_qty.mantissa() <= 0 {
            return;
        }
        let orphan_fill = IncidentRow {
            kind: IncidentKind::OrphanFill,
            instrument,
            client_id: Some(event.client_id),
            trade_id: event.trade_id,
            qty: event.cumulative_qty,
            amount: 0,
            window_close_ts_us: None,
            exchange_ts_us: event.exchange_ts_us,
            received_ts_us: received,
        };
        bank_incident(bank, &mut self.incidents, orphan_fill);
    }

    /// The quantity of a report moves by the venue's running totals, and its fee by the fee memory
    /// of its order. A step that the report's own trade does not explain, and a fee that the ledger
    /// cannot book, each leave a record.
    fn book_report(
        &mut self,
        slot: &OrderSlot,
        event: &ExecEvent,
        applied: Applied,
        ledger: &mut PositionLedger,
        bank: &mut Actions,
        received: LocalTs,
    ) {
        if !applied.fill.is_empty() {
            ledger.apply_fill(&LedgerFill {
                instrument: slot.instrument(),
                side: slot.side(),
                base: applied.fill.base,
                notional_quote: applied.fill.quote,
            });
        }
        let unexplained = applied.fee.unexplained;
        if unexplained.mantissa() > 0 {
            fee_gap_warning(slot, unexplained);
            let row = report_incident(IncidentKind::FeeGap, slot, event, received);
            bank_incident(
                bank,
                &mut self.incidents,
                IncidentRow {
                    qty: unexplained,
                    ..row
                },
            );
        }
        if event.commission == 0 {
            return;
        }
        let is_booked = match applied.fee.fold {
            FeeFold::Book => ledger
                .book_fee(slot.instrument(), self.fee_of(slot, event))
                .is_ok(),
            FeeFold::AlreadyBooked => return,
            FeeFold::Unbookable => false,
        };
        if is_booked {
            return;
        }
        unbooked_fee_warning(slot, event);
        let row = report_incident(IncidentKind::UnbookedFee, slot, event, received);
        let unbooked = IncidentRow {
            qty: event.last_qty,
            amount: event.commission,
            ..row
        };
        bank_incident(bank, &mut self.incidents, unbooked);
    }

    fn fee_of(&self, slot: &OrderSlot, event: &ExecEvent) -> FillFee {
        let instrument = &self.instruments[usize::from(slot.instrument().0)];
        let (asset, amount) = (event.commission_asset, event.commission);
        if asset == instrument.quote_asset {
            FillFee::Quote(Notional::from_mantissa(amount))
        } else if asset == instrument.base_asset {
            FillFee::Base {
                asset,
                qty: Qty::from_mantissa(amount),
            }
        } else {
            FillFee::Other { asset, amount }
        }
    }

    #[cold]
    fn prior_run_overflow_error(&self, event: &ExecEvent) {
        hot_error!(
            "prior-run order {:x} has nowhere to fold — all {} entries are still working, so quoting halts and its fill stays on the tape",
            event.client_id.0,
            self.prior_orders.capacity()
        );
    }

    #[cold]
    fn prior_run_without_instrument_error(&self, event: &ExecEvent) {
        hot_error!(
            "prior-run order {:x} names no instrument, so it has no slot and its fills cannot reach the ledger",
            event.client_id.0
        );
    }

    #[cold]
    fn resurrection_error(&self, event: &ExecEvent) {
        hot_error!(
            "order {:x} was believed closed and the venue still holds it — resurrected; an order believed dead is unbounded risk",
            event.client_id.0
        );
    }

    /// The release POLICY is a venue capability, [`ExecSettings::holds_reservations_until_settled`].
    ///
    /// A venue that locks funds on placement (Binance) folds the lock into every balance update, so
    /// the release waits for an update stamped later than the reservation — the watermark gate — on
    /// any slot past `PendingNew`, resting orders included. Ack alone would double-spend.
    ///
    /// A venue that does NOT lock (Polymarket) never moves balances for an open order, so `free`
    /// never subtracts a resting order: releasing while the slot still WORKS would over-admit against
    /// money it has committed, and a fill anywhere would then free a live order's reservation. There
    /// the reservation stands until the slot terminates — a zero-fill terminal moved no money and
    /// releases UNGATED; a filled terminal keeps the gate, because its post-fill restatement is what
    /// drops `free` and releasing before it lands would double-spend.
    fn release_reservation(&mut self, index: usize) {
        let slot = self.orders.slot(index);
        if slot.reserved_amount().is_none() {
            return;
        }
        let asset = self.instruments[usize::from(slot.instrument().0)].spend_asset(slot.side());
        if self.settings.holds_reservations_until_settled {
            if slot.state() != OrderState::PendingNew {
                self.gated_release(index, asset);
            }
            return;
        }
        if slot.state().is_working() {
            return;
        }
        if slot.filled_base().mantissa() == 0 {
            self.release_unsent_reservation(index);
            return;
        }
        self.gated_release(index, asset);
    }

    /// Release once the venue reports a balance stamped later than the reservation, else leave it
    /// held for the next spin to retry.
    fn gated_release(&mut self, index: usize, asset: AssetId) {
        let slot = self.orders.slot(index);
        let Some(amount) = slot.reserved_amount() else {
            return;
        };
        let outcome = self.account.release(asset, amount, slot.reserved_at());
        if outcome == ReleaseOutcome::Released {
            self.orders.clear_reservation(index);
        }
    }

    pub(super) fn release_unsent_reservation(&mut self, index: usize) {
        let slot = self.orders.slot(index);
        let Some(amount) = slot.reserved_amount() else {
            return;
        };
        let asset = self.instruments[usize::from(slot.instrument().0)].spend_asset(slot.side());
        self.account.release_unsent(asset, amount);
        self.orders.clear_reservation(index);
    }

    /// A venue answer ends the reject streak, extends it, or leaves it alone. `Refused` leaves it
    /// alone: a post-only quote the book moved through is the venue doing exactly what post-only
    /// asks, and counting it toward the kill switch parks the engine precisely when the market is
    /// moving.
    ///
    /// This relies on the Binance edge mapping `INSUFFICIENT_BALANCES` to `Fatal` and not to
    /// `Refused`: the two share venue code `-2010` and only the reject REASON separates them. Both
    /// Binance decoders split on the message, and an unrecognised `-2010` classifies `Fatal`, which
    /// parks the engine loudly rather than letting it place orders it cannot fund.
    fn record_outcome(&mut self, event: &ExecEvent, state: OrderState, now: LocalTs) {
        let Some(class) = event.reject else {
            // One order the venue accepts shows the requests are not wrong in kind, so it ends the
            // streak, and only the venue can do that. The kinds the EDGE synthesises for a request
            // that never left resolve a slot without the venue accepting anything, and an amend
            // refused for want of a socket leaves its order reading `Live`. Read here as acceptance,
            // that would reset the count exactly when the venue is least reachable.
            let is_venue_answer =
                !matches!(event.kind, ExecKind::PlaceNotSent | ExecKind::AmendNotSent);
            if is_venue_answer && matches!(state, OrderState::Live) {
                self.reject_streak = 0;
            }
            return;
        };
        match class {
            RejectClass::Fatal => {
                self.fatal_reject_error(event);
                self.halt(HaltReason::FatalReject, now);
            }
            // The ordinary cost of quoting post-only. It must never touch the hard-reject streak: a
            // maker earns these all day, and counting them parks the engine exactly when the market
            // is interesting.
            RejectClass::Refused => {}
            // A reconciliation answering that the order never existed is not the venue refusing a
            // request, so it must not count toward the kill switch.
            RejectClass::Gone => {}
            RejectClass::StillLive | RejectClass::Ambiguous => {
                self.reject_streak += 1;
                if self.reject_streak >= self.settings.max_consecutive_rejects {
                    self.halt(HaltReason::RejectStreak, now);
                }
            }
        }
    }

    /// One class covers several causes, so the line carries the venue's code.
    #[cold]
    fn fatal_reject_error(&self, event: &ExecEvent) {
        hot_error!(
            "venue refused order {:x} fatally with code {} — halting rather than retrying",
            event.client_id.0,
            event.reject_code
        );
    }
}

/// The incident of one report of an order, with no quantity and no amount yet.
fn report_incident(
    kind: IncidentKind,
    slot: &OrderSlot,
    event: &ExecEvent,
    received_ts_us: LocalTs,
) -> IncidentRow {
    IncidentRow {
        kind,
        instrument: slot.instrument(),
        client_id: Some(slot.client_id()),
        trade_id: event.trade_id,
        qty: Qty::from_mantissa(0),
        amount: 0,
        window_close_ts_us: None,
        exchange_ts_us: event.exchange_ts_us,
        received_ts_us,
    }
}

#[cold]
fn orphan_warning(event: &ExecEvent) {
    hot_warn!(
        "execution event {:?} for client id {:x} matches no slot and carries no fill — refused and counted",
        event.kind,
        event.client_id.0
    );
}

#[cold]
fn orphan_fill_error(event: &ExecEvent) {
    hot_error!(
        "order {:x} filled {} and no slot holds it — reaped too early, or a slot leaked; the ledger does not book the fill, and quoting halts",
        event.client_id.0,
        event.cumulative_qty.mantissa()
    );
}

#[cold]
fn fee_gap_warning(slot: &OrderSlot, unexplained: Qty) {
    hot_warn!(
        "order {:x} filled {} more than its report's own trade explains, so the fees of the trades in between are unknown — recorded as a fee gap",
        slot.client_id().0,
        unexplained.mantissa()
    );
}

#[cold]
fn unbooked_fee_warning(slot: &OrderSlot, event: &ExecEvent) {
    hot_warn!(
        "a fee of {} in asset {} on order {:x} is not booked: its asset has no row, or its trade cannot be told from one booked before — recorded with its amount",
        event.commission,
        event.commission_asset.0,
        slot.client_id().0
    );
}

/// A reconciliation retiring a working order is the engine LOSING one, not routine housekeeping: the
/// venue was asked what it holds and did not mention an order this table believed in. It is logged
/// as a warning because otherwise a duplicate resting on the venue is the first anyone hears of it.
#[cold]
fn swept_warning(slot: &OrderSlot, recon_seq: u64) {
    hot_warn!(
        "reconciliation pass {} did not name order {:x} on instrument {} — closing it as gone",
        recon_seq,
        slot.client_id().0,
        slot.instrument().0
    );
}
