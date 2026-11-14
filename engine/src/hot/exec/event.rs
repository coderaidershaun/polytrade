//! Folding a venue event: a stream reset, the end of an open-orders snapshot, or the transition of
//! an order of this run or an earlier one, with its money, reservation release and audit rows.

use crate::hot::ledger::PositionLedger;
use crate::hot::strategy::Actions;
use crate::log::{hot_error, hot_warn};
use crate::msg::exec::{ExecEvent, ExecKind, RejectClass};
use crate::msg::ids::AssetId;
use crate::msg::persist::{OrderLifecycle, OrderTransition};
use crate::msg::time::LocalTs;

use super::account::ReleaseOutcome;
use super::audit::{bank_engine_driven, bank_event_rows};
use super::engine::{ExecEngine, FoldStamps, view_of};
use super::gates::HaltReason;
use super::order::{OrderSlot, OrderState, PriorRunSeat, ReconcilePass, Unseated};
use super::view::{ExecCallback, Fill, OrderReject, RejectOrigin};

impl ExecEngine {
    #[cold]
    /// A stream reset of one venue leaves the orders, the readiness and the refusals of each other
    /// venue as they were.
    pub(super) fn on_stream_reset(
        &mut self,
        venue: usize,
        at: LocalTs,
        bank: &mut Actions,
    ) -> ExecCallback {
        hot_warn!(
            "execution stream reset — every open order is unknown until a reconciliation lands"
        );
        self.venues[venue].readiness.observe_stream_reset();
        // Every reason the pass last reported was derived from order state that is now a memory, so
        // the next refusal is news again rather than a repeat the latch would swallow.
        let (instruments, slots) = {
            let state = &self.venues[venue];
            (state.instruments.clone(), state.slots.clone())
        };
        self.refusals.forget_instruments(instruments);
        self.orders.invalidate_within(slots, &mut |slot, previous| {
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
        let mut has_swept = false;
        self.orders.sweep_unseen(pass, &mut |slot, previous| {
            has_swept = true;
            swept_warning(slot, pass.recon_seq);
            let transition = OrderTransition::SweepClosed;
            bank_engine_driven(bank, slot, transition, previous.into(), stamps.received);
        });
        let venue = self.venue_of(instrument);
        if has_swept {
            self.venues[venue]
                .silence
                .observe_read_change(pass.recon_seq);
        }
        self.venues[venue].readiness.observe_open_orders_snapshot();
        self.end_silence_read(venue, pass.recon_seq, stamps.fold);
        ExecCallback::None
    }

    /// Retries each release that beat its account update. A free slot holds no reservation, so the
    /// pass walks the slots in use.
    pub(super) fn retry_reservation_releases(&mut self, venue: usize) {
        let slots = self.venues[venue].slots.clone();
        let mut next = self.orders.next_in_use(slots.start);
        while let Some(index) = next.filter(|index| slots.contains(index)) {
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
    /// callback. Its fills after its first report still move the ledger.
    #[cold]
    pub(super) fn apply_to_prior_run(
        &mut self,
        event: &ExecEvent,
        stamps: FoldStamps,
        ledger: &mut PositionLedger,
        bank: &mut Actions,
    ) {
        let now = stamps.fold;
        let PriorRunSeat { index, unbooked } = match self.prior_orders.find_or_seat(event, now) {
            Ok(seat) => seat,
            Err(Unseated::TableFull) => return self.on_prior_run_overflow(event, stamps, bank),
            Err(Unseated::NoInstrument) => {
                return self.on_prior_run_without_instrument(event, stamps, bank);
            }
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
        if unbooked.mantissa() > 0 {
            self.on_unbooked_prior_run_total(event, stamps, bank);
        }
    }

    /// The total that an order of an earlier run shows at its first report can hold fills that the
    /// earlier run booked, which the restored position holds, and the fold cannot tell which part.
    /// So the ledger books none of it, the record keeps it for a person to book, and quoting halts.
    #[cold]
    fn on_unbooked_prior_run_total(
        &mut self,
        event: &ExecEvent,
        stamps: FoldStamps,
        bank: &mut Actions,
    ) {
        unbooked_prior_run_total_error(event);
        self.record_orphan_fill(event, stamps.received, bank);
        self.halt(HaltReason::PriorRunFillUnbooked, stamps.fold);
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

    /// With a fill and no instrument, the order has no entry and the fill no record, so quoting
    /// halts as for each other fill that no slot holds.
    #[cold]
    fn on_prior_run_without_instrument(
        &mut self,
        event: &ExecEvent,
        stamps: FoldStamps,
        bank: &mut Actions,
    ) {
        self.prior_run_without_instrument_error(event);
        if event.cumulative_qty.mantissa() > 0 {
            self.record_orphan_fill(event, stamps.received, bank);
            self.halt(HaltReason::OrphanFill, stamps.fold);
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
        if self
            .settings_of(slot.instrument())
            .holds_reservations_until_settled
        {
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
        let reserved_at = slot.reserved_at();
        let outcome = self
            .venue_mut(slot.instrument())
            .account
            .release(asset, amount, reserved_at);
        if outcome == ReleaseOutcome::Released {
            self.orders.clear_reservation(index);
        }
    }

    pub(super) fn release_unsent_reservation(&mut self, index: usize) {
        let slot = self.orders.slot(index);
        let Some(amount) = slot.reserved_amount() else {
            return;
        };
        let instrument = slot.instrument();
        let asset = self.instruments[usize::from(instrument.0)].spend_asset(slot.side());
        self.venue_mut(instrument)
            .account
            .release_unsent(asset, amount);
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
                self.venues[usize::from(event.venue.0)].reject_streak = 0;
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
                let venue = &mut self.venues[usize::from(event.venue.0)];
                venue.reject_streak += 1;
                if venue.reject_streak >= venue.settings.max_consecutive_rejects {
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

#[cold]
fn unbooked_prior_run_total_error(event: &ExecEvent) {
    hot_error!(
        "prior-run order {:x} shows {} filled at its first report, and the earlier run may have booked any part of it — the ledger books none of it, a record keeps it, and quoting halts",
        event.client_id.0,
        event.cumulative_qty.mantissa()
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
