//! The engine itself: construction, the readers a strategy sees, and the entry point every venue
//! event folds through. Apply is ungated — a fill during warmup is money — and only the callback
//! a strategy receives is gated.

use crate::config::MAX_QUOTE_LEVELS;
use crate::hot::ledger::PositionLedger;
use crate::hot::strategy::Actions;
use crate::ids::{AssetId, ClientOrderId, InstrumentId, Qty, Side};
use crate::msg::exec::{AccountChunk, CancelReason, ExecCommand, ExecEvent, ExecKind, Provenance};
use crate::msg::persist::OrderTransition;
use crate::registry::InstrumentRow;
use crate::time::{DurationUs, TsUs};
use crate::{error, warn};

use super::account::{AccountTable, Balance};
use super::audit::bank_engine_driven;
use super::budget::{BudgetMeter, OrderBudget};
use super::command::{ExecSink, PendingCommands};
use super::desired::QuoteLevel;
use super::event::PriorRunOrders;
use super::flatten::FeeModel;
use super::gates::{ExecHalt, HaltReason, Readiness, RefusalLatch};
use super::order::{OrderSlot, OrderTable, ReconcilePass};
use super::reconcile::{ExecLimits, RejectReason, RestingOrder, TickGrid};
use super::view::{ExecCallback, OrderReject, OrderView, WorkingOrderView};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecSettings {
    pub limits: ExecLimits,
    pub max_orders_per_side: usize,
    pub min_base_balance: i64,
    pub min_quote_balance: i64,
    pub max_consecutive_rejects: u32,
    pub max_session_loss_quote: i64,
    pub inflight_timeout: DurationUs,
    pub exec_silence_spins: u32,
    pub order_reap_window: DurationUs,
    // How much margin before a closing window the engine stops quoting. Inert on venues
    // whose instruments never rotate.
    pub quote_stop_margin: DurationUs,
    pub flatten_slack_ticks: u32,
    // A venue capability, not an operator tuning knob: how many placements the venue
    // grants, and over what windows.
    pub order_budget: OrderBudget,
    // A venue capability, not an operator tuning knob: which curve, if any, the venue
    // charges a taker by.
    pub fee_model: FeeModel,
    // The taker fee rate, as a 1e-8 mantissa, charged by whichever curve fee_model names.
    pub taker_fee_rate: i64,
    // A venue capability, not an operator tuning knob: does the venue lock funds into a
    // balance update the moment an order is placed? On Binance (true), the reservation is
    // released once a later balance update is stamped past it (a watermark gate). On
    // Polymarket (false), a resting order never touches the venue balance at all, so the
    // reservation is instead held until the trade itself settles. A zero-fill terminal
    // release on a non-locking venue skips that gate entirely, which risks over-admitting.
    pub holds_reservations_until_settled: bool,
}

impl ExecSettings {
    pub fn disabled() -> Self {
        Self {
            limits: ExecLimits::disabled(),
            // Kept non-zero to avoid halting on inherited state; the zero_alloc fitness
            // test measures this.
            max_orders_per_side: MAX_QUOTE_LEVELS,
            min_base_balance: 0,
            min_quote_balance: 0,
            max_consecutive_rejects: 0,
            max_session_loss_quote: 0,
            inflight_timeout: DurationUs::ZERO,
            exec_silence_spins: 0,
            order_reap_window: DurationUs::ZERO,
            quote_stop_margin: DurationUs::ZERO,
            flatten_slack_ticks: 0,
            order_budget: OrderBudget::NONE,
            fee_model: FeeModel::None,
            taker_fee_rate: 0,
            // The historical universal behaviour; inert with no orders, but the least surprising
            // value for a reader who reaches a disabled engine.
            holds_reservations_until_settled: true,
        }
    }
}

pub(super) struct InstrumentExec {
    pub(super) grid: TickGrid,
    pub(super) base_asset: AssetId,
    pub(super) quote_asset: AssetId,
    pub(super) max_exposure_quote: i64,
    pub(super) last_commit_ts_us: TsUs,
    pub(super) is_prior_run_cancel_sent: bool,
    /// What the flatten pass last refused for, so a position it cannot shed is reported when the
    /// answer CHANGES rather than on every spin — same edge-triggering as [`RefusalLatch`].
    pub(super) flatten_refusal: Option<RejectReason>,
    /// Realised result of windows this instrument has already retired. The ledger row goes with the
    /// window it belonged to; the kill switch measures the RUN.
    pub(super) realised_carried_quote: i64,
}

impl InstrumentExec {
    /// The spot rule: a buy spends quote, and a sell reserves the base inventory it delivers.
    #[inline]
    pub(super) fn spend_asset(&self, side: Side) -> AssetId {
        match side {
            Side::Buy => self.quote_asset,
            Side::Sell => self.base_asset,
        }
    }
}

pub struct ExecEngineSetup<'a> {
    pub instruments: &'a [InstrumentRow],
    pub run_nonce: u32,
    pub settings: ExecSettings,
    pub sink: Option<ExecSink>,
}

pub struct ExecEngine {
    pub(super) orders: OrderTable,
    pub(super) prior_orders: PriorRunOrders,
    pub(super) account: AccountTable,
    pub(super) pending: PendingCommands,
    pub(super) sink: Option<ExecSink>,
    pub(super) halt: ExecHalt,
    pub(super) readiness: Readiness,
    pub(super) refusals: RefusalLatch,
    pub(super) reject_streak: u32,
    pub(super) budget: BudgetMeter,
    pub(super) instruments: Vec<InstrumentExec>,
    pub(super) settings: ExecSettings,
    pub(super) spins_since_exec_event: u32,
    pub(super) recon_seq: u64,
    /// The PnL baseline at the first honest valuation; the session loss gate compares
    /// against this. `None` until that first mark.
    pub(super) pnl_at_baseline: Vec<Option<i64>>,
}

impl ExecEngine {
    pub fn new(setup: ExecEngineSetup<'_>) -> Self {
        assert!(
            setup.sink.is_none()
                || (1..=MAX_QUOTE_LEVELS).contains(&setup.settings.max_orders_per_side),
            "max_orders_per_side must be within 1..={MAX_QUOTE_LEVELS} when execution is wired"
        );
        let instruments = setup
            .instruments
            .iter()
            .map(|row| InstrumentExec {
                grid: TickGrid {
                    tick: row.tick_size.map_or(1, |tick| tick.0.max(1)),
                    step: row.lot_size.map_or(1, |lot| lot.0.max(1)),
                    min_qty: row.min_qty.unwrap_or(Qty(0)),
                    min_notional: row.min_notional.unwrap_or(0),
                    // When the venue declares no amend filter, a shrink is executed as a
                    // cancel followed by a place instead.
                    max_amends: row
                        .max_num_order_amends
                        .map_or(0, |amends| u8::try_from(amends).unwrap_or(u8::MAX)),
                    max_price: row.max_price,
                },
                base_asset: row.base_asset,
                quote_asset: row.quote_asset,
                max_exposure_quote: row.max_exposure_quote,
                last_commit_ts_us: TsUs::from_micros(0),
                is_prior_run_cancel_sent: false,
                flatten_refusal: None,
                realised_carried_quote: 0,
            })
            .collect();
        Self {
            orders: OrderTable::new(setup.run_nonce),
            prior_orders: PriorRunOrders::new(),
            account: AccountTable::new(),
            pending: PendingCommands::new(),
            sink: setup.sink,
            halt: ExecHalt::Armed,
            readiness: Readiness::default(),
            refusals: RefusalLatch::default(),
            reject_streak: 0,
            budget: BudgetMeter::new(setup.settings.order_budget),
            instruments,
            settings: setup.settings,
            spins_since_exec_event: 0,
            recon_seq: 0,
            pnl_at_baseline: vec![None; setup.instruments.len()],
        }
    }

    #[inline]
    pub(crate) fn commands_banked(&self) -> u64 {
        self.pending.banked_total()
    }

    pub fn drain_refusals(&mut self, sink: &mut impl FnMut(&OrderReject)) {
        self.refusals.drain_into(sink);
    }

    /// `None` when no sink is wired: `Armed` would otherwise read as "armed to trade" on a UI that
    /// could not send an order if it wanted to.
    #[inline]
    pub fn halt_state(&self) -> Option<ExecHalt> {
        self.sink.is_some().then_some(self.halt)
    }

    pub fn balance(&self, asset: AssetId) -> Balance {
        self.account.balance(asset)
    }

    /// Every asset the venue has named. Absolute state, so a caller may re-state it as often as it
    /// likes — see [`AccountTable::balances`].
    pub fn balances(&self) -> impl Iterator<Item = (AssetId, Balance)> + '_ {
        self.account.balances()
    }

    pub fn tick_grid(&self, instrument: InstrumentId) -> TickGrid {
        self.instruments[usize::from(instrument.0)].grid
    }

    /// The working order occupying one stable ladder level, as a strategy sees it.
    pub fn resting(
        &self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
    ) -> Option<OrderView> {
        self.orders.resting(instrument, side, level).map(view_of)
    }

    pub fn order(&self, id: ClientOrderId) -> Option<OrderView> {
        self.orders
            .find(id)
            .map(|index| view_of(self.orders.slot(index)))
    }

    /// Every order whose absence from the venue has not been proved, current run first and then
    /// inherited prior-run exposure. This is the authoritative source for the UI's atomic side
    /// snapshot; terminal rows are deliberately absent so an empty cut clears a stale UI side.
    pub fn working_orders(
        &self,
        instrument: InstrumentId,
        side: Side,
    ) -> impl Iterator<Item = WorkingOrderView> + '_ {
        let current = self
            .orders
            .side_slots(instrument, side)
            .iter()
            .filter(|slot| slot.state.is_working())
            .map(|slot| working_view_of(slot, Some(slot.level)));
        let prior = self
            .prior_orders
            .working(instrument, side)
            .map(|slot| working_view_of(slot, None));
        current.chain(prior)
    }

    /// The staleness gate keys on this commit stamp, never on a clock read.
    #[inline]
    pub fn on_book_commit(&mut self, instrument: InstrumentId, at: TsUs) {
        self.instruments[usize::from(instrument.0)].last_commit_ts_us = at;
    }

    /// Folds one venue event and answers which callback it earned. See the module header for why
    /// this applies state whether or not the strategy is live.
    pub(crate) fn on_exec_event(
        &mut self,
        event: &ExecEvent,
        ledger: &mut PositionLedger,
        bank: &mut Actions,
    ) -> ExecCallback {
        self.spins_since_exec_event = 0;
        match event.kind {
            ExecKind::StreamReset => return self.on_stream_reset(event.received_ts_us, bank),
            ExecKind::StreamReady => {
                self.readiness.observe_stream_ready();
                return ExecCallback::None;
            }
            ExecKind::SnapshotEnd => return self.on_snapshot_end(event, bank),
            _ => {}
        }
        match event.provenance {
            Provenance::Foreign => return ExecCallback::None,
            Provenance::PriorRun => {
                self.cancel_prior_run(event);
                self.apply_to_prior_run(event, ledger, bank);
                return ExecCallback::None;
            }
            Provenance::Mine => {}
        }
        let Some(index) = self.locate(event) else {
            return ExecCallback::None;
        };
        self.apply_to_slot(index, event, ledger, bank)
    }

    /// Absolute balances only — see [`AccountChunk`] for why a delta may never reach here.
    pub fn on_account(&mut self, chunk: &AccountChunk) {
        self.account.apply(chunk);
        self.retry_reservation_releases();
    }

    /// Pulls every order this run owns, on every instrument. [`ExecEngine::halt`] is the only caller
    /// — process exit sweeps through the edge's own exit plan instead — so this is the in-engine
    /// half of the same promise: an order outliving the engine that placed it is the one failure
    /// with no upper bound on its cost.
    #[cold]
    pub(super) fn cancel_all(&mut self, reason: CancelReason) {
        for index in 0..self.instruments.len() {
            let instrument = InstrumentId(index as u16);
            let command = ExecCommand::CancelOurs { instrument, reason };
            if !self.pending.bank(command) {
                unswept_error(instrument);
            }
        }
    }

    /// Halting stops quoting only; it never touches run state, recording markers only. It
    /// is one-way — only a restart rearms it.
    #[cold]
    pub fn halt(&mut self, reason: HaltReason, at: TsUs) {
        if self.halt.is_halted() {
            return;
        }
        error!("execution halted: {reason:?} — cancelling every order this run owns");
        self.halt = ExecHalt::Halted {
            reason,
            halted_ts_us: at,
        };
        self.cancel_all(CancelReason::Halt);
    }

    #[cold]
    fn on_stream_reset(&mut self, at: TsUs, bank: &mut Actions) -> ExecCallback {
        warn!("execution stream reset — every open order is unknown until a reconciliation lands");
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
    fn on_snapshot_end(&mut self, event: &ExecEvent, bank: &mut Actions) -> ExecCallback {
        let pass = ReconcilePass {
            instrument: event.instrument,
            recon_seq: event.recon_seq,
            recon_ts_us: event.received_ts_us,
        };
        self.orders.sweep_unseen(pass, &mut |slot, previous| {
            swept_warning(slot, pass.recon_seq);
            let transition = OrderTransition::SweepClosed;
            bank_engine_driven(bank, slot, transition, previous.into(), pass.recon_ts_us);
        });
        self.readiness.observe_open_orders_snapshot();
        ExecCallback::None
    }

    /// Finds the slot an event belongs to, adopting one the venue knows about that this
    /// table does not.
    fn locate(&mut self, event: &ExecEvent) -> Option<usize> {
        if let Some(index) = self.orders.find(event.client_id) {
            return Some(index);
        }
        if event.kind == ExecKind::SnapshotOrder
            && let Some(index) = self.orders.adopt(event)
        {
            return Some(index);
        }
        // Ours by tag and nonce, but no slot claims it: the reap window is too short, or the state
        // machine leaked. Either way it is a bug, and a silent one costs a fill.
        self.orphan_error(event);
        None
    }

    #[cold]
    fn orphan_error(&self, event: &ExecEvent) {
        error!(
            "execution event {:?} for client id {:x} matches no slot — reaped too early, or a state-machine leak",
            event.kind, event.client_id.0
        );
    }

    /// The latch records that the cancel WENT OUT, so it is set after the bank accepts it and never
    /// before. A bank that refused leaves the instrument unlatched and the next venue report about
    /// the inherited order asks again — latching first would make one full bank the last word on an
    /// order still resting on the venue.
    #[cold]
    fn cancel_prior_run(&mut self, event: &ExecEvent) {
        let index = usize::from(event.instrument.0);
        if self.instruments[index].is_prior_run_cancel_sent {
            return;
        }
        let command = ExecCommand::CancelPriorRun {
            instrument: event.instrument,
        };
        if !self.bank_or_halt(command, event.received_ts_us) {
            return;
        }
        self.instruments[index].is_prior_run_cancel_sent = true;
        warn!(
            "an earlier run of this trading engine left orders resting on instrument {} — cancelling them",
            event.instrument.0
        );
    }
}

/// The bank is full on the one path that must reach the venue, and the halt that brought us here has
/// already fired, so there is nothing left to escalate to. Said out loud because the orders are
/// still on the venue and somebody has to go and pull them.
#[cold]
fn unswept_error(instrument: InstrumentId) {
    error!(
        "the command bank is full, so the sweep of every order this run owns on instrument {} could not be sent — they may still be resting on the venue",
        instrument.0
    );
}

/// A reconciliation retiring a working order is the engine LOSING one, not routine housekeeping: the
/// venue was asked what it holds and did not mention an order this table believed in. It is logged
/// as a warning because otherwise a duplicate resting on the venue is the first anyone hears of it.
#[cold]
fn swept_warning(slot: &OrderSlot, recon_seq: u64) {
    warn!(
        "reconciliation pass {recon_seq} did not name order {:x} on instrument {} — closing it as gone",
        slot.client_id.0, slot.instrument.0
    );
}

#[inline]
pub(super) fn view_of(slot: &OrderSlot) -> OrderView {
    OrderView {
        client_id: slot.client_id,
        venue_order_id: slot.venue_order_id,
        instrument: slot.instrument,
        side: slot.side,
        level: slot.level,
        state: slot.state,
        price: slot.price,
        qty: slot.qty,
        filled: slot.filled_base,
    }
}

#[inline]
fn working_view_of(slot: &OrderSlot, level: Option<QuoteLevel>) -> WorkingOrderView {
    WorkingOrderView {
        client_id: slot.client_id,
        instrument: slot.instrument,
        side: slot.side,
        level,
        state: slot.state,
        price: slot.price,
        qty: slot.qty,
        filled: slot.filled_base,
    }
}

#[inline]
pub(super) fn resting_of(slot: &OrderSlot) -> RestingOrder {
    RestingOrder {
        price: slot.price,
        qty: slot.qty,
        filled: slot.filled_base,
        amends_used: slot.amends_used,
    }
}
