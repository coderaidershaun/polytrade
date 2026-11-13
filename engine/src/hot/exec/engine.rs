//! The execution engine: its construction, what a strategy reads of it, and the fold of each venue
//! event. A fill always moves money, warmup or not, and only the strategy's callback waits.

use crate::hot::digest::{digest_by_hash, digest_fields};
use crate::hot::ledger::PositionLedger;
use crate::hot::sink::{ExecSink, PendingCommands};
use crate::hot::strategy::Actions;
use crate::log::{hot_error, hot_warn};
use crate::msg::exec::{AccountChunk, CancelReason, ExecCommand, ExecEvent, ExecKind, Provenance};
use crate::msg::ids::{AssetId, ClientOrderId, InstrumentId, Notional, Qty, Side};
use crate::msg::time::{DurationUs, FoldTime, LocalTs};
use crate::runtime::config::MAX_QUOTE_LEVELS;
use crate::runtime::registry::InstrumentRow;

use super::account::{AccountTable, Balance};
use super::audit::IncidentCounts;
use super::budget::{BudgetMeter, OrderBudget};
use super::capacity::ExecCapacity;
use super::desired::QuoteLevel;
use super::flatten::FeeModel;
use super::gates::{ExecHalt, HaltReason, Readiness, RefusalLatch};
use super::order::{OrderSlot, OrderTable, PriorRunOrders};
use super::reconcile::{ExecLimits, RejectReason, RestingOrder, TickGrid};
use super::view::{ExecCallback, OrderReject, OrderView, WorkingOrderView};

/// The two times of the message under fold. The fold time stamps what the engine decides: a slot, a
/// deadline, a halt, and the bank stamp of a command. The received stamp of the message stamps each
/// row that it writes and each value that it hands the strategy, because the tape keeps the raw
/// stamp of each message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FoldStamps {
    pub(crate) fold: LocalTs,
    pub(crate) received: LocalTs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecSettings {
    pub limits: ExecLimits,
    pub max_orders_per_side: usize,
    pub min_base_balance: Qty,
    pub min_quote_balance: Notional,
    pub max_consecutive_rejects: u32,
    pub max_session_loss_quote: Notional,
    pub inflight_timeout: DurationUs,
    /// Probes of one unanswered order before the read of open orders, then the halt.
    pub max_status_probes: u8,
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
            // Kept non-zero, so an engine with no venue never halts on the state it inherits.
            max_orders_per_side: MAX_QUOTE_LEVELS,
            min_base_balance: Qty::from_mantissa(0),
            min_quote_balance: Notional::ZERO,
            max_consecutive_rejects: 0,
            max_session_loss_quote: Notional::ZERO,
            inflight_timeout: DurationUs::ZERO,
            // The most that the key allows, so an engine with no limits halts on probes last.
            max_status_probes: u8::MAX,
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
    pub(super) max_exposure_quote: Notional,
    pub(super) last_commit_ts_us: LocalTs,
    pub(super) is_prior_run_cancel_sent: bool,
    /// What the flatten pass last refused for, so a position it cannot shed is reported when the
    /// answer CHANGES rather than on every spin — same edge-triggering as [`RefusalLatch`].
    pub(super) flatten_refusal: Option<RejectReason>,
    /// Realised result of windows this instrument has already retired. The ledger row goes with the
    /// window it belonged to; the kill switch measures the RUN.
    pub(super) realised_carried_quote: Notional,
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
    pub(super) incidents: IncidentCounts,
    /// The PnL baseline at the first honest valuation; the session loss gate compares
    /// against this. `None` until that first mark.
    pub(super) pnl_at_baseline: Vec<Option<Notional>>,
}

digest_by_hash!(ExecSettings);
digest_fields! {
    InstrumentExec {
        grid, base_asset, quote_asset, max_exposure_quote, last_commit_ts_us,
        is_prior_run_cancel_sent, flatten_refusal, realised_carried_quote,
    }
}
digest_fields! {
    ExecEngine {
        orders, prior_orders, account, pending, sink, halt, readiness, refusals, reject_streak,
        budget, instruments, settings, spins_since_exec_event, recon_seq, incidents,
        pnl_at_baseline,
    }
}

impl ExecEngine {
    pub fn new(setup: ExecEngineSetup<'_>) -> Self {
        assert!(
            setup.sink.is_none()
                || (1..=MAX_QUOTE_LEVELS).contains(&setup.settings.max_orders_per_side),
            "max_orders_per_side must be within 1..={MAX_QUOTE_LEVELS} when execution is wired"
        );
        let capacity = ExecCapacity::sized_for(setup.instruments);
        let instruments = setup
            .instruments
            .iter()
            .map(|row| InstrumentExec {
                grid: TickGrid {
                    tick: row.tick_size.map_or(1, |tick| tick.mantissa().max(1)),
                    step: row.lot_size.map_or(1, |lot| lot.mantissa().max(1)),
                    min_qty: row.min_qty.unwrap_or(Qty::from_mantissa(0)),
                    min_notional: row.min_notional.unwrap_or(Notional::ZERO),
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
                last_commit_ts_us: LocalTs::from_micros(0),
                is_prior_run_cancel_sent: false,
                flatten_refusal: None,
                realised_carried_quote: Notional::ZERO,
            })
            .collect();
        Self {
            orders: OrderTable::new(&capacity, setup.run_nonce),
            prior_orders: PriorRunOrders::new(&capacity),
            account: AccountTable::new(&capacity),
            pending: PendingCommands::for_sink(setup.sink.as_ref(), capacity.command_bank()),
            sink: setup.sink,
            halt: ExecHalt::Armed,
            readiness: Readiness::default(),
            refusals: RefusalLatch::new(&capacity),
            reject_streak: 0,
            budget: BudgetMeter::new(setup.settings.order_budget),
            instruments,
            settings: setup.settings,
            spins_since_exec_event: 0,
            recon_seq: 0,
            incidents: IncidentCounts::default(),
            pnl_at_baseline: vec![None; setup.instruments.len()],
        }
    }

    #[inline]
    pub(crate) fn commands_banked(&self) -> u64 {
        self.pending.banked_total()
    }

    #[inline]
    pub(crate) fn incident_counts(&self) -> IncidentCounts {
        self.incidents
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
            .side_in_use(instrument, side)
            .filter(|slot| slot.state().is_working())
            .map(|slot| working_view_of(slot, Some(slot.level())));
        let prior = self
            .prior_orders
            .working(instrument, side)
            .map(|slot| working_view_of(slot, None));
        current.chain(prior)
    }

    /// The staleness gate keys on the fold time of this commit, never on a clock read.
    #[inline]
    pub fn on_book_commit(&mut self, instrument: InstrumentId, at: FoldTime) {
        self.instruments[usize::from(instrument.0)].last_commit_ts_us = at.as_local();
    }

    /// Folds one venue event and answers which callback it earned. See the module header for why
    /// this applies state whether or not the strategy is live.
    pub(crate) fn on_exec_event(
        &mut self,
        event: &ExecEvent,
        stamps: FoldStamps,
        ledger: &mut PositionLedger,
        bank: &mut Actions,
    ) -> ExecCallback {
        let now = stamps.fold;
        self.spins_since_exec_event = 0;
        match event.kind {
            ExecKind::StreamReset => return self.on_stream_reset(stamps.received, bank),
            ExecKind::StreamReady => {
                self.readiness.observe_stream_ready();
                return ExecCallback::None;
            }
            ExecKind::SnapshotEnd => return self.on_snapshot_end(event, stamps, bank),
            _ => {}
        }
        match event.provenance {
            Provenance::Foreign => return ExecCallback::None,
            Provenance::PriorRun => {
                self.cancel_prior_run(event, now);
                self.apply_to_prior_run(event, stamps, ledger, bank);
                return ExecCallback::None;
            }
            Provenance::Mine => {}
        }
        let Some(index) = self.locate(event, stamps, bank) else {
            return ExecCallback::None;
        };
        self.apply_to_slot(index, event, stamps, ledger, bank)
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
    pub(super) fn cancel_all(&mut self, reason: CancelReason, at: LocalTs) {
        for index in 0..self.instruments.len() {
            let instrument = InstrumentId(index as u16);
            let command = ExecCommand::CancelOurs { instrument, reason };
            if !self.pending.bank(command, at) {
                unswept_error(instrument);
            }
        }
    }

    /// Halting stops quoting only; it never touches run state, recording markers only. It
    /// is one-way — only a restart rearms it.
    #[cold]
    pub fn halt(&mut self, reason: HaltReason, at: LocalTs) {
        if self.halt.is_halted() {
            return;
        }
        hot_error!(
            "execution halted: {:?} — cancelling every order this run owns",
            reason
        );
        self.halt = ExecHalt::Halted {
            reason,
            halted_ts_us: at,
        };
        self.cancel_all(CancelReason::Halt, at);
    }

    /// Finds the slot an event belongs to, adopting one the venue knows about that this
    /// table does not.
    fn locate(
        &mut self,
        event: &ExecEvent,
        stamps: FoldStamps,
        bank: &mut Actions,
    ) -> Option<usize> {
        if let Some(index) = self.orders.find(event.client_id) {
            return Some(index);
        }
        if event.kind == ExecKind::SnapshotOrder
            && let Some(index) = self.orders.adopt(event, stamps.fold)
        {
            return Some(index);
        }
        self.on_orphan(event, stamps, bank);
        None
    }

    /// The latch records that the cancel WENT OUT, so it is set after the bank accepts it and never
    /// before. A bank that refused leaves the instrument unlatched and the next venue report about
    /// the inherited order asks again — latching first would make one full bank the last word on an
    /// order still resting on the venue.
    #[cold]
    fn cancel_prior_run(&mut self, event: &ExecEvent, now: LocalTs) {
        let Some(instrument) = event.instrument else {
            return;
        };
        let index = usize::from(instrument.0);
        if self.instruments[index].is_prior_run_cancel_sent {
            return;
        }
        let command = ExecCommand::CancelPriorRun { instrument };
        if !self.bank_or_halt(command, now) {
            return;
        }
        self.instruments[index].is_prior_run_cancel_sent = true;
        hot_warn!(
            "an earlier run of this trading engine left orders resting on instrument {} — cancelling them",
            instrument.0
        );
    }
}

/// The bank is full on the one path that must reach the venue, and the halt that brought us here has
/// already fired, so there is nothing left to escalate to. Said out loud because the orders are
/// still on the venue and somebody has to go and pull them.
#[cold]
fn unswept_error(instrument: InstrumentId) {
    hot_error!(
        "the command bank is full, so the sweep of every order this run owns on instrument {} could not be sent — they may still be resting on the venue",
        instrument.0
    );
}

#[inline]
pub(super) fn view_of(slot: &OrderSlot) -> OrderView {
    OrderView {
        client_id: slot.client_id(),
        venue_order_id: slot.venue_order_id(),
        instrument: slot.instrument(),
        side: slot.side(),
        level: slot.level(),
        state: slot.state(),
        price: slot.price(),
        qty: slot.qty(),
        filled: slot.filled_base(),
    }
}

#[inline]
fn working_view_of(slot: &OrderSlot, level: Option<QuoteLevel>) -> WorkingOrderView {
    WorkingOrderView {
        client_id: slot.client_id(),
        instrument: slot.instrument(),
        side: slot.side(),
        level,
        state: slot.state(),
        price: slot.price(),
        qty: slot.qty(),
        filled: slot.filled_base(),
    }
}

#[inline]
pub(super) fn resting_of(slot: &OrderSlot) -> RestingOrder {
    RestingOrder {
        price: slot.price(),
        qty: slot.qty(),
        filled: slot.filled_base(),
        amends_used: slot.amends_used(),
    }
}
