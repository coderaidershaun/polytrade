//! Main spin loop: expire stale orders, timeout unanswered requests, reconcile, decide quotes,
//! and drain the action bank. Split from engine.rs so venue-event folding and engine-cadence scheduling
//! stay separately readable. All deadlines derive from message timestamps for replay determinism.
//! No clock reads (enforced by fitness scan). Window and flatten passes live in separate files; their
//! order is load-bearing. Committing a decision to a command lives in mint.rs, which the flatten
//! pass reaches too — deciding and committing have different callers.

use crate::config::MAX_QUOTE_LEVELS;
use crate::hot::book::{Book, BookState};
use crate::hot::ledger::PositionLedger;
use crate::hot::strategy::{Actions, WindowInfo};
use crate::ids::{InstrumentId, Price, Side};
use crate::msg::exec::ExecCommand;
use crate::msg::inbound::SpinTick;
use crate::msg::persist::OrderTransition;
use crate::time::TsUs;

use super::desired::{DesiredBook, DesiredQuote, QuoteLevel};
use super::engine::{ExecEngine, resting_of};
use super::gates::{ExposureCheck, HaltReason, QuotePermission, assess_exposure};
use super::order::{MAX_ORDER_SLOTS, OrderState};
use super::reconcile::{
    BookTop, FundsView, ReconcileInput, ReconcileOutcome, RejectReason, reconcile_side,
};

impl ExecEngine {
    pub(crate) fn on_spin(&mut self, mut input: SpinInput<'_>) {
        if self.sink.is_none() {
            return;
        }
        let now = input.tick.received_ts_us;
        self.budget.observe_spin(now);
        self.orders.reap(now, self.settings.order_reap_window);
        self.sweep_timeouts(now, input.bank);
        self.detect_silence(now);
        self.observe_pnl_baseline(input.ledger);
        self.enforce_resting_limit(now);
        // The window sweep runs first: pulling orders off a closing market becomes
        // irreversible if it's late, and doing so frees the side for the flatten pass.
        self.sweep_closing_windows(&mut input);
        // Flatten runs before quote: claiming a slot marks the side as in-flight, so the
        // quote ladder skips it. Reversing the order would send two unanswered commands on
        // one side in a single spin.
        self.flatten_pass(&mut input);
        self.quote_pass(&mut input);
        if let Some(sink) = self.sink.as_mut() {
            self.pending.drain_into(sink, now);
        }
    }

    fn sweep_timeouts(&mut self, now: TsUs, bank: &mut Actions) {
        for index in 0..MAX_ORDER_SLOTS {
            if self
                .orders
                .is_timed_out(index, now, self.settings.inflight_timeout)
            {
                self.chase_timed_out(index, now, bank);
            }
        }
    }

    /// The venue never answered this order's request, so its state is unknown until the venue is
    /// asked about the order directly.
    #[cold]
    fn chase_timed_out(&mut self, index: usize, now: TsUs, bank: &mut Actions) {
        let slot = self.orders.slot_mut(index);
        slot.last_event_ts_us = now;
        let (instrument, client_id) = (slot.instrument, slot.client_id);
        let next = OrderState::Unknown;
        self.move_slot(index, next, OrderTransition::Timeout, now, bank);
        self.recon_seq += 1;
        let command = ExecCommand::ReconcileOrder {
            instrument,
            client_id,
            recon_seq: self.recon_seq,
        };
        self.bank_or_halt(command, now);
    }

    fn detect_silence(&mut self, now: TsUs) {
        self.spins_since_exec_event += 1;
        let is_silent = self.spins_since_exec_event >= self.settings.exec_silence_spins;
        let has_working =
            (0..MAX_ORDER_SLOTS).any(|index| self.orders.slot(index).state.is_working());
        if is_silent && has_working {
            self.request_open_orders(now);
        }
    }

    #[cold]
    fn request_open_orders(&mut self, now: TsUs) {
        self.spins_since_exec_event = 0;
        for index in 0..self.instruments.len() {
            self.recon_seq += 1;
            let command = ExecCommand::ReconcileOpenOrders {
                instrument: InstrumentId(index as u16),
                recon_seq: self.recon_seq,
            };
            self.bank_or_halt(command, now);
        }
    }

    fn quote_pass(&mut self, input: &mut SpinInput<'_>) {
        let session = self.session_permission(input.ledger);
        if session == QuotePermission::Neither && !self.halt.is_halted() {
            self.halt(HaltReason::RealisedLoss, input.tick.received_ts_us);
        }
        for index in 0..self.instruments.len() {
            let instrument = InstrumentId(index as u16);
            for side in [Side::Buy, Side::Sell] {
                let reason = self.reconcile_ladder(instrument, side, session, input);
                self.record_refusal(instrument, side, reason, input);
            }
        }
    }

    fn admitted_quote(
        &self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        session: QuotePermission,
        input: &SpinInput<'_>,
    ) -> Result<DesiredQuote, RejectReason> {
        if self.halt.is_halted() {
            return Err(RejectReason::Halted);
        }
        if let Some(gap) = self.readiness_gap() {
            return Err(RejectReason::NotReady(gap));
        }
        if !session.admits(side) {
            return Err(RejectReason::SessionReducingOnly);
        }
        let declared = input
            .desired
            .quote(instrument, side, level, input.tick.seq)
            .ok_or(RejectReason::NoQuoteDeclared)?;
        // Checked against the declaration, not ahead of it: an instrument with no quote
        // declared at all reports NoQuoteDeclared, not a calendar refusal.
        if !self.admits_window(instrument, input) {
            return Err(RejectReason::OutsideWindow);
        }
        let row = input.ledger.row(instrument);
        let permission = assess_exposure(ExposureCheck {
            exposure_quote: row.exposure_quote(),
            position_base: row.position_base(),
            has_mark: row.has_mark(),
            side,
            price: declared.price,
            qty: declared.qty,
            ceiling_quote: self.instruments[usize::from(instrument.0)].max_exposure_quote,
        });
        if permission.admits(side) { Ok(declared) } else { Err(RejectReason::ExposureCeiling) }
    }

    fn reconcile_ladder(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        session: QuotePermission,
        input: &mut SpinInput<'_>,
    ) -> Option<RejectReason> {
        let mut ladder = self.desired_ladder(instrument, side, session, input);
        // Nothing to decide while the venue owes this side an answer: a second command beside an
        // unanswered one is how a side ends up with two orders where it asked for one.
        if self.orders.is_awaiting_answer(instrument, side) {
            return ladder.refusal;
        }
        let Some((level, outcome)) = self.choose_action(instrument, side, &mut ladder, input)
        else {
            return ladder.resolve();
        };
        if matches!(outcome, ReconcileOutcome::Place(_))
            && let Some(reason) = self.place_refusal(instrument, side)
        {
            ladder.refusal.get_or_insert(reason);
            return ladder.refusal;
        }
        self.act_on(instrument, side, level, outcome, input);
        ladder.resolve()
    }

    /// Why this side may not mint a placement this spin. Checks how many orders it already
    /// has out, then the venue's account-wide placement budget.
    ///
    /// The budget refuses QUOTES only — the flatten pass never asks. A quote can wait a spin and
    /// lose nothing but a spread; an order that sheds a position cannot, and starving it locally
    /// would be the engine choosing to stay exposed. Refusing quotes early is how the headroom that
    /// exit needs is still there, since the quotes are what spend it.
    fn place_refusal(&self, instrument: InstrumentId, side: Side) -> Option<RejectReason> {
        if self.orders.possibly_live_count(instrument, side) >= self.settings.max_orders_per_side {
            return Some(RejectReason::OrderLimit);
        }
        (!self.budget.admits_place()).then_some(RejectReason::RateBudget)
    }

    /// What the strategy wants on this side once every gate has had its say, level by level.
    fn desired_ladder(
        &self,
        instrument: InstrumentId,
        side: Side,
        session: QuotePermission,
        input: &SpinInput<'_>,
    ) -> DesiredLadder {
        let tick = self.instruments[usize::from(instrument.0)].grid.tick;
        let depth = self.settings.max_orders_per_side.min(MAX_QUOTE_LEVELS);
        let mut ladder = DesiredLadder::default();
        let mut snapped_prices = [None; MAX_QUOTE_LEVELS];
        for level in QuoteLevel::ALL {
            let admitted = if level.index() < depth {
                self.admitted_quote(instrument, side, level, session, input)
            } else {
                Err(RejectReason::NoQuoteDeclared)
            };
            let quote = match admitted {
                Ok(quote) => {
                    let snapped = side.snap_passive(quote.price, tick);
                    if snapped_prices[..level.index()].contains(&Some(snapped)) {
                        ladder.refusal.get_or_insert(RejectReason::DuplicatePrice);
                        None
                    } else {
                        snapped_prices[level.index()] = Some(snapped);
                        ladder.has_admitted_quote = true;
                        Some(quote)
                    }
                }
                Err(reason) => {
                    // An empty unused level is ordinary ladder shape, not a side-wide refusal.
                    if reason != RejectReason::NoQuoteDeclared
                        || self.orders.resting(instrument, side, level).is_some()
                    {
                        ladder.refusal.get_or_insert(reason);
                    }
                    None
                }
            };
            ladder.quotes[level.index()] = quote;
        }
        ladder
    }

    /// The one command this side may issue this spin, chosen across the whole ladder: a withdrawal
    /// before a shrink before a new quote, so a side never widens its exposure while it still has
    /// something to take back.
    fn choose_action(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        ladder: &mut DesiredLadder,
        input: &SpinInput<'_>,
    ) -> Option<(QuoteLevel, ReconcileOutcome)> {
        let grid = self.instruments[usize::from(instrument.0)].grid;
        let funds = self.funds_view(instrument, side);
        let top = self.book_top(instrument, input);
        let mut cancel = None;
        let mut amend = None;
        let mut place = None;
        for level in QuoteLevel::ALL {
            let outcome = reconcile_side(ReconcileInput {
                side,
                desired: ladder.quotes[level.index()],
                resting: self
                    .orders
                    .resting(instrument, side, level)
                    .filter(|slot| slot.state == OrderState::Live)
                    .map(resting_of),
                grid,
                top,
                limits: self.settings.limits,
                funds,
            });
            match outcome {
                ReconcileOutcome::Cancel if cancel.is_none() => cancel = Some((level, outcome)),
                ReconcileOutcome::AmendQty(_) if amend.is_none() => amend = Some((level, outcome)),
                ReconcileOutcome::Place(_) if place.is_none() => place = Some((level, outcome)),
                ReconcileOutcome::Reject(reason) => {
                    ladder.refusal.get_or_insert(reason);
                }
                _ => {}
            }
        }
        cancel.or(amend).or(place)
    }

    pub(super) fn funds_view(&self, instrument: InstrumentId, side: Side) -> FundsView {
        let asset = self.instruments[usize::from(instrument.0)].spend_asset(side);
        let floor = match side {
            Side::Buy => self.settings.min_quote_balance,
            Side::Sell => self.settings.min_base_balance,
        };
        FundsView {
            spendable: self.account.balance(asset).spendable(),
            floor,
        }
    }

    pub(super) fn book_top(&self, instrument: InstrumentId, input: &SpinInput<'_>) -> BookTop {
        let book = &input.books[usize::from(instrument.0)];
        let (bid, ask) = (book.best_bid(), book.best_ask());
        let mid = match (bid, ask) {
            (Some(bid), Some(ask)) => Price(bid.price.0 + (ask.price.0 - bid.price.0) / 2),
            _ => Price(0),
        };
        BookTop {
            best_bid: bid.map(|level| level.price),
            best_ask: ask.map(|level| level.price),
            mid,
            is_valid: book.state() == BookState::Valid,
            last_commit_ts_us: self.instruments[usize::from(instrument.0)].last_commit_ts_us,
            now_ts_us: input.tick.received_ts_us,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct DesiredLadder {
    quotes: [Option<DesiredQuote>; MAX_QUOTE_LEVELS],
    /// The first reason the side had for not quoting, set with `get_or_insert` so a later reason
    /// never displaces it: an operator wants the reason the ladder stopped, not whatever else was
    /// also true by then.
    refusal: Option<RejectReason>,
    has_admitted_quote: bool,
}

impl DesiredLadder {
    /// A ladder holding at least one admitted quote IS quoting, so an undeclared rung on it is
    /// ladder shape rather than a refusal. A ladder holding none is refusing, and says so even where
    /// every gate stayed silent.
    fn resolve(self) -> Option<RejectReason> {
        if !self.has_admitted_quote {
            return self.refusal.or(Some(RejectReason::NoQuoteDeclared));
        }
        self.refusal
            .filter(|reason| *reason != RejectReason::NoQuoteDeclared)
    }
}

pub struct SpinInput<'a> {
    pub tick: &'a SpinTick,
    pub books: &'a [Book],
    pub desired: &'a DesiredBook,
    /// Per instrument, the window the venue last rotated it into. `None` where the venue does not
    /// rotate, which is what makes the quote-window gate inert everywhere it does not belong.
    pub windows: &'a [Option<WindowInfo>],
    pub(crate) ledger: &'a PositionLedger,
    pub(crate) bank: &'a mut Actions,
}
