//! The execution pass of each spin, in an order that each step relies on: expire, chase timeouts,
//! sweep closing windows, flatten, then quote. Each deadline reads message time, never a clock.

use crate::hot::book::{Book, BookState};
use crate::hot::dispatch::StrategyGate;
use crate::hot::ledger::PositionLedger;
use crate::hot::strategy::{Actions, WindowInfo};
use crate::msg::exec::ExecCommand;
use crate::msg::ids::{InstrumentId, Price, Side};
use crate::msg::inbound::SpinTick;
use crate::msg::persist::OrderTransition;
use crate::msg::time::{FoldTime, LocalTs};
use crate::runtime::config::MAX_QUOTE_LEVELS;

use super::budget::QuotePermit;
use super::desired::{DesiredBook, DesiredQuote, QuoteLevel};
use super::engine::{ExecEngine, FoldStamps, resting_of};
use super::gates::{ExposureCheck, HaltReason, LossVerdict, QuotePermission, assess_exposure};
use super::order::OrderState;
use super::reconcile::{
    BookTop, FundsView, ReconcileInput, ReconcileOutcome, RejectReason, reconcile_side,
};

impl ExecEngine {
    pub(crate) fn on_spin(&mut self, mut input: SpinInput<'_>) {
        if self.sink.is_none() {
            return;
        }
        let now = input.now.as_local();
        self.budget.observe_spin(now);
        self.orders.reap(now, self.settings.order_reap_window);
        self.sweep_timeouts(input.stamps(), input.bank);
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

    fn sweep_timeouts(&mut self, stamps: FoldStamps, bank: &mut Actions) {
        let mut next = self.orders.next_in_use(0);
        while let Some(index) = next {
            next = self.orders.next_in_use(index + 1);
            if self
                .orders
                .is_timed_out(index, stamps.fold, self.settings.inflight_timeout)
            {
                self.chase_timed_out(index, stamps, bank);
            }
        }
    }

    /// The venue never answered this order's request, so its state is unknown until the venue is
    /// asked. It is asked about the order a few times, then for the open orders of its instrument
    /// once. An order still unknown after that would leave the engine trading with an order it
    /// cannot account for, so quoting halts.
    #[cold]
    fn chase_timed_out(&mut self, index: usize, stamps: FoldStamps, bank: &mut Actions) {
        let now = stamps.fold;
        let probes = self.orders.slot(index).status_probes();
        let max_probes = u16::from(self.settings.max_status_probes);
        if probes > max_probes {
            return self.halt(HaltReason::StatusProbesSpent, now);
        }
        let previous = self.orders.mark_timed_out(index, now);
        self.bank_moved(
            index,
            previous,
            OrderTransition::Timeout,
            stamps.received,
            bank,
        );
        let slot = self.orders.slot(index);
        let (instrument, client_id) = (slot.instrument(), slot.client_id());
        self.recon_seq += 1;
        let recon_seq = self.recon_seq;
        let command = if probes < max_probes {
            ExecCommand::ReconcileOrder {
                instrument,
                client_id,
                recon_seq,
            }
        } else {
            ExecCommand::ReconcileOpenOrders {
                instrument,
                recon_seq,
            }
        };
        self.bank_or_halt(command, now);
    }

    fn detect_silence(&mut self, now: LocalTs) {
        self.spins_since_exec_event += 1;
        let is_silent = self.spins_since_exec_event >= self.settings.exec_silence_spins;
        let has_working = self
            .orders
            .slots_in_use()
            .any(|index| self.orders.slot(index).state().is_working());
        if is_silent && has_working {
            self.request_open_orders(now);
        }
    }

    #[cold]
    fn request_open_orders(&mut self, now: LocalTs) {
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

    /// A realised loss of a quote asset at the budget halts quoting before any side is decided, and
    /// then each instrument quotes the sides that its own loss permission leaves open.
    fn quote_pass(&mut self, input: &mut SpinInput<'_>) {
        self.sum_realised_losses(input.ledger);
        let is_spent = (0..self.instruments.len()).any(|index| {
            let verdict = self.loss_verdict(InstrumentId(index as u16), input.ledger);
            verdict == LossVerdict::Realised
        });
        if is_spent && !self.halt.is_halted() {
            self.halt(HaltReason::RealisedLoss, input.now.as_local());
        }
        for index in 0..self.instruments.len() {
            let instrument = InstrumentId(index as u16);
            let permission = self.loss_permission(instrument, input.ledger);
            for side in [Side::Buy, Side::Sell] {
                let reason = self.reconcile_ladder(instrument, side, permission, input);
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
        // declared at all reports NoQuoteDeclared, not a warmup or calendar refusal.
        if !input.gates[usize::from(instrument.0)].is_live() {
            return Err(RejectReason::Warming);
        }
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
        if let Some(reason) = self.act_on(instrument, side, level, outcome, input) {
            ladder.refusal.get_or_insert(reason);
        }
        ladder.resolve()
    }

    /// The permit to place a quote on this side this spin: a free order on the side, then the
    /// venue's account-wide placement budget.
    ///
    /// Only a quote asks. A quote can wait a spin and lose a spread, while the order that gets a
    /// market flat cannot wait, and a local refusal of it would be the engine choosing to stay
    /// exposed. Quotes are what spend the budget, so refusing them early leaves the headroom that
    /// the way out needs.
    pub(super) fn admit_quote_placement(
        &self,
        instrument: InstrumentId,
        side: Side,
    ) -> Result<QuotePermit, RejectReason> {
        if self.orders.possibly_live_count(instrument, side) >= self.settings.max_orders_per_side {
            return Err(RejectReason::OrderLimit);
        }
        self.budget.admit_quote()
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
                    .filter(|slot| slot.state() == OrderState::Live)
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
            Side::Buy => self.settings.min_quote_balance.mantissa(),
            Side::Sell => self.settings.min_base_balance.mantissa(),
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
            (Some(bid), Some(ask)) => Price::from_mantissa(
                bid.price.mantissa() + (ask.price.mantissa() - bid.price.mantissa()) / 2,
            ),
            _ => Price::from_mantissa(0),
        };
        BookTop {
            best_bid: bid.map(|level| level.price),
            best_ask: ask.map(|level| level.price),
            mid,
            is_valid: book.state() == BookState::Valid,
            last_commit_ts_us: self.instruments[usize::from(instrument.0)].last_commit_ts_us,
            now_ts_us: input.now.as_local(),
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
    /// The time that each deadline of the pass reads.
    pub now: FoldTime,
    /// The received stamp of the tick: the stamp of each row that the pass writes.
    pub(crate) received: LocalTs,
    pub books: &'a [Book],
    pub desired: &'a DesiredBook,
    /// Per instrument, the window the venue last rotated it into. `None` where the venue does not
    /// rotate, which is what makes the quote-window gate inert everywhere it does not belong.
    pub windows: &'a [Option<WindowInfo>],
    /// Per instrument, whether its market is live. A market that warms quotes nothing, and that is
    /// judged after the loss permission, so a warming market never halts quoting.
    pub(crate) gates: &'a [StrategyGate],
    pub(crate) ledger: &'a PositionLedger,
    pub(crate) bank: &'a mut Actions,
}

impl SpinInput<'_> {
    pub(crate) fn stamps(&self) -> FoldStamps {
        FoldStamps {
            fold: self.now.as_local(),
            received: self.received,
        }
    }
}
