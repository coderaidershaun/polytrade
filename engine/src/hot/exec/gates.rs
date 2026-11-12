//! Safety gates and what they report: readiness, the loss and exposure limits, the one-way halt
//! latch, and why a side is not quoting. Each verdict is a pure function, testable without an engine.

use crate::hot::ledger::{PositionLedger, narrow};
use crate::ids::{InstrumentId, Price, Qty, Side};
use crate::labelled_enum::labelled_enum;
use crate::time::TsUs;
use crate::warn;

use super::engine::ExecEngine;
use super::order::MAX_ORDER_INSTRUMENTS;
use super::reconcile::RejectReason;
use super::spin::SpinInput;
use super::view::{OrderReject, RejectOrigin};

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum HaltReason {
        RejectStreak = "reject streak",
        RealisedLoss = "realised loss",
        FatalReject = "fatal reject",
        SlotLeak = "slot leak",
        FilterViolation = "filter violation",
        DuplicateResting = "duplicate resting order",
        CommandBankOverflow = "command bank overflow",
    }
    /// Operator words for the halt banner: a halted engine's reason is read under pressure, and the
    /// table sits beside the variants so a new reason cannot ship as a bare Rust identifier.
    pub fn label;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecHalt {
    Armed,
    Halted {
        reason: HaltReason,
        halted_ts_us: TsUs,
    },
}

impl ExecHalt {
    #[inline]
    pub fn is_halted(self) -> bool {
        matches!(self, ExecHalt::Halted { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuotePermission {
    Both,
    ReducingOnly { reducing: Side },
    Neither,
}

impl QuotePermission {
    #[inline]
    pub fn admits(self, side: Side) -> bool {
        match self {
            QuotePermission::Both => true,
            QuotePermission::ReducingOnly { reducing } => side == reducing,
            QuotePermission::Neither => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LossVerdict {
    Within,
    MarkToMarket { reducing: Side },
    Realised,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionPnl {
    pub mark_to_market_quote: Option<i64>,
    pub realised_quote: i64,
    pub position_base: Qty,
}

#[inline]
pub fn assess_loss(pnl: SessionPnl, max_loss: i64) -> LossVerdict {
    debug_assert!(
        max_loss >= 0,
        "session loss budget is a magnitude, got {max_loss}"
    );
    // Realised loss (no price recovery possible) triggers halt.
    if breaches(pnl.realised_quote, max_loss) {
        return LossVerdict::Realised;
    }
    let Some(mark_to_market_quote) = pnl.mark_to_market_quote else {
        return LossVerdict::Within;
    };
    if !breaches(mark_to_market_quote, max_loss) {
        return LossVerdict::Within;
    }
    // Flat position = realised (except residue).
    let Some(reducing) = reducing_side(pnl.position_base) else {
        return LossVerdict::Realised;
    };
    LossVerdict::MarkToMarket { reducing }
}

#[inline]
fn breaches(pnl_quote: i64, max_loss: i64) -> bool {
    pnl_quote < 0 && pnl_quote <= -max_loss
}

/// The side that shrinks a held position, or `None` when there is nothing to shrink.
#[inline]
pub(super) fn reducing_side(position_base: Qty) -> Option<Side> {
    match position_base.0.signum() {
        1 => Some(Side::Sell),
        -1 => Some(Side::Buy),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExposureCheck {
    pub exposure_quote: i64,
    pub position_base: Qty,
    pub has_mark: bool,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub ceiling_quote: i64,
}

#[inline]
pub fn assess_exposure(check: ExposureCheck) -> QuotePermission {
    // Flat position: no accumulation risk, no gate.
    let Some(reducing) = reducing_side(check.position_base) else {
        return QuotePermission::Both;
    };
    // Reducing side allowed unconditionally.
    if check.side == reducing {
        return QuotePermission::Both;
    }
    // No mark means we can't value adding side (prevents false-flat quotes on restart).
    if !check.has_mark {
        return QuotePermission::ReducingOnly { reducing };
    }
    let projected =
        i128::from(check.exposure_quote) + signed(check.side, check.price.notional(check.qty));
    if projected.abs() > i128::from(check.ceiling_quote) {
        return QuotePermission::ReducingOnly { reducing };
    }
    QuotePermission::Both
}

#[inline]
fn signed(side: Side, notional_quote: i64) -> i128 {
    debug_assert!(
        notional_quote >= 0,
        "an order's notional is unsigned by contract, got {notional_quote}"
    );
    match side {
        Side::Buy => i128::from(notional_quote),
        Side::Sell => -i128::from(notional_quote),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub(super) struct Readiness {
    is_stream_ready: bool,
    has_open_orders_snapshot: bool,
}

impl Readiness {
    #[inline]
    pub fn observe_stream_ready(&mut self) {
        self.is_stream_ready = true;
    }

    #[inline]
    pub fn observe_open_orders_snapshot(&mut self) {
        self.has_open_orders_snapshot = true;
    }

    // Stream reset invalidates orders (all slots stale) but not balances (absolute state).
    #[inline]
    pub fn observe_stream_reset(&mut self) {
        self.is_stream_ready = false;
        self.has_open_orders_snapshot = false;
    }

    #[inline]
    pub fn is_stream_ready(self) -> bool {
        self.is_stream_ready
    }

    #[inline]
    pub fn has_open_orders_snapshot(self) -> bool {
        self.has_open_orders_snapshot
    }
}

labelled_enum! {
    /// The fact an unready engine is still waiting for.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ReadinessGap {
        /// Stream not subscribed or reset invalidated it.
        Stream = "account stream",
        /// No balance snapshot consumed.
        Balances = "balances",
        /// Open-order snapshot not landed.
        OpenOrders = "open orders",
    }
    pub fn label;
}

const SIDES: usize = 2;
const MAX_REFUSALS: usize = MAX_ORDER_INSTRUMENTS * SIDES;

/// Why a side is not quoting, reported when the answer CHANGES rather than every spin. A refusal
/// usually persists, so reporting each one-second spin would bury the transitions that matter, and
/// reporting once forever would leave the panel showing a reason that has since changed. The latch
/// reports the first spin of a reason and of its replacement, and clears when the side quotes.
/// Nothing here reads a clock, so a replay reports the same reasons in the same order.
#[derive(Default)]
pub struct RefusalLatch {
    current: [[Option<RejectReason>; SIDES]; MAX_ORDER_INSTRUMENTS],
    pending: [Option<OrderReject>; MAX_REFUSALS],
    len: usize,
}

impl RefusalLatch {
    fn observe(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        reason: Option<RejectReason>,
        at: TsUs,
    ) {
        let Some(cell) = self.slot_mut(instrument, side) else {
            return;
        };
        if *cell == reason {
            return;
        }
        *cell = reason;
        let Some(reason) = reason else {
            return;
        };
        self.push(OrderReject {
            instrument,
            client_id: None,
            side,
            level: None,
            origin: RejectOrigin::Local(reason),
            event_ts_us: at,
        });
        refusal_warning(instrument, side, reason);
    }

    pub fn drain_into(&mut self, sink: &mut impl FnMut(&OrderReject)) {
        for slot in self.pending.iter_mut().take(self.len) {
            if let Some(reject) = slot.take() {
                sink(&reject);
            }
        }
        self.len = 0;
    }

    pub fn forget(&mut self) {
        self.current = [[None; SIDES]; MAX_ORDER_INSTRUMENTS];
    }

    fn slot_mut(
        &mut self,
        instrument: InstrumentId,
        side: Side,
    ) -> Option<&mut Option<RejectReason>> {
        let row = self.current.get_mut(usize::from(instrument.0))?;
        Some(&mut row[side.index()])
    }

    fn push(&mut self, reject: OrderReject) {
        if self.len >= MAX_REFUSALS {
            return;
        }
        self.pending[self.len] = Some(reject);
        self.len += 1;
    }
}

impl ExecEngine {
    /// The first fact an unready engine is still waiting for, joined across its two owners: the
    /// stream and open-order facts the readiness latch holds, and the balance snapshot the account
    /// table does. Ordered furthest-upstream first, so a diagnostic names the cause rather than a
    /// consequence of it.
    #[inline]
    pub(super) fn readiness_gap(&self) -> Option<ReadinessGap> {
        if !self.readiness.is_stream_ready() {
            return Some(ReadinessGap::Stream);
        }
        if !self.account.has_snapshot() {
            return Some(ReadinessGap::Balances);
        }
        (!self.readiness.has_open_orders_snapshot()).then_some(ReadinessGap::OpenOrders)
    }

    /// Conservatively count possibly-live (pending/amending/unknown live until venue proves terminal).
    pub(super) fn enforce_resting_limit(&mut self, at: TsUs) {
        if self.halt.is_halted() {
            return;
        }
        for index in 0..self.instruments.len() {
            let instrument = InstrumentId(index as u16);
            for side in [Side::Buy, Side::Sell] {
                let possibly_live = self.orders.possibly_live_count(instrument, side);
                if possibly_live > self.settings.max_orders_per_side {
                    self.warn_duplicate_resting(instrument, side, possibly_live);
                    self.halt(HaltReason::DuplicateResting, at);
                    return;
                }
            }
        }
    }

    /// Numbers at WARN not ERROR (no second backtrace per event).
    #[cold]
    fn warn_duplicate_resting(&self, instrument: InstrumentId, side: Side, resting: usize) {
        warn!(
            "instrument {} holds {resting} resting {side:?} orders against a limit of {}",
            instrument.0, self.settings.max_orders_per_side
        );
    }

    /// Establishes each instrument's PnL baseline the first time it has an honest valuation.
    ///
    /// The ledger is CROSS-SESSION now: it is seeded at boot from persisted cost basis, so
    /// `pnl_quote()` carries prior runs' realised PnL and a session limit compared against it would
    /// trip on losses this run never made. The gate therefore measures the DELTA since the baseline.
    ///
    /// The baseline is taken at the first MARK rather than at construction, and that is what makes it
    /// honest. A restored long boots with `cash_quote` at minus its cost basis and no mark, so raw
    /// PnL reads as a catastrophic loss; a baseline captured there would be that same figure and the
    /// delta would silently include the whole restored position's cost. Waiting for the first
    /// two-sided book makes the baseline a mark-to-market valuation, so the delta is this session's
    /// PnL and nothing else. Message-driven, so it is replay-exact and depends on no boot ordering.
    pub(super) fn observe_pnl_baseline(&mut self, ledger: &PositionLedger) {
        for (instrument, row) in ledger.rows() {
            let index = usize::from(instrument.0);
            if index >= self.instruments.len() || self.pnl_at_baseline[index].is_some() {
                continue;
            }
            if row.has_mark() {
                self.pnl_at_baseline[index] = Some(row.pnl_quote());
            }
        }
    }

    /// The session loss verdict alone, over every instrument. The halt keys on THIS rather than on
    /// the combined permission the spin pass builds, because a realised breach is the only thing
    /// that may halt and the position ceiling withdrawing a side is not one.
    pub(super) fn session_permission(&self, ledger: &PositionLedger) -> QuotePermission {
        let mut permission = QuotePermission::Both;
        for (instrument, row) in ledger.rows() {
            if usize::from(instrument.0) >= self.instruments.len() {
                continue;
            }
            // No baseline yet means no honest valuation has ever existed for this instrument, so
            // there is no mark-to-market number to judge — a restored position with no mark is a
            // legitimate state, not an absent one. The REALISED leg is judged regardless: a round
            // trip closed before the first two-sided book really did lose what it lost.
            let baseline = self.pnl_at_baseline[usize::from(instrument.0)];
            // The realised leg is this window's PLUS every window already rotated away, because a
            // ledger row lasts one market and the budget lasts the run.
            match assess_loss(
                SessionPnl {
                    mark_to_market_quote: baseline.map(|baseline| row.pnl_quote() - baseline),
                    realised_quote: narrow(
                        i128::from(row.session_realised_quote())
                            + i128::from(self.realised_carried_quote(instrument)),
                        "session realised including carried windows",
                    ),
                    position_base: row.position_base(),
                },
                self.settings.max_session_loss_quote,
            ) {
                LossVerdict::Within => {}
                LossVerdict::MarkToMarket { reducing } => {
                    permission = QuotePermission::ReducingOnly { reducing };
                }
                LossVerdict::Realised => return QuotePermission::Neither,
            }
        }
        permission
    }

    pub(super) fn record_refusal(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        reason: Option<RejectReason>,
        input: &SpinInput<'_>,
    ) {
        let reason = reason.or_else(|| {
            let top = self.book_top(instrument, input);
            (!top.is_quotable(self.settings.limits.max_book_age))
                .then_some(RejectReason::BookNotQuotable)
        });
        self.refusals
            .observe(instrument, side, reason, input.tick.received_ts_us);
    }
}

#[cold]
fn refusal_warning(instrument: InstrumentId, side: Side, reason: RejectReason) {
    warn!(
        "quote refused locally on instrument {} {side:?}: {reason:?}",
        instrument.0
    );
}
