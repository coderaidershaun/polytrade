//! The safety gates and what they report: readiness, the loss and exposure limits, the halt latch,
//! and why a side does not quote. Each verdict is a pure function.

use crate::hot::digest::{digest_by_hash, digest_fields};
use crate::hot::ledger::{PositionLedger, narrow_quote, wide};
use crate::log::hot_warn;
use crate::msg::ids::{AssetId, InstrumentId, Notional, Price, Qty, Side};
use crate::msg::labelled_enum::labelled_enum;
use crate::msg::time::LocalTs;

use super::capacity::ExecCapacity;
use super::engine::ExecEngine;
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
        SealNotAccepted = "persistence seal not accepted",
        OrphanFill = "fill on an order with no slot",
        PriorRunOverflow = "prior-run order table full",
        StatusProbesSpent = "order status probes spent",
        StreamSilent = "order stream silent",
        WriterFailed = "file writer failed",
        PriorRunFillUnbooked = "prior-run fill not booked",
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
        halted_ts_us: LocalTs,
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
    pub mark_to_market_quote: Option<Notional>,
    pub realised_quote: Notional,
    pub position_base: Qty,
}

/// # Panics
/// When `max_loss` is negative.
#[inline]
pub fn assess_loss(pnl: SessionPnl, max_loss: Notional) -> LossVerdict {
    assert!(
        max_loss >= Notional::ZERO,
        "session loss budget is a magnitude, got {max_loss:?}"
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
    // A flat position has no side to withdraw, and its loss is realised, so the realised leg judges
    // it: only realised loss halts.
    let Some(reducing) = reducing_side(pnl.position_base) else {
        return LossVerdict::Within;
    };
    LossVerdict::MarkToMarket { reducing }
}

#[inline]
fn breaches(pnl_quote: Notional, max_loss: Notional) -> bool {
    pnl_quote < Notional::ZERO && pnl_quote <= -max_loss
}

/// The side that shrinks a held position, or `None` when there is nothing to shrink.
#[inline]
pub(super) fn reducing_side(position_base: Qty) -> Option<Side> {
    match position_base.mantissa().signum() {
        1 => Some(Side::Sell),
        -1 => Some(Side::Buy),
        _ => None,
    }
}

/// The realised loss of each quote asset over the run, indexed by asset, and this is the one place
/// where a sum is read. The quote pass sums the ledger again at each spin. The basis of each
/// position that a rotation stranded stays in the sum as a loss, because its contracts can settle
/// at nothing and no order can trade them any more.
pub(super) struct LossSums {
    realised_quote: Box<[i128]>,
    stranded_quote: Box<[Notional]>,
}

digest_fields! {
    LossSums { stranded_quote }
    not_state {
        // Summed again from the ledger and the instruments before each read.
        realised_quote,
    }
}

impl LossSums {
    pub(super) fn new(capacity: &ExecCapacity) -> Self {
        Self {
            realised_quote: vec![0; capacity.assets()].into_boxed_slice(),
            stranded_quote: vec![Notional::ZERO; capacity.assets()].into_boxed_slice(),
        }
    }

    pub(super) fn realised(&self, asset: AssetId) -> Notional {
        let index = usize::from(asset.0);
        let stranded = wide(self.stranded_quote[index]);
        narrow_quote(
            self.realised_quote[index] - stranded,
            "realised loss of a quote asset",
        )
    }

    pub(super) fn strand(&mut self, asset: AssetId, basis: Notional) {
        let stranded = &mut self.stranded_quote[usize::from(asset.0)];
        *stranded = narrow_quote(wide(*stranded) + wide(basis), "stranded basis");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExposureCheck {
    pub exposure_quote: Notional,
    pub position_base: Qty,
    pub has_mark: bool,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub ceiling_quote: Notional,
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
    let projected = i128::from(check.exposure_quote.mantissa())
        + signed(check.side, check.price.notional(check.qty));
    if projected.abs() > i128::from(check.ceiling_quote.mantissa()) {
        return QuotePermission::ReducingOnly { reducing };
    }
    QuotePermission::Both
}

#[inline]
fn signed(side: Side, notional_quote: Notional) -> i128 {
    assert!(
        notional_quote >= Notional::ZERO,
        "an order's notional is unsigned by contract, got {notional_quote:?}"
    );
    match side {
        Side::Buy => i128::from(notional_quote.mantissa()),
        Side::Sell => -i128::from(notional_quote.mantissa()),
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

/// Why a side is not quoting, reported when the answer CHANGES rather than every spin. A refusal
/// usually persists, so reporting each one-second spin would bury the transitions that matter, and
/// reporting once forever would leave the panel showing a reason that has since changed. The latch
/// reports the first spin of a reason and of its replacement, and clears when the side quotes.
/// Nothing here reads a clock, so a replay reports the same reasons in the same order.
pub struct RefusalLatch {
    current: Box<[[Option<RejectReason>; SIDES]]>,
    /// One for each side of each instrument: a spin reports at most one reason for each.
    pending: Box<[Option<OrderReject>]>,
    len: usize,
}

digest_by_hash!(ExecHalt, Readiness);
digest_fields! { RefusalLatch { current, pending, len } }

impl RefusalLatch {
    pub(super) fn new(capacity: &ExecCapacity) -> Self {
        let instruments = capacity.instruments();
        Self {
            current: vec![[None; SIDES]; instruments].into_boxed_slice(),
            pending: vec![None; instruments * SIDES].into_boxed_slice(),
            len: 0,
        }
    }

    fn observe(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        reason: Option<RejectReason>,
        at: LocalTs,
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

    pub(super) fn forget_instruments(&mut self, instruments: std::ops::Range<usize>) {
        for row in &mut self.current[instruments] {
            *row = [None; SIDES];
        }
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
        if self.len >= self.pending.len() {
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
    /// Conservatively count possibly-live (pending/amending/unknown live until venue proves terminal).
    pub(super) fn enforce_resting_limit(&mut self, venue: usize, at: LocalTs) {
        if self.halt.is_halted() {
            return;
        }
        for instrument in self.venues[venue].instrument_ids() {
            for side in [Side::Buy, Side::Sell] {
                let possibly_live = self.orders.possibly_live_count(instrument, side);
                if possibly_live > self.venues[venue].settings.max_orders_per_side {
                    self.warn_duplicate_resting(instrument, side, possibly_live);
                    self.halt(HaltReason::DuplicateResting, at);
                    return;
                }
            }
        }
    }

    /// At WARN, the level operators know this line by.
    #[cold]
    fn warn_duplicate_resting(&self, instrument: InstrumentId, side: Side, resting: usize) {
        hot_warn!(
            "instrument {} holds {} resting {:?} orders against a limit of {}",
            instrument.0,
            resting,
            side,
            self.settings_of(instrument).max_orders_per_side
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
    pub(super) fn observe_pnl_baseline(&mut self, venue: usize, ledger: &PositionLedger) {
        let instruments = self.venues[venue].instruments.clone();
        for (instrument, row) in ledger.rows() {
            let index = usize::from(instrument.0);
            if !instruments.contains(&index) || self.pnl_at_baseline[index].is_some() {
                continue;
            }
            if row.has_mark() {
                self.pnl_at_baseline[index] = Some(row.pnl_quote());
            }
        }
    }

    /// Sums the realised loss of each quote asset before the quote pass reads it: each window of
    /// each instrument that quotes in that asset, those already rotated away included, because a
    /// ledger row lasts one market and the budget lasts the run.
    pub(super) fn sum_realised_losses(&mut self, ledger: &PositionLedger) {
        self.loss_sums.realised_quote.fill(0);
        for index in 0..self.instruments.len() {
            let instrument = InstrumentId(index as u16);
            let realised = wide(ledger.row(instrument).session_realised_quote())
                + wide(self.realised_carried_quote(instrument));
            let asset = self.instruments[index].quote_asset;
            self.loss_sums.realised_quote[usize::from(asset.0)] += realised;
        }
    }

    /// The loss verdict of one instrument. Its realised leg is the sum of its quote asset, so one
    /// budget holds for each instrument of that asset, and a breach there halts. Its mark-to-market
    /// leg is its own, so a loss still open on one instrument withdraws only the side that would
    /// add to it, on that instrument.
    ///
    /// The mark-to-market leg reads the change since the first honest valuation of the instrument.
    /// With no baseline yet there is no mark-to-market number to judge: a restored position with no
    /// mark is a legitimate state.
    pub(super) fn loss_verdict(
        &self,
        instrument: InstrumentId,
        ledger: &PositionLedger,
    ) -> LossVerdict {
        let index = usize::from(instrument.0);
        let row = ledger.row(instrument);
        let baseline = self.pnl_at_baseline[index];
        assess_loss(
            SessionPnl {
                mark_to_market_quote: baseline.map(|baseline| row.pnl_quote() - baseline),
                realised_quote: self.loss_sums.realised(self.instruments[index].quote_asset),
                position_base: row.position_base(),
            },
            self.settings_of(instrument).max_session_loss_quote,
        )
    }

    /// The sides of one instrument that the loss budget leaves open.
    pub(super) fn loss_permission(
        &self,
        instrument: InstrumentId,
        ledger: &PositionLedger,
    ) -> QuotePermission {
        match self.loss_verdict(instrument, ledger) {
            LossVerdict::Within => QuotePermission::Both,
            LossVerdict::MarkToMarket { reducing } => QuotePermission::ReducingOnly { reducing },
            LossVerdict::Realised => QuotePermission::Neither,
        }
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
            (!top.is_quotable(self.settings_of(instrument).limits.max_book_age))
                .then_some(RejectReason::BookNotQuotable)
        });
        self.refusals
            .observe(instrument, side, reason, input.now.as_local());
    }
}

#[cold]
fn refusal_warning(instrument: InstrumentId, side: Side, reason: RejectReason) {
    hot_warn!(
        "quote refused locally on instrument {} {:?}: {:?}",
        instrument.0,
        side,
        reason
    );
}
