//! Execution windows for markets that expire. This module handles three aspects: when a window admits
//! quotes, what happens to resting orders as the window closes, and how rotation into the next market
//! affects accounting. All three relate to the same fundamental fact: each instrument slot points to
//! a market that rotates periodically. Instruments without rotation (like Binance) are unaffected.
//! Every deadline derives from message timestamps, so replays close and rotate at exact sequence points.

use crate::hot::ledger::{PositionLedger, narrow};
use crate::hot::strategy::Actions;
use crate::ids::{InstrumentId, Qty, Side};
use crate::time::TsUs;
use crate::warn;

use super::engine::ExecEngine;
use super::spin::SpinInput;

impl ExecEngine {
    /// Call before PositionLedger::reset_instrument(). Carries forward the realised result from this
    /// window before the ledger row is erased. The kill switch measures run-level PnL across all windows,
    /// not single windows, so this carry-forward is essential. The mark-to-market baseline clears (no
    /// longer valid for this market) and re-arms at the new window's first two-sided book.
    #[cold]
    pub(crate) fn on_market_rotation(&mut self, ledger: &PositionLedger, instrument: InstrumentId) {
        let index = usize::from(instrument.0);
        if index >= self.instruments.len() {
            return;
        }
        let row = ledger.row(instrument);
        self.instruments[index].realised_carried_quote = narrow(
            i128::from(self.instruments[index].realised_carried_quote)
                + i128::from(row.session_realised_quote()),
            "realised_carried_quote",
        );
        if row.position_base().0 != 0 {
            stranded_position_warning(instrument, row.position_base());
        }
        self.pnl_at_baseline[index] = None;
    }

    /// Realised result carried from windows this instrument has already rotated through.
    #[inline]
    pub(super) fn realised_carried_quote(&self, instrument: InstrumentId) -> i64 {
        self.instruments[usize::from(instrument.0)].realised_carried_quote
    }

    // Non-rotating instruments have no window; the gate must be invisible to them (e.g., Binance).
    #[inline]
    pub(super) fn admits_window(&self, instrument: InstrumentId, input: &SpinInput<'_>) -> bool {
        input.windows[usize::from(instrument.0)].is_none_or(|window| {
            window.admits_quote_at(input.tick.received_ts_us, self.settings.quote_stop_margin)
        })
    }

    /// Cancels all resting orders as the window closes. Uses per-order Cancel (not CancelOurs) so each
    /// stays tracked; venue losses are caught by in-flight timeout. CancelOurs is an exit that tells the
    /// edge to sweep and shut down, wrong for a routine rotation every few minutes that must leave the
    /// next window quotable. Runs every spin past the stop; refused cancels re-read as Live.
    pub(super) fn sweep_closing_windows(&mut self, input: &mut SpinInput<'_>) {
        let at = input.tick.received_ts_us;
        let margin = self.settings.quote_stop_margin;
        for index in 0..self.instruments.len() {
            let is_closing =
                input.windows[index].is_some_and(|window| window.is_past_quote_stop(at, margin));
            if !is_closing {
                continue;
            }
            let instrument = InstrumentId(index as u16);
            let pulled = self.cancel_resting_orders(instrument, at, input.bank);
            if pulled > 0 {
                report_window_sweep(instrument, pulled);
            }
        }
    }

    // Cancels only resting (passive) orders. Unanswered cancels are already being chased by the in-flight
    // timeout; a second cancel on an unknown request creates a duplicate live order.
    fn cancel_resting_orders(
        &mut self,
        instrument: InstrumentId,
        at: TsUs,
        bank: &mut Actions,
    ) -> usize {
        let mut pulled = 0;
        for side in [Side::Buy, Side::Sell] {
            for index in self.orders.side_slot_range(instrument, side) {
                if !self.orders.slot(index).is_resting_quote() {
                    continue;
                }
                if !self.cancel_slot(index, at, bank) {
                    return pulled;
                }
                pulled += 1;
            }
        }
        pulled
    }
}

/// A window closing with orders still on it is the state this whole gate exists to prevent, so it is
/// said out loud rather than left as a counter to be noticed later. It repeats only when there was
/// something to pull, which past the first pass means a cancel the venue refused.
#[cold]
fn report_window_sweep(instrument: InstrumentId, pulled: usize) {
    warn!(
        "instrument {} is inside the margin before its window closes — pulled {pulled} resting order(s)",
        instrument.0
    );
}

/// A position still open when its market rotates is a loss with no upper bound: the contracts settle
/// at nothing or at everything and no order can be placed against them any more.
///
/// At WARN rather than ERROR for the reason the reconciliation sweep's own warning is, and it binds
/// harder here. This carries the numbers an operator acts on and it repeats — every window that
/// strands something strands something NEW, so it cannot be latched — while `error!` captures a
/// backtrace by construction. A backtrace tells nobody anything the message has not already said,
/// and capturing one on the hot thread every few minutes is a cost the hot path does not accept
/// for a line that is already as loud as it needs to be.
#[cold]
fn stranded_position_warning(instrument: InstrumentId, position: Qty) {
    warn!(
        "instrument {} rotated to a new window still holding {} base — that position is stranded on \
         a market this engine can no longer trade",
        instrument.0, position.0
    );
}
