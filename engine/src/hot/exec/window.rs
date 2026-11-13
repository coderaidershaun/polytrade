//! Markets that expire: when a window admits quotes, which orders a closing window pulls, and what
//! a rotation carries. Deadlines read message time, so a replay closes at the same point.

use crate::hot::ledger::{PositionLedger, narrow_quote, wide};
use crate::hot::strategy::Actions;
use crate::log::hot_warn;
use crate::msg::ids::{InstrumentId, Notional, Qty, Side};
use crate::msg::persist::{IncidentKind, IncidentRow};
use crate::msg::time::{LocalTs, VenueTs};

use super::audit::bank_incident;
use super::engine::{ExecEngine, FoldStamps};
use super::spin::SpinInput;

/// What the fold knows of a rotation when it hands it to the execution engine.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RotationFacts {
    pub(crate) instrument: InstrumentId,
    /// The close of the window that the rotation ends, `None` at the first rotation of a run.
    pub(crate) closing_window_close_ts_us: Option<VenueTs>,
    /// The venue's stamp of the rotation: the open of the window that it starts.
    pub(crate) exchange_ts_us: VenueTs,
    pub(crate) received_ts_us: LocalTs,
}

impl ExecEngine {
    /// Call before `PositionLedger::reset_instrument()`, which erases the row. The realised result
    /// of the closing window is carried into the run, because the loss budget lasts the run and a
    /// ledger row lasts one window. A position still open is stranded: one incident record, and
    /// its basis counts as a loss of its quote asset, until a settlement message closes it. The
    /// mark-to-market baseline clears and re-arms at the new window's first two-sided book.
    #[cold]
    pub(crate) fn on_market_rotation(
        &mut self,
        ledger: &PositionLedger,
        rotation: RotationFacts,
        bank: &mut Actions,
    ) {
        let instrument = rotation.instrument;
        let index = usize::from(instrument.0);
        if index >= self.instruments.len() {
            return;
        }
        let row = ledger.row(instrument);
        self.instruments[index].realised_carried_quote = narrow_quote(
            wide(self.instruments[index].realised_carried_quote)
                + wide(row.session_realised_quote()),
            "realised_carried_quote",
        );
        let position = row.position_base();
        if position.mantissa() != 0 {
            stranded_position_warning(instrument, position);
            let basis = Notional::from_mantissa(row.basis_quote().mantissa().abs());
            self.loss_sums
                .strand(self.instruments[index].quote_asset, basis);
            let incident = IncidentRow {
                kind: IncidentKind::StrandedPosition,
                instrument,
                client_id: None,
                trade_id: None,
                qty: position,
                amount: basis.mantissa(),
                window_close_ts_us: rotation.closing_window_close_ts_us,
                exchange_ts_us: rotation.exchange_ts_us,
                received_ts_us: rotation.received_ts_us,
            };
            bank_incident(bank, &mut self.incidents, incident);
        }
        self.pnl_at_baseline[index] = None;
    }

    /// Realised result carried from windows this instrument has already rotated through.
    #[inline]
    pub(super) fn realised_carried_quote(&self, instrument: InstrumentId) -> Notional {
        self.instruments[usize::from(instrument.0)].realised_carried_quote
    }

    // Non-rotating instruments have no window; the gate must be invisible to them (e.g., Binance).
    #[inline]
    pub(super) fn admits_window(&self, instrument: InstrumentId, input: &SpinInput<'_>) -> bool {
        input.windows[usize::from(instrument.0)].is_none_or(|window| {
            window.admits_quote_at(input.now.as_local(), self.settings.quote_stop_margin)
        })
    }

    /// Cancels all resting orders as the window closes. Uses per-order Cancel (not CancelOurs) so each
    /// stays tracked; venue losses are caught by in-flight timeout. CancelOurs is an exit that tells the
    /// edge to sweep and shut down, wrong for a routine rotation every few minutes that must leave the
    /// next window quotable. Runs every spin past the stop; refused cancels re-read as Live.
    pub(super) fn sweep_closing_windows(&mut self, input: &mut SpinInput<'_>) {
        let at = input.now.as_local();
        let margin = self.settings.quote_stop_margin;
        for index in 0..self.instruments.len() {
            let is_closing =
                input.windows[index].is_some_and(|window| window.is_past_quote_stop(at, margin));
            if !is_closing {
                continue;
            }
            let instrument = InstrumentId(index as u16);
            let pulled = self.cancel_resting_orders(instrument, input.stamps(), input.bank);
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
        stamps: FoldStamps,
        bank: &mut Actions,
    ) -> usize {
        let mut pulled = 0;
        for side in [Side::Buy, Side::Sell] {
            for index in self.orders.side_slot_range(instrument, side) {
                if !self.orders.slot(index).is_resting_quote() {
                    continue;
                }
                if !self.cancel_slot(index, stamps, bank) {
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
    hot_warn!(
        "instrument {} is inside the margin before its window closes — pulled {} resting order(s)",
        instrument.0,
        pulled
    );
}

/// A position still open when its market rotates is a loss with no upper bound: the contracts settle
/// at nothing or at everything and no order can be placed against them any more.
///
/// It stays at WARN, the level operators know it by. It repeats on purpose: every window that
/// strands something strands something NEW, so it cannot be latched.
#[cold]
fn stranded_position_warning(instrument: InstrumentId, position: Qty) {
    hot_warn!(
        "instrument {} rotated to a new window still holding {} base — that position is stranded on \
         a market this engine can no longer trade",
        instrument.0,
        position.mantissa()
    );
}
