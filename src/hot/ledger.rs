//! Per-instrument position ledger: position base, cash, and mark-to-market. Engine owns it because
//! PnL calculation needs cost basis (fill prices are thrown away by strategy counters). Uses exact i64
//! quote mantissas throughout (no float). Only division is average cost (roll_basis) when closing a
//! partial position. Fills fold during their message, before the strategy sees on_fill.

use crate::exposure::InstrumentExposure;
use crate::ids::{InstrumentId, Price, Qty, Side};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct LedgerFill {
    pub(crate) instrument: InstrumentId,
    pub(crate) side: Side,
    pub(crate) base: Qty,
    pub(crate) notional_quote: i64,
    pub(crate) commission_quote: i64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) struct LedgerRow {
    position_base: i64,
    cash_quote: i64,
    basis_quote: i64,
    realised_at_boot: i64,
    mark: Option<Price>,
}

impl LedgerRow {
    #[inline]
    pub(crate) fn has_mark(&self) -> bool {
        self.mark.is_some()
    }

    #[inline]
    pub(crate) fn position_base(&self) -> Qty {
        Qty(self.position_base)
    }

    #[inline]
    pub(crate) fn cash_quote(&self) -> i64 {
        self.cash_quote
    }

    #[inline]
    pub(crate) fn basis_quote(&self) -> i64 {
        self.basis_quote
    }

    #[inline]
    pub(crate) fn exposure_quote(&self) -> i64 {
        self.mark
            .map_or(0, |mark| mark.notional(self.position_base()))
    }

    #[inline]
    pub(crate) fn pnl_quote(&self) -> i64 {
        narrow(
            i128::from(self.cash_quote) + i128::from(self.exposure_quote()),
            "pnl_quote",
        )
    }

    #[inline]
    pub(crate) fn session_realised_quote(&self) -> i64 {
        narrow(
            i128::from(self.realised_quote()) - i128::from(self.realised_at_boot),
            "session_realised_quote",
        )
    }

    #[inline]
    fn realised_quote(&self) -> i64 {
        narrow(
            i128::from(self.cash_quote) + i128::from(self.basis_quote),
            "realised_quote",
        )
    }

    #[inline]
    fn roll_basis(&mut self, base_delta: i128, cost_delta: i128) {
        let position = i128::from(self.position_base);
        let closed = if position.signum() == -base_delta.signum() {
            position.abs().min(base_delta.abs())
        } else {
            0
        };
        if closed == 0 {
            self.basis_quote = narrow(i128::from(self.basis_quote) + cost_delta, "basis_quote");
            return;
        }
        let released = i128::from(self.basis_quote) * closed / position.abs();
        let opened = cost_delta - cost_delta * closed / base_delta.abs();
        self.basis_quote = narrow(
            i128::from(self.basis_quote) - released + opened,
            "basis_quote",
        );
    }
}

#[derive(Debug)]
pub(crate) struct PositionLedger {
    rows: Vec<LedgerRow>,
}

impl PositionLedger {
    pub(crate) fn new(instrument_count: usize, restored: &[InstrumentExposure]) -> Self {
        debug_assert!(
            instrument_count <= usize::from(u16::MAX),
            "{instrument_count} instrument rows exceed the u16 instrument-id space"
        );
        let mut rows = vec![LedgerRow::default(); instrument_count];
        for exposure in restored {
            let Some(row) = rows.get_mut(usize::from(exposure.instrument.0)) else {
                continue;
            };
            row.position_base = exposure.position_base.0;
            row.cash_quote = exposure.cash_quote;
            row.basis_quote = exposure.basis_quote;
            row.realised_at_boot = row.realised_quote();
            debug_assert_eq!(
                row.session_realised_quote(),
                0,
                "a restored row must boot having realised nothing THIS session, or the kill switch \
                 measures a previous run's result"
            );
        }
        Self { rows }
    }

    #[inline]
    pub(crate) fn row(&self, instrument: InstrumentId) -> &LedgerRow {
        &self.rows[usize::from(instrument.0)]
    }

    // Rows paired with their instruments (index invariant; callers don't rebuild pairing).
    #[inline]
    pub(crate) fn rows(&self) -> impl Iterator<Item = (InstrumentId, &LedgerRow)> {
        self.rows
            .iter()
            .enumerate()
            .map(|(index, row)| (InstrumentId(index as u16), row))
    }

    /// Folds ONE real fill. The direction is the whole point of this function: buying our own bid
    /// adds base and spends quote, selling our own ask does the mirror. An inversion here compiles,
    /// passes every structural test, and trades backwards — which is why fitness asserts against the
    /// money rather than against the enum.
    #[inline]
    pub(crate) fn apply_fill(&mut self, fill: &LedgerFill) {
        // Side already carries the direction, so a negative size is not a sell — it is a bug that
        // would move the position and the cash the same way and never be visible again. Zero is
        // legitimate: a size rounded down at the scale boundary is a fill of nothing.
        debug_assert!(
            fill.base.0 >= 0 && fill.notional_quote >= 0,
            "a fill's size and value are unsigned by contract, got base {} quote {}",
            fill.base.0,
            fill.notional_quote
        );
        let notional = i128::from(fill.notional_quote);
        let qty = i128::from(fill.base.0);
        // `cost_delta` carries the position's own sign — what a buy PAID, what a sell RECEIVED — so
        // the basis and the position it belongs to always read the same way round. Cash is its
        // mirror, since money leaves on a buy and arrives on a sell.
        let (base_delta, cost_delta) = match fill.side {
            Side::Buy => (qty, notional),
            Side::Sell => (-qty, -notional),
        };
        let row = &mut self.rows[usize::from(fill.instrument.0)];
        row.roll_basis(base_delta, cost_delta);
        row.position_base = narrow(i128::from(row.position_base) + base_delta, "position_base");
        // The commission is a cost whichever way the fill went, and only ever reaches here already
        // filtered to the instrument's own quote asset — a fee paid in some other asset belongs to
        // that asset's balance, not to this instrument's PnL. It never touches the basis: a fee is
        // realised the moment it is paid, and burying it in the cost of an open position would let
        // the kill switch ignore the one cost a maker pays on every single fill.
        row.cash_quote = narrow(
            i128::from(row.cash_quote) - cost_delta - i128::from(fill.commission_quote),
            "cash_quote",
        );
    }

    #[inline]
    pub(crate) fn set_mark(&mut self, instrument: InstrumentId, mark: Price) {
        self.rows[usize::from(instrument.0)].mark = Some(mark);
    }

    /// A rotation hands the slot a NEW window: the position belonged to the old one, and the old
    /// window's mark is a lie about the new one's prices. This is the ONLY reset — a park does not
    /// sell the position, so nothing about a park may zero a row (see [`HotEngine::resume`]).
    ///
    /// The realised leg goes with the window, deliberately: cash here is what the OLD window's fills
    /// banked, and carrying it into a market whose contracts settle separately would report one
    /// window's result against another's prices. Doing better needs a settlement price — keeping
    /// cash while zeroing position assumes the window expired worthless, zeroing both assumes it
    /// broke even, and neither is knowable from a rotation message. Settlement accounting is
    /// deferred until something trades a venue that rotates; it is unreachable today.
    ///
    /// [`HotEngine::resume`]: crate::hot::dispatch::HotEngine
    #[cold]
    pub(crate) fn reset_instrument(&mut self, instrument: InstrumentId) {
        self.rows[usize::from(instrument.0)] = LedgerRow::default();
    }
}

/// Money past `i64@1e-8` (≳ 9.2e10 quote units) is a bug in the fill stream, not a market event, so
/// it panics rather than wrapping research data into nonsense .
#[inline]
pub(crate) fn narrow(value: i128, field: &'static str) -> i64 {
    i64::try_from(value)
        .unwrap_or_else(|_| panic!("position ledger overflow: {field} = {value} leaves i64"))
}
