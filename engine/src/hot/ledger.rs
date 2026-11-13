//! The position, cash and cost basis of each instrument, so the engine can value its own PnL: a
//! strategy's counters drop the fill prices. Each amount is an exact mantissa, never a float.

use crate::hot::digest::{digest_by_hash, digest_fields};
use crate::msg::ids::{InstrumentId, Notional, Price, Qty, Side};
use crate::runtime::exposure::InstrumentExposure;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct LedgerFill {
    pub(crate) instrument: InstrumentId,
    pub(crate) side: Side,
    pub(crate) base: Qty,
    pub(crate) notional_quote: Notional,
    pub(crate) commission_quote: Notional,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) struct LedgerRow {
    position_base: Qty,
    cash_quote: Notional,
    basis_quote: Notional,
    realised_at_boot: Notional,
    mark: Option<Price>,
}

impl LedgerRow {
    #[inline]
    pub(crate) fn has_mark(&self) -> bool {
        self.mark.is_some()
    }

    #[inline]
    pub(crate) fn position_base(&self) -> Qty {
        self.position_base
    }

    #[inline]
    pub(crate) fn cash_quote(&self) -> Notional {
        self.cash_quote
    }

    #[inline]
    pub(crate) fn basis_quote(&self) -> Notional {
        self.basis_quote
    }

    #[inline]
    pub(crate) fn exposure_quote(&self) -> Notional {
        self.mark
            .map_or(Notional::ZERO, |mark| mark.notional(self.position_base))
    }

    #[inline]
    pub(crate) fn pnl_quote(&self) -> Notional {
        narrow_quote(
            wide(self.cash_quote) + wide(self.exposure_quote()),
            "pnl_quote",
        )
    }

    #[inline]
    pub(crate) fn session_realised_quote(&self) -> Notional {
        narrow_quote(
            wide(self.realised_quote()) - wide(self.realised_at_boot),
            "session_realised_quote",
        )
    }

    #[inline]
    fn realised_quote(&self) -> Notional {
        narrow_quote(
            wide(self.cash_quote) + wide(self.basis_quote),
            "realised_quote",
        )
    }

    #[inline]
    fn roll_basis(&mut self, base_delta: i128, cost_delta: i128) {
        let position = i128::from(self.position_base.mantissa());
        let closed = if position.signum() == -base_delta.signum() {
            position.abs().min(base_delta.abs())
        } else {
            0
        };
        if closed == 0 {
            self.basis_quote = narrow_quote(wide(self.basis_quote) + cost_delta, "basis_quote");
            return;
        }
        let released = wide(self.basis_quote) * closed / position.abs();
        let opened = cost_delta - cost_delta * closed / base_delta.abs();
        self.basis_quote = narrow_quote(wide(self.basis_quote) - released + opened, "basis_quote");
    }
}

#[derive(Debug)]
pub(crate) struct PositionLedger {
    rows: Vec<LedgerRow>,
}

digest_by_hash!(LedgerRow);
digest_fields! { PositionLedger { rows } }

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
            row.position_base = exposure.position_base;
            row.cash_quote = Notional::from_mantissa(exposure.cash_quote);
            row.basis_quote = Notional::from_mantissa(exposure.basis_quote);
            row.realised_at_boot = row.realised_quote();
            debug_assert_eq!(
                row.session_realised_quote(),
                Notional::ZERO,
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
            fill.base.mantissa() >= 0 && fill.notional_quote >= Notional::ZERO,
            "a fill's size and value are unsigned by contract, got base {} quote {}",
            fill.base.mantissa(),
            fill.notional_quote.mantissa()
        );
        let notional = wide(fill.notional_quote);
        let qty = i128::from(fill.base.mantissa());
        // `cost_delta` carries the position's own sign — what a buy PAID, what a sell RECEIVED — so
        // the basis and the position it belongs to always read the same way round. Cash is its
        // mirror, since money leaves on a buy and arrives on a sell.
        let (base_delta, cost_delta) = match fill.side {
            Side::Buy => (qty, notional),
            Side::Sell => (-qty, -notional),
        };
        let row = &mut self.rows[usize::from(fill.instrument.0)];
        row.roll_basis(base_delta, cost_delta);
        row.position_base = Qty::from_mantissa(narrow(
            i128::from(row.position_base.mantissa()) + base_delta,
            "position_base",
        ));
        // The commission is a cost whichever way the fill went, and only ever reaches here already
        // filtered to the instrument's own quote asset — a fee paid in some other asset belongs to
        // that asset's balance, not to this instrument's PnL. It never touches the basis: a fee is
        // realised the moment it is paid, and burying it in the cost of an open position would let
        // the kill switch ignore the one cost a maker pays on every single fill.
        row.cash_quote = narrow_quote(
            wide(row.cash_quote) - cost_delta - wide(fill.commission_quote),
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

/// The quote money of an exact sum, with the panic of [`narrow`].
#[inline]
pub(crate) fn narrow_quote(value: i128, field: &'static str) -> Notional {
    Notional::from_mantissa(narrow(value, field))
}

/// Quote money widened for a sum that cannot wrap.
#[inline]
pub(crate) fn wide(quote: Notional) -> i128 {
    i128::from(quote.mantissa())
}
