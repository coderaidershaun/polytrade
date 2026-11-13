//! The half-width of the band inside which a hedge is left alone, because a trade there costs
//! more than the risk that it removes.

use super::error::{
    require_finite_input, require_finite_output, require_not_negative, require_positive,
};
use super::{OptionsError, cbrt};

/// The inputs of the band of one hedge instrument.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NoTradeBand {
    /// Cost of a trade as a fraction of the notional traded, at least 0.
    pub cost: f64,
    /// Quote currency for one unit of the hedge instrument, above 0.
    pub price: f64,
    /// Change of the net delta, in units of the hedge instrument, for a move of 1 of `price`.
    /// Any sign: only its square counts.
    pub gamma: f64,
    /// For each unit of quote currency, above 0.
    pub risk_aversion: f64,
}

impl NoTradeBand {
    /// Units of the hedge instrument, at least 0: the cube root of
    /// `3 × cost × price × gamma² / (2 × risk_aversion)`. Zero gamma or zero cost gives 0, and the
    /// caller clamps to its own smallest band.
    ///
    /// # Errors
    ///
    /// `OutsideDomain` names the first field that is not finite or not inside its domain.
    /// `NotFinite` if the half-width overflows.
    pub fn half_width(&self) -> Result<f64, OptionsError> {
        require_not_negative("cost", self.cost)?;
        require_positive("price", self.price)?;
        require_finite_input("gamma", self.gamma)?;
        require_positive("risk_aversion", self.risk_aversion)?;
        let radicand =
            3.0 * self.cost * self.price * (self.gamma * self.gamma) / (2.0 * self.risk_aversion);
        require_finite_output("half_width", cbrt(radicand))
    }
}
