//! Implied volatility of one option by a fixed count of halvings, so that each solve costs the same
//! and always ends.

use super::error::{require_finite_output, require_positive};
use super::{Black76, OptionRight, OptionsError};

pub(super) const LOWEST_VOLATILITY: f64 = 0.0001;

pub(super) const HIGHEST_VOLATILITY: f64 = 100.0;

/// Each step halves the bracket in the logarithm of volatility. After 48 steps it is 4.9e-14 of the
/// volatility wide.
const HALVINGS: u32 = 48;

impl Black76 {
    /// Annual volatility, as a fraction, that gives `price` for `right`. The price is in USD for
    /// each coin of notional, discounted. A price quoted in coin goes in times the forward.
    ///
    /// The solve runs on the out-of-the-money leg, which parity gives from the other one. Its
    /// price keeps its digits where the price of the other leg is mostly intrinsic value.
    ///
    /// # Errors
    ///
    /// `OutsideDomain` names the first input, in the order of the fields and then the arguments,
    /// that is not finite or not inside its domain: years and price must be above 0. `NotFinite`
    /// names `low` or `high` when a price at a bound overflows. `NoRoot` when no volatility inside
    /// the bounds gives the price. It carries the prices of `right` at the two bounds, and a price
    /// at or below intrinsic value is one such case.
    pub fn implied_volatility(&self, right: OptionRight, price: f64) -> Result<f64, OptionsError> {
        self.check_market_before_expiry()?;
        require_positive("price", price)?;
        let leg = self.out_of_the_money_right();
        let target = self.parity_price(right, price, leg);
        let log_moneyness = self.log_moneyness();
        let root_years = self.years.sqrt();
        let leg_price =
            |volatility: f64| self.price_at(leg, log_moneyness, volatility * root_years);
        let leg_low = leg_price(LOWEST_VOLATILITY);
        let leg_high = leg_price(HIGHEST_VOLATILITY);
        let low_bound = require_finite_output("low", self.parity_price(leg, leg_low, right))?;
        let high_bound = require_finite_output("high", self.parity_price(leg, leg_high, right))?;
        let is_bracketed = target > leg_low && target < leg_high;
        if !is_bracketed {
            return Err(OptionsError::NoRoot {
                price,
                low: low_bound,
                high: high_bound,
            });
        }
        let mut low = LOWEST_VOLATILITY;
        let mut high = HIGHEST_VOLATILITY;
        for _ in 0..HALVINGS {
            let middle = (low * high).sqrt();
            if leg_price(middle) < target {
                low = middle;
            } else {
                high = middle;
            }
        }
        require_finite_output("implied_volatility", (low * high).sqrt())
    }

    fn out_of_the_money_right(&self) -> OptionRight {
        if self.forward > self.strike { OptionRight::Put } else { OptionRight::Call }
    }

    /// The price of `to` at the strike, expiry and volatility that give `from` the price `price`:
    /// call less put is the discount times forward less strike.
    fn parity_price(&self, from: OptionRight, price: f64, to: OptionRight) -> f64 {
        let call_less_put = self.discount * (self.forward - self.strike);
        match (from, to) {
            (OptionRight::Call, OptionRight::Put) => price - call_less_put,
            (OptionRight::Put, OptionRight::Call) => price + call_less_put,
            _ => price,
        }
    }
}
