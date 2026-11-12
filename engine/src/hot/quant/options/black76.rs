//! Black-76 value and closed-form greeks of one European option on a forward.

use std::f64::consts::{FRAC_1_SQRT_2, FRAC_2_SQRT_PI};

use super::error::{require_finite_output, require_not_negative, require_positive};
use super::{OptionsError, exp, ln};

/// One point of volatility is 0.01.
const POINTS_PER_UNIT: f64 = 100.0;

const DAYS_PER_YEAR: f64 = 365.0;

const INVERSE_SQRT_TWO_PI: f64 = FRAC_1_SQRT_2 * FRAC_2_SQRT_PI * 0.5;

/// Below this argument erfc comes from the series of erf, and above it from the continued
/// fraction. Near this point the two lose about the same accuracy: the series to the cancellation
/// in 1 − erf, and the fraction to its fixed depth.
const ERFC_SPLIT: f64 = 1.5;

/// The smallest count with which the reference prices pass, plus a quarter.
const ERF_SERIES_TERMS: u32 = 24;

/// The smallest depth with which the reference prices pass, plus a quarter.
const ERFC_FRACTION_DEPTH: u32 = 67;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OptionRight {
    Call,
    Put,
}

/// One European option on a forward, valued for one coin of notional. All carry sits inside the
/// forward, and discounting enters only through the discount factor.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Black76 {
    /// USD for each coin, above 0.
    pub forward: f64,
    /// USD for each coin, above 0.
    pub strike: f64,
    /// Variance time to expiry in years of 365 days, at least 0.
    pub years: f64,
    /// Discount factor of the currency that pays the premium, above 0.
    pub discount: f64,
}

/// Closed-form greeks of one option, for one coin of notional.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Greeks {
    /// USD.
    pub price: f64,
    /// Coins: the change of value for a move of 1 USD of the forward.
    pub delta: f64,
    /// Coins for a move of 1 USD of the forward. For a move of 1%, multiply by forward / 100.
    pub gamma: f64,
    /// USD for one point of volatility.
    pub vega_per_point: f64,
    /// USD for one day of variance time, with the discount factor held. Never above 0.
    pub theta_per_day: f64,
}

/// One call and one put of one strike and one expiry, quoted in coin. They give the forward of
/// that expiry.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CoinParity {
    /// Coins for each coin of notional, at least 0.
    pub call: f64,
    /// Coins for each coin of notional, at least 0.
    pub put: f64,
    /// USD for each coin, above 0.
    pub strike: f64,
    /// Discount factor of the coin, above 0.
    pub discount: f64,
}

impl Black76 {
    /// USD for each coin of notional. `volatility` is annual, as a fraction: 0.5 is 50%. With no
    /// volatility or no time left, the price is the discounted intrinsic value.
    ///
    /// # Errors
    ///
    /// `OutsideDomain` names the first input, in the order of the fields and then the arguments,
    /// that is not finite or not inside its domain. `NotFinite` if the price overflows.
    pub fn price(&self, right: OptionRight, volatility: f64) -> Result<f64, OptionsError> {
        self.check_market()?;
        require_not_negative("volatility", volatility)?;
        require_finite_output("price", self.price_inside_domain(right, volatility))
    }

    /// Coins for each coin of notional: the price divided by the forward.
    ///
    /// # Errors
    ///
    /// As [`Black76::price`].
    pub fn coin_price(&self, right: OptionRight, volatility: f64) -> Result<f64, OptionsError> {
        let price = self.price(right, volatility)?;
        require_finite_output("coin_price", price / self.forward)
    }

    /// # Errors
    ///
    /// As [`Black76::price`]. Also `OutsideDomain` for no time left or no volatility, where delta
    /// jumps and gamma has no value.
    pub fn greeks(&self, right: OptionRight, volatility: f64) -> Result<Greeks, OptionsError> {
        self.check_market_before_expiry()?;
        require_positive("volatility", volatility)?;
        let root_years = self.years.sqrt();
        let deviation = volatility * root_years;
        let (d1, d2) = d1_d2(self.log_moneyness(), deviation);
        let density = normal_density(d1);
        let delta = match right {
            OptionRight::Call => self.discount * cumulative_normal(d1),
            OptionRight::Put => -self.discount * cumulative_normal(-d1),
        };
        let cash_density = self.discount * self.forward * density;
        let theta_per_year = -cash_density * volatility / (2.0 * root_years);
        Ok(Greeks {
            price: require_finite_output("price", self.price_from(right, d1, d2))?,
            delta: require_finite_output("delta", delta)?,
            gamma: require_finite_output(
                "gamma",
                self.discount * density / (self.forward * deviation),
            )?,
            vega_per_point: require_finite_output(
                "vega_per_point",
                cash_density * root_years / POINTS_PER_UNIT,
            )?,
            theta_per_day: require_finite_output("theta_per_day", theta_per_year / DAYS_PER_YEAR)?,
        })
    }

    /// Coins: the delta of the value in coin for a move of the forward, times the forward. It
    /// equals the delta less the coin price.
    ///
    /// # Errors
    ///
    /// As [`Black76::greeks`].
    pub fn coin_delta(&self, right: OptionRight, volatility: f64) -> Result<f64, OptionsError> {
        self.check_market_before_expiry()?;
        require_positive("volatility", volatility)?;
        let (_, d2) = d1_d2(self.log_moneyness(), volatility * self.years.sqrt());
        let discounted_strike_in_coins = self.discount * (self.strike / self.forward);
        let coin_delta = match right {
            OptionRight::Call => discounted_strike_in_coins * cumulative_normal(d2),
            OptionRight::Put => -discounted_strike_in_coins * cumulative_normal(-d2),
        };
        require_finite_output("coin_delta", coin_delta)
    }

    /// USD for each 1 USD of payoff: the value of a digital call at the strike, from two calls at
    /// `strike ± half_width`, each with the volatility of its own strike.
    ///
    /// # Errors
    ///
    /// As [`Black76::price`], with `half_width` above 0 and below the strike. Also
    /// `OutsideDomain` that names `high_volatility` when the two volatilities put the value below
    /// 0 or above the discount factor, which no call spread can do.
    pub fn digital_from_call_spread(
        &self,
        half_width: f64,
        low_volatility: f64,
        high_volatility: f64,
    ) -> Result<f64, OptionsError> {
        self.check_market()?;
        require_positive("half_width", half_width)?;
        if half_width >= self.strike {
            return Err(OptionsError::OutsideDomain {
                input: "half_width",
                value: half_width,
            });
        }
        require_not_negative("low_volatility", low_volatility)?;
        require_not_negative("high_volatility", high_volatility)?;
        let low_strike = Black76 {
            strike: self.strike - half_width,
            ..*self
        };
        let high_strike = Black76 {
            strike: self.strike + half_width,
            ..*self
        };
        let spread = low_strike.price_inside_domain(OptionRight::Call, low_volatility)
            - high_strike.price_inside_domain(OptionRight::Call, high_volatility);
        let digital = require_finite_output("digital", spread / (2.0 * half_width))?;
        if !(0.0..=self.discount).contains(&digital) {
            return Err(OptionsError::OutsideDomain {
                input: "high_volatility",
                value: high_volatility,
            });
        }
        Ok(digital)
    }

    fn check_market(&self) -> Result<(), OptionsError> {
        require_positive("forward", self.forward)?;
        require_positive("strike", self.strike)?;
        require_not_negative("years", self.years)?;
        require_positive("discount", self.discount)?;
        Ok(())
    }

    pub(super) fn check_market_before_expiry(&self) -> Result<(), OptionsError> {
        require_positive("forward", self.forward)?;
        require_positive("strike", self.strike)?;
        require_positive("years", self.years)?;
        require_positive("discount", self.discount)?;
        Ok(())
    }

    fn price_inside_domain(&self, right: OptionRight, volatility: f64) -> f64 {
        let deviation = volatility * self.years.sqrt();
        if deviation <= 0.0 {
            return self.intrinsic_value(right);
        }
        self.price_at(right, self.log_moneyness(), deviation)
    }

    pub(super) fn log_moneyness(&self) -> f64 {
        ln(self.forward / self.strike)
    }

    /// `log_moneyness` is `ln(forward / strike)`, and `deviation` is `volatility × √years`,
    /// above 0.
    pub(super) fn price_at(&self, right: OptionRight, log_moneyness: f64, deviation: f64) -> f64 {
        let (d1, d2) = d1_d2(log_moneyness, deviation);
        self.price_from(right, d1, d2)
    }

    fn price_from(&self, right: OptionRight, d1: f64, d2: f64) -> f64 {
        let Black76 {
            forward,
            strike,
            discount,
            ..
        } = *self;
        match right {
            OptionRight::Call => {
                discount * (forward * cumulative_normal(d1) - strike * cumulative_normal(d2))
            }
            OptionRight::Put => {
                discount * (strike * cumulative_normal(-d2) - forward * cumulative_normal(-d1))
            }
        }
    }

    fn intrinsic_value(&self, right: OptionRight) -> f64 {
        let payoff = match right {
            OptionRight::Call => self.forward - self.strike,
            OptionRight::Put => self.strike - self.forward,
        };
        if payoff > 0.0 { self.discount * payoff } else { 0.0 }
    }
}

/// `deviation` is `volatility × √years`, above 0.
fn d1_d2(log_moneyness: f64, deviation: f64) -> (f64, f64) {
    let d1 = (log_moneyness + 0.5 * deviation * deviation) / deviation;
    (d1, d1 - deviation)
}

impl CoinParity {
    /// USD for each coin: `strike / (1 − (call − put) / discount)`.
    ///
    /// # Errors
    ///
    /// `OutsideDomain` names the first field that is not finite or not inside its domain. It
    /// names `call` when `(call − put) / discount` is 1 or more, because then no forward fits.
    /// `NotFinite` if the forward overflows.
    pub fn forward(&self) -> Result<f64, OptionsError> {
        require_not_negative("call", self.call)?;
        require_not_negative("put", self.put)?;
        require_positive("strike", self.strike)?;
        require_positive("discount", self.discount)?;
        let strike_in_coins = 1.0 - (self.call - self.put) / self.discount;
        if strike_in_coins <= 0.0 {
            return Err(OptionsError::OutsideDomain {
                input: "call",
                value: self.call,
            });
        }
        require_finite_output("forward", self.strike / strike_in_coins)
    }
}

/// The puts take Φ(−d) and never 1 − Φ(d), which would lose each digit of a small value.
fn cumulative_normal(x: f64) -> f64 {
    0.5 * erfc(-x * FRAC_1_SQRT_2)
}

fn normal_density(x: f64) -> f64 {
    exp(-0.5 * x * x) * INVERSE_SQRT_TWO_PI
}

fn erfc(x: f64) -> f64 {
    if x < 0.0 {
        return 2.0 - erfc(-x);
    }
    if x < ERFC_SPLIT { 1.0 - erf_by_series(x) } else { erfc_by_continued_fraction(x) }
}

/// erf x = (2 / √π) e^(−x²) Σ 2^n x^(2n+1) / (1 × 3 × … × (2n+1)). Each term is positive, so the
/// sum loses nothing to cancellation.
fn erf_by_series(x: f64) -> f64 {
    let twice_square = 2.0 * x * x;
    let mut series = 1.0;
    for n in (1..ERF_SERIES_TERMS).rev() {
        series = 1.0 + twice_square / f64::from(2 * n + 1) * series;
    }
    FRAC_2_SQRT_PI * exp(-x * x) * x * series
}

/// erfc x = e^(−x²) / √π / (x + (1/2) / (x + 1 / (x + (3/2) / (x + …)))), evaluated from its
/// deepest level back to the top.
fn erfc_by_continued_fraction(x: f64) -> f64 {
    let mut fraction = x;
    for n in (1..=ERFC_FRACTION_DEPTH).rev() {
        fraction = x + 0.5 * f64::from(n) / fraction;
    }
    exp(-x * x) * (0.5 * FRAC_2_SQRT_PI) / fraction
}
