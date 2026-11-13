//! Estimates of the result of one hedged step from its greeks, so that each step of a run can be
//! split into the parts that caused it.

use super::black76::{DAYS_PER_YEAR, POINTS_PER_UNIT};
use super::error::{
    require_finite_input, require_finite_output, require_not_negative, require_positive,
};
use super::{OptionsError, Shift};

/// One step of a position that is hedged at its delta.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HedgedStep {
    /// Coins for a move of 1 USD, at the start of the step. Any sign.
    pub gamma: f64,
    /// USD for each coin at the start of the step, above 0.
    pub price: f64,
    /// Price at the end over price at the start, less 1.
    pub step_return: f64,
    /// Annual volatility of the hedge delta as a fraction, at least 0.
    pub volatility: f64,
    /// Length of the step in years, at least 0.
    pub years: f64,
}

/// The greeks that explain one step, in the units of `Greeks` and `SecondOrderGreeks`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExplainGreeks {
    pub delta: f64,
    pub gamma: f64,
    pub vega_per_point: f64,
    pub theta_per_day: f64,
    pub vanna_per_point: f64,
    pub volga_per_point_squared: f64,
}

/// The lines of the greek estimate of one step, USD each.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GreekExplain {
    pub delta: f64,
    pub gamma: f64,
    pub theta: f64,
    pub vega: f64,
    pub cross: f64,
}

impl HedgedStep {
    /// USD: `½ × gamma × price² × (step_return² − volatility² × years)`. It is 0 at the move that
    /// the volatility pays for, above 0 for a larger move of a long gamma, and below 0 for a
    /// smaller one.
    ///
    /// # Errors
    ///
    /// `OutsideDomain` names the first field that is not finite or not inside its domain.
    /// `NotFinite` if the result overflows.
    pub fn gamma_theta(&self) -> Result<f64, OptionsError> {
        require_finite_input("gamma", self.gamma)?;
        require_positive("price", self.price)?;
        require_finite_input("step_return", self.step_return)?;
        require_not_negative("volatility", self.volatility)?;
        require_not_negative("years", self.years)?;
        let variance_paid = self.volatility * self.volatility * self.years;
        let result = 0.5
            * self.gamma
            * self.price
            * self.price
            * (self.step_return * self.step_return - variance_paid);
        require_finite_output("gamma_theta", result)
    }
}

/// The greek estimate of one step, line by line: delta × move, ½ × gamma × move², theta over the
/// time of the step, vega × volatility move, and the cross line of vanna and volga.
///
/// # Errors
///
/// `OutsideDomain` names the first field of `greeks`, then of `shift`, that is not finite, or
/// `shift.years` below 0. `NotFinite` if a line overflows.
pub fn greek_explain(greeks: ExplainGreeks, shift: Shift) -> Result<GreekExplain, OptionsError> {
    require_finite_input("greeks.delta", greeks.delta)?;
    require_finite_input("greeks.gamma", greeks.gamma)?;
    require_finite_input("greeks.vega_per_point", greeks.vega_per_point)?;
    require_finite_input("greeks.theta_per_day", greeks.theta_per_day)?;
    require_finite_input("greeks.vanna_per_point", greeks.vanna_per_point)?;
    require_finite_input(
        "greeks.volga_per_point_squared",
        greeks.volga_per_point_squared,
    )?;
    require_finite_input("shift.underlying", shift.underlying)?;
    require_finite_input("shift.volatility", shift.volatility)?;
    require_not_negative("shift.years", shift.years)?;
    let move_in_usd = shift.underlying;
    let points = shift.volatility * POINTS_PER_UNIT;
    let cross = greeks.vanna_per_point * move_in_usd * shift.volatility * POINTS_PER_UNIT
        + 0.5 * greeks.volga_per_point_squared * points * points;
    Ok(GreekExplain {
        delta: require_finite_output("delta", greeks.delta * move_in_usd)?,
        gamma: require_finite_output("gamma", 0.5 * greeks.gamma * move_in_usd * move_in_usd)?,
        theta: require_finite_output("theta", greeks.theta_per_day * shift.years * DAYS_PER_YEAR)?,
        vega: require_finite_output(
            "vega",
            greeks.vega_per_point * shift.volatility * POINTS_PER_UNIT,
        )?,
        cross: require_finite_output("cross", cross)?,
    })
}

impl GreekExplain {
    /// USD: the five lines added in the order delta, gamma, theta, vega, cross. The order is
    /// fixed, so that each run adds them the same way.
    ///
    /// # Errors
    ///
    /// `OutsideDomain` names the first line that is not finite. `NotFinite` if the sum overflows.
    pub fn total(&self) -> Result<f64, OptionsError> {
        require_finite_input("delta", self.delta)?;
        require_finite_input("gamma", self.gamma)?;
        require_finite_input("theta", self.theta)?;
        require_finite_input("vega", self.vega)?;
        require_finite_input("cross", self.cross)?;
        require_finite_output(
            "total",
            self.delta + self.gamma + self.theta + self.vega + self.cross,
        )
    }
}
