//! The one error type of the options maths, so that a caller can count each refusal by its cause.

use super::implied_volatility::{HIGHEST_VOLATILITY, LOWEST_VOLATILITY};

/// Each variant refuses an input or a result. None of them is a fault of the code.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum OptionsError {
    #[error("{input} is {value}, outside its domain")]
    OutsideDomain { input: &'static str, value: f64 },

    #[error("{output} is not finite, though each input is inside its domain")]
    NotFinite { output: &'static str },

    #[error(
        "no volatility from {lowest} to {highest} gives price {price}: those bounds price at {low} and {high}",
        lowest = LOWEST_VOLATILITY,
        highest = HIGHEST_VOLATILITY
    )]
    NoRoot { price: f64, low: f64, high: f64 },

    #[error("total variance falls from {near} at the near expiry to {far} at the far expiry")]
    CalendarArbitrage { near: f64, far: f64 },

    #[error("the butterfly at strike {strike} is {value}, below the slack")]
    NegativeButterfly { strike: f64, value: f64 },

    #[error("value {index} rises by {rise} when the volatility level is bumped down")]
    PerverseResponse { index: usize, rise: f64 },
}

// Each check below states the condition that must hold. Every comparison with NaN is false, so NaN
// fails each check.

pub(crate) fn require_positive(input: &'static str, value: f64) -> Result<f64, OptionsError> {
    let is_inside = value > 0.0 && value < f64::INFINITY;
    if is_inside { Ok(value) } else { Err(OptionsError::OutsideDomain { input, value }) }
}

pub(crate) fn require_not_negative(input: &'static str, value: f64) -> Result<f64, OptionsError> {
    let is_inside = (0.0..f64::INFINITY).contains(&value);
    if is_inside { Ok(value) } else { Err(OptionsError::OutsideDomain { input, value }) }
}

pub(crate) fn require_finite_output(output: &'static str, value: f64) -> Result<f64, OptionsError> {
    let is_finite = value.abs() < f64::INFINITY;
    if is_finite { Ok(value) } else { Err(OptionsError::NotFinite { output }) }
}
