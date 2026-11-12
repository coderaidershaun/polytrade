//! Checks that refuse a set of marks that holds an arbitrage, so that no quote or hedge is built on
//! it.

use super::OptionsError;
use super::error::{
    require_finite_input, require_finite_output, require_not_negative, require_positive,
};

/// One expiry of a term structure of volatility.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pillar {
    /// Variance time to expiry in years, at least 0.
    pub years: f64,
    /// Annual volatility as a fraction, at least 0.
    pub volatility: f64,
}

impl Pillar {
    fn total_variance(&self) -> f64 {
        self.volatility * self.volatility * self.years
    }
}

/// Volatility from the near expiry to the far one: √((σ₂² τ₂ − σ₁² τ₁) / (τ₂ − τ₁)).
///
/// # Errors
///
/// `OutsideDomain` names the first field of `near`, then of `far`, that is not finite or not
/// inside its domain: years and volatility at least 0, and far years above near years.
/// `CalendarArbitrage` carries both total variances when the far one is below the near one.
/// `NotFinite` if a variance overflows.
pub fn forward_volatility(near: Pillar, far: Pillar) -> Result<f64, OptionsError> {
    let (near_variance, far_variance) = total_variances(near, far)?;
    let forward_variance = (far_variance - near_variance) / (far.years - near.years);
    require_finite_output("forward_volatility", forward_variance.sqrt())
}

/// Volatility to the expiry `years` between the two, with total variance linear in time from one
/// expiry to the other.
///
/// # Errors
///
/// As [`forward_volatility`], with near years above 0. Then `OutsideDomain` names `years` when it
/// lies outside the two expiries.
pub fn volatility_between(near: Pillar, far: Pillar, years: f64) -> Result<f64, OptionsError> {
    require_positive("near.years", near.years)?;
    let (near_variance, far_variance) = total_variances(near, far)?;
    if !(near.years..=far.years).contains(&years) {
        return Err(OptionsError::OutsideDomain {
            input: "years",
            value: years,
        });
    }
    let variance = near_variance
        + (far_variance - near_variance) * (years - near.years) / (far.years - near.years);
    require_finite_output("volatility_between", (variance / years).sqrt())
}

/// The lowest butterfly of a slice of prices of one right and one expiry. At each inner strike it
/// is `((K₊ − K) P₋ + (K − K₋) P₊) / (K₊ − K₋) − P`, which for equal spacing is half of the
/// butterfly spread of the three options. A sound slice has no butterfly below 0.
///
/// # Errors
///
/// `OutsideDomain` names `strikes`, with its length when it holds fewer than 3, or with the first
/// strike that is not above 0 and above the one before. Then it names `prices`, with its length
/// when it differs from that of `strikes`, or with the first price below 0. Then `slack` below 0.
/// `NegativeButterfly` carries the strike and the value of the lowest butterfly when that is
/// below `−slack`.
pub fn lowest_butterfly(strikes: &[f64], prices: &[f64], slack: f64) -> Result<f64, OptionsError> {
    if strikes.len() < 3 {
        return Err(OptionsError::OutsideDomain {
            input: "strikes",
            value: strikes.len() as f64,
        });
    }
    let mut previous = 0.0;
    for &strike in strikes {
        require_positive("strikes", strike)?;
        if strike <= previous {
            return Err(OptionsError::OutsideDomain {
                input: "strikes",
                value: strike,
            });
        }
        previous = strike;
    }
    if prices.len() != strikes.len() {
        return Err(OptionsError::OutsideDomain {
            input: "prices",
            value: prices.len() as f64,
        });
    }
    for &price in prices {
        require_not_negative("prices", price)?;
    }
    require_not_negative("slack", slack)?;

    let mut lowest = f64::INFINITY;
    let mut lowest_strike = 0.0;
    for (three_strikes, three_prices) in strikes.windows(3).zip(prices.windows(3)) {
        let butterfly = require_finite_output("butterfly", butterfly(three_strikes, three_prices))?;
        if butterfly < lowest {
            lowest = butterfly;
            lowest_strike = three_strikes[1];
        }
    }
    if lowest < -slack {
        return Err(OptionsError::NegativeButterfly {
            strike: lowest_strike,
            value: lowest,
        });
    }
    Ok(lowest)
}

/// The largest rise of a value when the caller bumps the volatility level down. In sound marks no
/// value rises then.
///
/// # Errors
///
/// `OutsideDomain` names `before`, with its length when it is empty, or with its first value that
/// is not finite. Then it names `after`, with its length when it differs from that of `before`,
/// or with its first value that is not finite. Then `slack` below 0. `PerverseResponse` carries
/// the index and the size of the largest rise when that is above `slack`.
pub fn largest_rise(before: &[f64], after: &[f64], slack: f64) -> Result<f64, OptionsError> {
    if before.is_empty() {
        return Err(OptionsError::OutsideDomain {
            input: "before",
            value: before.len() as f64,
        });
    }
    for &value in before {
        require_finite_input("before", value)?;
    }
    if after.len() != before.len() {
        return Err(OptionsError::OutsideDomain {
            input: "after",
            value: after.len() as f64,
        });
    }
    for &value in after {
        require_finite_input("after", value)?;
    }
    require_not_negative("slack", slack)?;

    let mut largest = f64::NEG_INFINITY;
    let mut largest_index = 0;
    for (index, (was, is)) in before.iter().zip(after).enumerate() {
        let rise = require_finite_output("rise", is - was)?;
        if rise > largest {
            largest = rise;
            largest_index = index;
        }
    }
    if largest > slack {
        return Err(OptionsError::PerverseResponse {
            index: largest_index,
            rise: largest,
        });
    }
    Ok(largest)
}

fn total_variances(near: Pillar, far: Pillar) -> Result<(f64, f64), OptionsError> {
    require_not_negative("near.years", near.years)?;
    require_not_negative("near.volatility", near.volatility)?;
    require_not_negative("far.years", far.years)?;
    if far.years <= near.years {
        return Err(OptionsError::OutsideDomain {
            input: "far.years",
            value: far.years,
        });
    }
    require_not_negative("far.volatility", far.volatility)?;
    let near_variance = require_finite_output("near_total_variance", near.total_variance())?;
    let far_variance = require_finite_output("far_total_variance", far.total_variance())?;
    if far_variance < near_variance {
        return Err(OptionsError::CalendarArbitrage {
            near: near_variance,
            far: far_variance,
        });
    }
    Ok((near_variance, far_variance))
}

/// The butterfly of three strikes and their prices, each a window of length 3.
fn butterfly(strikes: &[f64], prices: &[f64]) -> f64 {
    let (low, middle, high) = (strikes[0], strikes[1], strikes[2]);
    ((high - middle) * prices[0] + (middle - low) * prices[2]) / (high - low) - prices[1]
}
