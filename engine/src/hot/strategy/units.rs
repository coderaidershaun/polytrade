//! The units of a strategy's numbers: exact price, quantity and money, and the fair value, basis
//! points and ticks that a model works in. Only the sums that make sense compile, and no float
//! becomes an exact amount: a fair value becomes a price only through a verb of `Market` that
//! names its rounding.

use std::ops::{Add, Mul, Sub};

pub use crate::msg::ids::{Notional, Price, Qty};

/// An estimate of what a market is worth, in the price unit of the market. A model makes one; it
/// is never an amount that an order carries.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default)]
pub struct Fair(f64);

/// A distance relative to a fair value, in basis points: 10,000 basis points are the fair value
/// itself.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default)]
pub struct Bps(f64);

/// A whole number of steps of a market's price grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Ticks(i64);

impl Fair {
    pub const fn new(estimate: f64) -> Self {
        Self(estimate)
    }

    pub const fn value(self) -> f64 {
        self.0
    }

    pub fn is_finite(self) -> bool {
        self.0.is_finite()
    }
}

/// An exact price is a fair value with no error, so this way needs no rounding.
impl From<Price> for Fair {
    fn from(price: Price) -> Self {
        Self(price.to_f64())
    }
}

impl Bps {
    pub const fn new(distance: f64) -> Self {
        Self(distance)
    }

    pub const fn value(self) -> f64 {
        self.0
    }
}

impl Ticks {
    pub const fn new(count: i64) -> Self {
        Self(count)
    }

    pub const fn count(self) -> i64 {
        self.0
    }
}

/// `fair - distance` is `fair · (1 − bps / 10,000)`: the distance is relative to the fair value it
/// moves, so 10 bps below 100.00 is 99.90.
impl Sub<Bps> for Fair {
    type Output = Fair;

    fn sub(self, distance: Bps) -> Fair {
        Fair(self.0 - self.0 * distance.0 / BPS_PER_UNIT)
    }
}

/// `fair + distance` is `fair · (1 + bps / 10,000)`, the mirror of the subtraction.
impl Add<Bps> for Fair {
    type Output = Fair;

    fn add(self, distance: Bps) -> Fair {
        Fair(self.0 + self.0 * distance.0 / BPS_PER_UNIT)
    }
}

const BPS_PER_UNIT: f64 = 10_000.0;

impl Add for Bps {
    type Output = Bps;

    fn add(self, other: Bps) -> Bps {
        Bps(self.0 + other.0)
    }
}

impl Sub for Bps {
    type Output = Bps;

    fn sub(self, other: Bps) -> Bps {
        Bps(self.0 - other.0)
    }
}

impl Mul<f64> for Bps {
    type Output = Bps;

    fn mul(self, factor: f64) -> Bps {
        Bps(self.0 * factor)
    }
}

impl Add for Ticks {
    type Output = Ticks;

    fn add(self, other: Ticks) -> Ticks {
        Ticks(self.0.checked_add(other.0).expect("a sum of ticks fits"))
    }
}

impl Sub for Ticks {
    type Output = Ticks;

    fn sub(self, other: Ticks) -> Ticks {
        Ticks(
            self.0
                .checked_sub(other.0)
                .expect("a difference of ticks fits"),
        )
    }
}

impl Add for Qty {
    type Output = Qty;

    fn add(self, other: Qty) -> Qty {
        Qty::from_mantissa(
            self.mantissa()
                .checked_add(other.mantissa())
                .expect("a sum of quantities fits the mantissa"),
        )
    }
}

impl Sub for Qty {
    type Output = Qty;

    fn sub(self, other: Qty) -> Qty {
        Qty::from_mantissa(
            self.mantissa()
                .checked_sub(other.mantissa())
                .expect("a difference of quantities fits the mantissa"),
        )
    }
}

/// The money of `qty` at this price, exact and truncated toward zero, as [`Price::notional`].
impl Mul<Qty> for Price {
    type Output = Notional;

    fn mul(self, qty: Qty) -> Notional {
        self.notional(qty)
    }
}
