//! The knobs of a strategy, read from `strategy.params` of its config. A key the strategy does not
//! read, or a value out of its bounds, refuses the start and names the param, before any thread.

use std::fmt;

use serde::de::{DeserializeOwned, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::msg::ids::FIXED_SCALE;
use crate::runtime::config::NoParams;

use super::units::{Bps, Notional, Price, Qty, Ticks};

/// The params type of a strategy. The config refuses each key that the type does not write back
/// when it is serialized, so a misspelt knob refuses the start whatever serde attributes the type
/// carries. `params!` writes the whole impl from one declaration.
pub trait StrategyParams: DeserializeOwned + Serialize + Default + Clone + std::fmt::Debug {
    /// Each bound that a value must keep and that its type alone does not hold. The default holds
    /// none.
    ///
    /// # Errors
    /// [`ParamError::OutOfBounds`] or [`ParamError::Refused`] for the first param that breaks one.
    fn check(&self) -> Result<(), ParamError> {
        Ok(())
    }
}

impl StrategyParams for NoParams {}

/// Why the params of a config refuse the start.
#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum ParamError {
    #[error("strategy.params.{field} is not a param of this strategy; its params are: {known}")]
    Unknown { field: Box<str>, known: Box<str> },
    #[error("strategy.params.{field} is {seen}, expected {low} to {high}")]
    OutOfBounds {
        field: &'static str,
        seen: ParamValue,
        low: ParamValue,
        high: ParamValue,
    },
    #[error("strategy.params.{field} is {seen}, expected {expected}")]
    Refused {
        field: &'static str,
        seen: ParamValue,
        expected: &'static str,
    },
}

/// The value of one param as a refusal shows it: an exact amount prints as its decimal, so the
/// value in the message is the value the config wrote.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ParamValue {
    /// A mantissa of an exact amount.
    Exact(i64),
    Integer(i64),
    Float(f64),
    Bool(bool),
}

impl fmt::Display for ParamValue {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ParamValue::Exact(mantissa) => write_decimal(mantissa, out),
            ParamValue::Integer(value) => fmt::Display::fmt(&value, out),
            ParamValue::Float(value) => fmt::Display::fmt(&value, out),
            ParamValue::Bool(value) => fmt::Display::fmt(&value, out),
        }
    }
}

/// The decimal of a mantissa at the fixed scale, with no trailing zeros, so a refusal shows the
/// digits that the config wrote.
fn write_decimal(mantissa: i64, out: &mut fmt::Formatter<'_>) -> fmt::Result {
    if mantissa < 0 {
        out.write_str("-")?;
    }
    let scale = FIXED_SCALE.unsigned_abs();
    let (whole, mut fraction) = (
        mantissa.unsigned_abs() / scale,
        mantissa.unsigned_abs() % scale,
    );
    fmt::Display::fmt(&whole, out)?;
    if fraction == 0 {
        return Ok(());
    }
    let mut digits = [b'0'; FRACTION_DIGITS];
    for digit in digits.iter_mut().rev() {
        *digit = b'0' + (fraction % 10) as u8;
        fraction /= 10;
    }
    let shown = digits
        .iter()
        .rposition(|digit| *digit != b'0')
        .map_or(0, |last| last + 1);
    out.write_str(".")?;
    out.write_str(std::str::from_utf8(&digits[..shown]).map_err(|_| fmt::Error)?)
}

/// The places of a mantissa at the fixed scale.
const FRACTION_DIGITS: usize = 8;
const _: () = assert!(10i64.pow(FRACTION_DIGITS as u32) == FIXED_SCALE);

/// A unit that a param may have. Each one parses from the text of the config, so an exact amount
/// never passes through a float on its way in.
pub trait ParamUnit: Copy + PartialOrd {
    /// What the config must write, for the refusal of a value that does not parse.
    const EXPECTED: &'static str;

    fn parse(text: &str) -> Option<Self>;

    fn value(self) -> ParamValue;
}

impl ParamUnit for Notional {
    const EXPECTED: &'static str = "a decimal of quote money with at most 8 places";

    fn parse(text: &str) -> Option<Self> {
        Notional::parse_decimal(text).ok()
    }

    fn value(self) -> ParamValue {
        ParamValue::Exact(self.mantissa())
    }
}

impl ParamUnit for Qty {
    const EXPECTED: &'static str = "a decimal quantity with at most 8 places";

    fn parse(text: &str) -> Option<Self> {
        Qty::parse_decimal(text).ok()
    }

    fn value(self) -> ParamValue {
        ParamValue::Exact(self.mantissa())
    }
}

impl ParamUnit for Price {
    const EXPECTED: &'static str = "a decimal price with at most 8 places";

    fn parse(text: &str) -> Option<Self> {
        Price::parse_decimal(text).ok()
    }

    fn value(self) -> ParamValue {
        ParamValue::Exact(self.mantissa())
    }
}

impl ParamUnit for Ticks {
    const EXPECTED: &'static str = "a whole number of ticks";

    fn parse(text: &str) -> Option<Self> {
        text.parse().ok().map(Ticks::new)
    }

    fn value(self) -> ParamValue {
        ParamValue::Integer(self.count())
    }
}

impl ParamUnit for Bps {
    const EXPECTED: &'static str = "a finite number of basis points";

    fn parse(text: &str) -> Option<Self> {
        text.parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(Bps::new)
    }

    fn value(self) -> ParamValue {
        ParamValue::Float(self.value())
    }
}

impl ParamUnit for f64 {
    const EXPECTED: &'static str = "a finite number";

    fn parse(text: &str) -> Option<Self> {
        text.parse::<f64>().ok().filter(|value| value.is_finite())
    }

    fn value(self) -> ParamValue {
        ParamValue::Float(self)
    }
}

impl ParamUnit for u32 {
    const EXPECTED: &'static str = "a whole number from 0";

    fn parse(text: &str) -> Option<Self> {
        text.parse().ok()
    }

    fn value(self) -> ParamValue {
        ParamValue::Integer(i64::from(self))
    }
}

impl ParamUnit for bool {
    const EXPECTED: &'static str = "true or false";

    fn parse(text: &str) -> Option<Self> {
        text.parse().ok()
    }

    fn value(self) -> ParamValue {
        ParamValue::Bool(self)
    }
}

/// The value of a literal of `params!`.
///
/// # Panics
/// When the literal does not parse as its unit: a bug of the declaration, found at the first
/// default.
#[doc(hidden)]
pub fn param_literal<T: ParamUnit>(field: &str, text: &str) -> T {
    T::parse(text).unwrap_or_else(|| {
        panic!(
            "params! gives {field} the literal {text}, which is not {}",
            T::EXPECTED
        )
    })
}

/// # Errors
/// [`ParamError::OutOfBounds`] when `value` lies outside `low ..= high`. A value that is no number
/// at all, such as NaN, lies outside each bound.
#[doc(hidden)]
pub fn check_bounds<T: ParamUnit>(
    field: &'static str,
    value: T,
    low: &str,
    high: &str,
) -> Result<(), ParamError> {
    let (low, high) = (
        param_literal::<T>(field, low),
        param_literal::<T>(field, high),
    );
    if low <= value && value <= high {
        return Ok(());
    }
    Err(ParamError::OutOfBounds {
        field,
        seen: value.value(),
        low: low.value(),
        high: high.value(),
    })
}

/// Reads the text of a scalar, whatever YAML type it reads as, so a number of the config is
/// parsed from its digits.
struct ScalarText<T>(std::marker::PhantomData<T>);

impl<T: ParamUnit> Visitor<'_> for ScalarText<T> {
    type Value = T;

    fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str(T::EXPECTED)
    }

    fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<T, E> {
        T::parse(text.trim())
            .ok_or_else(|| E::invalid_value(serde::de::Unexpected::Str(text), &T::EXPECTED))
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<T, E> {
        self.visit_str(if value { "true" } else { "false" })
    }
}

macro_rules! exact_serde {
    ($($ty:ty),+) => {
        $(
            /// Reads the decimal text of the config and refuses a value it cannot hold exactly.
            impl<'de> Deserialize<'de> for $ty {
                fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    deserializer.deserialize_str(ScalarText::<$ty>(std::marker::PhantomData))
                }
            }

            /// Writes the decimal that [`Deserialize`] reads back.
            impl Serialize for $ty {
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    serializer.collect_str(&self.value())
                }
            }
        )+
    };
}

exact_serde!(Notional, Qty, Price, Ticks, Bps);
