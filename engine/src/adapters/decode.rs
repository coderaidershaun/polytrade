//! Every venue's JSON meets the same three hazards: a frame serde cannot read, a decimal too large
//! for an `i64` at eight decimal places, and a spelling this engine does not know. They share one
//! fault type so that whether a fault is fatal or dropped cannot drift between venues.

use serde::Deserialize;

use crate::ids::{DecimalError, Price, Qty, Side};
use crate::msg::inbound::Level;

/// `MantissaOverflow` is fatal — a venue number this scale cannot hold escalates, never truncates.
#[derive(thiserror::Error, Debug)]
pub enum DecimalFault {
    #[error("malformed {frame} frame: {source}")]
    Json {
        frame: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("malformed decimal in {field}: {value:?} ({reason})")]
    Decimal {
        field: &'static str,
        value: Box<str>,
        reason: &'static str,
    },
    #[error("mantissa overflow in {field}: {value:?} exceeds i64@1e-8 scale")]
    MantissaOverflow {
        field: &'static str,
        value: Box<str>,
    },
    #[error("unknown {field} {value:?} — refusing to map a spelling this engine does not know")]
    UnknownEnum {
        field: &'static str,
        value: Box<str>,
    },
}

impl DecimalFault {
    pub fn is_fatal(&self) -> bool {
        matches!(self, DecimalFault::MantissaOverflow { .. })
    }
}

/// The venue's own word in "malformed {frame} frame" — a newtype so it cannot be handed to a
/// decoder where the payload belongs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JsonFrame(pub(crate) &'static str);

impl JsonFrame {
    pub(crate) fn decode<T: for<'de> Deserialize<'de>>(
        self,
        json: &str,
    ) -> Result<T, DecimalFault> {
        serde_json::from_str(json).map_err(|source| self.fault(source))
    }

    pub(crate) fn decode_value<T: for<'de> Deserialize<'de>>(
        self,
        value: serde_json::Value,
    ) -> Result<T, DecimalFault> {
        serde_json::from_value(value).map_err(|source| self.fault(source))
    }

    pub(crate) fn fault(self, source: serde_json::Error) -> DecimalFault {
        DecimalFault::Json {
            frame: self.0,
            source,
        }
    }
}

pub(crate) fn price_field(field: &'static str, text: &str) -> Result<Price, DecimalFault> {
    Price::parse_decimal(text).map_err(|error| decimal_fault(field, text, error))
}

pub(crate) fn qty_field(field: &'static str, text: &str) -> Result<Qty, DecimalFault> {
    Qty::parse_decimal(text).map_err(|error| decimal_fault(field, text, error))
}

pub(crate) fn mantissa_field(field: &'static str, text: &str) -> Result<i64, DecimalFault> {
    qty_field(field, text).map(|qty| qty.0)
}

pub(crate) fn levels<'a>(
    price_name: &'static str,
    qty_name: &'static str,
    pairs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<Vec<Level>, DecimalFault> {
    let pairs = pairs.into_iter();
    let mut levels = Vec::with_capacity(pairs.size_hint().0);
    for (price, qty) in pairs {
        levels.push(Level {
            price: price_field(price_name, price)?,
            qty: qty_field(qty_name, qty)?,
        });
    }
    Ok(levels)
}

pub(crate) fn order_side(field: &'static str, text: &str) -> Result<Side, DecimalFault> {
    [Side::Buy, Side::Sell]
        .into_iter()
        .find(|side| side.wire_name() == text)
        .ok_or_else(|| unknown_enum(field, text))
}

pub(crate) fn unknown_enum(field: &'static str, value: &str) -> DecimalFault {
    DecimalFault::UnknownEnum {
        field,
        value: value.into(),
    }
}

fn decimal_fault(field: &'static str, text: &str, error: DecimalError) -> DecimalFault {
    match error {
        DecimalError::Overflow { .. } => DecimalFault::MantissaOverflow {
            field,
            value: text.into(),
        },
        DecimalError::Empty => DecimalFault::Decimal {
            field,
            value: text.into(),
            reason: "empty decimal string",
        },
        DecimalError::InvalidChar { .. } => DecimalFault::Decimal {
            field,
            value: text.into(),
            reason: "non-numeric character",
        },
        DecimalError::TooPrecise { .. } => DecimalFault::Decimal {
            field,
            value: text.into(),
            reason: "more than 8 decimal places",
        },
    }
}
