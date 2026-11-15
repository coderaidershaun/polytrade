//! The faults that end a market-data feed, each with its failure class and the reason that the
//! fatal latch keeps, and the one decision that the reconnect loop of both feeds takes on them.

use crate::error;
use crate::msg::failure::{FailureClass, Halt};
use crate::msg::ids::SourceId;
use crate::runtime::shutdown::{FatalReason, FatalSignal};
use crate::venues::edge::decode::DecimalFault;

/// A fault after which a feed cannot go on without giving the engine wrong market data. A frame
/// that is only malformed is not one: the feed drops it and counts it.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq, Hash)]
pub enum MarketDataError {
    #[error(
        "market data source {} sent {field} {value:?}, too large for the fixed-point range",
        source_id.0
    )]
    Unreadable {
        source_id: SourceId,
        field: &'static str,
        value: Box<str>,
    },
    #[error("market data source {} exhausted its connection epoch counter", source_id.0)]
    EpochExhausted { source_id: SourceId },
}

impl MarketDataError {
    /// The one way a parse fault becomes a market-data fault. Only a decimal that the mantissa
    /// cannot hold does, because a truncated number would be a wrong price.
    pub fn of_decimal_fault(source_id: SourceId, fault: &DecimalFault) -> Option<Self> {
        match fault {
            DecimalFault::MantissaOverflow { field, value } => Some(Self::Unreadable {
                source_id,
                field,
                value: value.clone(),
            }),
            DecimalFault::Json { .. }
            | DecimalFault::Decimal { .. }
            | DecimalFault::UnknownEnum { .. } => None,
        }
    }

    pub const fn failure_class(&self) -> FailureClass {
        match self {
            MarketDataError::Unreadable { .. } | MarketDataError::EpochExhausted { .. } => {
                FailureClass::Halt(Halt::Engine)
            }
        }
    }

    pub const fn fatal_reason(&self) -> FatalReason {
        match *self {
            MarketDataError::Unreadable { source_id, .. } => {
                FatalReason::MarketDataUnreadable { source_id }
            }
            MarketDataError::EpochExhausted { source_id } => {
                FatalReason::EpochExhausted { source_id }
            }
        }
    }
}

/// The decision that the reconnect loop of a market-data feed takes on a fault of its session: one
/// ERROR line with the values, then the fatal latch with the typed reason. The latch keeps the first
/// reason, so a later fault changes nothing.
pub fn halt_on_market_data_error(error: &MarketDataError, fatal: &FatalSignal) {
    match error.failure_class() {
        FailureClass::Halt(Halt::Engine) => {
            error!("{error}");
            fatal.trip(error.fatal_reason());
        }
        other => unreachable!("a market data fault of class {other:?} has no action at the edge"),
    }
}
