//! The faults of the session of an execution edge, each with its failure class and the reason that
//! the fatal latch keeps, and the one decision that the chassis of an edge takes on them.

use crate::error;
use crate::msg::failure::{FailureClass, Halt};
use crate::msg::ids::{ClientOrderId, TradeId};
use crate::runtime::shutdown::{AnswerPart, FatalReason, FatalSignal};

use super::{ObserveOrderError, PlaceNotSentReason};

/// A fault of the session itself. A venue error code with no mapping is not one: the fold halts
/// quoting on it, and the process stays up.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecSessionError {
    #[error("{venue} execution cannot retain an observed order in its possibly-live set")]
    MirrorRetainFailed {
        venue: &'static str,
        #[source]
        source: ObserveOrderError,
    },
    #[error(
        "{venue} execution placement admission failed closed for order {:016x} ({reason:?})",
        client_id.0
    )]
    PlacementFailedClosed {
        venue: &'static str,
        client_id: ClientOrderId,
        reason: PlaceNotSentReason,
    },
    #[error("{venue} execution cannot decode a possibly-owned open order {venue_order_id}")]
    UndecodableOpenOrder {
        venue: &'static str,
        venue_order_id: i64,
    },
    #[error("{venue} execution cannot decode a frame of its {part:?} part")]
    UndecodableAnswer {
        venue: &'static str,
        part: AnswerPart,
    },
    #[error("{venue} execution: the account stream subscribe was refused {failures} times")]
    SubscribeRefused { venue: &'static str, failures: u32 },
    #[error("{venue} execution: the rest worker stopped answering")]
    RestWorkerGone { venue: &'static str },
    #[error("{venue} execution order index is full and cannot record a venue order id")]
    OrderIndexFull { venue: &'static str },
    #[error("{venue} execution cannot build its account stream subscription")]
    SubscribeUnbuildable { venue: &'static str },
    #[error("{venue} execution trade {} settled failed after its fill was folded", trade.0)]
    SettlementFailed { venue: &'static str, trade: TradeId },
}

impl ExecSessionError {
    pub const fn failure_class(&self) -> FailureClass {
        match self {
            ExecSessionError::MirrorRetainFailed { .. }
            | ExecSessionError::PlacementFailedClosed { .. }
            | ExecSessionError::UndecodableOpenOrder { .. }
            | ExecSessionError::UndecodableAnswer { .. }
            | ExecSessionError::SubscribeRefused { .. }
            | ExecSessionError::RestWorkerGone { .. }
            | ExecSessionError::OrderIndexFull { .. }
            | ExecSessionError::SubscribeUnbuildable { .. }
            | ExecSessionError::SettlementFailed { .. } => FailureClass::Halt(Halt::Engine),
        }
    }

    pub const fn fatal_reason(&self) -> FatalReason {
        match *self {
            ExecSessionError::MirrorRetainFailed { venue, .. } => {
                FatalReason::MirrorRetainFailed { venue }
            }
            ExecSessionError::PlacementFailedClosed {
                venue, client_id, ..
            } => FatalReason::PlacementFailedClosed { venue, client_id },
            ExecSessionError::UndecodableOpenOrder {
                venue,
                venue_order_id,
            } => FatalReason::UndecodableOpenOrder {
                venue,
                venue_order_id,
            },
            ExecSessionError::UndecodableAnswer { venue, part } => {
                FatalReason::UndecodableAnswer { venue, part }
            }
            ExecSessionError::SubscribeRefused { venue, failures } => {
                FatalReason::SubscribeRefused { venue, failures }
            }
            ExecSessionError::RestWorkerGone { venue } => FatalReason::RestWorkerGone { venue },
            ExecSessionError::OrderIndexFull { venue } => FatalReason::OrderIndexFull { venue },
            ExecSessionError::SubscribeUnbuildable { venue } => {
                FatalReason::SubscribeUnbuildable { venue }
            }
            ExecSessionError::SettlementFailed { venue, trade } => {
                FatalReason::SettlementFailed { venue, trade }
            }
        }
    }
}

/// The decision that the chassis of an execution edge takes on a fault of its session: one ERROR
/// line with the values, then the fatal latch with the typed reason. The latch keeps the first
/// reason, so a later fault changes nothing.
pub fn halt_on_session_error(error: &ExecSessionError, fatal: &FatalSignal) {
    match error.failure_class() {
        FailureClass::Halt(Halt::Engine) => {
            error!("{error}");
            fatal.trip(error.fatal_reason());
        }
        other => unreachable!("a session error of class {other:?} has no action at the edge"),
    }
}
