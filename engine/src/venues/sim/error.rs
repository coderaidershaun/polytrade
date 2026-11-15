//! Errors raised by the simulated execution edge, and the one decision that the root of the edge
//! takes on a fault of the edge.

use super::core::wallet::WalletError;
use super::lanes::SimLane;
use crate::error;
use crate::msg::failure::{FailureClass, Halt};
use crate::runtime::shutdown::{FatalReason, FatalSignal};

/// Why a run cannot stand a simulated venue up at all.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimVenueError {
    #[error("the simulated venue's opening account is not spendable")]
    Wallet(#[from] WalletError),
    #[error(
        "execution.mode is sim but {detail} — refusing to start, because a run that reports itself armed and places nothing is worse than one that does not start"
    )]
    NotWired { detail: &'static str },
    #[error(
        "the simulated venue holds one instrument and this run configures {found} — a synthesised venue matches one price queue against one market, and splitting it would give each instrument its own clock and readiness"
    )]
    OneInstrument { found: usize },
    #[error(
        "execution.mode is sim but the registry holds no binance spot {lane} producer group for the simulated venue to tap — config validation refuses such a source, so this is a gap in validation"
    )]
    LaneMissing { lane: SimLane },
}

/// A fault after which a simulated run cannot go on and still say what its market would have filled.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SimEdgeError {
    #[error(
        "the exchange simulator's command lane is full at capacity {capacity}, so a command would get no answer"
    )]
    CommandLaneFull { capacity: usize },
    #[error(
        "the simulated venue's market inbox is full at {capacity} events: the safe horizon lagged its receipts past the declared delay bound, and evicting an event would change which orders filled, so the run is not valid"
    )]
    InboxFull { capacity: usize },
}

impl SimEdgeError {
    pub const fn failure_class(&self) -> FailureClass {
        match self {
            SimEdgeError::CommandLaneFull { .. } | SimEdgeError::InboxFull { .. } => {
                FailureClass::Halt(Halt::Engine)
            }
        }
    }

    pub const fn fatal_reason(&self) -> FatalReason {
        match *self {
            SimEdgeError::CommandLaneFull { capacity } => {
                FatalReason::SimulatorCommandLaneFull { capacity }
            }
            SimEdgeError::InboxFull { capacity } => FatalReason::SimulatorInboxFull { capacity },
        }
    }
}

/// The decision that the root of the simulator edge takes on a fault of the edge: one ERROR line
/// with the values, then the fatal latch with the typed reason. The latch keeps the first reason,
/// so a later fault changes nothing.
pub fn halt_on_sim_error(error: &SimEdgeError, fatal: &FatalSignal) {
    match error.failure_class() {
        FailureClass::Halt(Halt::Engine) => {
            error!("exchange simulator: {error}");
            fatal.trip(error.fatal_reason());
        }
        other => {
            unreachable!("a simulator edge fault of class {other:?} has no action at the edge")
        }
    }
}
