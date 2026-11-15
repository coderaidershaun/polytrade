//! Errors raised by the simulated execution edge.

use super::core::wallet::WalletError;
use super::lanes::SimLane;

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
