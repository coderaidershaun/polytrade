//! Errors raised by the simulated execution edge.

use super::core::latency::LatencyBudgetError;
use super::core::wallet::WalletError;

/// Why a run cannot stand a simulated venue up at all.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimVenueError {
    #[error("the simulated venue's opening account is not spendable")]
    Wallet(#[from] WalletError),
    #[error(
        "the simulated venue's latency budget cannot answer a command in time — config checks the same sum in whole milliseconds, so a sub-millisecond spin interval is binding here and nowhere else"
    )]
    Latency(#[from] LatencyBudgetError),
    #[error(
        "execution.mode is sim but {detail} — refusing to start, because a run that reports itself armed and places nothing is worse than one that does not start"
    )]
    NotWired { detail: &'static str },
    #[error(
        "the simulated venue holds one instrument and this run configures {found} — a synthesised venue matches one price queue against one market, and splitting it would give each instrument its own clock and readiness"
    )]
    OneInstrument { found: usize },
}
