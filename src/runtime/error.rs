//! Every way a run can refuse to start, after the config itself parsed: an unavailable core, an
//! unreachable venue, a scale or limit the engine cannot represent, a lease another process holds.

use crate::adapters::VenueError;
use crate::config::ConfigError;
use crate::exposure::ExposureError;
use std::path::{Path, PathBuf};

#[derive(thiserror::Error, Debug)]
pub enum EngineError {
    #[error("config invalid")]
    Config(#[from] ConfigError),
    #[error("exposure could not be restored")]
    Exposure(#[from] ExposureError),
    #[error(transparent)]
    Venue(#[from] VenueError),
    #[error(
        "execution.mode is {mode} but {detail} — refusing to start, because a run that reports itself armed and places nothing is worse than one that does not start"
    )]
    ExecutionNotWired {
        mode: &'static str,
        detail: &'static str,
    },
    #[error(
        "{found} instruments exceed the {max}-instrument order table — an engine that cannot track its own orders must not place any"
    )]
    ExecutionTooManyInstruments { found: usize, max: usize },
    #[error(
        "execution.{field} is {value}, but {venue_fact} — the setting reaches nothing on this venue, and a config that appears to tune one is worse than one that leaves it out"
    )]
    ExecutionFieldInert {
        field: &'static str,
        value: Box<str>,
        venue_fact: &'static str,
    },
    #[error(
        "execution.min_base_balance is {value}, but on this venue the base asset IS the position — a floor under it comes off every exit, so an offer sized to the whole position reads underfunded and a flatten rounds below the venue's minimum and strands what it was meant to close"
    )]
    ExecutionBaseFloorOnPosition { value: Box<str> },
    #[error(
        "instrument {instrument} ({symbol}) configures {configured_per_side} orders per side ({required_total} total), exceeding the venue MAX_NUM_ORDERS limit {venue_max}"
    )]
    ExecutionOrderCapacity {
        instrument: u16,
        symbol: Box<str>,
        configured_per_side: u32,
        required_total: u32,
        venue_max: u32,
    },
    #[error(
        "execution identity is already held at {path} — refusing a second process that could exceed the per-side order cap"
    )]
    ExecutionIdentityInUse { path: PathBuf },
    #[error("execution identity state at {path} could not be read or written")]
    ExecutionIdentityIo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "execution identity state at {path} contains {value:?}, expected an unsigned run nonce"
    )]
    ExecutionIdentityState { path: PathBuf, value: Box<str> },
    #[error("execution identity at {path} exhausted its 32-bit run nonce")]
    ExecutionIdentityExhausted { path: PathBuf },
    #[error("tokio runtime build failed")]
    Runtime(#[source] std::io::Error),
    #[error("hot_core_id {core_id} is not an available core (found {available})")]
    CoreId { core_id: usize, available: usize },
    #[error(
        "trading engine {trading_engine} could not bind link.bind {bind} — another process holds that port, or this host has no such address"
    )]
    LinkBind {
        bind: Box<str>,
        trading_engine: Box<str>,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "{name:?} is {found} bytes and the link's catalog frames carry {max} — shorten the name, or drop the link: block"
    )]
    LinkNameTooLong {
        name: Box<str>,
        found: usize,
        max: usize,
    },
}

impl EngineError {
    pub(super) fn identity_io_at(path: &Path) -> impl FnOnce(std::io::Error) -> Self + '_ {
        move |source| Self::ExecutionIdentityIo {
            path: path.to_path_buf(),
            source,
        }
    }
}
