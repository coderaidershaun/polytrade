//! The durable cost basis, one JSON file for each engine, written off the hot path. Position is
//! keyed by venue, market and venue symbol, because an instrument id can change between runs.

mod file;
mod writer;

use std::fmt;
use std::path::{Path, PathBuf};

use crate::hot::digest::digest_fields;
use crate::msg::failure::{FailureClass, Halt};
use crate::msg::ids::{AssetId, InstrumentId, Qty};
use crate::msg::time::LocalTs;
use crate::runtime::config::{ExecutionMode, RunIdentity};
use crate::runtime::registry::Registry;
use crate::warn;

pub use writer::{ExposureHandle, ExposureWriter, ExposureWriterSetup, WriterStatus};

/// Quote amounts are mantissas at the global fixed scale, never currency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InstrumentExposure {
    pub instrument: InstrumentId,
    pub position_base: Qty,
    pub cash_quote: i64,
    /// Cost basis (durable): cash accumulates runs, -cash leaks prior P/L if reused.
    pub basis_quote: i64,
}

impl InstrumentExposure {
    pub const EMPTY: InstrumentExposure = InstrumentExposure {
        instrument: InstrumentId(0),
        position_base: Qty::from_mantissa(0),
        cash_quote: 0,
        basis_quote: 0,
    };

    /// An all-zero row can be dropped on a config change without losing anything.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.position_base.mantissa() == 0 && self.cash_quote == 0 && self.basis_quote == 0
    }
}

/// The whole ledger at one sequence number, so a file never holds half of an update. It has room
/// for a row of each instrument of the registry, made once at start, and is filled in place, so the
/// hot thread never allocates or frees one.
#[derive(Clone)]
pub struct ExposureSnapshot {
    rows: Box<[InstrumentExposure]>,
    /// The rows in use, each one that is not empty. The rows past it are room.
    len: usize,
    /// Sequence (monotone, starts at 1): 0 = boot-restored state (writer skips if already written).
    pub seq: u64,
    /// Mark-to-market at emission (0 if no honest mark yet); written for human, ignored on load.
    pub exposure_quote: i64,
    /// Message received_ts_us (never hot-thread clock read).
    pub emitted_ts_us: LocalTs,
}

digest_fields! { ExposureSnapshot { rows, len, seq, exposure_quote, emitted_ts_us } }

impl ExposureSnapshot {
    /// Room for a row of each of `instruments` instruments, and no row in use.
    pub fn for_instruments(instruments: usize) -> Self {
        ExposureSnapshot {
            rows: vec![InstrumentExposure::EMPTY; instruments].into_boxed_slice(),
            len: 0,
            seq: 0,
            exposure_quote: 0,
            emitted_ts_us: LocalTs::from_micros(0),
        }
    }

    #[inline]
    pub fn active(&self) -> &[InstrumentExposure] {
        &self.rows[..self.len]
    }

    /// Takes each row that is not empty, in order. An all-zero row is the same as no row.
    ///
    /// # Panics
    /// More rows than the instruments that the snapshot has room for. The ledger and the snapshot
    /// are sized by one registry, so that is a bug.
    pub fn set_rows(&mut self, rows: impl IntoIterator<Item = InstrumentExposure>) {
        let mut len = 0;
        for row in rows.into_iter().filter(|row| !row.is_empty()) {
            let Some(slot) = self.rows.get_mut(len) else {
                room_overrun(self.rows.len());
            };
            *slot = row;
            len += 1;
        }
        self.len = len;
    }

    /// Takes the rows and the stamps of `other`, in place.
    ///
    /// # Panics
    /// `other` holds more rows than this snapshot has room for. Each snapshot of a run is sized by
    /// one registry, so that is a bug.
    pub fn copy_from(&mut self, other: &ExposureSnapshot) {
        let rows = other.active();
        let Some(room) = self.rows.get_mut(..rows.len()) else {
            room_overrun(self.rows.len());
        };
        room.copy_from_slice(rows);
        self.len = rows.len();
        self.seq = other.seq;
        self.exposure_quote = other.exposure_quote;
        self.emitted_ts_us = other.emitted_ts_us;
    }
}

#[cold]
#[inline(never)]
fn room_overrun(room: usize) -> ! {
    panic!(
        "an exposure snapshot has room for {room} instruments, and was given more rows than that"
    )
}

/// The rows past the ones in use are room, so two snapshots of one state are equal.
impl PartialEq for ExposureSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.active() == other.active()
            && self.seq == other.seq
            && self.exposure_quote == other.exposure_quote
            && self.emitted_ts_us == other.emitted_ts_us
    }
}

impl Eq for ExposureSnapshot {}

impl fmt::Debug for ExposureSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExposureSnapshot")
            .field("instruments", &self.active())
            .field("seq", &self.seq)
            .field("exposure_quote", &self.exposure_quote)
            .field("emitted_ts_us", &self.emitted_ts_us)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AssetAmount {
    pub asset: AssetId,
    pub amount: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExposureState {
    instruments: Vec<InstrumentExposure>,
    assets: Vec<AssetAmount>,
}

impl ExposureState {
    pub fn instruments(&self) -> &[InstrumentExposure] {
        &self.instruments
    }

    pub fn assets(&self) -> &[AssetAmount] {
        &self.assets
    }

    pub fn is_empty(&self) -> bool {
        self.instruments.is_empty()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ExposureError {
    #[error("exposure file unreadable at {}", .path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "exposure file at {} is malformed ({detail}) — refusing to start against a position that cannot be read",
        .path.display()
    )]
    Malformed { path: PathBuf, detail: Box<str> },
    #[error(
        "exposure file at {} was written by trading engine {found}, this run is {expected} — refusing to adopt another engine's position",
        .path.display()
    )]
    WrongIdentity {
        path: PathBuf,
        found: Box<str>,
        expected: Box<str>,
    },
    #[error(
        "exposure file at {} is format version {found}, this build reads {expected}",
        .path.display()
    )]
    UnknownVersion {
        path: PathBuf,
        found: u32,
        expected: u32,
    },
    #[error(
        "exposure file at {} was written at fixed scale {found} but this build uses {expected} — every mantissa in it means something else",
        .path.display()
    )]
    ScaleMismatch {
        path: PathBuf,
        found: i64,
        expected: i64,
    },
    #[error(
        "exposure file at {} holds {symbol} of {venue} {market} with position {position_base} and cash {cash_quote}, and this config names no such instrument — the next write would erase it",
        .path.display()
    )]
    UnknownInstrument {
        path: PathBuf,
        venue: Box<str>,
        market: Box<str>,
        symbol: Box<str>,
        position_base: i64,
        cash_quote: i64,
    },
    #[error("exposure write failed at {}", .path.display())]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("exposure writer panicked: {payload}")]
    WriterPanicked { payload: Box<str> },
    #[error("exposure writer drain was interrupted before the thread could be joined")]
    DrainInterrupted,
}

impl ExposureError {
    /// A file that a start cannot trust refuses the start. A failed write is tried again at the
    /// writer's next poll, and its limit halts quoting. A writer that died, or a drain that could
    /// not wait for it, leaves the file behind the position, so the engine halts.
    pub fn failure_class(&self) -> FailureClass {
        match self {
            ExposureError::Read { .. }
            | ExposureError::Malformed { .. }
            | ExposureError::WrongIdentity { .. }
            | ExposureError::UnknownVersion { .. }
            | ExposureError::ScaleMismatch { .. }
            | ExposureError::UnknownInstrument { .. } => FailureClass::Halt(Halt::Start),
            ExposureError::Write { .. } => FailureClass::Retry,
            ExposureError::WriterPanicked { .. } | ExposureError::DrainInterrupted => {
                FailureClass::Halt(Halt::Engine)
            }
        }
    }
}

/// Adds `-sim` to simulated exposure filenames.
pub fn file_path(dir: &Path, identity: &RunIdentity, mode: Option<ExecutionMode>) -> PathBuf {
    dir.join(format!(
        "{}.json",
        ExecutionMode::artifact_stem(mode, identity)
    ))
}

/// Load position, resolved to this run's ids. A missing file is a cold start, not a fault.
///
/// # Errors
/// [`ExposureError`] whenever the file exists but cannot be trusted: every such case refuses boot
/// rather than starting against a position that cannot be read.
pub fn load(
    dir: &Path,
    identity: &RunIdentity,
    registry: &Registry,
    mode: Option<ExecutionMode>,
) -> Result<ExposureState, ExposureError> {
    let path = file_path(dir, identity, mode);
    let Some(document) = file::read(&path)? else {
        return Ok(ExposureState::default());
    };
    document.check_header(&path, identity)?;

    let mut instruments = Vec::with_capacity(document.instruments.len());
    for entry in &document.instruments {
        let Some(row) = registry.instruments().iter().find(|row| {
            registry.venue_name(row.venue) == entry.venue
                && row.market.as_str() == entry.market
                && row.venue_symbol.as_ref() == entry.symbol
        }) else {
            if entry.is_zero() {
                continue;
            }
            return Err(ExposureError::UnknownInstrument {
                path,
                venue: entry.venue.as_str().into(),
                market: entry.market.as_str().into(),
                symbol: entry.symbol.as_str().into(),
                position_base: entry.position_base,
                cash_quote: entry.cash_quote,
            });
        };
        instruments.push(InstrumentExposure {
            instrument: row.instrument_id,
            position_base: Qty::from_mantissa(entry.position_base),
            cash_quote: entry.cash_quote,
            basis_quote: entry.basis_quote,
        });
    }
    if instruments.len() < document.instruments.len() {
        warn!(
            "exposure: {} zero rows in {} name instruments this config does not — dropped",
            document.instruments.len() - instruments.len(),
            path.display()
        );
    }
    let assets = aggregate_assets(&instruments, registry);
    Ok(ExposureState {
        instruments,
        assets,
    })
}

/// Fold cost basis into per-asset amounts: position = BASE amount, cash = signed QUOTE amount. Exact even with shared assets.
fn aggregate_assets(instruments: &[InstrumentExposure], registry: &Registry) -> Vec<AssetAmount> {
    let mut amounts = vec![0i128; registry.assets().len()];
    for exposure in instruments {
        let row = registry.instrument(exposure.instrument);
        add_amount(
            &mut amounts,
            row.base_asset,
            exposure.position_base.mantissa(),
        );
        add_amount(&mut amounts, row.quote_asset, exposure.cash_quote);
    }
    asset_amounts(&amounts)
}

/// Add amount to accumulator (i128 for mantissa sum range; asset outside dictionary = bug + loud count).
fn add_amount(amounts: &mut [i128], asset: AssetId, delta: i64) {
    let Some(slot) = amounts.get_mut(usize::from(asset.0)) else {
        warn!(
            "exposure: asset id {} is outside the dictionary — {delta} unattributed",
            asset.0
        );
        return;
    };
    *slot += i128::from(delta);
}

/// Filter zero amounts (same as absent row, bury noise).
fn asset_amounts(amounts: &[i128]) -> Vec<AssetAmount> {
    amounts
        .iter()
        .enumerate()
        .filter(|(_, amount)| **amount != 0)
        .map(|(index, amount)| AssetAmount {
            asset: AssetId(index as u16),
            amount: narrow_amount(*amount, index),
        })
        .collect()
}

/// Clamp overflow (bug upstream, but risk view must show it — operator needs reading when things are wrong).
fn narrow_amount(amount: i128, asset_index: usize) -> i64 {
    i64::try_from(amount).unwrap_or_else(|_| {
        crate::error!("exposure: asset {asset_index} total {amount} leaves the i64 mantissa range");
        if amount.is_negative() { i64::MIN } else { i64::MAX }
    })
}
