//! Durable cost basis (one JSON per engine, written off hot path). Only basis durable, not mark (recomputes on boot).
//! Position keyed by VENUE SYMBOL, not InstrumentId (index reordering would silently swap markets).

mod file;
mod writer;

use std::path::{Path, PathBuf};

use crate::config::{ExecutionMode, RunIdentity};
use crate::ids::{AssetId, InstrumentId, Qty};
use crate::registry::Registry;
use crate::time::TsUs;
use crate::warn;

pub use writer::{ExposureHandle, ExposureWriter, ExposureWriterConfig};

/// Max instruments per snapshot (engine serves one source).
pub const MAX_EXPOSURE_INSTRUMENTS: usize = 16;

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
        position_base: Qty(0),
        cash_quote: 0,
        basis_quote: 0,
    };

    /// An all-zero row can be dropped on a config change without losing anything.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.position_base.0 == 0 && self.cash_quote == 0 && self.basis_quote == 0
    }
}

/// Whole-ledger snapshot (fixed-size, absolute state): atomic file write, avoid half-updated ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExposureSnapshot {
    pub instruments: [InstrumentExposure; MAX_EXPOSURE_INSTRUMENTS],
    pub len: u8,
    /// Sequence (monotone, starts at 1): 0 = boot-restored state (writer skips if already written).
    pub seq: u64,
    /// Mark-to-market at emission (0 if no honest mark yet); written for human, ignored on load.
    pub exposure_quote: i64,
    /// Message received_ts_us (never hot-thread clock read).
    pub emitted_ts_us: TsUs,
}

impl ExposureSnapshot {
    pub const EMPTY: ExposureSnapshot = ExposureSnapshot {
        instruments: [InstrumentExposure::EMPTY; MAX_EXPOSURE_INSTRUMENTS],
        len: 0,
        seq: 0,
        exposure_quote: 0,
        emitted_ts_us: TsUs::from_micros(0),
    };

    /// Filled prefix (capacity invariant trapped here).
    #[inline]
    pub fn active(&self) -> &[InstrumentExposure] {
        debug_assert!(
            self.len as usize <= MAX_EXPOSURE_INSTRUMENTS,
            "snapshot len {} exceeds capacity {MAX_EXPOSURE_INSTRUMENTS}",
            self.len
        );
        &self.instruments[..self.len as usize]
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
        "exposure file at {} holds {symbol} with position {position_base} and cash {cash_quote}, and this config names no such instrument — the next write would erase it",
        .path.display()
    )]
    UnknownInstrument {
        path: PathBuf,
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
    fn write_at(path: &Path) -> impl FnOnce(std::io::Error) -> Self + '_ {
        move |source| Self::Write {
            path: path.to_path_buf(),
            source,
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
        let Some(row) = registry
            .instruments()
            .iter()
            .find(|row| row.venue_symbol.as_ref() == entry.symbol.as_str())
        else {
            if entry.is_zero() {
                continue;
            }
            return Err(ExposureError::UnknownInstrument {
                path,
                symbol: entry.symbol.as_str().into(),
                position_base: entry.position_base,
                cash_quote: entry.cash_quote,
            });
        };
        instruments.push(InstrumentExposure {
            instrument: row.instrument_id,
            position_base: Qty(entry.position_base),
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
        add_amount(&mut amounts, row.base_asset, exposure.position_base.0);
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
