//! The exposure file on disk: its form, the checks of its header, and its read and write. A write
//! never leaves half a file, so a kill in the middle keeps the last position.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::msg::ids::FIXED_SCALE;
use crate::runtime::config::RunIdentity;
use crate::runtime::durable_file::{self, WriteError};
use crate::warn;

use super::ExposureError;

/// Version 3 names each row by its venue and its symbol, and version 2 added `basis_quote`. An older
/// file is not migrated: a symbol alone names an instrument only within one venue, and a guessed
/// basis would bring back a defect that nothing reports.
pub(super) const FORMAT_VERSION: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExposureDocument {
    pub(super) version: u32,
    /// The ids repeat what the file name says, so a file that a person copied or renamed is caught.
    pub(super) strategy_id: String,
    pub(super) te_id: String,
    pub(super) written_ts_us: i64,
    pub(super) seq: u64,
    pub(super) fixed_scale: i64,
    /// The rows that a load reads. The fields below them are written for a person only.
    pub(super) instruments: Vec<InstrumentEntry>,
    #[serde(default)]
    pub(super) assets: Vec<AssetEntry>,
    #[serde(default)]
    pub(super) last_exposure_quote: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InstrumentEntry {
    /// The venue and its symbol name the instrument, because an instrument id can change between
    /// runs. Defaulted, so that a file of an older version parses and the version check refuses it.
    #[serde(default)]
    pub(super) venue: String,
    pub(super) symbol: String,
    pub(super) position_base: i64,
    pub(super) cash_quote: i64,
    /// Defaulted so that a file of version 1 parses and the version check refuses it, rather than
    /// reading as corrupt. No accepted file uses the default.
    #[serde(default)]
    pub(super) basis_quote: i64,
}

impl InstrumentEntry {
    pub(super) fn is_zero(&self) -> bool {
        self.position_base == 0 && self.cash_quote == 0 && self.basis_quote == 0
    }
}

/// Totals by asset, derived from the instruments and written for a person. A load computes them
/// again, so the file holds one source of truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AssetEntry {
    pub(super) asset: String,
    pub(super) amount: i64,
}

impl ExposureDocument {
    /// # Errors
    /// [`ExposureError`] when the file belongs to another engine, another format version, or another
    /// fixed-point scale — each of which makes every number in it mean something else.
    pub(super) fn check_header(
        &self,
        path: &Path,
        identity: &RunIdentity,
    ) -> Result<(), ExposureError> {
        if self.version != FORMAT_VERSION {
            return Err(ExposureError::UnknownVersion {
                path: path.to_path_buf(),
                found: self.version,
                expected: FORMAT_VERSION,
            });
        }
        let found = format!("{}-{}", self.strategy_id, self.te_id);
        let expected = identity.to_string();
        if found != expected {
            return Err(ExposureError::WrongIdentity {
                path: path.to_path_buf(),
                found: found.into(),
                expected: expected.into(),
            });
        }
        if self.fixed_scale != FIXED_SCALE {
            return Err(ExposureError::ScaleMismatch {
                path: path.to_path_buf(),
                found: self.fixed_scale,
                expected: FIXED_SCALE,
            });
        }
        Ok(())
    }
}

/// `None` for a missing file: a cold start, not a fault.
pub(super) fn read(path: &Path) -> Result<Option<ExposureDocument>, ExposureError> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ExposureError::Read {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    serde_json::from_str(&contents)
        .map(Some)
        .map_err(|error| ExposureError::Malformed {
            path: path.to_path_buf(),
            detail: error.to_string().into_boxed_str(),
        })
}

/// Serialises the document and writes it whole, so a crash never leaves half a file.
pub(super) fn write_atomically(
    path: &Path,
    document: &ExposureDocument,
) -> Result<(), ExposureError> {
    let body = serde_json::to_vec_pretty(document).map_err(|error| ExposureError::Write {
        path: path.to_path_buf(),
        source: std::io::Error::other(error),
    })?;
    match durable_file::write(path, &body) {
        Ok(()) => Ok(()),
        // The file is whole either way, and the next write syncs the folder again.
        Err(WriteError::FolderNotSynced { directory, source }) => {
            warn!(
                "could not flush directory {} ({source}) — {} is written, its directory entry is not yet durable",
                directory.display(),
                path.display()
            );
            Ok(())
        }
        Err(WriteError::NotWritten { path, source }) => Err(ExposureError::Write { path, source }),
    }
}
