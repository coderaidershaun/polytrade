//! Durable on-disk document: temp write + fsync + atomic rename protects against kill-mid-write.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::msg::ids::FIXED_SCALE;
use crate::runtime::config::RunIdentity;
use crate::runtime::durable_file;

use super::ExposureError;

/// Format version; v2 added basis_quote (no migration: v1 files unrecoverable, reintroduce silent defect).
pub(super) const FORMAT_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExposureDocument {
    pub(super) version: u32,
    /// Identity (strategy + te_id) redundancy catches hand-copied/renamed files.
    pub(super) strategy_id: String,
    pub(super) te_id: String,
    pub(super) written_ts_us: i64,
    pub(super) seq: u64,
    pub(super) fixed_scale: i64,
    /// Load-bearing rows; below written for human only.
    pub(super) instruments: Vec<InstrumentEntry>,
    #[serde(default)]
    pub(super) assets: Vec<AssetEntry>,
    #[serde(default)]
    pub(super) last_exposure_quote: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InstrumentEntry {
    /// Venue symbol, not dense id (index is wrong key).
    pub(super) symbol: String,
    pub(super) position_base: i64,
    pub(super) cash_quote: i64,
    /// Defaulted so v1 docs parse + are refused by version check (not die as corrupt); no accepted doc uses it.
    #[serde(default)]
    pub(super) basis_quote: i64,
}

impl InstrumentEntry {
    pub(super) fn is_zero(&self) -> bool {
        self.position_base == 0 && self.cash_quote == 0 && self.basis_quote == 0
    }
}

/// Asset entries (derived from instruments at write for human readability; load recomputes, avoids second source of truth).
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

/// Read document; Ok(None) for missing (cold start, not fault). Errors on unreadable/malformed.
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
    durable_file::write(path, &body).map_err(|error| ExposureError::Write {
        path: error.path,
        source: error.source,
    })
}
