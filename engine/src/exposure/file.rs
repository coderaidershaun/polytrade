//! Durable on-disk document: temp write + fsync + atomic rename protects against kill-mid-write.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config::RunIdentity;
use crate::ids::FIXED_SCALE;
use crate::warn;

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

/// Write: serialize + flush + atomic rename (temp carries PID to block interleaving).
pub(super) fn write_atomically(
    path: &Path,
    document: &ExposureDocument,
) -> Result<(), ExposureError> {
    let body = serde_json::to_vec_pretty(document).map_err(|error| ExposureError::Write {
        path: path.to_path_buf(),
        source: std::io::Error::other(error),
    })?;
    let directory = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(directory).map_err(ExposureError::write_at(directory))?;
    let temporary = temporary_path(path);
    if let Err(error) = write_then_rename(&temporary, path, &body) {
        remove_temporary(&temporary);
        return Err(error);
    }
    flush_directory(directory);
    Ok(())
}

fn write_then_rename(temporary: &Path, path: &Path, body: &[u8]) -> Result<(), ExposureError> {
    let mut file = File::create(temporary).map_err(ExposureError::write_at(temporary))?;
    file.write_all(body)
        .map_err(ExposureError::write_at(temporary))?;
    // Fsync before rename: rename publishes, don't publish unflushed bytes.
    file.sync_all()
        .map_err(ExposureError::write_at(temporary))?;
    std::fs::rename(temporary, path).map_err(ExposureError::write_at(path))
}

/// Dir fsync needed for rename durability; failure warns (prior file still exists, one write stale).
fn flush_directory(directory: &Path) {
    let flushed = File::open(directory).and_then(|handle| handle.sync_all());
    if let Err(error) = flushed {
        warn!(
            "exposure: could not flush directory {} ({error}) — the file is written, its directory entry is not yet durable",
            directory.display()
        );
    }
}

fn remove_temporary(temporary: &Path) {
    if let Err(error) = std::fs::remove_file(temporary)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            "exposure: could not remove partial file {} ({error})",
            temporary.display()
        );
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(".{}.tmp", std::process::id()));
    PathBuf::from(name)
}
