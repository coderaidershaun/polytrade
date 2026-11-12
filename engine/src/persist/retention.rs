//! Deletes old Parquet files while the engine runs, so recorded data cannot fill the disk.
//! A file's age is its modified time, so files left by earlier runs are deleted too.

use std::fs::DirEntry;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use super::table::{DATE_FOLDER_PREFIX, FILE_EXTENSION};

pub(super) struct Retention {
    run_dir: PathBuf,
    max_age: Duration,
}

impl Retention {
    pub(super) fn new(run_dir: PathBuf, max_age: Duration) -> Self {
        Self { run_dir, max_age }
    }

    pub(super) fn announce(&self) {
        // SMELL: this rounds the age down to whole hours, so a caller that passes 90 minutes sees
        // "1 h" in the log. Config allows only whole hours, so only a library caller is misled.
        crate::info!(
            "persist retention {} h under {}",
            self.max_age.as_secs() / 3_600,
            self.run_dir.display()
        );
    }

    /// Call this only while no file is open. Deleting an open file loses its rows, and the writer
    /// still seals that file without an error.
    ///
    /// The walk enters only date folders inside table folders, so a live run never reaches into
    /// `sim/`, whose children are table folders. It never follows a symlink, so it stays inside the
    /// run's own folder.
    pub(super) fn prune(&self) {
        let Some(cutoff) = SystemTime::now().checked_sub(self.max_age) else {
            return;
        };
        for table in subdirectories(&self.run_dir, |_| true) {
            for date_dir in subdirectories(&table, |name| name.starts_with(DATE_FOLDER_PREFIX)) {
                prune_date_dir(&date_dir, cutoff);
            }
        }
    }
}

fn prune_date_dir(date_dir: &Path, cutoff: SystemTime) {
    let Some(entries) = ok_or_warn(std::fs::read_dir(date_dir), date_dir) else {
        return;
    };
    let mut survivors = 0_u32;
    for entry in entries {
        let is_pruned = ok_or_warn(entry, date_dir)
            .is_some_and(|entry| is_expired_parquet(&entry, cutoff) && prune_file(&entry.path()));
        if !is_pruned {
            survivors += 1;
        }
    }
    if survivors == 0
        && let Err(error) = std::fs::remove_dir(date_dir)
    {
        warn_io(date_dir, &error);
    }
}

fn is_expired_parquet(entry: &DirEntry, cutoff: SystemTime) -> bool {
    let path = entry.path();
    let is_parquet_file = ok_or_warn(entry.file_type(), &path).is_some_and(|kind| kind.is_file())
        && path
            .extension()
            .is_some_and(|extension| extension == FILE_EXTENSION);
    if !is_parquet_file {
        return false;
    }
    let modified = entry.metadata().and_then(|metadata| metadata.modified());
    ok_or_warn(modified, &path).is_some_and(|modified| modified < cutoff)
}

/// Returns true once the file is gone, even when something else deleted it first.
fn prune_file(path: &Path) -> bool {
    match std::fs::remove_file(path) {
        Ok(()) => {
            crate::info!("persist pruned {}", path.display());
            true
        }
        Err(error) if error.kind() == ErrorKind::NotFound => true,
        Err(error) => {
            warn_io(path, &error);
            false
        }
    }
}

fn subdirectories(parent: &Path, is_wanted: impl Fn(&str) -> bool) -> Vec<PathBuf> {
    let Some(entries) = ok_or_warn(std::fs::read_dir(parent), parent) else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| ok_or_warn(entry, parent))
        .filter(|entry| entry.file_name().to_str().is_some_and(&is_wanted))
        .filter(|entry| {
            ok_or_warn(entry.file_type(), &entry.path()).is_some_and(|kind| kind.is_dir())
        })
        .map(|entry| entry.path())
        .collect()
}

fn ok_or_warn<T>(result: io::Result<T>, path: &Path) -> Option<T> {
    result.map_err(|error| warn_io(path, &error)).ok()
}

/// A missing path is normal: a first boot has no folders yet, and a file can vanish mid-walk.
#[cold]
fn warn_io(path: &Path, error: &io::Error) {
    if error.kind() != ErrorKind::NotFound {
        crate::warn!("persist retention failed at {}: {error}", path.display());
    }
}
