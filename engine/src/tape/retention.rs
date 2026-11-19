//! Deletes old Parquet files while the engine runs, so recorded data cannot fill the disk, and the
//! unfinished files that an earlier run left when it ended before their footer.

use std::fs::DirEntry;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use super::table::{DATE_FOLDER_PREFIX, FILE_EXTENSION, UNFINISHED_EXTENSION};

pub(super) struct Retention {
    /// The folder of each run of one kind: `{strategy}/{te}/{kind}` below the root.
    kind_dir: PathBuf,
    max_age: Duration,
    /// The boot stamp in the names of this writer's own files, which are never unfinished leftovers.
    boot_ts_us: i64,
}

impl Retention {
    pub(super) fn new(kind_dir: PathBuf, max_age: Duration, boot_ts_us: i64) -> Self {
        Self {
            kind_dir,
            max_age,
            boot_ts_us,
        }
    }

    pub(super) fn announce(&self) {
        // SMELL: this rounds the age down to whole hours, so a caller that passes 90 minutes sees
        // "1 h" in the log. Config allows only whole hours, so only a library caller is misled.
        crate::info!(
            "persist retention {} h under {}",
            self.max_age.as_secs() / 3_600,
            self.kind_dir.display()
        );
    }

    /// A file's age is its modified time, so files left by earlier runs are deleted too. An
    /// unfinished file of another boot is deleted at any age: no writer finishes it, and it has no
    /// footer. A file that this writer has open is unfinished and has its boot stamp, so the walk
    /// never deletes it.
    ///
    /// The walk enters only date folders inside the table folders of the runs of one kind, so a
    /// live run never reaches into `sim/`. It never follows a symlink, so it stays inside the
    /// folder of its kind. A run folder keeps its `run.json` after its files expire.
    #[expect(
        clippy::disallowed_methods,
        reason = "a writer stamps and paces its own files, which are never engine state"
    )]
    pub(super) fn prune(&self) {
        let Some(cutoff) = SystemTime::now().checked_sub(self.max_age) else {
            return;
        };
        let mut unfinished = 0_u32;
        for run in subdirectories(&self.kind_dir, |_| true) {
            for table in subdirectories(&run, |_| true) {
                for date_dir in subdirectories(&table, |name| name.starts_with(DATE_FOLDER_PREFIX))
                {
                    unfinished += self.prune_date_dir(&date_dir, cutoff);
                }
            }
        }
        if unfinished > 0 {
            crate::info!(
                "persist removed {unfinished} unfinished files that an earlier run left without a footer"
            );
        }
    }

    /// Returns the count of unfinished files that it removed.
    fn prune_date_dir(&self, date_dir: &Path, cutoff: SystemTime) -> u32 {
        let Some(entries) = ok_or_warn(std::fs::read_dir(date_dir), date_dir) else {
            return 0;
        };
        let mut survivors = 0_u32;
        let mut unfinished = 0_u32;
        for entry in entries {
            let Some(entry) = ok_or_warn(entry, date_dir) else {
                survivors += 1;
                continue;
            };
            let is_leftover = self.is_unfinished_of_another_boot(&entry);
            let is_pruned =
                (is_leftover || is_expired_parquet(&entry, cutoff)) && prune_file(&entry.path());
            if !is_pruned {
                survivors += 1;
            } else if is_leftover {
                unfinished += 1;
            }
        }
        if survivors == 0
            && let Err(error) = std::fs::remove_dir(date_dir)
        {
            warn_io(date_dir, &error);
        }
        unfinished
    }

    /// A name is `{hour}-{boot stamp}-{sequence}.parquet.part`.
    fn is_unfinished_of_another_boot(&self, entry: &DirEntry) -> bool {
        let path = entry.path();
        let is_unfinished = path
            .extension()
            .is_some_and(|extension| extension == UNFINISHED_EXTENSION)
            && ok_or_warn(entry.file_type(), &path).is_some_and(|kind| kind.is_file());
        let boot_ts_us = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.split('-').nth(1))
            .and_then(|stamp| stamp.parse::<i64>().ok());
        is_unfinished && boot_ts_us != Some(self.boot_ts_us)
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
