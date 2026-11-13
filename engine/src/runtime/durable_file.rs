//! A file that a crash never leaves half written: its new bytes go to a temporary file beside it,
//! are synced, and then replace it in one rename.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::warn;

/// A write that did not end durable, with the path where it failed.
#[derive(thiserror::Error, Debug)]
pub(super) enum WriteError {
    /// The file was not replaced, so it holds what it held before.
    #[error("could not write {}", .path.display())]
    NotWritten {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The file was replaced, but its folder was not synced, so a power cut can still bring back
    /// the previous version. Each caller decides whether that is acceptable.
    #[error("could not sync folder {} after a write", .directory.display())]
    FolderNotSynced {
        directory: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl WriteError {
    fn not_written_at(path: &Path) -> impl FnOnce(std::io::Error) -> Self + '_ {
        move |source| Self::NotWritten {
            path: path.to_path_buf(),
            source,
        }
    }
}

pub(super) fn write(path: &Path, body: &[u8]) -> Result<(), WriteError> {
    let directory = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(directory).map_err(WriteError::not_written_at(directory))?;
    let temporary = temporary_path(path);
    if let Err(error) = write_then_rename(&temporary, path, body) {
        remove_temporary(&temporary);
        return Err(error);
    }
    sync_folder(directory)
}

fn write_then_rename(temporary: &Path, path: &Path, body: &[u8]) -> Result<(), WriteError> {
    let mut file = File::create(temporary).map_err(WriteError::not_written_at(temporary))?;
    file.write_all(body)
        .map_err(WriteError::not_written_at(temporary))?;
    // The rename makes the new bytes visible, so they reach the disk first.
    file.sync_all()
        .map_err(WriteError::not_written_at(temporary))?;
    std::fs::rename(temporary, path).map_err(WriteError::not_written_at(path))
}

/// A rename survives a power cut only once its folder is synced.
fn sync_folder(directory: &Path) -> Result<(), WriteError> {
    File::open(directory)
        .and_then(|handle| handle.sync_all())
        .map_err(|source| WriteError::FolderNotSynced {
            directory: directory.to_path_buf(),
            source,
        })
}

fn remove_temporary(temporary: &Path) {
    if let Err(error) = std::fs::remove_file(temporary)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        warn!(
            "could not remove partial file {} ({error})",
            temporary.display()
        );
    }
}

/// The process id keeps two processes from writing one temporary file.
fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(".{}.tmp", std::process::id()));
    PathBuf::from(name)
}
