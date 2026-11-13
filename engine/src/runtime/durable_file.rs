//! A file that a crash never leaves half written: its new bytes go to a temporary file beside it,
//! are synced, and then replace it in one rename.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::warn;

/// A write that failed, with the path where it failed: the folder, the temporary file, or the file.
#[derive(thiserror::Error, Debug)]
#[error("could not write {}", .path.display())]
pub(super) struct WriteError {
    pub(super) path: PathBuf,
    #[source]
    pub(super) source: std::io::Error,
}

impl WriteError {
    fn at(path: &Path) -> impl FnOnce(std::io::Error) -> Self + '_ {
        move |source| Self {
            path: path.to_path_buf(),
            source,
        }
    }
}

pub(super) fn write(path: &Path, body: &[u8]) -> Result<(), WriteError> {
    let directory = path.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(directory).map_err(WriteError::at(directory))?;
    let temporary = temporary_path(path);
    if let Err(error) = write_then_rename(&temporary, path, body) {
        remove_temporary(&temporary);
        return Err(error);
    }
    flush_directory(directory);
    Ok(())
}

fn write_then_rename(temporary: &Path, path: &Path, body: &[u8]) -> Result<(), WriteError> {
    let mut file = File::create(temporary).map_err(WriteError::at(temporary))?;
    file.write_all(body).map_err(WriteError::at(temporary))?;
    // Fsync before rename: rename publishes, don't publish unflushed bytes.
    file.sync_all().map_err(WriteError::at(temporary))?;
    std::fs::rename(temporary, path).map_err(WriteError::at(path))
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
