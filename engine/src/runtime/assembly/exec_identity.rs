//! Host-scoped execution ownership and durable run nonces.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;

use crate::runtime::durable_file;
use crate::venues::exec::{EngineIdentity, LeaseNamespace, TeTag};

use super::StartError;

/// Locks one TE identity per host and advances its venue-specific run nonce.
pub struct ExecutionLease {
    _lock_file: File,
    run_nonce: u32,
}

impl ExecutionLease {
    /// # Errors
    /// The `ExecutionIdentity*` variants for lock, nonce, or filesystem failures, and
    /// [`StartError::ExecutionNonceEmpty`] for a nonce file that holds no nonce.
    pub fn acquire(
        directory: &Path,
        te_tag: TeTag,
        namespace: &LeaseNamespace<'_>,
    ) -> Result<Self, StartError> {
        std::fs::create_dir_all(directory).map_err(StartError::identity_io_at(directory))?;
        // The lock is TE identity alone, so one host runs one process per trading engine whichever
        // venue it trades; only the nonce history below is per venue and account.
        let lock_file = lock_host_identity(te_tag)?;
        let stem = namespace.nonce_file_stem(te_tag);
        let run_nonce = advance_nonce(&directory.join(format!("{stem}.nonce")))?;
        Ok(Self {
            _lock_file: lock_file,
            run_nonce,
        })
    }

    pub const fn run_nonce(&self) -> u32 {
        self.run_nonce
    }

    pub(crate) fn identity(&self, te_tag: TeTag) -> EngineIdentity {
        EngineIdentity {
            te_tag,
            run_nonce: self.run_nonce,
        }
    }
}

fn lock_host_identity(te_tag: TeTag) -> Result<File, StartError> {
    let lock_directory = std::env::temp_dir().join("polysim-execution-locks");
    std::fs::create_dir_all(&lock_directory)
        .map_err(StartError::identity_io_at(&lock_directory))?;
    let lock_path = lock_directory.join(format!(".exec-{:08x}.lock", te_tag.get()));
    let lock_file = open_read_write(&lock_path)?;
    match lock_file.try_lock() {
        Ok(()) => Ok(lock_file),
        Err(TryLockError::WouldBlock) => {
            Err(StartError::ExecutionIdentityInUse { path: lock_path })
        }
        Err(TryLockError::Error(source)) => Err(StartError::identity_io_at(&lock_path)(source)),
    }
}

fn advance_nonce(path: &Path) -> Result<u32, StartError> {
    let previous = read_nonce(path)?.unwrap_or(0);
    let run_nonce = previous
        .checked_add(1)
        .filter(|nonce| *nonce != 0)
        .ok_or_else(|| StartError::ExecutionIdentityExhausted {
            path: path.to_path_buf(),
        })?;
    durable_file::write(path, format!("{run_nonce}\n").as_bytes()).map_err(|error| {
        StartError::ExecutionIdentityIo {
            path: error.path,
            source: error.source,
        }
    })?;
    Ok(run_nonce)
}

fn open_read_write(path: &Path) -> Result<File, StartError> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(StartError::identity_io_at(path))
}

/// `None` when no run has written the file yet. An empty file is no first run: a run stopped while
/// it wrote the file, and its last nonce is unknown.
fn read_nonce(path: &Path) -> Result<Option<u32>, StartError> {
    let body = match std::fs::read_to_string(path) {
        Ok(body) => body,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(StartError::identity_io_at(path)(source)),
    };
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Err(StartError::ExecutionNonceEmpty {
            path: path.to_path_buf(),
        });
    }
    trimmed
        .parse::<u32>()
        .map(Some)
        .map_err(|_| StartError::ExecutionIdentityState {
            path: path.to_path_buf(),
            value: trimmed.into(),
        })
}
