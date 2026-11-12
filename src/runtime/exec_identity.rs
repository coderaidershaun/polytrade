//! Host-scoped execution ownership and durable run nonces.

use std::fs::{File, OpenOptions, TryLockError};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::adapters::exec::{EngineIdentity, LeaseNamespace, TeTag};

use super::EngineError;

/// Locks one TE identity per host and advances its venue-specific run nonce.
pub struct ExecutionLease {
    _lock_file: File,
    run_nonce: u32,
}

impl ExecutionLease {
    /// # Errors
    /// The `ExecutionIdentity*` variants for lock, nonce, or filesystem failures.
    pub fn acquire(
        directory: &Path,
        te_tag: TeTag,
        namespace: &LeaseNamespace<'_>,
    ) -> Result<Self, EngineError> {
        std::fs::create_dir_all(directory).map_err(EngineError::identity_io_at(directory))?;
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

fn lock_host_identity(te_tag: TeTag) -> Result<File, EngineError> {
    let lock_directory = std::env::temp_dir().join("polysim-execution-locks");
    std::fs::create_dir_all(&lock_directory)
        .map_err(EngineError::identity_io_at(&lock_directory))?;
    let lock_path = lock_directory.join(format!(".exec-{:08x}.lock", te_tag.get()));
    let lock_file = open_read_write(&lock_path)?;
    match lock_file.try_lock() {
        Ok(()) => Ok(lock_file),
        Err(TryLockError::WouldBlock) => {
            Err(EngineError::ExecutionIdentityInUse { path: lock_path })
        }
        Err(TryLockError::Error(source)) => Err(EngineError::identity_io_at(&lock_path)(source)),
    }
}

fn advance_nonce(path: &Path) -> Result<u32, EngineError> {
    let mut state_file = open_read_write(path)?;
    let previous = read_nonce(&mut state_file, path)?;
    let run_nonce = previous
        .checked_add(1)
        .filter(|nonce| *nonce != 0)
        .ok_or_else(|| EngineError::ExecutionIdentityExhausted {
            path: path.to_path_buf(),
        })?;
    state_file
        .seek(SeekFrom::Start(0))
        .and_then(|_| state_file.set_len(0))
        .and_then(|_| writeln!(state_file, "{run_nonce}"))
        .and_then(|_| state_file.sync_all())
        .map_err(EngineError::identity_io_at(path))?;
    Ok(run_nonce)
}

fn open_read_write(path: &Path) -> Result<File, EngineError> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(EngineError::identity_io_at(path))
}

fn read_nonce(file: &mut File, path: &Path) -> Result<u32, EngineError> {
    file.seek(SeekFrom::Start(0))
        .map_err(EngineError::identity_io_at(path))?;
    let mut body = String::new();
    file.read_to_string(&mut body)
        .map_err(EngineError::identity_io_at(path))?;
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Ok(0);
    }
    trimmed
        .parse::<u32>()
        .map_err(|_| EngineError::ExecutionIdentityState {
            path: path.to_path_buf(),
            value: trimmed.into(),
        })
}
