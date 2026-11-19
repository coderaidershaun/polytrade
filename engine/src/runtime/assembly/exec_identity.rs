//! One running process for each engine identity on a host, by the lock of that identity, and the
//! durable run nonce of an armed run.

use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

use crate::runtime::config::RunIdentity;
use crate::runtime::durable_file::{self, WriteError};
use crate::venues::exec::{EngineIdentity, LeaseNamespace, TeTag};

use super::StartError;

/// The folder where each running engine holds the lock of its identity. The engine takes the lock
/// and the fleet server probes it. One folder serves the whole host, so two engines of one
/// identity cannot run at once, whatever config, state folder, venue or account each reads.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdentityLocks {
    folder: PathBuf,
}

/// Held for the whole run, in each mode: while it lives, no second process of the identity
/// starts on this host.
#[derive(Debug)]
pub struct IdentityLock {
    _file: File,
    path: PathBuf,
}

impl IdentityLock {
    /// The file that holds the lock, which the manifest names so a reader can probe it.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl IdentityLocks {
    pub fn of_host() -> Self {
        Self::in_folder(std::env::temp_dir().join("polysim-execution-locks"))
    }

    /// A folder of its own, so that tests of two threads or two checkouts never share a lock.
    pub fn in_folder(folder: impl Into<PathBuf>) -> Self {
        Self {
            folder: folder.into(),
        }
    }

    /// The name is the engine identity alone, so a second account, venue or state folder is still
    /// a second process.
    pub fn path_of(&self, identity: &RunIdentity) -> PathBuf {
        self.folder
            .join(format!(".exec-{:08x}.lock", TeTag::of(identity).get()))
    }

    /// # Errors
    /// [`StartError::ExecutionIdentityInUse`] when another process holds the lock, and
    /// [`StartError::ExecutionIdentityIo`] when the folder or the file cannot be made or locked.
    pub fn take(&self, identity: &RunIdentity) -> Result<IdentityLock, StartError> {
        std::fs::create_dir_all(&self.folder).map_err(StartError::identity_io_at(&self.folder))?;
        let path = self.path_of(identity);
        let file = open_read_write(&path)?;
        match file.try_lock() {
            Ok(()) => Ok(IdentityLock { _file: file, path }),
            Err(TryLockError::WouldBlock) => Err(StartError::ExecutionIdentityInUse { path }),
            Err(TryLockError::Error(source)) => Err(StartError::identity_io_at(&path)(source)),
        }
    }

    /// Whether a running engine holds the lock. The probe takes a shared lock and lets it go at
    /// once, so two probes never see each other. An engine that starts in that instant is refused
    /// as held, which a person reads and retries.
    ///
    /// # Errors
    /// The error of the file system when the lock file exists but cannot be opened or locked.
    pub fn is_held(&self, identity: &RunIdentity) -> io::Result<bool> {
        let file = match File::open(self.path_of(identity)) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        match file.try_lock_shared() {
            Ok(()) => Ok(false),
            Err(TryLockError::WouldBlock) => Ok(true),
            Err(TryLockError::Error(error)) => Err(error),
        }
    }
}

impl Default for IdentityLocks {
    fn default() -> Self {
        Self::of_host()
    }
}

/// Advances the run nonce of one venue and account. The identity lock, taken before it, is what
/// keeps a second process of the identity from spending the same nonce.
pub struct ExecutionLease {
    run_nonce: u32,
}

impl ExecutionLease {
    /// # Errors
    /// The `ExecutionIdentity*` variants for nonce or filesystem failures, and
    /// [`StartError::ExecutionNonceEmpty`] for a nonce file that holds no nonce.
    pub fn acquire(
        directory: &Path,
        te_tag: TeTag,
        namespace: &LeaseNamespace<'_>,
    ) -> Result<Self, StartError> {
        std::fs::create_dir_all(directory).map_err(StartError::identity_io_at(directory))?;
        let stem = namespace.nonce_file_stem(te_tag);
        let run_nonce = advance_nonce(&directory.join(format!("{stem}.nonce")))?;
        Ok(Self { run_nonce })
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

fn advance_nonce(path: &Path) -> Result<u32, StartError> {
    let previous = read_nonce(path)?.unwrap_or(0);
    let run_nonce = previous
        .checked_add(1)
        .filter(|nonce| *nonce != 0)
        .ok_or_else(|| StartError::ExecutionIdentityExhausted {
            path: path.to_path_buf(),
        })?;
    // Unlike the exposure file, an unsynced folder refuses the start: a power cut could bring back
    // the previous nonce, and the next run would then mint the client ids of this one.
    durable_file::write(path, format!("{run_nonce}\n").as_bytes()).map_err(
        |error| match error {
            WriteError::NotWritten { path, source }
            | WriteError::FolderNotSynced {
                directory: path,
                source,
            } => StartError::ExecutionIdentityIo { path, source },
        },
    )?;
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
