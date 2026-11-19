//! Ships the files of one closed run: copy under a temporary name, sync, rename, compare the
//! checksum of the shipped copy with the local file, and only then delete the local file.

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use engine::runtime::manifest::MANIFEST_FILE;

use super::runs::Run;

/// The suffix of a file that the writer has not sealed. It is never shipped or deleted.
const UNSEALED_SUFFIX: &str = ".part";
/// The suffix of a copy that is not yet complete at the destination.
const COPYING_SUFFIX: &str = ".copying";

#[derive(thiserror::Error, Debug)]
pub(crate) enum ShipError {
    #[error("failed to copy {} to {}", .local.display(), .shipped.display())]
    Copy {
        local: PathBuf,
        shipped: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(
        "the copy {} has checksum {shipped_sum:016x} and the local file {} has {local_sum:016x}",
        .shipped.display(),
        .local.display()
    )]
    Checksum {
        local: PathBuf,
        shipped: PathBuf,
        local_sum: u64,
        shipped_sum: u64,
    },
    #[error("failed to delete {} after its copy checked out", .local.display())]
    Delete {
        local: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to remove the emptied folders of {}", .dir.display())]
    Tidy {
        dir: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to list the files of {}", .dir.display())]
    List {
        dir: PathBuf,
        #[source]
        source: io::Error,
    },
}

pub(crate) struct Shipped {
    pub(crate) shipped: usize,
    pub(crate) refused: Vec<ShipError>,
}

/// Ships each sealed file of `run`, then its manifest once no other file is left, then removes the
/// folders that the shipping emptied.
pub(crate) fn ship_run(run: &Run, destination: &Path) -> Shipped {
    let mut outcome = Shipped {
        shipped: 0,
        refused: Vec::new(),
    };
    let target = destination.join(&run.relative);
    let manifest = run.dir.join(MANIFEST_FILE);
    let files = match files_below(&run.dir) {
        Ok(files) => files,
        Err(source) => {
            outcome.refused.push(ShipError::List {
                dir: run.dir.clone(),
                source,
            });
            return outcome;
        }
    };
    let mut is_left = false;
    for local in files.iter().filter(|file| **file != manifest) {
        if is_unsealed(local) {
            is_left = true;
            continue;
        }
        let shipped = target.join(local.strip_prefix(&run.dir).unwrap_or(local));
        match ship_file(local, &shipped) {
            Ok(()) => outcome.shipped += 1,
            Err(error) => {
                is_left = true;
                outcome.refused.push(error);
            }
        }
    }
    if is_left {
        return outcome;
    }
    match ship_file(&manifest, &target.join(MANIFEST_FILE)) {
        Ok(()) => outcome.shipped += 1,
        Err(error) => {
            outcome.refused.push(error);
            return outcome;
        }
    }
    if let Err(source) = remove_empty_folders(&run.dir) {
        outcome.refused.push(ShipError::Tidy {
            dir: run.dir.clone(),
            source,
        });
    }
    outcome
}

fn is_unsealed(path: &Path) -> bool {
    path.to_string_lossy().ends_with(UNSEALED_SUFFIX)
}

/// # Errors
/// [`ShipError`] when the copy fails, when the copy at the destination differs, or when the local
/// file cannot be deleted. In each case the local file stays, and a copy that this call made and
/// that differs is removed.
fn ship_file(local: &Path, shipped: &Path) -> Result<(), ShipError> {
    let local_sum = checksum(local).map_err(copy_error(local, shipped))?;
    if !shipped.exists() {
        copy(local, shipped).map_err(copy_error(local, shipped))?;
    }
    let shipped_sum = checksum(shipped).map_err(copy_error(local, shipped))?;
    if shipped_sum != local_sum {
        return Err(ShipError::Checksum {
            local: local.to_path_buf(),
            shipped: shipped.to_path_buf(),
            local_sum,
            shipped_sum,
        });
    }
    fs::remove_file(local).map_err(|source| ShipError::Delete {
        local: local.to_path_buf(),
        source,
    })
}

fn copy_error<'a>(local: &'a Path, shipped: &'a Path) -> impl FnOnce(io::Error) -> ShipError + 'a {
    move |source| ShipError::Copy {
        local: local.to_path_buf(),
        shipped: shipped.to_path_buf(),
        source,
    }
}

/// The copy has its final name only once its bytes are synced, so a crash leaves no short file
/// under a final name, and the next sync writes the temporary file again.
fn copy(local: &Path, shipped: &Path) -> io::Result<()> {
    let folder = shipped
        .parent()
        .ok_or_else(|| io::Error::other("a shipped file has a folder"))?;
    fs::create_dir_all(folder)?;
    let mut copying = shipped.as_os_str().to_owned();
    copying.push(COPYING_SUFFIX);
    let copying = PathBuf::from(copying);
    fs::copy(local, &copying)?;
    File::open(&copying)?.sync_all()?;
    fs::rename(&copying, shipped)?;
    File::open(folder)?.sync_all()
}

/// FNV-1a over the bytes of the file. It finds a short or changed copy, which is what a sync
/// guards against; it is no defence against a person who forges one.
fn checksum(path: &Path) -> io::Result<u64> {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut file = File::open(path)?;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            return Ok(hash);
        }
        for byte in &buffer[..read] {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

fn files_below(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        match entry.file_type()?.is_dir() {
            true => files.extend(files_below(&path)?),
            false => files.push(path),
        }
    }
    files.sort();
    Ok(files)
}

/// Removes each folder of the run that holds nothing now, the run folder last. A folder that still
/// holds a file stays, so this never removes a record.
fn remove_empty_folders(dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            remove_empty_folders(&entry.path())?;
        }
    }
    if fs::read_dir(dir)?.next().is_none() {
        fs::remove_dir(dir)?;
    }
    Ok(())
}
