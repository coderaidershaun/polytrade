//! A file that a crash never leaves half written: its bytes go to a temporary name beside it, are
//! synced, and then take the final name in one rename. Each step goes through [`FileSteps`].

use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::warn;

/// Each step that writes a file and makes it durable. A test adapter can end or fail a write at
/// any one step, which no trick of the real file system can do once a file is open. Each method
/// returns the error of the file system.
pub trait FileSteps: Send + Sync {
    /// Creates the folder and each missing parent.
    fn create_folder(&self, folder: &Path) -> io::Result<()>;

    /// Creates the file, or empties it if it exists, open for writing.
    fn create(&self, path: &Path) -> io::Result<Box<dyn StepFile>>;

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// A rename survives a power cut only once its folder is synced.
    fn sync_folder(&self, folder: &Path) -> io::Result<()>;

    fn remove(&self, path: &Path) -> io::Result<()>;
}

/// A file that [`FileSteps::create`] opened. Each call of `write` is one write step.
pub trait StepFile: Write + Send {
    /// Brings the bytes written so far to the disk.
    fn sync(&mut self) -> io::Result<()>;
}

/// The steps on the real file system.
#[derive(Debug, Clone, Copy, Default)]
pub struct DiskFiles;

impl FileSteps for DiskFiles {
    fn create_folder(&self, folder: &Path) -> io::Result<()> {
        std::fs::create_dir_all(folder)
    }

    fn create(&self, path: &Path) -> io::Result<Box<dyn StepFile>> {
        Ok(Box::new(File::create(path)?))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn sync_folder(&self, folder: &Path) -> io::Result<()> {
        File::open(folder)?.sync_all()
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
}

impl StepFile for File {
    fn sync(&mut self) -> io::Result<()> {
        self.sync_all()
    }
}

/// A write that did not end durable, with the path where it failed.
#[derive(thiserror::Error, Debug)]
pub(crate) enum WriteError {
    /// The file was not replaced, so it holds what it held before.
    #[error("could not write {}", .path.display())]
    NotWritten {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    /// The file was replaced, but its folder was not synced, so a power cut can still bring back
    /// the previous version. Each caller decides whether that is acceptable.
    #[error("could not sync folder {} after a write", .directory.display())]
    FolderNotSynced {
        directory: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl WriteError {
    fn not_written_at(path: &Path) -> impl FnOnce(io::Error) -> Self + '_ {
        move |source| Self::NotWritten {
            path: path.to_path_buf(),
            source,
        }
    }
}

/// The temporary name that a file is written under, and the final name that the rename gives it.
pub(crate) struct Staging {
    pub(crate) temporary: PathBuf,
    pub(crate) target: PathBuf,
}

impl Staging {
    /// Syncs the written file, renames it to its target, and syncs the folder. Until the rename
    /// only the temporary name exists, so no reader sees a file that is not complete.
    ///
    /// # Errors
    /// [`WriteError::NotWritten`] when the sync or the rename fails, after the temporary file is
    /// removed. [`WriteError::FolderNotSynced`] when only the folder sync fails.
    pub(crate) fn finish(
        &self,
        steps: &dyn FileSteps,
        file: &mut dyn StepFile,
    ) -> Result<(), WriteError> {
        let renamed = file
            .sync()
            .map_err(WriteError::not_written_at(&self.temporary))
            .and_then(|()| {
                steps
                    .rename(&self.temporary, &self.target)
                    .map_err(WriteError::not_written_at(&self.target))
            });
        if let Err(error) = renamed {
            remove_temporary(steps, &self.temporary);
            return Err(error);
        }
        let folder = folder_of(&self.target);
        steps
            .sync_folder(folder)
            .map_err(|source| WriteError::FolderNotSynced {
                directory: folder.to_path_buf(),
                source,
            })
    }
}

pub(crate) fn write(path: &Path, body: &[u8]) -> Result<(), WriteError> {
    let steps = &DiskFiles;
    let directory = folder_of(path);
    steps
        .create_folder(directory)
        .map_err(WriteError::not_written_at(directory))?;
    let staging = Staging {
        temporary: temporary_path(path),
        target: path.to_path_buf(),
    };
    let written = steps
        .create(&staging.temporary)
        .and_then(|mut file| file.write_all(body).map(|()| file))
        .map_err(WriteError::not_written_at(&staging.temporary));
    match written {
        Ok(mut file) => staging.finish(steps, file.as_mut()),
        Err(error) => {
            remove_temporary(steps, &staging.temporary);
            Err(error)
        }
    }
}

fn folder_of(path: &Path) -> &Path {
    path.parent().unwrap_or(Path::new("."))
}

fn remove_temporary(steps: &dyn FileSteps, temporary: &Path) {
    if let Err(error) = steps.remove(temporary)
        && error.kind() != io::ErrorKind::NotFound
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
