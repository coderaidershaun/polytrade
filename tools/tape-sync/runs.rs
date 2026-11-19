//! Finds each run folder below the persistence root and decides whether anything may still be
//! writing it: a run is closed only when its manifest says it ended, or when it names an identity
//! lock that no engine holds. Its place in its folder proves nothing, since a re-fold takes the run
//! id of its source.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use engine::runtime::IdentityLocks;
use engine::runtime::config::RunIdentity;
use engine::runtime::manifest::{EndState, MANIFEST_FILE, RunManifest};

/// The folders of a run below the root: strategy, engine, kind, run.
const RUN_DEPTH: usize = 4;

pub(crate) struct Run {
    pub(crate) dir: PathBuf,
    /// The path of the run below the root, which it keeps at the destination.
    pub(crate) relative: PathBuf,
    pub(crate) state: RunState,
}

pub(crate) enum RunState {
    Closed,
    Open,
    /// Nothing proves the run closed, so the sync leaves it as it is.
    Unknown(String),
}

/// # Errors
/// The error of the file system when a folder below `root` cannot be listed.
pub(crate) fn find_runs(root: &Path) -> io::Result<Vec<Run>> {
    let mut folders = Vec::new();
    collect_folders(root, PathBuf::new(), &mut folders)?;
    let mut runs: Vec<Run> = folders
        .into_iter()
        .map(|relative| {
            let dir = root.join(&relative);
            let state = match read_manifest(&dir) {
                Err(why) => RunState::Unknown(why),
                Ok(manifest) => state_of(&manifest),
            };
            Run {
                dir,
                relative,
                state,
            }
        })
        .collect();
    runs.sort_by(|left, right| left.relative.cmp(&right.relative));
    Ok(runs)
}

fn collect_folders(root: &Path, relative: PathBuf, found: &mut Vec<PathBuf>) -> io::Result<()> {
    if relative.components().count() == RUN_DEPTH {
        found.push(relative);
        return Ok(());
    }
    for entry in fs::read_dir(root.join(&relative))? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            collect_folders(root, relative.join(entry.file_name()), found)?;
        }
    }
    Ok(())
}

fn read_manifest(dir: &Path) -> Result<RunManifest, String> {
    let path = dir.join(MANIFEST_FILE);
    let text = fs::read_to_string(&path)
        .map_err(|error| format!("its manifest {} cannot be read: {error}", path.display()))?;
    serde_json::from_str(&text)
        .map_err(|error| format!("its manifest {} does not parse: {error}", path.display()))
}

fn state_of(manifest: &RunManifest) -> RunState {
    if manifest.end_state != EndState::Open {
        return RunState::Closed;
    }
    // A run with no lock is a replay or a backtest, and nothing tells whether it still writes.
    let Some(lock) = manifest.identity_lock.as_deref() else {
        return RunState::Open;
    };
    match lock_is_held(manifest, Path::new(lock)) {
        Ok(true) => RunState::Open,
        Ok(false) => RunState::Closed,
        Err(why) => RunState::Unknown(why),
    }
}

fn lock_is_held(manifest: &RunManifest, lock: &Path) -> Result<bool, String> {
    let identity = RunIdentity::new(&manifest.strategy_id, &manifest.te_id)
        .map_err(|error| format!("its identity does not parse: {error}"))?;
    let folder = lock
        .parent()
        .ok_or_else(|| format!("its lock {} has no folder", lock.display()))?;
    let locks = IdentityLocks::in_folder(folder);
    if locks.path_of(&identity) != lock {
        return Err(format!(
            "its lock {} is not the lock of {identity}",
            lock.display()
        ));
    }
    locks
        .is_held(&identity)
        .map_err(|error| format!("its lock {} cannot be probed: {error}", lock.display()))
}
