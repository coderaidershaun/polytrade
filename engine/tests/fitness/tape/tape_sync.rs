//! The sync tool deletes a recorded file only once a copy with the same checksum sits at the
//! destination, and leaves each unfinished file and each file of a running engine alone, so no
//! sync ever costs a row of the record.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use engine::runtime::IdentityLocks;
use engine::runtime::config::RunIdentity;
use engine::runtime::manifest::{CONTRACT_VERSION, EndState, MANIFEST_FILE, RunKind, RunManifest};

use crate::parquet_readback::TempDir;

const STRATEGY: &str = "strat-sync";
const ENGINE: &str = "te-sync";

/// One run folder below `root`, with its manifest, the lock it names, and its tables.
struct Run {
    dir: PathBuf,
    relative: PathBuf,
}

impl Run {
    fn new(root: &Path, run_id: &str, end_state: EndState, lock: Option<&Path>) -> Self {
        Self::of_kind(root, RunKind::Live, run_id, end_state, lock)
    }

    fn of_kind(
        root: &Path,
        kind: RunKind,
        run_id: &str,
        end_state: EndState,
        lock: Option<&Path>,
    ) -> Self {
        let relative = Path::new(STRATEGY)
            .join(ENGINE)
            .join(kind.as_str())
            .join(run_id);
        let dir = root.join(&relative);
        fs::create_dir_all(&dir).expect("the run folder is made");
        let manifest = RunManifest {
            contract_version: CONTRACT_VERSION,
            run_id: run_id.to_owned(),
            kind,
            strategy_id: STRATEGY.to_owned(),
            te_id: ENGINE.to_owned(),
            execution_mode: "off".to_owned(),
            boot_ts_us: run_id.parse().expect("a run id is a stamp"),
            engine_version: "0.1.0".to_owned(),
            config_digest: "0x0000000000000000".to_owned(),
            seed: None,
            spin_interval_us: 100_000,
            seal_interval_spins: 3_000,
            warmup_us: 0,
            fixed_scale: 100_000_000,
            tables: vec!["trades".to_owned()],
            identity_lock: lock.map(|lock| lock.display().to_string()),
            source: None,
            venues: Vec::new(),
            instruments: Vec::new(),
            end_state,
            end_reason: None,
            stopped_tables: Vec::new(),
        };
        let text = serde_json::to_string_pretty(&manifest).expect("the manifest serializes");
        fs::write(dir.join(MANIFEST_FILE), text).expect("the manifest is written");
        Self { dir, relative }
    }

    /// A file of table `trades` below the run, with `bytes` as its content.
    fn file(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.dir.join("trades").join("date=2026-11-19").join(name);
        fs::create_dir_all(path.parent().expect("a file sits in a date folder"))
            .expect("the date folder is made");
        fs::write(&path, bytes).expect("the file is written");
        path
    }

    fn shipped(&self, destination: &Path, local: &Path) -> PathBuf {
        destination.join(&self.relative).join(
            local
                .strip_prefix(&self.dir)
                .expect("the file sits below its run"),
        )
    }
}

fn sync(source: &Path, destination: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tape-sync"))
        .arg("--source")
        .arg(source)
        .arg("--destination")
        .arg(destination)
        .output()
        .expect("the sync tool starts")
}

fn identity() -> RunIdentity {
    RunIdentity::new(STRATEGY, ENGINE).expect("a valid identity")
}

/// CONTRACT: a sealed file of a closed run is shipped and then deleted; a file whose name already
/// sits at the destination with the same bytes is deleted, and with other bytes it is kept, the
/// copy there is left as it was, and the tool exits 1. The manifest of a run goes last, once every
/// table file of the run is shipped.
#[test]
fn sync_deletes_only_after_checksum_of_shipped_copy() {
    let root = TempDir::new("sync-checksum");
    let (source, destination) = (root.path().join("tape"), root.path().join("shipped"));
    let closed = Run::new(&source, "1784000000000000", EndState::Graceful, None);
    let sealed = closed.file("00-1784000000000000-0.parquet", b"sealed rows");
    let output = sync(&source, &destination);
    assert!(output.status.success(), "{output:?}");
    assert!(
        !sealed.exists(),
        "a shipped file whose copy checks out is deleted"
    );
    assert_eq!(
        fs::read(closed.shipped(&destination, &sealed)).expect("the copy is at the destination"),
        b"sealed rows"
    );
    assert!(
        !closed.dir.join(MANIFEST_FILE).exists()
            && closed
                .shipped(&destination, &closed.dir.join(MANIFEST_FILE))
                .exists(),
        "the manifest of a fully shipped run goes too"
    );

    let resumed = Run::new(&source, "1784000600000000", EndState::Graceful, None);
    let already = resumed.file(
        "00-1784000600000000-0.parquet",
        b"rows shipped before a crash",
    );
    let other = resumed.file("00-1784000600000000-1.parquet", b"local rows");
    for (local, bytes) in [
        (&already, &b"rows shipped before a crash"[..]),
        (&other, b"other rows"),
    ] {
        let shipped = resumed.shipped(&destination, local);
        fs::create_dir_all(shipped.parent().expect("a parent")).expect("made");
        fs::write(&shipped, bytes).expect("a copy that a crash left");
    }
    let output = sync(&source, &destination);
    assert_eq!(
        output.status.code(),
        Some(1),
        "a differing copy fails the sync: {output:?}"
    );
    assert!(
        !already.exists(),
        "a copy with the same checksum lets the local file go"
    );
    assert_eq!(
        fs::read(&other).expect("kept"),
        b"local rows",
        "a differing copy keeps the local file"
    );
    assert_eq!(
        fs::read(resumed.shipped(&destination, &other)).expect("left"),
        b"other rows",
        "the copy at the destination is never overwritten"
    );
    assert!(
        resumed.dir.join(MANIFEST_FILE).exists(),
        "a run with a file left keeps its manifest"
    );
}

/// CONTRACT: an unfinished `.part` file is never shipped or deleted, nor is any file of a run
/// whose manifest is open while its identity lock is held, or of an open run with no lock, such
/// as a replay, whatever its place in its folder. Once the lock is free, an open run is cut, so
/// its sealed files ship and its `.part` file stays.
#[test]
fn sync_never_touches_part_file_or_open_run() {
    let root = TempDir::new("sync-open");
    let (source, destination) = (root.path().join("tape"), root.path().join("shipped"));
    let locks = IdentityLocks::in_folder(root.path().join("locks"));
    let lock_path = locks.path_of(&identity());

    let cut = Run::new(
        &source,
        "1784000000000000",
        EndState::Open,
        Some(&lock_path),
    );
    let cut_sealed = cut.file("00-1784000000000000-0.parquet", b"rows before the kill");
    let cut_part = cut.file("00-1784000000000000-1.parquet.part", b"rows the kill cut");
    let running = Run::new(
        &source,
        "1784000600000000",
        EndState::Open,
        Some(&lock_path),
    );
    let running_sealed = running.file("00-1784000600000000-0.parquet", b"sealed while running");
    let running_part = running.file("00-1784000600000000-1.parquet.part", b"being written");
    // A re-fold has no identity lock, and its run id is the boot stamp of its source, so an older
    // replay that still writes can sit below a newer one that ended.
    let writing = Run::of_kind(
        &source,
        RunKind::Replay,
        "1784000000000000",
        EndState::Open,
        None,
    );
    let writing_sealed = writing.file("00-1784000000000000-0.parquet", b"sealed by a re-fold");
    let ended = Run::of_kind(
        &source,
        RunKind::Replay,
        "1784000600000000",
        EndState::Graceful,
        None,
    );
    let ended_sealed = ended.file("00-1784000600000000-0.parquet", b"an ended re-fold");
    let held = locks
        .take(&identity())
        .expect("the test plays the running engine");

    let output = sync(&source, &destination);
    assert!(output.status.success(), "{output:?}");
    assert!(
        writing_sealed.exists() && writing.dir.join(MANIFEST_FILE).exists(),
        "a replay whose manifest is open still writes, whatever its place in the folder"
    );
    assert!(!ended_sealed.exists(), "an ended replay ships");
    for kept in [&running_sealed, &running_part, &cut_part] {
        assert!(kept.exists(), "{} is left alone", kept.display());
    }
    assert!(
        !destination.join(&running.relative).exists(),
        "nothing of the running engine ships"
    );
    assert!(
        !cut.shipped(&destination, &cut_part).exists(),
        "an unfinished file never ships"
    );
    assert!(
        cut_sealed.exists(),
        "a run whose lock is held is left alone, whatever its place in the folder"
    );

    drop(held);
    let output = sync(&source, &destination);
    assert!(output.status.success(), "{output:?}");
    assert!(
        !cut_sealed.exists() && !running_sealed.exists(),
        "once the lock is free, the sealed files of each cut run ship"
    );
    assert!(cut_part.exists() && running_part.exists());
    assert!(
        cut.dir.join(MANIFEST_FILE).exists(),
        "a run that keeps a file keeps its manifest"
    );
}

/// CONTRACT: a copy that fails leaves the local file, leaves no file under a final name at the
/// destination, and exits 1; the next sync with room ships it.
#[test]
fn failed_copy_keeps_local_file() {
    let root = TempDir::new("sync-failed");
    let (source, destination) = (root.path().join("tape"), root.path().join("shipped"));
    let run = Run::new(&source, "1784000000000000", EndState::Graceful, None);
    let sealed = run.file("00-1784000000000000-0.parquet", b"sealed rows");
    // A plain file where the run folder of the destination must go makes each copy fail.
    let blocked = destination.join(&run.relative);
    fs::create_dir_all(blocked.parent().expect("a parent")).expect("made");
    fs::write(&blocked, b"not a folder").expect("the blocking file is written");

    let output = sync(&source, &destination);
    assert_eq!(
        output.status.code(),
        Some(1),
        "a failed copy fails the sync: {output:?}"
    );
    assert_eq!(fs::read(&sealed).expect("kept"), b"sealed rows");
    assert!(run.dir.join(MANIFEST_FILE).exists());

    fs::remove_file(&blocked).expect("the test clears the way");
    let output = sync(&source, &destination);
    assert!(output.status.success(), "{output:?}");
    assert!(!sealed.exists(), "the next sync ships it");
}
