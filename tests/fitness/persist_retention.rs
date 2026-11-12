//! The writer deletes Parquet files older than the configured age while it runs, and removes the
//! date folders it empties. A wrong deletion loses data silently, so everything else must survive:
//! fresh files, files it does not target, and the whole tree of the other execution mode.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use polysim::config::{ExecutionMode, RecordedTables, TableKind};
use polysim::ids::InstrumentId;
use polysim::msg::persist::{FeatureId, FeatureRow, PersistRecord};
use polysim::persist::PersistConfig;
use polysim::time::TsUs;

use crate::parquet_readback::{
    BASE_TS_US, TempDir, drain_blocking, parquet_files, run_dir, spawn_writer_with,
};

const RETENTION: Duration = Duration::from_hours(6);
const STALE_AGE: Duration = Duration::from_hours(7);

struct Seeds {
    old_in_written_folder: PathBuf,
    old_alone: PathBuf,
    old_outside_date_folder: PathBuf,
    fresh: PathBuf,
    old_not_parquet: PathBuf,
}

impl Seeds {
    fn plant(run_dir: &Path) -> Self {
        let seeds = Self {
            old_in_written_folder: run_dir.join("features/date=1970-01-01/00-old-0.parquet"),
            old_alone: run_dir.join("trades/date=1969-12-31/00-old-0.parquet"),
            old_outside_date_folder: run_dir.join("features/00-old-0.parquet"),
            fresh: run_dir.join("klines/date=1970-01-01/00-fresh-0.parquet"),
            old_not_parquet: run_dir.join("klines/date=1970-01-01/00-old-0.txt"),
        };
        plant_file(&seeds.old_in_written_folder, STALE_AGE);
        plant_file(&seeds.old_alone, STALE_AGE);
        plant_file(&seeds.old_outside_date_folder, STALE_AGE);
        plant_file(&seeds.fresh, Duration::ZERO);
        plant_file(&seeds.old_not_parquet, STALE_AGE);
        seeds
    }

    fn all(&self) -> [&Path; 5] {
        [
            &self.old_in_written_folder,
            &self.old_alone,
            &self.old_outside_date_folder,
            &self.fresh,
            &self.old_not_parquet,
        ]
    }
}

fn plant_file(path: &Path, age: Duration) {
    std::fs::create_dir_all(path.parent().expect("seed has a folder")).expect("seed folder");
    std::fs::write(path, b"seed").expect("write seed");
    let modified = SystemTime::now() - age;
    File::options()
        .write(true)
        .open(path)
        .expect("reopen seed")
        .set_modified(modified)
        .expect("backdate seed");
}

fn feature_record() -> PersistRecord {
    PersistRecord::Feature(FeatureRow {
        instrument: InstrumentId(0),
        feature: FeatureId(0),
        value: 1.0,
        event_ts_us: TsUs::from_micros(BASE_TS_US),
    })
}

fn run_retention(mode: ExecutionMode) {
    let dir = TempDir::new("persist-retention");
    let live_dir = run_dir(dir.path());
    let sim_dir = live_dir.join("sim");
    let live_seeds = Seeds::plant(&live_dir);
    let sim_seeds = Seeds::plant(&sim_dir);
    let (own_dir, own, other) = match mode {
        ExecutionMode::Sim => (&sim_dir, &sim_seeds, &live_seeds),
        _ => (&live_dir, &live_seeds, &sim_seeds),
    };

    let config = PersistConfig {
        dir: dir.path().to_path_buf(),
        tables: RecordedTables::new(&[TableKind::Features]),
        retention: RETENTION,
    };
    let handle = spawn_writer_with(config, Some(mode), vec![feature_record()]);
    drain_blocking(handle).expect("the drain is clean");

    assert!(
        !own.old_in_written_folder.exists(),
        "{mode:?}: old file pruned"
    );
    assert!(!own.old_alone.exists(), "{mode:?}: old lone file pruned");
    assert!(
        !own.old_alone.parent().expect("has a folder").exists(),
        "{mode:?}: the emptied date folder is removed"
    );
    assert!(
        own.old_outside_date_folder.exists(),
        "{mode:?}: a file outside a date folder is kept"
    );
    assert!(own.fresh.exists(), "{mode:?}: fresh file kept");
    assert!(own.old_not_parquet.exists(), "{mode:?}: non-parquet kept");
    let features = parquet_files(&own_dir.join("features/date=1970-01-01"));
    assert_eq!(
        features.len(),
        1,
        "{mode:?}: only the new file is left there"
    );
    assert_ne!(
        features[0], own.old_in_written_folder,
        "{mode:?}: the file left there is the new one"
    );

    for path in other.all() {
        assert!(
            path.exists(),
            "{mode:?}: other tree untouched: {}",
            path.display()
        );
    }
}

#[test]
fn live_run_prunes_only_the_live_tree() {
    run_retention(ExecutionMode::Live);
}

#[test]
fn sim_run_prunes_only_the_sim_tree() {
    run_retention(ExecutionMode::Sim);
}
