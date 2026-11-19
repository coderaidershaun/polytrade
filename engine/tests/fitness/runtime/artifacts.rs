//! The mark of a simulated run on each of its artifacts, and its absence on a live run: the log,
//! the exposure, tape, nonce, and console files, the export service, and the badge.

use std::path::Path;
use std::time::Duration;

use engine::msg::ids::InstrumentId;
use engine::msg::persist::{FeatureId, FeatureRow, PersistRecord};
use engine::msg::time::LocalTs;
use engine::runtime::ExecutionLease;
use engine::runtime::artifacts::RunArtifacts;
use engine::runtime::config::{BinanceEnv, ExecutionMode, RecordedTables, RunIdentity, TableKind};
use engine::tape::PersistConfig;
use engine::venues::binance::exec as binance_exec;
use engine::venues::exec::TeTag;
use engine::venues::sim;

use crate::parquet_readback::{
    BASE_TS_US, TempDir, drain_blocking, parquet_files, read_parquet_file, spawn_writer_with,
};

/// The identity of the tape helpers, so one identity names each artifact.
const STRATEGY: &str = "recorder";
const ENGINE: &str = "te-recorder";

fn identity() -> RunIdentity {
    RunIdentity::new(STRATEGY, ENGINE).expect("a valid identity")
}

/// Each artifact of a run, as the code that writes it names it.
#[derive(Debug, PartialEq, Eq)]
struct Marks {
    log_stem: String,
    exposure_file: String,
    /// The folder of the tape, below its root.
    tape_dir: String,
    footer_mode: String,
    nonce_file: String,
    badge: &'static str,
    /// The console log, below the root of the fleet.
    #[cfg(feature = "mcp")]
    console_log: String,
    #[cfg(feature = "otlp")]
    otlp_service: String,
}

fn marks(mode: Option<ExecutionMode>) -> Marks {
    let identity = identity();
    let artifacts = RunArtifacts::new(mode, &identity);
    let (tape_dir, footer_mode) = tape_of(mode);
    Marks {
        log_stem: artifacts.stem().to_owned(),
        exposure_file: file_name(&artifacts.exposure_path(Path::new("state"))),
        tape_dir,
        footer_mode,
        nonce_file: nonce_file_of(mode, &identity),
        badge: ExecutionMode::badge(mode),
        #[cfg(feature = "mcp")]
        console_log: console_log_of(mode),
        #[cfg(feature = "otlp")]
        otlp_service: super::otlp::exported_service(&identity, mode),
    }
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .expect("a file path")
        .to_string_lossy()
        .into_owned()
}

/// The folder and the `execution_mode` footer of the one file that the tape writer writes for a
/// run of `mode`.
fn tape_of(mode: Option<ExecutionMode>) -> (String, String) {
    let root = TempDir::new("artifacts-tape");
    let config = PersistConfig {
        dir: root.path().to_path_buf(),
        tables: RecordedTables::new(&[TableKind::Features]),
        retention: Duration::from_hours(6),
    };
    let record = PersistRecord::Feature(FeatureRow {
        instrument: InstrumentId(0),
        feature: FeatureId(0),
        value: 1.0,
        event_ts_us: LocalTs::from_micros(BASE_TS_US),
    });
    drain_blocking(spawn_writer_with(config, mode, vec![record])).expect("the drain is clean");
    let files = parquet_files(root.path());
    let [file] = files.as_slice() else {
        panic!("one file of one feature row: {files:?}");
    };
    let table_dir = file
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .expect("a file sits below its table and its date folder");
    let tape_dir = table_dir
        .strip_prefix(root.path())
        .expect("the tape is below its root")
        .to_string_lossy()
        .into_owned();
    let footer = read_parquet_file(file).footer;
    let footer_mode = footer
        .iter()
        .find(|(key, _)| key == "execution_mode")
        .and_then(|(_, value)| value.clone())
        .expect("the footer carries execution_mode");
    (tape_dir, footer_mode)
}

/// The nonce file that the lease writes. A simulated run takes the namespace of the simulator,
/// and a live run the namespace of its venue and account.
fn nonce_file_of(mode: Option<ExecutionMode>, identity: &RunIdentity) -> String {
    let state = TempDir::new("artifacts-nonce");
    let namespace = if mode == Some(ExecutionMode::Sim) {
        sim::lease_namespace()
    } else {
        binance_exec::lease_namespace(BinanceEnv::Production, b"account-a")
    };
    ExecutionLease::acquire(state.path(), TeTag::of(identity), &namespace)
        .expect("the nonce of this engine identity advances");
    let mut names: Vec<String> = std::fs::read_dir(state.path())
        .expect("the state folder lists")
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".nonce"))
        .collect();
    assert_eq!(names.len(), 1, "one nonce file: {names:?}");
    names.remove(0)
}

/// The console log that the fleet names for the engine, from the mode it reads in the config.
#[cfg(feature = "mcp")]
fn console_log_of(mode: Option<ExecutionMode>) -> String {
    use engine::operator::fleet::{Fleet, FleetTiming};

    let root = TempDir::new("artifacts-fleet");
    let config = root
        .path()
        .join("strategies")
        .join(STRATEGY)
        .join(ENGINE)
        .join("config.yaml");
    std::fs::create_dir_all(config.parent().expect("config has a folder")).expect("mkdir");
    let text = match mode {
        Some(mode) => format!("sources:\n  - execution:\n      mode: {}\n", mode.as_str()),
        None => "engine:\n  spin_interval_us: 1000\n".to_owned(),
    };
    std::fs::write(&config, text).expect("write config");
    let timing = FleetTiming {
        start_grace: Duration::from_millis(500),
        stop_deadline: Duration::from_secs(1),
    };
    let listing = Fleet::new(root.path().to_path_buf(), root.path().join("bin"), timing)
        .list()
        .expect("the tree lists");
    let [engine] = listing.engines.as_slice() else {
        panic!("one engine in the tree");
    };
    Path::new(&engine.console_log)
        .strip_prefix(root.path())
        .expect("the console log is below the fleet root")
        .to_string_lossy()
        .into_owned()
}

/// FITNESS: each artifact of a simulated run carries the mark of the simulator, and a live run
/// carries none, so the names of a live run stay as they were. An artifact that a writer named
/// apart from the others could put simulated fills where a live run's are read.
#[test]
fn each_artifact_of_a_simulated_run_carries_the_mark_and_a_live_run_carries_none() {
    let live = marks(Some(ExecutionMode::Live));
    assert_eq!(
        live,
        Marks {
            log_stem: "recorder-te-recorder".into(),
            exposure_file: "recorder-te-recorder.json".into(),
            tape_dir: "recorder/te-recorder".into(),
            footer_mode: "live".into(),
            nonce_file: ".exec-production-c0ae9aaf-fc164f8250803ea8.nonce".into(),
            badge: "LIVE",
            #[cfg(feature = "mcp")]
            console_log: "logs/recorder-te-recorder-console.log".into(),
            #[cfg(feature = "otlp")]
            otlp_service: "recorder-te-recorder".into(),
        },
        "a live run carries no mark"
    );
    assert_eq!(
        marks(Some(ExecutionMode::Sim)),
        Marks {
            log_stem: "recorder-te-recorder-sim".into(),
            exposure_file: "recorder-te-recorder-sim.json".into(),
            tape_dir: "recorder/te-recorder/sim".into(),
            footer_mode: "sim".into(),
            nonce_file: ".exec-sim-c0ae9aaf.nonce".into(),
            badge: "SIM",
            #[cfg(feature = "mcp")]
            console_log: "logs/recorder-te-recorder-sim-console.log".into(),
            #[cfg(feature = "otlp")]
            otlp_service: "recorder-te-recorder-sim".into(),
        },
        "each artifact of a simulated run carries the mark"
    );
    for (mode, footer_mode) in [(Some(ExecutionMode::Off), "off"), (None, "absent")] {
        let other = marks(mode);
        assert_eq!(
            (&other.log_stem, &other.exposure_file, &other.tape_dir),
            (&live.log_stem, &live.exposure_file, &live.tape_dir),
            "a run of {mode:?} keeps the names of a live run"
        );
        assert_eq!(other.footer_mode, footer_mode, "{mode:?}");
        assert_eq!(other.badge, "OFF", "{mode:?}");
    }
}
