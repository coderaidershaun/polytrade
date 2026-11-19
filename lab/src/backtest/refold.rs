//! Folds the whole tape of a run again, from its first message, into a run of kind `replay`,
//! through the engine and the writer that the run itself used, so a reader can compare the two
//! runs table by table.

use std::path::{Path, PathBuf};

use engine::hot::dispatch::StateDigest;
use engine::hot::strategy::{EngineView, Strategy, StrategyConfig};
use engine::msg::latency::EngineCounters;
use engine::runtime::config::{Config, RunIdentity};
use engine::runtime::manifest::{
    EndState, MANIFEST_FILE, RunEnd, RunId, RunKind, RunManifest, SourceRun, config_digest,
};
use engine::runtime::registry::Registry;
use engine::runtime::{FoldMetrics, FoldRings, FoldTape, RunFold, RunFoldSetup, build_run_fold};
use engine::tape::TapePayload;

use super::check::{check_tape, parse_hex};
use super::driver::ReplayDriver;
use super::error::ReplayError;
use super::reader::{TapeReader, tape_files};

/// What a re-fold folds and where it writes. The config is the one the source run loaded, and its
/// text proves it: a config of another digest is refused.
pub struct RefoldPlan<P> {
    /// The folder of the source run, the one that holds its `run.json`.
    pub source_run: PathBuf,
    pub config: Config<P>,
    pub config_text: String,
    /// The persistence root that the replay run is written below.
    pub out_root: PathBuf,
}

/// The replay run on disk, how many messages it folded, and the state of its fold at the end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefoldOutcome {
    pub run_dir: PathBuf,
    pub folded: u64,
    pub state_digest: StateDigest,
}

/// Checks the whole tape, then folds each of its messages in tape order and pumps the writer after
/// each one.
///
/// # Errors
/// A [`ReplayError`] of the check writes no replay folder. A read, decode or writer failure after
/// the folder opened ends its `run.json` `refused`.
pub fn refold<S>(plan: RefoldPlan<S::Params>) -> Result<RefoldOutcome, ReplayError>
where
    S: Strategy + StrategyConfig + 'static,
{
    let RefoldPlan {
        source_run,
        mut config,
        config_text,
        out_root,
    } = plan;
    let source = read_manifest(&source_run)?;
    check_config(&source, &config_text)?;
    if source.end_state == EndState::Open {
        return Err(ReplayError::SourceOpen {
            run_dir: source_run,
        });
    }
    let files = tape_files(&source_run)?;
    check_tape(&files, &source)?;
    let reader = TapeReader::of_files(files)?;

    let identity =
        RunIdentity::new(&source.strategy_id, &source.te_id).map_err(ReplayError::Config)?;
    let mut registry = Registry::build(&config).map_err(ReplayError::Config)?;
    registry
        .stamp_terms(&source.instruments)
        .map_err(ReplayError::Terms)?;
    if let Some(persistence) = config.persistence.as_mut() {
        persistence.dir = out_root.clone();
    }
    let engine_view = EngineView {
        spin_interval: config.engine.spin_interval(),
    };
    let strategy = Box::new(S::from_spec(&config.strategy, engine_view));
    let run_id = free_run_id(&out_root, &identity, &source);
    let fold = build_run_fold(RunFoldSetup {
        config: &config,
        identity: &identity,
        registry: &registry,
        strategy,
        kind: RunKind::Replay,
        run_id,
        venues: source.venues.clone(),
        exposures: reader.boot().exposures.clone(),
        source: Some(source_run_of(&source_run, &source)),
        tape: FoldTape::Absent,
        metrics: FoldMetrics::Absent,
        rings: FoldRings::SHIPPED,
        bank_full_to: None,
    })
    .map_err(ReplayError::Fold)?;
    fold_tape(fold, reader)
}

fn fold_tape(fold: RunFold, reader: TapeReader) -> Result<RefoldOutcome, ReplayError> {
    let RunFold {
        engine,
        commands,
        writer: mut inline,
        manifest,
        run_dir,
        tee: _,
        rollups: _,
    } = fold;
    let mut driver = ReplayDriver::new(engine, commands);
    let mut sent = Vec::new();
    let mut failure = None;
    for row in reader {
        let row = match row {
            Ok(row) => row,
            Err(error) => {
                failure = Some(error);
                break;
            }
        };
        // The check refused each gap before the first fold; this one holds the rule where it
        // applies, so no later change of the listing lets a replay cross a gap.
        if let TapePayload::Gap(gap) = row.payload {
            failure = Some(ReplayError::Gap {
                first_lost_seq: gap.first_lost_seq,
                last_lost_seq: gap.last_lost_seq,
            });
            break;
        }
        if let Some(message) = row.message() {
            driver.step(&message, &mut sent);
            sent.clear();
            inline.pump();
        }
    }
    let counters = driver.engine().counters();
    let outcome = match (failure, inline.finish()) {
        (Some(error), _) => Err(error),
        (None, Err(error)) => Err(ReplayError::Writer(error)),
        (None, Ok(stopped)) => lost(&counters).map(|()| stopped),
    };
    let end = match &outcome {
        Ok(stopped) => RunEnd {
            state: EndState::Graceful,
            reason: None,
            stopped_tables: stopped
                .tables()
                .into_iter()
                .map(|table| table.as_str().to_owned())
                .collect(),
        },
        Err(error) => RunEnd {
            state: EndState::Refused,
            reason: Some(error.to_string()),
            stopped_tables: Vec::new(),
        },
    };
    // The failure of the fold comes first: a manifest that would not close must not hide it.
    let closed = manifest.close(end).map_err(ReplayError::ReplayManifest);
    outcome?;
    closed?;
    Ok(RefoldOutcome {
        run_dir,
        folded: driver.folded(),
        state_digest: driver.state_digest(),
    })
}

/// The replay pumps its writer after each message, so a drop or a banked row is a bug of the
/// replay, never pressure, and its tables would differ from the run's for no reason in the run.
fn lost(counters: &EngineCounters) -> Result<(), ReplayError> {
    let losses = [
        ("research rows", counters.research_dropped.total()),
        ("audit rows", counters.audit_lost),
        ("audit rows to the bank", counters.audit_banked_peak),
    ];
    match losses.into_iter().find(|(_, count)| *count > 0) {
        Some((what, count)) => Err(ReplayError::Lost { what, count }),
        None => Ok(()),
    }
}

fn read_manifest(run_dir: &Path) -> Result<RunManifest, ReplayError> {
    let file = run_dir.join(MANIFEST_FILE);
    let text = std::fs::read_to_string(&file).map_err(|source| ReplayError::Manifest {
        file: file.clone(),
        source,
    })?;
    serde_json::from_str(&text).map_err(|source| ReplayError::ManifestParse { file, source })
}

fn check_config(source: &RunManifest, config_text: &str) -> Result<(), ReplayError> {
    let expected = config_digest(config_text);
    if source.config_digest == expected {
        return Ok(());
    }
    Err(ReplayError::ConfigMismatch {
        found: parse_hex(&source.config_digest).unwrap_or(0),
        expected: parse_hex(&expected).unwrap_or(0),
    })
}

/// The run id of the replay: the first id from the source run's own that no replay of this engine
/// holds yet. It needs no clock, so a re-fold of one tape names the same folder each time it is
/// the first.
fn free_run_id(out_root: &Path, identity: &RunIdentity, source: &RunManifest) -> RunId {
    let kind_dir = out_root
        .join(identity.strategy_id.as_str())
        .join(identity.te_id.as_str())
        .join(RunKind::Replay.as_str());
    let mut run_id = source.boot_ts_us;
    while kind_dir.join(run_id.to_string()).exists() {
        run_id += 1;
    }
    RunId::from_micros(run_id)
}

/// The source run as `run.json` names it: its folder below its persistence root.
fn source_run_of(source_run: &Path, source: &RunManifest) -> SourceRun {
    let below_root: PathBuf = source_run
        .components()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    SourceRun {
        run_dir: below_root.display().to_string(),
        run_id: source.run_id.clone(),
        config_digest: source.config_digest.clone(),
    }
}
