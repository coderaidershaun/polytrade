//! Persistence setup: the run folder and its manifest, the tape, audit and research rings, the boot
//! record that opens the tape, the footer metadata that decodes their dense indices, and the writer
//! thread that drains them, which starts once its status queue exists.

use std::path::{Path, PathBuf};
use std::time::Duration;

use rtrb::{Consumer, RingBuffer};
use tokio::sync::mpsc;

use crate::hot::dispatch::PersistWiring;
use crate::hot::exec::FeeModel;
use crate::hot::sink::{
    AUDIT_RING_CAPACITY, AuditBank, AuditSink, ResearchSink, TAPE_RING_CAPACITY, TapeSink,
    touched_tape_ring,
};
use crate::hot::strategy::Strategy;
use crate::info;
use crate::msg::ids::{FIXED_SCALE, VenueId};
use crate::msg::link::LinkHash;
use crate::msg::persist::{
    AuditRecord, BootExposure, BootRecord, BootVenue, ResearchRecord, RotationRow,
};
use crate::msg::time::wall_clock_now;
use crate::runtime::artifacts::RunArtifacts;
use crate::runtime::config::{Config, RecordedTables, RunIdentity};
use crate::runtime::manifest::{
    BudgetWindow, EndState, ManifestError, ManifestOf, OpenManifest, RunEnd, RunId, RunManifest,
    VenueEntry, VenueTerms,
};
use crate::runtime::metrics::RollupRow;
use crate::runtime::registry::Registry;
use crate::tape::{
    PersistConfig, PersistHandle, PersistWriter, ROLLUP_RING_CAPACITY, RollupRows, RunMeta,
    StoppedTables, TapeInput, TapeStatus, WriterInputs, rollup_ring,
};
use crate::venues::exec::VenueCapabilities;
use crate::{error, warn};

use super::StartPlan;
use super::exit::Exit;

/// The run folder of an engine with persistence: its boot stamp and its manifest, opened `open`.
pub(super) struct OpenRun {
    pub(super) run_id: RunId,
    pub(super) manifest: OpenManifest,
    /// The start state of the fold, which opens the tape.
    boot: BootRecord,
}

/// Opens the manifest of the run, the last step of the start that can refuse. The boot stamp is the
/// one read of the clock that names the run.
///
/// # Errors
/// [`ManifestError`] when `run.json` cannot be written.
#[expect(
    clippy::disallowed_methods,
    reason = "the boot stamp keeps the folders and file names of two runs apart; it decides no seal and no state"
)]
pub(super) fn open_run<P>(
    config: &Config<P>,
    plan: &StartPlan,
) -> Result<Option<OpenRun>, ManifestError> {
    let Some(persistence) = &config.persistence else {
        return Ok(None);
    };
    let run_id = RunId::from_micros(wall_clock_now().micros());
    let artifacts = &plan.artifacts;
    let venues = venue_entries(config, plan);
    let boot = boot_record(&venues, plan);
    let manifest = RunManifest::of(ManifestOf {
        config,
        identity: &plan.identity,
        artifacts,
        registry: &plan.registry,
        run_id,
        venues,
        identity_lock: Some(plan.identity_lock.path()),
    });
    let dir = artifacts.run_dir(&persistence.dir, artifacts.kind(), run_id);
    let manifest = RunManifest::open(&dir, &manifest)?;
    info!("run {run_id} opened at {}", dir.display());
    Ok(Some(OpenRun {
        run_id,
        manifest,
        boot,
    }))
}

/// The run nonce of each venue that trades and each position that the run before left.
fn boot_record(venues: &[VenueEntry], plan: &StartPlan) -> BootRecord {
    BootRecord {
        venues: venues
            .iter()
            .filter_map(|entry| {
                entry.run_nonce.map(|run_nonce| BootVenue {
                    venue: VenueId(entry.venue_id),
                    run_nonce,
                })
            })
            .collect(),
        exposures: plan
            .exposure
            .instruments()
            .iter()
            .filter(|exposure| !exposure.is_empty())
            .map(|exposure| BootExposure {
                instrument: exposure.instrument,
                position_base: exposure.position_base,
                cash_quote: exposure.cash_quote,
                basis_quote: exposure.basis_quote,
            })
            .collect(),
    }
}

fn venue_entries<P>(config: &Config<P>, plan: &StartPlan) -> Vec<VenueEntry> {
    let registry = &plan.registry;
    let armed = config.armed_sources().next().map(|(venue, ..)| venue);
    let traded = armed
        .zip(plan.execution.as_ref())
        .map(|(venue, execution)| {
            let (nonce, capabilities) = execution.manifest_terms();
            (venue, nonce, venue_terms(capabilities))
        });
    let venue_count = registry
        .instruments()
        .iter()
        .map(|row| usize::from(row.venue.0) + 1)
        .max()
        .unwrap_or(0);
    (0..venue_count)
        .map(|index| {
            let venue = VenueId(index as u8);
            let terms = traded.as_ref().filter(|(id, ..)| *id == venue);
            VenueEntry {
                venue_id: venue.0,
                name: registry.venue_name(venue).to_owned(),
                run_nonce: terms.map(|(_, nonce, _)| *nonce),
                capabilities: terms.map(|(.., capabilities)| capabilities.clone()),
            }
        })
        .collect()
}

fn venue_terms(capabilities: &VenueCapabilities) -> VenueTerms {
    VenueTerms {
        holds_reservations_until_settled: capabilities.holds_reservations_until_settled,
        fee_model: match capabilities.fee_model {
            FeeModel::None => "none",
            FeeModel::BinaryOutcome => "binary_outcome",
        }
        .to_owned(),
        order_budget: capabilities
            .order_budget
            .windows()
            .iter()
            .map(|window| BudgetWindow {
                window_us: window.window.micros(),
                max_places: window.max_places,
            })
            .collect(),
        rotates_markets: capabilities.rotates_markets,
        base_asset_is_position: capabilities.base_asset_is_position,
    }
}

impl OpenRun {
    /// The last write of the manifest. A failure is logged: the run has already ended, and the
    /// manifest still reads `open`, which a reader takes as a cut run.
    pub(super) fn close(self, exit: &Exit, stopped: Option<&StoppedTables>) {
        let end = RunEnd {
            state: end_state(exit),
            reason: Some(exit.to_string()),
            stopped_tables: stopped
                .map(StoppedTables::tables)
                .unwrap_or_default()
                .into_iter()
                .map(|table| table.as_str().to_owned())
                .collect(),
        };
        let path = self.manifest.path().to_path_buf();
        if let Err(failure) = self.manifest.close(end) {
            error!(
                "the end of run {} is not on disk, so {} still reads open: {failure}",
                self.run_id,
                path.display()
            );
        }
    }
}

fn end_state(exit: &Exit) -> EndState {
    match exit {
        Exit::Graceful { .. } => EndState::Graceful,
        Exit::Halted(_) => EndState::Halted,
        Exit::DrainFailed(_) => EndState::DrainFailed,
        Exit::DeadlinePassed => EndState::DeadlinePassed,
        Exit::StartRefused => EndState::StartRefused,
    }
}

/// The ring from the metrics thread to the tape writer, when the run has a tape.
pub(super) fn rollup_ring_of<P>(
    config: &Config<P>,
) -> (Option<RollupRows>, Option<Consumer<RollupRow>>) {
    config
        .persistence
        .as_ref()
        .map(|_| rollup_ring(ROLLUP_RING_CAPACITY))
        .unzip()
}

pub(super) struct PersistBringUp {
    /// `None` with no `persistence:` block: no output ring, and every `ctx.emit*` discards.
    pub(super) wiring: Option<PersistWiring>,
    /// The tee of the hot driver, the write end of the tape ring.
    pub(super) tee: Option<TapeSink>,
    pub(super) tape: Option<TapeStart>,
}

/// The writer thread of the tape before it starts. The hot thread builds the status queue of the
/// writer with each other input queue, so the writer starts after the hot thread hands it back.
pub(super) struct TapeStart {
    config: PersistConfig,
    meta: RunMeta,
    inputs: WriterInputs,
    tape: TapeInput,
}

impl TapeStart {
    pub(super) fn spawn(self, status: Option<TapeStatus>) -> PersistHandle {
        let writer = PersistWriter::on_disk().recording_tape(self.tape);
        let writer = match status {
            Some(status) => writer.reporting_to(status),
            None => writer,
        };
        writer.spawn(self.config, self.meta, self.inputs)
    }
}

pub(super) struct PersistBringUpSetup<'a, P> {
    pub(super) config: &'a Config<P>,
    pub(super) identity: &'a RunIdentity,
    pub(super) artifacts: &'a RunArtifacts,
    pub(super) registry: &'a Registry,
    /// Read from the strategy before the engine takes it, because the footer needs them and nothing
    /// gives them later.
    pub(super) strategy: &'a dyn Strategy,
    pub(super) run: Option<&'a OpenRun>,
    /// The read end of the rows of the metrics thread, `None` with no `persistence:` block.
    pub(super) rollups: Option<Consumer<RollupRow>>,
    pub(super) rotations: mpsc::Receiver<RotationRow>,
}

pub(super) fn persist_bring_up<P>(setup: PersistBringUpSetup<'_, P>) -> PersistBringUp {
    let (Some(persist_config), Some(run)) = (&setup.config.persistence, setup.run) else {
        info!("no persistence configured — nothing is recorded to disk this run");
        // Adapters wired either way -> close channel: rotations drop+count at edge, not left in buffer.
        drop(setup.rotations);
        return PersistBringUp {
            wiring: None,
            tee: None,
            tape: None,
        };
    };
    let (tape_producer, tape_records) = touched_tape_ring(TAPE_RING_CAPACITY);
    let (research_producer, research) =
        RingBuffer::<ResearchRecord>::new(setup.config.queues.persistence_capacity);
    let (audit_producer, audit) = RingBuffer::<AuditRecord>::new(AUDIT_RING_CAPACITY);
    let tables = RecordedTables::new(&setup.config.strategy.tables);
    let seal_try_limit = setup.config.seal_try_limit();
    let tape = TapeStart {
        config: PersistConfig {
            dir: persist_config.dir.clone(),
            tables,
            retention: Duration::from_hours(persist_config.retention_hours),
        },
        meta: run_meta(&setup, run.run_id),
        inputs: WriterInputs {
            audit,
            research,
            rollups: setup.rollups,
            rotations: setup.rotations,
        },
        tape: TapeInput {
            records: tape_records,
            boot: run.boot.clone(),
        },
    };
    PersistBringUp {
        wiring: Some(PersistWiring {
            audit: AuditSink::new(audit_producer, AuditBank::SHIPPED, seal_try_limit),
            research: ResearchSink::new(research_producer, seal_try_limit),
            tables,
            seal_interval_spins: setup.config.seal_interval_spins(),
        }),
        tee: Some(TapeSink::new(tape_producer)),
        tape: Some(tape),
    }
}

/// The footer of each file of the run: its identity, its mode, and the dictionaries that decode
/// its dense ids.
fn run_meta<P>(setup: &PersistBringUpSetup<'_, P>, run_id: RunId) -> RunMeta {
    footer_meta(FooterOf {
        config: setup.config,
        identity: setup.identity,
        artifacts: setup.artifacts,
        registry: setup.registry,
        strategy: setup.strategy,
        run_id,
    })
}

/// What the footer of a run is made from.
pub(super) struct FooterOf<'a, P> {
    pub(super) config: &'a Config<P>,
    pub(super) identity: &'a RunIdentity,
    pub(super) artifacts: &'a RunArtifacts,
    pub(super) registry: &'a Registry,
    pub(super) strategy: &'a dyn Strategy,
    pub(super) run_id: RunId,
}

/// One footer for a run that the start builds and for a fold of a run on one thread.
pub(super) fn footer_meta<P>(of: FooterOf<'_, P>) -> RunMeta {
    let names = |names: &[&str]| names.iter().map(|name| (*name).into()).collect();
    RunMeta {
        strategy_id: of.identity.strategy_id.clone(),
        te_id: of.identity.te_id.clone(),
        artifacts: of.artifacts.clone(),
        run_kind: of.artifacts.kind(),
        run_id: of.run_id,
        fixed_scale: FIXED_SCALE,
        engine_version: env!("CARGO_PKG_VERSION").into(),
        feature_names: names(of.strategy.features()),
        feature_kinds: of.strategy.feature_kinds().to_vec(),
        instrument_symbols: of
            .registry
            .instruments()
            .iter()
            .map(|row| row.venue_symbol.clone())
            .collect(),
        asset_symbols: of.registry.asset_symbols(),
        link_slot_names: names(of.strategy.link_fields()),
        link_topic_names: names(of.strategy.link_topics()),
        senders: senders(of.identity, of.config.source.folder.as_deref()),
    }
}

/// The link takes frames only from engines of the same strategy, so each sender is the engine
/// itself or an engine folder beside its own that holds a config.
fn senders(identity: &RunIdentity, config_folder: Option<&Path>) -> Vec<(LinkHash, Box<str>)> {
    let own = identity.te_id.as_str();
    let mut te_ids: Vec<Box<str>> = vec![own.into()];
    let beside = config_folder.and_then(Path::parent).map(engine_folders_in);
    for te_id in beside.unwrap_or_default() {
        if te_id.as_ref() != own {
            te_ids.push(te_id);
        }
    }
    te_ids
        .into_iter()
        .map(|te_id| (LinkHash::of_name(&te_id), te_id))
        .collect()
}

/// Each folder below `strategy_folder` that holds a `config.yaml`, by name, sorted.
fn engine_folders_in(strategy_folder: &Path) -> Vec<Box<str>> {
    let entries = match std::fs::read_dir(strategy_folder) {
        Ok(entries) => entries,
        Err(failure) => {
            warn!(
                "the sender dictionary names only this engine: cannot list {}: {failure}",
                strategy_folder.display()
            );
            return Vec::new();
        }
    };
    let mut names: Vec<Box<str>> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path: &PathBuf| path.join("config.yaml").is_file())
        .filter_map(|path| path.file_name()?.to_str().map(Box::from))
        .collect();
    names.sort();
    names
}
