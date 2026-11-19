//! The fold of one run on one thread: the engine built from the config as the start builds it, but
//! with no venue read and no other thread, its rings drained by the caller. A re-fold builds its
//! engine here from the manifest of its run, so it folds what that run folded.

use std::path::PathBuf;
use std::time::Duration;

use rtrb::{Consumer, RingBuffer};

use crate::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup, PersistWiring};
use crate::hot::exec::{FeeModel, OrderBudget, OrderBudgetWindow};
use crate::hot::sink::{
    AUDIT_RING_CAPACITY, AuditBank, AuditSink, ExecSink, ExposureSink, ResearchSink,
    StrategyLogSink, TAPE_RING_CAPACITY, TapeSink, UiBookSink, UiEventSink, touched_tape_ring,
};
use crate::hot::spawn::QueueProducer;
use crate::hot::strategy::Strategy;
use crate::msg::exec::ExecLaneItem;
use crate::msg::ids::VenueId;
use crate::msg::persist::{AuditRecord, BootExposure, BootRecord, BootVenue, ResearchRecord};
use crate::msg::time::{DurationUs, Stamper};
use crate::runtime::artifacts::RunArtifacts;
use crate::runtime::config::{Config, RecordedTables, RunIdentity};
use crate::runtime::exposure::InstrumentExposure;
use crate::runtime::manifest::{
    ManifestError, ManifestOf, OpenManifest, RunId, RunKind, RunManifest, SourceRun, VenueEntry,
    VenueTerms,
};
use crate::runtime::registry::Registry;
use crate::tape::{
    InlineWriter, PersistConfig, PersistWriter, ROLLUP_RING_CAPACITY, Ring, RollupRows, TapeInput,
    TapeTable, WriterInputs, rollup_ring,
};
use crate::venues::exec::VenueCapabilities;

use super::exec::{command_lane_capacity, settings};
use super::persist::{FooterOf, footer_meta};

/// The output rings that no reader of a fold on one thread reads: they drop and count, and change
/// no state.
const UNREAD_RING: usize = 64;

/// Whether the fold writes the tape of what it folds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FoldTape {
    /// A run that a re-fold can read: a tee for the driver, and the tape table on disk.
    Recorded,
    /// A re-fold: its input is a tape already.
    Absent,
}

/// Whether the caller writes the rows of the metrics tables. A fold on one thread runs no metrics
/// thread, so a re-fold writes none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FoldMetrics {
    /// The caller writes rows through [`RunFold::rollups`].
    Written,
    Absent,
}

/// The sizes of the rings and the bank of the fold. A re-fold takes the shipped sizes; a test of a
/// full ring or bank passes small ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FoldRings {
    pub tape: usize,
    pub audit: usize,
    pub bank: AuditBank,
}

impl FoldRings {
    pub const SHIPPED: FoldRings = FoldRings {
        tape: TAPE_RING_CAPACITY,
        audit: AUDIT_RING_CAPACITY,
        bank: AuditBank::SHIPPED,
    };
}

/// What a fold of one run is built from.
pub struct RunFoldSetup<'a, P> {
    pub config: &'a Config<P>,
    pub identity: &'a RunIdentity,
    /// The registry with the terms of each instrument stamped, which the run read from its venue.
    pub registry: &'a Registry,
    pub strategy: Box<dyn Strategy>,
    pub kind: RunKind,
    pub run_id: RunId,
    /// Each venue of the run, in venue order; the one that trades holds its nonce and its terms.
    pub venues: Vec<VenueEntry>,
    /// The positions that the run started from.
    pub exposures: Vec<BootExposure>,
    /// For a re-fold: the run it folds again.
    pub source: Option<SourceRun>,
    pub tape: FoldTape,
    pub metrics: FoldMetrics,
    pub rings: FoldRings,
    /// The input queue that takes the one message of an audit bank at its high mark. A re-fold has
    /// none: it pumps its writer after each message, so its bank never fills.
    pub bank_full_to: Option<QueueProducer>,
}

/// The engine of the fold, the read end of its command lane, the tee, and the writer that the
/// caller pumps after each message.
pub struct RunFold {
    pub engine: HotEngine,
    /// One lane for each venue that trades, in venue order.
    pub commands: Vec<Consumer<ExecLaneItem>>,
    /// `Some` for a fold that records its tape.
    pub tee: Option<TapeSink>,
    pub writer: InlineWriter,
    /// The write end of the metrics tables, `Some` for [`FoldMetrics::Written`].
    pub rollups: Option<RollupRows>,
    pub manifest: OpenManifest,
    pub run_dir: PathBuf,
}

#[derive(thiserror::Error, Debug)]
pub enum RunFoldError {
    #[error("the config has no persistence block, so a fold of the run writes nothing")]
    NoPersistence,
    #[error(
        "venue {venue_id} trades but records no terms, so its order settings cannot be rebuilt"
    )]
    NoTerms { venue_id: u8 },
    #[error("venue {venue_id} records the fee model {found:?}, which this build does not know")]
    UnknownFeeModel { venue_id: u8, found: String },
    #[error("venue {venue_id} records {windows} order budget windows, more than this build holds")]
    OrderBudget { venue_id: u8, windows: usize },
    #[error(transparent)]
    Manifest(#[from] ManifestError),
}

/// Builds the fold of one run: its run folder with `run.json` open, its engine, and its writer.
///
/// # Errors
/// [`RunFoldError`] when the config records nothing, when the terms of the venue that trades
/// cannot be rebuilt, or when `run.json` cannot be written.
pub fn build_run_fold<P>(setup: RunFoldSetup<'_, P>) -> Result<RunFold, RunFoldError> {
    let RunFoldSetup {
        config,
        identity,
        registry,
        strategy,
        kind,
        run_id,
        venues,
        exposures,
        source,
        tape,
        metrics,
        rings,
        bank_full_to,
    } = setup;
    let persistence = config
        .persistence
        .as_ref()
        .ok_or(RunFoldError::NoPersistence)?;
    let artifacts = RunArtifacts::new(config.execution_mode(), identity);
    let mut meta = footer_meta(FooterOf {
        config,
        identity,
        artifacts: &artifacts,
        registry,
        strategy: strategy.as_ref(),
        run_id,
    });
    meta.run_kind = kind;
    let exec = exec_wiring(config, &venues)?;
    let boot = BootRecord {
        venues: venues
            .iter()
            .filter_map(|entry| {
                entry.run_nonce.map(|run_nonce| BootVenue {
                    venue: VenueId(entry.venue_id),
                    run_nonce,
                })
            })
            .collect(),
        exposures,
    };
    let mut manifest = RunManifest::of(ManifestOf {
        config,
        identity,
        artifacts: &artifacts,
        registry,
        run_id,
        venues,
        identity_lock: None,
    });
    manifest.kind = kind;
    manifest.source = source;
    manifest
        .tables
        .retain(|name| is_written(name, tape, metrics));
    let run_dir = artifacts.run_dir(&persistence.dir, kind, run_id);
    let manifest = RunManifest::open(&run_dir, &manifest)?;

    let tables = RecordedTables::new(&config.strategy.tables);
    let (research_producer, research) =
        RingBuffer::<ResearchRecord>::new(config.queues.persistence_capacity);
    let (audit_producer, audit) = RingBuffer::<AuditRecord>::new(rings.audit);
    let (rollups, rollup_input) = match metrics {
        FoldMetrics::Written => {
            let (rollups, input) = rollup_ring(ROLLUP_RING_CAPACITY);
            (Some(rollups), Some(input))
        }
        FoldMetrics::Absent => (None, None),
    };
    let writer = PersistWriter::on_disk();
    let (writer, tee) = match tape {
        FoldTape::Recorded => {
            let (producer, records) = touched_tape_ring(rings.tape);
            let input = TapeInput {
                records,
                boot: boot.clone(),
            };
            (writer.recording_tape(input), Some(TapeSink::new(producer)))
        }
        FoldTape::Absent => (writer, None),
    };
    let writer = writer.inline(
        PersistConfig {
            dir: persistence.dir.clone(),
            tables,
            retention: Duration::from_hours(persistence.retention_hours),
        },
        meta,
        WriterInputs {
            rollups: rollup_input,
            ..WriterInputs::of_rings(audit, research)
        },
    );
    let seal_try_limit = config.seal_try_limit();
    let restored = restored(registry, &boot.exposures);
    let (wirings, commands) = exec.into_iter().unzip();
    let engine = HotEngine::new(HotEngineSetup {
        instruments: registry.instruments(),
        strategy,
        persistence: Some(
            PersistWiring {
                audit: AuditSink::new(audit_producer, rings.bank, seal_try_limit),
                research: ResearchSink::new(research_producer, seal_try_limit),
                tables,
                seal_interval_spins: config.seal_interval_spins(),
            }
            .reporting_bank_full_to(bank_full_to),
        ),
        strategy_log_sink: StrategyLogSink::new(RingBuffer::new(UNREAD_RING).0),
        ui_book_sink: UiBookSink::new(RingBuffer::new(UNREAD_RING).0),
        ui_event_sink: UiEventSink::new(RingBuffer::new(UNREAD_RING).0),
        link: None,
        warmup: DurationUs::from_secs(config.engine.warmup_secs as i64),
        exec: wirings,
        exposure: ExposureWiring {
            restored: &restored,
            sink: ExposureSink::lane(UNREAD_RING, registry.instruments().len()).0,
        },
    })
    .writing_ledger_every(config.ledger_interval_spins());
    Ok(RunFold {
        engine,
        commands,
        tee,
        writer,
        rollups,
        manifest,
        run_dir,
    })
}

/// `run.json` lists each table the run writes: no tape for a fold that records none, and no
/// metrics table for a fold whose caller writes no metrics rows.
fn is_written(name: &str, tape: FoldTape, metrics: FoldMetrics) -> bool {
    let is_tape = name == TapeTable::Tape.as_str();
    let is_metrics = TapeTable::ALL
        .iter()
        .any(|table| table.ring() == Ring::Rollup && table.as_str() == name);
    match (is_tape, is_metrics) {
        (true, _) => tape == FoldTape::Recorded,
        (_, true) => metrics == FoldMetrics::Written,
        (false, false) => true,
    }
}

/// The command lane of the one venue that trades, with the settings that the start gives it, from
/// the config and the terms that the run recorded of its venue.
fn exec_wiring<P>(
    config: &Config<P>,
    venues: &[VenueEntry],
) -> Result<Option<(ExecWiring, Consumer<ExecLaneItem>)>, RunFoldError> {
    let Some((venue, _, execution)) = config.armed_sources().next() else {
        return Ok(None);
    };
    let Some(entry) = venues.iter().find(|entry| entry.venue_id == venue.0) else {
        return Ok(None);
    };
    let Some(run_nonce) = entry.run_nonce else {
        return Ok(None);
    };
    let terms = entry.capabilities.as_ref().ok_or(RunFoldError::NoTerms {
        venue_id: entry.venue_id,
    })?;
    let capabilities = capabilities_of(entry.venue_id, terms)?;
    let lane = command_lane_capacity(config.engine.spin_interval());
    let (producer, commands) = RingBuffer::<ExecLaneItem>::new(lane);
    let wiring = ExecWiring {
        sink: ExecSink::new(producer, Stamper::Off),
        settings: settings(execution, &capabilities),
        run_nonce,
    };
    Ok(Some((wiring, commands)))
}

/// The facts of a venue as `run.json` spells them, which a fold reads in place of the venue.
fn capabilities_of(venue_id: u8, terms: &VenueTerms) -> Result<VenueCapabilities, RunFoldError> {
    let fee_model = match terms.fee_model.as_str() {
        "none" => FeeModel::None,
        "binary_outcome" => FeeModel::BinaryOutcome,
        other => {
            return Err(RunFoldError::UnknownFeeModel {
                venue_id,
                found: other.to_owned(),
            });
        }
    };
    let windows: Vec<OrderBudgetWindow> = terms
        .order_budget
        .iter()
        .map(|window| OrderBudgetWindow {
            window: DurationUs::from_micros(window.window_us),
            max_places: window.max_places,
        })
        .collect();
    let order_budget = OrderBudget::of(&windows).ok_or(RunFoldError::OrderBudget {
        venue_id,
        windows: windows.len(),
    })?;
    Ok(VenueCapabilities {
        holds_reservations_until_settled: terms.holds_reservations_until_settled,
        fee_model,
        order_budget,
        rotates_markets: terms.rotates_markets,
        base_asset_is_position: terms.base_asset_is_position,
    })
}

/// One row for each instrument, each empty but the positions the run started from.
fn restored(registry: &Registry, exposures: &[BootExposure]) -> Vec<InstrumentExposure> {
    registry
        .instruments()
        .iter()
        .map(|row| {
            let boot = exposures
                .iter()
                .find(|exposure| exposure.instrument == row.instrument_id);
            InstrumentExposure {
                instrument: row.instrument_id,
                position_base: boot.map_or(InstrumentExposure::EMPTY.position_base, |boot| {
                    boot.position_base
                }),
                cash_quote: boot.map_or(0, |boot| boot.cash_quote),
                basis_quote: boot.map_or(0, |boot| boot.basis_quote),
            }
        })
        .collect()
}
