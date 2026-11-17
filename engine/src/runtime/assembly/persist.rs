//! Persistence setup: the Parquet output ring, the footer metadata that decodes its dense
//! indices, and the writer thread that drains it.

use std::time::Duration;

use rtrb::RingBuffer;
use tokio::sync::mpsc;

use crate::hot::dispatch::PersistWiring;
use crate::hot::sink::PersistSink;
use crate::info;
use crate::msg::ids::FIXED_SCALE;
use crate::msg::persist::{PersistRecord, RotationRow};
use crate::runtime::artifacts::RunArtifacts;
use crate::runtime::config::{Config, RecordedTables, RunIdentity};
use crate::runtime::registry::Registry;
use crate::tape::{PersistConfig, PersistHandle, PersistWriter, RunMeta};

pub(super) struct PersistBringUp {
    /// `None` with no `persistence:` block: no output ring, and every `ctx.emit*` discards.
    pub(super) wiring: Option<PersistWiring>,
    pub(super) handle: Option<PersistHandle>,
}

pub(super) struct PersistBringUpSetup<'a, P> {
    pub(super) config: &'a Config<P>,
    pub(super) identity: &'a RunIdentity,
    pub(super) artifacts: &'a RunArtifacts,
    pub(super) registry: &'a Registry,
    /// Read from the strategy before the engine takes it, because the footer needs them and nothing
    /// gives them later.
    pub(super) feature_names: &'a [Box<str>],
    pub(super) rotations: mpsc::Receiver<RotationRow>,
}

pub(super) fn persist_bring_up<P>(setup: PersistBringUpSetup<'_, P>) -> PersistBringUp {
    let Some(persist_config) = &setup.config.persistence else {
        info!("no persistence configured — nothing is recorded to disk this run");
        // Adapters wired either way -> close channel: rotations drop+count at edge, not left in buffer.
        drop(setup.rotations);
        return PersistBringUp {
            wiring: None,
            handle: None,
        };
    };
    // SMELL: research rows and audit rows, such as orders and fills, share this one ring, so a
    // flood of research rows fills it and a fill row is dropped. Audit rows need a ring of their
    // own that never drops.
    let (producer, consumer) =
        RingBuffer::<PersistRecord>::new(setup.config.queues.persistence_capacity);
    let tables = RecordedTables::new(&setup.config.strategy.tables);
    let handle = PersistWriter::on_disk().spawn(
        PersistConfig {
            dir: persist_config.dir.clone(),
            tables,
            retention: Duration::from_hours(persist_config.retention_hours),
        },
        run_meta(&setup),
        consumer,
        setup.rotations,
    );
    PersistBringUp {
        wiring: Some(PersistWiring {
            sink: PersistSink::new(producer, setup.config.seal_try_limit()),
            tables,
            seal_interval_spins: setup.config.seal_interval_spins(),
        }),
        handle: Some(handle),
    }
}

/// The footer of each file of the run: its identity, its mode, and the dictionaries that decode
/// its dense ids.
fn run_meta<P>(setup: &PersistBringUpSetup<'_, P>) -> RunMeta {
    RunMeta {
        strategy_id: setup.identity.strategy_id.clone(),
        te_id: setup.identity.te_id.clone(),
        artifacts: setup.artifacts.clone(),
        fixed_scale: FIXED_SCALE,
        engine_version: env!("CARGO_PKG_VERSION").into(),
        feature_names: setup.feature_names.to_vec(),
        instrument_symbols: setup
            .registry
            .instruments()
            .iter()
            .map(|row| row.venue_symbol.clone())
            .collect(),
        asset_symbols: setup.registry.asset_symbols(),
    }
}
