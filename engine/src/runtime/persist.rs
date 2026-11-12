//! Persistence setup: the Parquet output ring, the footer metadata that decodes its dense
//! indices, and the writer thread that drains it.

use std::time::Duration;

use rtrb::RingBuffer;
use tokio::sync::mpsc;

use crate::config::{Config, RecordedTables, RunIdentity};
use crate::hot::dispatch::PersistWiring;
use crate::ids::FIXED_SCALE;
use crate::info;
use crate::msg::persist::{PersistRecord, RotationRow};
use crate::persist::{PersistConfig, PersistHandle, PersistWriter, RunMeta};
use crate::registry::Registry;
use crate::sink::PersistSink;

pub(super) struct PersistBringUp {
    /// `None` with no `persistence:` block: no output ring, and every `ctx.emit*` discards.
    pub(super) wiring: Option<PersistWiring>,
    pub(super) handle: Option<PersistHandle>,
}

pub(super) struct PersistBringUpSetup<'a, P> {
    pub(super) config: &'a Config<P>,
    pub(super) identity: &'a RunIdentity,
    pub(super) registry: &'a Registry,
    /// Read off strategy before it moved to engine -> footer needs them, later none available.
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
    let (producer, consumer) =
        RingBuffer::<PersistRecord>::new(setup.config.queues.persistence_capacity);
    let tables = RecordedTables::new(&setup.config.strategy.tables);
    let handle = PersistWriter::spawn(
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
            sink: PersistSink::new(producer),
            tables,
        }),
        handle: Some(handle),
    }
}

/// Dictionaries so dense indices decode.
fn run_meta<P>(setup: &PersistBringUpSetup<'_, P>) -> RunMeta {
    RunMeta {
        strategy_id: setup.identity.strategy_id.clone(),
        te_id: setup.identity.te_id.clone(),
        execution_mode: setup
            .config
            .execution
            .as_ref()
            .map(|execution| execution.mode),
        fixed_scale: FIXED_SCALE,
        engine_version: env!("CARGO_PKG_VERSION").into(),
        feature_names: setup.feature_names.to_vec(),
        instrument_symbols: setup
            .registry
            .instruments()
            .iter()
            .map(|row| row.venue_symbol.clone())
            .collect(),
        asset_symbols: setup.registry.assets().names().to_vec(),
    }
}
