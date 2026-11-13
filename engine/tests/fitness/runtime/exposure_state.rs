//! The exposure lane keeps the newest position when its ring is full, and the registry, writer and
//! file helpers that each exposure test builds on.

use std::path::Path;

use engine::hot::sink::ExposureSink;
use engine::msg::ids::{InstrumentId, Qty};
use engine::msg::time::LocalTs;
use engine::runtime::config::{Config, ExecutionMode, NoParams, RunIdentity};
use engine::runtime::exposure::{
    ExposureError, ExposureHandle, ExposureSnapshot, ExposureState, ExposureWriter,
    ExposureWriterSetup, InstrumentExposure, WriterStatus, load,
};
use engine::runtime::registry::Registry;

use super::config_guards::sources_of;
use crate::parquet_readback::TempDir;

pub const BINANCE_SOURCE: &str = "  exchange: binance
  max_exposure_quote: 500
  market: spot
  base: BTC
  quote: USDT
  tracker: {}
";

/// Four slots that all share one base and one quote asset — the case per-asset aggregation exists
/// for, and the case per-instrument rows alone cannot answer.
pub const POLY_SOURCE: &str = "  exchange: polymarket
  max_exposure_quote: 500
  series: btc-updown-5m
  tracker: {}
";

pub fn registry_for(source_block: &str) -> Registry {
    let sources = sources_of(source_block);
    let yaml = format!(
        "engine:
  hot_core_id: 0
  spin_interval_us: 100000
queues:
  input_capacity: 65536
  persistence_capacity: 65536
{sources}strategy:
  instruments: all
logging:
  dir: ./logs
"
    );
    let config: Config<NoParams> = Config::from_yaml(&yaml).expect("document parses and validates");
    Registry::build(&config).expect("registry builds")
}

pub fn identity() -> RunIdentity {
    RunIdentity::new("recorder", "te-recorder").expect("valid identity")
}

pub fn snapshot(seq: u64, rows: &[InstrumentExposure]) -> ExposureSnapshot {
    let mut snapshot = ExposureSnapshot::for_instruments(rows.len());
    snapshot.set_rows(rows.iter().copied());
    snapshot.seq = seq;
    snapshot.exposure_quote = 999_999;
    snapshot.emitted_ts_us = LocalTs::from_micros(1_700_000_000_000_000);
    snapshot
}

pub fn row(
    instrument: u16,
    position_base: i64,
    cash_quote: i64,
    basis_quote: i64,
) -> InstrumentExposure {
    InstrumentExposure {
        instrument: InstrumentId(instrument),
        position_base: Qty::from_mantissa(position_base),
        cash_quote,
        basis_quote,
    }
}

/// Drive the real writer thread end to end: push, drain, and leave the file on disk exactly as a
/// shutdown would.
pub fn write_through_the_writer(dir: &Path, registry: &Registry, snapshots: &[ExposureSnapshot]) {
    write_from(dir, registry, &ExposureState::default(), snapshots);
}

pub fn write_from(
    dir: &Path,
    registry: &Registry,
    restored: &ExposureState,
    snapshots: &[ExposureSnapshot],
) {
    let (handle, mut sink) = spawn_live_writer(dir, registry, restored);
    for snapshot in snapshots {
        sink.push(snapshot);
    }
    // Standing in for the hot thread ending: the sink's destructor flushes what it is still holding,
    // because that snapshot is the run's final position. The boot fitness tests pin that flush directly.
    drop(sink);
    drain_writer(handle).expect("the final write succeeds");
}

pub fn spawn_live_writer(
    dir: &Path,
    registry: &Registry,
    restored: &ExposureState,
) -> (ExposureHandle, ExposureSink) {
    spawn_reporting_writer(dir, registry, restored, None)
}

/// A live writer that reports to `status` when it gives up.
pub fn spawn_reporting_writer(
    dir: &Path,
    registry: &Registry,
    restored: &ExposureState,
    status: Option<WriterStatus>,
) -> (ExposureHandle, ExposureSink) {
    let (sink, lane) = ExposureWriter::lane(registry.instruments().len());
    let handle = ExposureWriter::spawn(ExposureWriterSetup {
        dir: dir.to_path_buf(),
        identity: identity(),
        mode: Some(ExecutionMode::Live),
        registry,
        status,
        restored,
        lane,
    });
    (handle, sink)
}

/// The caller drops the sink first, so it alone decides what the writer still holds when it drains.
pub fn drain_writer(writer: ExposureHandle) -> Result<(), ExposureError> {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("build a current-thread runtime for the exposure drain")
        .block_on(writer.drain())
}

pub fn load_live(root: &Path, registry: &Registry) -> Result<ExposureState, ExposureError> {
    load(root, &identity(), registry, Some(ExecutionMode::Live))
}

/// Every other output sink drops when it is full, because a dropped frame is one never sent. A
/// position is not like that: the LAST snapshot is the one the file must end up holding, so the sink
/// keeps the newest instead of the earliest and retries it.
#[test]
fn a_full_ring_supersedes_the_stale_state_rather_than_losing_the_newest() {
    let root = TempDir::new("exposure-supersede");
    let registry = registry_for(BINANCE_SOURCE);
    // Far more snapshots than the ring holds, pushed without the writer being given a chance to
    // drain between them.
    let burst: Vec<ExposureSnapshot> = (1..=512)
        .map(|seq| {
            snapshot(
                seq,
                &[row(
                    0,
                    seq as i64 * 1_000,
                    -(seq as i64) * 7,
                    (seq as i64) * 7,
                )],
            )
        })
        .collect();
    write_through_the_writer(root.path(), &registry, &burst);

    let state = load_live(root.path(), &registry).expect("loads");
    assert_eq!(
        state.instruments(),
        [row(0, 512_000, -3_584, 3_584)],
        "the file ends on the newest state, whatever the ring did in between"
    );
}
