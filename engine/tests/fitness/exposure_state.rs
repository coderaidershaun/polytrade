//! A position outlives the process that opened it. This file pins the three ways that promise
//! breaks — and the third is the one that would cost real money silently.
//!
//!  1. The file stops being readable. Every write goes through a temporary sibling and a rename, so
//!     a process killed mid-write leaves a whole document behind, never a prefix of one.
//!  2. The file stops being trustworthy. Another engine's identity, another format version, another
//!     fixed-point scale, or a live position in an instrument this config no longer names all mean
//!     the numbers say something other than what they appear to. Each refuses the boot: trading
//!     against a position that cannot be read is worse than not trading.
//!  3. **The engine overwrites its own past.** A writer that starts believing the disk is empty puts
//!     an empty document over a real position the instant anything triggers a write — before the
//!     engine has folded a single message. Nothing downstream can detect it, because the file that
//!     would have proved it is the file that was destroyed.
//!
//! What is durable is the COST BASIS, never the derived exposure: `mark` is deliberately absent
//! until the first two-sided book, so a restored valuation would be a number with nothing behind it.
//! The file carries a last-known exposure for a human, and these tests pin that the load path
//! ignores it.

use std::path::Path;

use polysim::config::{Config, ExecutionMode, NoParams, RunIdentity};
use polysim::exposure::{
    ExposureError, ExposureHandle, ExposureSnapshot, ExposureState, ExposureWriter,
    ExposureWriterConfig, InstrumentExposure, file_path, load,
};
use polysim::ids::{FIXED_SCALE, InstrumentId, Qty};
use polysim::registry::Registry;
use polysim::sink::ExposureSink;
use polysim::time::TsUs;

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
const POLY_SOURCE: &str = "  exchange: polymarket
  max_exposure_quote: 500
  series: btc-updown-5m
  tracker: {}
";

pub fn registry_for(source_block: &str) -> Registry {
    let yaml = format!(
        "engine:
  hot_core_id: 0
  spin_interval_us: 100000
queues:
  input_capacity: 65536
  persistence_capacity: 65536
source:
{source_block}strategy:
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

fn snapshot(seq: u64, rows: &[InstrumentExposure]) -> ExposureSnapshot {
    let mut snapshot = ExposureSnapshot::EMPTY;
    for (slot, row) in snapshot.instruments.iter_mut().zip(rows) {
        *slot = *row;
    }
    snapshot.len = rows.len() as u8;
    snapshot.seq = seq;
    snapshot.exposure_quote = 999_999;
    snapshot.emitted_ts_us = TsUs::from_micros(1_700_000_000_000_000);
    snapshot
}

fn row(
    instrument: u16,
    position_base: i64,
    cash_quote: i64,
    basis_quote: i64,
) -> InstrumentExposure {
    InstrumentExposure {
        instrument: InstrumentId(instrument),
        position_base: Qty(position_base),
        cash_quote,
        basis_quote,
    }
}

/// Drive the real writer thread end to end: push, drain, and leave the file on disk exactly as a
/// shutdown would.
fn write_through_the_writer(dir: &Path, registry: &Registry, snapshots: &[ExposureSnapshot]) {
    write_from(dir, registry, &ExposureState::default(), snapshots);
}

fn write_from(
    dir: &Path,
    registry: &Registry,
    restored: &ExposureState,
    snapshots: &[ExposureSnapshot],
) {
    let (handle, mut sink) = spawn_live_writer(dir, registry, restored);
    for snapshot in snapshots {
        sink.push(*snapshot);
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
    ExposureWriter::spawn(
        ExposureWriterConfig::new(
            dir.to_path_buf(),
            identity(),
            Some(ExecutionMode::Live),
            registry,
        ),
        restored,
    )
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

#[test]
fn a_missing_file_is_a_cold_start_rather_than_a_fault() {
    let root = TempDir::new("exposure-cold");
    let registry = registry_for(BINANCE_SOURCE);
    let state = load_live(root.path(), &registry).expect("a first run has no past");
    assert!(state.is_empty());
    assert!(state.assets().is_empty());
    assert!(
        !file_path(root.path(), &identity(), Some(ExecutionMode::Live)).exists(),
        "reading a position must not create one"
    );
}

#[test]
fn a_written_position_reloads_as_the_same_cost_basis() {
    let root = TempDir::new("exposure-round-trip");
    let registry = registry_for(BINANCE_SOURCE);
    // The basis is deliberately NOT minus the cash: this engine banked 3 USDT before buying the
    // position it still holds. A writer or loader that derived one from the other would pass with
    // any pair where they happen to mirror, and that derivation is the defect the field exists to
    // stop (`InstrumentExposure::basis_quote`).
    let rows = [row(0, 100_000, -11_800_000_000, 11_500_000_000)];
    write_through_the_writer(root.path(), &registry, &[snapshot(1, &rows)]);

    let state = load_live(root.path(), &registry).expect("the file this build wrote loads");
    assert_eq!(state.instruments(), rows);
    // btc +0.001 and usdt -118: the position and what it cost, each against its own asset.
    let amounts: Vec<(&str, i64)> = state
        .assets()
        .iter()
        .map(|entry| {
            (
                registry.assets().name(entry.asset).expect("named asset"),
                entry.amount,
            )
        })
        .collect();
    assert_eq!(amounts, [("BTC", 100_000), ("USDT", -11_800_000_000)]);
}

/// The catastrophic case. A writer that begins life believing the disk is empty will, on its first
/// write, put that belief on top of a real position — and the run that does it looks entirely
/// healthy. Seeding the writer with what the boot restored is what makes it impossible.
#[test]
fn a_restored_position_survives_a_run_that_never_emitted_one() {
    let root = TempDir::new("exposure-no-erase");
    let registry = registry_for(BINANCE_SOURCE);
    let rows = [row(0, 250_000, -29_500_000_000, 30_100_000_000)];
    write_through_the_writer(root.path(), &registry, &[snapshot(1, &rows)]);
    let path = file_path(root.path(), &identity(), Some(ExecutionMode::Live));
    let after_first_run = std::fs::read_to_string(&path).expect("the first run wrote a file");

    let restored = load_live(root.path(), &registry).expect("loads");
    // A whole second run: writer up, writer drained, and not one snapshot in between.
    write_from(root.path(), &registry, &restored, &[]);

    assert_eq!(
        std::fs::read_to_string(&path).expect("the file is still there"),
        after_first_run,
        "a run that folded nothing must leave the position it inherited exactly as it found it"
    );
    assert_eq!(
        load_live(root.path(), &registry)
            .expect("loads")
            .instruments(),
        rows
    );
}

/// `assets` and `last_exposure_quote` are written for a human. Both are wrong here on purpose: if
/// either were load-bearing, this test would read back a position nobody ever held.
#[test]
fn the_informational_sections_are_never_read_back() {
    let root = TempDir::new("exposure-informational");
    let registry = registry_for(BINANCE_SOURCE);
    let path = file_path(root.path(), &identity(), Some(ExecutionMode::Live));
    std::fs::create_dir_all(root.path()).expect("create the exposure directory");
    std::fs::write(
        &path,
        format!(
            r#"{{
  "version": 2,
  "strategy_id": "recorder",
  "te_id": "te-recorder",
  "written_ts_us": 1,
  "seq": 7,
  "fixed_scale": {FIXED_SCALE},
  "instruments": [{{ "symbol": "btcusdt", "position_base": 100000, "cash_quote": -11800000000, "basis_quote": 11800000000 }}],
  "assets": [{{ "asset": "BTC", "amount": 999999999 }}, {{ "asset": "DOGE", "amount": 42 }}],
  "last_exposure_quote": -123456789
}}"#
        ),
    )
    .expect("write the handmade document");

    let state = load_live(root.path(), &registry).expect("loads");
    assert_eq!(
        state.instruments(),
        [row(0, 100_000, -11_800_000_000, 11_800_000_000)]
    );
    let btc = state
        .assets()
        .iter()
        .find(|entry| registry.assets().name(entry.asset) == Some("BTC"))
        .expect("btc is aggregated from the rows");
    assert_eq!(
        btc.amount, 100_000,
        "the asset section is recomputed from the rows, never parsed"
    );
    assert!(
        state
            .assets()
            .iter()
            .all(|entry| registry.assets().name(entry.asset) != Some("DOGE")),
        "an asset the file names but the rows do not imply reaches nothing"
    );
}

/// Whether a case loads (and reads empty) or refuses the boot — the last arm of `Refuses` is what
/// stops a case passing because the boot refused for some entirely different reason.
enum Expect {
    LoadsEmpty,
    Refuses(fn(&ExposureError) -> bool),
}

type UntrustedCase = (&'static str, String, Expect);

/// FITNESS: a file this build cannot trust refuses the boot by the reason it is untrustworthy, and a
/// config change that merely drops a FLAT instrument is not among them — dropping one that still
/// holds money is, because the next write would omit the row and the position would be gone with no
/// record it ever existed.
#[test]
fn a_file_that_cannot_be_trusted_refuses_the_boot() {
    let registry = registry_for(BINANCE_SOURCE);
    let cases: [UntrustedCase; 7] = [
        (
            "malformed",
            "{ not json".to_string(),
            Expect::Refuses(|error| matches!(error, ExposureError::Malformed { .. })),
        ),
        (
            "identity",
            r#"{"version":2,"strategy_id":"other","te_id":"te-other","written_ts_us":1,"seq":1,"fixed_scale":100000000,"instruments":[]}"#.to_string(),
            Expect::Refuses(|error| matches!(error, ExposureError::WrongIdentity { .. })),
        ),
        (
            "version",
            r#"{"version":99,"strategy_id":"recorder","te_id":"te-recorder","written_ts_us":1,"seq":1,"fixed_scale":100000000,"instruments":[]}"#.to_string(),
            Expect::Refuses(|error| matches!(error, ExposureError::UnknownVersion { .. })),
        ),
        (
            "scale",
            r#"{"version":2,"strategy_id":"recorder","te_id":"te-recorder","written_ts_us":1,"seq":1,"fixed_scale":1000,"instruments":[]}"#.to_string(),
            Expect::Refuses(|error| matches!(error, ExposureError::ScaleMismatch { .. })),
        ),
        // A version-1 document: rows with no `basis_quote`. What the position COST cannot be
        // recovered from cash, and inferring it is the defect the field was added to stop — so an
        // older file is refused rather than adopted, and refused by VERSION so the message names
        // what actually changed instead of reporting a corrupt document.
        (
            "pre-basis",
            r#"{"version":1,"strategy_id":"recorder","te_id":"te-recorder","written_ts_us":1,"seq":1,"fixed_scale":100000000,"instruments":[{"symbol":"btcusdt","position_base":100000,"cash_quote":-11800000000}]}"#.to_string(),
            Expect::Refuses(|error| matches!(error, ExposureError::UnknownVersion { found: 1, .. })),
        ),
        (
            "dropped-instrument-flat",
            document_naming("ethusdt", 0, 0),
            Expect::LoadsEmpty,
        ),
        (
            "dropped-instrument-live",
            document_naming("ethusdt", 100_000, -300_000_000),
            Expect::Refuses(|error| matches!(error, ExposureError::UnknownInstrument { .. })),
        ),
    ];
    for (name, body, expect) in cases {
        let root = TempDir::new(&format!("exposure-untrusted-{name}"));
        std::fs::create_dir_all(root.path()).expect("create the exposure directory");
        std::fs::write(
            file_path(root.path(), &identity(), Some(ExecutionMode::Live)),
            body,
        )
        .expect("write the case");
        let result = load_live(root.path(), &registry);
        match expect {
            Expect::LoadsEmpty => assert!(
                result
                    .expect("a flat row for a gone instrument costs nothing to drop")
                    .is_empty(),
                "case {name}: expected an empty state"
            ),
            Expect::Refuses(is_expected) => {
                let error = result.expect_err("an untrustworthy file must refuse the boot");
                assert!(is_expected(&error), "{name} produced {error:?}");
            }
        }
    }
}

fn document_naming(symbol: &str, position_base: i64, cash_quote: i64) -> String {
    format!(
        r#"{{"version":2,"strategy_id":"recorder","te_id":"te-recorder","written_ts_us":1,"seq":1,"fixed_scale":{FIXED_SCALE},"instruments":[{{"symbol":"{symbol}","position_base":{position_base},"cash_quote":{cash_quote},"basis_quote":0}}]}}"#
    )
}

/// The risk question is "how much of this asset am I holding", not "how much per market", and only
/// an asset view answers it — per-instrument rows cannot.
///
/// Which instruments SHARE an asset is the venue's answer, not a convenience. Four polymarket slots
/// are four separate conditional tokens settling against one collateral, so the collateral folds to
/// a single number across all four while the share balances stay four numbers. Folding those
/// together would report one leg's inventory as another's, and the sell-side funds gate spends
/// against exactly that number.
#[test]
fn assets_aggregate_across_every_instrument_that_shares_one() {
    let root = TempDir::new("exposure-aggregate");
    let registry = registry_for(POLY_SOURCE);
    let rows = [
        row(0, 10, -100, 100),
        row(1, 20, -200, 200),
        row(2, 30, -300, 300),
        row(3, -5, 50, -50),
    ];
    write_through_the_writer(root.path(), &registry, &[snapshot(1, &rows)]);

    let state = load_live(root.path(), &registry).expect("loads");
    assert_eq!(state.instruments(), rows);
    let amounts: Vec<(&str, i64)> = state
        .assets()
        .iter()
        .map(|entry| {
            (
                registry.assets().name(entry.asset).expect("named asset"),
                entry.amount,
            )
        })
        .collect();
    assert_eq!(
        amounts,
        [
            ("btc-updown-5m-a-up", 10),
            ("USD", -550),
            ("btc-updown-5m-a-down", 20),
            ("btc-updown-5m-b-up", 30),
            ("btc-updown-5m-b-down", -5),
        ],
        "one collateral folded across four markets, four share balances left apart"
    );
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
