//! Writes the golden run: two boots of one engine through the real rings, writer thread and run
//! manifest, with literal rows that give each table, label and absent value, and `expected.json`,
//! which holds each cell and each metric as a value a person can check by hand. Every reader of
//! the tape is pinned against these numbers.

mod cells;
mod columns;
mod first;
mod rollups;
mod second;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use rtrb::RingBuffer;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use engine::msg::ids::FIXED_SCALE;
use engine::msg::link::LinkHash;
use engine::msg::persist::{AuditRecord, ResearchRecord};
use engine::runtime::artifacts::RunArtifacts;
use engine::runtime::config::{
    ExecutionMode, RecordedTables, RunIdentity, StrategyId, TableKind, TradingEngineId,
};
use engine::runtime::manifest::{
    CONTRACT_VERSION, EndState, InstrumentTerms, RunEnd, RunKind, RunManifest, VenueEntry,
    VenueTerms, config_digest,
};
use engine::runtime::metrics::RollupSink;
use engine::tape::{
    PersistConfig, PersistWriter, RunMeta, TAPE_KINDS, TapeTable, WriterInputs, rollup_ring,
};

use cells::Run;

const STRATEGY: &str = "golden";
const ENGINE: &str = "te-golden";
const PEER: &str = "te-peer";
const RUN_NONCE: u32 = 7;
/// The config the golden runs say they used. No engine reads it: its digest is what a replay
/// would compare.
const CONFIG_TEXT: &str =
    "engine:\n  spin_interval_us: 5000000\npersistence:\n  dir: target/golden-run\n";
const END_REASON: &str = "received golden-run";
/// A fixed path, so two runs of the tool into two folders give the same `expected.json`.
const IDENTITY_LOCK: &str = "/tmp/polysim-execution-locks/.exec-golden.lock";

const ALL_TABLES: [TableKind; 7] = [
    TableKind::Orders,
    TableKind::Fills,
    TableKind::Features,
    TableKind::Trades,
    TableKind::BookEvents,
    TableKind::Klines,
    TableKind::LinkFrames,
];

fn identity() -> RunIdentity {
    RunIdentity::new(STRATEGY, ENGINE).expect("the golden identity is valid")
}

fn artifacts() -> RunArtifacts {
    RunArtifacts::new(Some(ExecutionMode::Sim), &identity())
}

fn sender_hex(te_id: &str) -> String {
    format!("0x{:016x}", LinkHash::of_name(te_id).0)
}

fn meta(run: &Run) -> RunMeta {
    let names = |names: &[&str]| names.iter().map(|name| (*name).into()).collect();
    RunMeta {
        strategy_id: StrategyId::new(STRATEGY).expect("a valid strategy id"),
        te_id: TradingEngineId::new(ENGINE).expect("a valid engine id"),
        artifacts: artifacts(),
        run_kind: RunKind::Sim,
        run_id: run.run_id,
        fixed_scale: FIXED_SCALE,
        engine_version: env!("CARGO_PKG_VERSION").into(),
        feature_names: names(run.feature_names),
        feature_kinds: run.feature_kinds.to_vec(),
        instrument_symbols: vec!["BTCUSDT".into()],
        asset_symbols: vec![
            ("sim".into(), "btc".into()),
            ("sim".into(), "usdt".into()),
            ("sim".into(), "bnb".into()),
        ],
        link_slot_names: vec!["spread".into()],
        link_topic_names: vec!["quotes".into()],
        senders: [ENGINE, PEER]
            .map(|te_id| (LinkHash::of_name(te_id), te_id.into()))
            .into(),
    }
}

/// The footer that each file of `run` must hold, spelled out key by key.
fn expected_footer(run: &Run) -> Value {
    let features: Vec<&str> = run.feature_names.to_vec();
    let kinds: Vec<&str> = run.feature_kinds.iter().map(|kind| kind.as_str()).collect();
    let senders = json!({ sender_hex(ENGINE): ENGINE, sender_hex(PEER): PEER });
    json!({
        "strategy_id": STRATEGY,
        "te_id": ENGINE,
        "execution_mode": "sim",
        "fixed_scale": "100000000",
        "feature_dictionary": serde_json::to_string(&features).expect("names are json"),
        "instrument_dictionary": "[\"BTCUSDT\"]",
        "asset_dictionary": "[[\"sim\",\"btc\"],[\"sim\",\"usdt\"],[\"sim\",\"bnb\"]]",
        "engine_version": env!("CARGO_PKG_VERSION"),
        "contract_version": "1",
        "boot_ts_us": run.run_id.to_string(),
        "run_kind": "sim",
        "link_slot_names": "[\"spread\"]",
        "link_topic_names": "[\"quotes\"]",
        "sender_dictionary": senders.to_string(),
        "tape_kinds": tape_kinds(),
        "feature_kinds": serde_json::to_string(&kinds).expect("labels are json"),
    })
}

/// The tape kinds of this build, as each footer names them. The golden run records no tape, and
/// the footer still names each kind the build writes.
fn tape_kinds() -> String {
    let kinds: Vec<Value> = TAPE_KINDS
        .iter()
        .map(|kind| json!({ "kind": kind.id, "name": kind.name, "digest": kind.digest_text() }))
        .collect();
    Value::Array(kinds).to_string()
}

fn manifest(run: &Run) -> RunManifest {
    // The golden run writes no tape, so its manifest names none.
    let tables = TapeTable::written(RecordedTables::new(&ALL_TABLES))
        .filter(|table| *table != TapeTable::Tape)
        .map(|table| table.as_str().to_owned())
        .collect();
    RunManifest {
        contract_version: CONTRACT_VERSION,
        run_id: run.run_id.to_string(),
        kind: RunKind::Sim,
        strategy_id: STRATEGY.into(),
        te_id: ENGINE.into(),
        execution_mode: "sim".into(),
        boot_ts_us: run.run_id.micros(),
        engine_version: env!("CARGO_PKG_VERSION").into(),
        config_digest: config_digest(CONFIG_TEXT),
        seed: None,
        spin_interval_us: 5_000_000,
        seal_interval_spins: 60,
        warmup_us: 10_000_000,
        fixed_scale: FIXED_SCALE,
        tables,
        identity_lock: Some(IDENTITY_LOCK.into()),
        source: None,
        venues: vec![VenueEntry {
            venue_id: 0,
            name: "sim".into(),
            run_nonce: Some(RUN_NONCE),
            capabilities: Some(VenueTerms {
                holds_reservations_until_settled: true,
                fee_model: "none".into(),
                order_budget: Vec::new(),
                rotates_markets: false,
                base_asset_is_position: false,
            }),
        }],
        instruments: vec![InstrumentTerms {
            instrument_id: 0,
            venue_id: 0,
            venue_symbol: "BTCUSDT".into(),
            display: "BTC/USDT".into(),
            base: "BTC".into(),
            quote: "USDT".into(),
            base_asset_id: 0,
            quote_asset_id: 1,
            tick_size: Some(1_000_000),
            lot_size: Some(1_000),
            min_qty: Some(1_000),
            min_notional: Some(500_000_000),
            max_num_orders: Some(200),
            max_num_order_amends: None,
            max_price: None,
            qty_scale: FIXED_SCALE,
        }],
        end_state: EndState::Open,
        end_reason: None,
        stopped_tables: Vec::new(),
    }
}

/// Writes one boot through the writer thread and its manifest, and gives back its part of
/// `expected.json`.
fn write_run(out: &Path, runtime: &tokio::runtime::Runtime, run: Run) -> Result<Value> {
    let meta = meta(&run);
    let run_dir = meta.run_dir(out);
    let manifest = manifest(&run);
    let open = RunManifest::open(&run_dir, &manifest)
        .with_context(|| format!("failed to open the manifest of run {}", run.run_id))?;

    let (mut audit_producer, audit) = RingBuffer::<AuditRecord>::new(run.audit.len() + 1);
    let (mut research_producer, research) =
        RingBuffer::<ResearchRecord>::new(run.research.len() + 1);
    let (mut rollups, rollup_consumer) = rollup_ring(run.rollups.len() + 1);
    let (rotations_tx, rotations) = mpsc::channel(run.rotations.len() + 1);
    for record in &run.audit {
        if audit_producer.push(*record).is_err() {
            bail!("the audit ring has no room for a row of run {}", run.run_id);
        }
    }
    for record in &run.research {
        if research_producer.push(*record).is_err() {
            bail!(
                "the research ring has no room for a row of run {}",
                run.run_id
            );
        }
    }
    for row in &run.rollups {
        rollups.write(*row);
    }
    for row in &run.rotations {
        rotations_tx
            .try_send(row.clone())
            .context("the rotation channel has room for each row")?;
    }
    drop(rotations_tx);
    let handle = PersistWriter::on_disk().spawn(
        PersistConfig {
            dir: out.to_path_buf(),
            tables: RecordedTables::new(&ALL_TABLES),
            retention: std::time::Duration::from_hours(6),
        },
        meta,
        WriterInputs {
            audit,
            research,
            rollups: Some(rollup_consumer),
            rotations,
        },
    );
    let stopped = handle.stopped_tables();
    runtime
        .block_on(handle.drain())
        .with_context(|| format!("the writer of run {} failed", run.run_id))?;
    if stopped.count() > 0 {
        bail!(
            "the writer of run {} stopped {:?}",
            run.run_id,
            stopped.tables()
        );
    }
    let end = RunEnd {
        state: EndState::Graceful,
        reason: Some(END_REASON.into()),
        stopped_tables: Vec::new(),
    };
    open.close(end.clone())
        .with_context(|| format!("failed to close the manifest of run {}", run.run_id))?;
    let closed = RunManifest {
        end_state: end.state,
        end_reason: end.reason,
        stopped_tables: end.stopped_tables,
        ..manifest
    };
    let relative = run_dir
        .strip_prefix(out)
        .context("each run sits below the output folder")?;
    Ok(json!({
        "run_dir": relative.to_string_lossy(),
        "manifest": closed,
        "footer": expected_footer(&run),
        "tables": run.tables_json(),
        "metrics": run.metrics,
    }))
}

fn run(out: &Path) -> Result<()> {
    if out.exists() {
        std::fs::remove_dir_all(out)
            .with_context(|| format!("failed to delete {}", out.display()))?;
    }
    std::fs::create_dir_all(out).with_context(|| format!("failed to create {}", out.display()))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .context("failed to build the runtime that drains the writer")?;
    let runs = [first::run(), second::run()]
        .into_iter()
        .map(|run| write_run(out, &runtime, run))
        .collect::<Result<Vec<Value>>>()?;
    let expected = json!({ "contract_version": CONTRACT_VERSION, "runs": runs });
    let mut text = serde_json::to_string_pretty(&expected).context("expected.json serialises")?;
    text.push('\n');
    let path = out.join("expected.json");
    std::fs::write(&path, text).with_context(|| format!("failed to write {}", path.display()))?;
    println!("wrote two golden runs and {}", path.display());
    Ok(())
}

fn main() -> ExitCode {
    let Some(out) = std::env::args().nth(1).map(PathBuf::from) else {
        eprintln!("usage: golden-run <output folder>");
        return ExitCode::FAILURE;
    };
    match run(&out) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("golden-run: {error:#}");
            ExitCode::FAILURE
        }
    }
}
