//! The manifest of a run says what the run used and how it ended, so a reader pairs and judges a
//! run by its `run.json`, never by the name of its folder.

use std::path::Path;

use engine::hot::strategy::StrategyParams;
use engine::runtime::artifacts::RunArtifacts;
use engine::runtime::config::{Config, RunIdentity};
use engine::runtime::manifest::{EndState, ManifestOf, RunEnd, RunId, RunManifest};
use engine::runtime::registry::Registry;

use crate::micro_strategy::MicroRecorderParams;
use crate::parquet_readback::TempDir;
use crate::poly_strategy::PolyUpParams;

const BINANCE: &str = "../strategies/strat-micro-recorder/te-binance-spot-btcusdt/config.yaml";
const POLYMARKET: &str =
    "../strategies/strat-micro-recorder/te-polymarket-btc-updown-5m/config.yaml";

fn manifest_of<P: StrategyParams>(config_path: &str, te_id: &str) -> RunManifest {
    let config = Config::<P>::load(Path::new(config_path))
        .unwrap_or_else(|error| panic!("{config_path} loads: {error}"));
    let identity = RunIdentity::new("strat-micro-recorder", te_id).expect("a valid identity");
    let registry = Registry::build(&config).expect("the shipped registry builds");
    RunManifest::of(ManifestOf {
        config: &config,
        identity: &identity,
        artifacts: &RunArtifacts::new(config.execution_mode(), &identity),
        registry: &registry,
        run_id: RunId::from_micros(1_784_410_630_178_000),
        venues: Vec::new(),
        identity_lock: None,
    })
}

/// FITNESS: neither shipped config names `persistence.seal_interval_spins`, so each seals at the
/// spins of 300 seconds of message time at its own spin interval: 300 spins of one second for
/// Binance, 3,000 spins of 100 ms for Polymarket. A default taken from the spins alone would seal
/// the Polymarket tape every 30 seconds.
#[test]
fn shipped_configs_seal_every_300_seconds_of_message_time() {
    let binance = manifest_of::<MicroRecorderParams>(BINANCE, "te-binance-spot-btcusdt");
    let polymarket = manifest_of::<PolyUpParams>(POLYMARKET, "te-polymarket-btc-updown-5m");
    assert_eq!(
        (binance.spin_interval_us, binance.seal_interval_spins),
        (1_000_000, 300),
        "binance seals each 300 spins of one second"
    );
    assert_eq!(
        (polymarket.spin_interval_us, polymarket.seal_interval_spins),
        (100_000, 3_000),
        "polymarket seals each 3,000 spins of 100 ms"
    );
}

/// FITNESS: the manifest is written open at the start, and only its close writes the end. A
/// process that dies, here the drop of the open manifest with no close, leaves `open` on disk, so
/// a reader never takes a cut run for a finished one.
#[test]
fn hard_kill_leaves_manifest_open() {
    let root = TempDir::new("manifest");
    let manifest = manifest_of::<MicroRecorderParams>(BINANCE, "te-binance-spot-btcusdt");
    let run_json = root.path().join("run.json");

    let open = RunManifest::open(root.path(), &manifest).expect("the manifest opens");
    drop(open);
    let killed: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(&run_json).expect("run.json exists"))
            .expect("run.json reads back");
    assert_eq!(killed.end_state, EndState::Open, "a killed run reads open");
    assert_eq!(
        killed, manifest,
        "the open manifest is the manifest as built"
    );

    RunManifest::open(root.path(), &manifest)
        .expect("the manifest opens")
        .close(RunEnd {
            state: EndState::Graceful,
            reason: Some("received SIGTERM".into()),
            stopped_tables: vec!["trades".into()],
        })
        .expect("the manifest closes");
    let closed: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(&run_json).expect("run.json exists"))
            .expect("run.json reads back");
    assert_eq!(
        (
            closed.end_state,
            closed.end_reason.as_deref(),
            closed.stopped_tables
        ),
        (
            EndState::Graceful,
            Some("received SIGTERM"),
            vec!["trades".to_owned()]
        ),
        "the close writes how the run ended"
    );
}
