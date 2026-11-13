//! Each refusal of a start check comes before any thread and before the run nonce advances, so a
//! refused start leaves nothing to stop, spends no nonce, and writes no file.

use std::collections::BTreeMap;
use std::fs;
use std::net::UdpSocket;
use std::path::Path;

use engine::msg::failure::{FailureClass, Halt};
use engine::msg::ids::{Notional, Price, Qty};
use engine::runtime::config::{
    Config, ConfigError, ExecutionMode, NoParams, RunIdentity, SourceSpec,
};
use engine::runtime::registry::{BinanceLimits, Registry};
use engine::runtime::{ExecutionLease, Preflight, StartError, exposure};
use engine::venues::VenueError;
use engine::venues::binance::exec::ProbeError;
use engine::venues::binance::preflight::{ScaleError, SymbolScales};
use engine::venues::edge::rest_quiet::SharedRestQuiet;
use engine::venues::exec::TeTag;
use engine::venues::port::{ProbedVenue, VenuePort, VenueScales};
use engine::venues::sim;

use super::config_guards::{
    BINANCE_KLINES_VOLUME_BARS, BINANCE_PERP_SOURCE, BINANCE_SPOT_SOURCE, SIM_BLOCK,
    config_with_execution_on, config_with_source, execution_block_with, link_block,
};
use crate::engine_support::Idle;
use crate::parquet_readback::TempDir;

const STRATEGY: &str = "strat-preflight";
const ENGINE: &str = "te-refusals";
const ONE: i64 = 100_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Scales {
    Stamped { max_num_orders: u32 },
    Unstamped,
    Refused,
}

/// The venue as a test sees it: no network, no key, no `.env`. A probe always refuses, because
/// only a real probe can hand on credentials.
pub(super) struct StandInVenue {
    pub(super) scales: Scales,
}

impl VenuePort for StandInVenue {
    fn read_scales(&mut self, registry: &Registry) -> Result<VenueScales, VenueError> {
        let max_num_orders = match self.scales {
            Scales::Refused => {
                return Err(VenueError::BinanceScale(ScaleError::OrderLimitsMissing {
                    market: "spot",
                }));
            }
            Scales::Unstamped => return Ok(VenueScales::default()),
            Scales::Stamped { max_num_orders } => max_num_orders,
        };
        let scales = SymbolScales {
            tick_size: Price::from_mantissa(ONE / 100),
            step_size: Qty::from_mantissa(ONE / 100_000),
            limits: BinanceLimits {
                min_qty: Qty::from_mantissa(ONE / 100_000),
                min_notional: Notional::from_mantissa(5 * ONE),
                max_num_orders,
                max_num_order_amends: Some(10),
            },
        };
        Ok(VenueScales {
            binance: registry
                .instruments()
                .iter()
                .map(|row| (row.instrument_id, scales))
                .collect(),
            ..VenueScales::default()
        })
    }

    fn probe(
        &mut self,
        _source: &SourceSpec,
        _registry: &Registry,
        _quiet: &SharedRestQuiet,
    ) -> Result<ProbedVenue, VenueError> {
        Err(VenueError::BinanceGate(ProbeError::TradingDisabled))
    }
}

fn identity() -> RunIdentity {
    RunIdentity::new(STRATEGY, ENGINE).expect("a valid identity")
}

/// A simulated Binance spot engine on one instrument, whose state folder is `state`.
fn sim_document(state: &Path, execution_extra: &str) -> String {
    let execution = execution_block_with("sim", &format!("{execution_extra}{SIM_BLOCK}"));
    with_state(
        config_with_execution_on(BINANCE_SPOT_SOURCE, &execution),
        state,
    )
}

fn with_state(document: String, state: &Path) -> String {
    format!("{document}exposure:\n  dir: \"{}\"\n", state.display())
}

/// Each file of the state folder with its bytes.
pub(super) fn snapshot(state: &Path) -> BTreeMap<String, Vec<u8>> {
    let Ok(entries) = fs::read_dir(state) else {
        return BTreeMap::new();
    };
    entries
        .map(|entry| entry.expect("a readable folder entry").path())
        .filter(|path| path.is_file())
        .map(|path| {
            let name = path
                .file_name()
                .expect("a file name")
                .to_string_lossy()
                .into_owned();
            (name, fs::read(&path).expect("a readable file"))
        })
        .collect()
}

/// Runs preflight on `document` and holds what each refusal must leave: no new file and no changed
/// byte in the state folder, so the run nonce is unspent, and the class of a refused start.
fn refusal(document: &str, scales: Scales, state: &Path) -> StartError {
    let config = Config::<NoParams>::from_yaml(document).expect("the document passes config");
    let before = snapshot(state);
    let refused = Preflight {
        identity: &identity(),
        config: &config,
        strategy: &Idle,
    }
    .run_with(&mut StandInVenue { scales });
    let Err(refused) = refused else {
        panic!("this start must be refused");
    };
    assert_eq!(
        snapshot(state),
        before,
        "a refused start writes no file and spends no nonce: {refused}"
    );
    assert_eq!(refused.failure_class(), FailureClass::Halt(Halt::Start));
    refused
}

fn the_nonce_file(state: &Path) -> std::path::PathBuf {
    let mut nonces = fs::read_dir(state)
        .expect("the state folder exists")
        .map(|entry| entry.expect("a readable entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "nonce")
        });
    let nonce = nonces.next().expect("the lease wrote its nonce file");
    assert!(
        nonces.next().is_none(),
        "one engine identity has one nonce file"
    );
    nonce
}

/// CONTRACT: each refusal of table (e) that a test can reach without a key comes before any thread
/// and before the lease, and the held lease, the last check, leaves the nonce file as it was. The
/// thread half of the census is the scan `preflight_names_no_thread_but_its_one_runtime`.
#[test]
fn each_refusal_starts_no_thread_and_spends_no_nonce() {
    let root = TempDir::new("preflight-refusals");
    let state = root.path().join("state");
    let stamped = Scales::Stamped {
        max_num_orders: 200,
    };
    let held = ExecutionLease::acquire(&state, TeTag::of(&identity()), &sim::lease_namespace())
        .expect("the test takes the lease of this engine identity");
    let nonce_before = fs::read(the_nonce_file(&state)).expect("the nonce file reads");

    // The trace: config, registry, core id, scale read, order capacity and exposure pass, and the
    // lease, the last check, is refused.
    let lease = refusal(&sim_document(&state, ""), stamped, &state);
    let StartError::ExecutionIdentityInUse { path } = &lease else {
        panic!("a held lease refuses with its own variant, got {lease}");
    };
    assert!(
        lease.to_string().contains(&path.display().to_string()),
        "the refusal names the lock that holds the engine: {lease}"
    );
    assert_eq!(
        fs::read(the_nonce_file(&state)).expect("reads"),
        nonce_before
    );
    drop(held);

    // E3: a hot core this host does not have.
    let core = refusal(
        &sim_document(&state, "").replace("hot_core_id: 0", "hot_core_id: 4096"),
        stamped,
        &state,
    );
    assert!(
        matches!(core, StartError::CoreId { core_id: 4096, .. }),
        "{core}"
    );

    // E5: the scale read refuses.
    let scale = refusal(&sim_document(&state, ""), Scales::Refused, &state);
    assert!(
        matches!(
            scale,
            StartError::Venue(VenueError::BinanceScale(
                ScaleError::OrderLimitsMissing { .. }
            ))
        ),
        "{scale}"
    );

    // E6: the ladder of the config overruns the venue's order count.
    let capacity = refusal(
        &sim_document(&state, ""),
        Scales::Stamped { max_num_orders: 1 },
        &state,
    );
    assert!(
        matches!(
            capacity,
            StartError::ExecutionOrderCapacity { venue_max: 1, .. }
        ),
        "{capacity}"
    );

    // E7: the position file cannot be read.
    let ledger = exposure::file_path(&state, &identity(), Some(ExecutionMode::Sim));
    fs::write(&ledger, "not a ledger").expect("the test writes the position file");
    let unreadable = refusal(&sim_document(&state, ""), stamped, &state);
    assert!(
        matches!(unreadable, StartError::Exposure(_)),
        "{unreadable}"
    );
    fs::remove_file(&ledger).expect("the test removes the position file");

    // E8: the permission probe of a live run refuses.
    let live = with_state(
        config_with_execution_on(BINANCE_SPOT_SOURCE, &execution_block_with("live", "")),
        &state,
    );
    let probe = refusal(&live, stamped, &state);
    assert!(
        matches!(
            probe,
            StartError::Venue(VenueError::BinanceGate(ProbeError::TradingDisabled))
        ),
        "{probe}"
    );

    // E9: the link address is taken.
    let taken = UdpSocket::bind("127.0.0.1:0").expect("the test binds a loopback port");
    let address = taken.local_addr().expect("a bound address").to_string();
    let linked = sim_document(&state, "").replace(
        "logging:\n",
        &format!("{}logging:\n", link_block(&address, "")),
    );
    let bind = refusal(&linked, stamped, &state);
    assert!(matches!(bind, StartError::LinkBind { .. }), "{bind}");

    // E14: a setting that this venue never reads.
    let inert = refusal(
        &sim_document(&state, "  quote_stop_margin_ms: 9000\n"),
        stamped,
        &state,
    );
    assert!(
        matches!(
            inert,
            StartError::ExecutionFieldInert {
                field: "quote_stop_margin_ms",
                ..
            }
        ),
        "{inert}"
    );

    // E16: the simulated venue finds no grid to match on.
    let unstamped = refusal(&sim_document(&state, ""), Scales::Unstamped, &state);
    assert!(
        matches!(unstamped, StartError::Venue(VenueError::Simulated(_))),
        "{unstamped}"
    );

    // E18: the nonce file holds what no run wrote, then the last nonce there is.
    let nonce = the_nonce_file(&state);
    fs::write(&nonce, "garbage\n").expect("the test writes the nonce file");
    let unreadable_nonce = refusal(&sim_document(&state, ""), stamped, &state);
    assert!(
        matches!(unreadable_nonce, StartError::ExecutionIdentityState { .. }),
        "{unreadable_nonce}"
    );
    fs::write(&nonce, format!("{}\n", u32::MAX)).expect("the test writes the nonce file");
    let exhausted = refusal(&sim_document(&state, ""), stamped, &state);
    assert!(
        matches!(exhausted, StartError::ExecutionIdentityExhausted { .. }),
        "{exhausted}"
    );

    // E1 and E2: the nine documents that config or the registry refuses, by the field named.
    for (case, document, is_expected) in refused_documents() {
        let error = match Config::<NoParams>::from_yaml(&document) {
            Err(error) => error,
            Ok(_) => match refusal(&document, stamped, &state) {
                StartError::Config(error) => error,
                other => panic!("{case}: the registry refuses this document, got {other}"),
            },
        };
        assert!(is_expected(&error), "{case}: wrong refusal {error}");
    }

    // E1: the config refusal of part 1, an engine that spins with no hot core. Only Linux refuses.
    let spinning = config_with_source(BINANCE_PERP_SOURCE).replace("  hot_core_id: 0\n", "");
    let spun = Config::<NoParams>::from_yaml(&spinning);
    if cfg!(target_os = "linux") {
        assert!(matches!(spun, Err(ConfigError::SpinWithoutHotCore)));
    } else {
        assert!(
            spun.is_ok(),
            "only linux requires a hot core for a spinning engine"
        );
    }
}

type RefusedDocument = (&'static str, String, fn(&ConfigError) -> bool);

/// The nine documents of the old config guard, each with the field that must name its refusal.
fn refused_documents() -> [RefusedDocument; 9] {
    let missing_spin = "engine:
  hot_core_id: 0
queues:
  input_capacity: 65536
  persistence_capacity: 65536
source:
"
    .to_owned()
        + BINANCE_PERP_SOURCE
        + "strategy:\n  instruments: all\npersistence:\n  dir: ./data\nlogging:\n  dir: ./logs\n";
    [
        (
            "omitted spin_interval_us",
            missing_spin,
            |error: &ConfigError| matches!(error, ConfigError::Parse { detail } if detail.contains("spin_interval_us")),
        ),
        (
            "spin_interval_us past the 60s ceiling",
            config_with_source(BINANCE_PERP_SOURCE)
                .replace("spin_interval_us: 100000", "spin_interval_us: 90000000"),
            |error: &ConfigError| {
                matches!(error, ConfigError::Invalid { field, value, .. }
                    if *field == "engine.spin_interval_us" && value.as_ref() == "90000000")
            },
        ),
        (
            "omitted max_exposure_quote",
            config_with_source(
                "  exchange: binance
  market: perpetual
  base: BTC
  quote: USDT
  tracker: {}
",
            ),
            |error: &ConfigError| {
                matches!(error, ConfigError::Parse { detail }
                    if detail.contains("max_exposure_quote"))
            },
        ),
        (
            "poly subscriptions with trades off",
            config_with_source(
                "  exchange: polymarket
  max_exposure_quote: 500
  series: btc-updown-5m
  subscriptions:
    trades: false
  tracker: {}
",
            ),
            |error: &ConfigError| {
                matches!(error, ConfigError::Invalid { field, value, .. }
                    if *field == "source.subscriptions" && value.as_ref() == "trades: false")
            },
        ),
        (
            "poly subscriptions with both book flags off",
            config_with_source(
                "  exchange: polymarket
  max_exposure_quote: 500
  series: btc-updown-5m
  subscriptions:
    book_updates: false
    book_snapshots: false
  tracker: {}
",
            ),
            |error: &ConfigError| {
                matches!(error, ConfigError::Invalid { field, value, .. }
                    if *field == "source.subscriptions"
                        && value.as_ref() == "book_updates: false, book_snapshots: false")
            },
        ),
        (
            "klines volume bars over too short a candle retention",
            config_with_source(&BINANCE_KLINES_VOLUME_BARS.replace("keep: 1440 }", "keep: 720 }")),
            |error: &ConfigError| {
                matches!(error, ConfigError::Invalid { field, value, .. }
                    if *field == "source.tracker.candles.keep" && value.as_ref() == "720")
            },
        ),
        (
            "klines volume bars on a venue with no candles",
            config_with_source(
                "  exchange: polymarket
  max_exposure_quote: 500
  series: btc-updown-5m
  tracker:
    volume_bars:
      threshold: klines
      keep: 64
",
            ),
            |error: &ConfigError| {
                matches!(error, ConfigError::Invalid { field, value, .. }
                    if *field == "source.tracker.volume_bars.threshold"
                        && value.as_ref() == "klines")
            },
        ),
        (
            "retention_hours of zero",
            config_with_source(BINANCE_PERP_SOURCE)
                .replace("  dir: ./data\n", "  dir: ./data\n  retention_hours: 0\n"),
            |error: &ConfigError| {
                matches!(error, ConfigError::Invalid { field, value, .. }
                    if *field == "persistence.retention_hours" && value.as_ref() == "0")
            },
        ),
        (
            "retention_hours past ten years",
            config_with_source(BINANCE_PERP_SOURCE).replace(
                "  dir: ./data\n",
                "  dir: ./data\n  retention_hours: 87601\n",
            ),
            |error: &ConfigError| {
                matches!(error, ConfigError::Invalid { field, value, .. }
                    if *field == "persistence.retention_hours" && value.as_ref() == "87601")
            },
        ),
    ]
}
