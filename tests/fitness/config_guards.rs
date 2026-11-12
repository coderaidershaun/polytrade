//! Config/registry venue-generalisation shape: one trading engine carries exactly one source, and
//! that source's fan-out to instruments, producer groups and input queues is asserted through the
//! real `from_yaml`/`build` path — one binance source is three connections onto one instrument, one
//! polymarket source is one connection onto the four fixed A/B-by-up/down slots. The shipped
//! reference config is asserted here too, since `deny_unknown_fields` makes it brittle to any
//! schema change.

use std::path::Path;

use polysim::config::{
    BinanceMarket, ConfigError, ExecutionMode, NoParams, PolySeries, TableKind, VenueMarket,
    VolumeThreshold,
};
use polysim::hot::strategy::{EngineView, StrategyConfig};
use polysim::ids::FIXED_SCALE;
use polysim::registry::{ConnectionCategory, Registry};
use polysim::time::DurationUs;

use crate::micro_strategy::{MicroRecorder, MicroRecorderParams};
use crate::poly_strategy::{PolyUpParams, PolyUpPublisher};

fn build_from(yaml: &str) -> Registry {
    let config: polysim::config::Config =
        polysim::config::Config::from_yaml(yaml).expect("document parses and validates");
    Registry::build(&config).expect("registry builds")
}

const POLY_SOURCE: &str = "  exchange: polymarket
  max_exposure_quote: 500
  series: btc-updown-5m
  tracker: {}
";

const POLY_SLOT_SYMBOLS: &[&str] = &[
    "btc-updown-5m-a-up",
    "btc-updown-5m-a-down",
    "btc-updown-5m-b-up",
    "btc-updown-5m-b-down",
];

fn config_with_source(source_block: &str) -> String {
    config_with_strategy(source_block, "  instruments: all\n")
}

const DEFAULT_PERSISTENCE: &str = "persistence:\n  dir: ./data\n";

fn config_with_strategy(source_block: &str, strategy_block: &str) -> String {
    with_link(source_block, strategy_block, DEFAULT_PERSISTENCE, "")
}

fn config_without_persistence(source_block: &str, strategy_block: &str) -> String {
    with_link(source_block, strategy_block, "", "")
}

fn with_link(
    source_block: &str,
    strategy_block: &str,
    persistence_block: &str,
    link_block: &str,
) -> String {
    format!(
        "engine:
  hot_core_id: 0
  spin_interval_us: 100000
queues:
  input_capacity: 65536
  persistence_capacity: 65536
source:
{source_block}strategy:
{strategy_block}{persistence_block}{link_block}logging:
  dir: ./logs
"
    )
}

fn link_block(bind: &str, extra: &str) -> String {
    format!("link:\n  bind: \"{bind}\"\n{extra}")
}

fn config_with_link(link_block: &str) -> String {
    with_link(
        BINANCE_PERP_SOURCE,
        "  instruments: all\n",
        DEFAULT_PERSISTENCE,
        link_block,
    )
}

const BINANCE_PERP_SOURCE: &str = "  exchange: binance
  max_exposure_quote: 500
  market: perpetual
  base: BTC
  quote: USDT
  tracker: {}
";

/// One polymarket source expands to the four fixed A/B x up/down slots, each a distinct venue symbol
/// carrying a dense id in config order, none of them subscribing to klines, all on one queue. "One
/// source" is not "one connection": a single binance source is ONE instrument served by three
/// per-category connections (trades/depth/klines), so it is three producer groups plus the timer.
/// The exposure ceiling reaches every row the block expands to as an exact quote mantissa — the four
/// poly slots are budgeted separately, not as one pot.
#[test]
fn a_source_fans_out_to_its_rows_connections_and_queues() {
    struct FanOut {
        case: &'static str,
        source: &'static str,
        venue_symbols: &'static [&'static str],
        market: VenueMarket,
        has_klines: bool,
        categories: &'static [ConnectionCategory],
        instruments_per_group: &'static [usize],
        input_queues: usize,
    }

    let cases = [
        FanOut {
            case: "polymarket",
            source: POLY_SOURCE,
            venue_symbols: POLY_SLOT_SYMBOLS,
            market: VenueMarket::Polymarket(PolySeries::BtcUpDown5m),
            has_klines: false,
            categories: &[ConnectionCategory::Market],
            instruments_per_group: &[4],
            input_queues: 2,
        },
        FanOut {
            case: "binance perpetual",
            source: BINANCE_PERP_SOURCE,
            venue_symbols: &["btcusdt"],
            market: VenueMarket::Binance(BinanceMarket::Perpetual),
            has_klines: true,
            categories: &[
                ConnectionCategory::Trades,
                ConnectionCategory::Depth,
                ConnectionCategory::Klines,
            ],
            instruments_per_group: &[1, 1, 1],
            input_queues: 4,
        },
    ];

    for expected in cases {
        let case = expected.case;
        let registry = build_from(&config_with_source(expected.source));
        let rows = registry.instruments();
        let symbols: Vec<&str> = rows.iter().map(|row| row.venue_symbol.as_ref()).collect();
        assert_eq!(
            symbols, expected.venue_symbols,
            "{case}: distinct venue symbols in config order"
        );
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(
                usize::from(row.instrument_id.0),
                index,
                "{case}: dense id in config order"
            );
            assert_eq!(row.market, expected.market, "{case}: the source's market");
            assert_eq!(
                row.subscriptions.klines, expected.has_klines,
                "{case}: klines subscription"
            );
            assert_eq!(
                row.kline_intervals.is_empty(),
                !expected.has_klines,
                "{case}: kline intervals"
            );
            assert_eq!(
                row.max_exposure_quote,
                500 * FIXED_SCALE,
                "{case}: every row the block expands to carries its 500 quote units, exactly"
            );
        }

        let groups = registry.producer_groups();
        let categories: Vec<ConnectionCategory> =
            groups.iter().map(|group| group.category).collect();
        assert_eq!(
            categories, expected.categories,
            "{case}: one connection per category"
        );
        let sizes: Vec<usize> = groups.iter().map(|group| group.instruments.len()).collect();
        assert_eq!(
            sizes, expected.instruments_per_group,
            "{case}: instruments served by each connection"
        );
        assert_eq!(
            registry.input_queue_count(),
            expected.input_queues,
            "{case}: producer groups + the timer"
        );
    }
}

/// A trading engine that only computes signals and ships them over the link should pay for no writer
/// thread, no ring and no run directory, so the whole `persistence:` block is optional. And because
/// `strategy.tables` is the lib's authority over which tables exist at all, naming tables with no
/// block to write them into is a contradiction: it must fail loud rather than record nothing.
#[test]
fn persistence_is_optional_and_naming_tables_without_it_is_rejected() {
    let headless = config_without_persistence(BINANCE_PERP_SOURCE, "  instruments: all\n");
    let config: polysim::config::Config = polysim::config::Config::from_yaml(&headless)
        .expect("an omitted persistence block is legal");
    assert!(
        config.persistence.is_none(),
        "an omitted block means no persistence at all, not a defaulted directory"
    );
    assert!(
        config.strategy.tables.is_empty(),
        "no persistence, no tables"
    );

    let contradiction =
        config_without_persistence(BINANCE_PERP_SOURCE, "  tables: [features, trades]\n");
    let error = polysim::config::Config::<NoParams>::from_yaml(&contradiction)
        .expect_err("tables with nowhere to land must reject");
    assert!(
        matches!(&error, ConfigError::TablesWithoutPersistence { tables }
            if tables.as_ref() == "features, trades"),
        "error names every table that has nowhere to land, got: {error}"
    );

    let with_block: polysim::config::Config =
        polysim::config::Config::from_yaml(&config_with_source(BINANCE_PERP_SOURCE))
            .expect("a persistence block still parses");
    let persistence = with_block
        .persistence
        .as_ref()
        .expect("the block is present");
    assert_eq!(persistence.dir, Path::new("./data"));
    assert_eq!(
        persistence.retention_hours, 6,
        "an omitted retention_hours keeps six hours of research data"
    );
}

/// Every document the config layer refuses, and the field each refusal has to name — an operator
/// reads the field they must edit. A defaulted spin interval would silently disagree with whatever
/// the strategy assumed, and an omitted exposure ceiling would adopt a risk budget nobody chose.
/// Polymarket serves trades and the book on one combined channel, so a partial subscriptions block
/// is a promise the venue arm can't honour — `trades: false` would still record trades. A klines
/// volume-bar target promises a trailing 1m average, which holds only if the source keeps 1440
/// closed 1m candles, and only on a venue that has candles at all.
#[test]
fn every_refused_document_names_the_field_that_refused_it() {
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

    type RejectionCase = (&'static str, String, fn(&ConfigError) -> bool);
    let cases: [RejectionCase; 9] = [
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
    ];

    for (case, yaml, is_expected_refusal) in cases {
        let error = refusal(&yaml);
        assert!(
            is_expected_refusal(&error),
            "{case}: the refusal must name the field, got: {error}"
        );
    }
}

/// A Linux engine with no hot core named would run its hot thread unpinned without a word, so the
/// config refuses it there. macOS cannot pin a thread at all, so the same document loads.
#[test]
fn the_hot_core_is_required_on_linux_and_ignored_elsewhere() {
    let unpinned = config_with_source(BINANCE_PERP_SOURCE).replace("  hot_core_id: 0\n", "");
    assert!(
        !unpinned.contains("hot_core_id"),
        "the document under test must name no hot core"
    );
    let loaded = polysim::config::Config::<NoParams>::from_yaml(&unpinned);
    if cfg!(target_os = "linux") {
        assert!(
            matches!(loaded, Err(ConfigError::MissingHotCoreId)),
            "linux must refuse a document with no hot core, got {loaded:?}"
        );
    } else {
        let config = loaded.expect("a document with no hot core loads where nothing is pinned");
        Registry::build(&config).expect("and its registry builds");
    }
}

const BINANCE_KLINES_VOLUME_BARS: &str = "  exchange: binance
  max_exposure_quote: 500
  market: perpetual
  base: BTC
  quote: USDT
  tracker:
    candles: { keep: 1440 }
    volume_bars:
      threshold: klines
      keep: 1440
";

const BINANCE_FIXED_VOLUME_BARS: &str = "  exchange: binance
  max_exposure_quote: 500
  market: perpetual
  base: BTC
  quote: USDT
  tracker:
    volume_bars:
      threshold: 250000
      keep: 512
      sampled: { fields: [best_bid], window: 256 }
";

/// A volume-bar target is written either as the word `klines` or as a whole-dollar integer, so the
/// hand-written visitor must accept both.
#[test]
fn volume_bar_thresholds_parse_in_both_forms() {
    for (case, source, threshold, sampled_window) in [
        (
            "klines",
            BINANCE_KLINES_VOLUME_BARS,
            VolumeThreshold::Klines,
            None,
        ),
        (
            "whole dollars",
            BINANCE_FIXED_VOLUME_BARS,
            VolumeThreshold::Fixed(250_000),
            Some(256),
        ),
    ] {
        let registry = build_from(&config_with_source(source));
        let spec = registry.instruments()[0]
            .tracker
            .volume_bars
            .as_ref()
            .expect("volume_bars parsed");
        assert_eq!(spec.threshold, threshold, "{case}: threshold form");
        assert_eq!(
            spec.sampled.as_ref().map(|sampled| sampled.window),
            sampled_window,
            "{case}: an absent sampled block is legal"
        );
    }
}

/// Generic over the strategy's `params:` type, so a shipped config is read through the same typed
/// pass its own binary uses — parsing it as [`NoParams`] would skip its knobs entirely.
fn load<P: serde::de::DeserializeOwned + Default>(path: &str) -> polysim::config::Config<P> {
    polysim::config::Config::load(Path::new(path))
        .unwrap_or_else(|error| panic!("{path} must load: {error}"))
}

/// The config the binary header tells a reader to run must parse, validate and build a registry,
/// and carry the tables its strategy actually writes — a config naming no table its strategy emits
/// records nothing at all. Its polymarket half ships disarmed twice over, collects nothing beyond
/// the rotations lineage its peer's data cannot do without, stops quoting before the engine's own
/// window gate would refuse it, and binds a port its peer does not. Both engines ship configured for
/// one box, where a collision is a startup failure an operator has to diagnose rather than a test
/// catching it.
#[test]
fn the_shipped_configs_run_their_strategies_and_ship_disarmed() {
    let micro = load::<MicroRecorderParams>(
        "strategies/strat-micro-recorder/te-binance-spot-btcusdt/config.yaml",
    );
    assert_eq!(
        micro.strategy.tables,
        vec![
            TableKind::Features,
            TableKind::LinkFrames,
            TableKind::Orders,
            TableKind::Fills
        ],
        "features, the consumed link tape, and the audit trail — a live run that omits orders and \
         fills keeps no record of what it did with real money, and nothing downstream can tell that \
         apart from a run that did nothing"
    );
    let micro_execution = micro
        .execution
        .as_ref()
        .expect("the shipped config carries an execution block");
    assert_eq!(
        micro_execution.mode,
        ExecutionMode::Sim,
        "the reference config ships paper-trading against the simulator; real money needs an \
         explicit live"
    );
    let micro_link = micro
        .link
        .as_ref()
        .expect("the shipped config binds a link");
    let micro_peer = match micro_link.subscribe.as_slice() {
        [only] => only,
        other => panic!("this recorder consumes exactly its polymarket peer, got {other:?}"),
    };
    assert_eq!(
        micro_peer
            .topics
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&str>>(),
        vec!["poly_up"],
        "naming the topic rather than omitting it — an absent list asks for the peer's whole feed"
    );
    assert_eq!(
        Registry::build(&micro)
            .expect("micro recorder registry builds")
            .instruments()
            .len(),
        1,
        "one source, one binance spot instrument"
    );
    // Construction is the strategy's own validation pass — a shipped params value its asserts
    // reject (a non-positive Guéant risk budget, say) must fail here, not at first launch.
    let micro_engine = EngineView {
        spin_interval: DurationUs::from_micros(micro.engine.spin_interval_us as i64),
    };
    let _ = MicroRecorder::from_spec(&micro.strategy, micro_engine);

    let publisher = load::<PolyUpParams>(
        "strategies/strat-micro-recorder/te-polymarket-btc-updown-5m/config.yaml",
    );
    assert!(
        publisher.strategy.tables.is_empty(),
        "collection is the binance TE's job; arming execution must restore [orders, fills] — \
         an engine that can place real orders records what it did with them"
    );
    assert!(
        publisher.persistence.is_some(),
        "the rotations lineage still needs the writer thread: it is the only map from the \
         role-keyed poly_* features back to a condition id and window"
    );
    let publisher_execution = publisher
        .execution
        .as_ref()
        .expect("the shipped config carries an execution block");
    assert_eq!(
        publisher_execution.mode,
        ExecutionMode::Off,
        "polymarket ships disarmed; live is the operator's decision and there is no sim venue"
    );
    assert!(
        !publisher.strategy.params.enabled,
        "the strategy's own switch ships off too — arming execution for any other reason must not \
         start a market maker by omission"
    );
    // The engine refuses a quote inside `quote_stop_margin_ms` of the close and sweeps what is
    // resting. A strategy whose own stop landed at or after that margin would declare a quote every
    // spin for the engine to refuse, and the refusal stream would bury anything else being said.
    assert!(
        u64::from(publisher.strategy.params.quote_stop_lead_ms)
            > publisher_execution.quote_stop_margin_ms,
        "the strategy stops quoting {}ms before the close, inside the engine's own {}ms margin",
        publisher.strategy.params.quote_stop_lead_ms,
        publisher_execution.quote_stop_margin_ms
    );
    let publisher_link = publisher
        .link
        .as_ref()
        .expect("the publisher must bind a link, it has no other output");
    assert!(
        publisher_link.subscribe.is_empty(),
        "the publisher serves and consumes nothing"
    );
    let registry = Registry::build(&publisher).expect("publisher registry builds");
    assert_eq!(
        registry.instruments().len(),
        4,
        "one polymarket source, four A/B x up/down slots"
    );
    // Eight of the twenty published slots are (A, k) fits, and without a reach histogram to fit they
    // are NaN for the life of the run while everything else keeps working — a deleted block would
    // read downstream as a series that never trades, not as a misconfiguration.
    assert!(
        registry
            .instruments()
            .iter()
            .all(|row| row.tracker.intensity.is_some()),
        "the publisher cannot fit trade intensity without the tracker's reach histogram"
    );
    let publisher_engine = EngineView {
        spin_interval: DurationUs::from_micros(publisher.engine.spin_interval_us as i64),
    };
    let _ = PolyUpPublisher::from_spec(&publisher.strategy, publisher_engine);
    // The one arithmetic relation between these numbers that nothing else checks. An outcome share
    // cannot be worth more than a dollar, so `order_shares` IS the worst-case notional — set it
    // above the single-order ceiling and every place is refused as oversized, on a config that
    // otherwise validates.
    assert!(
        publisher.strategy.params.order_shares <= publisher_execution.max_order_notional_quote,
        "{} shares can cost up to ${} at settlement, past the ${} single-order ceiling",
        publisher.strategy.params.order_shares,
        publisher.strategy.params.order_shares,
        publisher_execution.max_order_notional_quote
    );

    assert_ne!(
        publisher_link.bind, micro_link.bind,
        "two trading engines of one strategy ship runnable side by side on one box"
    );
    assert_eq!(
        micro_peer.address, publisher_link.bind,
        "the consumer's peer address must be the address the publisher actually binds"
    );
}

/// A strategy's typed `params:` block deserializes in the same pass with `deny_unknown_fields`
/// intact: a good block parses to the typed value, an unknown key inside `params` fails naming the
/// field, and an omitted block yields the strategy's default. This is the seam that lets a strategy
/// carry its own knobs without the lib knowing the shape.
#[test]
fn strategy_params_parse_typed_and_reject_unknown_keys() {
    #[derive(serde::Deserialize, Debug, Clone, Default)]
    #[serde(deny_unknown_fields)]
    struct FakeParams {
        threshold: f64,
    }

    let with_params = config_with_strategy(BINANCE_PERP_SOURCE, "  params:\n    threshold: 1.5\n");
    let config = polysim::config::Config::<FakeParams>::from_yaml(&with_params)
        .expect("typed params parse in the single pass");
    assert_eq!(config.strategy.params.threshold, 1.5);

    let unknown_key = config_with_strategy(BINANCE_PERP_SOURCE, "  params:\n    threshld: 1.5\n");
    let error = polysim::config::Config::<FakeParams>::from_yaml(&unknown_key)
        .expect_err("an unknown key inside params must reject");
    assert!(
        matches!(&error, ConfigError::Parse { detail } if detail.contains("threshld")),
        "error names the unknown params field, got: {error}"
    );

    let omitted = config_with_strategy(BINANCE_PERP_SOURCE, "  instruments: all\n");
    let config = polysim::config::Config::<FakeParams>::from_yaml(&omitted)
        .expect("an omitted params block is legal");
    assert_eq!(
        config.strategy.params.threshold, 0.0,
        "omitted params yields the strategy default"
    );
}

/// The link has no authentication, so the bind address IS the security boundary. A bind reachable
/// from outside a private network must fail the run by name — anyone who can reach that port can
/// stop the engine and inject signals into the strategy — and `allow_public_bind` must be the only
/// way past it, so the exposure is always somebody's stated decision.
#[test]
fn a_public_link_bind_is_a_startup_error_unless_allowed() {
    let private = [
        "127.0.0.1:9310",
        "10.1.2.3:9310",
        "172.16.0.1:9310",
        "192.168.1.4:9310",
        "100.64.0.3:9310",
        "[::1]:9310",
        "[fd00::1]:9310",
    ];
    for bind in private {
        let yaml = config_with_link(&link_block(bind, ""));
        polysim::config::Config::<NoParams>::from_yaml(&yaml)
            .unwrap_or_else(|error| panic!("{bind} is a private bind: {error}"));
    }

    let public = [
        "0.0.0.0:9310",
        "8.8.8.8:9310",
        "172.32.0.1:9310",
        "100.128.0.1:9310",
        "[::]:9310",
        "[2001:db8::1]:9310",
    ];
    for bind in public {
        let yaml = config_with_link(&link_block(bind, ""));
        let error = polysim::config::Config::<NoParams>::from_yaml(&yaml)
            .err()
            .unwrap_or_else(|| panic!("{bind} must be refused"));
        assert!(
            matches!(error, ConfigError::PublicLinkBind { .. }),
            "{bind} refused for the right reason, got {error}"
        );
        let allowed = config_with_link(&link_block(bind, "  allow_public_bind: true\n"));
        polysim::config::Config::<NoParams>::from_yaml(&allowed)
            .unwrap_or_else(|error| panic!("{bind} allowed explicitly: {error}"));
    }
}

/// The link's input queue exists only when its actor does, and it lands after the timer — a queue id
/// is a position in the ingress list, and `wire` zips producers back against exactly that order.
#[test]
fn the_link_adds_one_input_queue_only_when_configured() {
    let without = build_from(&config_with_source(BINANCE_PERP_SOURCE));
    assert_eq!(
        without.link_queue_id(),
        None,
        "no link block, no link queue"
    );
    assert_eq!(
        without.input_queue_count(),
        4,
        "three producers + the timer"
    );

    let with = build_from(&config_with_link(&link_block("127.0.0.1:9310", "")));
    assert_eq!(
        with.timer_queue_id().0,
        3,
        "the timer still follows the three producer groups"
    );
    assert_eq!(
        with.link_queue_id().map(|id| id.0),
        Some(4),
        "the link takes the queue after the timer"
    );
    assert_eq!(
        with.input_queue_count(),
        5,
        "three producers + the timer + the link"
    );
}

/// Every field the block has no default for, as pairs so one test can vary a single value without
/// restating the rest.
const EXECUTION_REQUIRED: [(&str, &str); 6] = [
    ("min_base_balance", "0.001"),
    ("min_quote_balance", "10.0"),
    ("max_order_notional_quote", "25.0"),
    ("max_quote_distance_bps", "50.0"),
    ("max_book_age_ms", "2000"),
    ("max_session_loss_quote", "5.0"),
];

fn config_with_execution(block: &str) -> String {
    config_with_execution_on(BINANCE_PERP_SOURCE, block)
}

fn config_with_execution_on(source_block: &str, block: &str) -> String {
    with_link(
        source_block,
        "  instruments: all\n",
        DEFAULT_PERSISTENCE,
        block,
    )
}

const SIM_BLOCK: &str = "  sim:
    order_entry_latency_ms: 15
    ack_latency_ms: 15
    max_market_data_delay_ms: 1000
    heartbeat_ms: 100
    opening_base_balance: 0.01
    opening_quote_balance: 1000.0
    maker_fee_bps: 10.0
";

const BINANCE_SPOT_SOURCE: &str = "  exchange: binance
  max_exposure_quote: 500
  market: spot
  base: BTC
  quote: USDT
  tracker: {}
";

fn execution_block(mode: &str) -> String {
    execution_block_overriding(mode, "", "")
}

fn execution_block_with(mode: &str, extra: &str) -> String {
    format!("{}{extra}", execution_block(mode))
}

/// The same block with one field's value replaced, so a rejection test states only what it changed.
fn execution_block_overriding(mode: &str, field: &str, value: &str) -> String {
    let mut block = format!("execution:\n  mode: {mode}\n");
    for (name, default) in EXECUTION_REQUIRED {
        let stated = if name == field { value } else { default };
        block.push_str(&format!("  {name}: {stated}\n"));
    }
    block
}

/// The `execution:` block, and the things about it that are easy to get silently wrong: which
/// spellings of `mode` exist, that a disarmed block is still fully validated, and that the queue it
/// costs appears only when an actor will exist to feed it. A mode a venue has no edge for would come
/// up reporting itself ARMED while placing nothing — the one way "configured to trade" and "trading"
/// can disagree without anyone noticing — so the capability table is enforced at parse rather than
/// warned about at wiring, and the queue follows it.
#[test]
fn every_execution_mode_parses_and_only_the_armed_ones_cost_a_queue() {
    let modes = [
        (
            "binance off",
            BINANCE_PERP_SOURCE,
            "off",
            "",
            ExecutionMode::Off,
            false,
            4,
        ),
        (
            "binance live",
            BINANCE_PERP_SOURCE,
            "live",
            "",
            ExecutionMode::Live,
            true,
            5,
        ),
        (
            "binance sim",
            BINANCE_SPOT_SOURCE,
            "sim",
            SIM_BLOCK,
            ExecutionMode::Sim,
            true,
            5,
        ),
        (
            "polymarket off",
            POLY_SOURCE,
            "off",
            "",
            ExecutionMode::Off,
            false,
            2,
        ),
        (
            "polymarket live",
            POLY_SOURCE,
            "live",
            "",
            ExecutionMode::Live,
            true,
            3,
        ),
    ];
    for (case, source, spelling, extra, expected, expects_queue, input_queues) in modes {
        let yaml = config_with_execution_on(source, &execution_block_with(spelling, extra));
        let config: polysim::config::Config =
            polysim::config::Config::from_yaml(&yaml).expect("the block parses and validates");
        assert_eq!(
            config.execution.as_ref().map(|block| block.mode),
            Some(expected),
            "{case}: {spelling} is the spelling an operator types"
        );
        let registry = Registry::build(&config).expect("registry builds");
        assert_eq!(
            registry.exec_queue_id().is_some(),
            expects_queue,
            "{case}: the exec input queue exists exactly when an edge will feed it"
        );
        assert_eq!(
            registry.input_queue_count(),
            input_queues,
            "{case}: producer groups + the timer, plus execution when it is armed"
        );
    }
}

/// A mode flip is a one-line edit, by an operator or an agent. If a sim block made a config fail
/// to boot under any other mode, switching away from sim would mean deleting the block too, and
/// switching back would mean retyping every venue assumption in it.
#[test]
fn a_sim_block_is_inert_outside_sim_mode() {
    for (spelling, expected) in [("off", ExecutionMode::Off), ("live", ExecutionMode::Live)] {
        let yaml = config_with_execution_on(
            BINANCE_SPOT_SOURCE,
            &execution_block_with(spelling, SIM_BLOCK),
        );
        let config = polysim::config::Config::<NoParams>::from_yaml(&yaml)
            .unwrap_or_else(|error| panic!("a sim block under {spelling} must load: {error}"));
        assert_eq!(
            config.execution.as_ref().map(|block| block.mode),
            Some(expected),
            "the block is carried, but the mode is still the one the operator typed"
        );
    }

    let bad_fee = SIM_BLOCK.replace("maker_fee_bps: 10.0", "maker_fee_bps: 10000.5");
    let latent =
        config_with_execution_on(BINANCE_SPOT_SOURCE, &execution_block_with("live", &bad_fee));
    assert!(
        matches!(
            refusal(&latent),
            ConfigError::Invalid {
                field: "execution.sim.maker_fee_bps",
                ..
            }
        ),
        "a block that is present is validated in every mode, so flipping back to sim can never \
         surface a bad value that sat unchecked"
    );
}

#[test]
fn every_reason_a_simulated_config_is_refused_names_itself() {
    let accepted =
        config_with_execution_on(BINANCE_SPOT_SOURCE, &execution_block_with("sim", SIM_BLOCK));
    polysim::config::Config::<NoParams>::from_yaml(&accepted)
        .expect("binance spot with all three market streams and a sim block is the valid shape");

    let with_maker_fee = |bps: &str| {
        let block = SIM_BLOCK.replace("maker_fee_bps: 10.0", &format!("maker_fee_bps: {bps}"));
        config_with_execution_on(BINANCE_SPOT_SOURCE, &execution_block_with("sim", &block))
    };
    for bps in ["-0.4", "7.5"] {
        polysim::config::Config::<NoParams>::from_yaml(&with_maker_fee(bps)).unwrap_or_else(
            |error| panic!("a fractional fee and a rebate are both real schedules, {bps}: {error}"),
        );
    }
    for bps in ["10000.5", "-10000.5", ".nan"] {
        assert!(
            matches!(
                refusal(&with_maker_fee(bps)),
                ConfigError::Invalid {
                    field: "execution.sim.maker_fee_bps",
                    ..
                }
            ),
            "{bps} is not a fee rate: it must be finite and move no more than the whole fill"
        );
    }

    let no_block = config_with_execution_on(BINANCE_SPOT_SOURCE, &execution_block_with("sim", ""));
    assert!(
        matches!(
            refusal(&no_block),
            ConfigError::Invalid {
                field: "execution.sim",
                ..
            }
        ),
        "mode: sim with no sim block leaves every venue assumption at a default nobody chose"
    );

    let perpetual =
        config_with_execution_on(BINANCE_PERP_SOURCE, &execution_block_with("sim", SIM_BLOCK));
    assert!(
        matches!(
            refusal(&perpetual),
            ConfigError::SimulatedExecutionMarket {
                market: "perpetual"
            }
        ),
        "the fill model is built on spot's depth granularity and aggregate trades"
    );

    let poly_sim = refusal(&config_with_execution_on(
        POLY_SOURCE,
        &execution_block_with("sim", SIM_BLOCK),
    ));
    let ConfigError::ExecutionModeUnsupported {
        venue,
        mode,
        supported,
    } = &poly_sim
    else {
        panic!("sim on a venue with no simulated edge must be refused by name, got {poly_sim}");
    };
    assert_eq!(
        (*venue, *mode, supported.as_ref()),
        ("polymarket", "sim", "off, live"),
        "the refusal names the venue, the mode it cannot serve, and what it can"
    );

    for (field, missing) in [
        ("trades", "trades"),
        ("book_updates", "book_updates"),
        ("book_snapshots", "book_snapshots"),
    ] {
        let source = format!("{BINANCE_SPOT_SOURCE}  subscriptions:\n    {field}: false\n");
        let yaml = config_with_execution_on(&source, &execution_block_with("sim", SIM_BLOCK));
        let error = refusal(&yaml);
        let ConfigError::SimulatedExecutionSubscriptions { missing: named } = &error else {
            panic!("disabling {field} must be refused by name, got {error}");
        };
        assert_eq!(
            named.as_ref(),
            missing,
            "the refusal names the stream the venue would have matched against"
        );
    }
}

#[test]
fn a_one_second_spin_has_a_3030ms_budget_and_fits_the_default_timeout() {
    let yaml = config_with_execution_on(
        BINANCE_SPOT_SOURCE,
        &execution_block_with("sim", "  sim: {}\n"),
    )
    .replace("spin_interval_us: 100000", "spin_interval_us: 1000000");

    polysim::config::Config::<NoParams>::from_yaml(&yaml)
        .expect("3030ms is strictly inside the default 5000ms timeout");

    let tight = yaml.replace(
        "execution:\n  mode: sim\n",
        "execution:\n  mode: sim\n  inflight_timeout_ms: 3030\n",
    );
    let error = refusal(&tight);
    let ConfigError::Invalid {
        field: "execution.sim",
        value,
        ..
    } = &error
    else {
        panic!("the budget refusal must name the sim block, got {error}");
    };
    assert_eq!(
        value.as_ref(),
        "3030",
        "the operator is told the millisecond sum they have to clear, not which field to guess at"
    );
    let just_inside = yaml.replace(
        "execution:\n  mode: sim\n",
        "execution:\n  mode: sim\n  inflight_timeout_ms: 3031\n",
    );
    polysim::config::Config::<NoParams>::from_yaml(&just_inside)
        .expect("one millisecond beyond the worst legal answer is sufficient");
}

/// Some documents are refused by the typed parse and some only once the registry is built from
/// them; a caller pinning the refusal cares about the field named, not which pass named it.
fn refusal(yaml: &str) -> ConfigError {
    match polysim::config::Config::<NoParams>::from_yaml(yaml) {
        Err(error) => error,
        Ok(config) => Registry::build(&config).expect_err("this document must be refused"),
    }
}

/// The two floors are asymmetric on purpose. The quote floor reserves cash the engine must not
/// spend down, so zero is a decision to trade the account to nothing and is refused. The base floor
/// reserves the asset being SOLD, and where the base asset IS the position — outcome shares — zero
/// is the only correct value: any positive reserve subtracts from every exit, so a full-position
/// offer reads Underfunded and a flatten rounds under the venue's minimum size and strands the
/// position to resolution. Negative is nonsense on either. Each refusal names the field.
#[test]
fn the_base_floor_may_be_zero_and_the_quote_floor_may_not() {
    let zero_base = config_with_execution(&execution_block_overriding(
        "live",
        "min_base_balance",
        "0.0",
    ));
    let config = polysim::config::Config::<NoParams>::from_yaml(&zero_base)
        .expect("zero is the only correct base reserve where the base asset IS the position");
    assert_eq!(
        config.execution.expect("execution block").min_base_balance,
        0.0
    );

    for (field, value) in [
        ("min_base_balance", "-0.001"),
        ("min_quote_balance", "0.0"),
        ("min_quote_balance", "-1.0"),
    ] {
        let yaml = config_with_execution(&execution_block_overriding("live", field, value));
        let error = polysim::config::Config::<NoParams>::from_yaml(&yaml)
            .expect_err("a reserve below zero, and a quote reserve of zero, must be refused");
        let text = error.to_string();
        assert!(
            text.contains(field),
            "the refusal names the field an operator edits, got {text}"
        );
    }
}

/// Every execution field whose range has no other floor/ceiling test: the µs-duration fields
/// overflow the runtime's storage type at `u64::MAX`, and the bps field overflows the runtime's
/// centi-bps conversion at a merely enormous finite float — two different overflow mechanisms,
/// same refusal shape.
#[test]
fn execution_numeric_fields_reject_out_of_range_values() {
    let max = u64::MAX.to_string();
    let cases: [(&str, String, Option<&str>); 6] = [
        (
            "execution.max_book_age_ms",
            execution_block_overriding("live", "max_book_age_ms", &max),
            Some(max.as_str()),
        ),
        (
            "execution.inflight_timeout_ms",
            format!("{}  inflight_timeout_ms: {max}\n", execution_block("live")),
            Some(max.as_str()),
        ),
        (
            "execution.order_reap_secs",
            format!("{}  order_reap_secs: {max}\n", execution_block("live")),
            Some(max.as_str()),
        ),
        (
            "execution.disconnect_sweep_secs",
            format!(
                "{}  disconnect_sweep_secs: {max}\n",
                execution_block("live")
            ),
            Some(max.as_str()),
        ),
        (
            "execution.max_clock_skew_ms",
            format!("{}  max_clock_skew_ms: {max}\n", execution_block("live")),
            Some(max.as_str()),
        ),
        (
            "execution.max_quote_distance_bps",
            execution_block_overriding("live", "max_quote_distance_bps", "1e308"),
            None,
        ),
    ];
    for (expected_field, block, expected_value) in cases {
        let error = refusal(&config_with_execution(&block));
        match &error {
            ConfigError::Invalid { field, value, .. } if *field == expected_field => {
                if let Some(expected_value) = expected_value {
                    assert_eq!(
                        value.as_ref(),
                        expected_value,
                        "{expected_field}: wrong value in refusal, got {error}"
                    );
                }
            }
            _ => panic!("{expected_field}: overflow must be refused by field, got {error}"),
        }
    }
}
