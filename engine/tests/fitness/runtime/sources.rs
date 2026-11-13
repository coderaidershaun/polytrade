//! The `sources:` list: each source gives its rows, its connections and the queues of its venue,
//! and the sources of one venue share that venue.

use engine::msg::ids::{AssetId, FIXED_SCALE, Notional, QueueId, VenueId};
use engine::runtime::config::{BinanceMarket, Config, ConfigError, PolySeries, VenueMarket};
use engine::runtime::registry::{ConnectionCategory, Registry};

use super::config_guards::{
    BINANCE_PERP_SOURCE, BINANCE_SPOT_SOURCE, POLY_SOURCE, execution_block_with, sources_with,
};

const POLY_SLOT_SYMBOLS: &[&str] = &[
    "btc-updown-5m-a-up",
    "btc-updown-5m-a-down",
    "btc-updown-5m-b-up",
    "btc-updown-5m-b-down",
];

/// A document whose `sources:` list holds one item for each pair of a feed block and the blocks
/// beside it, with a link block when `link` is true.
fn document(items: &[(&str, &str)], link: bool) -> String {
    let mut sources = String::from("sources:\n");
    for (feed, blocks) in items {
        let item = sources_with(feed, blocks);
        sources.push_str(
            item.strip_prefix("sources:\n")
                .expect("one list of one item"),
        );
    }
    let link = if link { "link:\n  bind: \"127.0.0.1:9310\"\n" } else { "" };
    format!(
        "engine:
  hot_core_id: 0
  spin_interval_us: 100000
queues:
  input_capacity: 65536
  persistence_capacity: 65536
{sources}strategy:
  instruments: all
persistence:
  dir: ./data
{link}logging:
  dir: ./logs
"
    )
}

fn registry_of(document: &str) -> Result<Registry, ConfigError> {
    let config: Config = Config::from_yaml(document)?;
    Registry::build(&config)
}

fn queue_ids(registry: &Registry, venues: u8) -> Vec<usize> {
    let mut ids: Vec<usize> = registry
        .producer_groups()
        .iter()
        .map(|group| group.queue_id.index())
        .collect();
    ids.push(registry.timer_queue_id().index());
    ids.extend(registry.link_queue_id().map(QueueId::index));
    for venue in 0..venues {
        ids.extend(registry.exec_queue_id(VenueId(venue)).map(QueueId::index));
    }
    ids.extend(registry.writer_status_queue_id().map(QueueId::index));
    ids
}

/// A binance coin of the spot market, one source of the venue binance.
fn spot_source(base: &str) -> String {
    BINANCE_SPOT_SOURCE.replace("base: BTC", &format!("base: {base}"))
}

/// FITNESS: each source of the list gives its own rows, with dense ids in source order and its own
/// exposure ceiling, its own connection for each category its market serves, and its venue. One
/// polymarket source is the four A/B x up/down slots on one connection; one binance source is one
/// instrument on three connections. Each venue that sends orders has its own input queue after the
/// timer and the link, in venue order, and the writer status comes last. The sources of one venue
/// share its id and its assets, so its USDT is one balance. A row of the wrong venue would book its
/// fills against another account.
#[test]
fn each_source_gives_its_rows_groups_and_queues() {
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
    let polymarket = FanOut {
        case: "polymarket",
        source: POLY_SOURCE,
        venue_symbols: POLY_SLOT_SYMBOLS,
        market: VenueMarket::Polymarket(PolySeries::BtcUpDown5m),
        has_klines: false,
        categories: &[ConnectionCategory::Market],
        instruments_per_group: &[4],
        input_queues: 2,
    };
    let binance = FanOut {
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
    };
    for expected in [&polymarket, &binance] {
        let case = expected.case;
        let registry = registry_of(&document(&[(expected.source, "")], false))
            .unwrap_or_else(|error| panic!("{case}: one source builds: {error}"));
        let rows = registry.instruments();
        let symbols: Vec<&str> = rows.iter().map(|row| row.venue_symbol.as_ref()).collect();
        assert_eq!(
            symbols, expected.venue_symbols,
            "{case}: venue symbols in config order"
        );
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(usize::from(row.instrument_id.0), index, "{case}: dense id");
            assert_eq!(row.market, expected.market, "{case}: the source's market");
            assert_eq!(
                row.subscriptions.klines, expected.has_klines,
                "{case}: klines"
            );
            assert_eq!(
                row.kline_intervals.is_empty(),
                !expected.has_klines,
                "{case}: kline intervals"
            );
            assert_eq!(
                row.max_exposure_quote,
                Notional::from_mantissa(500 * FIXED_SCALE),
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
            "{case}: rows of each connection"
        );
        assert_eq!(
            registry.input_queue_count(),
            expected.input_queues,
            "{case}: producer groups + the timer"
        );
    }

    let live = execution_block_with("live", "");
    let both = registry_of(&document(
        &[(BINANCE_PERP_SOURCE, &live), (POLY_SOURCE, &live)],
        true,
    ))
    .expect("a binance and a polymarket source build one registry");
    let venues: Vec<(u16, VenueId)> = both
        .instruments()
        .iter()
        .map(|row| (row.instrument_id.0, row.venue))
        .collect();
    assert_eq!(
        venues,
        [
            (0, VenueId(0)),
            (1, VenueId(1)),
            (2, VenueId(1)),
            (3, VenueId(1)),
            (4, VenueId(1))
        ],
        "the ids of the second source follow the first, and each row names its source's venue"
    );
    assert_eq!(
        [both.venue_name(VenueId(0)), both.venue_name(VenueId(1))],
        ["binance", "polymarket"]
    );
    let groups: Vec<(ConnectionCategory, Vec<u16>)> = both
        .producer_groups()
        .iter()
        .map(|group| {
            let ids = group.instruments.iter().map(|id| id.0).collect();
            (group.category, ids)
        })
        .collect();
    assert_eq!(
        groups,
        [
            (ConnectionCategory::Trades, vec![0]),
            (ConnectionCategory::Depth, vec![0]),
            (ConnectionCategory::Klines, vec![0]),
            (ConnectionCategory::Market, vec![1, 2, 3, 4]),
        ],
        "each source reads its own rows on its own connections"
    );
    assert_eq!(
        queue_ids(&both, 2),
        (0..9).collect::<Vec<usize>>(),
        "four groups, the timer, the link, the queue of each venue, then the writer status"
    );
    assert_eq!(
        [
            both.exec_queue_id(VenueId(0)),
            both.exec_queue_id(VenueId(1))
        ],
        [
            Some(QueueId::new(6).expect("id 6")),
            Some(QueueId::new(7).expect("id 7"))
        ],
        "each venue that sends orders reads its own queue"
    );
    let assets = both.assets();
    assert_ne!(
        assets.id(VenueId(0), "USDT"),
        AssetId::UNKNOWN,
        "binance names its USDT"
    );
    assert_eq!(
        assets.id(VenueId(1), "USDT"),
        AssetId::UNKNOWN,
        "polymarket's quote asset is USD, never binance's USDT"
    );

    let one_venue = registry_of(&document(
        &[(&spot_source("BTC"), &live), (BINANCE_PERP_SOURCE, "")],
        false,
    ))
    .expect("two sources of one venue build");
    let rows: Vec<(VenueId, AssetId)> = one_venue
        .instruments()
        .iter()
        .map(|row| (row.venue, row.quote_asset))
        .collect();
    assert_eq!(
        rows,
        [(VenueId(0), AssetId(1)), (VenueId(0), AssetId(1))],
        "the spot and perpetual sources of binance are one venue with one USDT"
    );
    assert_eq!(
        one_venue.producer_groups().len(),
        6,
        "each source of the venue has its own three connections"
    );
    assert_eq!(
        queue_ids(&one_venue, 1),
        (0..9).collect::<Vec<usize>>(),
        "six groups, the timer, the one queue of the venue, then the writer status"
    );

    let refusals = [
        (
            document(
                &[
                    (BINANCE_PERP_SOURCE, ""),
                    (POLY_SOURCE, ""),
                    (&spot_source("ETH"), ""),
                ],
                false,
            ),
            "apart",
        ),
        (
            document(
                &[(BINANCE_PERP_SOURCE, &live), (&spot_source("ETH"), &live)],
                false,
            ),
            "repeated execution",
        ),
        (
            document(
                &[
                    (
                        BINANCE_SPOT_SOURCE,
                        &execution_block_with("sim", "  sim: {}\n"),
                    ),
                    (POLY_SOURCE, &live),
                ],
                false,
            ),
            "modes differ",
        ),
    ];
    for (document, case) in refusals {
        let error = registry_of(&document).expect_err(case);
        let is_named = match case {
            "apart" => matches!(
                error,
                ConfigError::VenueSourcesApart {
                    venue: "binance",
                    first: 0,
                    second: 2
                }
            ),
            "repeated execution" => matches!(
                error,
                ConfigError::VenueExecutionRepeated {
                    venue: "binance",
                    first: 0,
                    second: 1
                }
            ),
            _ => matches!(
                error,
                ConfigError::SourceModesDiffer {
                    first: 0,
                    mode: "sim",
                    second: 1,
                    other: "live"
                }
            ),
        };
        assert!(is_named, "{case}: refused by its own variant, got {error}");
    }
}

/// The config case of the cap: six binance sources read on 18 connections, and with the timer and
/// the link they need 20 queues, which one hot thread reads. An execution block adds the queue of
/// the venue, the 21st, and the registry refuses the config.
pub(super) fn twenty_one_queue_documents() -> (String, String) {
    let coins = ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA"].map(spot_source);
    let live = execution_block_with("live", "");
    let without: Vec<(&str, &str)> = coins.iter().map(|coin| (coin.as_str(), "")).collect();
    let mut with = without.clone();
    with[0].1 = live.as_str();
    (document(&without, true), document(&with, true))
}
