//! Startup scale check: using the committed exchangeInfo fixtures, it passes a real symbol —
//! pinning the exact `tickSize`/`stepSize` mantissas it returns — and REFUSES one whose price/qty
//! bounds or trading grid are absent, non-positive, or unparseable, so a venue schema rename can
//! never make the fixed-point guard vacuously pass. The Polymarket arm pins the same taxonomy:
//! both venue ticks (0.01 and the 0.001 endgame tick) pass, a drifted tick or absent min size
//! refuses. `check_symbol_scale` and `check_poly_market` are pure.

use polysim::adapters::binance::preflight::{
    ScaleError, check_order_rate_limits, check_symbol_scale, order_budget,
};
use polysim::adapters::binance::rest::{ExchangeInfo, SymbolFilter, SymbolInfo};
use polysim::adapters::polymarket::preflight::{
    ScaleError as PolyScaleError, check_poly_market, stamp_poly_scales,
};
use polysim::adapters::polymarket::rest::GammaMarket;
use polysim::config::{BinanceEnv, BinanceMarket, VenueMarket};
use polysim::hot::exec::{MAX_ORDER_BUDGET_WINDOWS, OrderBudgetWindow};
use polysim::ids::{AssetId, Price, Qty};
use polysim::registry::{InstrumentRow, OrderRateLimit, RateInterval, Registry};
use polysim::runtime::{EngineError, check_symbol_order_capacity};
use polysim::time::{DurationUs, TsUs};

use crate::engine_support::{ONE, instrument_row, tracker_spec_all};

const PERP_EXCHANGE_INFO: &str = include_str!("../../fixtures/binance/perp_exchange_info.json");
const SPOT_EXCHANGE_INFO: &str = include_str!("../../fixtures/binance/spot_exchange_info.json");

fn perp_info() -> ExchangeInfo {
    serde_json::from_str(PERP_EXCHANGE_INFO).expect("parse committed perp exchangeInfo fixture")
}

fn spot_info() -> ExchangeInfo {
    serde_json::from_str(SPOT_EXCHANGE_INFO).expect("parse committed spot exchangeInfo fixture")
}

/// The fixture's instrument is BTCUSDT perp; `instrument_row` builds exactly that venue symbol.
fn btc_perp() -> InstrumentRow {
    instrument_row(0, tracker_spec_all(1), 64)
}

/// The same BTCUSDT symbol declared spot. The two markets publish the same limits under different
/// filter and field names, so the market ON THE ROW decides which names the preflight reads — a
/// spot row against the perp fixture (or the reverse) must not silently half-validate.
fn btc_spot() -> InstrumentRow {
    InstrumentRow {
        market: VenueMarket::Binance(BinanceMarket::Spot),
        ..btc_perp()
    }
}

/// The venue's grid, order floors and counts, read from the real payloads. Without them the engine
/// builds orders it believes are legal and the venue rejects — a failure that surfaces as an
/// unexplained rejection at runtime rather than a refusal to start, so the exact mantissas are
/// pinned here. Futures publishes the same limits under `MIN_NOTIONAL.notional` and
/// `MAX_NUM_ORDERS.limit` and publishes no amend-count filter at all, so each market is pinned
/// against the payload it actually serves.
#[test]
fn each_market_parses_to_its_own_exact_grid_and_limits() {
    // The perp fixture quotes tickSize "0.10" and stepSize "0.001"; spot writes the same kind of
    // grid in trailing-zero form ("0.01000000" / "0.00001000"). Both are exact at the 1e-8 scale,
    // and both fixtures name BTCUSDT, which the rows' venue symbol matches case-insensitively.
    let cases = [
        (
            "perp",
            btc_perp(),
            perp_info(),
            Price(10_000_000),
            Qty(100_000),
            Qty(100_000),
            50 * ONE,
            200,
            None,
        ),
        (
            "spot",
            btc_spot(),
            spot_info(),
            Price(1_000_000),
            Qty(1_000),
            Qty(1_000),
            500_000_000,
            200,
            Some(10),
        ),
    ];
    for (case, row, info, tick_size, step_size, min_qty, min_notional, max_orders, max_amends) in
        cases
    {
        let scales = check_symbol_scale(&row, &info)
            .unwrap_or_else(|error| panic!("{case}: real BTCUSDT bounds must fit, got {error:?}"));
        assert_eq!(scales.tick_size, tick_size, "{case}: tickSize");
        assert_eq!(scales.step_size, step_size, "{case}: stepSize");

        let limits = scales.limits;
        // LOT_SIZE.minQty — NOT MARKET_LOT_SIZE.minQty, which the spot fixture quotes as
        // "0.00000000"; reading the wrong filter yields a floor of zero, i.e. no floor at all.
        assert_eq!(limits.min_qty, min_qty, "{case}: LOT_SIZE.minQty");
        assert_eq!(limits.min_notional, min_notional, "{case}: minNotional");
        assert_eq!(
            limits.max_num_orders, max_orders,
            "{case}: max resting orders"
        );
        assert_eq!(
            limits.max_num_order_amends, max_amends,
            "{case}: futures publishes no MAX_NUM_ORDER_AMENDS filter — None is its schema, not an \
             unread field"
        );
    }
}

#[test]
fn configured_two_sided_ladder_must_fit_the_symbol_order_limit() {
    let mut row = btc_spot();
    row.max_num_orders = Some(16);
    check_symbol_order_capacity(&row, 8).expect("eight orders on both sides exactly fit sixteen");

    row.max_num_orders = Some(15);
    match check_symbol_order_capacity(&row, 8)
        .expect_err("the requested steady-state ladder cannot fit")
    {
        EngineError::ExecutionOrderCapacity {
            configured_per_side,
            required_total,
            venue_max,
            ..
        } => {
            assert_eq!(configured_per_side, 8);
            assert_eq!(required_total, 16);
            assert_eq!(venue_max, 15);
        }
        other => panic!("expected ExecutionOrderCapacity, got {other:?}"),
    }
}

/// Every distinct cause `check_symbol_scale` refuses for: a bound/limit/grid field absent, a limit
/// at zero (the dangerous case — it reads as "the venue has no floor"), a grid value unparseable, a
/// row's market reading the wrong venue's filter names, the symbol missing entirely, and the
/// `MARKET_LOT_SIZE` mirror never standing in for a stripped `LOT_SIZE`.
enum ScaleRejection<'a> {
    FieldMissing(&'a str),
    LimitNotPositive(&'a str, Option<&'a str>),
    NotPositive(&'a str),
    OutOfRange(&'a str),
    SymbolUnknown,
}

fn assert_scale_rejection(case: &str, error: &ScaleError, expected: &ScaleRejection) {
    match (expected, error) {
        (ScaleRejection::FieldMissing(field), ScaleError::FieldMissing { field: got, .. }) => {
            assert_eq!(got, field, "{case}: wrong missing field, got {error:?}");
        }
        (
            ScaleRejection::LimitNotPositive(field, value),
            ScaleError::LimitNotPositive {
                field: got_field,
                value: got_value,
                ..
            },
        ) => {
            assert_eq!(got_field, field, "{case}: wrong field, got {error:?}");
            if let Some(value) = value {
                assert_eq!(
                    got_value.as_ref(),
                    *value,
                    "{case}: wrong value, got {error:?}"
                );
            }
        }
        (ScaleRejection::NotPositive(field), ScaleError::NotPositive { field: got, .. }) => {
            assert_eq!(got, field, "{case}: wrong field, got {error:?}");
        }
        (ScaleRejection::OutOfRange(field), ScaleError::OutOfRange { field: got, .. }) => {
            assert_eq!(got, field, "{case}: wrong field, got {error:?}");
        }
        (ScaleRejection::SymbolUnknown, ScaleError::SymbolUnknown { .. }) => {}
        _ => panic!("{case}: wrong error variant, got {error:?}"),
    }
}

#[test]
fn check_symbol_scale_refuses_bad_missing_or_mismatched_inputs() {
    let mirror_only = strip_filter(spot_info(), "LOT_SIZE");
    assert!(
        mirror_only.symbols[0]
            .filters
            .iter()
            .any(|filter| filter.filter_type.as_ref() == "MARKET_LOT_SIZE"),
        "the mirror filter must survive, or the case below proves nothing"
    );

    let cases: Vec<(&str, InstrumentRow, ExchangeInfo, ScaleRejection)> = vec![
        (
            "perp: absent PRICE_FILTER",
            btc_perp(),
            strip_filter(perp_info(), "PRICE_FILTER"),
            ScaleRejection::FieldMissing("PRICE_FILTER.maxPrice"),
        ),
        (
            "perp: absent LOT_SIZE",
            btc_perp(),
            strip_filter(perp_info(), "LOT_SIZE"),
            ScaleRejection::FieldMissing("LOT_SIZE.maxQty"),
        ),
        (
            "perp: symbol not listed",
            btc_perp(),
            ExchangeInfo {
                rate_limits: Vec::new(),
                symbols: Vec::new(),
            },
            ScaleRejection::SymbolUnknown,
        ),
        (
            "spot row against perp fixture reads futures filter names",
            btc_spot(),
            perp_info(),
            ScaleRejection::FieldMissing("NOTIONAL.minNotional"),
        ),
        (
            "spot: absent NOTIONAL",
            btc_spot(),
            strip_filter(spot_info(), "NOTIONAL"),
            ScaleRejection::FieldMissing("NOTIONAL.minNotional"),
        ),
        (
            "spot: zero minNotional",
            btc_spot(),
            set_field(spot_info(), "NOTIONAL", |filter| {
                filter.min_notional = Some("0.00000000".into())
            }),
            ScaleRejection::LimitNotPositive("minNotional", Some("0")),
        ),
        (
            "spot: zero minQty",
            btc_spot(),
            set_field(spot_info(), "LOT_SIZE", |filter| {
                filter.min_qty = Some("0.00000000".into())
            }),
            ScaleRejection::LimitNotPositive("minQty", None),
        ),
        (
            "spot: absent MAX_NUM_ORDER_AMENDS",
            btc_spot(),
            strip_filter(spot_info(), "MAX_NUM_ORDER_AMENDS"),
            ScaleRejection::FieldMissing("MAX_NUM_ORDER_AMENDS.maxNumOrderAmends"),
        ),
        (
            "spot: zero maxNumOrders",
            btc_spot(),
            set_field(spot_info(), "MAX_NUM_ORDERS", |filter| {
                filter.max_num_orders = Some(0)
            }),
            ScaleRejection::LimitNotPositive("maxNumOrders", None),
        ),
        (
            "spot: LOT_SIZE gone, MARKET_LOT_SIZE mirror must not substitute",
            btc_spot(),
            mirror_only,
            ScaleRejection::FieldMissing("LOT_SIZE.maxQty"),
        ),
        (
            "perp: absent tickSize",
            btc_perp(),
            set_field(perp_info(), "PRICE_FILTER", |filter| {
                filter.tick_size = None
            }),
            ScaleRejection::FieldMissing("PRICE_FILTER.tickSize"),
        ),
        (
            "perp: absent stepSize",
            btc_perp(),
            set_field(perp_info(), "LOT_SIZE", |filter| filter.step_size = None),
            ScaleRejection::FieldMissing("LOT_SIZE.stepSize"),
        ),
        (
            "perp: zero tickSize",
            btc_perp(),
            set_field(perp_info(), "PRICE_FILTER", |filter| {
                filter.tick_size = Some("0.00".into())
            }),
            ScaleRejection::NotPositive("tickSize"),
        ),
        (
            "perp: zero stepSize",
            btc_perp(),
            set_field(perp_info(), "LOT_SIZE", |filter| {
                filter.step_size = Some("0.00".into())
            }),
            ScaleRejection::NotPositive("stepSize"),
        ),
        (
            "perp: unparseable tickSize",
            btc_perp(),
            set_field(perp_info(), "PRICE_FILTER", |filter| {
                filter.tick_size = Some("abc".into())
            }),
            ScaleRejection::OutOfRange("tickSize"),
        ),
    ];
    for (case, row, info, expected) in cases {
        let error = check_symbol_scale(&row, &info).expect_err(&format!("{case}: must refuse"));
        assert_scale_rejection(case, &error, &expected);
    }
}

fn strip_filter(mut info: ExchangeInfo, filter_type: &str) -> ExchangeInfo {
    for symbol in &mut info.symbols {
        symbol
            .filters
            .retain(|filter| filter.filter_type.as_ref() != filter_type);
    }
    // Guard the test itself: the fixture must actually contain the symbol we mutate.
    assert!(
        info.symbols
            .iter()
            .any(|s: &SymbolInfo| !s.filters.is_empty()),
        "fixture symbol lost all filters — test setup wrong"
    );
    info
}

/// Mutate one field on the named filter across every symbol — targets the tickSize/stepSize path
/// specifically, leaving the maxPrice/maxQty checks it follows intact so the refusal is the grid's.
fn set_field(
    mut info: ExchangeInfo,
    filter_type: &str,
    edit: impl Fn(&mut SymbolFilter),
) -> ExchangeInfo {
    let mut edited = false;
    for symbol in &mut info.symbols {
        for filter in &mut symbol.filters {
            if filter.filter_type.as_ref() == filter_type {
                edit(filter);
                edited = true;
            }
        }
    }
    assert!(
        edited,
        "fixture has no {filter_type} filter — test setup wrong"
    );
    info
}

/// A resolved market with the given tick and min size; other fields are grid-consistent filler.
pub(crate) fn poly_market(tick_size: Price, min_order_size: Qty) -> GammaMarket {
    GammaMarket {
        slug: "btc-updown-5m-1784439600".into(),
        condition_id: "0xabc".into(),
        token_up: "111".into(),
        token_down: "222".into(),
        tick_size,
        min_order_size,
        window_open_ts_us: TsUs::from_micros(1_784_439_600_000_000),
        window_close_ts_us: TsUs::from_micros(1_784_439_900_000_000),
    }
}

/// A registry carrying one polymarket source, its four slots unstamped as they leave `build`.
pub(crate) fn build_poly_registry() -> Registry {
    let yaml = "engine:
  hot_core_id: 0
  spin_interval_us: 100000
queues:
  input_capacity: 65536
  persistence_capacity: 65536
source:
  exchange: polymarket
  max_exposure_quote: 500
  series: btc-updown-5m
  tracker: {}
strategy:
  instruments: all
persistence:
  dir: ./data
logging:
  dir: ./logs
";
    let config: polysim::config::Config =
        polysim::config::Config::from_yaml(yaml).expect("poly config parses and validates");
    Registry::build(&config).expect("registry builds")
}

/// Both venue ticks pass and a drifted one refuses, then the accepted window's grid is stamped onto
/// every slot — the same [`polysim::registry::Registry::set_scales`] mechanism binance uses — so
/// downstream code quantises poly prices against a real tick and lot.
#[test]
fn poly_preflight_accepts_the_venue_ticks_and_stamps_the_accepted_grid() {
    // 0.01 at the 1e-8 scale = 1_000_000 — the tick book_capacity 128 and the 0..1 scale assume.
    check_poly_market(&poly_market(Price(1_000_000), Qty(500_000_000)))
        .expect("the 0.01 design tick with a positive min size passes");

    // 0.001 = 100_000: the venue's endgame tick near the 0/1 bounds must not refuse a boot.
    check_poly_market(&poly_market(Price(100_000), Qty(500_000_000)))
        .expect("the 0.001 endgame tick passes");

    assert!(
        matches!(
            check_poly_market(&poly_market(Price(500_000), Qty(500_000_000)))
                .expect_err("a 0.005 tick must refuse the start"),
            PolyScaleError::TickNotAccepted { .. }
        ),
        "a tick outside the accepted set refuses",
    );

    match check_poly_market(&poly_market(Price(2_000_000), Qty(500_000_000)))
        .expect_err("a 0.02 tick must refuse the start")
    {
        PolyScaleError::TickNotAccepted {
            actual, expected, ..
        } => {
            assert_eq!(actual, Price(2_000_000));
            assert_eq!(expected, [Price(1_000_000), Price(100_000)]);
        }
        other => panic!("expected TickNotAccepted, got {other:?}"),
    }

    assert!(
        matches!(
            check_poly_market(&poly_market(Price(1_000_000), Qty(0)))
                .expect_err("an absent orderMinSize must refuse"),
            PolyScaleError::MinSizeNotPositive { .. }
        ),
        "a non-positive min order size refuses",
    );

    let mut registry = build_poly_registry();
    assert!(
        registry
            .instruments()
            .iter()
            .all(|row| row.tick_size.is_none() && row.lot_size.is_none()),
        "poly rows leave build unstamped"
    );

    // The 0.01 design tick and a 5.0 min order size, both exact at the 1e-8 scale.
    let current = poly_market(Price(1_000_000), Qty(500_000_000));
    stamp_poly_scales(&mut registry, &current);

    let poly_rows: Vec<&InstrumentRow> = registry
        .instruments()
        .iter()
        .filter(|row| matches!(row.market, VenueMarket::Polymarket(_)))
        .collect();
    assert_eq!(poly_rows.len(), 4, "the series fans out to four slots");
    for row in poly_rows {
        assert_eq!(row.tick_size, Some(Price(1_000_000)), "slot tick stamped");
        // The venue's two-decimal share step, NOT the 5.0 minimum: a floor stamped as a step
        // quantises every order to whole multiples of the minimum. `poly_exec_rows` pins where the
        // minimum went and what else the same pass stamps.
        assert_eq!(row.lot_size, Some(Qty(1_000_000)), "slot step stamped");
        assert_eq!(row.min_qty, Some(Qty(500_000_000)), "slot floor stamped");
    }
}

/// A registry carrying one binance spot source, BTC/USDT. `env_line` goes into the source block, so
/// a caller can pin what an omitted `env:` means as well as an explicit one.
fn build_binance_registry(env_line: &str) -> Registry {
    let yaml = format!(
        "engine:
  hot_core_id: 0
  spin_interval_us: 100000
queues:
  input_capacity: 65536
  persistence_capacity: 65536
source:
  exchange: binance
  market: spot
{env_line}  base: BTC
  quote: USDT
  max_exposure_quote: 500
  tracker: {{}}
strategy:
  instruments: all
persistence:
  dir: ./data
logging:
  dir: ./logs
"
    );
    let config: polysim::config::Config =
        polysim::config::Config::from_yaml(&yaml).expect("binance config parses and validates");
    Registry::build(&config).expect("registry builds")
}

/// FITNESS: the buckets the venue published are the ones the engine paces against. The parse and
/// the gate are two halves of one promise, and a payload read correctly into a budget nobody carries
/// is the shape this whole seam was missing — every window has to survive the crossing, at its own
/// length and its own cap.
///
/// Both markets publish the budget under the same `rateLimitType` but over different windows, and it
/// is ACCOUNT-scoped, so it lands on the registry rather than being copied onto every instrument row
/// — a two-symbol engine reading it per row would believe it had twice the budget. The
/// REQUEST_WEIGHT and RAW_REQUESTS buckets ride the same array and are not an order budget:
/// counting one would let the engine believe it may send 6000 orders a minute.
#[test]
fn every_parsed_bucket_reaches_the_budget_the_engine_paces_against() {
    let spot_info = spot_info();
    assert!(
        spot_info
            .rate_limits
            .iter()
            .any(|entry| entry.rate_limit_type.as_ref() != "ORDERS"),
        "the fixture must carry non-ORDERS buckets, or the count below proves nothing"
    );
    let spot_buckets =
        check_order_rate_limits(BinanceMarket::Spot, &spot_info).expect("spot buckets parse");
    assert_eq!(
        spot_buckets.len(),
        2,
        "only the two ORDERS buckets are read"
    );

    let spot = order_budget(&spot_buckets).expect("two buckets are inside the model");
    assert_eq!(
        spot.windows(),
        [
            OrderBudgetWindow {
                window: DurationUs::from_secs(10),
                max_places: 100,
            },
            OrderBudgetWindow {
                window: DurationUs::from_secs(86_400),
                max_places: 200_000,
            },
        ],
        "spot grants 100 placements per 10 seconds and 200000 a day"
    );

    let perp = order_budget(
        &check_order_rate_limits(BinanceMarket::Perpetual, &perp_info())
            .expect("perp buckets parse"),
    )
    .expect("two buckets are inside the model");
    assert_eq!(
        perp.windows(),
        [
            OrderBudgetWindow {
                window: DurationUs::from_secs(60),
                max_places: 1_200,
            },
            OrderBudgetWindow {
                window: DurationUs::from_secs(10),
                max_places: 300,
            },
        ],
        "the perp windows are different lengths from spot's, and a budget that carried spot's would \
         pace against a grant this market never made"
    );
}

/// FITNESS: every way the budget could be read wrong refuses the start instead. Each of these fails
/// SILENTLY if tolerated: a dropped bucket leaves the engine pacing against a larger budget than it
/// has, and a venue declaring more buckets than the engine models is the same silent over-admission
/// — only harder to see, because every remaining window would look right.
#[test]
fn every_unreadable_absent_or_unmodellable_order_budget_refuses_the_start() {
    let bucket = OrderRateLimit {
        interval: RateInterval::Second,
        interval_num: 10,
        limit: 100,
    };
    let modelled = vec![bucket; MAX_ORDER_BUDGET_WINDOWS];
    assert!(
        order_budget(&modelled).is_ok(),
        "the model's own capacity must be accepted, or the case below is refusing everything"
    );

    let mut beyond = modelled;
    beyond.push(bucket);
    match order_budget(&beyond).expect_err("one bucket past the model refuses") {
        ScaleError::TooManyOrderWindows { found, max } => {
            assert_eq!(
                (found, max),
                (MAX_ORDER_BUDGET_WINDOWS + 1, MAX_ORDER_BUDGET_WINDOWS)
            );
        }
        other => panic!("wrong refusal: {other:?}"),
    }

    let mut none = spot_info();
    none.rate_limits
        .retain(|entry| entry.rate_limit_type.as_ref() != "ORDERS");
    assert!(
        matches!(
            check_order_rate_limits(BinanceMarket::Spot, &none)
                .expect_err("no ORDERS bucket must refuse"),
            ScaleError::OrderLimitsMissing { .. }
        ),
        "a payload with no ORDERS bucket refuses rather than pacing blind",
    );

    let mut unknown_interval = spot_info();
    for entry in &mut unknown_interval.rate_limits {
        if entry.rate_limit_type.as_ref() == "ORDERS" {
            entry.interval = "FORTNIGHT".into();
        }
    }
    match check_order_rate_limits(BinanceMarket::Spot, &unknown_interval)
        .expect_err("an unrecognised interval must refuse")
    {
        ScaleError::RateLimitUnreadable { field, value, .. } => {
            assert_eq!(field, "interval");
            assert_eq!(value.as_ref(), "FORTNIGHT");
        }
        other => panic!("expected RateLimitUnreadable, got {other:?}"),
    }

    let mut zero_limit = spot_info();
    for entry in &mut zero_limit.rate_limits {
        if entry.rate_limit_type.as_ref() == "ORDERS" {
            entry.limit = 0;
        }
    }
    match check_order_rate_limits(BinanceMarket::Spot, &zero_limit)
        .expect_err("a zero order budget must refuse")
    {
        ScaleError::RateLimitUnreadable { field, .. } => assert_eq!(field, "limit"),
        other => panic!("expected RateLimitUnreadable, got {other:?}"),
    }

    let mut zero_interval_num = spot_info();
    for entry in &mut zero_interval_num.rate_limits {
        if entry.rate_limit_type.as_ref() == "ORDERS" {
            entry.interval_num = 0;
        }
    }
    match check_order_rate_limits(BinanceMarket::Spot, &zero_interval_num)
        .expect_err("a zero intervalNum must refuse")
    {
        ScaleError::RateLimitUnreadable { field, .. } => assert_eq!(field, "intervalNum"),
        other => panic!("expected RateLimitUnreadable, got {other:?}"),
    }
}

/// One deployment choice for the whole run. The preflight and the market-data adapters both read it
/// here, so the engine cannot validate production's grid and then connect to testnet's hosts —
/// which is silent: both host sets answer, with different tick sizes and a different account.
#[test]
fn the_deployment_comes_from_config_and_defaults_to_production() {
    assert_eq!(
        build_binance_registry("").binance_env(),
        Some(BinanceEnv::Production),
        "an omitted env: keeps every config written before the field existed on production"
    );
    assert_eq!(
        build_binance_registry("  env: testnet\n").binance_env(),
        Some(BinanceEnv::Testnet)
    );
    assert_eq!(
        build_binance_registry("  env: production\n").binance_env(),
        Some(BinanceEnv::Production)
    );
    assert_eq!(
        build_poly_registry().binance_env(),
        None,
        "the deployment choice is binance's — a polymarket source has none"
    );
}

/// An [`AssetId`] is the identity the edge resolves a wire asset STRING into once, so nothing inward
/// of it compares text. Two builds of the same config must agree: an id recorded on one run is
/// read back against a registry rebuilt from that config, and a shifted assignment would silently
/// rename the asset it points at.
#[test]
fn asset_dictionary_interns_every_leg_and_is_stable_across_builds() {
    let first = build_binance_registry("");
    let row = &first.instruments()[0];
    assert_eq!(first.assets().name(row.base_asset), Some("BTC"));
    assert_eq!(first.assets().name(row.quote_asset), Some("USDT"));
    assert_ne!(row.base_asset, row.quote_asset, "the legs are distinct");
    assert_eq!(first.assets().len(), 2, "ids are dense 0..len");

    let second = build_binance_registry("");
    assert_eq!(second.instruments()[0].base_asset, row.base_asset);
    assert_eq!(second.instruments()[0].quote_asset, row.quote_asset);

    // How a venue cases its asset codes is its choice; an asset NO instrument names must resolve to
    // the sentinel, so its balance is counted and ignored rather than landing on a real one.
    assert_eq!(first.assets().id("usdt"), row.quote_asset);
    assert_eq!(first.assets().id("BNB"), AssetId::UNKNOWN);
    assert_eq!(first.assets().name(AssetId::UNKNOWN), None);

    // Four polymarket slots are four DIFFERENT conditional tokens over one shared collateral, so
    // the dictionary holds a base per slot plus one quote. A shared base would have every leg's
    // share balance read as every other leg's, which the sell-side funds gate spends against.
    let poly = build_poly_registry();
    assert_eq!(poly.assets().len(), 5);
    let legs: Vec<(AssetId, AssetId)> = poly
        .instruments()
        .iter()
        .map(|slot| (slot.base_asset, slot.quote_asset))
        .collect();
    let quotes: Vec<AssetId> = legs.iter().map(|(_, quote)| *quote).collect();
    assert_eq!(
        quotes,
        vec![quotes[0]; 4],
        "one collateral across the series"
    );
    let mut bases: Vec<AssetId> = legs.iter().map(|(base, _)| *base).collect();
    bases.sort_by_key(|asset| asset.0);
    bases.dedup();
    assert_eq!(bases.len(), 4, "each slot's shares are its own asset");
    for slot in poly.instruments() {
        assert_eq!(
            poly.assets().name(slot.base_asset),
            Some(&*slot.venue_symbol),
            "a leg's asset is named for the leg, so a balance can be routed back to it"
        );
    }
}
