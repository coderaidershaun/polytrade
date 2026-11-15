//! What config validation proves about a simulated run before it starts: the source can be
//! simulated, a command is answered inside the in-flight timeout, and the fee is a rate it can charge.

use engine::msg::ids::{
    AssetId, ClientOrderId, FIXED_SCALE, Notional, Price, Qty, Side, TradeId, VenueOrderId,
};
use engine::msg::time::LocalTs;
use engine::runtime::config::{Config, ConfigError, ExecutionMode, FeeBps, NoParams, RunIdentity};
use engine::venues::exec::{EngineIdentity, TeTag};
use engine::venues::sim::core::orders::SimOrder;
use engine::venues::sim::core::wallet::{
    ReservationRequest, ReserveOutcome, SimWallet, SimWalletSetup,
};
use engine::venues::sim::wire::{SimFill, VenueWire};

use crate::runtime::config_guards::{
    BINANCE_PERP_SOURCE, BINANCE_SPOT_SOURCE, POLY_SOURCE, SIM_BLOCK, config_with_execution_on,
    execution_block_with, refusal,
};

/// A run in mode `sim` on a source the simulator cannot stand in for would report itself armed and
/// place nothing, so each such source is refused at load, by name. A `sim:` block is validated in
/// every mode, so a later switch to `sim` cannot surface a bad value that sat unchecked.
#[test]
fn sim_mode_on_source_with_no_simulator_is_refused_by_name() {
    let simulated = |source: &str, sim: &str| {
        config_with_execution_on(source, &execution_block_with("sim", sim))
    };
    Config::<NoParams>::from_yaml(&simulated(BINANCE_SPOT_SOURCE, SIM_BLOCK))
        .expect("binance spot with all three market streams and a sim block is the valid shape");

    let poly_sim = refusal(&simulated(POLY_SOURCE, SIM_BLOCK));
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

    assert!(
        matches!(
            refusal(&simulated(BINANCE_PERP_SOURCE, SIM_BLOCK)),
            ConfigError::SimulatedExecutionMarket {
                market: "perpetual"
            }
        ),
        "the fill model is built on spot's depth granularity and aggregate trades"
    );

    for stream in ["trades", "book_updates", "book_snapshots"] {
        let source = format!("{BINANCE_SPOT_SOURCE}  subscriptions:\n    {stream}: false\n");
        let error = refusal(&simulated(&source, SIM_BLOCK));
        let ConfigError::SimulatedExecutionSubscriptions { missing } = &error else {
            panic!("disabling {stream} must be refused by name, got {error}");
        };
        assert_eq!(
            missing.as_ref(),
            stream,
            "the refusal names the stream the venue would have matched against"
        );
    }

    assert!(
        matches!(
            refusal(&simulated(BINANCE_SPOT_SOURCE, "")),
            ConfigError::Invalid {
                field: "execution.sim",
                ..
            }
        ),
        "mode: sim with no sim block leaves every venue assumption at a default nobody chose"
    );

    let with_maker_fee = |mode: &str, bps: &str| {
        let block = SIM_BLOCK.replace("maker_fee_bps: 10.0", &format!("maker_fee_bps: {bps}"));
        config_with_execution_on(BINANCE_SPOT_SOURCE, &execution_block_with(mode, &block))
    };
    for bps in ["-0.4", "7.5"] {
        Config::<NoParams>::from_yaml(&with_maker_fee("sim", bps)).unwrap_or_else(|error| {
            panic!("a fractional fee and a rebate are both real schedules, {bps}: {error}")
        });
    }
    for bps in ["10000.5", "-10000.5", ".nan"] {
        assert!(
            matches!(
                refusal(&with_maker_fee("sim", bps)),
                ConfigError::Invalid {
                    field: "execution.sim.maker_fee_bps",
                    ..
                }
            ),
            "{bps} is not a fee rate: it must be finite and move no more than the whole fill"
        );
    }

    for (spelling, expected) in [("off", ExecutionMode::Off), ("live", ExecutionMode::Live)] {
        let yaml = config_with_execution_on(
            BINANCE_SPOT_SOURCE,
            &execution_block_with(spelling, SIM_BLOCK),
        );
        let config = Config::<NoParams>::from_yaml(&yaml)
            .unwrap_or_else(|error| panic!("a sim block under {spelling} must load: {error}"));
        assert_eq!(
            config.sources[0].execution.as_ref().map(|block| block.mode),
            Some(expected),
            "the block is carried, but the mode is still the one the operator typed"
        );
    }
    assert!(
        matches!(
            refusal(&with_maker_fee("live", "10000.5")),
            ConfigError::Invalid {
                field: "execution.sim.maker_fee_bps",
                ..
            }
        ),
        "a block that is present is validated in every mode, so flipping back to sim can never \
         surface a bad value that sat unchecked"
    );
}

/// The simulator answers a command no later than entry, ack, the market-data bound, and two of the
/// slowest producer heartbeats. A timeout at or below that sum would expire requests that the
/// venue is still allowed to answer, so the start is refused with the sum the operator must clear.
#[test]
fn round_trip_at_or_above_timeout_refuses_start() {
    let yaml = config_with_execution_on(
        BINANCE_SPOT_SOURCE,
        &execution_block_with("sim", "  sim: {}\n"),
    )
    .replace("spin_interval_us: 100000", "spin_interval_us: 1000000");
    let with_timeout = |ms: u64| {
        yaml.replace(
            "execution:\n      mode: sim\n",
            &format!("execution:\n      mode: sim\n      inflight_timeout_ms: {ms}\n"),
        )
    };

    Config::<NoParams>::from_yaml(&yaml)
        .expect("3030ms is strictly inside the default 5000ms timeout");

    let error = refusal(&with_timeout(3_030));
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

    Config::<NoParams>::from_yaml(&with_timeout(3_031))
        .expect("one millisecond beyond the worst legal answer is sufficient");
}

struct FeeRow {
    name: &'static str,
    side: Side,
    maker_fee_bps: f64,
    price: Price,
    qty: Qty,
    slices: &'static [i64],
    last_fee: i64,
    fee_asset: &'static str,
    last_received_net: i64,
    base_free: i64,
    quote_free: i64,
    wire_fee: &'static str,
}

/// The configured rate in basis points becomes the rate the simulated wallet charges. A rate that
/// lost its scale on the way would still compile and charge almost nothing, so each row states the
/// amount it charges. A charge is taken from the asset received, and a rebate is paid in quote.
#[test]
fn maker_fee_of_10_bps_charges_known_amount() {
    const BASE: AssetId = AssetId(0);
    const QUOTE: AssetId = AssetId(1);
    let at_100_000 = Price::from_mantissa(100_000 * FIXED_SCALE);
    let at_117_999 = Price::from_mantissa(117_999 * FIXED_SCALE);
    let rows = [
        FeeRow {
            name: "10 bps charged on a buy of 0.001 BTC",
            side: Side::Buy,
            maker_fee_bps: 10.0,
            price: at_100_000,
            qty: Qty::from_mantissa(100_000),
            slices: &[100_000],
            last_fee: 100,
            fee_asset: "BTC",
            last_received_net: 99_900,
            base_free: 99_900,
            quote_free: 0,
            wire_fee: "0.00000100",
        },
        FeeRow {
            name: "a 0.4 bps rebate on a sell of 100 USDT",
            side: Side::Sell,
            maker_fee_bps: -0.4,
            price: at_100_000,
            qty: Qty::from_mantissa(100_000),
            slices: &[100_000],
            last_fee: -400_000,
            fee_asset: "USDT",
            last_received_net: 10_000_000_000 + 400_000,
            base_free: 0,
            quote_free: 10_000_000_000 + 400_000,
            wire_fee: "-0.00400000",
        },
        FeeRow {
            name: "rebated sell",
            side: Side::Sell,
            maker_fee_bps: -0.4,
            price: at_117_999,
            qty: Qty::from_mantissa(10_000),
            slices: &[10_000],
            last_fee: -47_199,
            fee_asset: "USDT",
            last_received_net: 1_179_990_000 + 47_199,
            base_free: 0,
            quote_free: 1_179_990_000 + 47_199,
            wire_fee: "-0.00047199",
        },
        FeeRow {
            name: "charged buy",
            side: Side::Buy,
            maker_fee_bps: 7.5,
            price: at_117_999,
            qty: Qty::from_mantissa(10_000),
            slices: &[10_000],
            last_fee: 7,
            fee_asset: "BTC",
            last_received_net: 9_993,
            base_free: 9_993,
            quote_free: 0,
            wire_fee: "0.00000007",
        },
        FeeRow {
            name: "rebated buy",
            side: Side::Buy,
            maker_fee_bps: -0.4,
            price: at_117_999,
            qty: Qty::from_mantissa(10_000),
            slices: &[5_000, 5_000],
            last_fee: -23_599,
            fee_asset: "USDT",
            last_received_net: 5_000,
            base_free: 10_000,
            quote_free: 2 * 23_599,
            wire_fee: "-0.00023599",
        },
    ];

    for row in rows {
        let name = row.name;
        let (opening_base, opening_quote) = match row.side {
            Side::Buy => (Qty::from_mantissa(0), row.price.notional(row.qty)),
            Side::Sell => (row.qty, Notional::ZERO),
        };
        let maker_fee_bps = FeeBps::from_bps(row.maker_fee_bps)
            .unwrap_or_else(|error| panic!("{name}: the rate is in range: {error}"));
        let mut wallet = SimWallet::new(SimWalletSetup {
            base_asset: BASE,
            quote_asset: QUOTE,
            opening_base,
            opening_quote,
            maker_fee_bps,
        })
        .expect("two distinct assets and non-negative balances");
        let request = ReservationRequest {
            side: row.side,
            price: row.price,
            qty: row.qty,
        };
        let ReserveOutcome::Reserved(mut reservation) = wallet.reserve(request) else {
            panic!("{name}: the opening balance covers the whole order");
        };

        let settlement = row
            .slices
            .iter()
            .map(|&slice| wallet.fill(&mut reservation, Qty::from_mantissa(slice)))
            .last()
            .expect("every row fills at least once");

        let fee_asset = match settlement.fee_asset {
            BASE => "BTC",
            QUOTE => "USDT",
            other => panic!(
                "{name}: the fee was paid in asset {} of neither slot",
                other.0
            ),
        };
        assert_eq!(
            (settlement.fee, fee_asset, settlement.received_net),
            (row.last_fee, row.fee_asset, row.last_received_net),
            "{name}: the last fill's fee, the asset it is paid in, and what the received asset rose by"
        );
        let [base, quote] = wallet.settle(0).balances;
        assert_eq!(
            (base.free, quote.free),
            (row.base_free, row.quote_free),
            "{name}: free balances after the order is fully filled"
        );

        let order = SimOrder {
            client_id: ClientOrderId(42),
            venue_order_id: VenueOrderId(12_510_053_281),
            side: row.side,
            price: row.price,
            qty: row.qty,
            filled: settlement.cumulative_qty,
            filled_quote: settlement.cumulative_quote,
        };
        let report = wire().trade_report(
            &order,
            SimFill {
                trade_id: TradeId(778_291),
                settlement: &settlement,
                fee_asset,
            },
            LocalTs::from_micros(1_785_000_123_456_789),
        );
        assert!(
            report.contains(&format!("\"n\":\"{}\"", row.wire_fee)),
            "{name}: the wire commission amount: {report}"
        );
        assert!(
            report.contains(&format!("\"N\":\"{}\"", row.fee_asset)),
            "{name}: the wire commission asset: {report}"
        );
    }
}

fn wire() -> VenueWire {
    let identity = RunIdentity::new("strat-micro-recorder", "te-binance-spot-btcusdt")
        .expect("the fixture strategy and trading engine ids are well formed");
    VenueWire::new(EngineIdentity {
        te_tag: TeTag::of(&identity),
        run_nonce: 1_785_000_000,
    })
}
