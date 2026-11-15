//! What config validation proves about a simulated run before it starts: the source can be
//! simulated, a command is answered inside the in-flight timeout, and the fee is a rate it can charge.

use engine::runtime::config::{Config, ConfigError, ExecutionMode, NoParams};

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
