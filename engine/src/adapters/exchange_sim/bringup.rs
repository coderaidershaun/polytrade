//! Standing a simulated venue up from a run's configuration: the market taps it matches against,
//! the matching model, the delivery schedule, the wire and the lifecycle core are assembled here
//! rather than by the caller, so the runtime hands over its inputs and never learns how the venue
//! is put together.

use crate::adapters::binance::exec::SymbolTable;
use crate::adapters::edge::{EdgeInputs, VenueEdge};
use crate::adapters::exec::{ExecCore, ExecStop};
use crate::config::{SimConfig, validated_mantissa};
use crate::hot::exec::MAX_ORDER_INSTRUMENTS;
use crate::info;
use crate::link::RunState;
use crate::registry::InstrumentRow;
use crate::time::DurationUs;

use super::actor::SimActor;
use super::core::latency::{LatencyBudget, StartupBudget};
use super::core::resting::{InstrumentLimits, ORDER_TABLE_CAPACITY};
use super::core::schedule::{DeliveryLimits, DeliverySchedule};
use super::core::wallet::{FeeBps, SimWalletSetup};
use super::core::{SimVenue, SimVenueSetup};
use super::driver::{SimExecDriver, SimExecDriverSetup};
use super::error::SimVenueError;
use super::lanes::{LaneBuffer, SimHorizon, SimLane};
use super::readiness::SimReadiness;
use super::tap::{MarketTapLane, MarketTaps, attach_market_taps};
use super::wire::VenueWire;

const ANSWER_CAPACITY: usize = 4_096;

/// # Errors
/// [`SimVenueError`] when the run is not wired for a simulated venue, when it configures other than
/// one instrument, when the opening account cannot be spent from, or when the configured latencies
/// leave no room to answer a command before it times out.
///
/// # Panics
/// If the producer groups do not hold exactly one Binance Spot trades lane and one depth lane.
pub(crate) fn bring_up(inputs: EdgeInputs<'_>) -> Result<VenueEdge, SimVenueError> {
    let EdgeInputs {
        registry,
        execution,
        identity,
        commands,
        producer,
        settings,
        groups,
        lane_capacity,
        spin_interval,
        input_capacity,
    } = inputs;
    let sim = execution.sim.as_ref().ok_or(SimVenueError::NotWired {
        detail: "the sim: block is absent, and config validation should have refused that",
    })?;
    let (tapped, taps) = attach_market_taps(registry, input_capacity, std::mem::take(groups));
    *groups = tapped;
    let instruments = registry.instruments();
    let [row] = instruments else {
        return Err(SimVenueError::OneInstrument {
            found: instruments.len(),
        });
    };

    let verdict_retention_micros = execution
        .order_reap_secs
        .checked_mul(1_000_000)
        .and_then(|reap| {
            execution
                .inflight_timeout_ms
                .checked_mul(1_000)
                .and_then(|timeout| reap.checked_add(timeout))
        })
        .and_then(|value| i64::try_from(value).ok())
        .expect("sim verdict retention was config-validated to fit i64 microseconds");
    let verdict_retention = DurationUs::from_micros(verdict_retention_micros);
    let inflight_timeout = DurationUs::from_millis(execution.inflight_timeout_ms as i64);
    let market_inbox_capacity = input_capacity.saturating_mul(2);
    let limits = instrument_limits(row)?;

    let latency = latency_budget(sim);
    let venue = SimVenue::new(SimVenueSetup {
        instrument: row.instrument_id,
        book_capacity: row.book_capacity,
        market_inbox_capacity,
        verdict_retention,
        limits,
        wallet: wallet_setup(row, sim)?,
    })?;
    info!(
        "simulated venue on {}: market inbox {market_inbox_capacity} events, order table \
         {ORDER_TABLE_CAPACITY} rows, verdict retention {}s — every one of them fatal on exhaustion",
        row.venue_symbol,
        verdict_retention.to_secs()
    );
    info!(
        "simulated venue latencies: entry {}ms, ack {}ms, hard market-data bound {}ms",
        sim.order_entry_latency_ms, sim.ack_latency_ms, sim.max_market_data_delay_ms
    );
    let worst_case = startup_budget(sim, spin_interval, inflight_timeout).check()?;
    info!(
        "simulated venue worst legal round trip {}ms inside a {}ms in-flight timeout",
        worst_case.micros() / 1_000,
        inflight_timeout.micros() / 1_000
    );

    let MarketTaps {
        trades,
        depth,
        gate,
    } = taps;
    let driver = SimExecDriver::new(SimExecDriverSetup {
        core: ExecCore::with_limits(settings.max_orders_per_side, MAX_ORDER_INSTRUMENTS),
        venue,
        schedule: DeliverySchedule::new(DeliveryLimits {
            ack_latency: latency.ack,
            answer_capacity: ANSWER_CAPACITY,
        }),
        wire: VenueWire::new(identity),
        instrument: row.instrument_id,
        fatal: settings.fatal.clone(),
    });
    let actor = SimActor {
        is_parked: settings.run_state.state() == RunState::Idle,
        market_recovery_generation: driver.venue().market().recovery().generation,
        driver,
        commands,
        trades: MarketTapLane::new(trades, SimLane::Trade),
        depth: MarketTapLane::new(depth, SimLane::Depth),
        producer,
        engine_clock: settings.clock,
        assets: registry.assets().clone(),
        symbols: SymbolTable::from_registry(registry),
        identity,
        latency,
        horizon: SimHorizon::unseeded(latency),
        commands_due: LaneBuffer::new(SimLane::Command, lane_capacity),
        readiness: SimReadiness::unseeded(),
        applied_through_ts_us: None,
        has_opened: false,
        stop: ExecStop::default(),
        run_state: settings.run_state,
        fatal: settings.fatal,
        sweep_deadline: settings.sweep_deadline,
    };
    Ok(VenueEdge {
        spawn: Box::new(move |runtime: &tokio::runtime::Handle| actor.spawn(runtime)),
        tap_gate: Some(gate),
    })
}

fn instrument_limits(row: &InstrumentRow) -> Result<InstrumentLimits, SimVenueError> {
    let missing = |detail: &'static str| SimVenueError::NotWired { detail };
    Ok(InstrumentLimits {
        tick: row
            .tick_size
            .ok_or(missing("the instrument has no stamped tick size"))?,
        step: row
            .lot_size
            .ok_or(missing("the instrument has no stamped lot size"))?,
        min_qty: row
            .min_qty
            .ok_or(missing("the instrument has no stamped minimum quantity"))?,
        min_notional: row
            .min_notional
            .ok_or(missing("the instrument has no stamped minimum notional"))?,
        max_orders_per_side: row
            .max_num_orders
            .ok_or(missing("the instrument has no stamped order-count limit"))?
            .try_into()
            .unwrap_or(u16::MAX),
        max_amends: row
            .max_num_order_amends
            .ok_or(missing("the instrument has no stamped amend-count limit"))?
            .try_into()
            .unwrap_or(u8::MAX),
    })
}

fn latency_budget(sim: &SimConfig) -> LatencyBudget {
    LatencyBudget {
        order_entry: millis(sim.order_entry_latency_ms),
        ack: millis(sim.ack_latency_ms),
        max_market_data_delay: millis(sim.max_market_data_delay_ms),
    }
}

fn startup_budget(
    sim: &SimConfig,
    spin_interval: DurationUs,
    inflight_timeout: DurationUs,
) -> StartupBudget {
    let slowest_heartbeat = millis(sim.heartbeat_ms).max(spin_interval);
    StartupBudget {
        latency: latency_budget(sim),
        max_heartbeat_interval: slowest_heartbeat,
        inflight_timeout,
    }
}

fn wallet_setup(row: &InstrumentRow, sim: &SimConfig) -> Result<SimWalletSetup, SimVenueError> {
    Ok(SimWalletSetup {
        base_asset: row.base_asset,
        quote_asset: row.quote_asset,
        opening_base: validated_mantissa(sim.opening_base_balance),
        opening_quote: validated_mantissa(sim.opening_quote_balance),
        // SMELL: no test reaches this conversion. The fee type counts hundred-millionths of a
        // basis point, so passing the configured number in unscaled still compiles and charges
        // almost nothing.
        maker_fee_bps: FeeBps::new(validated_mantissa(sim.maker_fee_bps))?,
    })
}

fn millis(ms: u64) -> DurationUs {
    DurationUs::from_millis(ms as i64)
}
