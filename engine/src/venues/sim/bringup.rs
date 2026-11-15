//! Builds a simulated venue from the run's config: its taps, matching model, schedule, wire and
//! core, so the runtime hands over its inputs and never learns how the venue is built.

use crate::info;
use crate::msg::ids::{InstrumentId, Notional, Qty};
use crate::msg::run_state::RunState;
use crate::msg::time::DurationUs;
use crate::runtime::config::{SimProofs, validated_mantissa};
use crate::runtime::registry::{InstrumentRow, venue_of_rows};
use crate::venues::binance::exec::SymbolTable;
use crate::venues::edge::{EdgeCheck, EdgeInputs, VenueEdge};
use crate::venues::exec::{ExecCore, ExecStop};

use super::actor::SimActor;
use super::core::latency::LatencyBudget;
use super::core::resting::{InstrumentLimits, ORDER_TABLE_CAPACITY};
use super::core::schedule::{DeliveryLimits, DeliverySchedule};
use super::core::wallet::SimWalletSetup;
use super::core::{SimVenue, SimVenueSetup};
use super::driver::{SimExecDriver, SimExecDriverSetup};
use super::error::SimVenueError;
use super::lanes::{LaneBuffer, SimHorizon, SimLane};
use super::readiness::SimReadiness;
use super::tap::{MarketTapLane, MarketTaps, TapLanes, attach_market_taps};
use super::wire::VenueWire;

const ANSWER_CAPACITY: usize = 4_096;

/// What a simulated venue needs from the run, checked in preflight so its bring-up cannot refuse.
pub(crate) struct SimPlan {
    venue: SimVenue,
    instrument: InstrumentId,
    latency: LatencyBudget,
    market_inbox_capacity: usize,
    verdict_retention: DurationUs,
    worst_case: DurationUs,
    /// The orders the venue lets rest for this run, so the mirror can hold each one, as on a live
    /// venue.
    mirror_capacity: usize,
    lanes: TapLanes,
}

/// # Errors
/// [`SimVenueError`] when the run configures other than one instrument, when the registry holds no
/// Binance spot trades or depth lane to tap, when the instrument lacks a stamp the matching model
/// needs, or when the opening account cannot be spent from.
pub(crate) fn plan(check: EdgeCheck<'_>, proofs: &SimProofs) -> Result<SimPlan, SimVenueError> {
    let EdgeCheck {
        registry,
        execution,
        input_capacity,
    } = check;
    let instruments = registry.instruments();
    let [row] = instruments else {
        return Err(SimVenueError::OneInstrument {
            found: instruments.len(),
        });
    };
    let lanes = TapLanes::find(registry)?;

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
    let market_inbox_capacity = input_capacity.saturating_mul(2);
    let limits = instrument_limits(row)?;
    let mirror_capacity = row
        .max_num_orders
        .map_or(0, |limit| usize::try_from(limit).unwrap_or(usize::MAX));

    let budget = proofs.budget();
    let latency = LatencyBudget {
        order_entry: budget.order_entry(),
        ack: budget.ack(),
        max_market_data_delay: budget.max_market_data_delay(),
    };
    let venue = SimVenue::new(SimVenueSetup {
        instrument: row.instrument_id,
        book_capacity: row.book_capacity,
        market_inbox_capacity,
        verdict_retention,
        limits,
        wallet: wallet_setup(row, proofs),
    })?;
    Ok(SimPlan {
        venue,
        instrument: row.instrument_id,
        latency,
        market_inbox_capacity,
        verdict_retention,
        worst_case: budget.worst_round_trip(),
        mirror_capacity,
        lanes,
    })
}

pub(crate) fn bring_up(plan: SimPlan, inputs: EdgeInputs<'_>) -> VenueEdge {
    let EdgeInputs {
        registry,
        execution,
        identity,
        commands,
        producer,
        settings,
        groups,
        lane_capacity,
        input_capacity,
        sender: _,
    } = inputs;
    let SimPlan {
        venue,
        instrument,
        latency,
        market_inbox_capacity,
        verdict_retention,
        worst_case,
        mirror_capacity,
        lanes,
    } = plan;
    let (tapped, taps) =
        attach_market_taps(registry, input_capacity, std::mem::take(groups), lanes);
    *groups = tapped;
    info!(
        "simulated venue on {}: market inbox {market_inbox_capacity} events, order table \
         {ORDER_TABLE_CAPACITY} rows, verdict retention {}s — every one of them fatal on exhaustion",
        registry.instrument(instrument).venue_symbol,
        verdict_retention.to_secs()
    );
    info!(
        "simulated venue latencies: entry {}ms, ack {}ms, hard market-data bound {}ms",
        latency.order_entry.micros() / 1_000,
        latency.ack.micros() / 1_000,
        latency.max_market_data_delay.micros() / 1_000
    );
    info!(
        "simulated venue worst legal round trip {}ms inside a {}ms in-flight timeout",
        worst_case.micros() / 1_000,
        DurationUs::from_millis(execution.inflight_timeout_ms as i64).micros() / 1_000
    );
    let MarketTaps {
        trades,
        depth,
        gate,
    } = taps;
    let driver = SimExecDriver::new(SimExecDriverSetup {
        core: ExecCore::with_limits(settings.max_orders_per_side, mirror_capacity),
        venue,
        schedule: DeliverySchedule::new(DeliveryLimits {
            ack_latency: latency.ack,
            answer_capacity: ANSWER_CAPACITY,
        }),
        wire: VenueWire::new(identity),
        instrument,
        fatal: settings.fatal.clone(),
        inflight_timeout: settings.inflight_timeout,
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
        venue: venue_of_rows(registry.instruments()),
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
    VenueEdge {
        spawn: Box::new(move |runtime: &tokio::runtime::Handle| actor.spawn(runtime)),
        tap_gate: Some(gate),
    }
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

fn wallet_setup(row: &InstrumentRow, proofs: &SimProofs) -> SimWalletSetup {
    let sim = proofs.block();
    SimWalletSetup {
        base_asset: row.base_asset,
        quote_asset: row.quote_asset,
        opening_base: Qty::from_mantissa(validated_mantissa(sim.opening_base_balance)),
        opening_quote: Notional::from_mantissa(validated_mantissa(sim.opening_quote_balance)),
        maker_fee_bps: proofs.maker_fee(),
    }
}
