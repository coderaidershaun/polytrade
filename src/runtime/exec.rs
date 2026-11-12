//! Shared bring-up for live and simulated execution edges: the venue-neutral half — the command
//! lane, the lease, the settings every edge is built from — around the venue's own bring-up.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rtrb::RingBuffer;
use tokio::sync::mpsc;

use crate::adapters::backoff::BackoffCaps;
use crate::adapters::edge::{EdgeInputs, EdgeSettings, EdgeSpawn, VenueEdge};
use crate::adapters::exec::{TeTag, VenueCapabilities};
use crate::adapters::venue::{ExecutionVenue, WindowAssignment};
use crate::config::{
    DEFAULT_QUOTE_STOP_MARGIN_MS, ExecutionConfig, ExecutionMode, RunIdentity, validated_mantissa,
};
use crate::hot::dispatch::ExecWiring;
use crate::hot::exec::{
    ExecLaneBudget, ExecLimits, ExecSettings, FeeModel, MAX_ORDER_INSTRUMENTS, exec_lane_capacity,
};
use crate::hot::spawn::{QueueProducer, SimTapGate};
use crate::info;
use crate::msg::exec::ExecLaneItem;
use crate::registry::Registry;
use crate::shutdown::{FatalSignal, RunStateCell};
use crate::sink::ExecSink;
use crate::time::{DurationUs, EngineClock};

use super::EngineError;
use super::exec_identity::ExecutionLease;

/// The cancel sweep's share of the drain deadline, as a fraction rather than a fixed
/// reserve: half goes to the sweep, half to the hot-thread join and the file drains.
const SWEEP_SHARE_OF_DRAIN: u32 = 2;

pub(super) struct ExecBringUp {
    pub(super) wiring: Option<ExecWiring>,
    pub(super) spawn: Option<EdgeSpawn>,
    /// Only the simulated venue feeds itself from market taps, so only it has any to hold open
    /// across its own shutdown sweep.
    pub(super) tap_gate: Option<Arc<SimTapGate>>,
    pub(super) lease: Option<ExecutionLease>,
}

impl ExecBringUp {
    fn disabled() -> Self {
        ExecBringUp {
            wiring: None,
            spawn: None,
            tap_gate: None,
            lease: None,
        }
    }
}

pub(super) struct ExecBringUpSetup<'a> {
    pub(super) execution: Option<&'a ExecutionConfig>,
    /// The venue the startup gate probed and passed. `None` when execution is disarmed or absent,
    /// since the probe never ran.
    pub(super) venue: Option<ExecutionVenue>,
    pub(super) registry: &'a Registry,
    pub(super) identity: &'a RunIdentity,
    /// The write end of the execution input queue; `None` if the registry allocated no queue.
    pub(super) producer: Option<QueueProducer>,
    /// The market-data queue producers, handed over so a venue that matches orders against the
    /// live market can tap them.
    pub(super) groups: &'a mut Vec<QueueProducer>,
    /// Rotation bindings flowing from the market-data actor to the execution edge, which only a
    /// venue that rotates markets reads.
    pub(super) window_assignments: mpsc::Receiver<WindowAssignment>,
    pub(super) clock: &'a EngineClock,
    pub(super) fatal: &'a FatalSignal,
    pub(super) desired_run_state: RunStateCell,
    pub(super) drain_deadline: Duration,
    pub(super) spin_interval: DurationUs,
    pub(super) input_capacity: usize,
    pub(super) lease_dir: &'a Path,
}

/// # Errors
/// [`EngineError::ExecutionNotWired`] when an armed config reaches here without the probe or the
/// input queue that arming implies, [`EngineError::ExecutionTooManyInstruments`] when the order
/// table cannot address every row, `ExecutionIdentity*` when lease acquisition fails, and
/// [`EngineError::Venue`] when the venue refuses the config or its own bring-up.
pub(super) fn exec_bring_up(mut setup: ExecBringUpSetup<'_>) -> Result<ExecBringUp, EngineError> {
    let Some(execution) = setup.execution.filter(|config| config.mode.is_enabled()) else {
        return Ok(ExecBringUp::disabled());
    };
    let mode = execution.mode.as_str();
    let Some(producer) = setup.producer.take() else {
        return Err(EngineError::ExecutionNotWired {
            mode,
            detail: "the registry allocated no execution input queue",
        });
    };
    let instruments = setup.registry.instruments();
    if instruments.len() > MAX_ORDER_INSTRUMENTS {
        return Err(EngineError::ExecutionTooManyInstruments {
            found: instruments.len(),
            max: MAX_ORDER_INSTRUMENTS,
        });
    }

    let te_tag = TeTag::of(setup.identity);
    let edge_stall = DurationUs::from_micros(BackoffCaps::default().max.as_micros() as i64);
    let lane_capacity = exec_lane_capacity(ExecLaneBudget {
        spin_interval: setup.spin_interval,
        edge_stall,
    });
    let (commands_producer, commands_consumer) = RingBuffer::<ExecLaneItem>::new(lane_capacity);
    info!(
        "execution command lane: {lane_capacity} slots ({} KiB), sized to hold a {}s edge stall at the configured {}ms spin",
        lane_capacity * size_of::<ExecLaneItem>() / 1_024,
        edge_stall.micros() / 1_000_000,
        setup.spin_interval.micros() / 1_000
    );
    let sweep_deadline = setup.drain_deadline / SWEEP_SHARE_OF_DRAIN;

    // Each venue's physics come from the venue's own module. Read here, where the venue is known,
    // and carried into `settings` — the hot engine stays venue-neutral and never names one.
    let venue = match execution.mode {
        ExecutionMode::Sim => ExecutionVenue::Simulated,
        ExecutionMode::Live => setup.venue.take().ok_or(EngineError::ExecutionNotWired {
            mode,
            detail: "the startup permission probe was never run",
        })?,
        ExecutionMode::Off => unreachable!("disabled execution was filtered above"),
    };
    let capabilities = venue.capabilities(setup.registry)?;
    // Acquiring the lease spends a run nonce that client order ids are minted under, so a config
    // this venue cannot honour is refused before one is burnt on it.
    check_execution_against_venue(&capabilities, execution)?;
    let lease = venue.with_lease_namespace(|namespace| {
        ExecutionLease::acquire(setup.lease_dir, te_tag, namespace)
    })?;
    let described = venue.describe();
    let VenueEdge { spawn, tap_gate } = venue.bring_up(
        EdgeInputs {
            registry: setup.registry,
            execution,
            identity: lease.identity(te_tag),
            commands: commands_consumer,
            producer,
            settings: EdgeSettings {
                clock: setup.clock.clone(),
                fatal: setup.fatal.clone(),
                run_state: setup.desired_run_state.clone(),
                backoff: BackoffCaps::default(),
                sweep_deadline,
                disconnect_sweep_after: DurationUs::from_secs(
                    execution.disconnect_sweep_secs as i64,
                ),
                max_orders_per_side: execution.max_orders_per_side as usize,
            },
            groups: setup.groups,
            lane_capacity,
            spin_interval: setup.spin_interval,
            input_capacity: setup.input_capacity,
        },
        setup.window_assignments,
    )?;

    let run_nonce = lease.run_nonce();
    info!(
        "execution {mode} on {described}: sweep deadline {}ms inside a {}ms drain, run nonce {run_nonce:08x}",
        sweep_deadline.as_millis(),
        setup.drain_deadline.as_millis()
    );
    Ok(ExecBringUp {
        wiring: Some(ExecWiring {
            sink: ExecSink::new(commands_producer),
            settings: settings(execution, &capabilities),
            run_nonce,
        }),
        spawn: Some(spawn),
        tap_gate,
        lease: Some(lease),
    })
}

/// Settings whose meaning depends on the venue, checked once the venue is known. Config validation
/// bounds each of these on its own; only here is there anything to compare them against.
///
/// # Errors
/// [`EngineError::ExecutionFieldInert`] for a setting this venue never reads, and
/// [`EngineError::ExecutionBaseFloorOnPosition`] for the one that does reach it and does harm.
fn check_execution_against_venue(
    capabilities: &VenueCapabilities,
    execution: &ExecutionConfig,
) -> Result<(), EngineError> {
    // Two settings are deliberately not checked here: recv_window_ms and max_clock_skew_ms bound a
    // request-signing protocol rather than anything a venue does. Each is bounded already — the
    // first where the signing edge turns it into a signed window, the second by config validation
    // for every venue — so on a venue that reads neither they are merely inert, which is accepted.
    // Giving a signing detail a capability of its own would grow VenueCapabilities every time one
    // venue gains a knob.
    if !capabilities.rotates_markets
        && execution.quote_stop_margin_ms != DEFAULT_QUOTE_STOP_MARGIN_MS
    {
        return Err(EngineError::ExecutionFieldInert {
            field: "quote_stop_margin_ms",
            value: execution.quote_stop_margin_ms.to_string().into(),
            venue_fact: "this venue's instruments never expire, so there is no closing window to \
                         stop quoting ahead of",
        });
    }
    if capabilities.fee_model == FeeModel::None && execution.taker_fee_rate > 0.0 {
        return Err(EngineError::ExecutionFieldInert {
            field: "taker_fee_rate",
            value: execution.taker_fee_rate.to_string().into(),
            venue_fact: "this venue takes its cut out of what a trade receives rather than \
                         charging it on top, so a flatten reserves the notional alone",
        });
    }
    if capabilities.base_asset_is_position && execution.min_base_balance > 0.0 {
        return Err(EngineError::ExecutionBaseFloorOnPosition {
            value: execution.min_base_balance.to_string().into(),
        });
    }
    Ok(())
}

fn settings(execution: &ExecutionConfig, capabilities: &VenueCapabilities) -> ExecSettings {
    ExecSettings {
        limits: ExecLimits {
            requote_threshold_ticks: execution.requote_threshold_ticks,
            // Expressed in hundredths of a basis point, so the band compares as integers —
            // comparing as floats would risk both a spurious violation and a determinism hazard.
            max_quote_distance_centi_bps: (execution.max_quote_distance_bps * 100.0).round() as i64,
            max_book_age: DurationUs::from_millis(execution.max_book_age_ms as i64),
            max_order_notional_quote: validated_mantissa(execution.max_order_notional_quote),
        },
        max_orders_per_side: execution.max_orders_per_side as usize,
        min_base_balance: validated_mantissa(execution.min_base_balance),
        min_quote_balance: validated_mantissa(execution.min_quote_balance),
        max_consecutive_rejects: execution.max_consecutive_rejects,
        max_session_loss_quote: validated_mantissa(execution.max_session_loss_quote),
        inflight_timeout: DurationUs::from_millis(execution.inflight_timeout_ms as i64),
        exec_silence_spins: execution.exec_silence_spins,
        order_reap_window: DurationUs::from_secs(execution.order_reap_secs as i64),
        quote_stop_margin: DurationUs::from_millis(execution.quote_stop_margin_ms as i64),
        flatten_slack_ticks: execution.flatten_slack_ticks,
        order_budget: capabilities.order_budget,
        fee_model: capabilities.fee_model,
        taker_fee_rate: validated_mantissa(execution.taker_fee_rate),
        holds_reservations_until_settled: capabilities.holds_reservations_until_settled,
    }
}
