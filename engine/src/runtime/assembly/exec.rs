//! Shared bring-up for live and simulated execution edges: the venue-neutral half — the command
//! lane, the lease, the settings every edge is built from — around the venue's own bring-up.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rtrb::{Consumer, RingBuffer};
use tokio::sync::mpsc;

use crate::hot::dispatch::ExecWiring;
use crate::hot::exec::{ExecLimits, ExecSettings, FeeModel};
use crate::hot::sink::{ExecLaneBudget, ExecSink, SampleSink, exec_lane_capacity};
use crate::hot::spawn::{QueueProducer, SimTapGate};
use crate::info;
use crate::msg::exec::ExecLaneItem;
use crate::msg::ids::{Notional, Qty, QueueId, VenueId};
use crate::msg::latency::WireSample;
use crate::msg::time::{DurationUs, EngineClock, Stamper};
use crate::runtime::config::{
    DEFAULT_QUOTE_STOP_MARGIN_MS, ExecutionConfig, ExecutionMode, RunIdentity, SendersConfig,
    SourceSpec, validated_mantissa,
};
use crate::runtime::registry::Registry;
use crate::runtime::shutdown::{FatalSignal, RunStateCell};
use crate::venues::edge::backoff::BackoffCaps;
use crate::venues::edge::{
    EdgeCheck, EdgeInputs, EdgeSettings, EdgeSpawn, SenderSettings, VenueEdge,
};
use crate::venues::exec::{TeTag, VenueCapabilities};
use crate::venues::venue::{CheckedVenue, ExecutionVenue, WindowAssignment, order_client_timeout};

use super::StartError;
use super::exec_identity::ExecutionLease;

/// The cancel sweep's share of the drain deadline, as a fraction rather than a fixed
/// reserve: half goes to the sweep, half to the hot-thread join and the file drains.
const SWEEP_SHARE_OF_DRAIN: u32 = 2;

pub(super) struct ExecBringUp {
    pub(super) spawn: Option<EdgeSpawn>,
    /// Only the simulated venue feeds itself from market taps, so only it has any to hold open
    /// across its own shutdown sweep.
    pub(super) tap_gate: Option<Arc<SimTapGate>>,
    pub(super) lease: Option<ExecutionLease>,
}

impl ExecBringUp {
    fn disabled() -> Self {
        ExecBringUp {
            spawn: None,
            tap_gate: None,
            lease: None,
        }
    }
}

/// What preflight proved for an armed run: the venue, checked for its bring-up, and the lease, taken
/// last, whose run nonce the client order ids of this run are minted under.
pub(super) struct ExecPlan {
    venue: CheckedVenue,
    capabilities: VenueCapabilities,
    lease: ExecutionLease,
    queue: QueueId,
    te_tag: TeTag,
}

pub(super) struct ExecCheck<'a> {
    /// The venue that sends orders, with its execution block. `None` when no source does.
    pub(super) armed: Option<(VenueId, &'a ExecutionConfig)>,
    /// The venue the permission probe passed. `None` when execution is disarmed or absent, since the
    /// probe never ran.
    pub(super) venue: Option<ExecutionVenue>,
    pub(super) registry: &'a Registry,
    pub(super) identity: &'a RunIdentity,
    pub(super) spin_interval: DurationUs,
    pub(super) input_capacity: usize,
    pub(super) lease_dir: &'a Path,
}

/// The checks of an armed run, the lease last: taking it spends a run nonce, so each config this
/// venue cannot honour is refused before one is burnt on it.
///
/// # Errors
/// [`StartError::ExecutionNotWired`] when an armed config reaches here without the probe or the
/// input queue that arming implies, [`StartError::Venue`] when the venue refuses the config or its
/// own bring-up, and `ExecutionIdentity*` when the lease cannot be taken.
pub(super) fn check_execution(check: ExecCheck<'_>) -> Result<Option<ExecPlan>, StartError> {
    let Some((venue_id, execution)) = check.armed else {
        return Ok(None);
    };
    let mode = execution.mode.as_str();
    let Some(queue) = check.registry.exec_queue_id(venue_id) else {
        return Err(StartError::ExecutionNotWired {
            mode,
            detail: "the registry allocated no execution input queue",
        });
    };
    // Each venue's physics come from the venue's own module. Read here, where the venue is known,
    // and carried into `settings` — the hot engine stays venue-neutral and never names one.
    let venue = match execution.mode {
        ExecutionMode::Sim => ExecutionVenue::Simulated,
        ExecutionMode::Live => check.venue.ok_or(StartError::ExecutionNotWired {
            mode,
            detail: "the startup permission probe was never run",
        })?,
        ExecutionMode::Off => unreachable!("disabled execution was filtered above"),
    };
    let capabilities = venue.capabilities(check.registry)?;
    check_execution_against_venue(&capabilities, execution)?;
    let venue = venue.check(EdgeCheck {
        registry: check.registry,
        execution,
        spin_interval: check.spin_interval,
        input_capacity: check.input_capacity,
    })?;
    let te_tag = TeTag::of(check.identity);
    let lease = venue.with_lease_namespace(|namespace| {
        ExecutionLease::acquire(check.lease_dir, te_tag, namespace)
    })?;
    Ok(Some(ExecPlan {
        venue,
        capabilities,
        lease,
        queue,
        te_tag,
    }))
}

/// The order client of a live venue must give its verdict on a request before the in-flight table
/// gives up on it. Otherwise the table reads back a request whose answer is still on its way, and
/// its expiry becomes the normal path rather than the exception. Only the config is read, so a run
/// that breaks this asks no venue anything.
///
/// # Errors
/// [`StartError::ExecutionHttpTimeout`] when `execution.inflight_timeout_ms` is at or below the
/// request deadline of the order client of the venue that `source` names.
pub(super) fn check_order_client_timeout(
    source: &SourceSpec,
    execution: &ExecutionConfig,
) -> Result<(), StartError> {
    let (venue, http_timeout) = order_client_timeout(source);
    let http_timeout_ms = u64::try_from(http_timeout.as_millis()).unwrap_or(u64::MAX);
    if execution.inflight_timeout_ms > http_timeout_ms {
        return Ok(());
    }
    Err(StartError::ExecutionHttpTimeout {
        venue,
        http_timeout_ms,
        inflight_timeout_ms: execution.inflight_timeout_ms,
    })
}

/// The command lane of an armed run. Its write end goes into the engine, which the hot thread
/// builds, and its read end into the edge, which is built once the input rings of the hot thread
/// exist.
pub(super) struct CommandLane {
    pub(super) wiring: Option<ExecWiring>,
    pub(super) edge: Option<PlannedEdge>,
}

/// An edge that preflight checked, with the read end of its command lane.
pub(super) struct PlannedEdge {
    plan: ExecPlan,
    commands: Consumer<ExecLaneItem>,
    lane_capacity: usize,
}

/// Opens the command lane of an armed run, before the engine that writes it is built.
pub(super) fn open_command_lane(
    plan: Option<ExecPlan>,
    execution: Option<&ExecutionConfig>,
    spin_interval: DurationUs,
    stamper: &Stamper,
) -> CommandLane {
    let (Some(plan), Some(execution)) = (plan, execution) else {
        return CommandLane {
            wiring: None,
            edge: None,
        };
    };
    let edge_stall = edge_stall();
    let lane_capacity = command_lane_capacity(spin_interval);
    let (commands_producer, commands) = RingBuffer::<ExecLaneItem>::new(lane_capacity);
    info!(
        "execution command lane: {lane_capacity} slots ({} KiB), sized to hold a {}s edge stall at the configured {}ms spin",
        lane_capacity * size_of::<ExecLaneItem>() / 1_024,
        edge_stall.micros() / 1_000_000,
        spin_interval.micros() / 1_000
    );
    let wiring = ExecWiring {
        sink: ExecSink::new(commands_producer, stamper.clone()),
        settings: settings(execution, &plan.capabilities),
        run_nonce: plan.lease.run_nonce(),
    };
    CommandLane {
        wiring: Some(wiring),
        edge: Some(PlannedEdge {
            plan,
            commands,
            lane_capacity,
        }),
    }
}

pub(super) struct ExecBringUpSetup<'a> {
    /// `None` when execution is disarmed or absent.
    pub(super) edge: Option<PlannedEdge>,
    pub(super) execution: Option<&'a ExecutionConfig>,
    pub(super) registry: &'a Registry,
    /// The write end of the execution input queue; `None` if the registry allocated no queue.
    pub(super) producer: Option<QueueProducer>,
    /// The market-data queue producers, handed over so a venue that matches orders against the
    /// live market can tap them.
    pub(super) groups: &'a mut Vec<QueueProducer>,
    /// Rotation bindings flowing from the market-data actor to the execution edge, which only a
    /// venue that rotates markets reads.
    pub(super) window_assignments: mpsc::Receiver<WindowAssignment>,
    pub(super) clock: &'a EngineClock,
    /// The latency counter the command bank reads when a command enters it, and the sender at the
    /// pop and the wire of each command.
    pub(super) stamper: &'a Stamper,
    pub(super) wires: SampleSink<WireSample>,
    pub(super) senders: &'a SendersConfig,
    pub(super) fatal: &'a FatalSignal,
    pub(super) desired_run_state: RunStateCell,
    pub(super) drain_deadline: Duration,
    pub(super) input_capacity: usize,
}

/// Builds the edge that preflight checked: nothing here can refuse the run.
pub(super) fn exec_bring_up(setup: ExecBringUpSetup<'_>) -> ExecBringUp {
    let (Some(edge), Some(execution)) = (setup.edge, setup.execution) else {
        return ExecBringUp::disabled();
    };
    let producer = setup
        .producer
        .expect("preflight checked that the registry allocated the execution input queue");
    let PlannedEdge {
        plan:
            ExecPlan {
                venue,
                lease,
                queue,
                te_tag,
                ..
            },
        commands,
        lane_capacity,
    } = edge;
    let mode = execution.mode.as_str();
    let sweep_deadline = setup.drain_deadline / SWEEP_SHARE_OF_DRAIN;
    let described = venue.describe();
    let VenueEdge { spawn, tap_gate } = venue.bring_up(
        EdgeInputs {
            registry: setup.registry,
            execution,
            identity: lease.identity(te_tag),
            commands,
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
                inflight_timeout: inflight_timeout(execution),
            },
            groups: setup.groups,
            lane_capacity,
            input_capacity: setup.input_capacity,
            sender: SenderSettings {
                stamper: setup.stamper.clone(),
                wires: setup.wires,
                placement: setup.senders.placement,
                tick: setup.senders.tick(),
                queue,
            },
        },
        setup.window_assignments,
    );

    let run_nonce = lease.run_nonce();
    info!(
        "execution {mode} on {described}: sweep deadline {}ms inside a {}ms drain, run nonce {run_nonce:08x}",
        sweep_deadline.as_millis(),
        setup.drain_deadline.as_millis()
    );
    ExecBringUp {
        spawn: Some(spawn),
        tap_gate,
        lease: Some(lease),
    }
}

/// Settings whose meaning depends on the venue, checked once the venue is known. Config validation
/// bounds each of these on its own; only here is there anything to compare them against.
///
/// # Errors
/// [`StartError::ExecutionFieldInert`] for a setting this venue never reads, and
/// [`StartError::ExecutionBaseFloorOnPosition`] for the one that does reach it and does harm.
fn check_execution_against_venue(
    capabilities: &VenueCapabilities,
    execution: &ExecutionConfig,
) -> Result<(), StartError> {
    // Two settings are deliberately not checked here: recv_window_ms and max_clock_skew_ms bound a
    // request-signing protocol rather than anything a venue does. Each is bounded already — the
    // first where the signing edge turns it into a signed window, the second by config validation
    // for every venue — so on a venue that reads neither they are merely inert, which is accepted.
    // Giving a signing detail a capability of its own would grow VenueCapabilities every time one
    // venue gains a knob.
    if !capabilities.rotates_markets
        && execution.quote_stop_margin_ms != DEFAULT_QUOTE_STOP_MARGIN_MS
    {
        return Err(StartError::ExecutionFieldInert {
            field: "quote_stop_margin_ms",
            value: execution.quote_stop_margin_ms.to_string().into(),
            venue_fact: "this venue's instruments never expire, so there is no closing window to \
                         stop quoting ahead of",
        });
    }
    if capabilities.fee_model == FeeModel::None && execution.taker_fee_rate > 0.0 {
        return Err(StartError::ExecutionFieldInert {
            field: "taker_fee_rate",
            value: execution.taker_fee_rate.to_string().into(),
            venue_fact: "this venue takes its cut out of what a trade receives rather than \
                         charging it on top, so a flatten reserves the notional alone",
        });
    }
    if capabilities.base_asset_is_position && execution.min_base_balance > 0.0 {
        return Err(StartError::ExecutionBaseFloorOnPosition {
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
            max_order_notional_quote: Notional::from_mantissa(validated_mantissa(
                execution.max_order_notional_quote,
            )),
        },
        max_orders_per_side: execution.max_orders_per_side as usize,
        min_base_balance: Qty::from_mantissa(validated_mantissa(execution.min_base_balance)),
        min_quote_balance: Notional::from_mantissa(validated_mantissa(execution.min_quote_balance)),
        max_consecutive_rejects: execution.max_consecutive_rejects,
        max_session_loss_quote: Notional::from_mantissa(validated_mantissa(
            execution.max_session_loss_quote,
        )),

        inflight_timeout: inflight_timeout(execution),
        max_status_probes: execution.max_status_probes,
        stream_silence: DurationUs::from_millis(execution.stream_silence_ms as i64),
        max_stream_resets: execution.max_stream_resets,
        order_reap_window: DurationUs::from_secs(execution.order_reap_secs as i64),
        quote_stop_margin: DurationUs::from_millis(execution.quote_stop_margin_ms as i64),
        flatten_slack_ticks: execution.flatten_slack_ticks,
        order_budget: capabilities.order_budget,
        fee_model: capabilities.fee_model,
        taker_fee_rate: validated_mantissa(execution.taker_fee_rate),
        holds_reservations_until_settled: capabilities.holds_reservations_until_settled,
    }
}

/// One deadline for a request with no answer, read by the hot slot and by the in-flight table of
/// the edge alike.
fn inflight_timeout(execution: &ExecutionConfig) -> DurationUs {
    DurationUs::from_millis(execution.inflight_timeout_ms as i64)
}

/// An edge that cannot reach its venue waits at most the cap of its backoff before it tries again,
/// so that cap is the longest stall that the command lane must hold.
fn edge_stall() -> DurationUs {
    DurationUs::from_micros(BackoffCaps::default().max.as_micros() as i64)
}

/// The command lane holds the commands of a busy spin and a watermark for each spin of the longest
/// edge stall. The execution input queue takes the same default, so the edge can answer each
/// command that the lane holds.
pub(super) fn command_lane_capacity(spin_interval: DurationUs) -> usize {
    exec_lane_capacity(ExecLaneBudget {
        spin_interval,
        edge_stall: edge_stall(),
    })
}
