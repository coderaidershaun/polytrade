//! Engine assembly and lifecycle: wired in dependency order, torn down via one drain
//! under a watchdog deadline.
//!
//! Every submodule is either a PHASE of one run — `prepare`, `run`, `drain`, in that order — or the
//! bring-up of ONE subsystem, named for it: `exec`, `link`, `persist`, `metrics`, `timer`,
//! `queues`. The rest are leaves those two lean on: `error`, `exec_identity`. Which venue a run
//! talks to is decided by the adapters, never here.

mod drain;
mod error;
mod exec;
mod exec_identity;
mod link;
mod metrics;
mod persist;
mod prepare;
mod queues;
mod run;
mod timer;

use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::thread::JoinHandle;
use std::time::Duration;

use rtrb::RingBuffer;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::adapters::exec::EdgeHandle;
use crate::adapters::venue::{
    MarketDataWiring, WindowAssignment, spawn_market_data, tap_heartbeat,
};
use crate::config::{Config, ExecutionMode, RunIdentity};
use crate::exposure::{ExposureHandle, ExposureWriter, ExposureWriterConfig};
use crate::hot::dispatch::{ExposureWiring, HotEngine, HotEngineSetup, LinkWiring};
use crate::hot::metrics::MetricsSnapshot;
use crate::hot::spawn::{HotThreadConfig, SimTapGate, spawn_hot_thread};
use crate::hot::strategy::Strategy;
use crate::link::{LinkActor, LinkActorSetup, OutboundLink};
use crate::log::{self, LogConfig, LogHandle, LogRecord};
use crate::msg::persist::RotationRow;
use crate::msg::ui::{UiCatalog, UiLifecycle, UiWiring, ui_channel};
use crate::persist::PersistHandle;
use crate::shutdown::{DrainSignal, FatalSignal, RunControlGate, TaskHandle};
use crate::sink::{LinkSink, MetricsSink, StrategyLogSink};
use crate::time::{DurationUs, EngineClock};
use crate::{error, info};

use drain::install_panic_hook;
use exec::{ExecBringUp, ExecBringUpSetup, exec_bring_up};
use link::link_bring_up;
use metrics::MetricsHandle;
use persist::{PersistBringUp, PersistBringUpSetup, persist_bring_up};
use prepare::{Prepared, prepare};
use queues::{InputQueues, build_input_queues};
use timer::spawn_timer;

// SMELL: `decide_exit` and `check_symbol_order_capacity` are public only because the fitness
// suite calls them. A test is not a reason to make an item public, so their checks should move
// onto a public entry point and these two should go back to crate visibility.
pub use drain::decide_exit;
pub use error::EngineError;
pub use exec_identity::ExecutionLease;
pub use prepare::check_symbol_order_capacity;
pub use run::run_trading_engine;

const METRICS_RING_CAPACITY: usize = 64;
const ROTATIONS_CHANNEL_CAPACITY: usize = 256;

/// One rotation binding every five minutes, drained on every edge select pass; the depth exists so
/// a slow bring-up cannot lose the first window.
const ASSIGNMENTS_CHANNEL_CAPACITY: usize = 8;
const STRATEGY_LOG_RING_CAPACITY: usize = 65_536;
const LINK_RING_CAPACITY: usize = 1_024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitReport {
    pub graceful: bool,
    pub reason: Box<str>,
}

struct Engine {
    /// One trading engine per host: while this is held, no second process under the same TE
    /// identity can place. The credential names the nonce file, not the lock, so swapping keys
    /// does not buy a second engine.
    _execution_lease: Option<ExecutionLease>,
    runtime: Runtime,
    log_handle: LogHandle,
    hot: JoinHandle<()>,
    adapters: Vec<TaskHandle>,
    timer: TaskHandle,
    /// `None` when the config carries no `link:` block.
    link: Option<TaskHandle>,
    /// `None` when the config carries no `persistence:` block.
    persistence: Option<PersistHandle>,
    /// Always present, since position outlives any single run.
    exposure: ExposureHandle,
    /// `None` with execution off. Stopped first, so cancels go out before anything else stops.
    exec: Option<EdgeHandle>,
    /// Simulated venues only: the taps the venue matches against, held open across its sweep.
    exec_tap_gate: Option<Arc<SimTapGate>>,
    metrics: MetricsHandle,
    fatal: FatalSignal,
    drain: DrainSignal,
    lifecycle: SyncSender<UiLifecycle>,
    drain_deadline: Duration,
}

impl Engine {
    /// Brings everything up in dependency order: logging, registry, runtime, persistence,
    /// metrics, hot thread, timer, adapters.
    ///
    /// # Errors
    /// Invalid config, unavailable hot_core_id, runtime build failure, or failed preflights.
    fn start<P>(
        identity: RunIdentity,
        config: Config<P>,
        strategy: Box<dyn Strategy>,
    ) -> Result<Engine, EngineError> {
        // The workstation feed is built here. Its receiving end goes to the link actor, which is
        // the only reader.
        let (wiring, ui_channels) = ui_channel();
        let UiWiring {
            lifecycle,
            books: ui_book_sink,
            events: ui_event_sink,
        } = wiring;

        let mode = config.execution.as_ref().map(|execution| execution.mode);
        let log_handle = log::init(&LogConfig {
            dir: config.logging.dir.clone(),
            file_stem: ExecutionMode::artifact_stem(mode, &identity).into(),
        });
        log::register_thread("main");
        info!("polysim starting — trading engine {identity}");

        let Prepared {
            registry,
            runtime,
            exposure: restored_exposure,
            execution: execution_venue,
            rest_quiet,
        } = match prepare(&config, &identity, mode) {
            Ok(prepared) => prepared,
            Err(error) => {
                error!("startup failed before any thread spawned: {error}");
                log_handle.drain();
                return Err(error);
            }
        };
        let tokio_handle = runtime.handle().clone();

        let fatal = FatalSignal::new();
        let drain = DrainSignal::new();
        install_panic_hook(fatal.clone());

        let feature_names: Vec<Box<str>> = strategy
            .features()
            .iter()
            .map(|name| (*name).into())
            .collect();

        // The socket is bound before anything spawns, so a port already taken fails the run early.
        let bound_link =
            match link_bring_up(&config, &identity, strategy.as_ref(), &registry, &runtime) {
                Ok(bound) => bound,
                Err(error) => {
                    error!("startup failed before any thread spawned: {error}");
                    log_handle.drain();
                    return Err(error);
                }
            };
        let control = RunControlGate::new();
        let on_controller_loss = config
            .link
            .as_ref()
            .map_or_else(Default::default, |link| link.on_controller_loss);

        let (rotations_tx, rotations_rx) = mpsc::channel::<RotationRow>(ROTATIONS_CHANNEL_CAPACITY);
        // The rotation binding the execution edge trades against. Sized for a handful of windows:
        // one assignment every five minutes, and the edge drains it on every select pass.
        let (assignments_tx, assignments_rx) =
            mpsc::channel::<WindowAssignment>(ASSIGNMENTS_CHANNEL_CAPACITY);
        let PersistBringUp {
            wiring: persist_wiring,
            handle: persistence,
        } = persist_bring_up(PersistBringUpSetup {
            config: &config,
            identity: &identity,
            registry: &registry,
            feature_names: &feature_names,
            rotations: rotations_rx,
        });

        // This runs before the hot thread, so the writer knows the disk state before anything
        // changes it. `mode` names the ledger file, so it must be the same value the load was
        // given, or this run writes a ledger it did not read.
        let (exposure, exposure_sink) = ExposureWriter::spawn(
            ExposureWriterConfig::new(
                config.exposure.dir.clone(),
                identity.clone(),
                mode,
                &registry,
            ),
            &restored_exposure,
        );

        let (metrics_producer, metrics_consumer) =
            RingBuffer::<MetricsSnapshot>::new(METRICS_RING_CAPACITY);
        let metrics = {
            let _guard = tokio_handle.enter();
            MetricsHandle::spawn(metrics_consumer)
        };

        // A dedicated ring isolates a telemetry flood from engine WARN/ERROR records.
        let (strategy_log_producer, strategy_log_consumer) =
            RingBuffer::<LogRecord>::new(STRATEGY_LOG_RING_CAPACITY);
        let strategy_log_sink = StrategyLogSink::new(strategy_log_producer);
        // Leaked exactly once, since the id is only known now; log::init panics on a
        // second call, so this never repeats.
        let strategy_tag: &'static str =
            Box::leak(identity.strategy_id.as_str().to_owned().into_boxed_str());
        log::register_external_ring(
            strategy_tag,
            strategy_log_consumer,
            strategy_log_sink.drops_handle(),
        );

        let queues = build_input_queues(&registry, config.queues.input_capacity, &fatal);
        let InputQueues {
            ingress,
            groups: mut group_producers,
            timer: timer_producer,
            link: link_producer,
            exec: exec_producer,
        } = queues;

        // One clock serves every edge producer's received/queued stamps; the hot engine
        // keeps its own separate one.
        let clock = EngineClock::start();
        let drain_deadline = Duration::from_millis(config.engine.drain_deadline_ms);
        let spin_interval = config.engine.spin_interval();
        let ExecBringUp {
            wiring: exec_wiring,
            spawn: exec_spawn,
            tap_gate: exec_tap_gate,
            lease: execution_lease,
        } = match exec_bring_up(ExecBringUpSetup {
            execution: config.execution.as_ref(),
            venue: execution_venue,
            registry: &registry,
            identity: &identity,
            producer: exec_producer,
            groups: &mut group_producers,
            window_assignments: assignments_rx,
            clock: &clock,
            fatal: &fatal,
            desired_run_state: control.desired().clone(),
            drain_deadline,
            spin_interval,
            input_capacity: config.queues.input_capacity,
            lease_dir: &config.exposure.dir,
        }) {
            Ok(brought_up) => brought_up,
            Err(error) => {
                error!("startup failed before the hot thread spawned: {error}");
                log_handle.drain();
                return Err(error);
            }
        };

        let (link_wiring, link_feed) = match &bound_link {
            None => (None, None),
            Some(_) => {
                let (producer, consumer) = RingBuffer::<OutboundLink>::new(LINK_RING_CAPACITY);
                (
                    Some(LinkWiring {
                        sink: LinkSink::new(producer),
                        acknowledged: control.acknowledged().clone(),
                    }),
                    Some(consumer),
                )
            }
        };

        let metrics_sink = MetricsSink::new(metrics_producer);
        let mut hot_engine = HotEngine::new(HotEngineSetup {
            exec: exec_wiring,
            instruments: registry.instruments(),
            strategy,
            persistence: persist_wiring,
            strategy_log_sink,
            metrics_sink,
            ui_book_sink,
            ui_event_sink,
            link: link_wiring,
            exposure: ExposureWiring {
                restored: restored_exposure.instruments(),
                sink: exposure_sink,
            },
            warmup: DurationUs::from_secs(config.engine.warmup_secs as i64),
        });
        let hot = spawn_hot_thread(
            HotThreadConfig {
                core_id: config.engine.hot_core_id,
                tag: "hot",
            },
            ingress,
            fatal.clone(),
            drain.clone(),
            move |pop, message| hot_engine.dispatch(pop, &message),
        );

        let timer = spawn_timer(spin_interval, timer_producer, &clock, &tokio_handle);
        // This runs after the hot thread, so input-queue producers start only after their
        // consumer exists.
        let exec = exec_spawn.map(|spawn| spawn(&tokio_handle));

        let adapters = spawn_market_data(MarketDataWiring {
            registry: &registry,
            producers: group_producers,
            clock: &clock,
            fatal: &fatal,
            desired_run_state: control.desired(),
            rotations_tx: &rotations_tx,
            window_assignments: Some(assignments_tx),
            tap_heartbeat: tap_heartbeat(&config),
            rest_quiet,
            tokio_handle: &tokio_handle,
        });
        info!(
            "polysim up — {} adapters, spin every {} us",
            adapters.len(),
            config.engine.spin_interval_us
        );

        // The catalog carries the feature names so the monitor can label its columns. It is sent
        // with try_send, so it can never block startup.
        let catalog = UiCatalog::from_registry(
            &identity.strategy_id,
            mode,
            config.engine.spin_interval_us,
            feature_names,
            &registry,
        );
        lifecycle.try_send(UiLifecycle::Ready(catalog)).ok();

        // The link both feeds the hot thread and drains the UI rings; it starts only once
        // the sinks it needs are live.
        let link = bound_link.map(|bound| {
            LinkActor::spawn(
                LinkActorSetup {
                    socket: bound.socket,
                    identity: bound.identity,
                    guard: bound.guard,
                    peers: bound.peers,
                    on_controller_loss,
                    inbound: link_producer.expect("a bound link always builds its input queue"),
                    outbound: link_feed.expect("a bound link always builds its outbound ring"),
                    channels: ui_channels,
                    control,
                    clock: clock.clone(),
                    spin_interval,
                    topic_count: bound.topic_count,
                    feature_count: bound.feature_count,
                },
                &tokio_handle,
            )
        });

        Ok(Engine {
            _execution_lease: execution_lease,
            runtime,
            log_handle,
            hot,
            adapters,
            timer,
            link,
            persistence,
            exposure,
            exec,
            exec_tap_gate,
            metrics,
            fatal,
            drain,
            lifecycle,
            drain_deadline,
        })
    }
}
