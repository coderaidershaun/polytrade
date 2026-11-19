//! Engine assembly and lifecycle: wired in dependency order, torn down via one drain
//! under a watchdog deadline.
//!
//! Every submodule is a PHASE of one run (`preflight`, `run`, `drain`), the bring-up of ONE subsystem
//! (`hot`, `exec`, `link`, `persist`, `metrics`, `senders`, `timer`, `queues`), `fold`, the same build
//! on one thread for a re-fold, or a leaf (`error`, `exec_identity`, `exit`). Which venue a run talks
//! to is decided by the adapters, never here.

mod drain;
mod error;
mod exec;
mod exec_identity;
mod exit;
pub(super) mod fold;
mod hot;
mod link;
pub mod metrics;
mod persist;
mod preflight;
mod queues;
mod run;
mod senders;
mod timer;

use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::hot::dispatch::{ExposureWiring, HotEngine, HotEngineSetup};
use crate::hot::metrics::DriverSamples;
use crate::hot::sink::{StrategyLogSink, UiBookSink, UiEventSink};
use crate::hot::spawn::{HotThreadConfig, SimTapGate, spawn_hot_thread};
use crate::hot::strategy::Strategy;
use crate::link::LinkCountsHandle;
use crate::log::{self, LaneReader, LogConfig, LogHandle};
use crate::msg::persist::RotationRow;
use crate::msg::time::{ClockSource, DurationUs, EngineClock, InvariantCounter, Stamper};
use crate::msg::ui::UiCatalog;
use crate::msg::ui::channel::{UiLifecycle, UiWiring, ui_channel};
use crate::runtime::config::{Config, SenderPlacement};
use crate::runtime::exposure::{ExposureHandle, ExposureWriter, ExposureWriterSetup, WriterStatus};
#[cfg(feature = "otlp")]
use crate::runtime::otlp::{OtlpExporter, OtlpLanes, OtlpPlan};
use crate::runtime::pin::PinCall;
use crate::runtime::shutdown::{DrainSignal, FatalSignal, RunStateHandles, TaskHandle};
use crate::tape::{PersistHandle, TapeStatus};
use crate::venues::exec::EdgeHandle;
use crate::venues::venue::{MarketDataWiring, WindowAssignment, spawn_market_data, tap_heartbeat};
use crate::{info, warn};

use drain::install_panic_hook;
pub use drain::{DrainWatchdog, ForcedExit};
pub use error::StartError;
use error::refuse;
#[cfg(feature = "otlp")]
pub(crate) use exec::SWEEP_SHARE_OF_DRAIN;
use exec::{CommandLane, ExecBringUp, ExecBringUpSetup, exec_bring_up, open_command_lane};
pub use exec_identity::{ExecutionLease, IdentityLock, IdentityLocks};
pub use exit::{DrainDeadline, DrainFailure, DrainOutcome, Exit, Trigger};
use hot::{HotBringUp, HotBringUpSetup, hot_bring_up};
use link::LinkStart;
use metrics::{
    MetricsBringUp, MetricsBringUpSetup, MetricsThread, RollupOutputs, SampleProducers,
    metrics_bring_up,
};
use persist::{
    OpenRun, PersistBringUp, PersistBringUpSetup, open_run, persist_bring_up, rollup_ring_of,
};
pub use preflight::{Preflight, StartPlan};
use queues::{InputProducers, RingPlan};
use run::build_runtime;
pub use run::run_trading_engine;
pub use senders::PinnedSenders;
use timer::spawn_timer;

const ROTATIONS_CHANNEL_CAPACITY: usize = 256;

pub struct Engine {
    /// One trading engine per host: while this is held, no second process under the same TE
    /// identity can start. The credential names the nonce file, not the lock, so swapping keys
    /// does not buy a second engine.
    _identity_lock: IdentityLock,
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
    /// The manifest of the run, closed after the drain. `None` with no `persistence:` block.
    run: Option<OpenRun>,
    /// Always present, since position outlives any single run.
    exposure: ExposureHandle,
    /// `None` with execution off. Stopped first, so cancels go out before anything else stops.
    exec: Option<EdgeHandle>,
    /// Simulated venues only: the taps the venue matches against, held open across its sweep.
    exec_tap_gate: Option<Arc<SimTapGate>>,
    /// `None` unless `senders.placement` is pinned. Stopped after the edges and the link it runs.
    senders: Option<PinnedSenders>,
    metrics: MetricsThread,
    #[cfg(feature = "otlp")]
    otlp: Option<OtlpExporter>,
    fatal: FatalSignal,
    drain: DrainSignal,
    lifecycle: SyncSender<UiLifecycle>,
    drain_deadline: Duration,
}

impl Engine {
    /// Starts what `plan` checked. Each thread that can refuse its pin starts first, in this order:
    /// the hot thread, the log, the metrics thread, the pinned senders' thread. Then the runtime
    /// and the writers start, the hot thread builds its rings and its engine on its own core, and
    /// only then do the timer, the edges and the link start. Each pin goes through `pin`.
    ///
    /// # Errors
    /// [`StartError::HotPin`], [`StartError::HousekeepingPin`] or [`StartError::SenderPin`] for a
    /// thread that cannot pin on Linux, and [`StartError::Runtime`] for a runtime that cannot be
    /// built. Each thread that started before the refusal has stopped when this returns, and no
    /// writer has started. [`StartError::Manifest`] when the run folder cannot be opened.
    #[expect(
        clippy::disallowed_methods,
        reason = "the one clock of the process, started before any producer of message stamps"
    )]
    pub fn start<P>(
        config: Config<P>,
        strategy: Box<dyn Strategy>,
        plan: StartPlan,
        pin: PinCall,
    ) -> Result<Engine, StartError> {
        let hot_thread = spawn_hot_thread(
            HotThreadConfig {
                core_id: config.engine.hot_core_id,
                tag: "hot",
            },
            pin,
        )
        .map_err(|refused| StartError::HotPin {
            core: refused.core_id,
        })?;

        // The one clock of the process: every producer of message stamps reads it.
        let clock = EngineClock::start();
        let latency_clock = ClockSource::check(InvariantCounter::detect());
        // The one counter of the process: every latency stamp reads it. Building it calibrates it.
        let stamper = Stamper::Counter(quanta::Clock::new());

        // The workstation feed is built here. Its receiving end goes to the link actor, which is
        // the only reader.
        let (wiring, ui_channels) = ui_channel();
        let UiWiring {
            lifecycle,
            books,
            events,
        } = wiring;
        let ui_book_sink = UiBookSink::new(books);
        let ui_event_sink = UiEventSink::new(events);

        let identity = plan.identity.clone();
        let artifacts = plan.artifacts.clone();
        let log_config = LogConfig {
            dir: config.logging.dir.clone(),
            file_stem: artifacts.stem().into(),
            ring_capacity: config.logging.ring_capacity,
            drain_deadline: Duration::from_millis(config.logging.drain_deadline_ms),
            reopen_limit: config.logging.reopen_limit,
            core_id: config.engine.housekeeping_core_id,
        };
        let log_handle = match log::init(&log_config, &clock, pin) {
            Ok(handle) => handle,
            Err(refused) => {
                hot_thread.abandon();
                return Err(StartError::HousekeepingPin {
                    core_id: refused.core_id,
                    thread: "log",
                });
            }
        };
        #[cfg(feature = "otlp")]
        let (otlp_rows, otlp) = plan.otlp.as_ref().map(OtlpPlan::open).unzip();
        let (tape_rows, rollup_rows) = rollup_ring_of(&config);
        log::register_thread("main");
        info!("polysim starting — trading engine {identity}");
        match latency_clock {
            Ok(source) => info!("latency stamps read the {} counter", source.label()),
            Err(refusal) => warn!("{refusal}"),
        }
        if config.engine.hot_core_id.is_some() && !cfg!(target_os = "linux") {
            warn!(
                "cpu pinning is best-effort on {} — hot thread may float",
                std::env::consts::OS
            );
        }
        plan.log_report();

        let link_counts = LinkCountsHandle::new();
        let MetricsBringUp {
            thread: metrics,
            samples,
            reports: latency_reports,
        } = match metrics_bring_up(MetricsBringUpSetup {
            latency: &config.latency,
            clock: &clock,
            stamper: &stamper,
            clock_source: latency_clock.unwrap_or_else(|refusal| refusal.fallback()),
            queue_count: plan.registry.input_queue_count(),
            core_id: config.engine.housekeeping_core_id,
            pin,
            link_counts: config.link.is_some().then(|| link_counts.clone()),
            outputs: RollupOutputs {
                tape: tape_rows,
                #[cfg(feature = "otlp")]
                otlp: otlp_rows,
            },
        }) {
            Ok(brought_up) => brought_up,
            Err(error) => return Err(refuse(error, log_handle, || hot_thread.abandon())),
        };
        let senders = match config.senders.placement {
            SenderPlacement::Shared => None,
            SenderPlacement::Pinned => match PinnedSenders::spawn(config.senders.core_id, pin) {
                Ok(senders) => Some(senders),
                Err(error) => {
                    return Err(refuse(error, log_handle, || {
                        metrics.stop();
                        hot_thread.abandon();
                    }));
                }
            },
        };
        // The manifest opens last, so no run folder holds a run whose start was refused.
        let built = build_runtime(config.engine.tokio_workers)
            .and_then(|runtime| Ok((runtime, open_run(&config, &plan)?)));
        let (runtime, run) = match built {
            Ok(built) => built,
            Err(error) => {
                return Err(refuse(error, log_handle, || {
                    drop(senders);
                    metrics.stop();
                    hot_thread.abandon();
                }));
            }
        };
        let tokio_handle = runtime.handle().clone();
        let edge_runtime = senders
            .as_ref()
            .map_or_else(|| tokio_handle.clone(), |senders| senders.handle().clone());

        let StartPlan {
            identity_lock,
            registry,
            exposure: restored_exposure,
            rest_quiet,
            link: bound_link,
            execution: execution_plan,
            ..
        } = plan;
        let fatal = FatalSignal::new();
        let drain = DrainSignal::new();
        install_panic_hook(fatal.clone());

        let feature_names: Vec<Box<str>> = strategy
            .features()
            .iter()
            .map(|name| (*name).into())
            .collect();

        let RunStateHandles {
            desired_writer,
            desired_reader,
            acknowledged_writer,
        } = RunStateHandles::new();
        let (rotations_tx, rotations_rx) = mpsc::channel::<RotationRow>(ROTATIONS_CHANNEL_CAPACITY);
        // The rotation binding the execution edge trades against. Sized for a handful of windows:
        // one assignment every five minutes, and the edge drains it on every select pass.
        let (assignments_tx, assignments_rx) =
            mpsc::channel::<WindowAssignment>(exec::ASSIGNMENTS_CHANNEL_CAPACITY);
        let PersistBringUp {
            wiring: persist_wiring,
            tee,
            tape,
        } = persist_bring_up(PersistBringUpSetup {
            config: &config,
            identity: &identity,
            artifacts: &artifacts,
            registry: &registry,
            strategy: strategy.as_ref(),
            run: run.as_ref(),
            rollups: rollup_rows,
            rotations: rotations_rx,
        });

        let SampleProducers {
            stages,
            counters,
            wires,
        } = samples;

        // A dedicated ring isolates a telemetry flood from engine WARN/ERROR records.
        let (strategy_log_producer, strategy_log_consumer) =
            log::hot_ring(config.logging.ring_capacity);
        let strategy_log_sink = StrategyLogSink::new(strategy_log_producer);
        // Leaked exactly once, since the id is only known now; log::init panics on a
        // second call, so this never repeats.
        let strategy_tag: &'static str =
            Box::leak(identity.strategy_id.as_str().to_owned().into_boxed_str());
        log::register_external_ring(
            strategy_tag,
            LaneReader::new(strategy_log_consumer, strategy_log_sink.drops_handle()),
        );

        let drain_deadline = Duration::from_millis(config.engine.drain_deadline_ms);
        let spin_interval = config.engine.spin_interval();
        let armed = config
            .armed_sources()
            .next()
            .map(|(.., execution)| execution);
        let CommandLane {
            wiring: exec_wiring,
            edge: planned_edge,
        } = open_command_lane(execution_plan, armed, spin_interval, &stamper);

        let (link_wiring, link_feed) = link::outbound_ring(&bound_link, acknowledged_writer);

        let instruments = registry.instruments().to_vec();
        let restored = restored_exposure.instruments().to_vec();
        let (exposure_sink, exposure_lane) = ExposureWriter::lane(instruments.len());
        let warmup = DurationUs::from_secs(config.engine.warmup_secs as i64);
        let ledger_interval = config.ledger_interval_spins();
        let HotBringUp {
            thread: hot,
            producers,
        } = hot_bring_up(HotBringUpSetup {
            thread: hot_thread,
            rings: RingPlan::of(&registry, &config),
            engine: move |audit_status| {
                HotEngine::new(HotEngineSetup {
                    exec: exec_wiring.into_iter().collect(),
                    instruments: &instruments,
                    strategy,
                    persistence: persist_wiring
                        .map(|wiring| wiring.reporting_bank_full_to(audit_status)),
                    strategy_log_sink,
                    ui_book_sink,
                    ui_event_sink,
                    link: link_wiring,
                    exposure: ExposureWiring {
                        restored: &restored,
                        sink: exposure_sink,
                    },
                    warmup,
                })
                .writing_ledger_every(ledger_interval)
            },
            samples: DriverSamples {
                stamper: stamper.clone(),
                stages,
                counters,
            },
            tape: tee,
            config: &config.engine,
            fatal: &fatal,
            drain: &drain,
        });
        let InputProducers {
            groups: mut group_producers,
            timer: timer_producer,
            link: link_producer,
            exec: exec_producer,
            writer_status,
            control: control_producer,
            tape_status,
            audit_status: _,
        } = producers;
        let persistence = tape.map(|tape| {
            tape.spawn(tape_status.map(|queue| TapeStatus {
                queue,
                clock: clock.clone(),
            }))
        });
        let exposure = ExposureWriter::spawn(ExposureWriterSetup {
            dir: config.exposure.dir.clone(),
            identity: identity.clone(),
            artifacts: artifacts.clone(),
            registry: &registry,
            status: writer_status.map(|queue| WriterStatus {
                queue,
                clock: clock.clone(),
                max_failed_writes: config.exposure.max_failed_writes,
            }),
            restored: &restored_exposure,
            lane: exposure_lane,
        });

        let ExecBringUp {
            spawn: exec_spawn,
            tap_gate: exec_tap_gate,
            lease: execution_lease,
        } = exec_bring_up(ExecBringUpSetup {
            edge: planned_edge,
            execution: armed,
            registry: &registry,
            producer: exec_producer,
            groups: &mut group_producers,
            window_assignments: assignments_rx,
            clock: &clock,
            stamper: &stamper,
            wires,
            senders: &config.senders,
            fatal: &fatal,
            desired_run_state: desired_reader.clone(),
            drain_deadline,
            input_capacity: config.queues.input_capacity,
        });

        let timer = spawn_timer(spin_interval, timer_producer, &clock, &tokio_handle);
        // Each producer of an input queue starts only now, after the hot thread that reads it.
        let exec = exec_spawn.map(|spawn| spawn(&edge_runtime));

        let adapters = spawn_market_data(MarketDataWiring {
            registry: &registry,
            producers: group_producers,
            clock: &clock,
            fatal: &fatal,
            desired_run_state: &desired_reader,
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
            artifacts.mode(),
            config.engine.spin_interval_us,
            feature_names,
            &registry,
        );
        lifecycle.try_send(UiLifecycle::Ready(catalog)).ok();

        // The link both feeds the hot thread and drains the UI rings; it starts only once
        // the sinks it needs are live.
        let link = bound_link.map(|bound| {
            link::spawn_link_actor(
                bound,
                LinkStart {
                    config: &config,
                    inbound: link_producer,
                    markers: control_producer,
                    outbound: link_feed,
                    latency: latency_reports,
                    channels: ui_channels,
                    control: desired_writer,
                    clock: clock.clone(),
                    spin_interval,
                    counts: link_counts,
                },
                &edge_runtime,
            )
        });

        Ok(Engine {
            _identity_lock: identity_lock,
            _execution_lease: execution_lease,
            runtime,
            log_handle,
            hot,
            adapters,
            timer,
            link,
            persistence,
            run,
            exposure,
            exec,
            exec_tap_gate,
            senders,
            metrics,
            #[cfg(feature = "otlp")]
            otlp: otlp.and_then(OtlpLanes::start),
            fatal,
            drain,
            lifecycle,
            drain_deadline,
        })
    }
}
