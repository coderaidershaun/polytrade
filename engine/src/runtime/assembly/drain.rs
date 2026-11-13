//! Shutdown and drain: waits for a signal or a fatal trip, tears everything down under a
//! watchdog, catches panics, and decides the process exit.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tokio::signal::unix::{SignalKind, signal};

use crate::hot::spawn::SimTapGate;
use crate::log::LogHandle;
use crate::msg::ui::channel::UiLifecycle;
use crate::runtime::shutdown::{FatalReason, FatalSignal, TaskHandle, panic_text};
use crate::venues::exec::{EdgeHandle, SweepFailure};
use crate::{error, info, warn};

use super::Engine;
use super::StartError;
use super::exit::{DrainDeadline, DrainFailure, DrainOutcome, Exit, Trigger};

/// How often the fatal latch is polled, since it cannot be awaited directly.
const LATCH_POLL: Duration = Duration::from_millis(20);

impl Engine {
    /// Blocks until a signal or a fatal trip, then drains under a hard deadline enforced by a
    /// watchdog.
    pub(super) fn run_until_shutdown(self, forced_exit: ForcedExit) -> Exit {
        let Engine {
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
            senders,
            metrics,
            fatal,
            drain,
            lifecycle,
            drain_deadline,
        } = self;

        let trigger = runtime.block_on(wait_for_shutdown(&fatal));
        match trigger {
            Trigger::Request(name) => info!("received {name} — draining"),
            Trigger::Fatal(reason) => error!("fatal: {reason} — draining"),
        }
        // Uses try_send only, so a dead receiver never blocks the drain.
        lifecycle.try_send(UiLifecycle::Draining).ok();

        let watchdog = DrainWatchdog::arm(drain_deadline, forced_exit);

        let (sweep_result, exposure_result, persist_result) = runtime.block_on(async {
            let sweep = stop_edge_producers(exec, exec_tap_gate, timer, adapters, link).await;
            drain.request();
            join_hot(hot).await;
            // Position is drained before research data: closing Parquet takes time, and
            // running out of it still leaves a recoverable position.
            let exposure = exposure.drain().await;
            let persist = match persistence {
                Some(handle) => handle.drain().await,
                None => Ok(()),
            };
            (sweep, exposure, persist)
        });
        // Each edge and the link actor that ran on the pinned senders' thread has stopped above.
        drop(senders);
        metrics.stop();
        // In the order the drain ran them, which is the order they name the exit: orders left on a
        // venue outlive the process, and position is what the next run inherits.
        let failures = [
            report_drain_failure("execution sweep", sweep_result).map(DrainFailure::Sweep),
            report_drain_failure("exposure", exposure_result).map(|error| DrainFailure::Exposure {
                cause: error.to_string().into(),
            }),
            report_drain_failure("persistence", persist_result).map(|error| {
                DrainFailure::Persistence {
                    cause: error.to_string().into(),
                }
            }),
        ];

        // The fatal latch is re-read after the drain, since a hot-thread panic during it
        // must override an otherwise graceful shutdown.
        let exit = Exit::decide(DrainOutcome {
            trigger,
            fatal: fatal.reason(),
            failures: failures.into_iter().flatten().collect(),
            deadline: watchdog.deadline(),
        });

        info!(
            "drain complete ({})",
            if exit.code() == 0 { "graceful" } else { "fatal" }
        );
        log_handle.drain();
        watchdog.disarm();
        // Released only after every order-related component has stopped, so a replacement
        // process cannot start while this one is still draining.
        drop(execution_lease);
        exit
    }
}

pub(super) fn install_panic_hook(fatal: FatalSignal) {
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|loc| format!("{}:{}", loc.file(), loc.line()))
            .unwrap_or_else(|| "unknown location".to_owned());
        let message = panic_text(info.payload());
        error!("thread panicked at {location}: {message}");
        fatal.trip(FatalReason::Panic);
    }));
}

async fn wait_for_shutdown(fatal: &FatalSignal) -> Trigger {
    tokio::select! {
        () = wait_sigint() => Trigger::Request("SIGINT"),
        () = wait_sigterm() => Trigger::Request("SIGTERM"),
        reason = poll_fatal(fatal) => Trigger::Fatal(reason),
    }
}

async fn wait_sigint() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!("could not install SIGINT handler: {error} — SIGTERM and fatal still observed");
        never().await;
    }
}

async fn wait_sigterm() {
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(sigterm) => sigterm,
        Err(error) => {
            error!("could not install SIGTERM handler: {error} — SIGINT and fatal still observed");
            return never().await;
        }
    };
    if sigterm.recv().await.is_none() {
        error!("SIGTERM stream closed — SIGINT and fatal still observed");
        never().await;
    }
}

async fn never() {
    std::future::pending::<()>().await;
}

/// Waits for the reason rather than the flag: the latch sets the reason first, so a set flag
/// always comes with one.
async fn poll_fatal(fatal: &FatalSignal) -> FatalReason {
    loop {
        if let Some(reason) = fatal.reason() {
            return reason;
        }
        tokio::time::sleep(LATCH_POLL).await;
    }
}

/// Stops producers in dependency order, and hands back how the exit sweep of the execution edge
/// ended.
async fn stop_edge_producers(
    exec: Option<EdgeHandle>,
    exec_tap_gate: Option<Arc<SimTapGate>>,
    timer: TaskHandle,
    adapters: Vec<TaskHandle>,
    link: Option<TaskHandle>,
) -> Result<(), SweepFailure> {
    timer.shutdown().await;
    let swept = match exec {
        Some(exec) => stop_execution(exec, exec_tap_gate).await,
        None => Ok(()),
    };
    if let Some(link) = link {
        link.shutdown().await;
    }
    for adapter in adapters {
        adapter.shutdown().await;
    }
    swept
}

/// A gate is present only under a simulated venue. The taps are that venue's own market feed, so
/// they stay open across its forced sweep and close only once nothing is left to drain them.
async fn stop_execution(
    exec: EdgeHandle,
    tap_gate: Option<Arc<SimTapGate>>,
) -> Result<(), SweepFailure> {
    let Some(gate) = tap_gate else {
        return exec.shutdown().await;
    };
    gate.begin_sweep();
    let swept = exec.shutdown().await;
    gate.disable();
    swept
}

async fn join_hot(hot: JoinHandle<()>) {
    let joined = tokio::task::spawn_blocking(move || {
        if hot.join().is_err() {
            warn!("hot thread ended by panic — cause already logged");
        }
    })
    .await;
    // A JoinError here can only mean the shim itself panicked, which in practice is unreachable.
    if let Err(join_error) = joined
        && join_error.is_panic()
    {
        warn!("hot-thread join shim panicked — cause already logged");
    }
}

/// The function that the binary gives to end the process when the drain passes its deadline. A
/// binary gives the exit of the standard library, which never returns.
pub type ForcedExit = fn(i32);

/// Ends the process through the forced exit when the drain is not done by its deadline, so a hung
/// step can never keep a halted engine alive.
pub struct DrainWatchdog {
    disarm: mpsc::Sender<()>,
    thread: JoinHandle<()>,
    has_fired: Arc<AtomicBool>,
}

impl DrainWatchdog {
    /// # Panics
    /// If the OS cannot start a thread at all.
    pub fn arm(deadline: Duration, forced_exit: ForcedExit) -> Self {
        let (disarm, armed) = mpsc::channel();
        let has_fired = Arc::new(AtomicBool::new(false));
        let thread_has_fired = Arc::clone(&has_fired);
        let thread = thread::Builder::new()
            .name("polysim-drain-watchdog".to_owned())
            .spawn(move || {
                if armed.recv_timeout(deadline) != Err(RecvTimeoutError::Timeout) {
                    return;
                }
                thread_has_fired.store(true, Ordering::Release);
                // A forced exit never lets the log drain flush, so the cause also goes to stderr.
                eprintln!(
                    "polysim: drain deadline {} ms exceeded — forcing exit",
                    deadline.as_millis()
                );
                error!(
                    "drain deadline {} ms exceeded — forcing exit",
                    deadline.as_millis()
                );
                forced_exit(i32::from(Exit::DeadlinePassed.code()));
            })
            .unwrap_or_else(|error| panic!("failed to spawn the drain watchdog thread: {error}"));
        Self {
            disarm,
            thread,
            has_fired,
        }
    }

    pub fn deadline(&self) -> DrainDeadline {
        match self.has_fired.load(Ordering::Acquire) {
            true => DrainDeadline::Passed,
            false => DrainDeadline::Kept,
        }
    }

    /// Wakes the watchdog and waits for its thread to end, so it can never fire afterwards.
    pub fn disarm(self) {
        let DrainWatchdog { disarm, thread, .. } = self;
        drop(disarm);
        if thread.join().is_err() {
            warn!("the drain watchdog ended in a panic, which the panic hook has already reported");
        }
    }
}

fn report_drain_failure<E: core::fmt::Display>(what: &str, result: Result<(), E>) -> Option<E> {
    let Err(error) = result else { return None };
    error!("{what} drain failed: {error}");
    Some(error)
}

/// A refusal after the log started: each thread that started before it stops first, so the log
/// waits for no lane of a running thread, then the log drains.
#[cold]
pub(super) fn refuse(
    error: StartError,
    log_handle: LogHandle,
    stop_started: impl FnOnce(),
) -> StartError {
    error!("startup failed after the hot thread pinned: {error}");
    stop_started();
    log_handle.drain();
    error
}
