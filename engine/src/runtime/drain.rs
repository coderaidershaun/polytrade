//! Shutdown and drain: waits for a signal or a fatal trip, tears everything down under a
//! watchdog, catches panics, and decides the process exit.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::signal::unix::{SignalKind, signal};

use crate::adapters::exec::EdgeHandle;
use crate::hot::spawn::SimTapGate;
use crate::msg::ui::UiLifecycle;
use crate::shutdown::{FatalSignal, TaskHandle, panic_text};
use crate::{error, info, warn};

use super::{Engine, ExitReport};

/// How often the fatal latch is polled, since it cannot be awaited directly.
const LATCH_POLL: Duration = Duration::from_millis(20);

enum Trigger {
    Signal(&'static str),
    Fatal,
}

impl Engine {
    /// Blocks until a signal or a fatal trip, then drains under a hard deadline enforced by a
    /// watchdog.
    pub(super) fn run_until_shutdown(self) -> ExitReport {
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
            metrics,
            fatal,
            drain,
            lifecycle,
            drain_deadline,
        } = self;

        let trigger = runtime.block_on(wait_for_shutdown(&fatal));
        match &trigger {
            Trigger::Signal(name) => info!("received {name} — draining"),
            Trigger::Fatal => {
                let reason = fatal
                    .reason()
                    .unwrap_or_else(|| Box::<str>::from("fatal signal tripped"));
                error!("fatal: {reason} — draining");
            }
        }
        // Uses try_send only, so a dead receiver never blocks the drain.
        lifecycle.try_send(UiLifecycle::Draining).ok();

        let done = Arc::new(AtomicBool::new(false));
        spawn_watchdog(drain_deadline, Arc::clone(&done));

        let (exposure_result, persist_result) = runtime.block_on(async {
            stop_edge_producers(exec, exec_tap_gate, timer, adapters, link).await;
            drain.request();
            join_hot(hot).await;
            // Position is drained before research data: closing Parquet takes time, and
            // running out of it still leaves a recoverable position.
            let exposure = exposure.drain().await;
            let persist = match persistence {
                Some(handle) => handle.drain().await,
                None => Ok(()),
            };
            metrics.abort();
            (exposure, persist)
        });
        let exposure_failure = report_drain_failure("exposure", exposure_result);
        let persist_failure = report_drain_failure("persistence", persist_result);
        // Exposure first: position is what the next run inherits, so it names the exit.
        let drain_error = exposure_failure.or(persist_failure);

        // The fatal latch is re-read after the drain, since a hot-thread panic during it
        // must override an otherwise graceful shutdown.
        let signal_name = match &trigger {
            Trigger::Signal(name) => Some(*name),
            Trigger::Fatal => None,
        };
        let report = decide_exit(signal_name, fatal.reason(), drain_error);

        info!(
            "drain complete ({})",
            if report.graceful { "graceful" } else { "fatal" }
        );
        log_handle.drain();
        done.store(true, Ordering::Release);
        // Released only after every order-related component has stopped, so a replacement
        // process cannot start while this one is still draining.
        drop(execution_lease);
        report
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
        fatal.trip(format!("panic at {location}: {message}"));
    }));
}

async fn wait_for_shutdown(fatal: &FatalSignal) -> Trigger {
    tokio::select! {
        () = wait_sigint() => Trigger::Signal("SIGINT"),
        () = wait_sigterm() => Trigger::Signal("SIGTERM"),
        () = poll_fatal(fatal) => Trigger::Fatal,
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

async fn poll_fatal(fatal: &FatalSignal) {
    while !fatal.is_tripped() {
        tokio::time::sleep(LATCH_POLL).await;
    }
}

/// Stops producers in dependency order.
async fn stop_edge_producers(
    exec: Option<EdgeHandle>,
    exec_tap_gate: Option<Arc<SimTapGate>>,
    timer: TaskHandle,
    adapters: Vec<TaskHandle>,
    link: Option<TaskHandle>,
) {
    timer.shutdown().await;
    if let Some(exec) = exec {
        stop_execution(exec, exec_tap_gate).await;
    }
    if let Some(link) = link {
        link.shutdown().await;
    }
    for adapter in adapters {
        adapter.shutdown().await;
    }
}

/// A gate is present only under a simulated venue. The taps are that venue's own market feed, so
/// they stay open across its forced sweep and close only once nothing is left to drain them.
async fn stop_execution(exec: EdgeHandle, tap_gate: Option<Arc<SimTapGate>>) {
    let Some(gate) = tap_gate else {
        exec.shutdown().await;
        return;
    };
    gate.begin_sweep();
    exec.shutdown().await;
    gate.disable();
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

fn spawn_watchdog(deadline: Duration, done: Arc<AtomicBool>) {
    std::thread::Builder::new()
        .name("polysim-drain-watchdog".to_owned())
        .spawn(move || {
            std::thread::sleep(deadline);
            if !done.load(Ordering::Acquire) {
                // This is a forced exit, so the log drain never gets to flush; the cause
                // goes to both stderr and the log.
                eprintln!(
                    "polysim: drain deadline {} ms exceeded — forcing exit",
                    deadline.as_millis()
                );
                error!(
                    "drain deadline {} ms exceeded — forcing exit",
                    deadline.as_millis()
                );
                std::process::exit(1);
            }
        })
        .expect("failed to spawn drain watchdog thread");
}

fn report_drain_failure<E: core::fmt::Display>(
    what: &str,
    result: Result<(), E>,
) -> Option<Box<str>> {
    let Err(error) = result else { return None };
    error!("{what} drain failed: {error}");
    Some(format!("{what} drain failed: {error}").into())
}

pub fn decide_exit(
    signal_name: Option<&str>,
    fatal_reason: Option<Box<str>>,
    drain_error: Option<Box<str>>,
) -> ExitReport {
    if let Some(reason) = fatal_reason {
        return ExitReport {
            graceful: false,
            reason,
        };
    }
    if let Some(reason) = drain_error {
        return ExitReport {
            graceful: false,
            reason,
        };
    }
    match signal_name {
        Some(name) => ExitReport {
            graceful: true,
            reason: format!("received {name}").into(),
        },
        None => ExitReport {
            graceful: false,
            reason: "fatal signal tripped".into(),
        },
    }
}
