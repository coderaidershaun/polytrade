//! The session chassis every venue edge runs on: dial, serve, back off, sweep, stop. A venue brings
//! its own select! loop and its own idea of what can be sent while the socket is down; the ordering
//! rules — which latch wins, how long blind before resting orders are pulled, when the settled
//! signal is raised — live here once. Two copies of a safety rule drift, and the cost of the drift
//! is an order left resting with nothing watching it.
//!
//! It sits beside the venue-neutral execution machinery rather than inside it. That machinery is
//! scanned for clock reads, because hot state has to replay identically from a recorded tape,
//! whereas this file cannot do its job without reading one: pacing a reconnect and measuring how
//! long an edge has been blind are both wall-clock questions.
//!
//! It also holds what every venue's bring-up is handed and what it hands back, so the runtime can
//! start any venue without naming one.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::StreamExt;

use crate::adapters::IDLE_POLL;
use crate::adapters::backoff::BackoffCaps;
use crate::adapters::exec::{
    EdgeHandle, EngineIdentity, EventSink, ExecStop, ExitPlan, SessionOutcome,
};
use crate::adapters::socket::{Reader, Writer, connect};
use crate::config::ExecutionConfig;
use crate::hot::spawn::{QueueProducer, SimTapGate};
use crate::link::RunState;
use crate::msg::exec::{AccountChunk, CancelReason, ExecEvent, ExecLaneItem};
use crate::msg::inbound::InboundMessage;
use crate::registry::Registry;
use crate::shutdown::{FatalSignal, RunStateCell};
use crate::time::{DurationUs, EngineClock, TsUs};
use crate::{info, warn};

// Polling the command ring every millisecond costs a little latency but avoids waking on every push.
pub(crate) const COMMAND_POLL: Duration = Duration::from_millis(1);
// Checks the exit latches and deadlines often enough that a stop is acted on within milliseconds.
pub(crate) const HOUSEKEEPING: Duration = Duration::from_millis(20);
// Often enough to catch a missed fill quickly, slow enough to stay well inside venue rate limits.
pub(crate) const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
// Enough trade ids to cover one reconcile interval, so a fill read twice is recognised as seen.
pub(crate) const RECENT_TRADES: usize = 256;

/// The settings every venue's edge is built from. The runtime builds them once; a simulated venue
/// reads only the ones it needs.
pub(crate) struct EdgeSettings {
    pub(crate) clock: EngineClock,
    pub(crate) fatal: FatalSignal,
    /// When the run goes idle, quotes are pulled and the socket drops, because resting orders
    /// cannot simply be parked.
    pub(crate) run_state: RunStateCell,
    pub(crate) backoff: BackoffCaps,
    /// Must stay inside the engine's drain deadline, since the watchdog force-exits once that
    /// deadline is reached.
    pub(crate) sweep_deadline: Duration,
    /// How long to stay blind before pulling orders. The venue gives no guidance here, so this is
    /// the operator's judgment.
    pub(crate) disconnect_sweep_after: DurationUs,
    pub(crate) max_orders_per_side: usize,
}

/// What the runtime hands every venue's bring-up, whichever venue it is.
pub(crate) struct EdgeInputs<'a> {
    pub(crate) registry: &'a Registry,
    pub(crate) execution: &'a ExecutionConfig,
    pub(crate) identity: EngineIdentity,
    pub(crate) commands: rtrb::Consumer<ExecLaneItem>,
    pub(crate) producer: QueueProducer,
    pub(crate) settings: EdgeSettings,
    /// The market-data producers, in producer-group order. A venue that matches against the live
    /// market attaches its taps here before the adapters that own these producers start.
    pub(crate) groups: &'a mut Vec<QueueProducer>,
    pub(crate) lane_capacity: usize,
    pub(crate) spin_interval: DurationUs,
    pub(crate) input_capacity: usize,
}

/// The chosen venue's setup, held until the hot thread exists: an edge is decided during bring-up
/// but must not produce into the execution input queue before its consumer is running.
pub(crate) type EdgeSpawn = Box<dyn FnOnce(&tokio::runtime::Handle) -> EdgeHandle + Send>;

/// What a venue's bring-up hands back to the runtime.
pub(crate) struct VenueEdge {
    pub(crate) spawn: EdgeSpawn,
    /// Only a venue that feeds itself from market taps has any to hold open across its own
    /// shutdown sweep.
    pub(crate) tap_gate: Option<Arc<SimTapGate>>,
}

/// What every venue edge runs under: the latches that can end the run, the clock its deadlines are
/// measured against, the pacing an operator configured, and the exit currently under way.
pub(crate) struct EdgeControl {
    pub(crate) clock: EngineClock,
    pub(crate) fatal: FatalSignal,
    pub(crate) run_state: RunStateCell,
    pub(crate) stop: ExecStop,
    pub(crate) backoff: BackoffCaps,
    pub(crate) sweep_deadline: Duration,
    pub(crate) disconnect_sweep_after: DurationUs,
    pub(crate) exit: Option<ExitPlan>,
    pub(crate) is_swept: bool,
}

impl EdgeControl {
    pub(crate) fn new(settings: &EdgeSettings, stop: ExecStop) -> Self {
        Self {
            clock: settings.clock.clone(),
            fatal: settings.fatal.clone(),
            run_state: settings.run_state.clone(),
            stop,
            backoff: settings.backoff,
            sweep_deadline: settings.sweep_deadline,
            disconnect_sweep_after: settings.disconnect_sweep_after,
            exit: None,
            is_swept: false,
        }
    }

    /// A terminal exit was planned and its sweep finished, so there is nothing left to reconnect
    /// for.
    pub(crate) fn is_finished(&self) -> bool {
        self.exit.as_ref().is_some_and(ExitPlan::is_final) && self.is_swept
    }

    /// The highest-precedence exit reason: Shutdown > Fatal > Park. Each level terminates more than
    /// the next.
    pub(crate) fn wanted_exit(&self) -> Option<CancelReason> {
        if self.stop.requested.load(Ordering::Acquire) {
            return Some(CancelReason::Shutdown);
        }
        if self.fatal.is_tripped() {
            return Some(CancelReason::Fatal);
        }
        (self.run_state.state() == RunState::Idle).then_some(CancelReason::Park)
    }

    /// The exit a latch asks for, while none is under way yet.
    pub(crate) fn wanted_new_exit(&self) -> Option<CancelReason> {
        if self.exit.is_some() {
            return None;
        }
        self.wanted_exit()
    }

    /// Ends the session once the exit's sweep has finished, or once its deadline has passed with
    /// orders still mirrored as resting.
    pub(crate) fn finished_exit(
        &self,
        now: TsUs,
        venue: &str,
        mirrored: usize,
    ) -> Option<SessionOutcome> {
        let exit = self.exit.as_ref()?;
        if self.is_swept {
            return Some(SessionOutcome::Swept);
        }
        if now > exit.deadline {
            warn!(
                "{venue} execution gave up waiting for its {:?} sweep — {mirrored} orders are still mirrored as resting",
                exit.reason
            );
            return Some(SessionOutcome::Swept);
        }
        None
    }

    /// Opens the exit unless one is already under way, answering whether this call is the one that
    /// opened it — so the caller sweeps once rather than on every latch read.
    pub(crate) fn open_exit(&mut self, reason: CancelReason) -> bool {
        if self.exit.is_some() {
            return false;
        }
        self.exit = Some(ExitPlan::new(reason, self.clock.now(), self.sweep_deadline));
        self.is_swept = false;
        true
    }

    /// The next paced cancel attempt, once the retry interval allows and the sweep is unfinished.
    pub(crate) fn claim_sweep_retry(&mut self) -> Option<CancelReason> {
        if self.is_swept {
            return None;
        }
        let now = self.clock.now();
        self.exit.as_mut().and_then(|exit| exit.claim_retry(now))
    }

    /// When a failed pass may be tried again, jittered by the same caps that pace reconnects.
    pub(crate) fn next_attempt_at(&self, attempt: u32) -> TsUs {
        let delay = self.backoff.delay(attempt);
        self.clock.now() + DurationUs::from_micros(delay.as_micros() as i64)
    }
}

/// The one path from a venue edge to the hot thread. Every message is stamped as it is queued,
/// because the hot side measures its own lateness against that stamp and cannot take it later.
pub(crate) struct EventFunnel {
    producer: QueueProducer,
    clock: EngineClock,
}

impl EventFunnel {
    pub(crate) fn new(producer: QueueProducer, clock: EngineClock) -> Self {
        Self { producer, clock }
    }

    pub(crate) fn send_account(&mut self, chunks: Vec<AccountChunk>) {
        for chunk in chunks {
            self.send(InboundMessage::Account(chunk));
        }
    }

    pub(crate) fn send(&mut self, mut message: InboundMessage) {
        message.set_queued_ts_us(self.clock.now());
        self.producer.push(message);
    }
}

impl EventSink for EventFunnel {
    fn send_exec(&mut self, event: ExecEvent) {
        self.send(InboundMessage::Exec(event));
    }
}

/// What one servicing step with the socket down produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OfflineStep {
    /// An answer landed and was folded in, so the sweep gets another paced attempt.
    Serviced,
    /// The wait elapsed, or the answer channel closed for good.
    Ended,
}

/// What a venue supplies to [`run_edge`]. Everything here is either a venue's own decision or a
/// window onto state the chassis has to read; the loop that calls them is not a venue's business.
pub(crate) trait EdgeDriver {
    /// Names this edge in the chassis's own lines, which read "<venue> execution connected".
    fn venue(&self) -> &'static str;

    /// Where the stream lives. One dial serves every venue; only the URL differs.
    fn stream_url(&self) -> &str;

    /// The exit state, held by the driver because its own passes read it too.
    fn control(&mut self) -> &mut EdgeControl;

    /// Whatever has to be true before the first dial. A venue with nothing to do leaves it alone.
    async fn start(&mut self) {}

    /// The venue's own loop, running until the connection ends one way or the other.
    async fn serve(&mut self, writer: Writer, reader: Reader) -> SessionOutcome;

    /// One servicing step with the socket down: wait for an answer until `deadline` and fold it in.
    /// Folding only — the sweep that answer may unblock is [`EdgeDriver::retry_sweep`]'s, and doing
    /// it here as well would spend two cancel attempts where the pacing allows one.
    async fn while_offline(&mut self, deadline: tokio::time::Instant) -> OfflineStep;

    /// One paced cancel attempt. The chassis passes no writer, because it calls this with the
    /// socket down.
    async fn retry_sweep(&mut self, writer: Option<&mut Writer>);

    /// Open the exit and send its first sweep, unless an exit is already under way. The chassis
    /// passes no writer, because it calls this with the socket down.
    async fn plan_exit(&mut self, reason: CancelReason, writer: Option<&mut Writer>);

    /// What this edge leaves in the log once its loop ends.
    fn report_stop(&self);

    /// Stop whatever the driver spawned beside itself.
    fn abort_workers(&self);
}

/// Dial, serve, back off, repeat — until a terminal exit has swept every order this run owns.
pub(crate) async fn run_edge<D: EdgeDriver>(mut driver: D) {
    driver.start().await;

    let mut attempt: u32 = 0;
    let mut down_since: Option<TsUs> = None;
    while !driver.control().is_finished() {
        check_latches(&mut driver).await;
        if driver.control().is_finished() {
            break;
        }

        let venue = driver.venue();
        let dialled = connect(driver.stream_url()).await;
        match dialled {
            Ok(socket) => {
                info!("{venue} execution connected");
                down_since = None;
                attempt = 0;
                let (writer, reader) = socket.split();
                if driver.serve(writer, reader).await == SessionOutcome::Swept {
                    complete_exit(&mut driver).await;
                }
            }
            Err(error) => {
                warn!("{venue} execution connect failed: {error}");
                let control = driver.control();
                let now = control.clock.now();
                let since = *down_since.get_or_insert(now);
                // Blind for too long means the orders are stale, so they are cancelled by REST.
                let is_blind_too_long =
                    now.diff(since) > control.disconnect_sweep_after && control.exit.is_none();
                if is_blind_too_long {
                    driver.plan_exit(CancelReason::Disconnect, None).await;
                }
                let wait = driver.control().backoff.delay(attempt);
                settle_offline(&mut driver, wait).await;
                if driver.control().is_swept {
                    complete_exit(&mut driver).await;
                    down_since = None;
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }

    driver.report_stop();
    driver.control().stop.settled.notify_one();
    driver.abort_workers();
}

// Read the latches between connections; the sweep they trigger rides REST.
async fn check_latches<D: EdgeDriver>(driver: &mut D) {
    if let Some(reason) = driver.control().wanted_new_exit() {
        driver.plan_exit(reason, None).await;
    }
    // Also reached with a dead socket, so the cancels go over REST and their answers land here.
    if driver.control().exit.is_some() {
        let wait = driver.control().sweep_deadline;
        settle_offline(driver, wait).await;
        complete_exit(driver).await;
    }
}

// While offline, answers are read rather than slept through, and each one carries the sweep forward.
async fn settle_offline<D: EdgeDriver>(driver: &mut D, wait: Duration) {
    let deadline = tokio::time::Instant::now() + wait;
    while !(driver.control().is_swept && driver.control().exit.is_some()) {
        match driver.while_offline(deadline).await {
            OfflineStep::Serviced => driver.retry_sweep(None).await,
            OfflineStep::Ended => return,
        }
    }
}

// Park is the reentrant exit. Final exits don't return.
async fn complete_exit<D: EdgeDriver>(driver: &mut D) {
    let venue = driver.venue();
    let control = driver.control();
    let Some(exit) = control.exit.take() else {
        return;
    };
    if exit.is_final() {
        // On a final exit the drain watchdog takes over from here.
        control.is_swept = true;
        control.exit = Some(exit);
        return;
    }
    if matches!(exit.reason, CancelReason::Park) {
        info!("{venue} execution parked — quotes pulled, socket dropped");
        // Park outranks nothing: a stop or a fatal arriving here ends the wait.
        while control.wanted_exit() == Some(CancelReason::Park) {
            tokio::time::sleep(IDLE_POLL).await;
        }
    }
    control.is_swept = false;
}
