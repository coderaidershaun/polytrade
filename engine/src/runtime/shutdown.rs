//! The latches of a run: `FatalSignal` keeps the first `FatalReason` and stops now, `DrainSignal`
//! finishes the input first, and the run-state handles turn trading off and on without an ending.

use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::msg::failure::{FailureClass, Halt};
use crate::msg::ids::{ClientOrderId, QueueId, SourceId, TradeId};
use crate::msg::run_state::{RunAssertion, RunState};
use crate::warn;

/// A one-way latch: the engine drains and exits immediately. Cheap to clone and to check, and the
/// first tripper's reason is the one kept.
#[derive(Clone, Default)]
pub struct FatalSignal {
    tripped: Arc<AtomicBool>,
    reason: Arc<OnceLock<FatalReason>>,
}

impl FatalSignal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Keeps `reason` unless an earlier trip left one, then latches the signal. The reason is set
    /// before the flag, so a reader that sees the flag also finds the reason.
    #[cold]
    pub fn trip(&self, reason: FatalReason) {
        self.reason.get_or_init(|| reason);
        self.tripped.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_tripped(&self) -> bool {
        self.tripped.load(Ordering::Acquire)
    }

    pub fn reason(&self) -> Option<FatalReason> {
        self.reason.get().copied()
    }
}

/// Why the fatal latch tripped. Each value is a plain copy, so the latch keeps the first one with no
/// lock. Text that no value can hold, such as a parse error, is in the ERROR line that the site
/// writes before it trips.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FatalReason {
    #[error("input queue {} full at capacity {capacity} — engine cannot keep up", queue.index())]
    InputQueueFull { queue: QueueId, capacity: usize },
    #[error(
        "market tap ring for input queue {} full at capacity {capacity} — simulator cannot keep up",
        queue.index()
    )]
    MarketTapFull { queue: QueueId, capacity: usize },
    #[error("a thread panicked — the log holds its place and message")]
    Panic,
    #[error("{venue} execution cannot retain an observed order in its possibly-live set")]
    MirrorRetainFailed { venue: &'static str },
    #[error("{venue} execution placement admission failed closed for order {:016x}", client_id.0)]
    PlacementFailedClosed {
        venue: &'static str,
        client_id: ClientOrderId,
    },
    #[error("the exchange simulator's command lane is full at capacity {capacity}")]
    SimulatorCommandLaneFull { capacity: usize },
    #[error(
        "the simulated venue's market inbox is full at {capacity} events — the run is not valid"
    )]
    SimulatorInboxFull { capacity: usize },
    #[error("{venue} execution: the rest worker stopped answering")]
    RestWorkerGone { venue: &'static str },
    #[error("{venue} execution cannot decode {}", part.noun())]
    UndecodableAnswer {
        venue: &'static str,
        part: AnswerPart,
    },
    #[error("{venue} execution: the account stream subscribe was refused {failures} times")]
    SubscribeRefused { venue: &'static str, failures: u32 },
    #[error("market data source {} exhausted its connection epoch counter", source_id.0)]
    EpochExhausted { source_id: SourceId },
    #[error(
        "market data source {} sent a decimal too large for the fixed-point range",
        source_id.0
    )]
    MarketDataUnreadable { source_id: SourceId },
    #[error("{venue} execution cannot decode a possibly-owned open order {venue_order_id}")]
    UndecodableOpenOrder {
        venue: &'static str,
        venue_order_id: i64,
    },
    #[error("{venue} execution order index is full and cannot record a venue order id")]
    OrderIndexFull { venue: &'static str },
    #[error("{venue} execution cannot build its account stream subscription")]
    SubscribeUnbuildable { venue: &'static str },
    #[error("{venue} execution trade {} settled failed after its fill was folded", trade.0)]
    SettlementFailed { venue: &'static str, trade: TradeId },
}

impl FatalReason {
    pub const fn failure_class(&self) -> FailureClass {
        match self {
            FatalReason::InputQueueFull { .. }
            | FatalReason::MarketTapFull { .. }
            | FatalReason::Panic
            | FatalReason::MirrorRetainFailed { .. }
            | FatalReason::PlacementFailedClosed { .. }
            | FatalReason::SimulatorCommandLaneFull { .. }
            | FatalReason::SimulatorInboxFull { .. }
            | FatalReason::RestWorkerGone { .. }
            | FatalReason::UndecodableAnswer { .. }
            | FatalReason::SubscribeRefused { .. }
            | FatalReason::EpochExhausted { .. }
            | FatalReason::MarketDataUnreadable { .. }
            | FatalReason::UndecodableOpenOrder { .. }
            | FatalReason::OrderIndexFull { .. }
            | FatalReason::SubscribeUnbuildable { .. }
            | FatalReason::SettlementFailed { .. } => FailureClass::Halt(Halt::Engine),
        }
    }
}

/// Which part of a venue's wire held the frame that could not be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AnswerPart {
    Response,
    Stream,
}

impl AnswerPart {
    const fn noun(self) -> &'static str {
        match self {
            AnswerPart::Response => "an answer to one of its requests",
            AnswerPart::Stream => "an event of its account stream",
        }
    }
}

/// A one-way latch for a graceful drain: the hot loop processes until the queues are empty, then
/// exits.
#[derive(Clone, Default)]
pub struct DrainSignal {
    requested: Arc<AtomicBool>,
}

impl DrainSignal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }

    #[inline]
    pub fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

/// The state and epoch live in ONE atomic word, so a reader never sees a mismatched pair. Each
/// handle below wraps one or two of these, and only a writer handle can store into one.
#[derive(Clone)]
struct RunStateCell {
    packed: Arc<AtomicU64>,
}

/// The epoch occupies the low 63 bits and the state the sign bit, which keeps the whole assertion
/// in one word while still letting the epoch reach 2^63.
const IDLE_BIT: u64 = 1 << 63;

impl RunStateCell {
    fn new() -> Self {
        Self {
            packed: Arc::new(AtomicU64::new(pack(RunAssertion::INITIAL))),
        }
    }

    #[inline]
    fn store(&self, assertion: RunAssertion) {
        self.packed.store(pack(assertion), Ordering::Release);
    }

    #[inline]
    fn load(&self) -> RunAssertion {
        unpack(self.packed.load(Ordering::Acquire))
    }
}

const fn pack(assertion: RunAssertion) -> u64 {
    match assertion.state {
        RunState::Running => assertion.epoch,
        RunState::Idle => assertion.epoch | IDLE_BIT,
    }
}

const fn unpack(word: u64) -> RunAssertion {
    RunAssertion {
        state: if word & IDLE_BIT == 0 { RunState::Running } else { RunState::Idle },
        epoch: word & !IDLE_BIT,
    }
}

/// The three handles on the run state of one run, from its one constructor. The desired state is
/// what the controller asked for, and the acknowledged state is what the fold applied.
pub struct RunStateHandles {
    /// For the link actor, the one writer of the desired state.
    pub desired_writer: DesiredWriter,
    /// For each edge: clone it once for each.
    pub desired_reader: DesiredReader,
    /// For the hot thread, the one writer of the acknowledged state.
    pub acknowledged_writer: AcknowledgedWriter,
}

impl RunStateHandles {
    pub fn new() -> Self {
        let desired = RunStateCell::new();
        let acknowledged = RunStateCell::new();
        Self {
            desired_writer: DesiredWriter {
                desired: desired.clone(),
                acknowledged: acknowledged.clone(),
            },
            desired_reader: DesiredReader { desired },
            acknowledged_writer: AcknowledgedWriter { acknowledged },
        }
    }
}

impl Default for RunStateHandles {
    fn default() -> Self {
        Self::new()
    }
}

/// The handle of the link actor: it writes the desired state and reads both. The desired and
/// acknowledged pair is what makes control converge. `pending()` asks which marker still needs
/// pushing, which makes the control LEVEL-triggered. The marker rides a ring that drops and counts
/// when it is full, and an edge-triggered design would leave the adapters parked after such a drop;
/// asking again on the next tick repairs it instead. Not `Clone`, so it has one holder.
pub struct DesiredWriter {
    desired: RunStateCell,
    acknowledged: RunStateCell,
}

impl DesiredWriter {
    /// A highest-epoch-wins store. `false` means this controller lost the race and is not in
    /// charge.
    pub(crate) fn accept_if_newer(&self, assertion: RunAssertion) -> bool {
        if assertion.epoch <= self.desired.load().epoch {
            return false;
        }
        self.desired.store(assertion);
        true
    }

    pub fn desired(&self) -> RunAssertion {
        self.desired.load()
    }

    pub fn acknowledged(&self) -> RunAssertion {
        self.acknowledged.load()
    }

    /// The marker to push, or `None` once the hot thread has caught up.
    pub fn pending(&self) -> Option<RunAssertion> {
        let desired = self.desired.load();
        (desired != self.acknowledged.load()).then_some(desired)
    }
}

/// The handle of an edge: it reads the desired state to park or resume itself, and can write no
/// state.
#[derive(Clone)]
pub struct DesiredReader {
    desired: RunStateCell,
}

impl DesiredReader {
    #[inline]
    pub fn state(&self) -> RunState {
        self.desired.load().state
    }
}

/// The handle of the hot thread: it reports the run state that the fold applied. Not `Clone`, so
/// it has one holder.
pub struct AcknowledgedWriter {
    acknowledged: RunStateCell,
}

impl AcknowledgedWriter {
    #[inline]
    pub(crate) fn store(&self, assertion: RunAssertion) {
        self.acknowledged.store(assertion);
    }
}

/// Aborts a spawned task and notes whether it panicked. The panic hook has already logged the
/// cause, so this records only the fact and never re-raises.
pub(crate) async fn abort_and_warn(join: tokio::task::JoinHandle<()>, task_name: &str) {
    join.abort();
    if let Err(join_error) = join.await
        && join_error.is_panic()
    {
        warn!("{task_name} task panicked during shutdown — cause already logged");
    }
}

/// Dropping it detaches the task; `shutdown` aborts it and notes whether it panicked.
pub struct TaskHandle {
    join: tokio::task::JoinHandle<()>,
    name: &'static str,
}

impl TaskHandle {
    pub(crate) fn new(join: tokio::task::JoinHandle<()>, name: &'static str) -> Self {
        Self { join, name }
    }

    pub async fn shutdown(self) {
        abort_and_warn(self.join, self.name).await;
    }
}

pub(crate) fn panic_text(payload: &(dyn Any + Send)) -> Box<str> {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
        .into()
}

/// A blocking writer on its own thread, told to finish by a flag and joined from async code.
pub(crate) struct WriterThread<E> {
    drain_requested: Arc<AtomicBool>,
    join: std::thread::JoinHandle<Result<(), E>>,
}

impl<E: Send + 'static> WriterThread<E> {
    /// `body` receives the drain flag: once it is set, write what is left and return.
    pub(crate) fn spawn(
        name: &str,
        body: impl FnOnce(Arc<AtomicBool>) -> Result<(), E> + Send + 'static,
    ) -> Self {
        let drain_requested = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&drain_requested);
        let join = std::thread::Builder::new()
            .name(name.into())
            .spawn(move || body(flag))
            .unwrap_or_else(|error| {
                panic!("os refused to spawn the {name} writer thread at init: {error}")
            });
        Self {
            drain_requested,
            join,
        }
    }

    /// A writer that panicked is reported through `panicked` and never re-raised: the panic hook
    /// has already turned the panic fatal.
    pub(crate) async fn drain(self, panicked: fn(Box<str>) -> E, interrupted: E) -> Result<(), E> {
        self.drain_requested.store(true, Ordering::Release);
        let thread = self.join;
        match tokio::task::spawn_blocking(move || thread.join()).await {
            Ok(Ok(result)) => result,
            Ok(Err(panic)) => Err(panicked(panic_text(&*panic))),
            Err(join_error) if join_error.is_panic() => {
                Err(panicked(panic_text(&*join_error.into_panic())))
            }
            Err(_) => Err(interrupted),
        }
    }
}
