//! The one loop that takes commands off the lane to a venue. It pops each command once and in lane
//! order, stamps its pop and the moment its transport takes the bytes, and waits between wakes in
//! the way its placement asks.

use std::future::Future;

use crate::hot::sink::SampleSink;
use crate::msg::exec::{ExecCommand, ExecLaneItem, StampedExecCommand};
use crate::msg::ids::{ClientOrderId, QueueId};
use crate::msg::latency::{CommandKind, WireSample};
use crate::msg::time::{Stamper, Tick};
use crate::runtime::config::SenderPlacement;
use crate::venues::exec::SessionOutcome;

/// A command of a sweep or of a read of every order names no single order, and a missing number is
/// written as zero.
const NO_CLIENT: ClientOrderId = ClientOrderId(0);

/// What wakes a shared sender: the runtime's interval in an engine, or a test's own tick.
pub trait TickSource: Send {
    fn tick(&mut self) -> impl Future<Output = ()> + Send;
}

impl TickSource for tokio::time::Interval {
    async fn tick(&mut self) {
        tokio::time::Interval::tick(self).await;
    }
}

/// How a loop that sends waits between wakes. Shared, it waits for a tick. Pinned, it yields once to
/// its own thread's runtime, so it never sleeps and a command leaves on its next pass.
pub struct SendWait<K> {
    placement: SenderPlacement,
    ticks: K,
}

impl<K: TickSource> SendWait<K> {
    pub fn new(placement: SenderPlacement, ticks: K) -> Self {
        Self { placement, ticks }
    }

    pub async fn wait(&mut self) {
        match self.placement {
            SenderPlacement::Shared => self.ticks.tick().await,
            SenderPlacement::Pinned => tokio::task::yield_now().await,
        }
    }
}

/// What a venue answers for one command it was handed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Handover {
    /// The transport took the bytes as it was handed the command.
    Taken,
    /// The bytes wait for the flush that ends the wake.
    Fed,
    /// No byte reaches the venue: the command asked for nothing to send, or it was refused and
    /// answered as not sent.
    NotSent,
}

/// Where a sender hands its commands. A venue folds each command through its execution core and
/// gives what the core asks for to its own transport.
pub trait CommandTransport: Send {
    /// # Errors
    /// The [`SessionOutcome`] of a connection that ended while the command was handed over.
    fn hand_over(
        &mut self,
        command: ExecCommand,
    ) -> impl Future<Output = Result<Handover, SessionOutcome>> + Send;

    /// Ends a wake. A venue that feeds a socket flushes it here, once for every command of the wake.
    ///
    /// # Errors
    /// The [`SessionOutcome`] of a connection that ended during the flush.
    fn flush(&mut self) -> impl Future<Output = Result<(), SessionOutcome>> + Send;
}

pub struct SenderSetup<K> {
    pub commands: rtrb::Consumer<ExecLaneItem>,
    pub stamper: Stamper,
    pub wires: SampleSink<WireSample>,
    /// The input queue of the execution edge, which names its venue in each wire sample.
    pub queue: QueueId,
    pub wait: SendWait<K>,
}

/// A command whose bytes wait for the flush that ends the wake.
struct FedCommand {
    stamped: StampedExecCommand,
    pop_tick: Tick,
}

pub struct Sender<K> {
    commands: rtrb::Consumer<ExecLaneItem>,
    stamper: Stamper,
    wires: SampleSink<WireSample>,
    queue: QueueId,
    wait: SendWait<K>,
    fed: Vec<FedCommand>,
}

impl<K: TickSource> Sender<K> {
    pub fn new(setup: SenderSetup<K>) -> Self {
        Self {
            commands: setup.commands,
            stamper: setup.stamper,
            wires: setup.wires,
            queue: setup.queue,
            wait: setup.wait,
            fed: Vec::new(),
        }
    }

    pub async fn wait(&mut self) {
        self.wait.wait().await;
    }

    /// Takes every item off the lane and hands each command to `transport`, then ends the wake with
    /// one flush. A watermark belongs to the simulated venue, so a live venue never sees one. Answers
    /// how the connection ended, when it did, and leaves what is still on the lane for the next one.
    #[expect(
        clippy::disallowed_methods,
        reason = "a sender stamps the counter at the pop and at the wire for its wire samples"
    )]
    pub async fn drain<T: CommandTransport>(
        &mut self,
        transport: &mut T,
    ) -> Option<SessionOutcome> {
        let mut is_handed = false;
        while let Ok(item) = self.commands.pop() {
            let ExecLaneItem::Command(stamped) = item else {
                continue;
            };
            let pop_tick = self.stamper.stamp();
            is_handed = true;
            match transport.hand_over(stamped.command).await {
                Ok(Handover::Taken) => {
                    let wire_tick = self.stamper.stamp();
                    self.wires.push(self.sample(&stamped, pop_tick, wire_tick));
                }
                Ok(Handover::Fed) => self.fed.push(FedCommand { stamped, pop_tick }),
                Ok(Handover::NotSent) => {}
                Err(ended) => {
                    self.fed.clear();
                    return Some(ended);
                }
            }
        }
        if !is_handed {
            return None;
        }
        if let Err(ended) = transport.flush().await {
            self.fed.clear();
            return Some(ended);
        }
        let wire_tick = self.stamper.stamp();
        let mut fed = std::mem::take(&mut self.fed);
        for command in fed.drain(..) {
            let sample = self.sample(&command.stamped, command.pop_tick, wire_tick);
            self.wires.push(sample);
        }
        self.fed = fed;
        None
    }

    fn sample(&self, stamped: &StampedExecCommand, pop_tick: Tick, wire_tick: Tick) -> WireSample {
        let (kind, client_id) = kind_of(&stamped.command);
        WireSample {
            issue_tick: stamped.issue_tick,
            pop_tick,
            wire_tick,
            issued_ts_us: stamped.issued_ts_us,
            client_id,
            kind,
            queue: self.queue,
        }
    }
}

fn kind_of(command: &ExecCommand) -> (CommandKind, ClientOrderId) {
    match *command {
        ExecCommand::Place { client_id, .. } => (CommandKind::Place, client_id),
        ExecCommand::AmendQty { client_id, .. } => (CommandKind::Amend, client_id),
        ExecCommand::Cancel { client_id, .. } => (CommandKind::Cancel, client_id),
        ExecCommand::ReconcileOrder { client_id, .. } => (CommandKind::Reconcile, client_id),
        ExecCommand::CancelOurs { .. } | ExecCommand::CancelPriorRun { .. } => {
            (CommandKind::Cancel, NO_CLIENT)
        }
        ExecCommand::ReconcileOpenOrders { .. } => (CommandKind::Reconcile, NO_CLIENT),
    }
}
