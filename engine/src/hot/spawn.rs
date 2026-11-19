//! The hot thread, which pins first and is built on its own core, and its driver. The write ends
//! of its input queues live beside their read end, and are named here too.

use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::dispatch::HotEngine;
use super::ingress::IngressQueues;
use super::metrics::{DriverSamples, Folded};
use super::sink::TapeSink;
use crate::msg::ids::QueueId;
use crate::msg::inbound::{InboundMessage, MessageBody};
use crate::msg::latency::CounterSample;
use crate::runtime::PinRefused;
use crate::runtime::pin::{PinCall, spawn_pinned};
use crate::runtime::shutdown::{DrainSignal, FatalSignal};

pub use super::ingress::{
    ControlQueueProducer, LinkPush, LinkQueueProducer, QueueProducer, SimTapGate,
    touched_input_ring,
};

/// Where the hot thread runs and its tag in the log. What it folds is built later, on the thread
/// itself, once its pin holds.
pub struct HotThreadConfig {
    pub core_id: Option<usize>,
    pub tag: &'static str,
}

/// How the hot loop waits while every queue is empty. Either way the fold is the same: only when a
/// message is popped changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdleWait {
    /// Spins on its core, so the next message is popped the moment it lands. It takes the core
    /// whole.
    Spin,
    /// Sleeps this long and looks again, which leaves the core to other threads.
    Park(Duration),
}

/// What one step of the hot loop did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Step {
    /// The oldest message of all the queues was folded.
    Folded,
    /// Every queue was empty.
    Idle,
}

/// The body of the hot loop. Each step pops the oldest message, folds it, and records what the
/// samples ask for, on the caller's thread.
pub struct HotDriver {
    queues: IngressQueues,
    engine: HotEngine,
    samples: Option<DriverSamples>,
    /// The tee of each popped message, before the fold handles it. `None` for a driver that records
    /// no tape.
    tape: Option<TapeSink>,
    pops: u32,
    parks: u64,
}

impl HotDriver {
    pub fn new(
        queues: IngressQueues,
        engine: HotEngine,
        samples: Option<DriverSamples>,
    ) -> HotDriver {
        HotDriver {
            queues,
            engine,
            samples,
            tape: None,
            pops: 0,
            parks: 0,
        }
    }

    /// Tees each popped message onto the tape, with the queue it came from.
    pub fn with_tape(self, tape: TapeSink) -> HotDriver {
        HotDriver {
            tape: Some(tape),
            ..self
        }
    }

    /// The fold this driver steps, for a replay to compare its state with.
    pub fn engine(&self) -> &HotEngine {
        &self.engine
    }

    #[inline]
    pub fn step(&mut self) -> Step {
        let Some((queue, message)) = self.queues.pop_next() else {
            return Step::Idle;
        };
        let pop_tick = self.samples.as_mut().map(|samples| samples.stamp());
        self.tee(queue, &message);
        self.engine.dispatch(&message);
        let (Some(samples), Some(pop_tick)) = (self.samples.as_mut(), pop_tick) else {
            return Step::Folded;
        };
        let done_tick = samples.stamp();
        samples.record_stage(&Folded {
            message: &message,
            queue,
            pop_tick,
            done_tick,
            pop_seq: self.pops,
        });
        self.pops = self.pops.wrapping_add(1);
        if let MessageBody::SpinTick(tick) = &message.body {
            let mut counters = self.engine.counters();
            if let Some(tape) = &self.tape {
                counters.tape_lost = tape.lost();
                counters.tape_gaps = tape.gaps();
            }
            samples.counters.push(CounterSample {
                spin_ts_us: message.header.received_ts_us,
                spin_seq: tick.seq,
                backlog: self.queues.backlog_by_queue(),
                counters,
                idle_parks: self.parks,
            });
        }
        Step::Folded
    }

    /// The tape seals at the spin where the fold seals, before the record of that spin, so the next
    /// tape file opens with each book as the fold held it.
    #[inline]
    fn tee(&mut self, queue: QueueId, message: &InboundMessage) {
        let Some(tape) = self.tape.as_mut() else {
            return;
        };
        let books = self.engine.books();
        if let MessageBody::SpinTick(tick) = &message.body
            && self.engine.seals_at(tick.seq)
        {
            tape.seal_with_snapshots(books, message.header.received_ts_us);
        }
        tape.record(queue, message, books);
    }

    /// Sleeps while every queue is empty, and counts it: each counter sample carries the count.
    #[cold]
    fn park(&mut self, nap: Duration) {
        self.parks += 1;
        thread::sleep(nap);
    }
}

/// What the hot thread runs once it is built: the driver, how it waits while every queue is empty,
/// and the two latches that stop it. A driver with no samples reads no counter and pushes nothing,
/// so a replay folds exactly what a live run folds.
pub struct HotLoop {
    pub driver: HotDriver,
    pub idle: IdleWait,
    pub fatal: FatalSignal,
    pub drain: DrainSignal,
}

impl HotLoop {
    /// Steps the driver until a fatal stops it at once, or a drain request finds every queue empty.
    /// The queues are read once more after the request is seen: a producer pushes and then
    /// requests the drain, so its last push can land between an empty read and the request. The
    /// flag is stored with Release and loaded with Acquire, so the read after it sees each push.
    fn run(mut self) {
        loop {
            if self.fatal.is_tripped() {
                return;
            }
            match self.driver.step() {
                Step::Folded => {}
                Step::Idle if self.drain.is_requested() => {
                    if self.driver.step() == Step::Idle {
                        return;
                    }
                }
                Step::Idle => match self.idle {
                    IdleWait::Spin => std::hint::spin_loop(),
                    IdleWait::Park(nap) => self.driver.park(nap),
                },
            }
        }
    }
}

/// What the hot thread is built from. The start sends it one time, and the thread runs it on its
/// own core after its log lanes exist.
type HotBuild = Box<dyn FnOnce() -> HotLoop + Send>;

/// The hot thread once its pin holds, waiting for what it folds.
pub struct PinnedHotThread {
    thread: JoinHandle<()>,
    build: SyncSender<HotBuild>,
}

impl PinnedHotThread {
    /// Hands the thread its build, which builds the rings and the engine on the hot core, then
    /// steps until a fatal or a drain stops it.
    pub fn start(self, build: impl FnOnce() -> HotLoop + Send + 'static) -> JoinHandle<()> {
        self.build
            .send(Box::new(build))
            .expect("the hot thread waits for its build until the build is sent or abandoned");
        self.thread
    }

    /// For a start that is refused after the hot pin: the thread ends at its wait with nothing
    /// built, and this waits for it.
    pub fn abandon(self) {
        drop(self.build);
        assert!(
            self.thread.join().is_ok(),
            "a hot thread that was never built runs nothing that can panic"
        );
    }
}

/// Starts the hot thread, whose first act is its pin. The caller learns of a refused pin before
/// this returns, and the thread then ends. Once the pin holds, the thread waits for its build, so
/// that what it folds is built and first touched on the core that runs it.
///
/// # Errors
/// [`PinRefused`] when the thread cannot pin to `config.core_id` on Linux.
///
/// # Panics
/// If the OS cannot start a thread at all.
pub fn spawn_hot_thread(
    config: HotThreadConfig,
    pin: PinCall,
) -> Result<PinnedHotThread, PinRefused> {
    let (build, built) = mpsc::sync_channel::<HotBuild>(1);
    let thread = spawn_pinned("polysim-hot", config.core_id, pin, move || {
        let Ok(build) = built.recv() else {
            return;
        };
        crate::log::register_thread(config.tag);
        crate::log::register_hot_lane(config.tag);
        build().run();
    })?;
    Ok(PinnedHotThread { thread, build })
}
