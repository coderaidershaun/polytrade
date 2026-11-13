//! The hot thread, which pins first and is built on its own core, its driver, and the producers of
//! its input queues. A full queue halts the engine, except the link queue, which drops.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use rtrb::{Consumer, Producer, RingBuffer};

use super::dispatch::HotEngine;
use super::ingress::IngressQueues;
use super::metrics::{DriverSamples, Folded};
use crate::log::{hot_error, hot_warn};
use crate::msg::failure::{FailureClass, Halt};
use crate::msg::ids::{QueueId, SourceId};
use crate::msg::inbound::{
    InboundMessage, MarketTapItem, MessageBody, SpinTick, TappedMessage, VenueMeta,
};
use crate::msg::latency::CounterSample;
use crate::msg::time::LocalTs;
use crate::runtime::PinRefused;
use crate::runtime::pin::{PinCall, spawn_pinned};
use crate::runtime::shutdown::{DrainSignal, FatalReason, FatalSignal};

/// Keeps market taps open until the simulator finishes its final sweep.
#[derive(Debug, Default)]
pub struct SimTapGate {
    phase: AtomicU8,
}

const TAP_ENABLED: u8 = 0;
const TAP_SWEEPING: u8 = 1;
const TAP_DISABLED: u8 = 2;

// The derived default starts the phase at zero, so zero must be the enabled phase.
const _: () = assert!(TAP_ENABLED == 0);

impl SimTapGate {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.phase.load(Ordering::Acquire) != TAP_DISABLED
    }

    pub fn begin_sweep(&self) {
        self.phase.fetch_max(TAP_SWEEPING, Ordering::AcqRel);
    }

    /// # Panics
    /// If called before [`SimTapGate::begin_sweep`].
    pub fn disable(&self) {
        let phase = self.phase.swap(TAP_DISABLED, Ordering::AcqRel);
        assert!(
            phase != TAP_ENABLED,
            "the simulator market tap was closed before its forced sweep was declared — the \
             shutdown sequence is sweep, then disable, then drop the consumers"
        );
    }
}

struct MarketTap {
    producer: Producer<MarketTapItem>,
    gate: Arc<SimTapGate>,
    high_water_ts_us: LocalTs,
}

/// The write end of one input queue, handed to the producer that feeds it.
pub struct QueueProducer {
    producer: Producer<InboundMessage>,
    tap: Option<MarketTap>,
    fatal: FatalSignal,
    queue_id: QueueId,
    source_id: SourceId,
}

impl QueueProducer {
    /// A full input ring or a full market tap trips the fatal latch: the engine has stalled.
    pub const WHEN_FULL: FailureClass = FailureClass::Halt(Halt::Engine);

    pub fn new(
        producer: Producer<InboundMessage>,
        fatal: FatalSignal,
        queue_id: QueueId,
        source_id: SourceId,
    ) -> Self {
        Self {
            producer,
            tap: None,
            fatal,
            queue_id,
            source_id,
        }
    }

    pub fn with_tap(mut self, tap: Producer<MarketTapItem>, gate: Arc<SimTapGate>) -> Self {
        self.tap = Some(MarketTap {
            producer: tap,
            gate,
            high_water_ts_us: LocalTs::from_micros(i64::MIN),
        });
        self
    }

    pub fn has_tap(&self) -> bool {
        self.tap.is_some()
    }

    pub(crate) fn source_id(&self) -> SourceId {
        self.source_id
    }

    /// A full queue means the engine has stalled, so this trips the fatal latch at once. It never
    /// drops in silence and never waits.
    pub fn push(&mut self, message: InboundMessage) {
        if self.producer.push(message).is_err() {
            self.signal_full(FullRing::Input);
        }
    }

    /// Writes the tap before the hot queue so both consumers observe a consistent prefix.
    pub fn push_tapped(&mut self, message: InboundMessage, venue_meta: VenueMeta) {
        let Some(tap) = self.tap.as_mut() else {
            return self.push(message);
        };
        if !tap.gate.is_enabled() {
            return self.push(message);
        }
        if self.producer.is_full() {
            return self.signal_full(FullRing::Input);
        }
        if tap.producer.is_full() {
            return self.signal_full(FullRing::MarketTap);
        }
        tap.high_water_ts_us = tap.high_water_ts_us.max(message.header.received_ts_us);
        let tapped = MarketTapItem::Event(TappedMessage {
            message,
            venue_meta,
        });
        assert!(
            tap.producer.push(tapped).is_ok(),
            "market tap ring filled between its capacity check and its push — single producer violated"
        );
        assert!(
            self.producer.push(message).is_ok(),
            "input queue {} filled between its capacity check and its push — single producer violated",
            self.queue_id.index()
        );
    }

    pub fn push_tap_watermark(&mut self, received_ts_us: LocalTs) {
        let Some(tap) = self.tap.as_mut() else { return };
        if !tap.gate.is_enabled() {
            return;
        }
        tap.high_water_ts_us = tap.high_water_ts_us.max(received_ts_us);
        let watermark = MarketTapItem::Watermark {
            received_ts_us: tap.high_water_ts_us,
        };
        if tap.producer.push(watermark).is_err() {
            self.signal_full(FullRing::MarketTap);
        }
    }

    /// A full ring means its consumer has stalled, so it is fatal rather than waited out.
    #[cold]
    fn signal_full(&self, ring: FullRing) {
        let (queue, source) = (self.queue_id, self.source_id);
        let reason = match ring {
            FullRing::Input => {
                let capacity = self.producer.buffer().capacity();
                hot_error!(
                    "input queue {} (source {}) full at capacity {} — engine cannot keep up",
                    queue.index(),
                    source.0,
                    capacity
                );
                FatalReason::InputQueueFull { queue, capacity }
            }
            FullRing::MarketTap => {
                let capacity = self
                    .tap
                    .as_ref()
                    .map_or(0, |tap| tap.producer.buffer().capacity());
                hot_error!(
                    "market tap ring for input queue {} (source {}) full at capacity {} — simulator cannot keep up",
                    queue.index(),
                    source.0,
                    capacity
                );
                FatalReason::MarketTapFull { queue, capacity }
            }
        };
        self.fatal.trip(reason);
    }
}

/// The two rings an input producer writes; the fatal reason names the one that filled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FullRing {
    Input,
    MarketTap,
}

/// The write end of the link input queue. A peer on the network fills it, so a full ring drops the
/// frame and counts it: a halt there would let any peer stop the engine.
/// WARNING: a drop is safe only while each link topic carries a whole state, never a delta or an
/// event. A strategy that folds a stream of deltas breaks on the first drop.
pub struct LinkQueueProducer {
    producer: Producer<InboundMessage>,
    queue_id: QueueId,
    dropped: u64,
}

impl LinkQueueProducer {
    /// A full ring drops the frame and counts it, because an untrusted peer fills it.
    pub const WHEN_FULL: FailureClass = FailureClass::Reject;

    pub fn new(producer: Producer<InboundMessage>, queue_id: QueueId) -> Self {
        Self {
            producer,
            queue_id,
            dropped: 0,
        }
    }

    pub fn push(&mut self, message: InboundMessage) {
        if self.producer.push(message).is_err() {
            self.count_drop();
        }
    }

    /// The frames dropped because the queue was full.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Warns at the 1st, 2nd, 4th, 8th drop and so on, so a flood shows in the log without filling
    /// it.
    #[cold]
    fn count_drop(&mut self) {
        self.dropped += 1;
        if self.dropped.is_power_of_two() {
            hot_warn!(
                "link input queue {} full at capacity {} — dropped {} frames from remote peers",
                self.queue_id.index(),
                self.producer.buffer().capacity(),
                self.dropped
            );
        }
    }
}

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
            pops: 0,
            parks: 0,
        }
    }

    #[inline]
    pub fn step(&mut self) -> Step {
        let Some((queue, message)) = self.queues.pop_next() else {
            return Step::Idle;
        };
        let Some(samples) = self.samples.as_mut() else {
            self.engine.dispatch(&message);
            return Step::Folded;
        };
        let pop_tick = samples.stamp();
        self.engine.dispatch(&message);
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
            samples.counters.push(CounterSample {
                spin_ts_us: message.header.received_ts_us,
                spin_seq: tick.seq,
                backlog: self.queues.backlog_by_queue(),
                counters: self.engine.counters(),
                idle_parks: self.parks,
            });
        }
        Step::Folded
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
    fn run(mut self) {
        loop {
            if self.fatal.is_tripped() {
                return;
            }
            match self.driver.step() {
                Step::Folded => {}
                Step::Idle if self.drain.is_requested() => return,
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

/// One input ring, with each slot written and read once on the calling thread. Its memory then
/// sits where that thread runs, and no push of the run is the first touch of a page.
///
/// # Panics
/// If `capacity` is 0.
pub fn touched_input_ring(capacity: usize) -> (Producer<InboundMessage>, Consumer<InboundMessage>) {
    assert!(capacity > 0, "an input ring holds at least one message");
    let (mut producer, mut consumer) = RingBuffer::new(capacity);
    let filler = InboundMessage::new(
        LocalTs::from_micros(0),
        MessageBody::SpinTick(SpinTick { seq: 0 }),
    );
    for _ in 0..capacity {
        assert!(
            producer.push(filler).is_ok(),
            "an empty ring of {capacity} slots takes {capacity} messages"
        );
    }
    for _ in 0..capacity {
        assert!(
            consumer.pop().is_ok(),
            "a ring that took {capacity} messages gives {capacity} back"
        );
    }
    (producer, consumer)
}
