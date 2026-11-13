//! The hot thread, the driver that runs its loop, and the producers of its input queues. A full
//! queue is fatal, except the link queue: an untrusted peer fills it, so it drops and counts.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use rtrb::Producer;

use super::dispatch::HotEngine;
use super::ingress::IngressQueues;
use super::metrics::{DriverSamples, Folded};
use crate::log::{hot_error, hot_warn};
use crate::msg::ids::{QueueId, SourceId};
use crate::msg::inbound::{InboundMessage, MarketTapItem, TappedMessage, VenueMeta};
use crate::msg::latency::CounterSample;
use crate::msg::time::TsUs;
use crate::runtime::pin::pin_current_thread;
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
    high_water_ts_us: TsUs,
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
            high_water_ts_us: TsUs::from_micros(i64::MIN),
        });
        self
    }

    pub fn has_tap(&self) -> bool {
        self.tap.is_some()
    }

    pub(crate) fn source_id(&self) -> SourceId {
        self.source_id
    }

    /// Full queue = engine stalled: trips fatal signal (never silent, never backpressure).
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
        tap.high_water_ts_us = tap.high_water_ts_us.max(message.received_ts_us());
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
            self.queue_id.0
        );
    }

    pub fn push_tap_watermark(&mut self, received_ts_us: TsUs) {
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
                    queue.0,
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
                    queue.0,
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

/// Link input: drop+count (untrusted remote producer). Fatal-on-full would expose remote-kill.
/// Sound ONLY if: link topics carry STATE (not deltas/events). Strategies folding delta streams break.
pub struct LinkQueueProducer {
    producer: Producer<InboundMessage>,
    queue_id: QueueId,
    dropped: u64,
}

impl LinkQueueProducer {
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

    /// Count of frames dropped (link queue full).
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Power-of-two WARNs: catch floods without filling log.
    #[cold]
    fn count_drop(&mut self) {
        self.dropped += 1;
        if self.dropped.is_power_of_two() {
            hot_warn!(
                "link input queue {} full at capacity {} — dropped {} frames from remote peers",
                self.queue_id.0,
                self.producer.buffer().capacity(),
                self.dropped
            );
        }
    }
}

/// What the hot thread is built with. With no samples it reads no counter and pushes nothing, so a
/// replay folds exactly what a live run folds.
pub struct HotThreadConfig {
    pub core_id: Option<usize>,
    pub tag: &'static str,
    pub samples: Option<DriverSamples>,
    pub idle: IdleWait,
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
        if let InboundMessage::SpinTick(tick) = &message {
            samples.counters.push(CounterSample {
                spin_ts_us: tick.received_ts_us,
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

/// The hot thread pins itself, builds its hot lane on that core, and steps the driver until a fatal
/// stops it at once, or a drain request finds every queue empty.
pub fn spawn_hot_thread(
    config: HotThreadConfig,
    queues: IngressQueues,
    engine: HotEngine,
    fatal: FatalSignal,
    drain: DrainSignal,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("polysim-hot".to_owned())
        .spawn(move || {
            crate::log::register_thread(config.tag);
            if let Some(core_id) = config.core_id {
                pin_current_thread(core_id, &fatal);
            }
            crate::log::register_hot_lane(config.tag);
            let mut driver = HotDriver::new(queues, engine, config.samples);
            loop {
                if fatal.is_tripped() {
                    return;
                }
                match driver.step() {
                    Step::Folded => {}
                    Step::Idle if drain.is_requested() => return,
                    Step::Idle => match config.idle {
                        IdleWait::Spin => std::hint::spin_loop(),
                        IdleWait::Park(nap) => driver.park(nap),
                    },
                }
            }
        })
        .expect("failed to spawn hot thread")
}
