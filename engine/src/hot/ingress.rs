//! The input queues of the hot thread: the write end of each producer, and the read end, which picks
//! the oldest head. Order is exact within one queue, and a message late to an empty queue stays late.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use rtrb::{Consumer, Producer, RingBuffer};

use crate::link::SenderCounts;
use crate::log::{hot_error, hot_warn};
use crate::msg::failure::{FailureClass, Halt};
use crate::msg::ids::{MAX_INPUT_QUEUES, QueueId, SourceId};
use crate::msg::inbound::{
    InboundMessage, MarketTapItem, MessageBody, RunControl, SpinTick, TappedMessage, VenueMeta,
};
use crate::msg::run_state::RunAssertion;
use crate::msg::time::LocalTs;
use crate::runtime::shutdown::{FatalReason, FatalSignal};

/// The consumer ends of every input queue; only the hot thread holds this.
pub struct IngressQueues {
    queues: Vec<(QueueId, Consumer<InboundMessage>)>,
}

impl IngressQueues {
    /// # Panics
    /// More than [`MAX_INPUT_QUEUES`] queues, which the registry never builds.
    pub fn new(consumers: Vec<Consumer<InboundMessage>>) -> Self {
        let queues = consumers
            .into_iter()
            .enumerate()
            .map(|(index, consumer)| {
                let queue_id = QueueId::new(index).unwrap_or_else(|refused| panic!("{refused}"));
                (queue_id, consumer)
            })
            .collect();
        Self { queues }
    }

    pub fn pop_next(&mut self) -> Option<(QueueId, InboundMessage)> {
        let heads = self
            .queues
            .iter()
            .map(|(_, consumer)| consumer.peek().ok().map(|head| head.header.received_ts_us));
        let position = select_oldest(heads)?;
        let (queue_id, consumer) = &mut self.queues[position];
        let message = consumer
            .pop()
            .expect("peeked message vanished before pop — hot thread is the only consumer");
        Some((*queue_id, message))
    }

    /// Messages that wait in each queue, by queue id. Each count is one read of an index that
    /// another core writes, so the hot loop takes it once a spin, never once a message.
    #[inline]
    pub(crate) fn backlog_by_queue(&self) -> [u32; MAX_INPUT_QUEUES] {
        let mut waiting = [0; MAX_INPUT_QUEUES];
        for (count, (_, consumer)) in waiting.iter_mut().zip(&self.queues) {
            *count = u32::try_from(consumer.slots()).unwrap_or(u32::MAX);
        }
        waiting
    }
}

/// The position of the oldest head. `heads` holds the received stamp of the head of each queue in
/// queue order, and `None` for an empty queue. On a tie the lowest position wins, so the order across
/// queues is a function of the heads alone.
pub fn select_oldest(heads: impl IntoIterator<Item = Option<LocalTs>>) -> Option<usize> {
    let mut oldest: Option<(usize, LocalTs)> = None;
    for (position, head) in heads.into_iter().enumerate() {
        let Some(received) = head else {
            continue;
        };
        if oldest.is_none_or(|(_, best)| received < best) {
            oldest = Some((position, received));
        }
    }
    oldest.map(|(position, _)| position)
}

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
/// frame and counts it for its sender: a halt there would let any peer stop the engine.
/// WARNING: a drop is safe only while each link topic carries a whole state, never a delta or an
/// event. A strategy that folds a stream of deltas breaks on the first drop.
pub struct LinkQueueProducer {
    producer: Producer<InboundMessage>,
    queue_id: QueueId,
    dropped: u64,
    senders: SenderCounts,
    sender_rate: NonZeroU32,
}

impl LinkQueueProducer {
    /// A full ring drops the frame and counts it, because an untrusted peer fills it.
    pub const WHEN_FULL: FailureClass = FailureClass::Reject;

    /// No sender has a rate until [`LinkQueueProducer::with_sender_rate`] gives one.
    pub fn new(producer: Producer<InboundMessage>, queue_id: QueueId) -> Self {
        Self {
            producer,
            queue_id,
            dropped: 0,
            senders: SenderCounts::default(),
            sender_rate: NonZeroU32::MAX,
        }
    }

    /// The frames each sender may put into one heartbeat period. The frames above it are refused
    /// and counted for that sender.
    pub fn with_sender_rate(mut self, rate: NonZeroU32) -> Self {
        self.sender_rate = rate;
        self
    }

    /// Refuses a frame of a sender above its rate, and drops a frame that finds the ring full; each
    /// is counted for `from`.
    pub fn push(&mut self, from: SocketAddr, message: InboundMessage) {
        let slot = match self.senders.admit(from, self.sender_rate) {
            Ok(slot) => slot,
            Err(refused) => return self.warn_over_rate(from, refused),
        };
        if self.producer.push(message).is_err() {
            self.senders.count_queue_full(slot);
            self.count_drop();
        }
    }

    /// Each sender gets its whole rate again. The link actor calls it at each heartbeat.
    pub fn start_rate_period(&mut self) {
        self.senders.start_period();
    }

    /// The frames dropped because the queue was full, from each sender together.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    pub fn senders(&self) -> &SenderCounts {
        &self.senders
    }

    /// Warns at the 1st, 2nd, 4th, 8th refusal of one sender and so on. The link actor's task
    /// calls it, never the hot thread, so a text line may carry the address.
    #[cold]
    fn warn_over_rate(&self, from: SocketAddr, refused: u64) {
        if refused.is_power_of_two() {
            crate::warn!(
                "link sender {from} above its rate of {} frames a heartbeat — refused {refused} frames",
                self.sender_rate
            );
        }
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

/// The write end of the control queue, which holds run-control markers only. A full ring drops the
/// marker and counts it: the link actor pushes the pending marker again at its next heartbeat, so a
/// drop delays control and loses none of it. Peer frames ride the link queue, so a flood of them
/// never fills this one.
pub struct ControlQueueProducer {
    producer: Producer<InboundMessage>,
    queue_id: QueueId,
    dropped: u64,
}

impl ControlQueueProducer {
    /// Its sender is outside the engine, and the level repair sends the marker again.
    pub const WHEN_FULL: FailureClass = FailureClass::Reject;

    pub fn new(producer: Producer<InboundMessage>, queue_id: QueueId) -> Self {
        Self {
            producer,
            queue_id,
            dropped: 0,
        }
    }

    /// The marker of `desired`, stamped `now`.
    pub fn push(&mut self, desired: RunAssertion, now: LocalTs) {
        let marker = InboundMessage::new(now, MessageBody::RunControl(RunControl { desired }));
        if self.producer.push(marker).is_err() {
            self.count_drop();
        }
    }

    /// The markers dropped because the queue was full.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Warns at the 1st, 2nd, 4th, 8th drop and so on, so a stalled hot thread shows in the log
    /// without filling it.
    #[cold]
    fn count_drop(&mut self) {
        self.dropped += 1;
        if self.dropped.is_power_of_two() {
            hot_warn!(
                "control queue {} full at capacity {} — dropped {} run-control markers",
                self.queue_id.index(),
                self.producer.buffer().capacity(),
                self.dropped
            );
        }
    }
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
