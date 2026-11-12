//! Hot-thread spawn + input producers. Full queue = fatal (engine stalled).
//! [`LinkQueueProducer`] exception: drop+count (untrusted remote, not engine-fed).

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread::{self, JoinHandle};

use rtrb::Producer;

use super::ingress::{IngressQueues, QueueSample};
use crate::ids::{QueueId, SourceId};
use crate::msg::inbound::{InboundMessage, MarketTapItem, TappedMessage, VenueMeta};
use crate::shutdown::{DrainSignal, FatalSignal};
use crate::time::TsUs;
use crate::{error, warn};

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
        let (ring_name, capacity, stalled) = match ring {
            FullRing::Input => ("input queue", self.producer.buffer().capacity(), "engine"),
            FullRing::MarketTap => {
                let capacity = self
                    .tap
                    .as_ref()
                    .map_or(0, |tap| tap.producer.buffer().capacity());
                ("market tap ring for input queue", capacity, "simulator")
            }
        };
        let queue = self.queue_id.0;
        error!(
            "{ring_name} {queue} (source {}) full at capacity {capacity} — {stalled} cannot keep up",
            self.source_id.0
        );
        self.fatal.trip(format!(
            "{ring_name} {queue} full at capacity {capacity} — {stalled} cannot keep up"
        ));
    }
}

/// The two rings an input producer writes; the fatal message names the one that filled.
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
            warn!(
                "link input queue {} full at capacity {} — dropped {} frames from remote peers",
                self.queue_id.0,
                self.producer.buffer().capacity(),
                self.dropped
            );
        }
    }
}

pub struct HotThreadConfig {
    pub core_id: Option<usize>,
    pub tag: &'static str,
}

/// Spawn pinned hot loop: pop oldest, dispatch. Fatal stops immediately. Drain drains first.
pub fn spawn_hot_thread(
    config: HotThreadConfig,
    mut queues: IngressQueues,
    fatal: FatalSignal,
    drain: DrainSignal,
    mut dispatch: impl FnMut(QueueSample, InboundMessage) + Send + 'static,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("polysim-hot".to_owned())
        .spawn(move || {
            crate::log::register_thread(config.tag);
            if let Some(core_id) = config.core_id {
                pin_current_thread(core_id, &fatal);
            }
            loop {
                if fatal.is_tripped() {
                    return;
                }
                let Some((queue_id, message)) = queues.pop_next() else {
                    if drain.is_requested() {
                        return;
                    }
                    std::hint::spin_loop();
                    continue;
                };
                let spin_backlog =
                    matches!(message, InboundMessage::SpinTick(_)).then(|| queues.backlog());
                dispatch(
                    QueueSample {
                        queue_id,
                        depth: queues.occupancy(queue_id),
                        spin_backlog,
                    },
                    message,
                );
            }
        })
        .expect("failed to spawn hot thread")
}

/// Linux: pinning mandatory (failure trips fatal). Other OS: best-effort, single WARN.
fn pin_current_thread(core_id: usize, fatal: &FatalSignal) {
    let pinned = core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
    if !cfg!(target_os = "linux") {
        warn!(
            "cpu pinning is best-effort on {} — hot thread may float",
            std::env::consts::OS
        );
        return;
    }
    if !pinned {
        error!("hot thread failed to pin to core {core_id} — pinning is mandatory on linux");
        fatal.trip(format!(
            "hot thread failed to pin to core {core_id} — pinning is mandatory on linux"
        ));
    }
}
