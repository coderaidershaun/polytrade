//! The rings that carry samples to the metrics thread. A full ring drops the sample and counts it,
//! so the hot thread and the senders never wait for the metrics thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rtrb::{Consumer, Producer, RingBuffer};

use crate::msg::labelled_enum::labelled_enum;
use crate::msg::latency::{CounterSample, StageSample, WireSample};

use super::rollup::LatencyRollup;

/// The driver writes one counter sample for each spin, and the metrics thread drains every few
/// milliseconds, so 64 spins of room is ample.
const COUNTER_RING_CAPACITY: usize = 64;

/// Commands are far rarer than messages: 4,096 holds seconds of the busiest order flow.
const WIRE_RING_CAPACITY: usize = 4_096;

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum SampleRing {
        Stages = "stage sample",
        Counters = "counter sample",
        Wires = "wire sample",
    }
    pub fn label;
}

/// The write end of one sample ring.
pub struct SampleProducer<T> {
    producer: Producer<T>,
    dropped: Arc<AtomicU64>,
}

impl<T> SampleProducer<T> {
    /// Never waits: a full ring drops `sample` and counts it.
    #[inline]
    pub fn push(&mut self, sample: T) {
        if self.producer.push(sample).is_err() {
            self.count_drop();
        }
    }

    /// The producer is the counter's only writer, so a load and a store add one exactly, and the
    /// metrics thread reads the total with no lock.
    #[cold]
    fn count_drop(&self) {
        let dropped = self.dropped.load(Ordering::Relaxed);
        self.dropped.store(dropped + 1, Ordering::Relaxed);
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// The write ends of every sample ring: the driver takes the stage and counter rings, a sender the
/// wire ring.
pub struct SampleProducers {
    pub stages: SampleProducer<StageSample>,
    pub counters: SampleProducer<CounterSample>,
    pub wires: SampleProducer<WireSample>,
}

struct SampleConsumer<T> {
    consumer: Consumer<T>,
    dropped: Arc<AtomicU64>,
}

impl<T> SampleConsumer<T> {
    fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

pub(super) struct SampleConsumers {
    stages: SampleConsumer<StageSample>,
    counters: SampleConsumer<CounterSample>,
    wires: SampleConsumer<WireSample>,
}

impl SampleConsumers {
    /// Each ring is drained to empty, and the drop totals are read after, so a drop that the
    /// producer counted during the drain is reported in this pass or the next one.
    pub(super) fn drain_into(&mut self, rollup: &mut LatencyRollup) {
        while let Ok(sample) = self.stages.consumer.pop() {
            rollup.record_stage(&sample);
        }
        while let Ok(sample) = self.counters.consumer.pop() {
            rollup.record_counters(&sample);
        }
        while let Ok(sample) = self.wires.consumer.pop() {
            rollup.record_wire(&sample);
        }
        rollup.note_ring_drops(SampleRing::Stages, self.stages.dropped());
        rollup.note_ring_drops(SampleRing::Counters, self.counters.dropped());
        rollup.note_ring_drops(SampleRing::Wires, self.wires.dropped());
    }
}

pub(super) fn sample_rings(stage_capacity: usize) -> (SampleProducers, SampleConsumers) {
    let (stage_producer, stages) = ring(stage_capacity);
    let (counter_producer, counters) = ring(COUNTER_RING_CAPACITY);
    let (wire_producer, wires) = ring(WIRE_RING_CAPACITY);
    (
        SampleProducers {
            stages: stage_producer,
            counters: counter_producer,
            wires: wire_producer,
        },
        SampleConsumers {
            stages,
            counters,
            wires,
        },
    )
}

fn ring<T>(capacity: usize) -> (SampleProducer<T>, SampleConsumer<T>) {
    let (producer, consumer) = RingBuffer::new(capacity);
    let dropped = Arc::new(AtomicU64::new(0));
    (
        SampleProducer {
            producer,
            dropped: Arc::clone(&dropped),
        },
        SampleConsumer { consumer, dropped },
    )
}
