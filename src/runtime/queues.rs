//! Input-queue fabric: one SPSC ring per queue id the registry issued, all built before any
//! producer exists so the hot thread owns every read end from the first message on.

use rtrb::{Consumer, Producer, RingBuffer};

use crate::hot::ingress::IngressQueues;
use crate::hot::spawn::{LinkQueueProducer, QueueProducer};
use crate::ids::{QueueId, SourceId};
use crate::msg::inbound::InboundMessage;
use crate::registry::Registry;
use crate::shutdown::FatalSignal;

pub(super) struct InputQueues {
    pub ingress: IngressQueues,
    /// In the registry's producer-group order, because the market-data spawn pairs each group with
    /// its producer by position.
    pub groups: Vec<QueueProducer>,
    pub timer: QueueProducer,
    /// None with no link: block. Drop+count, not fatal-on-full.
    pub link: Option<LinkQueueProducer>,
    /// None unless execution actor exists. Engine-fed -> FATAL-on-full: engine can't keep up.
    pub exec: Option<QueueProducer>,
}

pub(super) fn build_input_queues(
    registry: &Registry,
    capacity: usize,
    fatal: &FatalSignal,
) -> InputQueues {
    let mut consumers: Vec<Option<Consumer<InboundMessage>>> =
        (0..registry.input_queue_count()).map(|_| None).collect();
    // Pushed in the registry's producer-group order, which the market-data spawn relies on.
    let mut group_producers = Vec::with_capacity(registry.producer_groups().len());
    for group in registry.producer_groups() {
        let producer = attach_ring(&mut consumers, group.queue_id, capacity);
        group_producers.push(QueueProducer::new(
            producer,
            fatal.clone(),
            group.queue_id,
            group.source_id,
        ));
    }

    let timer_queue_id = registry.timer_queue_id();
    let timer_ring = attach_ring(&mut consumers, timer_queue_id, capacity);
    // Timer not a producer group -> source id one past last group.
    let timer_source_id = SourceId(registry.producer_groups().len() as u16);
    let timer_producer =
        QueueProducer::new(timer_ring, fatal.clone(), timer_queue_id, timer_source_id);

    let link_producer = registry.link_queue_id().map(|queue_id| {
        LinkQueueProducer::new(attach_ring(&mut consumers, queue_id, capacity), queue_id)
    });

    // One past last producer group + timer -> unique source id.
    let exec_producer = registry.exec_queue_id().map(|queue_id| {
        let exec_ring = attach_ring(&mut consumers, queue_id, capacity);
        let source_id = SourceId(registry.producer_groups().len() as u16 + 1);
        QueueProducer::new(exec_ring, fatal.clone(), queue_id, source_id)
    });

    let consumers = consumers
        .into_iter()
        .map(|slot| slot.expect("every queue id assigned a ring"))
        .collect();
    InputQueues {
        ingress: IngressQueues::new(consumers),
        groups: group_producers,
        timer: timer_producer,
        link: link_producer,
        exec: exec_producer,
    }
}

fn attach_ring(
    consumers: &mut [Option<Consumer<InboundMessage>>],
    queue_id: QueueId,
    capacity: usize,
) -> Producer<InboundMessage> {
    let (producer, consumer) = RingBuffer::<InboundMessage>::new(capacity);
    consumers[usize::from(queue_id.0)] = Some(consumer);
    producer
}
