//! One input ring for each queue id, sized by its kind. The start thread reads the layout, and the
//! hot thread builds and touches each ring on its own core before any producer runs.

use rtrb::Consumer;

use super::exec::command_lane_capacity;
use crate::hot::ingress::IngressQueues;
use crate::hot::spawn::{LinkQueueProducer, QueueProducer, touched_input_ring};
use crate::msg::ids::{QueueId, SourceId};
use crate::msg::inbound::InboundMessage;
use crate::runtime::config::Config;
use crate::runtime::registry::Registry;
use crate::runtime::shutdown::FatalSignal;

/// A slow spin fits only a few spins in the drain deadline, and a timer ring that small would halt
/// the engine after a short pause of the hot thread.
const MIN_TIMER_CAPACITY: usize = 16;

/// The ring capacity of each kind of input queue, with the default of each key that is absent.
struct QueueCapacities {
    market_data: usize,
    timer: usize,
    link: usize,
    execution: usize,
}

impl QueueCapacities {
    fn of<P>(config: &Config<P>) -> QueueCapacities {
        let queues = &config.queues;
        let drain_deadline_us = config.engine.drain_deadline_ms.saturating_mul(1_000);
        let spins_in_drain = drain_deadline_us.div_ceil(config.engine.spin_interval_us) as usize;
        QueueCapacities {
            market_data: queues.input_capacity,
            timer: queues
                .timer_capacity
                .unwrap_or(spins_in_drain.max(MIN_TIMER_CAPACITY)),
            link: queues.link_capacity,
            execution: queues
                .execution_capacity
                .unwrap_or_else(|| command_lane_capacity(config.engine.spin_interval())),
        }
    }
}

/// Which ring each queue id gets, with whose source id and how many slots, read from the registry
/// so that the hot thread can build the rings with no registry of its own.
pub(super) struct RingPlan {
    queue_count: usize,
    /// In the registry's producer-group order, because the market-data spawn pairs each group with
    /// its producer by position.
    groups: Vec<(QueueId, SourceId)>,
    timer: (QueueId, SourceId),
    link: Option<QueueId>,
    exec: Option<(QueueId, SourceId)>,
    capacities: QueueCapacities,
}

/// The write ends of the input queues, handed back to the start by the hot thread.
pub(super) struct InputProducers {
    /// In the registry's producer-group order.
    pub(super) groups: Vec<QueueProducer>,
    pub(super) timer: QueueProducer,
    /// `None` with no `link:` block. A full ring drops the frame and counts it.
    pub(super) link: Option<LinkQueueProducer>,
    /// `None` unless execution is armed. The engine's own edge fills it, so a full ring halts the
    /// engine.
    pub(super) exec: Option<QueueProducer>,
}

impl RingPlan {
    pub(super) fn of<P>(registry: &Registry, config: &Config<P>) -> RingPlan {
        let group_count = registry.producer_groups().len() as u16;
        RingPlan {
            queue_count: registry.input_queue_count(),
            groups: registry
                .producer_groups()
                .iter()
                .map(|group| (group.queue_id, group.source_id))
                .collect(),
            // The timer and the execution edge are no producer groups, so their source ids are the
            // two after the last group.
            timer: (registry.timer_queue_id(), SourceId(group_count)),
            link: registry.link_queue_id(),
            exec: registry
                .exec_queue_id()
                .map(|queue_id| (queue_id, SourceId(group_count + 1))),
            capacities: QueueCapacities::of(config),
        }
    }

    /// Builds each ring on the calling thread, the hot thread, and touches each slot once there.
    pub(super) fn build(self, fatal: &FatalSignal) -> (IngressQueues, InputProducers) {
        let capacities = &self.capacities;
        let mut consumers: Vec<Option<Consumer<InboundMessage>>> =
            (0..self.queue_count).map(|_| None).collect();
        let mut ring = |queue_id: QueueId, capacity: usize| {
            let (producer, consumer) = touched_input_ring(capacity);
            consumers[queue_id.index()] = Some(consumer);
            producer
        };
        let groups = self
            .groups
            .iter()
            .map(|&(queue_id, source_id)| {
                let producer = ring(queue_id, capacities.market_data);
                QueueProducer::new(producer, fatal.clone(), queue_id, source_id)
            })
            .collect();
        let (timer_queue, timer_source) = self.timer;
        let timer = QueueProducer::new(
            ring(timer_queue, capacities.timer),
            fatal.clone(),
            timer_queue,
            timer_source,
        );
        let link = self
            .link
            .map(|queue_id| LinkQueueProducer::new(ring(queue_id, capacities.link), queue_id));
        let exec = self.exec.map(|(queue_id, source_id)| {
            let producer = ring(queue_id, capacities.execution);
            QueueProducer::new(producer, fatal.clone(), queue_id, source_id)
        });
        let consumers = consumers
            .into_iter()
            .map(|slot| slot.expect("every queue id assigned a ring"))
            .collect();
        let producers = InputProducers {
            groups,
            timer,
            link,
            exec,
        };
        (IngressQueues::new(consumers), producers)
    }
}
