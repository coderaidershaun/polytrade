//! Picks the next message of the hot thread: the oldest head among the queues that hold one now.
//! Order is exact within one queue, and a message that is late to an empty queue stays late.

use rtrb::Consumer;

use crate::msg::ids::{MAX_INPUT_QUEUES, QueueId};
use crate::msg::inbound::InboundMessage;
use crate::msg::time::LocalTs;

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
