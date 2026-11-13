//! The hot thread's intake coordinator. Pops the oldest among the queue heads present now, ties
//! broken by lowest `QueueId`. Per-producer FIFO is exact; cross-producer order is best-effort — a
//! straggler on a then-empty queue arrives late by design, since waiting for it would stall the path.

use rtrb::Consumer;

use crate::msg::ids::{MAX_INPUT_QUEUES, QueueId};
use crate::msg::inbound::InboundMessage;
use crate::msg::time::LocalTs;

/// The consumer ends of every input queue; only the hot thread holds this.
pub struct IngressQueues {
    consumers: Vec<Consumer<InboundMessage>>,
}

impl IngressQueues {
    /// # Panics
    /// More than [`MAX_INPUT_QUEUES`] queues, which the registry never builds.
    pub fn new(consumers: Vec<Consumer<InboundMessage>>) -> Self {
        assert!(
            consumers.len() <= MAX_INPUT_QUEUES,
            "{} input queues, max {MAX_INPUT_QUEUES}",
            consumers.len()
        );
        Self { consumers }
    }

    pub fn pop_next(&mut self) -> Option<(QueueId, InboundMessage)> {
        let mut chosen: Option<(usize, LocalTs)> = None;
        for (index, consumer) in self.consumers.iter().enumerate() {
            if let Ok(head) = consumer.peek() {
                let received = head.received_ts_us();
                let is_older = chosen.is_none_or(|(_, best)| received < best);
                if is_older {
                    chosen = Some((index, received));
                }
            }
        }
        let (index, _) = chosen?;
        let message = self.consumers[index]
            .pop()
            .expect("peeked message vanished before pop — hot thread is the only consumer");
        Some((QueueId(index as u8), message))
    }

    /// Messages that wait in each queue, by queue id. Each count is one read of an index that
    /// another core writes, so the hot loop takes it once a spin, never once a message.
    #[inline]
    pub(crate) fn backlog_by_queue(&self) -> [u32; MAX_INPUT_QUEUES] {
        let mut waiting = [0; MAX_INPUT_QUEUES];
        for (count, consumer) in waiting.iter_mut().zip(&self.consumers) {
            *count = u32::try_from(consumer.slots()).unwrap_or(u32::MAX);
        }
        waiting
    }
}
