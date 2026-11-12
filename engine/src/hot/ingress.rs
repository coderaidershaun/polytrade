//! The hot thread's intake coordinator. Pops the oldest among the queue heads present now, ties
//! broken by lowest `QueueId`. Per-producer FIFO is exact; cross-producer order is best-effort — a
//! straggler on a then-empty queue arrives late by design, since waiting for it would stall the path.

use rtrb::Consumer;

use crate::ids::QueueId;
use crate::msg::inbound::InboundMessage;
use crate::time::TsUs;

/// What one pop leaves behind, read after the message came off: `depth` counts only the popped
/// queue, `spin_backlog` every queue at once. The total is present on spin ticks alone — the one
/// cadence that arrives whether or not the market does, so the reading is comparable over time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueSample {
    pub queue_id: QueueId,
    pub depth: usize,
    pub spin_backlog: Option<usize>,
}

/// The consumer ends of every input queue; only the hot thread holds this.
pub struct IngressQueues {
    consumers: Vec<Consumer<InboundMessage>>,
}

impl IngressQueues {
    pub fn new(consumers: Vec<Consumer<InboundMessage>>) -> Self {
        Self { consumers }
    }

    pub fn pop_next(&mut self) -> Option<(QueueId, InboundMessage)> {
        let mut chosen: Option<(usize, TsUs)> = None;
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

    #[inline]
    pub fn occupancy(&self, queue: QueueId) -> usize {
        self.consumers[usize::from(queue.0)].slots()
    }

    #[inline]
    pub fn backlog(&self) -> usize {
        self.consumers.iter().map(Consumer::slots).sum()
    }
}
