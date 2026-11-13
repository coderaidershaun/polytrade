//! The order in which the hot thread pops the heads of its input queues. That order is the order of
//! the fold, so a change in it changes each state that comes after it.

use engine::hot::ingress::IngressQueues;
use engine::msg::ids::QueueId;
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::LocalTs;
use proptest::prelude::*;
use rtrb::RingBuffer;

fn spin(seq: u64, received_us: i64) -> InboundMessage {
    InboundMessage::new(
        LocalTs::from_micros(received_us),
        MessageBody::SpinTick(SpinTick { seq }),
    )
}

proptest! {
    /// FITNESS: with each message present, the hot thread pops the oldest head first, and on a tie
    /// the head of the lowest queue, so the pops merge the queues by received stamp. A pop that lost
    /// a message, gave one twice, or took a newer head first would fold an order that a replay of
    /// the same queues does not.
    #[test]
    fn pops_are_oldest_first_and_lose_nothing(
        per_queue in prop::collection::vec(prop::collection::vec(0_i64..1_000, 0..80), 1..=20),
    ) {
        let mut consumers = Vec::new();
        let mut expected: Vec<(i64, usize, u64)> = Vec::new();
        let mut next_seq = 0_u64;
        for (queue, stamps) in per_queue.iter().enumerate() {
            let mut ascending = stamps.clone();
            ascending.sort_unstable();
            let (mut producer, consumer) = RingBuffer::<InboundMessage>::new(ascending.len() + 1);
            for &received_us in &ascending {
                producer.push(spin(next_seq, received_us)).expect("the ring holds each message");
                expected.push((received_us, queue, next_seq));
                next_seq += 1;
            }
            consumers.push(consumer);
        }
        expected.sort_unstable();

        let mut ingress = IngressQueues::new(consumers);
        let mut popped: Vec<(QueueId, u64)> = Vec::new();
        while let Some((queue_id, message)) = ingress.pop_next() {
            let MessageBody::SpinTick(tick) = message.body else {
                unreachable!("only spin ticks were pushed");
            };
            popped.push((queue_id, tick.seq));
        }

        let popped_seqs: Vec<u64> = popped.iter().map(|&(_, seq)| seq).collect();
        let expected_seqs: Vec<u64> = expected.iter().map(|&(_, _, seq)| seq).collect();
        prop_assert_eq!(popped_seqs, expected_seqs, "the pops are the merge by stamp, then queue");

        let mut id_of_queue: Vec<Option<QueueId>> = vec![None; per_queue.len()];
        for (&(queue_id, _), &(_, queue, _)) in popped.iter().zip(&expected) {
            let named = *id_of_queue[queue].get_or_insert(queue_id);
            prop_assert_eq!(named, queue_id, "each queue pops under one id");
        }
        let ids: Vec<QueueId> = id_of_queue.into_iter().flatten().collect();
        prop_assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "queue ids follow the order of the queues: {:?}",
            ids
        );
    }
}
