//! Ingress fitness: `pop_next` yields the oldest message among the present queue heads
//! (tie-break lowest `QueueId`) and loses nothing; a full input queue trips the fatal signal.

use std::collections::HashSet;

use engine::hot::ingress::IngressQueues;
use engine::hot::spawn::QueueProducer;
use engine::msg::ids::{QueueId, SourceId};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::LocalTs;
use engine::runtime::shutdown::FatalSignal;
use proptest::prelude::*;
use rtrb::RingBuffer;

fn spin(seq: u64, received: i64) -> InboundMessage {
    InboundMessage::new(
        LocalTs::from_micros(received),
        MessageBody::SpinTick(SpinTick { seq }),
    )
}

proptest! {
    /// The pop sequence is non-decreasing in `(received_ts_us, QueueId)` and an exact permutation
    /// of everything pushed (no loss/duplication/starvation). Pins order among the heads PRESENT
    /// at pop time, NOT global cross-producer order under live interleaving (a late straggler to
    /// an empty queue is by design — see `IngressQueues::pop_next`).
    #[test]
    fn pops_global_order_without_loss(
        per_queue in prop::collection::vec(prop::collection::vec(0i64..1_000, 0..80), 1..8),
    ) {
        let mut consumers = Vec::new();
        let mut expected_ids: HashSet<u64> = HashSet::new();
        let mut next_id = 0u64;

        for timestamps in &per_queue {
            let mut ascending = timestamps.clone();
            ascending.sort_unstable();
            let (mut producer, consumer) = RingBuffer::<InboundMessage>::new(ascending.len() + 1);
            for &received in &ascending {
                let id = next_id;
                next_id += 1;
                expected_ids.insert(id);
                producer.push(spin(id, received)).expect("ring sized for all messages");
            }
            consumers.push(consumer);
        }

        let mut ingress = IngressQueues::new(consumers);
        let mut popped: Vec<(i64, usize, u64)> = Vec::new();
        while let Some((queue_id, message)) = ingress.pop_next() {
            let received = message.header.received_ts_us.micros();
            let MessageBody::SpinTick(tick) = message.body else {
                unreachable!("only spin ticks were pushed");
            };
            popped.push((received, queue_id.index(), tick.seq));
        }

        // No loss / duplication (the no-starvation half): every id popped exactly once.
        prop_assert_eq!(popped.len(), next_id as usize);
        let popped_ids: HashSet<u64> = popped.iter().map(|&(_, _, id)| id).collect();
        prop_assert_eq!(popped_ids, expected_ids);

        // Global order: lexicographically non-decreasing in (received_ts_us, QueueId).
        for pair in popped.windows(2) {
            let earlier = (pair[0].0, pair[0].1);
            let later = (pair[1].0, pair[1].1);
            prop_assert!(earlier <= later, "out of order: {:?} then {:?}", pair[0], pair[1]);
        }
    }
}

#[test]
fn full_input_queue_trips_fatal() {
    let (producer, _consumer) = RingBuffer::<InboundMessage>::new(2);
    let fatal = FatalSignal::new();
    let mut queue_producer = QueueProducer::new(
        producer,
        fatal.clone(),
        QueueId::new(0).unwrap(),
        SourceId(0),
    );

    assert!(!fatal.is_tripped());
    queue_producer.push(spin(0, 0));
    queue_producer.push(spin(1, 1));
    assert!(!fatal.is_tripped(), "two pushes fit a 2-slot ring");

    queue_producer.push(spin(2, 2));
    assert!(
        fatal.is_tripped(),
        "overflowing the input queue must trip fatal"
    );
    assert!(fatal.reason().is_some());
}
