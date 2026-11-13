//! Ingress fitness: a full input queue trips the fatal signal.

use engine::hot::spawn::QueueProducer;
use engine::msg::ids::{QueueId, SourceId};
use engine::msg::inbound::{InboundMessage, MessageBody, SpinTick};
use engine::msg::time::LocalTs;
use engine::runtime::shutdown::FatalSignal;
use rtrb::RingBuffer;

fn spin(seq: u64, received: i64) -> InboundMessage {
    InboundMessage::new(
        LocalTs::from_micros(received),
        MessageBody::SpinTick(SpinTick { seq }),
    )
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
