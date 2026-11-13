//! What a venue edge hands the hot thread goes through one funnel, which stamps each message as it
//! is queued.

use crate::hot::spawn::QueueProducer;
use crate::msg::exec::{AccountChunk, ExecEvent};
use crate::msg::inbound::{InboundMessage, MessageBody};
use crate::msg::time::{EngineClock, LocalTs};
use crate::venues::exec::EventSink;

/// The one path from a venue edge to the hot thread. Every message is stamped as it is queued,
/// because the hot side measures its own lateness against that stamp and cannot take it later.
pub(crate) struct EventFunnel {
    producer: QueueProducer,
    clock: EngineClock,
}

impl EventFunnel {
    pub(crate) fn new(producer: QueueProducer, clock: EngineClock) -> Self {
        Self { producer, clock }
    }

    pub(crate) fn send_account(&mut self, received_ts_us: LocalTs, chunks: Vec<AccountChunk>) {
        for chunk in chunks {
            self.send(InboundMessage::new(
                received_ts_us,
                MessageBody::Account(chunk),
            ));
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the session chassis stamps queued time and paces reconnects by wall time"
    )]
    pub(crate) fn send(&mut self, mut message: InboundMessage) {
        message.header.queued_ts_us = self.clock.now();
        self.producer.push(message);
    }
}

impl EventSink for EventFunnel {
    fn send_exec(&mut self, received_ts_us: LocalTs, event: ExecEvent) {
        self.send(InboundMessage::new(
            received_ts_us,
            MessageBody::Exec(event),
        ));
    }
}
