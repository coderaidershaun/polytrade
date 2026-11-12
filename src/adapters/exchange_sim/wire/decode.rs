//! Production decoding of simulated venue payloads.

use crate::adapters::binance::exec::{
    DecodeContext, ResponseContext, StreamEvent, decode_response, decode_stream_event,
};
use crate::msg::inbound::InboundMessage;

pub fn response_messages(
    payloads: &[String],
    context: &ResponseContext<'_>,
) -> Vec<InboundMessage> {
    payloads
        .iter()
        .flat_map(|json| {
            decode_response(json, context)
                .expect("the venue emits only payloads the decoder accepts")
                .events
        })
        .map(InboundMessage::Exec)
        .collect()
}

pub fn stream_messages(payloads: &[String], decode: DecodeContext<'_>) -> Vec<InboundMessage> {
    payloads
        .iter()
        .flat_map(|json| {
            match decode_stream_event(json, &decode)
                .expect("the venue emits only payloads the decoder accepts")
            {
                StreamEvent::Exec(event) => vec![InboundMessage::Exec(event)],
                StreamEvent::Account(chunks) => {
                    chunks.into_iter().map(InboundMessage::Account).collect()
                }
                StreamEvent::BalanceChanged | StreamEvent::Ignored(_) => Vec::new(),
            }
        })
        .collect()
}
