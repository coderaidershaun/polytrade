//! Production decoding of simulated venue payloads.

use crate::msg::inbound::{InboundMessage, MessageBody};
use crate::venues::binance::exec::{
    DecodeContext, ResponseContext, StreamEvent, decode_response, decode_stream_event,
};

pub fn response_messages(
    payloads: &[String],
    context: &ResponseContext<'_>,
) -> Vec<InboundMessage> {
    let received_ts_us = context.decode.received_ts_us;
    payloads
        .iter()
        .flat_map(|json| {
            decode_response(json, context)
                .expect("the venue emits only payloads the decoder accepts")
                .events
        })
        .map(|event| InboundMessage::new(received_ts_us, MessageBody::Exec(event)))
        .collect()
}

pub fn stream_messages(payloads: &[String], decode: DecodeContext<'_>) -> Vec<InboundMessage> {
    let received_ts_us = decode.received_ts_us;
    let message = |body| InboundMessage::new(received_ts_us, body);
    payloads
        .iter()
        .flat_map(|json| {
            match decode_stream_event(json, &decode)
                .expect("the venue emits only payloads the decoder accepts")
            {
                StreamEvent::Exec(event) => vec![message(MessageBody::Exec(event))],
                StreamEvent::Account(chunks) => chunks
                    .into_iter()
                    .map(|chunk| message(MessageBody::Account(chunk)))
                    .collect(),
                StreamEvent::BalanceChanged | StreamEvent::Ignored(_) => Vec::new(),
            }
        })
        .collect()
}
