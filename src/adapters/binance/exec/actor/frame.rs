//! Wire encoding: intent becomes bytes, and inbound frames are routed by id or by event.
//! On the WebSocket the API key travels in a signed parameter, which puts it in the frame as
//! plaintext, so these frames are never logged. The REST key rides in a header instead, which is
//! safe.

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::adapters::exec::{ExecRequest, RequestId};

use super::super::{
    EncodeContext, EncodedRequest, RecvWindow, RequestSigner, RequestStamp, SignError, WireError,
    encode_request,
};

const API_KEY_PARAM: &str = "apiKey";
const SIGNATURE_PARAM: &str = "signature";

#[derive(thiserror::Error, Debug)]
pub(super) enum FrameError {
    #[error("encoding the {method} request failed")]
    Encode {
        method: &'static str,
        #[source]
        source: WireError,
    },
    #[error("signing a request failed")]
    Sign {
        #[source]
        source: SignError,
    },
    /// An API key that is not UTF-8 can neither be routed nor signed.
    #[error("the binance api key holds bytes that are not text — it cannot ride a json param")]
    ApiKeyNotText,
}

/// A response frame carries the id its request was tagged with; a stream event is
/// unsolicited and correlates by client order id instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InboundFrame {
    Response(RequestId),
    StreamEvent,
    Unroutable,
}

/// Peeks at the routing keys only; the full frame is parsed separately by the decoder.
#[derive(Deserialize)]
struct FrameKeys {
    id: Option<u64>,
    event: Option<serde::de::IgnoredAny>,
}

pub(super) fn route(text: &str) -> Result<InboundFrame, serde_json::Error> {
    let keys: FrameKeys = serde_json::from_str(text)?;
    // The stream envelope is checked first because unsolicited events carry no id.
    if keys.event.is_some() {
        return Ok(InboundFrame::StreamEvent);
    }
    Ok(match keys.id {
        Some(id) => InboundFrame::Response(RequestId::new(id)),
        None => InboundFrame::Unroutable,
    })
}

pub(super) struct FrameCredentials<'a> {
    pub signer: &'a RequestSigner,
    pub api_key: &'a str,
    pub recv_window: RecvWindow,
    pub stamp: RequestStamp,
}

/// Builds a `{id, method, params}` frame with credentials folded into params. The signed
/// values are taken directly from the signature result rather than rebuilt, so the wire
/// frame can never drift from what was actually signed.
///
/// # Errors
/// Returns [`FrameError::Encode`] when the request names an unknown instrument, or
/// [`FrameError::Sign`] when a parameter cannot be escaped for signing.
pub(super) fn frame_request(
    request_id: RequestId,
    request: ExecRequest,
    context: &EncodeContext<'_>,
    credentials: FrameCredentials<'_>,
) -> Result<String, FrameError> {
    let EncodedRequest { method, params } =
        encode_request(request, context).map_err(|source| FrameError::Encode {
            method: method_hint(request),
            source,
        })?;
    let signed = credentials
        .signer
        .sign(
            params
                .set(API_KEY_PARAM, credentials.api_key)
                .set_recv_window(credentials.recv_window),
            credentials.stamp,
        )
        .map_err(|source| FrameError::Sign { source })?;

    let mut object = Map::with_capacity(signed.signed_params().len() + 1);
    for (name, value) in signed.signed_params() {
        object.insert((*name).to_owned(), Value::String(value.clone()));
    }
    object.insert(
        SIGNATURE_PARAM.to_owned(),
        Value::String(signed.signature().as_str().to_owned()),
    );

    let mut frame = Map::with_capacity(3);
    frame.insert("id".to_owned(), Value::from(request_id.get()));
    frame.insert("method".to_owned(), Value::String(method.to_owned()));
    frame.insert("params".to_owned(), Value::Object(object));
    Ok(Value::Object(frame).to_string())
}

/// The API key as text, since it rides a JSON parameter rather than a header. A lossy conversion
/// of non-UTF-8 bytes would fail the signature and look like the wrong key.
///
/// # Errors
/// [`FrameError::ApiKeyNotText`] when the key is not UTF-8.
pub(super) fn api_key_text(key: &crate::secrets::Secret) -> Result<String, FrameError> {
    std::str::from_utf8(key.expose_bytes())
        .map(str::to_owned)
        .map_err(|_| FrameError::ApiKeyNotText)
}

fn method_hint(request: ExecRequest) -> &'static str {
    match request {
        ExecRequest::Place { .. } => "place",
        ExecRequest::Cancel { .. } => "cancel",
        ExecRequest::AmendQty { .. } => "amend",
        ExecRequest::OrderStatus { .. } => "order-status",
        ExecRequest::OpenOrders { .. } => "open-orders",
        ExecRequest::SubscribeUserStream => "subscribe",
    }
}
