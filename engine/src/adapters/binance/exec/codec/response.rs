//! Decodes the venue's answers to order requests, from the WebSocket API and from REST reads alike,
//! so that both transports judge ownership and status the same way. An error answer names no order,
//! so the request it answers supplies the client id.

use crate::adapters::binance::rest::OrderRecord;
use crate::adapters::decode::{order_side, price_field, qty_field};
use crate::adapters::exec::{ExecRequest, blank};
use crate::adapters::venue_clock::clamp_or_receipt;
use crate::ids::{ClientOrderId, InstrumentId, Side, VenueOrderId};
use crate::msg::exec::{ExecEvent, ExecKind};

use super::client_id::classify_client_order_id;
use super::reject::{RejectSubject, amend_budget_remaining, classify_error};
use super::wire::{AmendResult, ResponseEnvelope};
use super::{DecodeContext, FRAME, WireError, money_field, venue_status};

pub struct ResponseContext<'a> {
    pub decode: DecodeContext<'a>,
    pub request: ExecRequest,
    pub recon_seq: u64,
}

/// Every normalised event carried by one response.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedResponse {
    pub events: Vec<ExecEvent>,
}

/// # Errors
/// An answer with neither a result nor an error, or a field that is malformed, negated as
/// unavailable, or spelled in a way this engine does not know. Only a mantissa overflow is fatal.
pub fn decode_response(
    json: &str,
    context: &ResponseContext<'_>,
) -> Result<DecodedResponse, WireError> {
    let envelope: ResponseEnvelope = FRAME.decode(json)?;
    decode_single(envelope, context).map(|events| DecodedResponse { events })
}

/// Decodes one order a read returned, over REST or as a row of the WebSocket open-orders answer. A
/// REST sweep covers the whole account, so a symbol this engine does not track is an ordinary
/// answer and yields `None`.
///
/// # Errors
/// A field that is malformed, negated as unavailable, or spelled in a way this engine does not
/// know. Only a mantissa overflow is fatal.
pub fn decode_order_record(
    record: &OrderRecord,
    kind: ExecKind,
    recon_seq: u64,
    context: &DecodeContext<'_>,
) -> Result<Option<ExecEvent>, WireError> {
    let Some(instrument) = context.symbols.instrument(&record.symbol) else {
        return Ok(None);
    };
    order_event(record, kind, instrument, recon_seq, context).map(Some)
}

fn decode_single(
    envelope: ResponseEnvelope,
    context: &ResponseContext<'_>,
) -> Result<Vec<ExecEvent>, WireError> {
    if let Some(error) = envelope.error {
        return Ok(failure_events(error.code, &error.msg, context));
    }
    let Some(result) = envelope.result else {
        return Err(WireError::EmptyResponse);
    };
    match context.request {
        ExecRequest::Place { .. } => {
            order_answer(&FRAME.decode_value(result)?, ExecKind::AckPlaced, context)
        }
        ExecRequest::Cancel { .. } => {
            order_answer(&FRAME.decode_value(result)?, ExecKind::AckCanceled, context)
        }
        ExecRequest::AmendQty { .. } => {
            let amended: AmendResult = FRAME.decode_value(result)?;
            order_answer(&amended.amended_order, ExecKind::AckAmended, context)
        }
        ExecRequest::OrderStatus { .. } => order_answer(
            &FRAME.decode_value(result)?,
            ExecKind::SnapshotOrder,
            context,
        ),
        ExecRequest::OpenOrders { instrument } => decode_open_orders(result, instrument, context),
        ExecRequest::SubscribeUserStream => Ok(vec![lifecycle_event(
            ExecKind::StreamReady,
            InstrumentId::NOT_APPLICABLE,
            context,
        )]),
    }
}

fn decode_open_orders(
    result: serde_json::Value,
    instrument: InstrumentId,
    context: &ResponseContext<'_>,
) -> Result<Vec<ExecEvent>, WireError> {
    let records: Vec<OrderRecord> = FRAME.decode_value(result)?;
    let mut events = Vec::with_capacity(records.len() + 1);
    for record in &records {
        events.extend(decode_order_record(
            record,
            ExecKind::SnapshotOrder,
            context.recon_seq,
            &context.decode,
        )?);
    }
    events.push(lifecycle_event(ExecKind::SnapshotEnd, instrument, context));
    Ok(events)
}

fn failure_events(code: i32, message: &str, context: &ResponseContext<'_>) -> Vec<ExecEvent> {
    let subject_kind = match context.request {
        ExecRequest::Place { .. } => RejectSubject::Placement,
        ExecRequest::Cancel { .. } => RejectSubject::Cancellation,
        ExecRequest::AmendQty { .. } => RejectSubject::Amendment,
        ExecRequest::OrderStatus { .. } => RejectSubject::StatusQuery,
        ExecRequest::OpenOrders { .. } | ExecRequest::SubscribeUserStream => return Vec::new(),
    };
    let Some(client_id) = request_client_id(context.request) else {
        return Vec::new();
    };
    vec![ExecEvent {
        client_id,
        reject: Some(classify_error(code, message, subject_kind)),
        reject_code: code,
        amends_remaining: amend_budget_remaining(code, message),
        ..lifecycle_event(
            ExecKind::AckFailed,
            request_instrument(context.request),
            context,
        )
    }]
}

/// The answer to one of this engine's own requests is about the order it asked for, so a symbol the
/// table does not know falls back to the request's instrument rather than dropping the answer.
fn order_answer(
    record: &OrderRecord,
    kind: ExecKind,
    context: &ResponseContext<'_>,
) -> Result<Vec<ExecEvent>, WireError> {
    let instrument = context
        .decode
        .symbols
        .instrument(&record.symbol)
        .unwrap_or_else(|| request_instrument(context.request));
    let event = order_event(record, kind, instrument, context.recon_seq, &context.decode)?;
    Ok(vec![event])
}

fn order_event(
    record: &OrderRecord,
    kind: ExecKind,
    instrument: InstrumentId,
    recon_seq: u64,
    context: &DecodeContext<'_>,
) -> Result<ExecEvent, WireError> {
    // A cancel or amend answer names the acting request in `clientOrderId` and the order it acted
    // on in `origClientOrderId`. Only the original id addresses a slot.
    let subject = record
        .orig_client_order_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .unwrap_or(&record.client_order_id);
    let ownership = classify_client_order_id(subject, context.identity);
    let venue_ms = record
        .transact_time_ms
        .or(record.update_time_ms)
        .or(record.time_ms);

    Ok(ExecEvent {
        venue_order_id: (record.order_id >= 0).then_some(VenueOrderId(record.order_id)),
        status: Some(venue_status("status", &record.status)?),
        provenance: ownership.provenance,
        side: order_side("side", &record.side)?,
        price: price_field("price", &record.price)?,
        qty: qty_field("origQty", &record.orig_qty)?,
        cumulative_qty: qty_field("executedQty", &record.executed_qty)?,
        cumulative_quote: money_field("cummulativeQuoteQty", &record.cumulative_quote_qty)?,
        recon_seq,
        exchange_ts_us: clamp_or_receipt(venue_ms, context.received_ts_us),
        ..blank(
            kind,
            instrument,
            ownership.client_id,
            context.received_ts_us,
        )
    })
}

fn lifecycle_event(
    kind: ExecKind,
    instrument: InstrumentId,
    context: &ResponseContext<'_>,
) -> ExecEvent {
    ExecEvent {
        side: request_side(context.request).unwrap_or(Side::Buy),
        recon_seq: context.recon_seq,
        ..blank(
            kind,
            instrument,
            ClientOrderId(0),
            context.decode.received_ts_us,
        )
    }
}

fn request_instrument(request: ExecRequest) -> InstrumentId {
    match request {
        ExecRequest::Place { instrument, .. }
        | ExecRequest::Cancel { instrument, .. }
        | ExecRequest::AmendQty { instrument, .. }
        | ExecRequest::OrderStatus { instrument, .. }
        | ExecRequest::OpenOrders { instrument } => instrument,
        ExecRequest::SubscribeUserStream => InstrumentId::NOT_APPLICABLE,
    }
}

fn request_side(request: ExecRequest) -> Option<Side> {
    match request {
        ExecRequest::Place { side, .. } => Some(side),
        _ => None,
    }
}

fn request_client_id(request: ExecRequest) -> Option<ClientOrderId> {
    match request {
        ExecRequest::Place { client_id, .. }
        | ExecRequest::Cancel { client_id, .. }
        | ExecRequest::AmendQty { client_id, .. }
        | ExecRequest::OrderStatus { client_id, .. } => Some(client_id),
        ExecRequest::OpenOrders { .. } | ExecRequest::SubscribeUserStream => None,
    }
}
