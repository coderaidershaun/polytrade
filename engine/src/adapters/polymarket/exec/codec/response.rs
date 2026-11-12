//! Decoders for placement and cancellation responses.
//!
//! Fill law: a taker fill folds from the placement response's own amounts, which are the
//! sole report of it. A maker fill instead folds from the order stream's cumulative
//! size_matched. Trade payloads carry lineage, fees, and settlement only — the same id
//! repeats at every settlement step, so it is never itself accumulated.

use crate::adapters::decode::unknown_enum;
use crate::adapters::exec::blank;
use crate::ids::{ClientOrderId, InstrumentId, Price, Qty, Side};
use crate::msg::exec::{ExecEvent, ExecKind, VenueOrderStatus};

use super::correlation::venue_order_id_digest;
use super::reject::{RejectSubject, VenueFailure, cancel_refusal, classify_error};
use super::wire::{CancelResponse, ErrorResponse, PlaceResponse};
use super::{
    DecodeContext, FRAME, HttpAnswer, RejectVerdict, VenueAnswer, WireError, optional_venue_amount,
    status_with_fill,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlacementStatus {
    Live,
    Delayed,
    Unmatched,
    Matched,
}

/// A placement the venue accepted, with the id every later message will use to name it.
#[derive(Debug, Clone, PartialEq)]
pub struct PlaceOutcome {
    pub event: ExecEvent,
    /// `None` on a refusal, since there is neither an order nor an id to report.
    pub placed: Option<PlacedOrder>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedOrder {
    pub client_id: ClientOrderId,
    pub venue_order_id: Box<str>,
    pub status: PlacementStatus,
}

/// # Errors
/// Malformed JSON, a decimal the engine's scale cannot hold, or an unknown status spelling.
pub fn decode_place(
    answer: HttpAnswer<'_>,
    request: &PlaceRequestContext,
    context: &DecodeContext<'_>,
) -> Result<VenueAnswer<PlaceOutcome>, WireError> {
    if let Some(failure) = decode_failure(answer, RejectSubject::Placement, context)? {
        return Ok(failure.map(|event| PlaceOutcome {
            event: ExecEvent {
                client_id: request.client_id,
                instrument: request.instrument,
                side: request.side,
                price: request.price,
                qty: request.qty,
                ..event
            },
            placed: None,
        }));
    }

    let response: PlaceResponse = FRAME.decode(answer.body)?;
    // HTTP 200 with `success: false` is a documented shape, and `errorMsg` is populated even where
    // `success` is true in post-only mode. Neither field decides alone.
    if !response.error_msg.is_empty() || !response.success {
        let verdict = classify_error(
            VenueFailure::new(answer.status, &response.error_msg),
            RejectSubject::Placement,
        );
        return Ok(
            reject_answer(verdict, request, context).map(|event| PlaceOutcome {
                event,
                placed: None,
            }),
        );
    }

    let status = placement_status(&response.status)?;
    // makingAmount and takingAmount are what we gave and what we got. Which one holds shares and
    // which holds pUSD depends on our side.
    let making = optional_venue_amount("makingAmount", &response.making_amount)?;
    let taking = optional_venue_amount("takingAmount", &response.taking_amount)?;
    let (filled_qty, filled_quote) = match request.side {
        Side::Buy => (Qty(taking), making),
        Side::Sell => (Qty(making), taking),
    };

    let venue_status = match status {
        PlacementStatus::Matched => VenueOrderStatus::Filled,
        _ => VenueOrderStatus::New,
    };
    let event = ExecEvent {
        venue_order_id: Some(venue_order_id_digest(&response.order_id)),
        status: Some(status_with_fill(venue_status, filled_qty, request.qty)),
        side: request.side,
        price: request.price,
        qty: request.qty,
        // Taker fill reported once here; trade events are settlement progress only.
        last_price: request.price,
        last_qty: filled_qty,
        cumulative_qty: filled_qty,
        cumulative_quote: filled_quote,
        ..blank_event(
            ExecKind::AckPlaced,
            request.instrument,
            request.client_id,
            context,
        )
    };
    Ok(VenueAnswer::Answered(PlaceOutcome {
        event,
        placed: Some(PlacedOrder {
            client_id: request.client_id,
            venue_order_id: response.order_id.into(),
            status,
        }),
    }))
}

/// Request context needed for attribution, since the venue echoes back neither this run's
/// client id nor the price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlaceRequestContext {
    pub instrument: InstrumentId,
    pub client_id: ClientOrderId,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
}

/// # Errors
/// Malformed JSON, or an id the venue named that this run never placed.
pub fn decode_cancel(
    answer: HttpAnswer<'_>,
    context: &DecodeContext<'_>,
) -> Result<VenueAnswer<Vec<ExecEvent>>, WireError> {
    if let Some(failure) = decode_failure(answer, RejectSubject::Cancellation, context)? {
        return Ok(failure.map(|event| vec![event]));
    }
    let response: CancelResponse = FRAME.decode(answer.body)?;
    let mut events = Vec::with_capacity(response.canceled.len() + response.not_canceled.len());
    for venue_order_id in &response.canceled {
        let Some(known) = context.orders.resolve(venue_order_id) else {
            continue;
        };
        events.push(ExecEvent {
            venue_order_id: Some(venue_order_id_digest(venue_order_id)),
            status: Some(VenueOrderStatus::Canceled),
            ..blank_event(
                ExecKind::AckCanceled,
                known.instrument,
                known.client_id,
                context,
            )
        });
    }
    // Partial success is this venue's design; each decline carries its own verdict.
    for (venue_order_id, reason) in &response.not_canceled {
        let Some(known) = context.orders.resolve(venue_order_id) else {
            continue;
        };
        events.push(ExecEvent {
            venue_order_id: Some(venue_order_id_digest(venue_order_id)),
            reject: Some(cancel_refusal(reason)),
            ..blank_event(
                ExecKind::AckFailed,
                known.instrument,
                known.client_id,
                context,
            )
        });
    }
    Ok(VenueAnswer::Answered(events))
}

/// The shared failure path for every call. `Ok(None)` means the answer is normal, and the
/// specific decoder should go on to read it.
pub(super) fn decode_failure(
    answer: HttpAnswer<'_>,
    subject: RejectSubject,
    context: &DecodeContext<'_>,
) -> Result<Option<VenueAnswer<ExecEvent>>, WireError> {
    if (200..300).contains(&answer.status) {
        return Ok(None);
    }
    // On 425 or 429, the body may be empty or plain text. Error shape is optional here but
    // required on a 400.
    let error: ErrorResponse = FRAME.decode(answer.body).unwrap_or(ErrorResponse {
        error: answer.body.trim().into(),
        error_msg: String::new(),
        code: String::new(),
        retry_after_seconds: None,
    });
    let verdict = classify_error(
        VenueFailure {
            status: answer.status,
            message: error.message(),
            code: &error.code,
            retry_after_secs: error.retry_after_seconds,
        },
        subject,
    );
    Ok(Some(match verdict {
        RejectVerdict::Venue(availability) => VenueAnswer::Unavailable(availability),
        RejectVerdict::Order(class) => VenueAnswer::Answered(ExecEvent {
            reject: Some(class),
            ..blank_event(
                ExecKind::AckFailed,
                InstrumentId::NOT_APPLICABLE,
                ClientOrderId(0),
                context,
            )
        }),
    }))
}

fn reject_answer(
    verdict: RejectVerdict,
    request: &PlaceRequestContext,
    context: &DecodeContext<'_>,
) -> VenueAnswer<ExecEvent> {
    match verdict {
        RejectVerdict::Venue(availability) => VenueAnswer::Unavailable(availability),
        RejectVerdict::Order(class) => VenueAnswer::Answered(ExecEvent {
            reject: Some(class),
            side: request.side,
            price: request.price,
            qty: request.qty,
            ..blank_event(
                ExecKind::AckFailed,
                request.instrument,
                request.client_id,
                context,
            )
        }),
    }
}

/// This venue has no amend, so every event it raises says the amend budget is spent.
pub(super) fn blank_event(
    kind: ExecKind,
    instrument: InstrumentId,
    client_id: ClientOrderId,
    context: &DecodeContext<'_>,
) -> ExecEvent {
    ExecEvent {
        amends_remaining: ExecEvent::AMENDS_EXHAUSTED,
        ..blank(kind, instrument, client_id, context.received_ts_us)
    }
}

fn placement_status(status: &str) -> Result<PlacementStatus, WireError> {
    Ok(match status.to_ascii_lowercase().as_str() {
        "live" => PlacementStatus::Live,
        "delayed" => PlacementStatus::Delayed,
        "unmatched" => PlacementStatus::Unmatched,
        "matched" => PlacementStatus::Matched,
        // The venue's own spelling, not the lowercased one it was matched against: an operator
        // chasing an unknown status needs the string that was actually sent.
        _ => return Err(unknown_enum("status", status).into()),
    })
}
