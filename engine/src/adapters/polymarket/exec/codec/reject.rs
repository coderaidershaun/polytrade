//! Maps a venue error onto a reject class. This venue names no numeric codes, so
//! classification matches on HTTP status and message prefix instead.
//!
//! An unrecognised 4xx is Fatal, because retrying an error we cannot name would flood duplicate
//! orders. An unrecognised 5xx is Ambiguous, because the gateway may have accepted the order even
//! though the response failed.
//!
//! HTTP 425/429/503 are venue states, not order rejects. They leave as VenueAvailability and never
//! hit the hot path, so they never feed the reject streak or park the engine during outages.

pub use crate::adapters::exec::{RejectVerdict, VenueAvailability};

use crate::msg::exec::RejectClass;

/// The same message can mean different things depending on which request it answers — for
/// example, "order match delayed" is a taker hold on a placement, but a refusal on a cancel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectSubject {
    Placement,
    Cancellation,
    Read,
}

/// The error exactly as the venue spells it. `code` is a string, since there are no numeric
/// codes here, and only the 503 family of statuses ever carries one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VenueFailure<'a> {
    pub status: u16,
    pub message: &'a str,
    pub code: &'a str,
    pub retry_after_secs: Option<i64>,
}

impl<'a> VenueFailure<'a> {
    /// For the placement path, which carries only a message; `code` and `retry_after_secs`
    /// are only ever populated for status 503 and above.
    pub fn new(status: u16, message: &'a str) -> Self {
        Self {
            status,
            message,
            code: "",
            retry_after_secs: None,
        }
    }
}

pub fn classify_error(failure: VenueFailure<'_>, subject: RejectSubject) -> RejectVerdict {
    if let Some(availability) = venue_availability(failure) {
        return RejectVerdict::Venue(availability);
    }
    RejectVerdict::Order(
        order_class(failure.message, subject).unwrap_or_else(|| unmatched_class(failure.status)),
    )
}

/// The classification used when the message itself is not recognized. On a 5xx, the gateway
/// may have already accepted the order, so Fatal would orphan it — Ambiguous instead nudges
/// a resync and a probe to find it, matching the transport-failure path. Below 500, the
/// message describes a bad request that would fail identically on retry, so it is Fatal.
fn unmatched_class(status: u16) -> RejectClass {
    match status >= 500 {
        true => RejectClass::Ambiguous,
        false => RejectClass::Fatal,
    }
}

fn venue_availability(failure: VenueFailure<'_>) -> Option<VenueAvailability> {
    let lowered = failure.message.to_ascii_lowercase();
    let retry_after_secs = failure.retry_after_secs;
    match failure.status {
        425 => Some(VenueAvailability::Restarting),
        429 => Some(VenueAvailability::RateLimited { retry_after_secs }),
        503 if failure.code == "post_only_mode" || lowered.contains("post-only mode") => {
            Some(VenueAvailability::PostOnlyMode { retry_after_secs })
        }
        503 if lowered.contains("cancel-only") => Some(VenueAvailability::CancelOnly),
        503 => Some(VenueAvailability::TradingDisabled),
        _ => None,
    }
}

/// Message fragments and the class each draws. The first row with a fragment the lowered message
/// contains decides, so the order of the rows matters.
const MESSAGE_RULES: &[(&[&str], RejectClass)] = &[
    (
        &[
            // Auth, signature, payload, and account failures all fail identically on retry.
            "unauthorized",
            "invalid api key",
            "invalid l1 request headers",
            "invalid order payload",
            "has to be the owner of the api key",
            "has to be the address of the api key",
            "address banned",
            "in closed only mode",
            "invalid expiration",
            // Funding is fatal. The sell gate depends on allowance refresh, and quoting through an
            // empty wallet burns the reject budget unnecessarily.
            "not enough balance",
            "allowance",
            // A grid violation: preflight already stamps tick size and minimum, so a venue
            // disagreement here means those stamps were wrong.
            "minimum tick size rule",
            "lower than the minimum",
            "breaks minimum",
        ],
        RejectClass::Fatal,
    ),
    // A duplicate order shares the same signed fields and timestamp as one already sent. The first
    // one is likely resting under an id this run never received, so it is Ambiguous; resync will
    // find it.
    (&["duplicated"], RejectClass::Ambiguous),
    (
        &[
            // The routine cost of post-only quoting, or a marketable order with no counterparty.
            // The level-triggered declaration will resubmit on its own: this slot simply closes and
            // the next spin decides again.
            "post-only order",
            "fok orders are fully filled or killed",
            "no orders found to match",
            "there are no matching orders",
            "rounding issues",
            "not yet ready to process new orders",
            // A 500 "order timed out" means the order never reached the book. This is Refused
            // rather than Ambiguous, since there is no probe handle for it and Ambiguous would
            // stall the slot.
            "order timed out",
        ],
        RejectClass::Refused,
    ),
    // A taker hold: the order was accepted and cannot be cancelled until the hold lapses. This is
    // StillLive on every subject, because closing the slot would abandon an order the venue is about
    // to match; the edge withholds the cancel itself until the hold lapses.
    (&["order match delayed"], RejectClass::StillLive),
    // The order was cancelled on-chain, outside the CLOB. It is definitively gone.
    (
        &["canceled in the ctf exchange contract"],
        RejectClass::Gone,
    ),
];

/// The verdict a KNOWN message draws, or `None` when this engine cannot name it — the caller then
/// decides from the HTTP status (see [`unmatched_class`]), which the message alone cannot see.
fn order_class(message: &str, subject: RejectSubject) -> Option<RejectClass> {
    let lowered = message.to_ascii_lowercase();
    let rule = MESSAGE_RULES
        .iter()
        .find(|(fragments, _)| fragments.iter().any(|fragment| lowered.contains(fragment)));
    if let Some(&(_, class)) = rule {
        return Some(class);
    }

    // "not found" is definitive only on the cancel path. Elsewhere it claims the request was bad.
    if lowered.contains("order not found") || lowered.contains("no such order") {
        return Some(match subject {
            RejectSubject::Cancellation => RejectClass::Gone,
            _ => RejectClass::Ambiguous,
        });
    }

    None
}

/// Reads a refusal from a cancel response. Partial success is by design here: each decline
/// carries its own verdict.
pub(super) fn cancel_refusal(reason: &str) -> RejectClass {
    let lowered = reason.to_ascii_lowercase();
    // Our decision to cancel and the venue's own read of the order disagree because it
    // matched first. This is Ambiguous: the fill can land after this run considered the
    // slot closed, and resync will find it.
    if lowered.contains("already matched") {
        return RejectClass::Ambiguous;
    }
    // The not_canceled reason rides in a 200 response, so there is no failing status to read
    // here. An unrecognized decline is classified Fatal, the conservative direction for an
    // order this run cannot account for.
    order_class(reason, RejectSubject::Cancellation).unwrap_or(RejectClass::Fatal)
}
