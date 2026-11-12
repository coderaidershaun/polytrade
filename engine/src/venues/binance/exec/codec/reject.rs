//! Maps Binance error codes onto reject classes. The mapping is small, but every entry is
//! consequential — several codes carry meanings that differ by request type or by message text.

use crate::msg::exec::{ExecEvent, RejectClass};

/// Distinguishes what kind of request failed, since the same Binance error code can mean
/// different things depending on it — -2013 is only definitive when it answers a status query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RejectSubject {
    Placement,
    Cancellation,
    Amendment,
    StatusQuery,
}

/// Classifies a Binance error code into a reject class. Public because the driver classifies
/// transport failures (no answer at all) separately, and the two classifications must agree.
pub fn classify_error(code: i32, message: &str, subject: RejectSubject) -> RejectClass {
    match code {
        // The gateway rejected this before it reached the match engine, so the order's state
        // is unchanged; the engine's normal spin cadence will simply retry it.
        -1003 | -1021 | -1015 => RejectClass::StillLive,
        // Binance calls this "execution status unknown" — the request may have gone through.
        // A placement is Ambiguous under that doubt; every other request type stays StillLive.
        -1006 | -1007 => match subject {
            RejectSubject::Placement => RejectClass::Ambiguous,
            _ => RejectClass::StillLive,
        },
        // The amend was refused but the order is still resting. The specific reason is read
        // from the message text by amend_budget_remaining().
        -2038 => RejectClass::StillLive,
        // This engine's own gates should already catch what triggers -1013, so seeing it here
        // means the same parameters would fail forever; retrying would just mean an outage
        // went unnoticed.
        -1013 => RejectClass::Fatal,
        -1022 | -2015 | -2014 => RejectClass::Fatal,
        -2010 => insufficient_balance_or_cross(message),
        // Binance's "unknown order sent" collides with an order that has actually FILLED.
        // Reading this as Gone would leave the ledger wrong forever, so it is probed instead.
        -2011 => RejectClass::Ambiguous,
        // This code is only definitive when it answers a status query. Otherwise "not there"
        // is a claim about the request, not the order, so it stays Ambiguous.
        -2013 => match subject {
            RejectSubject::StatusQuery => RejectClass::Gone,
            _ => RejectClass::Ambiguous,
        },
        _ => RejectClass::Fatal,
    }
}

/// Reads the remaining amend budget from a -2038 message. Zero means the ceiling has been
/// hit; anything unreadable reports the sentinel AMENDS_UNKNOWN rather than a value that
/// could be mistaken for a real budget.
pub(super) fn amend_budget_remaining(code: i32, message: &str) -> u8 {
    if code != -2038 {
        return ExecEvent::AMENDS_UNKNOWN;
    }
    // The message text naming the filter is the only definitive statement Binance makes here.
    match message
        .to_ascii_uppercase()
        .contains("MAX_NUM_ORDER_AMENDS")
    {
        true => ExecEvent::AMENDS_EXHAUSTED,
        false => ExecEvent::AMENDS_UNKNOWN,
    }
}

/// A -2010 means either a self-cross or insufficient balance; the message text decides
/// which. A cross is Refused, and anything else defaults to Fatal as the safer direction.
fn insufficient_balance_or_cross(message: &str) -> RejectClass {
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("insufficient balance") {
        return RejectClass::Fatal;
    }
    match lowered.contains("would immediately match") {
        true => RejectClass::Refused,
        false => RejectClass::Fatal,
    }
}
