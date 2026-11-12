//! Safe venue timestamp conversion.

use crate::time::TsUs;

/// Max venue-time skew before saturation (prevents overflow on diff). 400d = REST backfill OK.
pub const MAX_VENUE_SKEW_US: i64 = 400 * 24 * 60 * 60 * 1_000_000;

/// ms->µs [now ± skew], saturating. The route for OBSERVATION stamps — when something happened.
pub fn clamp_exchange_ts(venue_ms: i64, now: TsUs) -> TsUs {
    let venue_us = venue_ms.saturating_mul(1_000);
    let lo = now.micros().saturating_sub(MAX_VENUE_SKEW_US);
    let hi = now.micros().saturating_add(MAX_VENUE_SKEW_US);
    TsUs::from_micros(venue_us.clamp(lo, hi))
}

/// The venue's own stamp when it sent one, else the moment this process received the message.
pub(crate) fn clamp_or_receipt(venue_ms: Option<i64>, received_ts_us: TsUs) -> TsUs {
    venue_ms.map_or(received_ts_us, |venue_ms| {
        clamp_exchange_ts(venue_ms, received_ts_us)
    })
}

/// ms->µs for a BOUNDARY — a bar's grid coordinate, which identifies it rather than dating it.
///
/// Deliberately unclamped, and the second route exists so the choice is visible. Snapping a
/// boundary to `now ± skew` would land several bars on one coordinate, and the kline sequencer
/// reads gaps and duplicates off exactly that value: a backfill reaching past the skew window
/// would collapse its oldest bars together and be read as a duplicate run rather than as old data.
/// A stale boundary is recoverable; a colliding one is not.
pub fn boundary_ts(venue_ms: i64) -> TsUs {
    TsUs::from_micros(venue_ms.saturating_mul(1_000))
}
