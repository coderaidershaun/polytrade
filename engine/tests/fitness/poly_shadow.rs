//! Collapse detector + shadow-book validator: silent book corruption is the failure mode both guard.
//! The detector must fire only on a genuine same-ts removal burst, and the validator must catch any
//! divergence between the forwarded stream and the venue's authoritative book. The comparison is
//! deliberately order-sensitive — it expects the venue's native ascending-by-price arrays, so
//! callers reverse best-first bids before validating.

use polysim::adapters::polymarket::shadow::{BookFrameOutcome, ShadowValidator};
use polysim::adapters::polymarket::teardown::{CollapseDetector, CollapseSignal, LevelUpdate};
use polysim::ids::{Price, Qty, Side};
use polysim::time::TsUs;

use crate::engine_support::level;

fn update(side: Side, price: i64, qty: i64, venue_ts: i64) -> LevelUpdate {
    LevelUpdate {
        side,
        price: Price(price),
        qty: Qty(qty),
        exchange_ts_us: TsUs::from_micros(venue_ts),
    }
}

fn build_two_sided(detector: &mut CollapseDetector, venue_ts: i64) {
    for update in [
        update(Side::Buy, 50_000_000, 100, venue_ts),
        update(Side::Buy, 49_000_000, 200, venue_ts),
        update(Side::Sell, 51_000_000, 150, venue_ts),
        update(Side::Sell, 52_000_000, 250, venue_ts),
    ] {
        assert_eq!(detector.observe(update), CollapseSignal::Quiet);
    }
}

#[test]
fn same_ts_removal_burst_emptying_a_side_collapses() {
    let mut detector = CollapseDetector::default();
    build_two_sided(&mut detector, 1_000);

    // Burst at a single venue ts wipes the bid side: the second removal empties it.
    assert_eq!(
        detector.observe(update(Side::Buy, 50_000_000, 0, 2_000)),
        CollapseSignal::Quiet
    );
    assert_eq!(
        detector.observe(update(Side::Buy, 49_000_000, 0, 2_000)),
        CollapseSignal::Collapsed
    );
    assert!(detector.has_collapsed());
}

#[test]
fn only_a_confirmed_desync_resets_the_shadow() {
    let bids = [level(50_000_000, 100)];
    let asks = [level(51_000_000, 150)];

    let mut validator = ShadowValidator::default();
    assert_eq!(
        validator.on_venue_book(&bids, &asks),
        BookFrameOutcome::ForwardSnapshot
    );

    // One venue cut disagrees (a ~150ms snapshot caught mid-flight) — suspected, not yet a desync.
    let stale_bids = [level(50_000_000, 80)];
    assert_eq!(
        validator.on_venue_book(&stale_bids, &asks),
        BookFrameOutcome::Validated
    );
    // The next cut agrees again (the delta stream never desynced) — the suspicion clears, no reset.
    assert_eq!(
        validator.on_venue_book(&bids, &asks),
        BookFrameOutcome::Validated
    );

    let mut validator = ShadowValidator::default();
    assert_eq!(
        validator.on_venue_book(&bids, &asks),
        BookFrameOutcome::ForwardSnapshot
    );

    // A real desync mismatches every cut; the first two are suspected silently (shadow untouched).
    let desynced = [level(50_000_000, 80)];
    assert_eq!(
        validator.on_venue_book(&desynced, &asks),
        BookFrameOutcome::Validated
    );
    assert_eq!(
        validator.on_venue_book(&desynced, &asks),
        BookFrameOutcome::Validated
    );

    // The third confirms it: exactly one reset, and the shadow re-baselines to the venue.
    assert!(matches!(
        validator.on_venue_book(&desynced, &asks),
        BookFrameOutcome::Diverged(_)
    ));
    assert_eq!(
        validator.on_venue_book(&desynced, &asks),
        BookFrameOutcome::Validated
    );

    let mut validator = ShadowValidator::default();
    assert_eq!(
        validator.on_venue_book(&bids, &asks),
        BookFrameOutcome::ForwardSnapshot
    );

    // A stray zero-size level is no level — it must not diverge (else every ~150ms frame would).
    let venue_asks = [level(51_000_000, 150), level(52_000_000, 0)];
    assert_eq!(
        validator.on_venue_book(&bids, &venue_asks),
        BookFrameOutcome::Validated
    );
}
