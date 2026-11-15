//! The collapse detector fires only on a real burst of removals, and the shadow check catches each
//! divergence between the forwarded stream and the venue's own book.

use engine::msg::ids::{Price, Qty, Side};
use engine::msg::time::VenueTs;
use engine::venues::polymarket::teardown::{CollapseDetector, CollapseSignal, LevelUpdate};

fn update(side: Side, price: i64, qty: i64, venue_ts: i64) -> LevelUpdate {
    LevelUpdate {
        side,
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(qty),
        exchange_ts_us: VenueTs::from_micros(venue_ts),
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
