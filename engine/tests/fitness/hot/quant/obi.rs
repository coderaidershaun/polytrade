//! Order-book imbalance over the top levels and in a band around the mid. A level on the edge of
//! the band counts, each side counts its best level, and a book with no size reads 0.

use engine::hot::quant::micro::{BookSides, PriceBand};
use engine::msg::ids::{Price, Qty};
use engine::msg::inbound::Level;

const SCALE: i64 = 100_000_000;

fn level(price: i64, qty: i64) -> Level {
    Level {
        price: Price::from_mantissa(price * SCALE),
        qty: Qty::from_mantissa(qty * SCALE),
    }
}

fn banded_imbalance(bids: &[Level], asks: &[Level], band: PriceBand) -> f64 {
    BookSides { bids, asks }.banded_imbalance(band)
}

fn imbalance(bids: &[Level], asks: &[Level], top_n: usize) -> f64 {
    BookSides { bids, asks }.imbalance(top_n)
}

#[test]
fn edge_levels_are_included_and_first_beyond_excluded() {
    let band = PriceBand::around(100_000.0, 0.5);
    let bids = [
        level(99_999, 1),
        level(99_996, 2),
        level(99_994, 4),
        level(99_990, 8),
    ];
    let asks = [level(100_001, 1), level(100_005, 2), level(100_008, 5)];

    let imbalance = banded_imbalance(&bids, &asks, band);
    assert_eq!(imbalance, (7.0 - 3.0) / 10.0);
}

#[test]
fn band_always_reaches_the_first_level_of_each_side() {
    let band = PriceBand::around(100_000.0, 0.5);
    let wide_bids = [level(99_900, 3), level(99_800, 9)];
    let wide_asks = [level(100_100, 1), level(100_200, 9)];
    assert_eq!(banded_imbalance(&wide_bids, &wide_asks, band), 0.5);

    assert_eq!(banded_imbalance(&wide_bids, &[], band), 1.0);
    assert_eq!(banded_imbalance(&[], &wide_asks, band), -1.0);
    assert_eq!(banded_imbalance(&[], &[], band), 0.0);
}

/// The imbalance of the top levels of each side. A book with no size reads 0, never 0 / 0, and a
/// book with one side reads that side alone.
#[test]
fn top_level_imbalance_reads_zero_when_the_book_holds_no_size() {
    let bids = [level(99_999, 3), level(99_998, 1), level(99_990, 50)];
    let asks = [level(100_001, 1), level(100_005, 50)];
    // The top two levels: 3 + 1 on the bid against 1 + 50 on the ask.
    assert_eq!(imbalance(&bids, &asks, 2), (4.0 - 51.0) / 55.0);
    assert_eq!(imbalance(&bids, &[], 2), 1.0);
    assert_eq!(imbalance(&[], &asks, 2), -1.0);
    assert_eq!(imbalance(&[], &[], 2), 0.0);
    assert_eq!(imbalance(&bids, &asks, 0), 0.0, "no level counted");
    let sizeless = [level(99_999, 0)];
    assert_eq!(
        imbalance(&sizeless, &sizeless, 1),
        0.0,
        "levels with no size"
    );
}
