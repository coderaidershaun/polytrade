use polysim::hot::quant::micro::{PriceBand, banded_imbalance};
use polysim::ids::{Price, Qty};
use polysim::msg::inbound::Level;

const SCALE: i64 = 100_000_000;

fn level(price: i64, qty: i64) -> Level {
    Level {
        price: Price(price * SCALE),
        qty: Qty(qty * SCALE),
    }
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
