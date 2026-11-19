//! VPIN over completed volume bars: a wrong denominator, or a window with no target, gives a
//! toxicity number that looks right, or a NaN, in the research tape.

use engine::hot::quant::toxicity::VolumeBar;
use engine::hot::quant::toxicity::vpin;
use engine::msg::time::VenueTs;

fn bar(buy: i64, sell: i64, target: i64) -> VolumeBar {
    VolumeBar {
        open_ts_us: VenueTs::from_micros(0),
        close_ts_us: VenueTs::from_micros(0),
        buy_notional: buy,
        sell_notional: sell,
        target,
        trade_arrivals: 0,
    }
}

fn approx(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

#[test]
fn worked_example_matches_hand_arithmetic() {
    // Five bars with a target of 1000 each: Σ|B−S| = 1800 over Σtarget = 5000 gives VPIN 0.36. The
    // signs alternate.
    let bars = [
        bar(700, 300, 1000),
        bar(400, 600, 1000),
        bar(800, 200, 1000),
        bar(450, 550, 1000),
        bar(750, 250, 1000),
    ];
    let est = vpin(&bars, 5).expect("five bars, positive target");
    assert!(approx(est.vpin, 0.36), "vpin {}", est.vpin);
    assert!(
        approx(est.signed_flow, 0.24),
        "signed_flow {}",
        est.signed_flow
    );

    // Six bars and five buckets: the oldest bar, all buying, is outside the window, so the values
    // are those of the five bars again. With it, VPIN would be 2800 / 6000.
    let with_older = [
        bar(1000, 0, 1000),
        bars[0],
        bars[1],
        bars[2],
        bars[3],
        bars[4],
    ];
    let est = vpin(&with_older, 5).expect("six bars, positive target");
    assert!(approx(est.vpin, 0.36), "vpin {}", est.vpin);
    assert!(
        approx(est.signed_flow, 0.24),
        "signed_flow {}",
        est.signed_flow
    );

    // Targets that differ weight each bar by its target: Σ|B−S| = 400 + 1000 = 1400 over
    // Σtarget = 1000 + 3000 = 4000 gives 0.35, and Σ(B−S) = 400 − 1000 = −600 gives −0.15. A mean
    // of the two bar ratios would give (0.4 + 1/3) / 2.
    let unequal = [bar(700, 300, 1000), bar(1000, 2000, 3000)];
    let est = vpin(&unequal, 2).expect("two bars, positive target");
    assert!(approx(est.vpin, 0.35), "vpin {}", est.vpin);
    assert!(
        approx(est.signed_flow, -0.15),
        "signed_flow {}",
        est.signed_flow
    );
}

#[test]
fn insufficient_data_gates_to_none() {
    let bars = [
        bar(700, 300, 1000),
        bar(400, 600, 1000),
        bar(800, 200, 1000),
    ];
    assert!(vpin(&bars, 5).is_none(), "fewer bars than buckets");
    assert!(vpin(&bars, 0).is_none(), "zero buckets");
    assert!(vpin(&[], 1).is_none(), "empty slice");
    // A Σtarget of zero gives None, not the NaN of 0.0 / 0.0.
    assert!(vpin(&[bar(0, 0, 0)], 1).is_none(), "zero total target");
}
