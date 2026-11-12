//! VPIN over completed volume bars. The estimator is a pure ratio: a wrong denominator or an
//! ungated zero-target window produces a plausible-looking toxicity number (or a NaN) that lands in
//! the research tape unremarked.

use polysim::hot::quant::toxicity::vpin;
use polysim::hot::tracker::VolumeBar;
use polysim::time::TsUs;

fn bar(buy: i64, sell: i64, target: i64) -> VolumeBar {
    VolumeBar {
        open_ts_us: TsUs::from_micros(0),
        close_ts_us: TsUs::from_micros(0),
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
    // 5 bars, targets 1000 each: Σ|B−S| = 1800, Σtarget = 5000 → VPIN 0.36. Signs alternate.
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
    // Zero Σtarget gates to None, not 0.0/0.0 = NaN.
    assert!(vpin(&[bar(0, 0, 0)], 1).is_none(), "zero total target");
}
