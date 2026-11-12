//! Bar stream -> Kyle observations. The feed decides where one observation ends and the next
//! begins, and getting that boundary wrong silently halves or inverts the price-impact slope every
//! downstream sizing model reads.

use polysim::hot::quant::liquidity::{KyleEstimate, KyleFeed, KylesLambdaSpec};
use polysim::ids::Price;

const TICK: Price = Price(1_000_000); // $0.01 at the 1e-8 fixed-point scale.

fn feed() -> KyleFeed {
    KyleFeed::new(
        KylesLambdaSpec {
            window: 100,
            min_observations: 2,
            // One-sided flow is the point of the sequence below; the sign gate would refuse it.
            min_sign_fraction: 0.0,
            ..KylesLambdaSpec::default()
        },
        TICK,
    )
}

/// A trade bigger than one target closes several bars at once, and the volume clock books the
/// arrival to the FIRST of them — the ones it pours through carry a full target of notional and
/// zero arrivals (`volume_bars::on_volume_fires_once_per_closed_bar_oldest_first` pins that at the
/// producer). Every one of those bars also reaches the strategy inside a single trade dispatch, so
/// they all read the same mid.
///
/// The feed must therefore fold them into the run the arrival opened. Scoring them as observations
/// in their own right credits the trade's whole price move to its first slice and then pairs the
/// remaining slices with a mid that has not moved, which drags the slope toward zero and past it:
/// the sequence below fits +0.375 read as arrivals and -0.214 read as bars.
#[test]
fn a_bar_no_trade_arrived_in_joins_the_run_it_poured_out_of() {
    let mut feed = feed();
    // (flow, arrivals, mid) — three trades, two of which sweep through a second bar.
    let bars = [
        (1.0, 1, 100.0),
        (2.0, 1, 101.0),
        (6.0, 0, 101.0),
        (4.0, 1, 105.0),
        (12.0, 0, 105.0),
        (1.0, 1, 106.0),
    ];

    let mut last = None;
    for (flow, arrivals, mid) in bars {
        last = feed.on_bar(flow, arrivals, Some(mid));
    }

    let KyleEstimate {
        observations,
        lambda,
        intercept,
        ..
    } = last.expect("two observations reach min_observations");
    assert_eq!(
        observations, 2,
        "one observation per trade, not one per closed bar"
    );
    // Two points, (8, 1.0) and (16, 4.0): the slope is exact, not a least-squares approximation.
    assert!(
        (lambda - 0.375).abs() < 1e-12,
        "lambda {lambda}, expected the slope through the two aggregated runs"
    );
    assert!(
        (intercept - -2.0).abs() < 1e-12,
        "intercept {intercept}, expected the line through the two aggregated runs"
    );
}
