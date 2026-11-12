//! Zero-allocation fitness for the Kyle's lambda estimator: once the two rolling windows
//! exist, pushing completed bars and refitting the OLS slope over the live window touches the
//! allocator not at all — including across the `#[cold]` copy-back that fires every time the
//! oversized backing fills, which the measured region crosses many times over.
//!
//! A liveness counter rides alongside the allocation assertion: an estimator that quietly gated
//! every call to `None` would satisfy "allocated nothing" perfectly.

use crate::quant::Lcg;
use polysim::hot::quant::liquidity::{KylesLambda, KylesLambdaSpec};
use polysim::ids::Price;

const TICK: Price = Price(1_000_000); // $0.01 at the 1e-8 fixed-point scale.

const WINDOW: usize = 256;

/// Pushes before the measured region: enough to fill the window twice over, so every estimate
/// inside the region runs on a full, already-evicting buffer.
const PRIME: usize = 2 * WINDOW;

/// Pushes inside the measured region, each followed by a refit.
const MEASURED: usize = 50_000;

/// One well-conditioned bar: two-sided flow around a λ = 0.0002 impact line with light noise.
fn bar(lcg: &mut Lcg) -> (f64, f64) {
    let flow = (lcg.unit() - 0.5) * 200.0;
    (flow, 0.0002 * flow + (lcg.unit() - 0.5) * 2e-4)
}

#[test]
fn kyles_lambda_push_and_fit_do_not_allocate() {
    let mut lcg = Lcg(0xF17E_5510);
    let mut estimator = KylesLambda::new(
        KylesLambdaSpec {
            window: WINDOW,
            min_observations: 64,
            ..KylesLambdaSpec::default()
        },
        TICK,
    );

    for _ in 0..PRIME {
        let (flow, mid_change) = bar(&mut lcg);
        estimator.push(flow, mid_change);
    }
    // The first estimate is an initialisation event, not steady state.
    estimator.fit().expect("primed window estimates");

    let mut estimates = 0u64;
    let before = crate::alloc_count();
    for _ in 0..MEASURED {
        let (flow, mid_change) = bar(&mut lcg);
        estimator.push(flow, mid_change);
        estimates += u64::from(estimator.fit().is_some());
    }
    let after = crate::alloc_count();

    assert_eq!(
        after, before,
        "kyles lambda push or estimate allocated in steady state"
    );
    assert!(
        estimates > 0,
        "kyles lambda produced no estimate in the measured window"
    );

    // The window really was evicting throughout, so the refits ran on a sliding buffer.
    assert_eq!(estimator.len(), WINDOW);
}
