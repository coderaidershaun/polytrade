//! Zero-allocation fitness for the Hawkes monitor's live path: once the event window and the
//! fitter's warm-start cache exist, streaming events into the window and refitting touches the
//! allocator not at all. The cold fit is primed OUTSIDE the measured window, because a cold fit is
//! an initialisation event, not steady state.
//!
//! Liveness counters ride alongside the allocation assertion: a fitter that quietly stopped
//! producing estimates would satisfy "allocated nothing" perfectly, so it must be shown to have
//! issued estimates AND to have converged at least once inside the measured window.

use crate::quant::ExpSimulation;
use polysim::hot::quant::hawkes::{HawkesEvents, HawkesMle, HawkesParams};
use polysim::time::{DurationUs, TsUs};

const START: TsUs = TsUs::from_micros(0);

/// Shorter than a production window on purpose: the fit's cost scales with it, and what this test
/// proves (no allocation, live fits) does not depend on window length, so the suite buys nothing
/// by being slower.
const WINDOW: usize = 1024;

/// Pushes before the measured region: enough to fill the window twice over, so the fits inside the
/// region see a fully-populated, already-evicting buffer.
const PRIME: usize = 2 * WINDOW;

/// Pushes inside the measured region.
const MEASURED: usize = 50_000;

/// Pushes between refits: 20 refits across the region. Each refit's window has turned over more
/// than twice, so no refit is trivially warm on unchanged data.
const FIT_STRIDE: usize = 2_500;

/// Repeats a simulated path end-to-end, shifting each lap past the previous one so the stamps stay
/// non-decreasing — an out-of-order stamp would be clamped and collapse the fit window's span.
fn tile(path: &[TsUs], wanted: usize) -> Vec<TsUs> {
    let lap = path[path.len() - 1].micros() - path[0].micros() + 1_000_000;
    (0..wanted)
        .map(|index| {
            TsUs::from_micros(path[index % path.len()].micros() + (index / path.len()) as i64 * lap)
        })
        .collect()
}

fn exponential_path() -> Vec<TsUs> {
    ExpSimulation {
        params: HawkesParams::new(0.5, 0.8, 2.0),
        start_ts: START,
        horizon: DurationUs::from_secs(20_000),
        seed: 0xF17E_5501,
        max_events: 40_000,
    }
    .run()
}

#[test]
fn hawkes_accumulators_and_warm_fits_do_not_allocate() {
    let stamps = tile(&exponential_path(), PRIME + MEASURED);
    let mut events = HawkesEvents::new(WINDOW);
    let mut mle = HawkesMle::new(64);

    for &stamp in &stamps[..PRIME] {
        events.push(stamp);
    }
    // Cold fit: allocation is allowed here, and the warm-start cache is populated by it.
    mle.fit(&events, stamps[PRIME - 1]).expect("cold mle fit");

    let mut estimates = 0u64;
    let mut converged = 0u64;
    let before = crate::alloc_count();
    for (index, &stamp) in stamps.iter().enumerate().skip(PRIME) {
        events.push(stamp);
        if index % FIT_STRIDE != 0 {
            continue;
        }
        if let Some(estimate) = mle.fit(&events, stamp) {
            estimates += 1;
            converged += u64::from(estimate.converged);
        }
    }
    let after = crate::alloc_count();

    assert_eq!(
        after, before,
        "hawkes window or warm fit allocated in steady state"
    );
    assert!(
        estimates > 0,
        "hawkes mle produced no estimate in the measured window"
    );
    assert!(
        converged > 0,
        "hawkes mle never converged in the measured window"
    );

    // The window really was evicting throughout, so the fits ran on a sliding buffer rather than a
    // frozen one that would have made every refit trivially warm.
    assert_eq!(events.len(), WINDOW);
    assert_eq!(events.out_of_order_count(), 0);
}
