//! FastQueue fitness: it never loses or reorders data, `as_slice` is always the model window
//! contiguous, and its rolling `sum` is bit-exact against a naive recompute across copy-backs.

use std::collections::VecDeque;

use polysim::hot::series::{FastQueue, MedianScratch};
use proptest::prelude::*;

fn model_push<T: Copy>(model: &mut VecDeque<T>, window: usize, item: T) {
    model.push_back(item);
    if model.len() > window {
        model.pop_front();
    }
}

proptest! {
    #[test]
    fn matches_naive_model(
        window in 1usize..64,
        multiple in 2usize..6,
        items in prop::collection::vec(any::<i64>(), 0..1500),
    ) {
        let mut queue = FastQueue::<i64>::new(window, multiple);
        let mut model: VecDeque<i64> = VecDeque::new();

        for &item in &items {
            queue.push(item);
            model_push(&mut model, window, item);
            prop_assert_eq!(queue.len(), model.len());
            prop_assert_eq!(queue.is_empty(), model.is_empty());
            prop_assert!(queue.as_slice().iter().eq(model.iter()));
        }

        prop_assert_eq!(queue.window(), window);
        prop_assert_eq!(queue.first(), model.front().copied());
        prop_assert_eq!(queue.last(), model.back().copied());
        for (i, &expected) in model.iter().enumerate() {
            prop_assert_eq!(queue.get(i), Some(expected));
        }
        prop_assert_eq!(queue.get(model.len()), None);
        prop_assert!(queue.iter().eq(model.iter().copied()));
    }

    /// Bit-exact vs a naive recompute; generator bounds keep sums in i64.
    #[test]
    fn i64_sum_is_exact_across_copybacks(
        window in 1usize..48,
        multiple in 2usize..5,
        items in prop::collection::vec(-(i64::MAX / 64)..=i64::MAX / 64, 0..1500),
    ) {
        let mut queue = FastQueue::<i64>::new(window, multiple);
        let mut model: VecDeque<i64> = VecDeque::new();

        for &item in &items {
            queue.push(item);
            model_push(&mut model, window, item);
        }

        let naive: i128 = model.iter().copied().map(i128::from).sum();
        let naive = i64::try_from(naive).expect("bounded by generator");
        prop_assert_eq!(queue.sum(), naive);
    }

    /// Median matches a sort-based oracle at every push. Finite-range floats (no NaN) match the
    /// production contract, and the even/odd arithmetic is bit-identical so the equality is exact.
    /// One `MedianScratch` is reused across every call, and the allocator is watched around each
    /// one: the whole point of handing a workspace in is that the median never allocates.
    #[test]
    fn f64_median_matches_a_sorted_oracle(
        window in 1usize..64,
        multiple in 2usize..6,
        items in prop::collection::vec(-1e6f64..1e6, 0..1500),
    ) {
        let mut queue = FastQueue::<f64>::new(window, multiple);
        let mut model: VecDeque<f64> = VecDeque::new();
        let mut scratch = MedianScratch::for_window(window);

        prop_assert_eq!(queue.median(&mut scratch), None);

        for &item in &items {
            queue.push(item);
            model_push(&mut model, window, item);

            let mut sorted: Vec<f64> = model.iter().copied().collect();
            sorted.sort_by(f64::total_cmp);
            let n = sorted.len();
            let expected = if n % 2 == 1 {
                sorted[n / 2]
            } else {
                (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
            };
            let before = crate::alloc_count();
            let median = queue.median(&mut scratch);
            let after = crate::alloc_count();
            prop_assert_eq!(median, Some(expected));
            prop_assert_eq!(after, before, "median reached the allocator");
        }
    }

    /// EMA matches the same recursion folded over the model window, bit-for-bit — the strategy
    /// smooths every markout series through it, so a drift here is silent feature corruption.
    #[test]
    fn f64_ema_matches_a_naive_fold(
        window in 1usize..64,
        multiple in 2usize..6,
        halflife in 1u32..32,
        items in prop::collection::vec(-1e6f64..1e6, 0..1500),
    ) {
        let mut queue = FastQueue::<f64>::new(window, multiple);
        let mut model: VecDeque<f64> = VecDeque::new();
        let decay = (-std::f64::consts::LN_2 / f64::from(halflife)).exp();

        prop_assert_eq!(queue.ema(halflife), None);

        for &item in &items {
            queue.push(item);
            model_push(&mut model, window, item);

            let mut expected = model.iter().copied();
            let seed = expected.next().expect("model is non-empty after a push");
            let expected = expected.fold(seed, |acc, value| decay * acc + (1.0 - decay) * value);
            prop_assert_eq!(queue.ema(halflife), Some(expected));
        }
    }
}

/// Pins the decay convention itself, which the proptest oracle shares and so cannot catch:
/// `λ = 2^(−1/H)` seeded from the OLDEST sample, identical to the RiskMetrics volatility
/// recursion. Swapping the seed end or the halflife base silently reweights every EMA feature.
#[test]
fn ema_halflife_one_weights_each_sample_by_half() {
    let mut queue = FastQueue::<f64>::new(8, 2);

    queue.push(4.0);
    assert_eq!(queue.ema(1), Some(4.0));

    queue.push(10.0);
    assert_eq!(queue.ema(1), Some(0.5 * 4.0 + 0.5 * 10.0));

    queue.push(1.0);
    let two = 0.5 * 4.0 + 0.5 * 10.0;
    assert_eq!(queue.ema(1), Some(0.5 * two + 0.5 * 1.0));

    // A four-sample halflife weights the newest sample 1 − 2^(−1/4) ≈ 0.159, not 0.5.
    let decay = 2f64.powf(-0.25);
    let mut long = FastQueue::<f64>::new(8, 2);
    long.push(0.0);
    long.push(1.0);
    assert_eq!(long.ema(4), Some(1.0 - decay));
}
