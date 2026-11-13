//! Pins of the bounded Nelder-Mead minimiser that every fitter shares. A search that goes wrong here
//! still returns a plausible fit, so the error shows only in the research columns it feeds.

use engine::hot::quant::optimise::NelderMead;

fn bowl(point: &[f64; 2]) -> f64 {
    (point[0] - 2.0).powi(2) + (point[1] - 3.0).powi(2)
}

fn nelder_mead(start: [f64; 2], bounds: [(f64, f64); 2]) -> NelderMead<2> {
    NelderMead {
        tolerance: 1e-10,
        ..NelderMead::new(start, bounds)
    }
}

/// Found bug: each fitter warm-starts the simplex from its previous optimum, and an optimum on a
/// bound built a simplex with no extent in that direction. The parameter then stayed at the bound,
/// each later refit reported the bound as the fit, and the convergence check still passed.
#[test]
fn start_on_a_bound_still_moves_that_parameter() {
    let result = nelder_mead([10.0, 3.0], [(-10.0, 10.0), (-10.0, 10.0)]).minimize::<3>(bowl);
    assert!((result.x[0] - 2.0).abs() < 1e-4, "x0 = {}", result.x[0]);
}

/// A likelihood can have no finite value in part of its box. A vertex there must sort last and
/// never win the best pick, or the search stalls until its iteration cap or reports that vertex.
#[test]
fn a_nan_objective_still_gives_a_finite_best() {
    for not_finite in [f64::NAN, f64::NEG_INFINITY] {
        // The minimum of (x0 - 2)² + (x1 - 1)² is (2, 1), where the value is finite.
        let bowl_with_a_region_that_is_not_finite = |point: &[f64; 2]| {
            if point[0] > 3.0 {
                not_finite
            } else {
                (point[0] - 2.0).powi(2) + (point[1] - 1.0).powi(2)
            }
        };
        // The first step of 5% moves x0 from 2.9 to 3.045, so one vertex of the first simplex is
        // in that region.
        let search = NelderMead::new([2.9, 3.5], [(0.0, 4.0), (0.0, 4.0)]);
        let result = search.minimize::<3>(bowl_with_a_region_that_is_not_finite);
        assert!(
            result.value.is_finite() && result.value < f64::MAX,
            "{not_finite}: no finite best: {result:?}"
        );
        assert!(
            (result.x[0] - 2.0).abs() < 1e-4,
            "{not_finite}: x0 = {}",
            result.x[0]
        );
        assert!(
            result.converged && result.iterations < search.max_iterations,
            "{not_finite}: the search ran to its cap: {result:?}"
        );
    }
}

/// The adaptive Nelder-Mead of Gao and Han contracts toward the reflected point when the reflected
/// value is at least the second worst and below the worst, and keeps that point when it is no worse
/// than the reflected one. A contraction toward the worst vertex there turns away from the better
/// side, so each fit takes other moves and can end at another point.
#[test]
fn a_reflection_between_the_two_worst_values_contracts_outside() {
    // From (20, 20) each first step is 5% of 20 = 1, so the simplex is (20, 20), (21, 20), (20, 21),
    // where f = 0.6875, 0.1875, 3.6875. The worst, (20, 21), reflects through (20.5, 20), the mean
    // of the other two, to (21, 19), where f = 1.1875: at least 0.6875 and below 3.6875.
    // With N = 2 the contraction is 0.75 - 1 / 4 = 0.5, so the outside point is
    // (20.5, 20) + 0.5 ((21, 19) - (20.5, 20)) = (20.75, 19.5), where f = 0.125 <= 1.1875.
    // It replaces the worst and is the new best. The inside point, (20.25, 20.5) with f = 1.375,
    // would leave (21, 20) the best.
    let objective =
        |point: &[f64; 2]| (point[0] - 20.75).powi(2) + 2.0 * (point[1] - 19.75).powi(2);
    let one_move = NelderMead {
        max_iterations: 1,
        ..NelderMead::new([20.0, 20.0], [(0.0, 100.0), (0.0, 100.0)])
    };
    let result = one_move.minimize::<3>(objective);
    assert_eq!(result.iterations, 1);
    assert!(
        (result.x[0] - 20.75).abs() < 1e-12 && (result.x[1] - 19.5).abs() < 1e-12,
        "not the outside point: {result:?}"
    );
    assert!((result.value - 0.125).abs() < 1e-12, "{result:?}");
}
