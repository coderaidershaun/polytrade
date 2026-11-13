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
