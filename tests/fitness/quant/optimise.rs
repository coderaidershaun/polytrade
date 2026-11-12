//! Nelder-Mead warm start. Found-bug regression: every quant fitter here warm-starts the simplex
//! from its previous optimum, and an optimum sitting ON a bound built a degenerate simplex with no
//! extent in that direction — the parameter froze at the wall and every later refit reported the
//! wall as the fit. Silent, and it survives convergence checks.

use polysim::hot::quant::optimise::NelderMead;

fn bowl(point: &[f64; 2]) -> f64 {
    (point[0] - 2.0).powi(2) + (point[1] - 3.0).powi(2)
}

fn nelder_mead(start: [f64; 2], bounds: [(f64, f64); 2]) -> NelderMead<2> {
    NelderMead {
        tolerance: 1e-10,
        ..NelderMead::new(start, bounds)
    }
}

#[test]
fn start_on_a_bound_still_moves_that_parameter() {
    // Warm-start-at-bound: inward step or dimension freezes at wall.
    let result = nelder_mead([10.0, 3.0], [(-10.0, 10.0), (-10.0, 10.0)]).minimize::<3>(bowl);
    assert!((result.x[0] - 2.0).abs() < 1e-4, "x0 = {}", result.x[0]);
}
