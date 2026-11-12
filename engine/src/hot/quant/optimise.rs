//! Nelder-Mead simplex minimiser (derivative-free, bounded, allocation-free; SIMPLEX = N + 1).

/// Best vertex found (even if not converged; >= seed).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Optimum<const N: usize> {
    pub x: [f64; N],
    pub value: f64,
    pub iterations: usize,
    pub converged: bool,
}

/// start (clamped to bounds) seeds simplex; reshapes until collapsed below tolerance or
/// max_iterations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NelderMead<const N: usize> {
    pub start: [f64; N],
    pub bounds: [(f64, f64); N],
    pub tolerance: f64,
    pub max_iterations: usize,
}

impl<const N: usize> NelderMead<N> {
    /// Every fitter in the crate takes these, so the `converged` flags they report are comparable
    /// across estimators rather than each one's own idea of convergence.
    pub const TOLERANCE: f64 = 1e-8;
    pub const MAX_ITERATIONS: usize = 1000;

    pub fn new(start: [f64; N], bounds: [(f64, f64); N]) -> Self {
        Self {
            start,
            bounds,
            tolerance: Self::TOLERANCE,
            max_iterations: Self::MAX_ITERATIONS,
        }
    }

    /// Adaptive coefficients scale with N (Gao & Han); candidates clamped before scoring.
    /// # Panics
    /// If SIMPLEX != N + 1 or bounds invalid (lo>hi, non-finite) — caller bug.
    pub fn minimize<const SIMPLEX: usize>(
        &self,
        objective: impl Fn(&[f64; N]) -> f64,
    ) -> Optimum<N> {
        assert_eq!(
            SIMPLEX,
            N + 1,
            "nelder-mead simplex must hold N + 1 vertices"
        );
        for (lower, upper) in self.bounds {
            assert!(
                lower.is_finite() && upper.is_finite() && lower <= upper,
                "nelder-mead bound not finite and ordered: [{lower}, {upper}]"
            );
        }

        let clamp = |point: &mut [f64; N]| {
            for (value, (lower, upper)) in point.iter_mut().zip(self.bounds) {
                *value = value.clamp(lower, upper);
            }
        };

        let dimensions = N as f64;
        let reflection = 1.0;
        let expansion = 1.0 + 2.0 / dimensions;
        let contraction = 0.75 - 1.0 / (2.0 * dimensions);
        let shrink = 1.0 - 1.0 / dimensions;

        let mut simplex = [[0.0; N]; SIMPLEX];
        let mut values = [0.0; SIMPLEX];
        let mut origin = self.start;
        clamp(&mut origin);
        simplex[0] = origin;
        for dimension in 0..N {
            let step =
                if origin[dimension] != 0.0 { 0.05 * origin[dimension].abs() } else { 0.00025 };
            let mut vertex = origin;
            vertex[dimension] += step;
            clamp(&mut vertex);
            // Warm-start-at-bound: step inward or dimension freezes.
            if vertex[dimension] == origin[dimension] {
                vertex[dimension] = origin[dimension] - step;
                clamp(&mut vertex);
            }
            simplex[dimension + 1] = vertex;
        }
        for vertex in 0..SIMPLEX {
            values[vertex] = objective(&simplex[vertex]);
        }
        let mut iterations = 0;
        let mut converged = false;

        let worst = SIMPLEX - 1;
        while iterations < self.max_iterations {
            iterations += 1;
            sort_by_value(&mut simplex, &mut values);
            if has_collapsed(&simplex, &values, self.tolerance) {
                converged = true;
                break;
            }

            let mut centroid = [0.0; N];
            for dimension in 0..N {
                let mut sum = 0.0;
                for row in &simplex[..worst] {
                    sum += row[dimension];
                }
                centroid[dimension] = sum / worst as f64;
            }

            let mut reflected = centroid;
            for dimension in 0..N {
                reflected[dimension] +=
                    reflection * (centroid[dimension] - simplex[worst][dimension]);
            }
            clamp(&mut reflected);
            let reflected_value = objective(&reflected);

            if values[0] <= reflected_value && reflected_value < values[worst - 1] {
                simplex[worst] = reflected;
                values[worst] = reflected_value;
            } else if reflected_value < values[0] {
                let mut expanded = centroid;
                for dimension in 0..N {
                    expanded[dimension] += expansion * (reflected[dimension] - centroid[dimension]);
                }
                clamp(&mut expanded);
                let expanded_value = objective(&expanded);
                if expanded_value < reflected_value {
                    simplex[worst] = expanded;
                    values[worst] = expanded_value;
                } else {
                    simplex[worst] = reflected;
                    values[worst] = reflected_value;
                }
            } else {
                let mut contracted = centroid;
                for dimension in 0..N {
                    contracted[dimension] +=
                        contraction * (simplex[worst][dimension] - centroid[dimension]);
                }
                clamp(&mut contracted);
                let contracted_value = objective(&contracted);
                if contracted_value < values[worst] {
                    simplex[worst] = contracted;
                    values[worst] = contracted_value;
                } else {
                    let best = simplex[0];
                    for vertex in 1..SIMPLEX {
                        for (value, base) in simplex[vertex].iter_mut().zip(best) {
                            *value = base + shrink * (*value - base);
                        }
                        clamp(&mut simplex[vertex]);
                        values[vertex] = objective(&simplex[vertex]);
                    }
                }
            }
        }

        let mut best = 0;
        for vertex in 1..SIMPLEX {
            if values[vertex] < values[best] {
                best = vertex;
            }
        }
        Optimum {
            x: simplex[best],
            value: values[best],
            iterations,
            converged,
        }
    }
}

fn sort_by_value<const N: usize, const SIMPLEX: usize>(
    simplex: &mut [[f64; N]; SIMPLEX],
    values: &mut [f64; SIMPLEX],
) {
    for unsorted in 1..SIMPLEX {
        let mut vertex = unsorted;
        while vertex > 0 && values[vertex - 1] > values[vertex] {
            simplex.swap(vertex - 1, vertex);
            values.swap(vertex - 1, vertex);
            vertex -= 1;
        }
    }
}

/// Collapsed when all vertices within tolerance of best in value and coordinates.
fn has_collapsed<const N: usize, const SIMPLEX: usize>(
    simplex: &[[f64; N]; SIMPLEX],
    values: &[f64; SIMPLEX],
    tolerance: f64,
) -> bool {
    (1..SIMPLEX).all(|vertex| {
        (values[vertex] - values[0]).abs() < tolerance
            && (0..N).all(|dimension| {
                (simplex[vertex][dimension] - simplex[0][dimension]).abs() < tolerance
            })
    })
}
