//! A bounded minimiser that needs no gradient, for the likelihood fits of the quant estimators. It
//! allocates nothing, because each refit runs on the thread that folds the market data.

/// The best vertex of a search, also when the search stopped at its iteration cap. Its value is
/// never above the value at the clamped start, and it is `f64::MAX` when the search met no finite
/// value of the objective.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Optimum<const N: usize> {
    pub x: [f64; N],
    pub value: f64,
    pub iterations: usize,
    pub converged: bool,
}

/// A search that starts at `start`, clamped into `bounds`. It ends when each vertex lies within
/// `tolerance` of the best vertex, in value and in each coordinate, or after `max_iterations` moves.
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

    // The adaptive coefficients of Gao and Han, which depend on N.
    const REFLECTION: f64 = 1.0;
    const EXPANSION: f64 = 1.0 + 2.0 / N as f64;
    const CONTRACTION: f64 = 0.75 - 1.0 / (2.0 * N as f64);
    const SHRINK: f64 = 1.0 - 1.0 / N as f64;

    pub fn new(start: [f64; N], bounds: [(f64, f64); N]) -> Self {
        Self {
            start,
            bounds,
            tolerance: Self::TOLERANCE,
            max_iterations: Self::MAX_ITERATIONS,
        }
    }

    /// Minimises `objective` inside the bounds. Each candidate is clamped into the bounds before it
    /// is scored, and a value that is not finite scores `f64::MAX`. `SIMPLEX` must be `N + 1`: any
    /// other value fails the build.
    ///
    /// # Panics
    /// If a bound is not finite, or its lower end is above its upper end: a bug of the caller.
    pub fn minimize<const SIMPLEX: usize>(
        &self,
        objective: impl Fn(&[f64; N]) -> f64,
    ) -> Optimum<N> {
        const {
            assert!(
                SIMPLEX == N + 1,
                "nelder-mead simplex must hold N + 1 vertices"
            )
        };
        for (lower, upper) in self.bounds {
            assert!(
                lower.is_finite() && upper.is_finite() && lower <= upper,
                "nelder-mead bound not finite and ordered: [{lower}, {upper}]"
            );
        }

        let score = |point: &[f64; N]| scored(objective(point));
        let (mut simplex, mut values) = self.seed_simplex::<SIMPLEX>(&score);
        let mut iterations = 0;
        let mut converged = false;
        while iterations < self.max_iterations {
            iterations += 1;
            sort_by_value(&mut simplex, &mut values);
            if has_collapsed(&simplex, &values, self.tolerance) {
                converged = true;
                break;
            }
            self.step(&mut simplex, &mut values, &score);
        }

        let (x, value) = best(&simplex, &values);
        Optimum {
            x,
            value,
            iterations,
            converged,
        }
    }

    fn seed_simplex<const SIMPLEX: usize>(
        &self,
        score: &impl Fn(&[f64; N]) -> f64,
    ) -> ([[f64; N]; SIMPLEX], [f64; SIMPLEX]) {
        let mut simplex = [[0.0; N]; SIMPLEX];
        let mut values = [0.0; SIMPLEX];
        let mut origin = self.start;
        self.clamp(&mut origin);
        simplex[0] = origin;
        for dimension in 0..N {
            let step =
                if origin[dimension] != 0.0 { 0.05 * origin[dimension].abs() } else { 0.00025 };
            let mut vertex = origin;
            vertex[dimension] += step;
            self.clamp(&mut vertex);
            // A start on a bound steps inward, or that parameter could never move.
            if vertex[dimension] == origin[dimension] {
                vertex[dimension] = origin[dimension] - step;
                self.clamp(&mut vertex);
            }
            simplex[dimension + 1] = vertex;
        }
        for vertex in 0..SIMPLEX {
            values[vertex] = score(&simplex[vertex]);
        }
        (simplex, values)
    }

    /// One move on a sorted simplex. The worst vertex reflects through the centroid of the others,
    /// and the search keeps the reflected point, expands past it, contracts toward it or toward the
    /// worst vertex, or else shrinks toward the best vertex.
    fn step<const SIMPLEX: usize>(
        &self,
        simplex: &mut [[f64; N]; SIMPLEX],
        values: &mut [f64; SIMPLEX],
        score: &impl Fn(&[f64; N]) -> f64,
    ) {
        let worst = SIMPLEX - 1;
        let centroid = centroid_without_worst(simplex);

        let mut reflected = centroid;
        for dimension in 0..N {
            reflected[dimension] +=
                Self::REFLECTION * (centroid[dimension] - simplex[worst][dimension]);
        }
        self.clamp(&mut reflected);
        let reflected_value = score(&reflected);

        if values[0] <= reflected_value && reflected_value < values[worst - 1] {
            simplex[worst] = reflected;
            values[worst] = reflected_value;
        } else if reflected_value < values[0] {
            let mut expanded = centroid;
            for dimension in 0..N {
                expanded[dimension] +=
                    Self::EXPANSION * (reflected[dimension] - centroid[dimension]);
            }
            self.clamp(&mut expanded);
            let expanded_value = score(&expanded);
            if expanded_value < reflected_value {
                simplex[worst] = expanded;
                values[worst] = expanded_value;
            } else {
                simplex[worst] = reflected;
                values[worst] = reflected_value;
            }
        } else if reflected_value < values[worst] {
            let mut outside = centroid;
            for dimension in 0..N {
                outside[dimension] +=
                    Self::CONTRACTION * (reflected[dimension] - centroid[dimension]);
            }
            self.clamp(&mut outside);
            let outside_value = score(&outside);
            if outside_value <= reflected_value {
                simplex[worst] = outside;
                values[worst] = outside_value;
            } else {
                self.shrink(simplex, values, score);
            }
        } else {
            let mut inside = centroid;
            for dimension in 0..N {
                inside[dimension] +=
                    Self::CONTRACTION * (simplex[worst][dimension] - centroid[dimension]);
            }
            self.clamp(&mut inside);
            let inside_value = score(&inside);
            if inside_value < values[worst] {
                simplex[worst] = inside;
                values[worst] = inside_value;
            } else {
                self.shrink(simplex, values, score);
            }
        }
    }

    fn shrink<const SIMPLEX: usize>(
        &self,
        simplex: &mut [[f64; N]; SIMPLEX],
        values: &mut [f64; SIMPLEX],
        score: &impl Fn(&[f64; N]) -> f64,
    ) {
        let best = simplex[0];
        for vertex in 1..SIMPLEX {
            for (value, base) in simplex[vertex].iter_mut().zip(best) {
                *value = base + Self::SHRINK * (*value - base);
            }
            self.clamp(&mut simplex[vertex]);
            values[vertex] = score(&simplex[vertex]);
        }
    }

    fn clamp(&self, point: &mut [f64; N]) {
        for (value, (lower, upper)) in point.iter_mut().zip(self.bounds) {
            *value = value.clamp(lower, upper);
        }
    }
}

/// A value that is not finite sorts last and never wins the best pick.
fn scored(value: f64) -> f64 {
    if value.is_finite() { value } else { f64::MAX }
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

/// True when each vertex lies within `tolerance` of the best vertex, in value and in each
/// coordinate.
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

fn centroid_without_worst<const N: usize, const SIMPLEX: usize>(
    simplex: &[[f64; N]; SIMPLEX],
) -> [f64; N] {
    let worst = SIMPLEX - 1;
    let mut centroid = [0.0; N];
    for dimension in 0..N {
        let mut sum = 0.0;
        for row in &simplex[..worst] {
            sum += row[dimension];
        }
        centroid[dimension] = sum / worst as f64;
    }
    centroid
}

/// The first vertex with the lowest value, which need not be the first one: the last move came
/// after the last sort.
fn best<const N: usize, const SIMPLEX: usize>(
    simplex: &[[f64; N]; SIMPLEX],
    values: &[f64; SIMPLEX],
) -> ([f64; N], f64) {
    let mut best = 0;
    for vertex in 1..SIMPLEX {
        if values[vertex] < values[best] {
            best = vertex;
        }
    }
    (simplex[best], values[best])
}
