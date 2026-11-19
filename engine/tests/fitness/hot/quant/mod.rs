//! Quant calculator pins: leaf numeric correctness for the estimators feeding the research tape.
//! One module per calculator, each named for the concept rather than the source path.

use core::num::NonZeroUsize;

use engine::hot::quant::hawkes::{HawkesEvents, HawkesParams};
use engine::msg::time::{DurationUs, LocalTs};

mod egarch;
mod gueant;
mod hawkes_mle;
mod hawkes_univariate;
mod intensity;
mod kyle_feed;
mod kyles_lambda;
mod markouts;
mod microprice;
mod obi;
mod optimise;
mod options;
mod orderbook_resilience;
mod vpin;

/// Deterministic U(0,1), exponential and Box-Muller normal draws; no `rand` dependency.
pub(crate) struct Lcg(pub(crate) u64);

impl Lcg {
    pub(crate) fn unit(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 11) as f64) / ((1u64 << 53) as f64)
    }

    /// Never zero, so its logarithm stays finite.
    fn positive_unit(&mut self) -> f64 {
        self.unit().max(1e-12)
    }

    pub(crate) fn next_normal(&mut self) -> f64 {
        let u1 = self.positive_unit();
        let u2 = self.unit();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }

    pub(crate) fn exp_draw(&mut self, rate: f64) -> f64 {
        -self.positive_unit().ln() / rate
    }
}

/// Seeded exponential-kernel Hawkes path with known truth, drawn by Ogata thinning. Between events
/// the intensity only decays, so the rate at the current time bounds it until the next draw.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ExpSimulation {
    pub(crate) params: HawkesParams,
    pub(crate) start_ts: LocalTs,
    pub(crate) horizon: DurationUs,
    pub(crate) seed: u64,
    pub(crate) max_events: usize,
}

impl ExpSimulation {
    pub(crate) fn run(&self) -> Vec<LocalTs> {
        let (mu, alpha, beta) = (
            self.params.mu_per_sec(),
            self.params.alpha_per_sec(),
            self.params.beta_per_sec(),
        );
        let horizon = self.horizon.to_secs();
        let mut generator = Lcg(self.seed);
        let mut times = Vec::new();
        let mut elapsed = 0.0;
        let mut excitation = 0.0;
        while times.len() < self.max_events {
            let bound = mu + alpha * excitation;
            let wait = generator.exp_draw(bound);
            elapsed += wait;
            if elapsed > horizon {
                break;
            }
            excitation *= (-beta * wait).exp();
            if generator.positive_unit() <= (mu + alpha * excitation) / bound {
                times.push(elapsed);
                excitation += 1.0;
            }
        }
        stamps(self.start_ts, &times)
    }
}

/// Time runs in f64 seconds and rounds to microseconds only here, so the rounding never feeds
/// back into the simulated intensity.
fn stamps(start: LocalTs, seconds: &[f64]) -> Vec<LocalTs> {
    seconds
        .iter()
        .map(|secs| start + DurationUs::from_micros((secs * 1e6).round() as i64))
        .collect()
}

fn ts(secs: f64) -> LocalTs {
    LocalTs::from_micros((secs * 1e6).round() as i64)
}

const START: LocalTs = LocalTs::from_micros(0);

fn window(stamps: &[LocalTs]) -> HawkesEvents {
    let mut events = HawkesEvents::new(NonZeroUsize::new(32_768).expect("a window holds events"));
    for &stamp in stamps {
        events.push(stamp);
    }
    events
}

/// The exponential path the fitter is scored on: truth, its event window, and the fit time.
fn exponential_fixture() -> (HawkesParams, HawkesEvents, LocalTs) {
    let (truth, path, now) = exponential_path();
    (truth, window(&path), now)
}

/// The stamps of that path, for a reader that takes one arrival at a time.
fn exponential_path() -> (HawkesParams, Vec<LocalTs>, LocalTs) {
    let truth = HawkesParams::new(0.5, 0.8, 2.0);
    let horizon = DurationUs::from_secs(6000);
    let path = ExpSimulation {
        params: truth,
        start_ts: START,
        horizon,
        seed: 0xC0FF_EE01,
        max_events: 20_000,
    }
    .run();
    assert!(path.len() > 3000, "thin path: {} events", path.len());
    (truth, path, START + horizon)
}
