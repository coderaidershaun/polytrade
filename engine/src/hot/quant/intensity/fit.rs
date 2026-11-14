//! Fits A and k to the decayed counts by Poisson likelihood, each fit starting from the last
//! optimum, and paces that fit so that it runs only once the counts have moved on.

use super::IntensityCounts;
use crate::hot::quant::MIN_RATE;
use crate::hot::quant::optimise::NelderMead;
use crate::msg::time::{DurationUs, LocalTs};
use crate::runtime::config::IntensitySpec;

const PARAMS: usize = 2;
const SIMPLEX: usize = PARAMS + 1;

/// ln A and ln k search box (fence off overflow).
const LOG_BOUNDS: [(f64, f64); PARAMS] = [(-30.0, 30.0), (-12.0, 5.0)];

/// Fraction of histogram half-life before (A,k) re-solve. Tenth holds drift <7%, three NM per span.
const REFIT_HALF_LIFE_FRACTION: f64 = 0.1;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SideEstimate {
    pub a_per_sec: f64,
    pub k_per_tick: f64,
    pub events: f64,
    pub converged: bool,
    pub iterations: usize,
    /// Not a fresh solve: too few decayed touches remained, so these are the previous fit's
    /// numbers re-dated.
    pub is_stale: bool,
}

impl SideEstimate {
    /// The rate of fills at the mid, A·e^(k·h) for a half spread of h ticks, since A is fitted at
    /// the touch, h ticks out from the mid. `None` where that rate is not finite, as once k·h
    /// passes ln(f64::MAX) and the exponential overflows: the side then gets no quote.
    pub fn a_mid_per_sec(&self, half_spread_ticks: f64) -> Option<f64> {
        let rate = self.a_per_sec * (self.k_per_tick * half_spread_ticks).exp();
        rate.is_finite().then_some(rate)
    }

    /// A stale fit is a previous one re-dated: recording or publishing it would date a dead
    /// market's liquidity to now.
    pub fn fresh(self) -> Option<Self> {
        (!self.is_stale).then_some(self)
    }
}

/// Bid/ask/pooled fits (None before first fit).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IntensityEstimate {
    pub bid: Option<SideEstimate>,
    pub ask: Option<SideEstimate>,
    pub all: Option<SideEstimate>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct SlotFit {
    estimate: SideEstimate,
    log_params: [f64; PARAMS],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Slot {
    Bid,
    Ask,
    Pooled,
}

impl Slot {
    fn index(self) -> usize {
        self as usize
    }

    /// Pooling fits both sides against doubled exposure (A kept on per-quote scale).
    fn exposure_multiple(self) -> f64 {
        match self {
            Slot::Pooled => 2.0,
            _ => 1.0,
        }
    }
}

/// Warm-start cache (not Copy to prevent fork).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct IntensityFit {
    slots: [Option<SlotFit>; 3],
}

impl IntensityFit {
    /// Fits A, k per side and pool. Below min_events returns prior fit (stale) or None.
    pub fn fit(&mut self, counts: &IntensityCounts, now: LocalTs) -> IntensityEstimate {
        IntensityEstimate {
            bid: self.fit_slot(Slot::Bid, counts, now),
            ask: self.fit_slot(Slot::Ask, counts, now),
            all: self.fit_slot(Slot::Pooled, counts, now),
        }
    }

    fn fit_slot(
        &mut self,
        slot: Slot,
        counts: &IntensityCounts,
        now: LocalTs,
    ) -> Option<SideEstimate> {
        let index = slot.index();
        let max_depth = counts.max_depth();
        let (factor, exposure_base) = counts.decay_to_now(now);
        let exposure = exposure_base * slot.exposure_multiple();
        // Decay-adjusted reach counts per slot.
        let survival = move |depth: usize| -> f64 {
            let raw = match slot {
                Slot::Bid => counts.bid_reach[depth],
                Slot::Ask => counts.ask_reach[depth],
                Slot::Pooled => counts.bid_reach[depth] + counts.ask_reach[depth],
            };
            factor * raw
        };

        let touch = survival(0);
        if touch < counts.min_events || exposure <= 0.0 {
            return self.slots[index].map(|fit| SideEstimate {
                is_stale: true,
                ..fit.estimate
            });
        }

        let deeper: f64 = (1..max_depth).map(&survival).sum();
        let q = if touch + deeper > 0.0 { deeper / (touch + deeper) } else { 0.0 };
        let ln_k_seed = if q > 0.0 { (-q.ln()).ln() } else { LOG_BOUNDS[1].1 };
        let seed =
            self.slots[index].map_or([(touch / exposure).ln(), ln_k_seed], |fit| fit.log_params);

        let optimum = NelderMead::new(seed, LOG_BOUNDS).minimize::<SIMPLEX>(|p| {
            poisson_nll(p[0].exp(), p[1].exp(), exposure, max_depth, survival)
        });

        let estimate = SideEstimate {
            a_per_sec: optimum.x[0].exp(),
            k_per_tick: optimum.x[1].exp(),
            events: touch,
            converged: optimum.converged,
            iterations: optimum.iterations,
            is_stale: false,
        };
        self.slots[index] = Some(SlotFit {
            estimate,
            log_params: optimum.x,
        });
        Some(estimate)
    }
}

/// An [`IntensityFit`] paced by the histogram it reads: every spin gets the fit in force, the solve
/// itself only once the cadence has elapsed.
#[derive(Debug, Clone, PartialEq)]
pub struct PacedIntensity {
    fit: IntensityFit,
    /// (A,k) fit in force, read every spin. Re-solved on refit_interval.
    estimate: Option<IntensityEstimate>,
    last_fit_ts_us: Option<LocalTs>,
    /// `None` never fits: no reach histogram is configured, so no counts reach it either.
    refit_interval: Option<DurationUs>,
}

impl PacedIntensity {
    pub fn new(refit_interval: Option<DurationUs>) -> Self {
        Self {
            fit: IntensityFit::default(),
            estimate: None,
            last_fit_ts_us: None,
            refit_interval,
        }
    }

    /// A tenth of the half-life of the counts, to the nearest microsecond: a refit that comes
    /// sooner fits nearly the same counts again.
    pub fn refit_interval(spec: &IntensitySpec) -> DurationUs {
        let secs = spec.half_life_secs * REFIT_HALF_LIFE_FRACTION;
        DurationUs::from_micros((secs * 1e6).round() as i64)
    }

    /// Refits when the interval has passed since the last fit, then gives the fit in force. A
    /// refit runs three searches, one for each side and one for both, so it follows the decay of
    /// the counts and not each read. `None` with no interval or with no counts.
    pub fn estimate(
        &mut self,
        counts: Option<&IntensityCounts>,
        now: LocalTs,
    ) -> Option<IntensityEstimate> {
        let refit_interval = self.refit_interval?;
        let counts = counts?;
        let is_due = self
            .last_fit_ts_us
            .is_none_or(|last| now.diff(last) >= refit_interval);
        if is_due {
            self.last_fit_ts_us = Some(now);
            self.estimate = Some(self.fit.fit(counts, now));
        }
        self.estimate
    }

    /// Market data back to post-new state. The cadence is config-derived and stays.
    pub fn reset(&mut self) {
        *self = Self::new(self.refit_interval);
    }
}

/// Poisson NLL: mu_j = T*A*(e^(-k j) - e^(-k(j+1))) over exact interior bins, censored tail at mu = T*A*e^(-k(max-1)).
fn poisson_nll(
    a: f64,
    k: f64,
    exposure: f64,
    max_depth: usize,
    survival: impl Fn(usize) -> f64,
) -> f64 {
    let mut acc = 0.0;
    for depth in 0..max_depth - 1 {
        let count = survival(depth) - survival(depth + 1);
        let mu = exposure * a * ((-k * depth as f64).exp() - (-k * (depth + 1) as f64).exp());
        acc += mu - count * mu.max(MIN_RATE).ln();
    }
    let tail = max_depth - 1;
    let count = survival(tail);
    let mu = exposure * a * (-k * tail as f64).exp();
    acc + mu - count * mu.max(MIN_RATE).ln()
}
