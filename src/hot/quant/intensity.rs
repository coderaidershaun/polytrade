//! Trade-intensity (A, k) estimator — feeds Guéant-Lehalle-Fernandez-Tapia quoting.
//! Hot thread: [`IntensityCounts`] (in-place, zero alloc). Strategy: [`IntensityFit`] (warm-started Poisson MLE).
//! Depth in ticks behind same-side best; A in /sec, k in /tick.

use crate::config::IntensitySpec;
use crate::hot::quant::MIN_RATE;
use crate::hot::quant::optimise::NelderMead;
use crate::ids::{Price, Side};
use crate::msg::inbound::TradeEvent;
use crate::time::{DurationUs, TsUs};

const PARAMS: usize = 2;
const SIMPLEX: usize = PARAMS + 1;

/// ln A and ln k search box (fence off overflow).
const LOG_BOUNDS: [(f64, f64); PARAMS] = [(-30.0, 30.0), (-12.0, 5.0)];

/// Fraction of histogram half-life before (A,k) re-solve. Tenth holds drift <7%, three NM per span.
const REFIT_HALF_LIFE_FRACTION: f64 = 0.1;

#[derive(Debug, Clone, PartialEq)]
pub struct IntensityCounts {
    tick: Price,
    decay_per_sec: f64,
    min_events: f64,
    bid_reach: Vec<f64>,
    ask_reach: Vec<f64>,
    exposure_secs: f64,
    last_decay_ts: Option<TsUs>,
    open_group: Option<Group>,
    inside_spread: u64,
    without_book: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Group {
    side: Side,
    exchange_ts_us: TsUs,
    deepest_bucket: usize,
    anchor: Price,
}

impl IntensityCounts {
    /// tick buckets the histogram.
    ///
    /// # Panics
    /// Fewer than two depth buckets: the fit needs an interior bin plus the censored tail, and the
    /// bucket arithmetic below would underflow on an empty histogram.
    pub(crate) fn new(spec: &IntensitySpec, tick: Price) -> Self {
        assert!(
            spec.max_depth_ticks >= 2,
            "intensity histogram needs at least two depth buckets, got {}",
            spec.max_depth_ticks
        );
        Self {
            tick,
            decay_per_sec: std::f64::consts::LN_2 / spec.half_life_secs,
            min_events: spec.min_events,
            bid_reach: vec![0.0; spec.max_depth_ticks],
            ask_reach: vec![0.0; spec.max_depth_ticks],
            exposure_secs: 0.0,
            last_decay_ts: None,
            open_group: None,
            inside_spread: 0,
            without_book: 0,
        }
    }

    /// Buy -> ASK side, sell -> BID side. Sweep (same side + ts) extends group; else new.
    pub fn on_trade(&mut self, event: &TradeEvent, bid: Option<Price>, ask: Option<Price>) {
        if let Some(group) = self.open_group
            && group.side == event.side
            && group.exchange_ts_us == event.exchange_ts_us
        {
            self.extend_group(group, event.price);
            return;
        }
        self.start_group(event, bid, ask);
    }

    /// Wipes histograms/exposure/open sweep (lifetime counters survive).
    pub fn clear(&mut self) {
        self.bid_reach.iter_mut().for_each(|value| *value = 0.0);
        self.ask_reach.iter_mut().for_each(|value| *value = 0.0);
        self.exposure_secs = 0.0;
        self.last_decay_ts = None;
        self.open_group = None;
    }

    /// Stale-top race count (lifetime).
    pub fn inside_spread_count(&self) -> u64 {
        self.inside_spread
    }

    /// Unanchored trade count (lifetime).
    pub fn without_book_count(&self) -> u64 {
        self.without_book
    }

    fn max_depth(&self) -> usize {
        self.ask_reach.len()
    }

    fn reach_mut(&mut self, side: Side) -> &mut [f64] {
        match side {
            Side::Buy => &mut self.ask_reach,
            Side::Sell => &mut self.bid_reach,
        }
    }

    fn start_group(&mut self, event: &TradeEvent, bid: Option<Price>, ask: Option<Price>) {
        self.decay_to(event.received_ts_us);
        let anchor = match event.side {
            Side::Buy => ask,
            Side::Sell => bid,
        };
        let Some(anchor) = anchor else {
            self.without_book += 1;
            return;
        };
        let bucket = self
            .bucket_for(event.side, anchor, event.price)
            .unwrap_or(0);
        for level in self.reach_mut(event.side)[..=bucket].iter_mut() {
            *level += 1.0;
        }
        self.open_group = Some(Group {
            side: event.side,
            exchange_ts_us: event.exchange_ts_us,
            deepest_bucket: bucket,
            anchor,
        });
    }

    fn extend_group(&mut self, group: Group, price: Price) {
        let Some(bucket) = self.bucket_for(group.side, group.anchor, price) else {
            return;
        };
        if bucket <= group.deepest_bucket {
            return;
        }
        for level in self.reach_mut(group.side)[group.deepest_bucket + 1..=bucket].iter_mut() {
            *level += 1.0;
        }
        self.open_group = Some(Group {
            deepest_bucket: bucket,
            ..group
        });
    }

    /// Depth bucket of a print against its anchor; a print inside the spread is counted and has none.
    fn bucket_for(&mut self, side: Side, anchor: Price, price: Price) -> Option<usize> {
        let depth = tick_depth(side, anchor, price, self.tick);
        if depth < 0 {
            self.inside_spread += 1;
            return None;
        }
        Some((depth as usize).min(self.max_depth() - 1))
    }

    /// Decays histograms by `e^(-lambda*dt)` and advances the exposure integral to `now`; a trade at
    /// or before `last_decay_ts` doesn't advance.
    fn decay_to(&mut self, now: TsUs) {
        let Some(last) = self.last_decay_ts else {
            self.last_decay_ts = Some(now);
            return;
        };
        // `decay_to_now` clamps a backwards step to zero; returning here instead keeps
        // `last_decay_ts` from moving back to an older trade.
        if now.diff(last).to_secs() <= 0.0 {
            return;
        }
        let (factor, exposure) = self.decay_to_now(now);
        for value in self.bid_reach.iter_mut().chain(&mut self.ask_reach) {
            *value *= factor;
        }
        self.exposure_secs = exposure;
        self.last_decay_ts = Some(now);
    }

    /// Read-only: `(decay_factor, exposure_secs)` as of `now`.
    fn decay_to_now(&self, now: TsUs) -> (f64, f64) {
        let dt = match self.last_decay_ts {
            Some(last) => now.diff(last).to_secs().max(0.0),
            None => return (1.0, 0.0),
        };
        let factor = (-self.decay_per_sec * dt).exp();
        let exposure = self.exposure_secs * factor + (1.0 - factor) / self.decay_per_sec;
        (factor, exposure)
    }
}

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
    /// Mid-relative rate = A_touch * e^(k * half_spread_ticks).
    pub fn a_mid_per_sec(&self, half_spread_ticks: f64) -> f64 {
        self.a_per_sec * (self.k_per_tick * half_spread_ticks).exp()
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
    pub fn fit(&mut self, counts: &IntensityCounts, now: TsUs) -> IntensityEstimate {
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
        now: TsUs,
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
    last_fit_ts_us: Option<TsUs>,
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

    /// Re-solving faster than the counts decay re-fits the same data.
    pub fn refit_interval(spec: &IntensitySpec) -> DurationUs {
        let secs = spec.half_life_secs * REFIT_HALF_LIFE_FRACTION;
        DurationUs::from_micros((secs * 1e6) as i64)
    }

    /// Current (A,k) fit. Re-solve costs 3 NM (bid/ask/pooled) -> rides histogram decay not spin.
    pub fn refit(
        &mut self,
        counts: Option<&IntensityCounts>,
        now: TsUs,
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

fn tick_depth(side: Side, anchor: Price, price: Price, tick: Price) -> i64 {
    let diff = match side {
        Side::Buy => price.0 - anchor.0,
        Side::Sell => anchor.0 - price.0,
    };
    diff.div_euclid(tick.0)
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
