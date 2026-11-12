//! Per-instrument state + emission plumbing: volatility, model feeds (EGARCH, intensity, Guéant, markouts, Kyle, Hawkes),
//! per-side emit helpers. Strategy owns callbacks; this module owns folded state.

use polysim::config::KlineInterval;
use polysim::hot::quant::hawkes::{HawkesChoice, HawkesSide};
use polysim::hot::quant::intensity::{PacedIntensity, SideEstimate};
use polysim::hot::quant::liquidity::{KyleFeed, KylesLambdaSpec};
use polysim::hot::quant::micro::OrderbookResilience;
use polysim::hot::quant::pricing::GueantParams;
use polysim::hot::quant::toxicity::{
    ForwardHorizon, MarkoutSpec, MarkoutTracker, ReverseHorizon, SideMarkouts,
};
use polysim::hot::quant::volatility::{Egarch, EgarchEstimate, Returns};
use polysim::hot::series::{FastQueue, MedianScratch};
use polysim::hot::strategy::{InstrumentRow, StrategyCtx};
use polysim::ids::{FIXED_SCALE, InstrumentId, Price, Qty, Side};
use polysim::msg::inbound::Level;
use polysim::time::{DurationUs, TsUs};

use super::features::{
    Features, GueantSideColumns, HawkesColumns, IntensityColumns, MarkoutColumns, QuoteSide,
};

const SERIES_BACKING_MULTIPLE: usize = 2;

/// Time horizon realised_vol_st looks back. Fixed in time not samples.
const REALISED_HORIZON: DurationUs = DurationUs::from_secs(600);

/// Capacity floor for realised-vol window, not emission gate.
const MIN_VOL_SAMPLES: usize = 30;

/// Returns required before emitting realised-vol. Avoid noise wearing number's clothes.
const MIN_VOL_RETURNS: usize = 5;

/// Closed candles before first EGARCH fit (~5h 1m). Identify against real series.
const EGARCH_MIN_CLOSES: usize = 300;

/// Horizon rolling resilience median/mean summarise. Count-sized from spin cadence.
const RESILIENCE_HORIZON: DurationUs = DurationUs::from_secs(60);

/// Markout EMA halflife in PSEUDO-FILLS not spins. Series advance on prints at quoted levels.
const MARKOUT_EMA_HALFLIFE: u32 = 8;

/// Worst-case mid observations/sec for markout ring. Binance 10x/s + spin + headroom.
const MARKOUT_MAX_MIDS_PER_SEC: u32 = 12;

/// Long-horizon EGARCH + short-horizon realised vol. estimate caches last fit; refit when dirty.
pub(crate) struct BinanceVol {
    /// Candle interval EGARCH rescale built against; on_kline accepts this interval alone.
    pub(crate) interval: KlineInterval,
    pub(crate) closes: FastQueue<f64>,
    pub(crate) spin_mids: FastQueue<f64>,
    egarch: Egarch,
    pub(crate) is_dirty: bool,
    is_seeded: bool,
    pub(crate) estimate: Option<EgarchEstimate>,
    pub(crate) resilience: OrderbookResilience,
    pub(crate) resilience_window: FastQueue<f64>,
    pub(crate) median_scratch: MedianScratch,
    pub(crate) intensity: PacedIntensity,
    /// Guéant tick-space grid. None if startup preflight left unstamped.
    pub(crate) tick: Option<Price>,
    pub(crate) markouts: MarkoutTracker,
    last_mid_ts_us: Option<TsUs>,
    /// None if no volume clock (on_volume never fires) or no tick (λ has no grid).
    pub(crate) kyle: Option<KyleFeed>,
    pub(crate) hawkes_bid: HawkesSide,
    pub(crate) hawkes_ask: HawkesSide,
}

impl BinanceVol {
    /// # Panics
    /// No fixed-minute kline interval, no retention, or retention < fit floor. All strategy inputs.
    fn new(row: &InstrumentRow, setup: InstrumentSetup) -> Self {
        let spin_interval = setup.spin_interval;
        let (interval, span) = row
            .kline_intervals
            .iter()
            .filter_map(|interval| Some((*interval, interval.fixed_duration()?)))
            .min_by_key(|(_, span)| *span)
            .expect("micro recorder fits EGARCH to closed candles — configure kline_intervals");
        let keep = row
            .tracker
            .candles
            .as_ref()
            .expect("micro recorder sizes its EGARCH window from tracker.candles — configure keep")
            .keep;
        assert!(
            keep >= EGARCH_MIN_CLOSES,
            "tracker.candles.keep = {keep} retains fewer than the {EGARCH_MIN_CLOSES} closes an \
             EGARCH fit needs, so egarch_vol_lt could never be emitted — raise keep"
        );
        let resilience_len = samples_in_horizon(RESILIENCE_HORIZON, spin_interval).max(1);
        Self {
            interval,
            closes: FastQueue::new(keep, SERIES_BACKING_MULTIPLE),
            spin_mids: FastQueue::new(realised_window_len(spin_interval), SERIES_BACKING_MULTIPLE),
            egarch: Egarch::new(span, EGARCH_MIN_CLOSES, keep),
            is_dirty: false,
            is_seeded: false,
            estimate: None,
            resilience: OrderbookResilience::new(),
            resilience_window: FastQueue::new(resilience_len, SERIES_BACKING_MULTIPLE),
            median_scratch: MedianScratch::for_window(resilience_len),
            intensity: PacedIntensity::new(
                row.tracker
                    .intensity
                    .as_ref()
                    .map(PacedIntensity::refit_interval),
            ),
            tick: row.tick_size,
            markouts: MarkoutTracker::new(MarkoutSpec {
                spin_interval,
                max_mids_per_sec: MARKOUT_MAX_MIDS_PER_SEC,
            }),
            last_mid_ts_us: None,
            kyle: row
                .tick_size
                .filter(|_| row.tracker.volume_bars.is_some())
                .map(|tick| KyleFeed::new(setup.kyles_lambda, tick)),
            hawkes_bid: HawkesSide::new(setup.hawkes),
            hawkes_ask: HawkesSide::new(setup.hawkes),
        }
    }

    /// First live spin: absorb tracked warmup candles (REST backfill), refit on fresh candle. Clear-copy keeps idempotent.
    pub(crate) fn refresh(&mut self, ctx: &mut StrategyCtx<'_>, instrument: InstrumentId) {
        if !self.is_seeded {
            self.is_seeded = true;
            if let Some(series) = ctx.tracker(instrument).candles(self.interval) {
                self.closes.clear();
                for candle in series.closed.iter() {
                    self.closes.push(candle.close.to_f64());
                }
                self.is_dirty |= !self.closes.is_empty();
            }
        }
        if !self.is_dirty {
            return;
        }
        self.estimate = self.closes.egarch(&mut self.egarch);
        self.is_dirty = false;
        if let Some(estimate) = self.estimate
            && !estimate.converged
        {
            polysim::strategy_warn!(
                ctx,
                "egarch fit did not converge for instrument {} after {} iterations",
                instrument.0,
                estimate.iterations
            );
        }
    }

    /// Market data back to post-new state, keeping preallocated buffers. Config-derived (interval/tick/refit) stay.
    pub(crate) fn reset(&mut self) {
        self.closes.clear();
        self.spin_mids.clear();
        self.egarch.reset_warm_start();
        self.is_dirty = false;
        // Re-seeded from tracker's candles on next live spin (same as boot). REST backfill contiguous.
        self.is_seeded = false;
        self.estimate = None;
        self.resilience = OrderbookResilience::new();
        self.resilience_window.clear();
        self.intensity.reset();
        self.markouts.clear();
        self.last_mid_ts_us = None;
        if let Some(kyle) = self.kyle.as_mut() {
            kyle.reset();
        }
        self.hawkes_bid.reset();
        self.hawkes_ask.reset();
    }

    /// The markout mid entry point. Spins and chunks arrive on different queues, so a late stamp is
    /// clamped forward rather than allowed to disorder the series.
    pub(crate) fn feed_markout_mid(&mut self, ts_us: TsUs, mid: Option<f64>) {
        let Some(mid) = mid else { return };
        let ts_us = self.last_mid_ts_us.map_or(ts_us, |last| last.max(ts_us));
        self.last_mid_ts_us = Some(ts_us);
        self.markouts.on_mid(ts_us, mid);
    }
}

/// Instrument sizing + validation from registration. All config-derived.
#[derive(Debug, Clone, Copy)]
pub(crate) struct InstrumentSetup {
    pub(crate) spin_interval: DurationUs,
    pub(crate) kyles_lambda: KylesLambdaSpec,
    pub(crate) hawkes: HawkesChoice,
    /// One fill's notional as exact mantissa. Risk gate step.
    pub(crate) order_notional: i64,
}

/// Mantissa on buy, negation on sell. Direction one fill moves position.
pub(crate) fn signed(side: Side, mantissa: i64) -> i64 {
    match side {
        Side::Buy => mantissa,
        Side::Sell => -mantissa,
    }
}

/// Risk gate comparison pair carried together. Config ceiling + step size one fill.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RiskBudget {
    pub(crate) order_notional: i64,
    pub(crate) max_exposure_quote: i64,
}

impl RiskBudget {
    /// Whether side must withdraw. Two tests: breach ceiling AND bigger than held now. Reduces never withdraw
    /// (avoid deadlock). Exact mantissas: fill on ceiling admitted.
    pub(crate) fn would_breach(&self, exposure_quote: i64, side: Side) -> bool {
        let projected = exposure_quote + signed(side, self.order_notional);
        projected.abs() > self.max_exposure_quote && projected.abs() > exposure_quote.abs()
    }
}

/// Qty notional buys at price, truncated to 1e-8 grid. Sizes declared quotes.
/// # Panics
/// On a zero price, which is a caller bug: armed and quoted levels are guaranteed positive.
pub(crate) fn qty_at(notional: i64, price: Price) -> Qty {
    Qty((notional as i128 * FIXED_SCALE as i128 / price.0 as i128) as i64)
}

/// The quantity resting at this price on this side, which is the queue a quote would join. Zero
/// when no level sits there.
pub(crate) fn level_qty_at(levels: &[Level], price: Price) -> Qty {
    levels
        .iter()
        .find(|level| level.price == price)
        .map_or(Qty(0), |level| level.qty)
}

/// Per-instrument recorder state: model bundle, quote ceiling, recorded flag. Position lives in engine.
pub(crate) struct InstrumentState {
    pub(crate) is_recorded: bool,
    pub(crate) binance: BinanceVol,
    /// max_exposure_quote cached for spin gate exact mantissa comparison.
    pub(crate) max_exposure_quote: i64,
}

impl InstrumentState {
    /// # Panics
    /// Fill notional exceeds budget: no side could quote. Prevented at startup, not gated every spin.
    pub(crate) fn new(row: &InstrumentRow, is_recorded: bool, setup: InstrumentSetup) -> Self {
        assert!(
            setup.order_notional <= row.max_exposure_quote,
            "order_notional mantissa {} exceeds max_exposure_quote {} on {} — one fill \
             could never fit the budget, so no side would ever quote; lower the order or raise the budget",
            setup.order_notional,
            row.max_exposure_quote,
            row.venue_symbol
        );
        Self {
            is_recorded,
            binance: BinanceVol::new(row, setup),
            max_exposure_quote: row.max_exposure_quote,
        }
    }
}

/// How many spins of `spin_interval` fit in `horizon` — the count-sizing shared by every rolling
/// window whose horizon is fixed in time, not in samples.
fn samples_in_horizon(horizon: DurationUs, spin_interval: DurationUs) -> usize {
    (horizon.micros() / spin_interval.micros()) as usize
}

/// Spin mids covering [`REALISED_HORIZON`], floored at [`MIN_VOL_SAMPLES`] so a slow spin still
/// buffers enough to take a stdev over.
fn realised_window_len(spin_interval: DurationUs) -> usize {
    samples_in_horizon(REALISED_HORIZON, spin_interval).max(MIN_VOL_SAMPLES)
}

/// Realised vol of `closes` sampled `interval` apart, withheld until [`MIN_VOL_RETURNS`] returns
/// back it. Counts samples, not usable returns — a non-positive close would drop inside the
/// estimator and leave one fewer, which mids being prices makes unreachable.
pub(crate) fn gated_realised_vol(
    closes: &FastQueue<f64>,
    returns: Returns,
    interval: DurationUs,
) -> Option<f64> {
    if closes.len() <= MIN_VOL_RETURNS {
        return None;
    }
    closes.realised_volatility(returns, interval)
}

/// σ_log per second: the EGARCH conditional forecast raised by the fast realised window, whichever
/// have an opinion. Maxing the two volatilities and maxing the two variances pick the same number,
/// since both are non-negative. `None` when neither model has produced one yet.
pub(crate) fn effective_log_vol(egarch: Option<f64>, realised: Option<f64>) -> Option<f64> {
    match (egarch, realised) {
        (Some(long_horizon), Some(short_horizon)) => Some(long_horizon.max(short_horizon)),
        (long_horizon, short_horizon) => long_horizon.or(short_horizon),
    }
}

pub(crate) const BPS: f64 = 1e4;

pub(crate) const MIN_QUOTE_DISTANCE_BPS: f64 = 0.5;

pub(crate) fn ticks_per_bp(mid: f64, tick: Price) -> f64 {
    mid / (BPS * tick.to_f64())
}

/// The live market inputs a Guéant solve needs, each `None` until its source has an opinion.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GueantInputs {
    pub(crate) tick: Option<Price>,
    pub(crate) mid: Option<f64>,
    pub(crate) spread: Option<f64>,
    pub(crate) log_vol: Option<f64>,
    /// Not an `Option`: a flat book is q = 0, a known inventory, never a missing one.
    pub(crate) inventory: f64,
}

/// One instrument's Guéant emission context for one spin: everything that does not vary by side,
/// already rescaled into the bps space the closed form solves in. Not a quote —
/// the depths and the snapped price are per side, and come out of [`GueantScale::emit_side`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct GueantScale {
    params: GueantParams,
    features: Features,
    tick: Price,
    /// S̃ = S/τ, the fair price as a continuous tick index — S is the mid, not the microprice.
    fair_ticks: f64,
    ticks_per_bp: f64,
    /// σ̃ in bps per √second.
    pub(crate) sigma_bps: f64,
    /// Mid-to-touch distance in bps, the offset A has to be re-anchored across.
    half_spread_bps: f64,
    /// q, the signed inventory the quote leans against — the engine's ledger, from real fills.
    inventory: f64,
}

impl GueantScale {
    /// `None` unless every shared input is live. The rescale it performs is where the model's
    /// classic scale errors live: σ arrives as a per-second LOG rate — already a relative rate, so
    /// bps is one multiply, applied exactly once — and the intensity's A is fitted at the touch
    /// while δ is measured from S, half a spread further in; [`SideEstimate::a_mid_per_sec`]
    /// carries that re-anchoring, in the tick space the estimator fitted k in.
    pub(crate) fn from_inputs(
        params: GueantParams,
        features: Features,
        inputs: GueantInputs,
    ) -> Option<Self> {
        let (tick, mid, spread, log_vol) =
            (inputs.tick?, inputs.mid?, inputs.spread?, inputs.log_vol?);
        Some(Self {
            params,
            features,
            tick,
            fair_ticks: mid / tick.to_f64(),
            ticks_per_bp: ticks_per_bp(mid, tick),
            sigma_bps: log_vol * BPS,
            half_spread_bps: (spread / 2.0) / mid * BPS,
            inventory: inputs.inventory,
        })
    }

    /// One side's flat-inventory half-spread, its inventory skew and the resulting quote, or nothing.
    /// The quote prices the live inventory, δᵇ(q) = h + jq and δᵃ(q) = h − jq, so a long book
    /// quotes to sell it down. The two depth columns stay h and j, the inventory-free pair, and
    /// because `inventory_quote` is recorded beside them, the quote for any other q can be rebuilt
    /// offline.
    /// Returns the snapped level exactly when the price column was emitted, so the markout arm and the
    /// recorded quote can never disagree.
    pub(crate) fn emit_side(
        &self,
        ctx: &mut StrategyCtx<'_>,
        instrument: InstrumentId,
        side: QuoteSide,
        estimate: Option<SideEstimate>,
    ) -> Option<Price> {
        let estimate = estimate?.fresh()?;
        let half_spread_ticks = self.half_spread_bps * self.ticks_per_bp;
        let a_mid_per_sec = estimate.a_mid_per_sec(half_spread_ticks);
        let k_per_bp = estimate.k_per_tick * self.ticks_per_bp;
        let coefficients = self
            .params
            .coefficients(a_mid_per_sec, k_per_bp, self.sigma_bps)?;
        let depth_ticks = match side {
            QuoteSide::Bid => coefficients.bid_depth(self.inventory),
            QuoteSide::Ask => coefficients.ask_depth(self.inventory),
        } * self.ticks_per_bp;
        // Nearest tick, then clamped to the far edge of the minimum-distance band: the strategy
        // never quotes tighter than MIN_QUOTE_DISTANCE_BPS from fair, which also stops an
        // inventory-skewed δ(q) from crossing its own mid — the old fair-side clamp is the band
        // edge's special case. The engine snaps a bid down and an ask up onto the same grid this
        // already sits on, so the snap is idempotent and the price recorded is the price declared.
        let min_distance_ticks = MIN_QUOTE_DISTANCE_BPS * self.ticks_per_bp;
        let tick_index = match side {
            QuoteSide::Bid => ((self.fair_ticks - depth_ticks).round() as i64)
                .min((self.fair_ticks - min_distance_ticks).floor() as i64),
            QuoteSide::Ask => ((self.fair_ticks + depth_ticks).round() as i64)
                .max((self.fair_ticks + min_distance_ticks).ceil() as i64),
        };
        let GueantSideColumns {
            half_spread_bps: half_spread,
            skew_bps: skew,
            price,
        } = self.features.gueant(side);
        ctx.emit(half_spread, instrument, coefficients.half_spread());
        ctx.emit(skew, instrument, coefficients.skew_per_inventory());
        // A half-spread wider than the whole price would put the bid through zero, which is
        // invalid. Both conditions here gate negative and wrapped prices.
        if tick_index > 0
            && let Some(mantissa) = tick_index.checked_mul(self.tick.0)
        {
            ctx.emit(price, instrument, Price(mantissa).to_f64());
            return Some(Price(mantissa));
        }
        None
    }
}

pub(crate) fn emit_side_intensity(
    ctx: &mut StrategyCtx<'_>,
    columns: IntensityColumns,
    instrument: InstrumentId,
    side: Option<SideEstimate>,
    ticks_per_bp: Option<f64>,
) {
    let IntensityColumns { a, k } = columns;
    let Some(estimate) = side.and_then(SideEstimate::fresh) else {
        return;
    };
    ctx.emit(a, instrument, estimate.a_per_sec);
    if let Some(scale) = ticks_per_bp {
        ctx.emit(k, instrument, estimate.k_per_tick * scale);
    }
}

/// Hawkes trade-arrival kernel. Refit on arrival cadence, record λ + params. Resident O(1) live intensity.
pub(crate) fn emit_side_hawkes(
    ctx: &mut StrategyCtx<'_>,
    features: Features,
    instrument: InstrumentId,
    side: QuoteSide,
    hawkes: &mut HawkesSide,
    now: TsUs,
) {
    let HawkesColumns {
        lambda,
        mu,
        alpha,
        beta,
        branching,
    } = features.hawkes(side);
    // Full simplex over arrival window, once per HAWKES.refit_arrivals. Rest reads resident.
    if hawkes.is_refit_due() {
        hawkes.refit(now);
    }
    if let Some(estimate) = hawkes.estimate().filter(|fit| !fit.is_stale) {
        ctx.emit(mu, instrument, estimate.mu);
        ctx.emit(alpha, instrument, estimate.alpha);
        ctx.emit(beta, instrument, estimate.beta);
        ctx.emit(branching, instrument, estimate.branching_ratio);
    }
    if let Some(live) = hawkes.live() {
        ctx.emit(lambda, instrument, live.intensity(now));
    }
}

/// Realised adverse selection: forward 1/3/5s + reverse 1/5s markouts + pseudo-fills. Short horizons only.
pub(crate) fn emit_side_markouts(
    ctx: &mut StrategyCtx<'_>,
    features: Features,
    instrument: InstrumentId,
    side: QuoteSide,
    markouts: &SideMarkouts,
) {
    let MarkoutColumns {
        forward_1s,
        forward_3s,
        forward_5s,
        reverse_1s,
        reverse_5s,
        fills,
    } = features.markout(side);
    let forward = |horizon| markouts.forward(horizon).ema(MARKOUT_EMA_HALFLIFE);
    let reverse = |horizon| markouts.reverse(horizon).ema(MARKOUT_EMA_HALFLIFE);
    ctx.emit_present(forward_1s, instrument, forward(ForwardHorizon::Secs1));
    ctx.emit_present(forward_3s, instrument, forward(ForwardHorizon::Secs3));
    ctx.emit_present(forward_5s, instrument, forward(ForwardHorizon::Secs5));
    ctx.emit_present(reverse_1s, instrument, reverse(ReverseHorizon::Secs1));
    ctx.emit_present(reverse_5s, instrument, reverse(ReverseHorizon::Secs5));
    ctx.emit(fills, instrument, markouts.fill_count() as f64);
}
