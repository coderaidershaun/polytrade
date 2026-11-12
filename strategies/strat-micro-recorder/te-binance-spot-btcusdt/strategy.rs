//! Records microstructure columns + quotes Guéant levels each spin. Per-instrument state in models/features/params siblings.

use polysim::config::{Instruments, StrategySpec, TableKind};
use polysim::hot::quant::hawkes::HawkesChoice;
use polysim::hot::quant::liquidity::KylesLambdaSpec;
use polysim::hot::quant::micro::{PriceBand, ResilienceSample, banded_imbalance};
use polysim::hot::quant::pricing::{GueantParams, Objective};
use polysim::hot::quant::toxicity::{MarkoutSide, vpin};
use polysim::hot::quant::volatility::Returns;
use polysim::hot::strategy::{
    BookState, DesiredQuote, EngineView, FeatureId, LinkFrame, OrderStyle, QuoteLevel,
    Registration, Strategy, StrategyConfig, StrategyCtx, VolumeBar, resolve_filter,
};
use polysim::ids::{FIXED_SCALE, InstrumentId, Price, Side};
use polysim::msg::inbound::{BookChunk, KlineEvent, SpinTick, TradeEvent};
use polysim::time::{DurationUs, TsUs};

#[path = "../common.rs"]
mod common;
#[path = "features.rs"]
pub(crate) mod features;
#[path = "models.rs"]
pub(crate) mod models;
#[path = "params.rs"]
mod params;

use features::{FEATURE_NAMES, Features, QuoteSide};
use models::{
    BPS, BinanceVol, GueantInputs, GueantScale, InstrumentSetup, InstrumentState,
    MIN_QUOTE_DISTANCE_BPS, RiskBudget, effective_log_vol, emit_side_hawkes, emit_side_intensity,
    emit_side_markouts, gated_realised_vol, level_qty_at, qty_at, ticks_per_bp,
};
pub use params::MicroRecorderParams;

const GAMMA_BPS: f64 = 10.0;

const OBI_BAND_HALF_WIDTH_BPS: f64 = MIN_QUOTE_DISTANCE_BPS;

const OBJECTIVE: Objective = Objective::InventoryPenalty;

const KYLES_LAMBDA: KylesLambdaSpec = KylesLambdaSpec {
    window: 100,
    min_observations: 10,
    min_flow_variance: 1.0e-12,
    min_sign_fraction: 0.2,
};

const HAWKES: HawkesChoice = HawkesChoice {
    max_events: HAWKES_MAX_EVENTS,
    min_events: 100,
    refit_arrivals: HAWKES_REFIT_ARRIVALS,
};

const HAWKES_MAX_EVENTS: usize = 1024;

/// Refit cadence: every 1/8 window, 7/8 old data overlap → scales by market pace.
const HAWKES_REFIT_ARRIVALS: usize = HAWKES_MAX_EVENTS / 8;

const VPIN_BUCKETS_ST: usize = 5;
const VPIN_BUCKETS_LT: usize = 60;

pub struct MicroRecorder {
    has_features_table: bool,
    /// Sampling cadence: scales all derived window lengths + buffer sizes.
    spin_interval: DurationUs,
    features: Option<Features>,
    filter: Instruments,
    gueant: GueantParams,
    /// Δ as exact mantissa (quote units).
    order_notional: i64,
    by_instrument: Vec<InstrumentState>,
    /// Peer's instrument row for link column.
    link_target: Option<InstrumentId>,
    /// Link slot index -> feature column, so `on_link` is a zip rather than twenty branches.
    poly_features: Option<[FeatureId; common::LINK_FIELDS.len()]>,
}

impl MicroRecorder {
    fn recorded_mut(&mut self, instrument: InstrumentId) -> Option<&mut BinanceVol> {
        self.by_instrument
            .get_mut(usize::from(instrument.0))
            .filter(|state| state.is_recorded)
            .map(|state| &mut state.binance)
    }
}

impl StrategyConfig for MicroRecorder {
    type Params = MicroRecorderParams;

    fn from_spec(spec: &StrategySpec<MicroRecorderParams>, engine: EngineView) -> Self {
        Self {
            has_features_table: spec.tables.contains(&TableKind::Features),
            spin_interval: engine.spin_interval,
            features: None,
            filter: spec.instruments.clone(),
            gueant: GueantParams::new(GAMMA_BPS, spec.params.order_notional, OBJECTIVE),
            order_notional: (spec.params.order_notional * FIXED_SCALE as f64).round() as i64,
            by_instrument: Vec::new(),
            link_target: None,
            poly_features: None,
        }
    }
}

impl Strategy for MicroRecorder {
    fn features(&self) -> &'static [&'static str] {
        FEATURE_NAMES
    }

    fn link_fields(&self) -> &'static [&'static str] {
        &common::LINK_FIELDS
    }

    /// Declares topics (never sends). List resolves topic name → peer ID on wire.
    fn link_topics(&self) -> &'static [&'static str] {
        common::LINK_TOPICS
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.poly_features = Some(registration.feature_ids_of(&common::LINK_FIELDS));
        self.features = self
            .has_features_table
            .then(|| Features::from_ids(registration.features));
        let instruments = registration.instruments;
        let recorded = resolve_filter(&self.filter, instruments);
        self.link_target = instruments
            .iter()
            .find(|row| recorded.contains(row.instrument_id))
            .map(|row| row.instrument_id);
        let setup = InstrumentSetup {
            spin_interval: self.spin_interval,
            kyles_lambda: KYLES_LAMBDA,
            hawkes: HAWKES,
            order_notional: self.order_notional,
        };
        self.by_instrument = instruments
            .iter()
            .map(|row| InstrumentState::new(row, recorded.contains(row.instrument_id), setup))
            .collect();
    }

    /// NaN is the peer's word for absent — an unfilled role, an unquoted side, a fit it does not yet
    /// have. Recording it would put a hole in the column that reads like a number.
    fn on_link(&mut self, ctx: &mut StrategyCtx<'_>, frame: &LinkFrame) {
        let (Some(poly_features), Some(instrument)) = (self.poly_features, self.link_target) else {
            return;
        };
        for (&feature, &value) in poly_features.iter().zip(frame.payload.values()) {
            if value.is_finite() {
                ctx.emit(feature, instrument, value);
            }
        }
    }

    /// A park breaks the time series models (EGARCH, Hawkes, intensity, resilience, markouts, Kyle
    /// window), so each restarts in place without allocating. The position is neither reset nor held
    /// here: the engine owns the ledger and carries it through a park on purpose, since parking sells
    /// nothing. A copy on this side would be a second truth free to drift from the one the quote
    /// leans on.
    fn on_resume(&mut self, _ctx: &mut StrategyCtx<'_>) {
        for state in &mut self.by_instrument {
            state.binance.reset();
        }
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let Some(features) = self.features else {
            return;
        };
        let now = ctx.event_ts();
        let cfg = SpinCfg {
            features,
            gueant: self.gueant,
            spin_interval: self.spin_interval,
            order_notional: self.order_notional,
        };
        for (index, state) in self.by_instrument.iter_mut().enumerate() {
            if !state.is_recorded {
                continue;
            }
            let instrument = InstrumentId(index as u16);
            record_spin(ctx, cfg, instrument, state, now);
        }
    }

    fn on_kline(&mut self, _ctx: &mut StrategyCtx<'_>, event: &KlineEvent) {
        if !event.is_closed {
            return;
        }
        let Some(model) = self.recorded_mut(event.instrument) else {
            return;
        };
        if event.interval != model.interval {
            return;
        }
        model.closes.push(event.close.to_f64());
        model.is_dirty = true;
    }

    fn on_trade(&mut self, _ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        let Some(model) = self.recorded_mut(event.instrument) else {
            return;
        };
        model.markouts.on_trade(event);
        let hawkes = match event.side {
            Side::Buy => &mut model.hawkes_ask,
            Side::Sell => &mut model.hawkes_bid,
        };
        hawkes.on_arrival(event.exchange_ts_us, event.received_ts_us);
    }

    fn on_book_update(&mut self, ctx: &mut StrategyCtx<'_>, chunk: &BookChunk) {
        if !chunk.is_last_chunk {
            return;
        }
        let Some(model) = self.recorded_mut(chunk.instrument) else {
            return;
        };
        if ctx.book(chunk.instrument).state() != BookState::Valid {
            return;
        }
        let mid = ctx.tracker(chunk.instrument).mid();
        model.feed_markout_mid(ctx.event_ts(), mid);
    }

    fn on_book_reset(&mut self, _ctx: &mut StrategyCtx<'_>, instrument: InstrumentId) {
        if let Some(state) = self.by_instrument.get_mut(usize::from(instrument.0)) {
            state.binance.markouts.reset_continuity();
        }
    }

    fn on_volume(&mut self, ctx: &mut StrategyCtx<'_>, instrument: InstrumentId, bar: &VolumeBar) {
        let Some(features) = self.features else {
            return;
        };
        let Some(model) = self.recorded_mut(instrument) else {
            return;
        };
        let imbalance = (bar.buy_notional - bar.sell_notional) as f64 / bar.target as f64;
        ctx.emit(features.volume_bar_imbalance, instrument, imbalance);
        ctx.emit(
            features.volume_bar_duration_secs,
            instrument,
            bar.close_ts_us.diff(bar.open_ts_us).to_secs(),
        );

        let closed = ctx
            .tracker(instrument)
            .volume_bars()
            .map(|series| series.closed.as_slice())
            .unwrap_or(&[]);
        let vpin_short = vpin(closed, VPIN_BUCKETS_ST);
        let vpin_long = vpin(closed, VPIN_BUCKETS_LT);
        if let Some(estimate) = vpin_short {
            ctx.emit(features.vpin_st, instrument, estimate.vpin);
            ctx.emit(
                features.vpin_signed_flow_st,
                instrument,
                estimate.signed_flow,
            );
        }
        if let Some(estimate) = vpin_long {
            ctx.emit(features.vpin_lt, instrument, estimate.vpin);
            ctx.emit(
                features.vpin_signed_flow_lt,
                instrument,
                estimate.signed_flow,
            );
        }

        let Some(feed) = model.kyle.as_mut() else {
            return;
        };
        let flow = (bar.buy_notional - bar.sell_notional) as f64 / FIXED_SCALE as f64;
        let mid_now = ctx.tracker(instrument).mid();
        let Some(estimate) = feed.on_bar(flow, bar.trade_arrivals, mid_now) else {
            return;
        };
        ctx.emit(
            features.kyle_lambda_per_notional,
            instrument,
            estimate.lambda,
        );
        ctx.emit(features.kyle_intercept, instrument, estimate.intercept);
        if let Some(mid) = mid_now {
            let lambda_bps = estimate.lambda / mid * BPS;
            ctx.emit(
                features.kyle_lambda_bps_per_notional,
                instrument,
                lambda_bps,
            );
            ctx.emit_present(
                features.kyle_one_bp_notional,
                instrument,
                Some(1.0 / lambda_bps).filter(|flow| flow.is_finite() && *flow > 0.0),
            );
        }
    }
}

/// `PostOnly`: model prices passive quotes only.
fn declare(
    ctx: &mut StrategyCtx<'_>,
    instrument: InstrumentId,
    side: Side,
    price: Option<Price>,
    order_notional: i64,
) {
    ctx.quote(
        instrument,
        side,
        QuoteLevel::ZERO,
        price.map(|price| DesiredQuote {
            price,
            qty: qty_at(order_notional, price),
            style: OrderStyle::PostOnly,
        }),
    );
}

#[derive(Debug, Clone, Copy)]
struct SpinCfg {
    features: Features,
    gueant: GueantParams,
    spin_interval: DurationUs,
    order_notional: i64,
}

fn record_spin(
    ctx: &mut StrategyCtx<'_>,
    cfg: SpinCfg,
    instrument: InstrumentId,
    state: &mut InstrumentState,
    now: TsUs,
) {
    let max_exposure_quote = state.max_exposure_quote;
    let model = &mut state.binance;
    let features = cfg.features;
    let tracker = ctx.tracker(instrument);
    let mid = tracker.mid();
    let microprice = tracker.last_microprice();
    let imbalance = tracker.last_imbalance();
    let spread = tracker.last_spread();
    // Ledger seeded from persisted cost basis → restart resumes real inventory. Read once per spin so all columns describe same position.
    let exposure_quote = ctx.exposure_quote(instrument);
    let inventory = exposure_quote as f64 / FIXED_SCALE as f64;

    if let Some(mid) = mid {
        model.spin_mids.push(mid);
    }
    model.feed_markout_mid(now, mid);
    if let (Some(mid), Some(equilibrium), Some(spread)) = (mid, microprice, spread) {
        let sample = ResilienceSample {
            event_ts_us: now,
            mid,
            equilibrium,
            half_spread: spread / 2.0,
        };
        if let Some(rate) = model.resilience.on_sample(sample) {
            model.resilience_window.push(rate);
        }
    }
    let intensity = model
        .intensity
        .refit(ctx.tracker(instrument).intensity(), now);
    model.refresh(ctx, instrument);
    let egarch_vol = model
        .estimate
        .map(|estimate| estimate.conditional_vol_per_sec);
    let realised_st = gated_realised_vol(&model.spin_mids, Returns::Log, cfg.spin_interval);
    ctx.emit_present(features.mid, instrument, mid);
    ctx.emit_present(features.microprice, instrument, microprice);
    ctx.emit_present(features.imbalance, instrument, imbalance);
    let book = ctx.book(instrument);
    let obi = (book.state() == BookState::Valid)
        .then(|| book.mid())
        .flatten()
        .map(|book_mid| {
            banded_imbalance(
                book.bids(),
                book.asks(),
                PriceBand::around(book_mid, OBI_BAND_HALF_WIDTH_BPS),
            )
        });
    ctx.emit_present(features.obi_half_bp, instrument, obi);
    ctx.emit_present(features.egarch_vol_lt, instrument, egarch_vol);
    ctx.emit_present(features.realised_vol_st, instrument, realised_st);
    ctx.emit_present(
        features.realised_vol_st_bps,
        instrument,
        realised_st.map(|vol| vol * BPS),
    );
    if let Some(estimate) = model.estimate {
        ctx.emit(features.egarch_omega, instrument, estimate.omega);
        ctx.emit(features.egarch_gamma, instrument, estimate.gamma);
        ctx.emit(features.egarch_theta, instrument, estimate.theta);
        ctx.emit(features.egarch_beta, instrument, estimate.beta);
        ctx.emit(
            features.egarch_uncond_vol_lt,
            instrument,
            estimate.unconditional_vol_per_sec,
        );
    }
    let ewma_vol = ctx.ewma_vol(instrument);
    ctx.emit_present(features.ewma_vol_per_event, instrument, ewma_vol);
    let resilience_median = model.resilience_window.median(&mut model.median_scratch);
    ctx.emit_present(features.resilience_median_1m, instrument, resilience_median);
    ctx.emit_present(
        features.resilience_mean_1m,
        instrument,
        model.resilience_window.mean(),
    );
    let intensity_scale = mid
        .zip(model.tick)
        .map(|(mid, tick)| ticks_per_bp(mid, tick));
    if let Some(estimate) = intensity {
        emit_side_intensity(
            ctx,
            features.intensity(QuoteSide::Bid),
            instrument,
            estimate.bid,
            intensity_scale,
        );
        emit_side_intensity(
            ctx,
            features.intensity(QuoteSide::Ask),
            instrument,
            estimate.ask,
            intensity_scale,
        );
    }
    if mid.is_some() {
        ctx.emit(features.inventory_quote, instrument, inventory);
    }
    let log_vol = effective_log_vol(egarch_vol, realised_st);
    let scale = GueantScale::from_inputs(
        cfg.gueant,
        features,
        GueantInputs {
            tick: model.tick,
            mid,
            spread,
            log_vol,
            inventory,
        },
    );
    let bid_fit = intensity.and_then(|fit| fit.bid);
    let ask_fit = intensity.and_then(|fit| fit.ask);
    let (bid_model, ask_model) = match scale {
        Some(scale) => {
            ctx.emit(features.gueant_sigma_bps, instrument, scale.sigma_bps);
            (
                scale.emit_side(ctx, instrument, QuoteSide::Bid, bid_fit),
                scale.emit_side(ctx, instrument, QuoteSide::Ask, ask_fit),
            )
        }
        None => (None, None),
    };
    // Only the side that GROWS past budget is withdrawn (see RiskBudget::would_breach). Unmeasured reads FLAT but harmless: models are None without a mid, mid is set with mark, so on every spin with a withdrawal, exposure_quote is real.
    let budget = RiskBudget {
        order_notional: cfg.order_notional,
        max_exposure_quote,
    };
    let bid_quote = bid_model.filter(|_| !budget.would_breach(exposure_quote, Side::Buy));
    let ask_quote = ask_model.filter(|_| !budget.would_breach(exposure_quote, Side::Sell));
    polysim::strategy_info!(
        ctx,
        "gueant i{} in: mid={mid:?} spread={spread:?} log_vol={log_vol:?} sigma_bps={:?} \
         a_bid={:?} k_bid={:?} a_ask={:?} k_ask={:?}",
        instrument.0,
        scale.map(|scale| scale.sigma_bps),
        bid_fit.map(|fit| fit.a_per_sec),
        bid_fit.map(|fit| fit.k_per_tick),
        ask_fit.map(|fit| fit.a_per_sec),
        ask_fit.map(|fit| fit.k_per_tick),
    );
    polysim::strategy_info!(
        ctx,
        "gueant i{} out: q={} order={} budget={} model_bid={:?} model_ask={:?} \
         quoted_bid={:?} quoted_ask={:?}",
        instrument.0,
        inventory,
        cfg.order_notional as f64 / FIXED_SCALE as f64,
        max_exposure_quote as f64 / FIXED_SCALE as f64,
        bid_model.map(Price::to_f64),
        ask_model.map(Price::to_f64),
        bid_quote.map(Price::to_f64),
        ask_quote.map(Price::to_f64),
    );
    let (markouts_bid, markouts_ask) = (model.markouts.bid(), model.markouts.ask());
    polysim::strategy_info!(
        ctx,
        "drops i{} markout gaps={}/{} stale={}/{} overflow={}/{} hawkes_out_of_order={}/{}",
        instrument.0,
        markouts_bid.reverse_gap_count(),
        markouts_ask.reverse_gap_count(),
        markouts_bid.stale_maturation_count(),
        markouts_ask.stale_maturation_count(),
        markouts_bid.pending_overflow_count(),
        markouts_ask.pending_overflow_count(),
        model.hawkes_bid.out_of_order_count(),
        model.hawkes_ask.out_of_order_count(),
    );
    // A side we would not be quoting this spin must not fill: no quote disarms it until one returns.
    // The level's current qty arms the queue gate — an order joining that level now would rest
    // behind exactly what is showing there.
    match bid_quote {
        Some(price) => model.markouts.arm(
            MarkoutSide::Bid,
            price,
            level_qty_at(ctx.book(instrument).bids(), price),
        ),
        None => model.markouts.disarm(MarkoutSide::Bid),
    }
    match ask_quote {
        Some(price) => model.markouts.arm(
            MarkoutSide::Ask,
            price,
            level_qty_at(ctx.book(instrument).asks(), price),
        ),
        None => model.markouts.disarm(MarkoutSide::Ask),
    }
    declare(ctx, instrument, Side::Buy, bid_quote, cfg.order_notional);
    declare(ctx, instrument, Side::Sell, ask_quote, cfg.order_notional);
    if mid.is_some() {
        emit_side_markouts(
            ctx,
            features,
            instrument,
            QuoteSide::Bid,
            model.markouts.bid(),
        );
        emit_side_markouts(
            ctx,
            features,
            instrument,
            QuoteSide::Ask,
            model.markouts.ask(),
        );
    }
    emit_side_hawkes(
        ctx,
        features,
        instrument,
        QuoteSide::Bid,
        &mut model.hawkes_bid,
        now,
    );
    emit_side_hawkes(
        ctx,
        features,
        instrument,
        QuoteSide::Ask,
        &mut model.hawkes_ask,
        now,
    );
}
