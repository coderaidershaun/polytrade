//! Per-instrument state hub: preallocate config-declared series, handlers only push (zero steady alloc).

mod candles;
mod sampling;
mod volume;

use candles::CandleBook;
use sampling::SpinSampler;
use volume::VolumeClock;

use crate::config::{KlineInterval, SpinField, TrackerSpec};
use crate::hot::book::Book;
use crate::hot::quant::intensity::IntensityCounts;
use crate::hot::quant::micro;
use crate::hot::series::FastQueue;
use crate::ids::{Price, Side};
use crate::msg::inbound::{KlineEvent, TradeEvent};
use crate::warn;

pub use candles::{Candle, CandleSeries};
pub use volume::{VolumeBar, VolumeBarSeries};

/// Backing multiple for every tracker FastQueue.
const BACKING_MULTIPLE: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SideFilter {
    All,
    Buy,
    Sell,
}

/// Rolling series per window, matched on FastQueue::window().
#[derive(Debug, Clone, PartialEq)]
struct Windows {
    series: Vec<FastQueue<f64>>,
}

impl Windows {
    fn new(windows: &[usize]) -> Self {
        Self {
            series: windows
                .iter()
                .map(|&window| FastQueue::new(window, BACKING_MULTIPLE))
                .collect(),
        }
    }

    fn push(&mut self, value: f64) {
        for queue in &mut self.series {
            queue.push(value);
        }
    }

    fn get(&self, window: usize) -> Option<&FastQueue<f64>> {
        self.series.iter().find(|queue| queue.window() == window)
    }

    fn clear(&mut self) {
        for queue in &mut self.series {
            queue.clear();
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ImbalanceStream {
    top_n: usize,
    series: Windows,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
struct Latest {
    microprice: Option<f64>,
    spread: Option<f64>,
    imbalance: Option<f64>,
    best_bid: Option<Price>,
    best_ask: Option<Price>,
}

impl Latest {
    #[inline]
    fn mid(self) -> Option<f64> {
        self.best_bid
            .zip(self.best_ask)
            .map(|(bid, ask)| micro::mid(bid, ask))
    }
}

/// Per-instrument tracker (series optional when unconfigured).
#[derive(Debug, Clone, PartialEq)]
pub struct MicroTracker {
    trades_all: Option<Windows>,
    trades_buy: Option<Windows>,
    trades_sell: Option<Windows>,
    microprice: Option<Windows>,
    imbalance: Option<ImbalanceStream>,
    candles: Option<CandleBook>,
    spin: Option<SpinSampler>,
    volume: Option<VolumeClock>,
    intensity: Option<IntensityCounts>,
    latest: Latest,
    last_trade: Option<TradeEvent>,
    unconfigured_klines: u64,
}

impl MicroTracker {
    /// Preallocate config series (one per kline_intervals).
    /// # Panics
    /// intensity set but no tick_size, or volume_bars.threshold overflows i64 (startup gate refuses).
    pub fn new(
        spec: &TrackerSpec,
        kline_intervals: &[KlineInterval],
        tick_size: Option<Price>,
    ) -> Self {
        Self {
            trades_all: spec.trades_all.as_ref().map(|w| Windows::new(&w.windows)),
            trades_buy: spec.trades_buy.as_ref().map(|w| Windows::new(&w.windows)),
            trades_sell: spec.trades_sell.as_ref().map(|w| Windows::new(&w.windows)),
            microprice: spec.microprice.as_ref().map(|w| Windows::new(&w.windows)),
            imbalance: spec.imbalance.as_ref().map(|s| ImbalanceStream {
                top_n: s.top_n,
                series: Windows::new(&s.windows),
            }),
            candles: spec
                .candles
                .as_ref()
                .map(|s| CandleBook::new(s, kline_intervals)),
            spin: spec
                .spin_sampled
                .as_ref()
                .map(|s| SpinSampler::new(&s.fields, s.window)),
            volume: spec.volume_bars.as_ref().map(VolumeClock::new),
            intensity: spec.intensity.as_ref().map(|s| {
                let tick = tick_size
                    .expect("intensity configured but instrument tick_size unset — startup gate must refuse");
                IntensityCounts::new(s, tick)
            }),
            latest: Latest::default(),
            last_trade: None,
            unconfigured_klines: 0,
        }
    }

    /// Returns how many volume bars this trade closed — 0 unless a volume clock is configured and
    /// armed. Closed bars land oldest-first in [`Self::volume_bars`]; dispatch fans them out.
    pub fn on_trade(&mut self, event: &TradeEvent) -> usize {
        self.last_trade = Some(*event);
        if let Some(sampler) = self.spin.as_mut() {
            sampler.trades.add(event);
        }
        self.push_trade_series(event);
        let closed = self.cut_volume_bars(event);
        // The trade handler never mutates the book, so `latest` is the pre-trade top of book the
        // reach histogram anchors against.
        let (best_bid, best_ask) = (self.latest.best_bid, self.latest.best_ask);
        if let Some(intensity) = self.intensity.as_mut() {
            intensity.on_trade(event, best_bid, best_ask);
        }
        closed
    }

    fn push_trade_series(&mut self, event: &TradeEvent) {
        let side_stream = match event.side {
            Side::Buy => self.trades_buy.as_mut(),
            Side::Sell => self.trades_sell.as_mut(),
        };
        let price = event.price.to_f64();
        for stream in [self.trades_all.as_mut(), side_stream]
            .into_iter()
            .flatten()
        {
            stream.push(price);
        }
    }

    fn cut_volume_bars(&mut self, event: &TradeEvent) -> usize {
        let latest = self.latest;
        let Some(clock) = self.volume.as_mut() else {
            return 0;
        };
        clock.on_trade(event, latest)
    }

    /// Recomputes book-derived series and scalars, returning the microprice computed *this* event —
    /// `None` on a one-sided book. Callers feeding EWMA vol must gate on `Some`, not the stale slot.
    pub fn on_book(&mut self, book: &Book) -> Option<f64> {
        let (bid, ask) = book.best_bid().zip(book.best_ask())?;
        let microprice = micro::microprice(bid, ask);
        let spread = micro::spread(bid.price, ask.price);
        self.latest.microprice = Some(microprice);
        self.latest.spread = Some(spread);
        self.latest.best_bid = Some(bid.price);
        self.latest.best_ask = Some(ask.price);
        if let Some(series) = self.microprice.as_mut() {
            series.push(microprice);
        }
        if let Some(stream) = self.imbalance.as_mut() {
            let value = micro::imbalance(book.bids(), book.asks(), stream.top_n);
            stream.series.push(value);
            self.latest.imbalance = Some(value);
        }
        Some(microprice)
    }

    /// Clears the latest book-derived scalars on reset; the series stay (history). Reads `None`
    /// until a fresh snapshot.
    pub fn on_book_reset(&mut self) {
        self.latest = Latest::default();
    }

    /// Wipes every configured series, latest, and trade-side state to the post-`new` state, keeping the
    /// preallocated backing (zero alloc) — a rotation is a new distribution, so old-window history must
    /// not bleed in (stronger than a book reset, which keeps the series). The lifetime
    /// `unconfigured_klines` counter is not window state and survives.
    #[cold]
    pub fn on_rotation(&mut self) {
        for series in [
            self.trades_all.as_mut(),
            self.trades_buy.as_mut(),
            self.trades_sell.as_mut(),
            self.microprice.as_mut(),
            self.imbalance.as_mut().map(|stream| &mut stream.series),
        ]
        .into_iter()
        .flatten()
        {
            series.clear();
        }
        if let Some(candles) = self.candles.as_mut() {
            candles.clear();
        }
        if let Some(sampler) = self.spin.as_mut() {
            sampler.reset();
        }
        if let Some(clock) = self.volume.as_mut() {
            clock.reset();
        }
        if let Some(intensity) = self.intensity.as_mut() {
            intensity.clear();
        }
        self.latest = Latest::default();
        self.last_trade = None;
    }

    pub fn on_kline(&mut self, event: &KlineEvent) {
        let Some(candles) = self.candles.as_mut() else {
            return;
        };
        let Some(series) = candles.get_mut(event.interval) else {
            self.drop_unconfigured_kline(event);
            return;
        };
        let candle = Candle::from_event(event);
        if !event.is_closed {
            series.open = Some(candle);
            return;
        }
        series.closed.push(candle);
        series.open = None;
        if event.interval == KlineInterval::OneMinute {
            self.retarget_volume_clock();
        }
    }

    /// An interval this instrument does not track. Warns once and counts the rest: a config or
    /// subscription mismatch holds for the life of the run, so warning per message would put the
    /// hot path in a permanent print loop over a condition one line already described.
    #[cold]
    fn drop_unconfigured_kline(&mut self, event: &KlineEvent) {
        if self.unconfigured_klines == 0 {
            warn!(
                "kline for unconfigured interval {} on instrument {} dropped",
                event.interval.as_str(),
                event.instrument.0
            );
        }
        self.unconfigured_klines += 1;
    }

    /// A klines-mode volume target is the trailing 1m quote-volume average, so it moves whenever a
    /// 1m candle closes. Reads the candle book through the field, not [`Self::candles`], so the
    /// clock and the series borrows stay disjoint.
    fn retarget_volume_clock(&mut self) {
        let Some(clock) = self.volume.as_mut() else {
            return;
        };
        let one_minute = self
            .candles
            .as_ref()
            .and_then(|candles| candles.get(KlineInterval::OneMinute));
        if let Some(series) = one_minute {
            clock.refresh_target(series);
        }
    }

    /// Samples from committed state, never the live book (a spin tick can catch it mid-update): book
    /// fields from `latest`, trades from the newest trade and this tick's taken-and-reset aggregates.
    /// The sample carries no timestamp of its own — its position in the series IS the tick it
    /// belongs to, which is why the tick itself is not an argument.
    pub fn on_spin(&mut self) {
        let latest = self.latest;
        let last_trade = self.last_trade;
        let Some(sampler) = self.spin.as_mut() else {
            return;
        };
        let trades = sampler.trades.take();
        sampler.series.sample(latest, last_trade, trades);
    }

    /// The rolling series for `window`; `None` when this stream or window is not configured.
    pub fn trades_price(&self, side: SideFilter, window: usize) -> Option<&FastQueue<f64>> {
        self.trade_stream(side)?.get(window)
    }

    pub fn microprice_series(&self, window: usize) -> Option<&FastQueue<f64>> {
        self.microprice.as_ref()?.get(window)
    }

    pub fn imbalance_series(&self, window: usize) -> Option<&FastQueue<f64>> {
        self.imbalance.as_ref()?.series.get(window)
    }

    pub fn candles(&self, interval: KlineInterval) -> Option<&CandleSeries> {
        self.candles.as_ref()?.get(interval)
    }

    /// Closed volume bars plus the still-filling one; `None` when no volume clock is configured.
    pub fn volume_bars(&self) -> Option<&VolumeBarSeries> {
        Some(self.volume.as_ref()?.bars())
    }

    /// The time-clock series for `field`; same sample-and-hold contract as [`Self::volume_sampled`].
    pub fn spin_sampled(&self, field: SpinField) -> Option<&FastQueue<f64>> {
        self.spin.as_ref()?.series.get(field)
    }

    /// The volume-clock series for `field`. Sample-and-hold: a bar close records the standing
    /// `latest` even if transiently one-sided; after a reset, cleared fields are skipped until
    /// recomputed.
    pub fn volume_sampled(&self, field: SpinField) -> Option<&FastQueue<f64>> {
        self.volume.as_ref()?.sampled()?.get(field)
    }

    #[inline]
    pub fn last_microprice(&self) -> Option<f64> {
        self.latest.microprice
    }

    #[inline]
    pub fn last_spread(&self) -> Option<f64> {
        self.latest.spread
    }

    #[inline]
    pub fn last_imbalance(&self) -> Option<f64> {
        self.latest.imbalance
    }

    #[inline]
    pub fn best_bid(&self) -> Option<Price> {
        self.latest.best_bid
    }

    #[inline]
    pub fn best_ask(&self) -> Option<Price> {
        self.latest.best_ask
    }

    /// Mid of the COMMITTED top of book, `None` while one side is unknown. Differs from the live
    /// book mid-resync — a caller wanting what the book holds right now must read the book.
    #[inline]
    pub fn mid(&self) -> Option<f64> {
        self.latest.mid()
    }

    /// Klines dropped because their interval is not tracked on this instrument.
    pub fn unconfigured_kline_count(&self) -> u64 {
        self.unconfigured_klines
    }

    /// The trade-intensity reach histograms, when configured — the input a strategy's
    /// [`IntensityFit`](crate::hot::quant::intensity::IntensityFit) pulls each fit.
    pub fn intensity(&self) -> Option<&IntensityCounts> {
        self.intensity.as_ref()
    }

    fn trade_stream(&self, side: SideFilter) -> Option<&Windows> {
        match side {
            SideFilter::All => self.trades_all.as_ref(),
            SideFilter::Buy => self.trades_buy.as_ref(),
            SideFilter::Sell => self.trades_sell.as_ref(),
        }
    }
}
