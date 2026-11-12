//! The volume clock: trades cut into equal-notional bars. A trade's notional is split exactly at
//! each target, so a closed bar always holds its target and the excess pours into the next bar —
//! the equal-volume buckets toxicity measures (VPIN) are defined over.

use super::sampling::{FieldSeries, TradeAggregates, notional_i128};
use super::{BACKING_MULTIPLE, CandleSeries, Latest};
use crate::config::{TARGET_WINDOW_CANDLES, VolumeBarsSpec, VolumeThreshold};
use crate::hot::series::{Element, FastQueue};
use crate::ids::{FIXED_SCALE, Side};
use crate::msg::inbound::TradeEvent;
use crate::time::TsUs;

const MIN_TARGET_CANDLES: usize = 60;

const MIN_TARGET_NOTIONAL: i128 = FIXED_SCALE as i128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VolumeBar {
    pub open_ts_us: TsUs,
    pub close_ts_us: TsUs,
    pub buy_notional: i64,
    pub sell_notional: i64,
    pub target: i64,
    /// Trades that ARRIVED while this bar was open, which partitions the tape across bars rather
    /// than measuring each bar's activity: a trade worth several targets books its one arrival to
    /// the bar it landed in, and the bars its notional pours through carry a full target and zero.
    pub trade_arrivals: u32,
}

impl crate::hot::series::sealed::Sealed for VolumeBar {}
impl Element for VolumeBar {}

impl VolumeBar {
    fn opening(open_ts_us: TsUs, target: i64) -> Self {
        Self {
            open_ts_us,
            close_ts_us: open_ts_us,
            buy_notional: 0,
            sell_notional: 0,
            target,
            trade_arrivals: 0,
        }
    }

    #[inline]
    fn filled(&self) -> i64 {
        self.buy_notional + self.sell_notional
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VolumeBarSeries {
    pub closed: FastQueue<VolumeBar>,
    pub open: Option<VolumeBar>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct VolumeClock {
    fixed_target: Option<i64>,
    next_target: Option<i64>,
    bars: VolumeBarSeries,
    trades: TradeAggregates,
    sampled: Option<FieldSeries>,
}

impl VolumeClock {
    /// # Panics
    /// Init-boundary only: startup gate bounds mantissa.
    pub(super) fn new(spec: &VolumeBarsSpec) -> Self {
        let fixed_target = match spec.threshold {
            VolumeThreshold::Fixed(usd) => {
                let mantissa = i128::from(usd) * i128::from(FIXED_SCALE);
                Some(i64::try_from(mantissa).expect(
                    "volume_bars.threshold exceeds the i64 notional range — startup gate must refuse",
                ))
            }
            VolumeThreshold::Klines => None,
        };
        Self {
            fixed_target,
            next_target: fixed_target,
            bars: VolumeBarSeries {
                closed: FastQueue::new(spec.keep, BACKING_MULTIPLE),
                open: None,
            },
            trades: TradeAggregates::default(),
            sampled: spec
                .sampled
                .as_ref()
                .map(|s| FieldSeries::new(&s.fields, s.window)),
        }
    }

    pub(super) fn bars(&self) -> &VolumeBarSeries {
        &self.bars
    }

    pub(super) fn sampled(&self) -> Option<&FieldSeries> {
        self.sampled.as_ref()
    }

    /// Dormant clock (no target yet) -> 0 bars closed; bars cut before target arm are incomparable with later bars.
    pub(super) fn on_trade(&mut self, event: &TradeEvent, latest: Latest) -> usize {
        let Some(target) = self.next_target else {
            return 0;
        };
        // A bar that can never fill makes the split loop below take nothing and loop forever — the
        // one failure mode the engine can neither warn about nor drain out of. Every path that arms
        // a target already floors it, and this is where a new one would have to prove it did.
        assert!(
            target > 0,
            "volume bar target must be positive, got {target}"
        );
        self.trades.add(event);
        let arrival = self
            .bars
            .open
            .get_or_insert_with(|| VolumeBar::opening(event.exchange_ts_us, target));
        arrival.trade_arrivals += 1;

        let mut remaining = notional_i128(event.price, event.qty);
        let mut closed = 0;
        loop {
            let bar = self
                .bars
                .open
                .get_or_insert_with(|| VolumeBar::opening(event.exchange_ts_us, target));
            let take = remaining.min(i128::from(bar.target - bar.filled())) as i64;
            match event.side {
                Side::Buy => bar.buy_notional += take,
                Side::Sell => bar.sell_notional += take,
            }
            bar.close_ts_us = event.exchange_ts_us;
            let is_full = bar.filled() == bar.target;
            remaining -= i128::from(take);
            if !is_full {
                return closed;
            }
            let full = *bar;
            self.bars.open = None;
            self.bars.closed.push(full);
            closed += 1;
            // `take` bounded by bar's i64 headroom, narrowing exact
            let aggregates = self.trades.take().with_notional(i128::from(full.target));
            if let Some(sampled) = self.sampled.as_mut() {
                sampled.sample(latest, Some(*event), aggregates);
            }
            if remaining == 0 {
                return closed;
            }
        }
    }

    /// Fixed clock ignores; open bar keeps its current target. Mean < MIN_TARGET_NOTIONAL -> dormant.
    pub(super) fn refresh_target(&mut self, one_minute: &CandleSeries) {
        if self.fixed_target.is_some() {
            return;
        }
        let closed = one_minute.closed.as_slice();
        let window = closed.len().min(TARGET_WINDOW_CANDLES);
        if window < MIN_TARGET_CANDLES {
            self.next_target = None;
            return;
        }
        let total: i128 = closed[closed.len() - window..]
            .iter()
            .map(|candle| i128::from(candle.quote_volume))
            .sum();
        let mean = total / window as i128;
        self.next_target = (mean >= MIN_TARGET_NOTIONAL).then_some(mean as i64);
    }

    pub(super) fn reset(&mut self) {
        self.next_target = self.fixed_target;
        self.bars.closed.clear();
        self.bars.open = None;
        self.trades = TradeAggregates::default();
        if let Some(sampled) = self.sampled.as_mut() {
            sampled.clear();
        }
    }
}
