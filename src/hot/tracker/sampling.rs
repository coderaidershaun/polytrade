//! Spin/volume-clock field samplers: same SpinField set from committed inputs (latest/trade/agg).

use super::{BACKING_MULTIPLE, Latest};
use crate::config::SpinField;
use crate::hot::series::FastQueue;
use crate::ids::{FIXED_SCALE, Price, Qty, Side};
use crate::msg::inbound::TradeEvent;

/// Since-last-sample trade aggregates (exact i128, f64 at sample point).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct TradeAggregates {
    qty: i128,
    notional: i128,
    count: u64,
    buy_count: u64,
    sell_count: u64,
}

impl TradeAggregates {
    #[inline]
    pub(super) fn add(&mut self, event: &TradeEvent) {
        self.qty += i128::from(event.qty.0);
        self.notional += notional_i128(event.price, event.qty);
        self.count += 1;
        match event.side {
            Side::Buy => self.buy_count += 1,
            Side::Sell => self.sell_count += 1,
        }
    }

    /// Take: return aggregates + zero them (first call gets all, retakes see zeros).
    #[inline]
    pub(super) fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    /// Restate notional volume-clock samples (closed bar = exact target, accumulated = wrong per crossing).
    #[inline]
    pub(super) fn with_notional(mut self, notional: i128) -> Self {
        self.notional = notional;
        self
    }
}

/// Rolling window per SpinField (shared by both sampling clocks).
#[derive(Debug, Clone, PartialEq)]
pub(super) struct FieldSeries {
    series: Vec<(SpinField, FastQueue<f64>)>,
}

impl FieldSeries {
    pub(super) fn new(fields: &[SpinField], window: usize) -> Self {
        let series = fields
            .iter()
            .map(|&field| (field, FastQueue::new(window, BACKING_MULTIPLE)))
            .collect();
        Self { series }
    }

    pub(super) fn get(&self, field: SpinField) -> Option<&FastQueue<f64>> {
        self.series
            .iter()
            .find(|(each, _)| *each == field)
            .map(|(_, queue)| queue)
    }

    pub(super) fn clear(&mut self) {
        for (_, queue) in &mut self.series {
            queue.clear();
        }
    }

    /// Sample per field: book from latest, last_trade from newest trade, agg from reset trades (always sample).
    pub(super) fn sample(
        &mut self,
        latest: Latest,
        last_trade: Option<TradeEvent>,
        trades: TradeAggregates,
    ) {
        for (field, queue) in &mut self.series {
            let value = match field {
                SpinField::Microprice => latest.microprice,
                SpinField::Spread => latest.spread,
                SpinField::BestBid => latest.best_bid.map(Price::to_f64),
                SpinField::BestAsk => latest.best_ask.map(Price::to_f64),
                SpinField::Mid => latest.mid(),
                SpinField::Imbalance => latest.imbalance,
                SpinField::LastTradePrice => last_trade.map(|trade| trade.price.to_f64()),
                SpinField::LastTradeQty => last_trade.map(|trade| trade.qty.to_f64()),
                SpinField::TradedQty => Some(mantissa_f64(trades.qty)),
                SpinField::TradedNotional => Some(mantissa_f64(trades.notional)),
                SpinField::TradeCount => Some(trades.count as f64),
                SpinField::BuyTradeCount => Some(trades.buy_count as f64),
                SpinField::SellTradeCount => Some(trades.sell_count as f64),
            };
            if let Some(value) = value {
                queue.push(value);
            }
        }
    }
}

/// Time-clock sampler: field series + own trade aggregates.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct SpinSampler {
    pub(super) series: FieldSeries,
    pub(super) trades: TradeAggregates,
}

impl SpinSampler {
    pub(super) fn new(fields: &[SpinField], window: usize) -> Self {
        Self {
            series: FieldSeries::new(fields, window),
            trades: TradeAggregates::default(),
        }
    }

    pub(super) fn reset(&mut self) {
        self.series.clear();
        self.trades = TradeAggregates::default();
    }
}

/// Notional (1e-8 mantissa) in i128 (never overflow on volume-clock accumulation).
#[inline]
pub(super) fn notional_i128(price: Price, qty: Qty) -> i128 {
    i128::from(price.0) * i128::from(qty.0) / i128::from(FIXED_SCALE)
}

/// Stats-only mantissa→f64 (series values, never key/money).
#[inline]
fn mantissa_f64(mantissa: i128) -> f64 {
    mantissa as f64 / FIXED_SCALE as f64
}
