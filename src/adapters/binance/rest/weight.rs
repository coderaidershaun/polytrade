//! Per-minute IP-weight budget + order-count tracking (separate limit).

use std::time::{Duration, Instant};

use crate::config::BinanceMarket;
use crate::{info, warn};

pub(super) const ORDER_COUNT_PREFIX: &str = "x-mbx-order-count-";

pub(super) struct WeightBudget {
    limit_per_minute: u32,
    used_this_window: u32,
    window_start: Instant,
    has_warned_this_window: bool,
}

impl WeightBudget {
    pub(super) fn new(limit_per_minute: u32) -> Self {
        Self {
            limit_per_minute,
            used_this_window: 0,
            window_start: Instant::now(),
            has_warned_this_window: false,
        }
    }

    pub(super) fn charge(&mut self, weight: u32, endpoint: &str) {
        if self.window_start.elapsed() >= Duration::from_secs(60) {
            self.window_start = Instant::now();
            self.used_this_window = 0;
            self.has_warned_this_window = false;
        }
        self.used_this_window = self.used_this_window.saturating_add(weight);
        // Once per window, on the crossing. A resync storm is precisely when this fires, and
        // repeating it per call would bury the events worth reading under one repeated sentence.
        if self.has_warned_this_window || self.used_this_window <= self.limit_per_minute / 2 {
            return;
        }
        self.has_warned_this_window = true;
        warn!(
            "binance rest weight {}/{} this minute after {} (call weight {})",
            self.used_this_window, self.limit_per_minute, endpoint, weight
        );
    }

    pub(super) fn observe_server(&mut self, used: u32) {
        self.used_this_window = self.used_this_window.max(used);
    }
}

/// Venue order-count window (10s, 1d on spot).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderCountWindow {
    /// Variable intervals (not fixed-1m like IP weight).
    pub(super) interval: Box<str>,
    pub(super) used: u32,
    pub(super) peak: u32,
}

/// Observed and surfaced, never enforced. Differs from IP weight: tracked per ACCOUNT (shared across
/// hosts), only UNFILLED orders count (fill decrements it). Silence = "not reported", never "zero".
#[derive(Debug, Default)]
pub(super) struct OrderCountBudget {
    windows: Vec<OrderCountWindow>,
}

impl OrderCountBudget {
    pub(super) fn observe(&mut self, interval: &str, used: u32) {
        let Some(window) = self
            .windows
            .iter_mut()
            .find(|window| &*window.interval == interval)
        else {
            info!("binance unfilled-order count {used} per {interval} (first report this run)");
            self.windows.push(OrderCountWindow {
                interval: interval.into(),
                used,
                peak: used,
            });
            return;
        };
        window.used = used;
        if used > window.peak {
            window.peak = used;
            warn!("binance unfilled-order count peak {used} per {interval}");
        }
    }
}

impl OrderCountWindow {
    /// Extract interval from header name (multiple intervals per spot).
    pub fn interval_of_header(header_name: &str) -> Option<&str> {
        header_name
            .strip_prefix(ORDER_COUNT_PREFIX)
            .filter(|interval| !interval.is_empty())
    }
}

pub(super) fn weight_budget(market: BinanceMarket) -> u32 {
    match market {
        BinanceMarket::Spot => 6000,
        BinanceMarket::Perpetual => 2400,
    }
}

pub(super) fn depth_weight(market: BinanceMarket, limit: u32) -> u32 {
    match market {
        BinanceMarket::Spot => match limit {
            0..=100 => 5,
            101..=500 => 25,
            501..=1000 => 50,
            _ => 250,
        },
        BinanceMarket::Perpetual => match limit {
            0..=50 => 2,
            51..=100 => 5,
            101..=500 => 10,
            _ => 20,
        },
    }
}

pub(super) fn klines_weight(market: BinanceMarket, limit: u32) -> u32 {
    match market {
        BinanceMarket::Spot => 2,
        BinanceMarket::Perpetual => match limit {
            0..=99 => 1,
            100..=499 => 2,
            500..=1000 => 5,
            _ => 10,
        },
    }
}

pub(super) fn exchange_info_weight(market: BinanceMarket) -> u32 {
    match market {
        BinanceMarket::Spot => 20,
        BinanceMarket::Perpetual => 1,
    }
}
