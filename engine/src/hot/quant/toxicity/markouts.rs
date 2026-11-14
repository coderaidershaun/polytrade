//! Toxic-flow markouts: pseudo-fills price adverse selection (zero alloc).

use super::markout_side::{BACKING_MULTIPLE, MarkoutSide, MidSample, SideMarkouts, spins_in};
use crate::hot::series::FastQueue;
use crate::msg::ids::{Price, Qty};
use crate::msg::inbound::TradeEvent;
use crate::msg::time::{DurationUs, LocalTs};

/// Each series holds as many markouts as there are spins in this span. A series grows only on a
/// fill, so it holds the last N fills, which can reach back far more than ten minutes.
const MATURED_CAPACITY: DurationUs = DurationUs::from_secs(600);

/// Mid ring depth = deepest reverse lookback + 1s slack.
const MID_RING: DurationUs = DurationUs::from_secs(6);

/// Fill mature > this many spins past ideal = feed gap, not markout.
const STALE_TOLERANCE_SPINS: i64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarkoutSpec {
    pub spin_interval: DurationUs,
    /// Max mids/sec: book commits + spins.
    pub max_mids_per_sec: u32,
}

/// Pseudo-fill (for UI); price = armed level at receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarkoutFill {
    pub side: MarkoutSide,
    pub price: Price,
    pub fill_ts_us: LocalTs,
}

/// Pseudo-fill markouts: arm quotes each placement, feed prints/mids, read realised bps.
#[derive(Debug, Clone, PartialEq)]
pub struct MarkoutTracker {
    stale_tolerance: DurationUs,
    mids: FastQueue<MidSample>,
    /// The last stamp fed to `on_mid`. A resync keeps it, so the mid clock never runs backwards.
    last_mid_ts_us: Option<LocalTs>,
    bid: SideMarkouts,
    ask: SideMarkouts,
}

impl MarkoutTracker {
    /// # Panics
    /// If spin_interval <=0 or max_mids_per_sec = 0 (init-time bug, buffer lengths derive from these).
    pub fn new(spec: MarkoutSpec) -> Self {
        assert!(
            spec.spin_interval.micros() > 0,
            "markout spin interval must be positive, got {}us",
            spec.spin_interval.micros()
        );
        assert!(
            spec.max_mids_per_sec > 0,
            "markout max_mids_per_sec must be non-zero"
        );

        let matured_capacity = spins_in(MATURED_CAPACITY, spec.spin_interval);
        let mid_capacity = (MID_RING.to_secs() as usize * spec.max_mids_per_sec as usize).max(2);
        Self {
            stale_tolerance: DurationUs::from_micros(
                STALE_TOLERANCE_SPINS * spec.spin_interval.micros(),
            ),
            mids: FastQueue::new(mid_capacity, BACKING_MULTIPLE),
            last_mid_ts_us: None,
            bid: SideMarkouts::new(MarkoutSide::Bid, matured_capacity, spec.spin_interval),
            ask: SideMarkouts::new(MarkoutSide::Ask, matured_capacity, spec.spin_interval),
        }
    }

    /// Arms one side's quote, which fills once nine tenths of the queue shown at its level has
    /// traded. Re-arming at the same price keeps the place in the queue already earned.
    ///
    /// A price at or below zero is no level, and the forward markout divides by the armed price. It
    /// arms nothing and withdraws the side's quote, so no print fills a level that is not shown.
    #[inline]
    pub fn arm(&mut self, side: MarkoutSide, price: Price, level_qty: Qty) {
        if price.mantissa() <= 0 {
            self.disarm(side);
            return;
        }
        self.side_mut(side).arm(price, level_qty);
    }

    /// Withdraws one side's quote without a fill, because a level we are not quoting must not fill.
    #[inline]
    pub fn disarm(&mut self, side: MarkoutSide) {
        self.side_mut(side).armed = None;
    }

    #[inline]
    fn side_mut(&mut self, side: MarkoutSide) -> &mut SideMarkouts {
        match side {
            MarkoutSide::Bid => &mut self.bid,
            MarkoutSide::Ask => &mut self.ask,
        }
    }

    /// Route print by aggressor (seller->bid, buyer->ask). Fill at OUR armed level (not print), stamped at receipt.
    /// Report recorded fill (routed side only) or None if no armed level reached.
    pub fn on_trade(&mut self, trade: &TradeEvent, received_ts_us: LocalTs) -> Option<MarkoutFill> {
        let side = MarkoutSide::from_aggressor(trade.side);
        let markouts = match side {
            MarkoutSide::Bid => &mut self.bid,
            MarkoutSide::Ask => &mut self.ask,
        };
        let price = markouts.on_trade(trade, received_ts_us, &self.mids)?;
        Some(MarkoutFill {
            side,
            price,
            fill_ts_us: received_ts_us,
        })
    }

    /// Feeds a mid and realises each fill that it ripens. The ring is searched by time, so a stamp
    /// below the last fed stamp is raised to it. A mid that is not a positive number is skipped.
    pub fn on_mid(&mut self, ts_us: LocalTs, mid: f64) {
        let ts_us = self.last_mid_ts_us.map_or(ts_us, |last| last.max(ts_us));
        self.last_mid_ts_us = Some(ts_us);
        if !(mid.is_finite() && mid > 0.0) {
            return;
        }
        let sample = MidSample { ts_us, mid };
        self.mids.push(sample);
        self.bid.mature(sample, self.stale_tolerance);
        self.ask.mature(sample, self.stale_tolerance);
    }

    #[inline]
    pub fn bid(&self) -> &SideMarkouts {
        &self.bid
    }

    #[inline]
    pub fn ask(&self) -> &SideMarkouts {
        &self.ask
    }

    /// Book resync: void mid path + quotes + in-flight fills; realised markouts survive.
    pub fn reset_continuity(&mut self) {
        self.mids.clear();
        self.bid.reset_continuity();
        self.ask.reset_continuity();
    }

    /// Wipe realised history for instrument rotation (lifetime counters survive).
    pub fn clear(&mut self) {
        self.last_mid_ts_us = None;
        self.reset_continuity();
        self.bid.clear_series();
        self.ask.clear_series();
    }
}
