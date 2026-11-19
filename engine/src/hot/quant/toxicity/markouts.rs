//! Markouts measure adverse selection with no fills of our own: a public print that reaches our
//! quote stands in for a fill, and the mid after it prices the fill. Nothing allocates after new.

use super::markout_side::{BACKING_MULTIPLE, MarkoutSide, MidSample, SideMarkouts, spins_in};
use crate::hot::series::FastQueue;
use crate::msg::ids::{Price, Qty};
use crate::msg::inbound::TradeEvent;
use crate::msg::time::{DurationUs, LocalTs};

/// Each series holds as many markouts as there are spins in this span. A series grows only on a
/// fill, so it holds the last N fills, which can reach back far more than ten minutes.
const MATURED_CAPACITY: DurationUs = DurationUs::from_secs(600);

/// The mid ring reaches back over the deepest reverse lookback, with one second to spare.
const MID_RING: DurationUs = DurationUs::from_secs(6);

/// A fill whose first ripe mid comes more than this many spins late is a feed gap, not a markout.
const STALE_TOLERANCE_SPINS: i64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarkoutSpec {
    pub spin_interval: DurationUs,
    /// The most mids in one second: the book commits and the spins together.
    pub max_mids_per_sec: u32,
}

/// A pseudo-fill, at the armed level of our quote and stamped when the print was received.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarkoutFill {
    pub side: MarkoutSide,
    pub price: Price,
    pub fill_ts_us: LocalTs,
}

/// The markouts of both sides of our quote. Arm each quote at each placement, feed the prints and
/// the mids, and read the realised markouts in basis points.
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
    /// If the spin interval is not positive or the mids per second are 0. Both size the buffers, so
    /// either is a bug of the caller at start.
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

    /// Routes a print by its aggressor: a seller reaches our bid, and a buyer our ask. A fill is at
    /// our armed level, not at the price of the print, and is stamped when the print was received.
    /// Returns the fill, or `None` when the print reached no armed level.
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

    /// After a book resync, drops the mid path, the quotes, and the fills in flight. The realised
    /// markouts stay, because those fills happened.
    pub fn reset_continuity(&mut self) {
        self.mids.clear();
        self.bid.reset_continuity();
        self.ask.reset_continuity();
    }

    /// Wipes the realised markouts too, for a rotation onto another instrument. The lifetime
    /// counters stay.
    pub fn clear(&mut self) {
        self.last_mid_ts_us = None;
        self.reset_continuity();
        self.bid.clear_series();
        self.ask.clear_series();
    }
}
