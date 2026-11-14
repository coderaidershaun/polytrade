//! The counts that the hot thread keeps as trades print: how often a print reached each depth
//! behind the touch, decayed with time. The fit reads them.

use crate::hot::digest::digest_fields;
use crate::msg::ids::{Price, Side};
use crate::msg::time::{LocalTs, VenueTs};

/// The shape of the counts of one instrument, fixed at its start: the tick that measures the depth
/// of a print, the number of depth buckets, the half-life of the decay, and the count at the touch
/// below which a fit gives no new estimate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct IntensityShape {
    pub(crate) tick: Price,
    pub(crate) max_depth_ticks: usize,
    pub(crate) half_life_secs: f64,
    pub(crate) min_events: f64,
}

/// One trade print as the counts read it. The prints of one sweep share their side and their
/// exchange stamp, and the received stamp drives the decay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TradePrint {
    pub(crate) side: Side,
    pub(crate) price: Price,
    pub(crate) exchange_ts_us: VenueTs,
    pub(crate) received_ts_us: LocalTs,
}

/// The best bid and ask from before the print, which anchor its depth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TopOfBook {
    pub(crate) best_bid: Option<Price>,
    pub(crate) best_ask: Option<Price>,
}

impl TopOfBook {
    /// A buy takes the ask and a sell the bid, so each print is measured from the side it takes.
    fn anchor_for(self, side: Side) -> Option<Price> {
        match side {
            Side::Buy => self.best_ask,
            Side::Sell => self.best_bid,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IntensityCounts {
    tick: Price,
    decay_per_sec: f64,
    pub(super) min_events: f64,
    pub(super) bid_reach: Vec<f64>,
    pub(super) ask_reach: Vec<f64>,
    exposure_secs: f64,
    last_decay_ts: Option<LocalTs>,
    open_group: Option<Group>,
    inside_spread: u64,
    without_book: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Group {
    side: Side,
    exchange_ts_us: VenueTs,
    deepest_bucket: usize,
    anchor: Price,
}

digest_fields! { Group { side, exchange_ts_us, deepest_bucket, anchor } }
digest_fields! {
    IntensityCounts {
        tick, decay_per_sec, min_events, bid_reach, ask_reach, exposure_secs, last_decay_ts,
        open_group, inside_spread, without_book,
    }
}

impl IntensityCounts {
    /// # Panics
    /// Fewer than two depth buckets: the fit needs an interior bin plus the censored tail, and the
    /// bucket arithmetic below would underflow on an empty histogram.
    pub(crate) fn new(shape: IntensityShape) -> Self {
        assert!(
            shape.max_depth_ticks >= 2,
            "intensity histogram needs at least two depth buckets, got {}",
            shape.max_depth_ticks
        );
        Self {
            tick: shape.tick,
            decay_per_sec: std::f64::consts::LN_2 / shape.half_life_secs,
            min_events: shape.min_events,
            bid_reach: vec![0.0; shape.max_depth_ticks],
            ask_reach: vec![0.0; shape.max_depth_ticks],
            exposure_secs: 0.0,
            last_decay_ts: None,
            open_group: None,
            inside_spread: 0,
            without_book: 0,
        }
    }

    /// A buy counts on the ask side and a sell on the bid side. A print with the side and the
    /// exchange stamp of the open sweep extends that sweep; each other print starts a new one.
    pub(crate) fn on_print(&mut self, print: TradePrint, top: TopOfBook) {
        if let Some(group) = self.open_group
            && group.side == print.side
            && group.exchange_ts_us == print.exchange_ts_us
        {
            self.extend_group(group, print.price);
            return;
        }
        self.start_group(print, top);
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

    pub(super) fn max_depth(&self) -> usize {
        self.ask_reach.len()
    }

    fn reach_mut(&mut self, side: Side) -> &mut [f64] {
        match side {
            Side::Buy => &mut self.ask_reach,
            Side::Sell => &mut self.bid_reach,
        }
    }

    fn start_group(&mut self, print: TradePrint, top: TopOfBook) {
        self.decay_to(print.received_ts_us);
        let Some(anchor) = top.anchor_for(print.side) else {
            self.without_book += 1;
            return;
        };
        let bucket = self
            .bucket_for(print.side, anchor, print.price)
            .unwrap_or(0);
        for level in self.reach_mut(print.side)[..=bucket].iter_mut() {
            *level += 1.0;
        }
        self.open_group = Some(Group {
            side: print.side,
            exchange_ts_us: print.exchange_ts_us,
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
    fn decay_to(&mut self, now: LocalTs) {
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
    pub(super) fn decay_to_now(&self, now: LocalTs) -> (f64, f64) {
        let dt = match self.last_decay_ts {
            Some(last) => now.diff(last).to_secs().max(0.0),
            None => return (1.0, 0.0),
        };
        let factor = (-self.decay_per_sec * dt).exp();
        let exposure = self.exposure_secs * factor + (1.0 - factor) / self.decay_per_sec;
        (factor, exposure)
    }
}

fn tick_depth(side: Side, anchor: Price, price: Price, tick: Price) -> i64 {
    let diff = match side {
        Side::Buy => price.mantissa() - anchor.mantissa(),
        Side::Sell => anchor.mantissa() - price.mantissa(),
    };
    diff.div_euclid(tick.mantissa())
}
