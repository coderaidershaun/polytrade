//! Toxic-flow markouts: pseudo-fills price adverse selection (zero alloc).

use std::collections::VecDeque;

use super::{ForwardHorizon, MarkoutSpec, ReverseHorizon};
use crate::hot::quant::BPS;
use crate::hot::series::{Element, FastQueue};
use crate::ids::{Price, Qty, Side};
use crate::msg::inbound::TradeEvent;
use crate::time::{DurationUs, TsUs};

/// Queue heuristic: our quote joins LAST, fills when this fraction of level qty traded (9/10 covers unknown cancellations).
const QUEUE_FILL_NUM: i128 = 9;
const QUEUE_FILL_DEN: i128 = 10;

const MATURED_WINDOW: DurationUs = DurationUs::from_secs(600);

/// Mid ring depth = deepest reverse lookback + 1s slack.
const MID_RING: DurationUs = DurationUs::from_secs(6);

const BACKING_MULTIPLE: usize = 2;

/// Headroom over fills in flight (at most one per side per placement).
const PENDING_SLACK: usize = 2;

/// Fill mature > this many spins past ideal = feed gap, not markout.
const STALE_TOLERANCE_SPINS: i64 = 2;

/// Which of OUR quotes a print reached (not Side: selling aggressor fills BID).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarkoutSide {
    Bid,
    Ask,
}

/// Pseudo-fill (for UI); price = armed level at receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarkoutFill {
    pub side: MarkoutSide,
    pub price: Price,
    pub fill_ts_us: TsUs,
}

impl MarkoutSide {
    #[inline]
    const fn from_aggressor(side: Side) -> Self {
        match side {
            Side::Sell => MarkoutSide::Bid,
            Side::Buy => MarkoutSide::Ask,
        }
    }

    /// Sign orientation: negative = toxic to us (bought bid hurt by mid falling, sold ask by mid running up).
    #[inline]
    const fn sign(self) -> f64 {
        match self {
            MarkoutSide::Bid => 1.0,
            MarkoutSide::Ask => -1.0,
        }
    }

    /// Exact mantissa comparison (float compare would fill on rounding dust).
    #[inline]
    const fn is_through(self, armed: Price, print: Price) -> bool {
        match self {
            MarkoutSide::Bid => print.0 <= armed.0,
            MarkoutSide::Ask => print.0 >= armed.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct MidSample {
    ts_us: TsUs,
    mid: f64,
}

impl crate::hot::series::sealed::Sealed for MidSample {}
impl Element for MidSample {}

#[derive(Debug, Clone, Copy, PartialEq)]
struct PendingFill {
    fill_ts_us: TsUs,
    fill_price: f64,
}

/// One horizon's in-flight fills + realised markouts.
#[derive(Debug, Clone, PartialEq)]
struct ForwardLane {
    horizon: ForwardHorizon,
    pending: VecDeque<PendingFill>,
    capacity: usize,
    matured: FastQueue<f64>,
}

impl ForwardLane {
    fn new(horizon: ForwardHorizon, matured_window: usize, spin_interval: DurationUs) -> Self {
        let capacity = spins_in(horizon.duration(), spin_interval) + PENDING_SLACK;
        Self {
            horizon,
            pending: VecDeque::with_capacity(capacity),
            capacity,
            matured: FastQueue::new(matured_window, BACKING_MULTIPLE),
        }
    }

    /// true once lane holds sized depth (further fills refused, no resize).
    #[inline]
    fn is_full(&self) -> bool {
        self.pending.len() == self.capacity
    }

    /// Realise fills ripened by sample, return stale count (dropped fills still pop to avoid wedge).
    fn mature(&mut self, sample: MidSample, tolerance: DurationUs, sign: f64) -> u64 {
        let horizon = self.horizon.duration();
        let mut stale = 0;
        while let Some(fill) = self.pending.front().copied() {
            let ideal = fill.fill_ts_us + horizon;
            if sample.ts_us < ideal {
                break;
            }
            self.pending.pop_front();
            if sample.ts_us.diff(ideal) > tolerance {
                stale += 1;
                continue;
            }
            let drift = (sample.mid - fill.fill_price) / fill.fill_price;
            self.matured.push(sign * drift * BPS);
        }
        stale
    }
}

/// One side's realised markouts, live quote, and gate counters (lifetime totals).
#[derive(Debug, Clone, PartialEq)]
pub struct SideMarkouts {
    side: MarkoutSide,
    armed: Option<Price>,
    /// Qty resting + eaten; fill gate @ 9/10 per arm.
    queue_ahead: Qty,
    eaten: Qty,
    forward_lanes: [ForwardLane; ForwardHorizon::ALL.len()],
    reverse_series: [FastQueue<f64>; ReverseHorizon::ALL.len()],
    fills: u64,
    reverse_gaps: u64,
    stale_maturations: u64,
    pending_overflows: u64,
}

impl SideMarkouts {
    fn new(side: MarkoutSide, matured_window: usize, spin_interval: DurationUs) -> Self {
        Self {
            side,
            armed: None,
            queue_ahead: Qty(0),
            eaten: Qty(0),
            forward_lanes: ForwardHorizon::ALL
                .map(|horizon| ForwardLane::new(horizon, matured_window, spin_interval)),
            reverse_series: ReverseHorizon::ALL
                .map(|_| FastQueue::new(matured_window, BACKING_MULTIPLE)),
            fills: 0,
            reverse_gaps: 0,
            stale_maturations: 0,
            pending_overflows: 0,
        }
    }

    /// Realised markouts (bps, oldest first); smooth with FastQueue::ema.
    #[inline]
    pub fn forward(&self, horizon: ForwardHorizon) -> &FastQueue<f64> {
        &self.forward_lanes[horizon.index()].matured
    }

    #[inline]
    pub fn reverse(&self, horizon: ReverseHorizon) -> &FastQueue<f64> {
        &self.reverse_series[horizon.index()]
    }

    /// Level a print must reach to fill (None once filled until next placement).
    #[inline]
    pub fn armed_quote(&self) -> Option<Price> {
        self.armed
    }

    #[inline]
    pub fn fill_count(&self) -> u64 {
        self.fills
    }

    /// Reverse samples dropped (mid ring didn't reach back whole horizon).
    #[inline]
    pub fn reverse_gap_count(&self) -> u64 {
        self.reverse_gaps
    }

    /// Forward samples dropped (maturing mid too late).
    #[inline]
    pub fn stale_maturation_count(&self) -> u64 {
        self.stale_maturations
    }

    /// Lane pushes refused (lane at max in-flight fills).
    #[inline]
    pub fn pending_overflow_count(&self) -> u64 {
        self.pending_overflows
    }

    /// Arm price behind level_qty; re-arm same = keep position.
    fn arm(&mut self, price: Price, level_qty: Qty) {
        if self.armed != Some(price) {
            self.queue_ahead = level_qty;
            self.eaten = Qty(0);
        }
        self.armed = Some(price);
    }

    /// Print AT armed level; true if past fill threshold.
    fn eat(&mut self, qty: Qty) -> bool {
        self.eaten = Qty(self.eaten.0.saturating_add(qty.0));
        i128::from(self.eaten.0) * QUEUE_FILL_DEN >= i128::from(self.queue_ahead.0) * QUEUE_FILL_NUM
    }

    /// Some(armed) if print filled us at our level, None else (side effects apply either way).
    fn on_trade(&mut self, trade: &TradeEvent, mids: &FastQueue<MidSample>) -> Option<Price> {
        let armed = self.armed?;
        if !self.side.is_through(armed, trade.price) {
            return None;
        }
        // Print strictly through swept all orders (including ours); only AT-level order queues.
        if trade.price == armed && !self.eat(trade.qty) {
            return None;
        }
        self.armed = None;
        self.fills += 1;
        self.record_reverse(trade.received_ts_us, mids);
        self.queue_forward(PendingFill {
            fill_ts_us: trade.received_ts_us,
            fill_price: armed.to_f64(),
        });
        Some(armed)
    }

    /// Pre-fill drift over each reverse horizon (vs newest mid at/before target).
    fn record_reverse(&mut self, fill_ts_us: TsUs, mids: &FastQueue<MidSample>) {
        let Some(latest) = mids.last() else {
            self.reverse_gaps += ReverseHorizon::ALL.len() as u64;
            return;
        };
        let sign = self.side.sign();
        for horizon in ReverseHorizon::ALL {
            let Some(earlier) = sample_at_or_before(mids, fill_ts_us - horizon.duration()) else {
                self.reverse_gaps += 1;
                continue;
            };
            let drift = (latest.mid - earlier.mid) / earlier.mid;
            self.reverse_series[horizon.index()].push(sign * drift * BPS);
        }
    }

    fn queue_forward(&mut self, fill: PendingFill) {
        let mut refused = 0;
        for lane in &mut self.forward_lanes {
            if lane.is_full() {
                refused += 1;
                continue;
            }
            lane.pending.push_back(fill);
        }
        if refused > 0 {
            self.count_pending_overflows(refused);
        }
    }

    fn mature(&mut self, sample: MidSample, tolerance: DurationUs) {
        let sign = self.side.sign();
        let mut stale = 0;
        for lane in &mut self.forward_lanes {
            stale += lane.mature(sample, tolerance, sign);
        }
        if stale > 0 {
            self.count_stale_maturations(stale);
        }
    }

    #[cold]
    fn count_pending_overflows(&mut self, refused: u64) {
        self.pending_overflows += refused;
    }

    #[cold]
    fn count_stale_maturations(&mut self, dropped: u64) {
        self.stale_maturations += dropped;
    }

    fn reset_continuity(&mut self) {
        self.armed = None;
        for lane in &mut self.forward_lanes {
            lane.pending.clear();
        }
    }

    fn clear_series(&mut self) {
        for lane in &mut self.forward_lanes {
            lane.matured.clear();
        }
        for series in &mut self.reverse_series {
            series.clear();
        }
    }
}

/// Pseudo-fill markouts: arm quotes each placement, feed prints/mids, read realised bps.
#[derive(Debug, Clone, PartialEq)]
pub struct MarkoutTracker {
    stale_tolerance: DurationUs,
    mids: FastQueue<MidSample>,
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

        let matured_window = spins_in(MATURED_WINDOW, spec.spin_interval);
        let mid_capacity = (MID_RING.to_secs() as usize * spec.max_mids_per_sec as usize).max(2);
        Self {
            stale_tolerance: DurationUs::from_micros(
                STALE_TOLERANCE_SPINS * spec.spin_interval.micros(),
            ),
            mids: FastQueue::new(mid_capacity, BACKING_MULTIPLE),
            bid: SideMarkouts::new(MarkoutSide::Bid, matured_window, spec.spin_interval),
            ask: SideMarkouts::new(MarkoutSide::Ask, matured_window, spec.spin_interval),
        }
    }

    /// Arms one side's quote, which fills once nine tenths of the queue shown at its level has
    /// traded. Re-arming at the same price keeps the place in the queue already earned.
    #[inline]
    pub fn arm(&mut self, side: MarkoutSide, price: Price, level_qty: Qty) {
        debug_assert!(
            price.0 > 0,
            "armed {side:?} quote is the forward-markout denominator, got {}",
            price.0
        );
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
    pub fn on_trade(&mut self, trade: &TradeEvent) -> Option<MarkoutFill> {
        let side = MarkoutSide::from_aggressor(trade.side);
        let markouts = match side {
            MarkoutSide::Bid => &mut self.bid,
            MarkoutSide::Ask => &mut self.ask,
        };
        let price = markouts.on_trade(trade, &self.mids)?;
        Some(MarkoutFill {
            side,
            price,
            fill_ts_us: trade.received_ts_us,
        })
    }

    /// Feed mid, mature fills (non-decreasing ts_us; skip invalid).
    pub fn on_mid(&mut self, ts_us: TsUs, mid: f64) {
        if !(mid.is_finite() && mid > 0.0) {
            return;
        }
        debug_assert!(
            self.mids.last().is_none_or(|last| ts_us >= last.ts_us),
            "mid samples must arrive in time order, got {}us",
            ts_us.micros()
        );
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
        self.reset_continuity();
        self.bid.clear_series();
        self.ask.clear_series();
    }
}

/// Newest sample at/before target (None if unreachable).
fn sample_at_or_before(mids: &FastQueue<MidSample>, target: TsUs) -> Option<MidSample> {
    let samples = mids.as_slice();
    let reaching = samples.partition_point(|sample| sample.ts_us <= target);
    reaching.checked_sub(1).map(|index| samples[index])
}

#[inline]
fn spins_in(span: DurationUs, spin_interval: DurationUs) -> usize {
    ((span.micros() / spin_interval.micros()) as usize).max(1)
}
