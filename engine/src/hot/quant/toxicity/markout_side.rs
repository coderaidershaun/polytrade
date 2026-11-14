//! One side of our quote in the markouts: which public prints fill it, and the drift of the mid
//! that each fill realises before and after.

use std::collections::VecDeque;

use super::{ForwardHorizon, ReverseHorizon};
use crate::hot::quant::BPS;
use crate::hot::series::{Element, FastQueue};
use crate::msg::ids::{Price, Qty, Side};
use crate::msg::inbound::TradeEvent;
use crate::msg::time::{DurationUs, LocalTs};

/// Queue heuristic: our quote joins LAST, fills when this fraction of level qty traded (9/10 covers unknown cancellations).
const QUEUE_FILL_NUM: i128 = 9;
const QUEUE_FILL_DEN: i128 = 10;

pub(super) const BACKING_MULTIPLE: usize = 2;

/// Headroom over fills in flight (at most one per side per placement).
const PENDING_SLACK: usize = 2;

/// Which of OUR quotes a print reached (not Side: selling aggressor fills BID).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarkoutSide {
    Bid,
    Ask,
}

impl MarkoutSide {
    #[inline]
    pub(super) const fn from_aggressor(side: Side) -> Self {
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
            MarkoutSide::Bid => print.mantissa() <= armed.mantissa(),
            MarkoutSide::Ask => print.mantissa() >= armed.mantissa(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct MidSample {
    pub(super) ts_us: LocalTs,
    pub(super) mid: f64,
}

impl crate::hot::series::sealed::Sealed for MidSample {}
impl Element for MidSample {}

#[derive(Debug, Clone, Copy, PartialEq)]
struct PendingFill {
    fill_ts_us: LocalTs,
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
    pub(super) armed: Option<Price>,
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
    pub(super) fn new(side: MarkoutSide, matured_window: usize, spin_interval: DurationUs) -> Self {
        Self {
            side,
            armed: None,
            queue_ahead: Qty::from_mantissa(0),
            eaten: Qty::from_mantissa(0),
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

    /// Realised markouts in bps, oldest first. Smooth them with `FastQueue::ema_over_window`.
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
    pub(super) fn arm(&mut self, price: Price, level_qty: Qty) {
        if self.armed != Some(price) {
            self.queue_ahead = level_qty;
            self.eaten = Qty::from_mantissa(0);
        }
        self.armed = Some(price);
    }

    /// Print AT armed level; true if past fill threshold.
    fn eat(&mut self, qty: Qty) -> bool {
        self.eaten = Qty::from_mantissa(self.eaten.mantissa().saturating_add(qty.mantissa()));
        i128::from(self.eaten.mantissa()) * QUEUE_FILL_DEN
            >= i128::from(self.queue_ahead.mantissa()) * QUEUE_FILL_NUM
    }

    /// Some(armed) if print filled us at our level, None else (side effects apply either way).
    pub(super) fn on_trade(
        &mut self,
        trade: &TradeEvent,
        received_ts_us: LocalTs,
        mids: &FastQueue<MidSample>,
    ) -> Option<Price> {
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
        self.record_reverse(received_ts_us, mids);
        self.queue_forward(PendingFill {
            fill_ts_us: received_ts_us,
            fill_price: armed.to_f64(),
        });
        Some(armed)
    }

    /// Pre-fill drift over each reverse horizon (vs newest mid at/before target).
    fn record_reverse(&mut self, fill_ts_us: LocalTs, mids: &FastQueue<MidSample>) {
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

    pub(super) fn mature(&mut self, sample: MidSample, tolerance: DurationUs) {
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

    pub(super) fn reset_continuity(&mut self) {
        self.armed = None;
        for lane in &mut self.forward_lanes {
            lane.pending.clear();
        }
    }

    pub(super) fn clear_series(&mut self) {
        for lane in &mut self.forward_lanes {
            lane.matured.clear();
        }
        for series in &mut self.reverse_series {
            series.clear();
        }
    }
}

/// Newest sample at/before target (None if unreachable).
fn sample_at_or_before(mids: &FastQueue<MidSample>, target: LocalTs) -> Option<MidSample> {
    let samples = mids.as_slice();
    let reaching = samples.partition_point(|sample| sample.ts_us <= target);
    reaching.checked_sub(1).map(|index| samples[index])
}

#[inline]
pub(super) fn spins_in(span: DurationUs, spin_interval: DurationUs) -> usize {
    ((span.micros() / spin_interval.micros()) as usize).max(1)
}
