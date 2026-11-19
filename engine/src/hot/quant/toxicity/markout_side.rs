//! One side of our quote in the markouts: which public prints fill it, and the drift of the mid
//! that each fill realises before and after.

use std::collections::VecDeque;

use crate::hot::quant::BPS;
use crate::hot::series::{Element, FastQueue};
use crate::msg::ids::{Price, Qty, Side};
use crate::msg::inbound::TradeEvent;
use crate::msg::time::{DurationUs, LocalTs};

/// Our quote joins the back of its level and fills once this share of the queue ahead has traded.
/// Nine tenths, not all of it, because some of the queue leaves by cancels that no print shows.
const QUEUE_FILL_NUM: i128 = 9;
const QUEUE_FILL_DEN: i128 = 10;

pub(super) const BACKING_MULTIPLE: usize = 2;

/// Room over the fills in flight that the spins allow, at most one for each placement.
const PENDING_SLACK: usize = 2;

/// The horizons after a fill at which its forward markout is realised.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ForwardHorizon {
    Secs1,
    Secs3,
    Secs5,
    Secs10,
    Secs30,
    Secs60,
}

impl ForwardHorizon {
    pub const ALL: [ForwardHorizon; 6] = [
        ForwardHorizon::Secs1,
        ForwardHorizon::Secs3,
        ForwardHorizon::Secs5,
        ForwardHorizon::Secs10,
        ForwardHorizon::Secs30,
        ForwardHorizon::Secs60,
    ];

    pub const fn duration(self) -> DurationUs {
        DurationUs::from_secs(match self {
            ForwardHorizon::Secs1 => 1,
            ForwardHorizon::Secs3 => 3,
            ForwardHorizon::Secs5 => 5,
            ForwardHorizon::Secs10 => 10,
            ForwardHorizon::Secs30 => 30,
            ForwardHorizon::Secs60 => 60,
        })
    }

    #[inline]
    const fn index(self) -> usize {
        self as usize
    }
}

// The lanes are built in the order of `ALL` and read back by the discriminant of a horizon, so a
// reordered `ALL` would read one horizon from the lane of another. The build fails on it instead.
const _: () = {
    let mut index = 0;
    while index < ForwardHorizon::ALL.len() {
        assert!(
            ForwardHorizon::ALL[index].index() == index,
            "ForwardHorizon::ALL must list each horizon at the index of its discriminant"
        );
        index += 1;
    }
};

/// The horizons before a fill over which its reverse markout reads back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReverseHorizon {
    Secs1,
    Secs5,
}

impl ReverseHorizon {
    pub const ALL: [ReverseHorizon; 2] = [ReverseHorizon::Secs1, ReverseHorizon::Secs5];

    pub const fn duration(self) -> DurationUs {
        DurationUs::from_secs(match self {
            ReverseHorizon::Secs1 => 1,
            ReverseHorizon::Secs5 => 5,
        })
    }

    #[inline]
    const fn index(self) -> usize {
        self as usize
    }
}

// The series are read back by the discriminant of a horizon, as the forward lanes are.
const _: () = {
    let mut index = 0;
    while index < ReverseHorizon::ALL.len() {
        assert!(
            ReverseHorizon::ALL[index].index() == index,
            "ReverseHorizon::ALL must list each horizon at the index of its discriminant"
        );
        index += 1;
    }
};

/// Which of our quotes a print reached. It is not the side of the print: a selling aggressor fills
/// our bid.
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

    /// A negative markout is toxic to us: a bought bid loses when the mid falls, and a sold ask
    /// loses when it rises.
    #[inline]
    const fn sign(self) -> f64 {
        match self {
            MarkoutSide::Bid => 1.0,
            MarkoutSide::Ask => -1.0,
        }
    }

    /// Compares the exact mantissas, because a float comparison would fill on rounding noise.
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

/// The fills in flight and the realised markouts of one horizon.
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

    /// The lane is full at its sized depth. It refuses a further fill and never grows.
    #[inline]
    fn is_full(&self) -> bool {
        self.pending.len() == self.capacity
    }

    /// Realises the fills that the sample ripens and returns how many came too late. A late fill
    /// still leaves the lane, or one feed gap would hold back each fill behind it.
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

/// The realised markouts of one side, its live quote, and the counters of its drops over the
/// lifetime of the tracker.
#[derive(Debug, Clone, PartialEq)]
pub struct SideMarkouts {
    side: MarkoutSide,
    pub(super) armed: Option<Price>,
    /// The quantity resting ahead of our quote at its arm. The quote fills once nine tenths of it
    /// has traded at the level.
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

    /// Realised markouts in basis points, oldest first. Smooth them with
    /// `FastQueue::ema_over_window`.
    #[inline]
    pub fn forward(&self, horizon: ForwardHorizon) -> &FastQueue<f64> {
        &self.forward_lanes[horizon.index()].matured
    }

    #[inline]
    pub fn reverse(&self, horizon: ReverseHorizon) -> &FastQueue<f64> {
        &self.reverse_series[horizon.index()]
    }

    /// The level that a print must reach to fill. `None` after a fill, until the next placement.
    #[inline]
    pub fn armed_quote(&self) -> Option<Price> {
        self.armed
    }

    #[inline]
    pub fn fill_count(&self) -> u64 {
        self.fills
    }

    /// Reverse markouts dropped because the mid ring did not reach back over the whole horizon.
    #[inline]
    pub fn reverse_gap_count(&self) -> u64 {
        self.reverse_gaps
    }

    /// Forward markouts dropped because the first ripe mid came too late.
    #[inline]
    pub fn stale_maturation_count(&self) -> u64 {
        self.stale_maturations
    }

    /// Fills that a full lane refused.
    #[inline]
    pub fn pending_overflow_count(&self) -> u64 {
        self.pending_overflows
    }

    /// Arms at a price behind the quantity shown there. A re-arm at the same price keeps the place
    /// already earned in the queue.
    pub(super) fn arm(&mut self, price: Price, level_qty: Qty) {
        if self.armed != Some(price) {
            self.queue_ahead = level_qty;
            self.eaten = Qty::from_mantissa(0);
        }
        self.armed = Some(price);
    }

    /// Takes a print at the armed level from the queue ahead. True once the quote fills.
    fn eat(&mut self, qty: Qty) -> bool {
        self.eaten = Qty::from_mantissa(self.eaten.mantissa().saturating_add(qty.mantissa()));
        i128::from(self.eaten.mantissa()) * QUEUE_FILL_DEN
            >= i128::from(self.queue_ahead.mantissa()) * QUEUE_FILL_NUM
    }

    /// Returns the armed level when the print filled us there, else `None`. A print at the level
    /// eats the queue ahead either way.
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
        // A print through our level swept each order there, ours too. Only a print at the level
        // waits for the queue ahead.
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

    /// The drift of the mid into the fill over each reverse horizon, against the newest mid at or
    /// before the start of that horizon.
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

/// The newest sample at or before the target, not the nearest one, which could sit after it and
/// shorten the lookback. `None` when the ring does not reach back that far.
fn sample_at_or_before(mids: &FastQueue<MidSample>, target: LocalTs) -> Option<MidSample> {
    let samples = mids.as_slice();
    let reaching = samples.partition_point(|sample| sample.ts_us <= target);
    reaching.checked_sub(1).map(|index| samples[index])
}

#[inline]
pub(super) fn spins_in(span: DurationUs, spin_interval: DurationUs) -> usize {
    ((span.micros() / spin_interval.micros()) as usize).max(1)
}
