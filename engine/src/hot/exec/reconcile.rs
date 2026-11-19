//! The quote reconciler: one pure function from "what the strategy wants" and "what is actually
//! resting" to the single primitive that closes the gap.
//!
//! No `&mut self`, no clock, no ring, no allocation: every input is a parameter, so a test drives
//! it with no engine at all. That is the point: this is where the money decisions live, and a
//! function testable only through a running engine is a function tested less.
//!
//! The order of the guard clauses carries meaning, not style: the cross guard comes before the
//! funds gate, because a quote that would take liquidity must be refused whether or not we could
//! have afforded it.
//!
//! The single-flight rule is NOT here. This function decides one level, and whether the side
//! already waits for an answer is a fact of the whole side, so the engine holds it, in its ladder
//! pass and in its flatten pass. A second copy here would be a second place to get
//! `max_orders_per_side` wrong.

use crate::hot::digest::digest_by_hash;
use crate::msg::exec::OrderStyle;
use crate::msg::ids::{Notional, Price, Qty, Side, Spend};
use crate::msg::time::{DurationUs, LocalTs};

pub use crate::msg::order::RejectReason;

use super::desired::DesiredQuote;

digest_by_hash!(TickGrid, RejectReason);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TickGrid {
    pub tick: i64,
    pub step: i64,
    pub min_qty: Qty,
    pub min_notional: Notional,
    pub max_amends: u8,
    /// Venue price ceiling where one is published. Only an aggressive price can reach it, so the
    /// passive reconciler never consults it and the flatten planner always does.
    pub max_price: Option<Price>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BookTop {
    pub best_bid: Option<Price>,
    pub best_ask: Option<Price>,
    pub mid: Price,
    pub is_valid: bool,
    pub last_commit_ts_us: LocalTs,
    pub now_ts_us: LocalTs,
}

impl BookTop {
    #[inline]
    pub fn is_quotable(&self, max_age: DurationUs) -> bool {
        self.is_valid
            && self.best_bid.is_some()
            && self.best_ask.is_some()
            && self.now_ts_us.diff(self.last_commit_ts_us) <= max_age
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExecLimits {
    pub requote_threshold_ticks: u32,
    pub max_quote_distance_centi_bps: i64,
    pub max_book_age: DurationUs,
    pub max_order_notional_quote: Notional,
}

impl ExecLimits {
    pub fn disabled() -> Self {
        Self {
            requote_threshold_ticks: 0,
            max_quote_distance_centi_bps: 0,
            max_book_age: DurationUs::ZERO,
            max_order_notional_quote: Notional::ZERO,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FundsView {
    pub spendable: i64,
    pub floor: i64,
}

impl FundsView {
    #[inline]
    fn can_spend(&self, amount: Spend) -> bool {
        self.spendable >= self.floor + amount.mantissa()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RestingOrder {
    pub price: Price,
    pub qty: Qty,
    pub filled: Qty,
    pub amends_used: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReconcileInput {
    pub side: Side,
    pub desired: Option<DesiredQuote>,
    pub resting: Option<RestingOrder>,
    pub grid: TickGrid,
    pub top: BookTop,
    pub limits: ExecLimits,
    pub funds: FundsView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlaceIntent {
    pub price: Price,
    pub qty: Qty,
    pub style: OrderStyle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReconcileOutcome {
    Nothing,
    Place(PlaceIntent),
    Cancel,
    AmendQty(Qty),
    Reject(RejectReason),
}

/// The one decision function; see the module header for why the guard order is not stylistic.
///
/// # Panics
/// When the tick or the step of the grid is not positive.
pub fn reconcile_side(input: ReconcileInput) -> ReconcileOutcome {
    let ReconcileInput {
        side,
        desired,
        resting,
        grid,
        top,
        limits,
        funds,
    } = input;
    assert!(
        grid.tick > 0 && grid.step > 0,
        "grid increments must be positive, got tick {} step {}",
        grid.tick,
        grid.step
    );

    if !top.is_quotable(limits.max_book_age) {
        return withdraw(resting);
    }
    let Some(desired) = desired else {
        return withdraw(resting);
    };

    let price = side.snap_passive(desired.price, grid.tick);
    let qty = desired.qty.floor_to_step(grid.step);

    if qty < grid.min_qty {
        return ReconcileOutcome::Reject(RejectReason::QtyBelowMin);
    }
    let notional = price.notional(qty);
    if notional < grid.min_notional {
        return ReconcileOutcome::Reject(RejectReason::NotionalBelowMin);
    }
    if notional > limits.max_order_notional_quote {
        return ReconcileOutcome::Reject(RejectReason::NotionalAboveMax);
    }
    if would_cross(side, price, &top) {
        return ReconcileOutcome::Reject(RejectReason::WouldCross);
    }
    if is_outside_band(price, top.mid, limits.max_quote_distance_centi_bps) {
        return ReconcileOutcome::Reject(RejectReason::OutsideBand);
    }

    let intent = PlaceIntent {
        price,
        qty,
        style: OrderStyle::PostOnly,
    };
    let Some(resting) = resting else {
        if !funds.can_spend(side.spend(price, qty)) {
            return ReconcileOutcome::Reject(RejectReason::Underfunded);
        }
        return ReconcileOutcome::Place(intent);
    };

    let delta_ticks =
        price.mantissa().abs_diff(resting.price.mantissa()) / grid.tick.unsigned_abs();
    if delta_ticks > u64::from(limits.requote_threshold_ticks) {
        return ReconcileOutcome::Cancel;
    }

    if qty <= resting.filled {
        return ReconcileOutcome::Cancel;
    }
    if qty == resting.qty {
        return ReconcileOutcome::Nothing;
    }
    if qty > resting.qty {
        return ReconcileOutcome::Cancel;
    }
    if resting.amends_used >= grid.max_amends {
        return ReconcileOutcome::Cancel;
    }
    ReconcileOutcome::AmendQty(qty)
}

#[inline]
fn withdraw(resting: Option<RestingOrder>) -> ReconcileOutcome {
    match resting {
        Some(_) => ReconcileOutcome::Cancel,
        None => ReconcileOutcome::Nothing,
    }
}

#[inline]
fn would_cross(side: Side, price: Price, top: &BookTop) -> bool {
    match side {
        Side::Buy => top.best_ask.is_some_and(|ask| price >= ask),
        Side::Sell => top.best_bid.is_some_and(|bid| price <= bid),
    }
}

#[inline]
fn is_outside_band(price: Price, mid: Price, max_centi_bps: i64) -> bool {
    let distance = i128::from(price.mantissa().abs_diff(mid.mantissa()));
    distance * 1_000_000 > i128::from(mid.mantissa()) * i128::from(max_centi_bps)
}
