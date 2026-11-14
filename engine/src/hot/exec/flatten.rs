//! The flatten planner: it turns a position into one marketable order that closes it, as a pure
//! function of position and market state, or gives the reason that it refuses.
//!
//! Separate from the quote reconciler because it answers a different question. The reconciler asks
//! what the book should look like; flatten asks how to stop holding a position. Both are pure:
//! a test can run them without an engine, and a strategy uses this path only when trying to exit.
//!
//! Idempotent by design: the strategy re-declares intent every spin, the plan sizes to current
//! position, and partial fills produce a smaller order next spin. Nothing remembers what was sent.
//! The engine's own pass sits at the end of this file, beside the planner, so the two decide alike.

use crate::log::hot_warn;
use crate::msg::exec::OrderStyle;
use crate::msg::ids::{FIXED_SCALE, InstrumentId, Notional, Price, Qty, Side};

use super::engine::ExecEngine;
use super::gates::reducing_side;
use super::mint::PlacementKind;
use super::reconcile::{BookTop, ExecLimits, FundsView, PlaceIntent, RejectReason, TickGrid};
use super::spin::SpinInput;

/// How the venue charges the side that takes liquidity. Chosen once at bring-up from the venue's
/// own physics, so the engine never has to know which venue it is trading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeeModel {
    /// Nothing is charged on top of what the order spends. A venue that takes its cut out of what
    /// the trade RECEIVES belongs here: the buyer's budget is the notional and no more.
    None,
    BinaryOutcome,
}

impl FeeModel {
    /// What a taker pays for `qty` at `price`, on top of the notional, as a quote-side mantissa.
    ///
    /// [`FeeModel::BinaryOutcome`] reads price as a probability p and charges
    /// `shares × rate × p × (1 − p)` — symmetric about even money, zero at the bounds, and zero
    /// outside them rather than negative. Because it is charged on top, a buy must reserve it
    /// separately from the notional.
    #[inline]
    pub fn taker_fee_quote(self, price: Price, qty: Qty, rate: i64) -> Notional {
        match self {
            FeeModel::None => Notional::ZERO,
            FeeModel::BinaryOutcome => binary_outcome_fee(price, qty, rate),
        }
    }
}

#[inline]
fn binary_outcome_fee(price: Price, qty: Qty, rate: i64) -> Notional {
    let complement = i128::from(FIXED_SCALE - price.mantissa());
    if rate == 0 || complement <= 0 || price.mantissa() <= 0 {
        return Notional::ZERO;
    }
    let scale = i128::from(FIXED_SCALE);
    let fee =
        i128::from(qty.mantissa()) * i128::from(rate) * i128::from(price.mantissa()) * complement
            / (scale * scale * scale);
    Notional::from_mantissa(i64::try_from(fee).unwrap_or(i64::MAX))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlattenInput {
    // A signed quantity: positive means long, closed with a sell; negative means short,
    // closed with a buy.
    pub position_base: Qty,
    pub grid: TickGrid,
    pub top: BookTop,
    pub limits: ExecLimits,
    // Pre-filtered to the closing side's available budget, the same convention the quote
    // reconciler uses.
    pub funds: FundsView,
    // Slack beyond the far touch, in ticks.
    pub slack_ticks: u32,
    pub fee_model: FeeModel,
    // Taker fee as a 1e-8 mantissa, charged by whichever curve fee_model names.
    pub taker_fee_rate: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FlattenOutcome {
    Nothing,
    Place(PlaceIntent),
    Refuse(RejectReason),
}

/// Plans a single order to close the position, or explains why it cannot.
///
/// `Nothing` means flat (goal reached). `Refuse` means the engine still holds a position it could not
/// shed (residue below venue minimum). On a resolving market, an unflattened position realizes its
/// full value at resolution, so the distinction matters.
///
/// # Panics
/// When the tick or the step of the grid is not positive.
pub fn plan_flatten(input: FlattenInput) -> FlattenOutcome {
    let FlattenInput {
        position_base,
        grid,
        top,
        limits,
        funds,
        slack_ticks,
        fee_model,
        taker_fee_rate,
    } = input;
    assert!(
        grid.tick > 0 && grid.step > 0,
        "grid increments must be positive, got tick {} step {}",
        grid.tick,
        grid.step
    );

    let side = match position_base.mantissa().signum() {
        1 => Side::Sell,
        -1 => Side::Buy,
        _ => return FlattenOutcome::Nothing,
    };
    if !top.is_quotable(limits.max_book_age) {
        return FlattenOutcome::Refuse(RejectReason::BookNotQuotable);
    }
    let Some(price) = marketable_price(side, &top, grid, slack_ticks) else {
        return FlattenOutcome::Refuse(RejectReason::BookNotQuotable);
    };

    let held = Qty::from_mantissa(position_base.mantissa().abs());
    let affordable = affordable_qty(side, price, funds, fee_model, taker_fee_rate);
    let within_notional = notional_capped_qty(price, limits.max_order_notional_quote);
    let wanted = held.min(affordable).min(within_notional);
    let qty = wanted.floor_to_step(grid.step);

    if qty < grid.min_qty || qty.mantissa() == 0 {
        // Distinguish the failure mode for operators: insufficient balance vs venue size too coarse.
        let reason = if affordable < held.min(within_notional) {
            RejectReason::Underfunded
        } else {
            RejectReason::QtyBelowMin
        };
        return FlattenOutcome::Refuse(reason);
    }
    FlattenOutcome::Place(PlaceIntent {
        price,
        qty,
        // SMELL: the simulator refuses every `Immediate` order, so a flatten placed here never
        // fills in a simulated run. The prints that the simulator drops are not counted either.
        style: OrderStyle::Immediate,
    })
}

/// Prices through the far touch by configured slack, clamped to venue bounds.
/// On a probability market, slack at the extremes can skip past acceptable prices, so clamping matters.
#[inline]
fn marketable_price(side: Side, top: &BookTop, grid: TickGrid, slack_ticks: u32) -> Option<Price> {
    let slack = grid.tick * i64::from(slack_ticks);
    let (touch, through) = match side {
        Side::Buy => (top.best_ask?, slack),
        Side::Sell => (top.best_bid?, -slack),
    };
    // Snap in the aggressive direction (opposite the passive resting direction) so off-grid touches
    // cannot round back to a resting price instead of trading.
    let aggressive = side
        .opposite()
        .snap_passive(Price::from_mantissa(touch.mantissa() + through), grid.tick);
    let ceiling = grid
        .max_price
        .map_or(aggressive.mantissa(), |max| max.mantissa());
    Some(Price::from_mantissa(
        aggressive.mantissa().min(ceiling).max(grid.tick),
    ))
}

/// Maximum quantity affordable under the side's budget, including taker fees where charged.
/// Buying, the cost per share is the price plus the fee. Selling, the fee is deducted from
/// proceeds instead, with no additional reserve needed.
#[inline]
fn affordable_qty(
    side: Side,
    price: Price,
    funds: FundsView,
    fee_model: FeeModel,
    taker_fee_rate: i64,
) -> Qty {
    let budget = funds.spendable - funds.floor;
    if budget <= 0 {
        return Qty::from_mantissa(0);
    }
    match side {
        Side::Sell => Qty::from_mantissa(budget),
        Side::Buy => {
            let unit_cost = i128::from(price.mantissa())
                + i128::from(
                    fee_model
                        .taker_fee_quote(price, Qty::from_mantissa(FIXED_SCALE), taker_fee_rate)
                        .mantissa(),
                );
            if unit_cost <= 0 {
                return Qty::from_mantissa(0);
            }
            let affordable = i128::from(budget) * i128::from(FIXED_SCALE) / unit_cost;
            Qty::from_mantissa(i64::try_from(affordable).unwrap_or(i64::MAX))
        }
    }
}

#[inline]
fn notional_capped_qty(price: Price, max_notional_quote: Notional) -> Qty {
    if price.mantissa() <= 0 {
        return Qty::from_mantissa(0);
    }
    let capped = i128::from(max_notional_quote.mantissa()) * i128::from(FIXED_SCALE)
        / i128::from(price.mantissa());
    Qty::from_mantissa(i64::try_from(capped).unwrap_or(i64::MAX))
}

impl ExecEngine {
    /// Sends one marketable order per instrument per spin while a strategy wants out and holding.
    /// Deliberately NOT window-gated — the close margin is when positions most need closing — and
    /// deliberately not budget-gated either, for the reason `admit_quote_placement` gives. Still
    /// gated on every money protection (halt, readiness, single-flight, per-side order count,
    /// funds, notional), so it bypasses only what would keep a position open.
    pub(super) fn flatten_pass(&mut self, venue: usize, input: &mut SpinInput<'_>) {
        if self.halt.is_halted() || self.venues[venue].readiness_gap().is_some() {
            return;
        }
        for instrument in self.venues[venue].instrument_ids() {
            let index = usize::from(instrument.0);
            if !input.desired.is_flattening(instrument, input.tick.seq) {
                self.instruments[index].flatten_refusal = None;
                continue;
            }
            let position = input.ledger.row(instrument).position_base();
            let Some(side) = reducing_side(position) else {
                self.instruments[index].flatten_refusal = None;
                continue;
            };
            if self.orders.is_awaiting_answer(instrument, side)
                || self.orders.possibly_live_count(instrument, side)
                    >= self.venues[venue].settings.max_orders_per_side
            {
                continue;
            }
            let outcome = plan_flatten(FlattenInput {
                position_base: position,
                grid: self.instruments[index].grid,
                top: self.book_top(instrument, input),
                limits: self.venues[venue].settings.limits,
                funds: self.funds_view(instrument, side),
                slack_ticks: self.venues[venue].settings.flatten_slack_ticks,
                fee_model: self.venues[venue].settings.fee_model,
                taker_fee_rate: self.venues[venue].settings.taker_fee_rate,
            });
            self.act_on_flatten(instrument, side, outcome, input);
        }
    }

    fn act_on_flatten(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        outcome: FlattenOutcome,
        input: &mut SpinInput<'_>,
    ) {
        let refusal = match outcome {
            FlattenOutcome::Nothing => None,
            FlattenOutcome::Place(intent) => self.place(
                instrument,
                side,
                intent,
                PlacementKind::Flatten,
                input.stamps(),
                input.bank,
            ),
            FlattenOutcome::Refuse(reason) => Some(reason),
        };
        let index = usize::from(instrument.0);
        let Some(reason) = refusal else {
            self.instruments[index].flatten_refusal = None;
            return;
        };
        if self.instruments[index].flatten_refusal.replace(reason) != Some(reason) {
            report_unflattened(
                instrument,
                input.ledger.row(instrument).position_base(),
                reason,
            );
        }
    }
}

#[cold]
fn report_unflattened(instrument: InstrumentId, position: Qty, reason: RejectReason) {
    hot_warn!(
        "instrument {} still holds {} base and cannot be flattened: {:?}",
        instrument.0,
        position.mantissa(),
        reason
    );
}
