//! Admission and execution of simulated venue requests.

use super::market::MarketFold;
use super::ordering::Phase;
use super::orders::SimOrder;
use super::queue::{QueueAhead, SimOrderIndex};
use super::resting::{
    ClosedReason, InstrumentLimits, OrderPhase, OrderSnapshot, RefusalReason, RestingOrder,
    RestingOrders,
};
use super::wallet::{ReservationRequest, ReserveOutcome, SimWallet};
use super::{EmissionBook, VenueEvent};
use crate::adapters::exec::ExecRequest;
use crate::ids::{ClientOrderId, Price, Qty, Side};
use crate::msg::exec::OrderStyle;
use crate::time::TsUs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimedAction {
    Activate(SimOrderIndex),
    Cancel(SimOrderIndex),
    Amend {
        index: SimOrderIndex,
        total_qty: Qty,
    },
    AnswerOrderStatus(ClientOrderId),
    AnswerOpenOrders,
}

impl TimedAction {
    pub fn phase(self) -> Phase {
        match self {
            TimedAction::Activate(_) | TimedAction::Cancel(_) | TimedAction::Amend { .. } => {
                Phase::OrderAction
            }
            TimedAction::AnswerOrderStatus(_) | TimedAction::AnswerOpenOrders => Phase::Reconcile,
        }
    }
}

pub struct RequestFold<'a> {
    pub orders: &'a mut RestingOrders,
    pub market: &'a mut MarketFold,
    pub emissions: &'a mut EmissionBook,
    pub limits: &'a InstrumentLimits,
    pub wallet: &'a mut SimWallet,
}

impl RequestFold<'_> {
    pub fn admit(&mut self, request: &ExecRequest, effective_ts_us: TsUs) -> AdmitPlan {
        match *request {
            ExecRequest::Place {
                client_id,
                side,
                price,
                qty,
                style,
                ..
            } => self.admit_place(Placement {
                client_id,
                side,
                price,
                qty,
                style,
                effective_ts_us,
            }),
            ExecRequest::Cancel { client_id, .. } => self.admit_cancel(client_id, effective_ts_us),
            ExecRequest::AmendQty { client_id, qty, .. } => {
                self.admit_amend(client_id, qty, effective_ts_us)
            }
            ExecRequest::OrderStatus { client_id, .. } => {
                Some((effective_ts_us, TimedAction::AnswerOrderStatus(client_id)))
            }
            ExecRequest::OpenOrders { .. } => {
                Some((effective_ts_us, TimedAction::AnswerOpenOrders))
            }
            ExecRequest::SubscribeUserStream => {
                self.emissions
                    .push(effective_ts_us, VenueEvent::StreamSubscribed);
                None
            }
        }
    }

    pub fn run(&mut self, action: TimedAction, at_ts_us: TsUs) {
        match action {
            TimedAction::Activate(index) => self.activate(index, at_ts_us),
            TimedAction::Cancel(index) => self.cancel(index, at_ts_us),
            TimedAction::Amend { index, total_qty } => self.amend(index, total_qty, at_ts_us),
            TimedAction::AnswerOrderStatus(client_id) => self.answer_status(client_id, at_ts_us),
            TimedAction::AnswerOpenOrders => self.answer_open_orders(at_ts_us),
        }
    }

    pub fn crosses(&self, side: Side, price: Price) -> bool {
        let book = self.market.book();
        let is_public_cross = match side {
            Side::Buy => book.best_ask().is_some_and(|ask| ask.price.0 <= price.0),
            Side::Sell => book.best_bid().is_some_and(|bid| bid.price.0 >= price.0),
        };
        is_public_cross
            || self
                .market
                .ladder(side.opposite())
                .holds_liquidity_crossing(price)
    }

    fn admit_place(&mut self, placement: Placement) -> AdmitPlan {
        let venue_order_id = self.orders.mint_venue_order_id();
        let index = self.orders.admit(RestingOrder::pending(
            SimOrder {
                client_id: placement.client_id,
                venue_order_id,
                side: placement.side,
                price: placement.price,
                qty: placement.qty,
                filled: Qty(0),
                filled_quote: 0,
            },
            placement.effective_ts_us,
        ));
        if let Some(reason) = self.refuse_placement(&placement) {
            self.refuse(index, reason, placement.effective_ts_us);
            return None;
        }
        Some((placement.effective_ts_us, TimedAction::Activate(index)))
    }

    fn refuse(&mut self, index: SimOrderIndex, reason: RefusalReason, at_ts_us: TsUs) {
        self.close(index, ClosedReason::Refused(reason), at_ts_us);
        let snapshot = self.snapshot(index);
        self.emissions
            .push(at_ts_us, VenueEvent::PlaceRefused { snapshot, reason });
    }

    fn refuse_placement(&self, placement: &Placement) -> Option<RefusalReason> {
        if placement.style != OrderStyle::PostOnly {
            return Some(RefusalReason::StyleNotPermitted);
        }
        self.limits.refuse(placement.price, placement.qty)
    }

    fn activate(&mut self, index: SimOrderIndex, at_ts_us: TsUs) {
        let Some(record) = self.orders.get(index) else {
            return;
        };
        if record.phase != OrderPhase::Pending {
            return;
        }
        let (side, price, qty) = (record.order.side, record.order.price, record.order.qty);
        // The order stays Pending and its timeline entry is spent: a venue that cannot see its own
        // book cannot judge a post-only cross. `SimVenue::restore_matching` reschedules every
        // Pending order once the book is whole again, and a run that ends first withdraws them as
        // never-sent through the forced sweep, so the wait is bounded either way.
        if !self.market.is_matching_live() {
            return;
        }
        if self.crosses(side, price) {
            self.close(index, ClosedReason::CrossedPostOnly, at_ts_us);
            let snapshot = self.snapshot(index);
            self.emissions
                .push(at_ts_us, VenueEvent::PostOnlyCrossed { snapshot });
            return;
        }
        if self.orders.count_resting_on(side) >= usize::from(self.limits.max_orders_per_side) {
            self.refuse(index, RefusalReason::MaxOrders, at_ts_us);
            return;
        }
        match self.wallet.reserve(ReservationRequest { side, price, qty }) {
            ReserveOutcome::Reserved(reservation) => {
                if let Some(record) = self.orders.get_mut(index) {
                    record.reservation = Some(reservation);
                }
            }
            ReserveOutcome::InsufficientFunds => {
                self.refuse(index, RefusalReason::InsufficientFunds, at_ts_us);
                return;
            }
        }

        let seeded = self.market.public_at(side, price);
        let queue = self.market.ladder_mut(side).entry(price);
        match seeded {
            QueueAhead::Unobservable => queue.push_public(QueueAhead::Unobservable),
            QueueAhead::Known(visible) => queue.reconcile_known_public_to(visible),
        }
        queue.push_own(index);
        if let Some(record) = self.orders.get_mut(index) {
            record.phase = OrderPhase::Resting;
            record.joined_ts_us = at_ts_us;
        }
        let snapshot = self.snapshot(index);
        let queue_ahead = self
            .market
            .ladder(side)
            .queue(price)
            .and_then(|queue| queue.public_ahead_of(index))
            .unwrap_or(QueueAhead::Unobservable);
        self.emissions.push(
            at_ts_us,
            VenueEvent::Rested {
                snapshot,
                queue_ahead,
            },
        );
    }

    fn answer_status(&mut self, client_id: ClientOrderId, at_ts_us: TsUs) {
        let event = match self.orders.find(client_id) {
            Some(index) => VenueEvent::OrderStatus {
                snapshot: self.snapshot(index),
            },
            None => VenueEvent::NoSuchOrder { client_id },
        };
        self.emissions.push(at_ts_us, event);
    }

    fn answer_open_orders(&mut self, at_ts_us: TsUs) {
        let rows: Vec<OrderSnapshot> = self
            .orders
            .open_indices()
            .map(|index| self.snapshot(index))
            .collect();
        self.emissions
            .push(at_ts_us, VenueEvent::OpenOrders { rows });
    }

    pub(super) fn close(&mut self, index: SimOrderIndex, reason: ClosedReason, at_ts_us: TsUs) {
        let Some((side, price)) = close_order(self.orders, self.wallet, index, reason, at_ts_us)
        else {
            return;
        };
        let ladder = self.market.ladder_mut(side);
        if let Some(queue) = ladder.queue_mut(price) {
            queue.remove_own(index);
        }
        ladder.drop_vacant();
    }

    pub(super) fn snapshot(&self, index: SimOrderIndex) -> OrderSnapshot {
        self.orders
            .snapshot(index)
            .unwrap_or_else(|| panic!("simulated order snapshot missing for slot {}", index.0))
    }
}

pub(super) fn close_order(
    orders: &mut RestingOrders,
    wallet: &mut SimWallet,
    index: SimOrderIndex,
    reason: ClosedReason,
    at_ts_us: TsUs,
) -> Option<(Side, Price)> {
    let record = orders.get_mut(index)?;
    if !record.is_open() {
        return None;
    }
    record.phase = OrderPhase::Closed(reason);
    record.closed_ts_us = Some(at_ts_us);
    if let Some(reservation) = record.reservation.as_mut() {
        wallet.release(reservation);
    }
    Some((record.order.side, record.order.price))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Placement {
    client_id: ClientOrderId,
    side: Side,
    price: Price,
    qty: Qty,
    style: OrderStyle,
    effective_ts_us: TsUs,
}

pub type AdmitPlan = Option<(TsUs, TimedAction)>;
