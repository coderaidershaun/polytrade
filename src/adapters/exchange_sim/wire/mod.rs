//! Binance-compatible payloads for simulated venue events.

mod decode;
mod templates;

use super::core::orders::SimOrder;
use super::core::schedule::Rejection;
use super::core::wallet::FillSettlement;
use crate::adapters::binance::exec::format_client_order_id;
use crate::adapters::exec::EngineIdentity;
use crate::ids::{FIXED_SCALE, Qty, TradeId};
use crate::msg::exec::VenueOrderStatus;
use crate::time::TsUs;

pub use decode::{response_messages, stream_messages};

const NO_VENUE_ORDER_ID: i64 = -1;

const VENUE_MINTED_CANCEL_ID: &str = "4zR9HFcEq8gM1tWUqPEUHc";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SimBalance<'a> {
    pub asset: &'a str,
    pub free: Qty,
    pub locked: Qty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SimFill<'a> {
    pub trade_id: TradeId,
    pub settlement: &'a FillSettlement,
    pub fee_asset: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub struct VenueWire {
    identity: EngineIdentity,
}

impl VenueWire {
    pub fn new(identity: EngineIdentity) -> Self {
        Self { identity }
    }

    pub fn would_match_error(&self) -> String {
        templates::ERROR_WOULD_MATCH.to_owned()
    }

    pub fn unknown_order_error(&self) -> String {
        templates::ERROR_UNKNOWN_ORDER.to_owned()
    }

    pub(crate) fn refusal(&self, rejection: Rejection) -> String {
        match rejection {
            Rejection::WouldMatchImmediately => self.would_match_error(),
            Rejection::InsufficientBalance => templates::ERROR_INSUFFICIENT_BALANCE.to_owned(),
            Rejection::FilterFailure | Rejection::TooManyOrders => {
                templates::ERROR_FILTER_FAILURE.to_owned()
            }
            Rejection::CancelRejected => self.unknown_order_error(),
            Rejection::NoSuchOrder => templates::ERROR_NO_SUCH_ORDER.to_owned(),
            Rejection::AmendBudgetSpent => templates::ERROR_AMEND_BUDGET_SPENT.to_owned(),
            Rejection::AmendQuantityIncrease => templates::ERROR_AMEND_QUANTITY_INCREASE.to_owned(),
            Rejection::AmendFilterFailure => templates::ERROR_AMEND_FILTER_FAILURE.to_owned(),
        }
    }

    pub fn place_ack(&self, order: &SimOrder, event_ts_us: TsUs) -> String {
        self.order_ack(
            templates::PLACE_ACK,
            order,
            VenueOrderStatus::New,
            event_ts_us,
            |_| {},
        )
    }

    pub fn cancel_ack(&self, order: &SimOrder, event_ts_us: TsUs) -> String {
        let subject = self.client_order_id(order);
        self.order_ack(
            templates::CANCEL_ACK,
            order,
            VenueOrderStatus::Canceled,
            event_ts_us,
            |result| {
                result["clientOrderId"] = VENUE_MINTED_CANCEL_ID.into();
                result["origClientOrderId"] = subject.into();
            },
        )
    }

    pub fn amend_ack(&self, order: &SimOrder, event_ts_us: TsUs) -> String {
        let mut document = parse(templates::AMEND_ACK);
        stamp_response(&mut document, event_ts_us);
        let amended = &mut document["result"]["amendedOrder"];
        amended["transactTime"] = event_ts_us.micros().div_euclid(1_000).into();
        amended["clientOrderId"] = self.client_order_id(order).into();
        amended["origClientOrderId"] = self.client_order_id(order).into();
        amended["orderId"] = order.venue_order_id.0.into();
        amended["side"] = order.side.wire_name().into();
        amended["price"] = decimal(order.price.0).into();
        amended["qty"] = decimal(order.qty.0).into();
        amended["executedQty"] = decimal(order.filled.0).into();
        document.to_string()
    }

    pub fn order_status_as(
        &self,
        order: &SimOrder,
        status: VenueOrderStatus,
        event_ts_us: TsUs,
    ) -> String {
        self.order_ack(templates::ORDER_STATUS, order, status, event_ts_us, |_| {})
    }

    pub fn open_orders(&self, resting: &[SimOrder], event_ts_us: TsUs) -> String {
        if resting.is_empty() {
            return templates::OPEN_ORDERS_EMPTY.to_owned();
        }
        let mut document = parse(templates::OPEN_ORDERS);
        let template = document["result"][0].clone();
        let rows: Vec<serde_json::Value> = resting
            .iter()
            .map(|order| {
                let mut row = template.clone();
                row["clientOrderId"] = self.client_order_id(order).into();
                row["orderId"] = order.venue_order_id.0.into();
                row["price"] = decimal(order.price.0).into();
                row["origQty"] = decimal(order.qty.0).into();
                row["executedQty"] = decimal(order.filled.0).into();
                row["cummulativeQuoteQty"] = decimal(order.filled_quote).into();
                row["side"] = order.side.wire_name().into();
                row["status"] = match order.filled.0 > 0 {
                    true => "PARTIALLY_FILLED",
                    false => "NEW",
                }
                .into();
                stamp_order(&mut row, event_ts_us);
                row
            })
            .collect();
        document["result"] = rows.into();
        document.to_string()
    }

    pub fn new_report(&self, order: &SimOrder, event_ts_us: TsUs) -> String {
        self.report(templates::REPORT_NEW, order, event_ts_us, |_| {})
    }

    /// # Panics
    /// If the settlement's cumulative totals disagree with the order it names.
    pub fn trade_report(&self, order: &SimOrder, fill: SimFill<'_>, event_ts_us: TsUs) -> String {
        let settlement = fill.settlement;
        assert_eq!(
            (settlement.cumulative_qty, settlement.cumulative_quote),
            (order.filled, order.filled_quote),
            "the venue's book and its wallet disagree about what order {} has filled",
            order.client_id.0
        );
        let template = match order.is_complete() {
            true => templates::REPORT_TRADE_FILLED,
            false => templates::REPORT_TRADE_PARTIAL,
        };
        self.report(template, order, event_ts_us, |event| {
            event["l"] = decimal(settlement.last_qty.0).into();
            event["L"] = decimal(order.price.0).into();
            event["Y"] = decimal(settlement.last_quote).into();
            event["t"] = fill.trade_id.0.into();
            event["n"] = decimal(settlement.fee).into();
            event["N"] = match settlement.fee == 0 {
                true => serde_json::Value::Null,
                false => fill.fee_asset.into(),
            };
            event["X"] = match order.is_complete() {
                true => "FILLED",
                false => "PARTIALLY_FILLED",
            }
            .into();
        })
    }

    pub fn cancel_report(&self, order: &SimOrder, event_ts_us: TsUs) -> String {
        let subject = self.client_order_id(order);
        self.report(templates::REPORT_CANCELED, order, event_ts_us, |event| {
            event["c"] = VENUE_MINTED_CANCEL_ID.into();
            event["C"] = subject.into();
        })
    }

    pub fn rejected_report(&self, order: &SimOrder, event_ts_us: TsUs) -> String {
        self.report(
            templates::REPORT_REJECTED_CROSS,
            order,
            event_ts_us,
            |event| {
                event["i"] = NO_VENUE_ORDER_ID.into();
            },
        )
    }

    /// # Panics
    /// If `balances` is empty.
    pub fn account_position(
        &self,
        balances: &[SimBalance<'_>],
        event_ts_us: TsUs,
        update_ts_ms: u64,
    ) -> String {
        assert!(
            !balances.is_empty(),
            "an account update names the assets that moved; an empty one empties the hot table"
        );
        let mut document = parse(templates::ACCOUNT_POSITION);
        let event = &mut document["event"];
        event["E"] = (event_ts_us.micros() / 1_000).into();
        event["u"] = update_ts_ms.into();
        let template = event["B"][0].clone();
        let rows: Vec<serde_json::Value> = balances
            .iter()
            .map(|balance| {
                let mut row = template.clone();
                row["a"] = balance.asset.into();
                row["f"] = decimal(balance.free.0).into();
                row["l"] = decimal(balance.locked.0).into();
                row
            })
            .collect();
        event["B"] = rows.into();
        document.to_string()
    }

    fn client_order_id(&self, order: &SimOrder) -> String {
        format_client_order_id(self.identity.te_tag, order.client_id)
    }

    fn report(
        &self,
        template: &str,
        order: &SimOrder,
        event_ts_us: TsUs,
        extra: impl FnOnce(&mut serde_json::Value),
    ) -> String {
        let mut document = parse(template);
        let event = &mut document["event"];
        stamp_stream_event(event, event_ts_us);
        event["c"] = self.client_order_id(order).into();
        event["i"] = order.venue_order_id.0.into();
        event["S"] = order.side.wire_name().into();
        event["p"] = decimal(order.price.0).into();
        event["q"] = decimal(order.qty.0).into();
        event["z"] = decimal(order.filled.0).into();
        event["Z"] = decimal(order.filled_quote).into();
        extra(event);
        document.to_string()
    }

    fn order_ack(
        &self,
        template: &str,
        order: &SimOrder,
        status: VenueOrderStatus,
        event_ts_us: TsUs,
        extra: impl FnOnce(&mut serde_json::Value),
    ) -> String {
        let mut document = parse(template);
        stamp_response(&mut document, event_ts_us);
        let result = &mut document["result"];
        result["clientOrderId"] = self.client_order_id(order).into();
        result["orderId"] = order.venue_order_id.0.into();
        result["status"] = venue_status_text(status).into();
        result["side"] = order.side.wire_name().into();
        result["price"] = decimal(order.price.0).into();
        result["origQty"] = decimal(order.qty.0).into();
        result["executedQty"] = decimal(order.filled.0).into();
        result["cummulativeQuoteQty"] = decimal(order.filled_quote).into();
        extra(result);
        document.to_string()
    }
}

/// Fractional digits are the fixed-point scale's own width: a hand-written one silently misplaces
/// every decimal point the day the scale moves, and the venue decoder accepts the result.
const DECIMAL_DIGITS: usize = FIXED_SCALE.ilog10() as usize;

pub fn decimal(mantissa: i64) -> String {
    let scale = FIXED_SCALE as u64;
    let magnitude = mantissa.unsigned_abs();
    let sign = match mantissa < 0 {
        true => "-",
        false => "",
    };
    format!(
        "{sign}{}.{:0width$}",
        magnitude / scale,
        magnitude % scale,
        width = DECIMAL_DIGITS
    )
}

fn venue_status_text(status: VenueOrderStatus) -> &'static str {
    match status {
        VenueOrderStatus::New => "NEW",
        VenueOrderStatus::PartiallyFilled => "PARTIALLY_FILLED",
        VenueOrderStatus::Filled => "FILLED",
        VenueOrderStatus::Canceled => "CANCELED",
        VenueOrderStatus::PendingCancel => "PENDING_CANCEL",
        VenueOrderStatus::Rejected => "REJECTED",
        VenueOrderStatus::Expired => "EXPIRED",
        VenueOrderStatus::ExpiredInMatch => "EXPIRED_IN_MATCH",
    }
}

fn stamp_stream_event(event: &mut serde_json::Value, event_ts_us: TsUs) {
    let event_ts_ms = event_ts_us.micros().div_euclid(1_000);
    event["E"] = event_ts_ms.into();
    event["T"] = event_ts_ms.into();
}

fn stamp_response(document: &mut serde_json::Value, event_ts_us: TsUs) {
    let Some(result) = document.get_mut("result") else {
        return;
    };
    stamp_order(result, event_ts_us);
}

fn stamp_order(order: &mut serde_json::Value, event_ts_us: TsUs) {
    let event_ts_ms = event_ts_us.micros().div_euclid(1_000);
    for field in ["transactTime", "updateTime"] {
        if order.get(field).is_some() {
            order[field] = event_ts_ms.into();
        }
    }
}

fn parse(template: &str) -> serde_json::Value {
    serde_json::from_str(template).expect("a committed template parses")
}
