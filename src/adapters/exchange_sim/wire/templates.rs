//! Committed Binance fixture templates used by the simulator.

pub(super) const PLACE_ACK: &str =
    include_str!("../../../../fixtures/binance/exec/ack_order_place.json");
pub(super) const CANCEL_ACK: &str =
    include_str!("../../../../fixtures/binance/exec/ack_order_cancel.json");
pub(super) const AMEND_ACK: &str =
    include_str!("../../../../fixtures/binance/exec/ack_order_amend_keep_priority.json");
pub(super) const ORDER_STATUS: &str =
    include_str!("../../../../fixtures/binance/exec/ack_order_status_filled.json");
pub(super) const OPEN_ORDERS: &str =
    include_str!("../../../../fixtures/binance/exec/ack_open_orders.json");
pub(super) const OPEN_ORDERS_EMPTY: &str =
    include_str!("../../../../fixtures/binance/exec/ack_open_orders_empty.json");

pub(super) const ERROR_WOULD_MATCH: &str =
    include_str!("../../../../fixtures/binance/exec/error_2010_would_match_immediately.json");
pub(super) const ERROR_UNKNOWN_ORDER: &str =
    include_str!("../../../../fixtures/binance/exec/error_2011_cancel_rejected.json");
pub(super) const ERROR_NO_SUCH_ORDER: &str =
    include_str!("../../../../fixtures/binance/exec/error_2013_no_such_order.json");
pub(super) const ERROR_FILTER_FAILURE: &str =
    include_str!("../../../../fixtures/binance/exec/error_1013_filter_failure.json");
pub(super) const ERROR_INSUFFICIENT_BALANCE: &str =
    include_str!("../../../../fixtures/binance/exec/error_2010_insufficient_balance.json");
pub(super) const ERROR_AMEND_FILTER_FAILURE: &str =
    include_str!("../../../../fixtures/binance/exec/error_1013_amend_filter_failure.json");
pub(super) const ERROR_AMEND_BUDGET_SPENT: &str =
    include_str!("../../../../fixtures/binance/exec/error_2038_amend_budget_spent.json");
pub(super) const ERROR_AMEND_QUANTITY_INCREASE: &str =
    include_str!("../../../../fixtures/binance/exec/error_2038_amend_quantity_increase.json");

pub(super) const REPORT_NEW: &str =
    include_str!("../../../../fixtures/binance/exec/report_new.json");
pub(super) const REPORT_TRADE_PARTIAL: &str =
    include_str!("../../../../fixtures/binance/exec/report_trade_partially_filled.json");
pub(super) const REPORT_TRADE_FILLED: &str =
    include_str!("../../../../fixtures/binance/exec/report_trade_filled.json");
pub(super) const REPORT_CANCELED: &str =
    include_str!("../../../../fixtures/binance/exec/report_canceled.json");
pub(super) const REPORT_REJECTED_CROSS: &str =
    include_str!("../../../../fixtures/binance/exec/report_rejected_would_match_immediately.json");

pub(super) const ACCOUNT_POSITION: &str =
    include_str!("../../../../fixtures/binance/exec/account_position.json");
