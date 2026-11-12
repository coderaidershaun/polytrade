//! The venue's JSON shapes, in its own spelling. Amounts stay strings here and become mantissas
//! exactly once, in the decoders. Payload structs ignore fields they do not name, because Binance
//! adds fields over time; envelopes are strict, because a missing envelope field fails silently.

use serde::Deserialize;

use crate::adapters::binance::rest::OrderRecord;

/// The event must arrive wrapped, so the legacy bare form fails to decode rather than reading as
/// empty fields.
#[derive(Deserialize)]
pub(super) struct StreamEnvelope {
    pub event: StreamPayload,
}

#[derive(Deserialize)]
#[serde(tag = "e")]
pub(super) enum StreamPayload {
    /// Boxed because an execution report is wide, and every frame would otherwise pay for its size.
    #[serde(rename = "executionReport")]
    ExecutionReport(Box<ExecutionReport>),
    #[serde(rename = "outboundAccountPosition")]
    AccountPosition(AccountPosition),
    /// Carries a delta only, so one lost frame would leave the balance wrong forever.
    #[serde(rename = "balanceUpdate")]
    BalanceUpdate,
    #[serde(other)]
    Unhandled,
}

#[derive(Deserialize)]
pub(super) struct ExecutionReport {
    /// The transaction time. The event time is left out on purpose, because two stamps invite
    /// reading the wrong one.
    #[serde(rename = "T")]
    pub transact_ts_ms: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub client_order_id: String,
    /// The order a cancel or amend acted on, empty otherwise; `c` is then the request's own id.
    #[serde(rename = "C", default)]
    pub orig_client_order_id: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "q")]
    pub qty: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "x")]
    pub execution_type: String,
    #[serde(rename = "X")]
    pub order_status: String,
    #[serde(rename = "r")]
    pub reject_reason: String,
    #[serde(rename = "i")]
    pub order_id: i64,
    #[serde(rename = "l")]
    pub last_qty: String,
    #[serde(rename = "L")]
    pub last_price: String,
    #[serde(rename = "z")]
    pub cumulative_qty: String,
    #[serde(rename = "Z")]
    pub cumulative_quote: String,
    #[serde(rename = "n")]
    pub commission: String,
    /// Null when there is no fee, unlike the other absent fields, which arrive as empty strings.
    #[serde(rename = "N")]
    pub commission_asset: Option<String>,
    #[serde(rename = "t")]
    pub trade_id: i64,
    #[serde(rename = "m")]
    pub is_maker: bool,
}

#[derive(Deserialize)]
pub(super) struct AccountPosition {
    #[serde(rename = "E")]
    pub event_ts_ms: i64,
    #[serde(rename = "u")]
    pub last_update_ms: i64,
    #[serde(rename = "B")]
    pub balances: Vec<WireBalance>,
}

#[derive(Deserialize)]
pub(super) struct WireBalance {
    #[serde(rename = "a")]
    pub asset: String,
    #[serde(rename = "f")]
    pub free: String,
    #[serde(rename = "l")]
    pub locked: String,
}

/// A WebSocket API answer carries either a result or an error, and the result's shape depends on
/// the method.
#[derive(Deserialize)]
pub(super) struct ResponseEnvelope {
    pub result: Option<serde_json::Value>,
    pub error: Option<ResponseError>,
}

#[derive(Deserialize)]
pub(super) struct ResponseError {
    pub code: i32,
    #[serde(default)]
    pub msg: String,
}

#[derive(Deserialize)]
pub(super) struct AmendResult {
    #[serde(rename = "amendedOrder")]
    pub amended_order: OrderRecord,
}
