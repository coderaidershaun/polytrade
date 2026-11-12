//! The venue's JSON shapes, in its own spelling. Request bodies are serialised once, because the
//! HMAC covers the exact bytes that reach the socket.
//!
//! REST and the stream spell the same order differently: REST sends `created_at` as integer
//! seconds, the stream sends its stamp as a string of milliseconds. One shared struct would fail at
//! run time, not at compile time.

use serde::{Deserialize, Serialize};

/// Fields in typehash order. `salt` is a JSON number while every other numeric field is a string,
/// and `expiration` appears here but not in the signed struct.
#[derive(Serialize)]
pub(super) struct SignedOrderBody<'a> {
    pub salt: u64,
    pub maker: &'a str,
    pub signer: &'a str,
    #[serde(rename = "tokenId")]
    pub token_id: &'a str,
    #[serde(rename = "makerAmount")]
    pub maker_amount: String,
    #[serde(rename = "takerAmount")]
    pub taker_amount: String,
    pub side: &'static str,
    pub expiration: &'static str,
    pub timestamp: String,
    #[serde(rename = "signatureType")]
    pub signature_type: u8,
    pub signature: &'a str,
    pub metadata: &'static str,
    pub builder: &'static str,
}

#[derive(Serialize)]
pub(super) struct PlaceOrderBody<'a> {
    pub order: SignedOrderBody<'a>,
    pub owner: &'a str,
    #[serde(rename = "orderType")]
    pub order_type: &'static str,
    #[serde(rename = "deferExec")]
    pub defer_exec: bool,
    #[serde(rename = "postOnly")]
    pub post_only: bool,
}

#[derive(Serialize)]
pub(super) struct CancelOrderBody<'a> {
    #[serde(rename = "orderID")]
    pub order_id: &'a str,
}

#[derive(Serialize)]
pub(super) struct CancelMarketOrdersBody<'a> {
    pub asset_id: &'a str,
}

#[derive(Serialize)]
pub(super) struct HeartbeatBody<'a> {
    pub heartbeat_id: &'a str,
}

/// A success and a stale-id 400 both carry `heartbeat_id`; in the 400 it is the id the venue
/// expects, so one shape reads both.
#[derive(Deserialize)]
pub(super) struct HeartbeatResponse {
    #[serde(default)]
    pub heartbeat_id: String,
}

/// The raw secret travels on this wire only, with no HMAC. The server closes the socket if it goes
/// silent.
#[derive(Serialize)]
pub(super) struct SubscribeBody<'a> {
    pub auth: SubscribeAuth<'a>,
    #[serde(rename = "type")]
    pub channel: &'static str,
}

#[derive(Serialize)]
pub(super) struct SubscribeAuth<'a> {
    #[serde(rename = "apiKey")]
    pub api_key: &'a str,
    pub secret: &'a str,
    pub passphrase: &'a str,
}

/// The answer to `POST /order`. A failure can arrive with HTTP 200, and the amounts are empty
/// strings when nothing matched.
#[derive(Deserialize)]
pub(super) struct PlaceResponse {
    #[serde(default)]
    pub success: bool,
    #[serde(rename = "errorMsg", default)]
    pub error_msg: String,
    #[serde(rename = "orderID", default)]
    pub order_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(rename = "makingAmount", default)]
    pub making_amount: String,
    #[serde(rename = "takingAmount", default)]
    pub taking_amount: String,
}

/// Every cancel path answers in this shape, and partial success is by design.
#[derive(Deserialize)]
pub(super) struct CancelResponse {
    #[serde(default)]
    pub canceled: Vec<String>,
    #[serde(default)]
    pub not_canceled: std::collections::BTreeMap<String, String>,
}

/// The venue sends no numeric error codes; `code`, when present, is text.
#[derive(Deserialize)]
pub(super) struct ErrorResponse {
    #[serde(default)]
    pub error: String,
    #[serde(rename = "error_msg", default)]
    pub error_msg: String,
    #[serde(default)]
    pub code: String,
    pub retry_after_seconds: Option<i64>,
}

impl ErrorResponse {
    pub fn message(&self) -> &str {
        match self.error.is_empty() {
            true => &self.error_msg,
            false => &self.error,
        }
    }
}

/// Response is wrapped in a data field. The prose docs show a bare array, but that is stale.
#[derive(Deserialize)]
pub(super) struct Page<T> {
    #[serde(default)]
    pub next_cursor: String,
    // A bare `default` would make serde demand `Default` of every row type.
    #[serde(default = "Vec::new")]
    pub data: Vec<T>,
}

/// created_at is an integer here. The stream sends it as a string.
#[derive(Deserialize)]
pub(super) struct OrderRecord {
    pub id: String,
    pub asset_id: String,
    pub side: String,
    pub price: String,
    pub original_size: String,
    #[serde(default)]
    pub size_matched: String,
    pub status: String,
    #[serde(default)]
    pub created_at: i64,
}

/// The wire carries the taker's side, so this struct omits it. Reading it would flip attribution
/// on every trade we made. Use trader_side to tell which role we played.
#[derive(Deserialize)]
pub(super) struct TradeRecord {
    pub id: String,
    pub asset_id: String,
    #[serde(default)]
    pub taker_order_id: String,
    pub price: String,
    pub size: String,
    pub status: String,
    #[serde(default)]
    pub trader_side: String,
    #[serde(default)]
    pub fee_rate_bps: String,
    #[serde(default)]
    pub maker_orders: Vec<MakerOrderFill>,
    #[serde(default)]
    pub match_time: String,
}

#[derive(Deserialize)]
pub(super) struct MakerOrderFill {
    pub order_id: String,
    #[serde(default)]
    pub owner: String,
    pub matched_amount: String,
    pub price: String,
    #[serde(default)]
    pub fee_rate_bps: String,
}

/// `event_type` separates order frames from trade frames; `type` then separates the kinds of order
/// event.
#[derive(Deserialize)]
#[serde(tag = "event_type")]
pub(super) enum StreamFrame {
    #[serde(rename = "order")]
    Order(Box<OrderEvent>),
    #[serde(rename = "trade")]
    Trade(Box<TradeRecord>),
    #[serde(other)]
    Unhandled,
}

/// The stream sends `timestamp` as a string of milliseconds, and `created_at` as a string too.
#[derive(Deserialize)]
pub(super) struct OrderEvent {
    pub id: String,
    pub asset_id: String,
    pub side: String,
    pub price: String,
    pub original_size: String,
    #[serde(default)]
    pub size_matched: String,
    #[serde(rename = "type")]
    pub event: String,
    pub status: String,
    #[serde(default)]
    pub timestamp: String,
}

/// Balance is a 6-decimal integer. Allowance is keyed by exchange address and read-only; we check
/// it to verify the CLOB cache is warm.
#[derive(Deserialize)]
pub(super) struct BalanceAllowance {
    #[serde(default)]
    pub balance: String,
}

/// A public endpoint, called once per rotation binding. `mos` and `mts` arrive as JSON numbers and
/// are read as exact numbers, not floats, because a tick is a price-grid key.
#[derive(Deserialize)]
pub(super) struct ClobMarketResponse {
    #[serde(rename = "c")]
    pub condition_id: String,
    #[serde(rename = "t", default)]
    pub tokens: Vec<ClobTokenResponse>,
    #[serde(rename = "mos")]
    pub min_order_size: serde_json::Number,
    #[serde(rename = "mts")]
    pub min_tick_size: serde_json::Number,
    #[serde(rename = "mbf", default)]
    pub maker_fee_bps: i32,
    #[serde(rename = "tbf", default)]
    pub taker_fee_bps: i32,
    #[serde(rename = "ao", default)]
    pub is_accepting_orders: bool,
    /// Omitted when the market has no taker delay.
    #[serde(rename = "itode", default)]
    pub is_taker_order_delay_enabled: bool,
}

#[derive(Deserialize)]
pub(super) struct ClobTokenResponse {
    #[serde(rename = "t")]
    pub token_id: String,
    #[serde(rename = "o", default)]
    pub outcome: String,
}

/// A separate call, because this flag picks the contract an order is signed for.
#[derive(Deserialize)]
pub(super) struct NegRiskResponse {
    #[serde(default)]
    pub neg_risk: bool,
}

/// The venue answers either this object or a bare number, so a failure here is not yet a refusal.
#[derive(Deserialize)]
pub(super) struct VersionResponse {
    pub version: u32,
}

/// closed_only is required here with no serde default. An absent field must not deserialise to
/// false, because the gate treats None as a refusal and will not arm on an absent field.
#[derive(Deserialize)]
pub(super) struct ClosedOnlyResponse {
    pub closed_only: bool,
}
