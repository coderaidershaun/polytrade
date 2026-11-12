//! Binance REST response shapes, public and private. Decimals stay as the venue's strings until a
//! decoder turns them into mantissas.

use serde::Deserialize;

/// The part of `exchangeInfo` the startup scale check reads.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ExchangeInfo {
    #[serde(rename = "rateLimits", default)]
    pub rate_limits: Vec<RateLimit>,
    #[serde(default)]
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RateLimit {
    #[serde(rename = "rateLimitType")]
    pub rate_limit_type: Box<str>,
    pub interval: Box<str>,
    #[serde(rename = "intervalNum")]
    pub interval_num: u32,
    pub limit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SymbolInfo {
    pub symbol: Box<str>,
    #[serde(default)]
    pub filters: Vec<SymbolFilter>,
}

/// Every filter shape the engine reads, in one struct, because spot and futures name the same
/// limits differently. Match on `filter_type` first: several filters spell a `limit`, and
/// `MARKET_LOT_SIZE` mirrors `LOT_SIZE`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SymbolFilter {
    #[serde(rename = "filterType")]
    pub filter_type: Box<str>,
    #[serde(rename = "tickSize")]
    pub tick_size: Option<Box<str>>,
    #[serde(rename = "stepSize")]
    pub step_size: Option<Box<str>>,
    #[serde(rename = "minPrice")]
    pub min_price: Option<Box<str>>,
    #[serde(rename = "maxPrice")]
    pub max_price: Option<Box<str>>,
    #[serde(rename = "minQty")]
    pub min_qty: Option<Box<str>>,
    #[serde(rename = "maxQty")]
    pub max_qty: Option<Box<str>>,
    #[serde(rename = "minNotional")]
    pub min_notional: Option<Box<str>>,
    pub notional: Option<Box<str>>,
    #[serde(rename = "maxNumOrders")]
    pub max_num_orders: Option<u32>,
    pub limit: Option<u32>,
    #[serde(rename = "maxNumOrderAmends")]
    pub max_num_order_amends: Option<u32>,
}

/// `GET /api/v3/account`, holding only the fields the engine acts on out of the twenty or so more
/// the venue returns. Unknown fields are allowed so a venue addition cannot break startup.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AccountInfo {
    #[serde(rename = "canTrade")]
    pub can_trade: bool,
    #[serde(rename = "accountType")]
    pub account_type: Box<str>,
    /// The account's trading groups. They are logged, never used as a gate.
    #[serde(default)]
    pub permissions: Vec<Box<str>>,
    #[serde(rename = "commissionRates")]
    pub commission_rates: CommissionRates,
    #[serde(default)]
    pub balances: Vec<Balance>,
    #[serde(rename = "updateTime", default)]
    pub update_time_ms: i64,
}

impl AccountInfo {
    /// The balances worth showing, because a live account holds dozens of zero dust entries.
    pub fn funded_balances(&self) -> impl Iterator<Item = &Balance> {
        self.balances.iter().filter(|balance| balance.is_funded())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CommissionRates {
    pub maker: Box<str>,
    pub taker: Box<str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Balance {
    pub asset: Box<str>,
    pub free: Box<str>,
    pub locked: Box<str>,
}

impl Balance {
    /// Tests the text instead of parsing it: the venue pads to 8 decimals, so any non-zero digit
    /// means a balance, and no dust amount has to be converted.
    pub fn is_funded(&self) -> bool {
        let has_value = |amount: &str| amount.chars().any(|digit| ('1'..='9').contains(&digit));
        has_value(&self.free) || has_value(&self.locked)
    }
}

/// A fill as `GET /api/v3/myTrades` reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AccountTrade {
    pub symbol: Box<str>,
    pub id: i64,
    #[serde(rename = "orderId")]
    pub order_id: i64,
    pub price: Box<str>,
    pub qty: Box<str>,
    pub commission: Box<str>,
    #[serde(rename = "commissionAsset")]
    pub commission_asset: Box<str>,
    #[serde(rename = "time")]
    pub time_ms: i64,
    #[serde(rename = "isMaker")]
    pub is_maker: bool,
}

/// An order as the venue reports it. REST reads and every WebSocket order answer share this shape;
/// only the amend answer spells two of its fields differently, hence the aliases.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct OrderRecord {
    pub symbol: Box<str>,
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "clientOrderId", default)]
    pub client_order_id: Box<str>,
    #[serde(rename = "origClientOrderId")]
    pub orig_client_order_id: Option<Box<str>>,
    pub price: Box<str>,
    #[serde(rename = "origQty", alias = "qty")]
    pub orig_qty: Box<str>,
    #[serde(rename = "executedQty")]
    pub executed_qty: Box<str>,
    #[serde(rename = "cummulativeQuoteQty", alias = "cumulativeQuoteQty", default)]
    pub cumulative_quote_qty: Box<str>,
    pub status: Box<str>,
    #[serde(default)]
    pub side: Box<str>,
    #[serde(rename = "time")]
    pub time_ms: Option<i64>,
    #[serde(rename = "updateTime")]
    pub update_time_ms: Option<i64>,
    #[serde(rename = "transactTime")]
    pub transact_time_ms: Option<i64>,
}
