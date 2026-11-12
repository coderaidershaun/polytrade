//! REST request plans: path, params, weight, method, auth per venue call.

use crate::config::BinanceMarket;

use super::weight::{depth_weight, exchange_info_weight, klines_weight};
use super::{KlineQuery, RestRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpMethod {
    Get,
    Delete,
}

/// Auth level; `Signed` only reachable via [`SignedRestClient`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RequestAuth {
    Public,
    Signed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestPlan {
    pub path: String,
    pub query: Vec<(&'static str, String)>,
    pub weight: u32,
    pub endpoint: &'static str,
    pub method: HttpMethod,
    pub auth: RequestAuth,
}

impl RestRequest {
    /// Charged before call, corrected by server header — see [`super::weight::WeightBudget`].
    pub fn plan(&self, market: BinanceMarket) -> RequestPlan {
        let prefix = api_prefix(market);
        match self {
            RestRequest::DepthSnapshot { symbol, limit } => RequestPlan {
                path: format!("{prefix}/depth"),
                query: vec![
                    ("symbol", symbol.to_uppercase()),
                    ("limit", limit.to_string()),
                ],
                weight: depth_weight(market, *limit),
                endpoint: "depth",
                method: HttpMethod::Get,
                auth: RequestAuth::Public,
            },
            RestRequest::Klines(query) => RequestPlan {
                path: format!("{prefix}/klines"),
                query: kline_query(query),
                weight: klines_weight(market, query.limit),
                endpoint: "klines",
                method: HttpMethod::Get,
                auth: RequestAuth::Public,
            },
            RestRequest::ExchangeInfo { symbol } => RequestPlan {
                path: format!("{prefix}/exchangeInfo"),
                query: exchange_info_query(market, symbol),
                weight: exchange_info_weight(market),
                endpoint: "exchangeInfo",
                method: HttpMethod::Get,
                auth: RequestAuth::Public,
            },
            RestRequest::ServerTime => RequestPlan {
                path: format!("{prefix}/time"),
                query: Vec::new(),
                weight: 1,
                endpoint: "time",
                method: HttpMethod::Get,
                auth: RequestAuth::Public,
            },
            RestRequest::AccountInfo => RequestPlan {
                path: format!("{prefix}/account"),
                query: Vec::new(),
                weight: 20,
                endpoint: "account",
                method: HttpMethod::Get,
                auth: RequestAuth::Signed,
            },
            RestRequest::MyTrades {
                symbol,
                from_id,
                limit,
            } => RequestPlan {
                path: format!("{prefix}/myTrades"),
                query: my_trades_query(symbol, *from_id, *limit),
                // This request never sends an orderId, so it costs 20; with one it would cost 5.
                weight: 20,
                endpoint: "myTrades",
                method: HttpMethod::Get,
                auth: RequestAuth::Signed,
            },
            RestRequest::OpenOrders { symbol } => RequestPlan {
                path: format!("{prefix}/openOrders"),
                query: vec![("symbol", symbol.to_uppercase())],
                // The symbol is always sent, so this costs 6; without it the venue charges 80.
                weight: 6,
                endpoint: "openOrders",
                method: HttpMethod::Get,
                auth: RequestAuth::Signed,
            },
            RestRequest::OrderStatus {
                symbol,
                orig_client_order_id,
            } => RequestPlan {
                path: format!("{prefix}/order"),
                query: vec![
                    ("symbol", symbol.to_uppercase()),
                    ("origClientOrderId", orig_client_order_id.clone()),
                ],
                weight: 4,
                endpoint: "order",
                method: HttpMethod::Get,
                auth: RequestAuth::Signed,
            },
            RestRequest::OrderStatusByVenueId {
                symbol,
                venue_order_id,
            } => RequestPlan {
                path: format!("{prefix}/order"),
                query: vec![
                    ("symbol", symbol.to_uppercase()),
                    ("orderId", venue_order_id.to_string()),
                ],
                weight: 4,
                endpoint: "order",
                method: HttpMethod::Get,
                auth: RequestAuth::Signed,
            },
            RestRequest::CancelOrder {
                symbol,
                orig_client_order_id,
            } => RequestPlan {
                path: format!("{prefix}/order"),
                query: vec![
                    ("symbol", symbol.to_uppercase()),
                    ("origClientOrderId", orig_client_order_id.clone()),
                ],
                weight: 1,
                endpoint: "cancelOrder",
                method: HttpMethod::Delete,
                auth: RequestAuth::Signed,
            },
        }
    }
}

fn kline_query(query: &KlineQuery) -> Vec<(&'static str, String)> {
    let mut params = vec![
        ("symbol", query.symbol.to_uppercase()),
        ("interval", query.interval.as_str().to_owned()),
        ("limit", query.limit.to_string()),
    ];
    if let Some(start) = query.start_ts_ms {
        params.push(("startTime", start.to_string()));
    }
    if let Some(end) = query.end_ts_ms {
        params.push(("endTime", end.to_string()));
    }
    params
}

fn my_trades_query(symbol: &str, from_id: Option<i64>, limit: u32) -> Vec<(&'static str, String)> {
    let mut params = vec![
        ("symbol", symbol.to_uppercase()),
        ("limit", limit.to_string()),
    ];
    if let Some(from_id) = from_id {
        params.push(("fromId", from_id.to_string()));
    }
    params
}

fn api_prefix(market: BinanceMarket) -> &'static str {
    match market {
        BinanceMarket::Spot => "/api/v3",
        BinanceMarket::Perpetual => "/fapi/v1",
    }
}

fn exchange_info_query(market: BinanceMarket, symbol: &str) -> Vec<(&'static str, String)> {
    // only spot narrows the payload by symbol; futures ignores the param and returns all
    match market {
        BinanceMarket::Spot => vec![("symbol", symbol.to_uppercase())],
        BinanceMarket::Perpetual => Vec::new(),
    }
}
