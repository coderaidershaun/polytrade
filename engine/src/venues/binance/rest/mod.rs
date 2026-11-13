//! The Binance REST client: public snapshots and backfills, each returned raw so that a recorder
//! can save it byte for byte, and a rolling weight counter that warns before the limit of a minute.
//!
//! That counter is per CLIENT. The venue bills per IP, and the signed half builds a client of its
//! own, so the two halves of one deployment each behave as though they held the whole budget: the
//! warning fires later than the real spend deserves, and a 429 one of them earns does not quiet the
//! other.

mod payload;
mod request;
mod signed;
mod weight;

use serde::de::DeserializeOwned;

use crate::msg::time::VenueTs;
use crate::runtime::config::{BinanceMarket, KlineInterval};
use crate::runtime::secrets::Secret;
use crate::venues::edge::http::{self, Deadlines, HttpClient, TransportError};

use weight::{OrderCountBudget, WeightBudget, weight_budget};

/// Re-exported for the adapters. The config owns the choice of deployment.
pub use crate::runtime::config::BinanceEnv;

pub use payload::{
    AccountInfo, AccountTrade, Balance, CommissionRates, ExchangeInfo, OrderRecord, RateLimit,
    SymbolFilter, SymbolInfo,
};
pub use request::{HttpMethod, RequestAuth, RequestPlan};
pub use signed::{SignedRestClient, SignedRestConfig};
pub use weight::OrderCountWindow;

/// One request, in the form that the typed getters and the raw read of the recorder share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestRequest {
    DepthSnapshot {
        symbol: String,
        limit: u32,
    },
    Klines(KlineQuery),
    ExchangeInfo {
        symbol: String,
    },
    ServerTime,
    AccountInfo,
    MyTrades {
        symbol: String,
        from_id: Option<i64>,
        limit: u32,
    },
    OpenOrders {
        symbol: String,
    },
    OrderStatus {
        symbol: String,
        orig_client_order_id: String,
    },
    /// Keyed by the venue's order id, because a trade of `myTrades` names its order only that way.
    /// It is the one route from a fill back to its owner: ours to book, or a person's to leave.
    OrderStatusByVenueId {
        symbol: String,
        venue_order_id: i64,
    },
    CancelOrder {
        symbol: String,
        orig_client_order_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KlineQuery {
    pub symbol: String,
    pub interval: KlineInterval,
    pub limit: u32,
    pub start_ts_ms: Option<i64>,
    pub end_ts_ms: Option<i64>,
}

/// How the driver treats a failure. The codes of the venue stay behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FailureVerdict {
    Retry,
    Routine,
    Fatal,
}

#[derive(thiserror::Error, Debug)]
pub enum RestError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(
        "binance rate limited {url}: http {status}, retry after {retry_after_secs:?}s (code {code:?}: {message:?})"
    )]
    RateLimited {
        url: String,
        status: u16,
        retry_after_secs: Option<u64>,
        code: Option<i64>,
        message: Option<String>,
    },
    #[error("{url} returned http {status} (binance code {code:?}: {message:?})")]
    Status {
        url: String,
        status: u16,
        code: Option<i64>,
        message: Option<String>,
    },
    #[error(
        "binance refused {url} as unauthorised: http {status} (code {code:?}: {message:?}) — check the key's ip allowlist and that spot trading is enabled on it"
    )]
    Unauthorized {
        url: String,
        status: u16,
        code: Option<i64>,
        message: Option<String>,
    },
    #[error("decoding the response from {url} failed")]
    Decode {
        url: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("the api key holds bytes an http header cannot carry")]
    ApiKeyHeader,
    #[error("{endpoint} is a private endpoint — reach it through SignedRestClient, not RestClient")]
    RequiresSignature { endpoint: &'static str },
    #[error("signing the {endpoint} request failed")]
    Sign {
        endpoint: &'static str,
        #[source]
        source: crate::venues::binance::exec::SignError,
    },
}

impl RestError {
    pub fn verdict(&self) -> FailureVerdict {
        match self {
            RestError::Transport(
                TransportError::Request { .. } | TransportError::Timeout { .. },
            )
            | RestError::RateLimited { .. } => FailureVerdict::Retry,
            // A decode failure means the venue said something this build cannot read; the same
            // request will say it again.
            RestError::Transport(TransportError::ClientBuild { .. })
            | RestError::ApiKeyHeader
            | RestError::RequiresSignature { .. }
            | RestError::Sign { .. }
            | RestError::Unauthorized { .. }
            | RestError::Decode { .. } => FailureVerdict::Fatal,
            RestError::Status { code, message, .. } => status_verdict(*code, message.as_deref()),
        }
    }

    pub(crate) fn is_symbol_rejected(&self) -> bool {
        matches!(
            self,
            RestError::Status {
                code: Some(-1121),
                ..
            }
        )
    }
}

fn status_verdict(code: Option<i64>, message: Option<&str>) -> FailureVerdict {
    match code {
        Some(-1021 | -1003 | -1006 | -1007 | -1000 | -1001) => FailureVerdict::Retry,
        Some(-2010) => new_order_rejected_verdict(message),
        Some(-2013 | -2011) => FailureVerdict::Routine,
        _ => FailureVerdict::Fatal,
    }
}

/// Binance code -2010 is two codes in one: a "match" refusal is the ordinary cost of post-only, and
/// a "balance" refusal is not. Only the message tells them apart, so a -2010 with any other message
/// is not routine.
fn new_order_rejected_verdict(message: Option<&str>) -> FailureVerdict {
    match message {
        Some(text) if text.contains("immediately match") => FailureVerdict::Routine,
        _ => FailureVerdict::Fatal,
    }
}

/// REST client for one (market, deployment).
pub struct RestClient {
    http: HttpClient,
    market: BinanceMarket,
    env: BinanceEnv,
    host: Host,
    weight: WeightBudget,
    order_counts: OrderCountBudget,
}

/// Where the requests go. A loopback port is the only other place than the venue, so a client
/// that a test builds can reach nothing beyond this host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Host {
    Venue,
    Loopback(u16),
}

impl RestClient {
    /// # Errors
    /// [`RestError::Transport`] if the TLS backend fails to initialise.
    pub fn new(market: BinanceMarket, env: BinanceEnv) -> Result<Self, RestError> {
        Self::with_deadlines(market, env, Deadlines::STANDARD)
    }

    pub(crate) fn with_deadlines(
        market: BinanceMarket,
        env: BinanceEnv,
        deadlines: Deadlines,
    ) -> Result<Self, RestError> {
        let http = HttpClient::new(deadlines)?;
        Ok(Self {
            http,
            market,
            env,
            host: Host::Venue,
            weight: WeightBudget::new(weight_budget(market)),
            order_counts: OrderCountBudget::default(),
        })
    }

    /// The response body as the venue sent it, so that a recorder can save it byte for byte.
    ///
    /// # Errors
    /// Transport, non-2xx status, or 429/418 rate limiting.
    pub async fn fetch_text(&mut self, request: &RestRequest) -> Result<String, RestError> {
        Ok(self.request(request).await?.body)
    }

    /// # Errors
    /// [`RestError`].
    pub async fn exchange_info(&mut self, symbol: &str) -> Result<ExchangeInfo, RestError> {
        let fetched = self
            .request(&RestRequest::ExchangeInfo {
                symbol: symbol.to_owned(),
            })
            .await?;
        decode(&fetched)
    }

    /// # Errors
    /// [`RestError`].
    pub async fn server_time(&mut self) -> Result<VenueTs, RestError> {
        let fetched = self.request(&RestRequest::ServerTime).await?;
        let parsed: ServerTime = decode(&fetched)?;
        Ok(VenueTs::from_micros(
            parsed.server_time_ms.saturating_mul(1000),
        ))
    }

    async fn request(&mut self, request: &RestRequest) -> Result<Fetched, RestError> {
        let plan = request.plan(self.market);
        if plan.auth == RequestAuth::Signed {
            return Err(RestError::RequiresSignature {
                endpoint: plan.endpoint,
            });
        }
        self.send(Prepared {
            plan: &plan,
            signed_query: None,
            api_key: None,
        })
        .await
    }

    async fn send(&mut self, prepared: Prepared<'_>) -> Result<Fetched, RestError> {
        let plan = prepared.plan;
        self.weight.charge(plan.weight, plan.endpoint);
        let base = match self.host {
            Host::Venue => format!("{}{}", base_url(self.market, self.env), plan.path),
            Host::Loopback(port) => format!("http://127.0.0.1:{port}{}", plan.path),
        };
        let url = match prepared.signed_query {
            Some(signed_query) => format!("{base}?{signed_query}"),
            None => base,
        };

        let mut builder = self.http.request(method_of(plan.method), &url);
        if prepared.signed_query.is_none() {
            builder = builder.query(&plan.query);
        }
        if let Some(api_key) = prepared.api_key {
            builder = builder.header("X-MBX-APIKEY", api_key_header(api_key)?);
        }

        let response = self.http.send(builder, &url).await?;

        let status = response.status().as_u16();
        self.observe_limit_headers(response.headers());
        if !(200..300).contains(&status) {
            let retry_after_secs = http::retry_after_secs(response.headers());
            let (code, message) = binance_error(response).await;
            return Err(match status {
                429 | 418 => RestError::RateLimited {
                    url,
                    status,
                    retry_after_secs,
                    code,
                    message,
                },
                401 | 403 => RestError::Unauthorized {
                    url,
                    status,
                    code,
                    message,
                },
                _ => RestError::Status {
                    url,
                    status,
                    code,
                    message,
                },
            });
        }

        let body = self.http.body(response, &url).await?;
        Ok(Fetched { url, body })
    }

    fn observe_limit_headers(&mut self, headers: &reqwest::header::HeaderMap) {
        if let Some(used) = header_value(headers, "x-mbx-used-weight-1m") {
            self.weight.observe_server(used);
        }
        for (name, value) in headers {
            let Some(interval) = OrderCountWindow::interval_of_header(name.as_str()) else {
                continue;
            };
            let Some(used) = value.to_str().ok().and_then(|used| used.parse().ok()) else {
                continue;
            };
            self.order_counts.observe(interval, used);
        }
    }
}

/// A planned request and its authentication. A public read passes neither; the signed client fills
/// both.
struct Prepared<'a> {
    plan: &'a RequestPlan,
    signed_query: Option<&'a str>,
    api_key: Option<&'a Secret>,
}

struct Fetched {
    url: String,
    body: String,
}

#[derive(serde::Deserialize)]
struct ServerTime {
    #[serde(rename = "serverTime")]
    server_time_ms: i64,
}

#[derive(serde::Deserialize)]
struct BinanceErrorBody {
    code: i64,
    msg: String,
}

fn api_key_header(api_key: &Secret) -> Result<reqwest::header::HeaderValue, RestError> {
    let mut value = reqwest::header::HeaderValue::from_bytes(api_key.expose_bytes())
        .map_err(|_| RestError::ApiKeyHeader)?;
    value.set_sensitive(true);
    Ok(value)
}

fn method_of(method: HttpMethod) -> reqwest::Method {
    match method {
        HttpMethod::Get => reqwest::Method::GET,
        HttpMethod::Delete => reqwest::Method::DELETE,
    }
}

fn decode<T: DeserializeOwned>(fetched: &Fetched) -> Result<T, RestError> {
    serde_json::from_str(&fetched.body).map_err(|source| RestError::Decode {
        url: fetched.url.clone(),
        source,
    })
}

async fn binance_error(response: reqwest::Response) -> (Option<i64>, Option<String>) {
    let Ok(body) = response.text().await else {
        return (None, None);
    };
    match serde_json::from_str::<BinanceErrorBody>(&body) {
        Ok(parsed) => (Some(parsed.code), Some(parsed.msg)),
        Err(_) => (None, None),
    }
}

fn base_url(market: BinanceMarket, env: BinanceEnv) -> &'static str {
    match (env, market) {
        (BinanceEnv::Production, BinanceMarket::Spot) => "https://api.binance.com",
        (BinanceEnv::Production, BinanceMarket::Perpetual) => "https://fapi.binance.com",
        (BinanceEnv::Testnet, BinanceMarket::Spot) => "https://testnet.binance.vision",
        (BinanceEnv::Testnet, BinanceMarket::Perpetual) => "https://demo-fapi.binance.com",
    }
}

fn header_value<T: std::str::FromStr>(
    headers: &reqwest::header::HeaderMap,
    name: &str,
) -> Option<T> {
    headers.get(name)?.to_str().ok()?.parse().ok()
}
