//! REST layer: Gamma market discovery + CLOB `/book` probe. Deterministic slug. Teardown trusts 404
//! only. `Gamma*` names are the vendor's own word for the events API, not this module's identity.

use reqwest::Method;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::adapters::http::{self, Deadlines, HttpClient, TransportError};
use crate::config::PolySeries;
use crate::ids::{FIXED_SCALE, Price, Qty};
use crate::time::TsUs;

const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
const CLOB_BASE: &str = "https://clob.polymarket.com";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GammaMarket {
    pub slug: Box<str>,
    pub condition_id: Box<str>,
    pub token_up: Box<str>,
    pub token_down: Box<str>,
    pub tick_size: Price,
    pub min_order_size: Qty,
    pub window_open_ts_us: TsUs,
    pub window_close_ts_us: TsUs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookProbe {
    Live,
    TornDown,
}

#[derive(thiserror::Error, Debug)]
pub enum GammaError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(
        "polymarket rate limited {url}: http {status}, {}",
        format_retry_after(retry_after_secs)
    )]
    RateLimited {
        url: String,
        status: u16,
        retry_after_secs: Option<u64>,
    },
    #[error("{url} returned http {status}")]
    Status { url: String, status: u16 },
    #[error("decoding the response from {context} failed")]
    Decode {
        context: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("no polymarket market for slug {slug}")]
    MarketNotFound { slug: String },
    #[error("time-filtered fallback returned {found} of the 2 required windows")]
    FallbackTooFew { found: usize },
    #[error("clobTokenIds not a two-element json array: {raw:?}")]
    TokenIds { raw: Box<str> },
    #[error(
        "market {slug} outcomes {outcomes:?} are not [\"Up\", \"Down\"] — token index alignment unverifiable"
    )]
    InvalidOutcomes { slug: String, outcomes: Box<str> },
    #[error("market {slug} invalid: {reason}")]
    InvalidMarket { slug: String, reason: &'static str },
}

pub fn events_slug_url(series: PolySeries, window_start: TsUs) -> String {
    format!(
        "{GAMMA_BASE}/events?slug={}",
        slug_for(series, to_unix_seconds(window_start))
    )
}

pub fn fallback_url(series: PolySeries, now: TsUs) -> String {
    format!(
        "{GAMMA_BASE}/events?series_id={}&closed=false&end_date_min={}&order=endDate&ascending=true&limit=4",
        series.gamma_series_id(),
        iso8601_utc(now)
    )
}

pub fn book_url(token_id: &str) -> String {
    format!("{CLOB_BASE}/book?token_id={token_id}")
}

pub fn parse_events(
    series: PolySeries,
    raw: &str,
    window_start: TsUs,
) -> Result<GammaMarket, GammaError> {
    let events: Vec<RawEvent> = decode(raw, "gamma events?slug")?;
    let window_start_s = to_unix_seconds(window_start);
    let slug = slug_for(series, window_start_s);
    let market = first_market(&events).ok_or(GammaError::MarketNotFound { slug: slug.clone() })?;
    build_market(series, &slug, window_start_s, market)
}

pub fn parse_fallback(
    series: PolySeries,
    raw: &str,
) -> Result<(GammaMarket, GammaMarket), GammaError> {
    let events: Vec<RawEvent> = decode(raw, "gamma events fallback")?;
    let mut markets = events
        .iter()
        .filter_map(|event| resolve_fallback_row(series, event));
    let current = markets
        .next()
        .ok_or(GammaError::FallbackTooFew { found: 0 })??;
    let next = markets
        .next()
        .ok_or(GammaError::FallbackTooFew { found: 1 })??;
    Ok((current, next))
}

#[derive(Debug)]
pub struct PolyRest {
    http: HttpClient,
    series: PolySeries,
}

impl PolyRest {
    /// # Errors
    /// [`GammaError::Transport`] if the TLS backend fails to initialise.
    pub fn new(series: PolySeries) -> Result<Self, GammaError> {
        let http = HttpClient::new(Deadlines::STANDARD)?;
        Ok(Self { http, series })
    }

    pub async fn resolve_slug(&self, window_start: TsUs) -> Result<GammaMarket, GammaError> {
        let body = self
            .get_ok_text(&events_slug_url(self.series, window_start))
            .await?;
        parse_events(self.series, &body, window_start)
    }

    pub async fn resolve_current_and_next(
        &self,
        now: TsUs,
    ) -> Result<(GammaMarket, GammaMarket), GammaError> {
        let body = self.get_ok_text(&fallback_url(self.series, now)).await?;
        parse_fallback(self.series, &body)
    }

    pub async fn probe_book(&self, token_id: &str) -> Result<BookProbe, GammaError> {
        let url = book_url(token_id);
        let response = self.fetch(&url).await?;
        if response.status == 404 {
            return Ok(BookProbe::TornDown);
        }
        check_status(&url, response.status, response.retry_after_secs)?;
        Ok(BookProbe::Live)
    }

    pub async fn fetch_status_and_text(&self, url: &str) -> Result<(u16, String), GammaError> {
        let response = self.fetch(url).await?;
        Ok((response.status, response.body))
    }

    async fn fetch(&self, url: &str) -> Result<RawResponse, GammaError> {
        let request = self.http.request(Method::GET, url);
        let response = self.http.send(request, url).await?;
        let status = response.status().as_u16();
        let retry_after_secs = http::retry_after_secs(response.headers());
        let body = self.http.body(response, url).await?;
        Ok(RawResponse {
            status,
            retry_after_secs,
            body,
        })
    }

    async fn get_ok_text(&self, url: &str) -> Result<String, GammaError> {
        let response = self.fetch(url).await?;
        check_status(url, response.status, response.retry_after_secs)?;
        Ok(response.body)
    }
}

struct RawResponse {
    status: u16,
    retry_after_secs: Option<u64>,
    body: String,
}

fn check_status(url: &str, status: u16, retry_after_secs: Option<u64>) -> Result<(), GammaError> {
    if status == 429 {
        return Err(GammaError::RateLimited {
            url: url.to_owned(),
            status,
            retry_after_secs,
        });
    }
    if !(200..300).contains(&status) {
        return Err(GammaError::Status {
            url: url.to_owned(),
            status,
        });
    }
    Ok(())
}

fn format_retry_after(retry_after_secs: &Option<u64>) -> String {
    match retry_after_secs {
        Some(seconds) => format!("retry after {seconds}s"),
        None => "no retry-after header".to_owned(),
    }
}

fn decode<T: DeserializeOwned>(raw: &str, context: &'static str) -> Result<T, GammaError> {
    serde_json::from_str(raw).map_err(|source| GammaError::Decode { context, source })
}

fn resolve_fallback_row(
    series: PolySeries,
    event: &RawEvent,
) -> Option<Result<GammaMarket, GammaError>> {
    let slug = event.slug.as_deref()?;
    let window_start_s = window_start_from_slug(series, slug)?;
    let market = event.markets.first()?;
    Some(build_market(series, slug, window_start_s, market))
}

fn build_market(
    series: PolySeries,
    slug: &str,
    window_start_s: i64,
    raw: &RawMarket,
) -> Result<GammaMarket, GammaError> {
    let [token_up, token_down] = parse_token_ids(&raw.clob_token_ids)?;
    if !outcomes_are_up_down(&raw.outcomes) {
        return Err(GammaError::InvalidOutcomes {
            slug: slug.to_owned(),
            outcomes: raw.outcomes.as_str().into(),
        });
    }
    if raw.tick_size <= 0.0 {
        return Err(GammaError::InvalidMarket {
            slug: slug.to_owned(),
            reason: "orderPriceMinTickSize is not positive",
        });
    }
    Ok(GammaMarket {
        slug: slug.into(),
        condition_id: raw.condition_id.as_str().into(),
        token_up: token_up.into(),
        token_down: token_down.into(),
        tick_size: Price(mantissa(raw.tick_size)),
        min_order_size: Qty(mantissa(raw.min_order_size)),
        window_open_ts_us: seconds_to_us(window_start_s),
        window_close_ts_us: seconds_to_us(window_start_s) + series.window_len(),
    })
}

/// `clobTokenIds` double-serialised (string → array).
fn parse_token_ids(raw: &str) -> Result<[String; 2], GammaError> {
    serde_json::from_str::<[String; 2]>(raw).map_err(|_| GammaError::TokenIds { raw: raw.into() })
}

/// Tokens taken by index from `clobTokenIds`: index 0="Up", 1="Down". Reject other orderings (silent inversion breaks downstream).
fn outcomes_are_up_down(raw: &str) -> bool {
    serde_json::from_str::<[String; 2]>(raw).is_ok_and(|outcomes| outcomes == ["Up", "Down"])
}

fn to_unix_seconds(ts: TsUs) -> i64 {
    ts.micros().div_euclid(1_000_000)
}

fn slug_for(series: PolySeries, window_start_s: i64) -> String {
    format!("{}-{window_start_s}", series.as_str())
}

/// Slug trailing integer = window grid start (not `startDate` creation time).
fn window_start_from_slug(series: PolySeries, slug: &str) -> Option<i64> {
    slug.strip_prefix(series.as_str())?
        .strip_prefix('-')?
        .parse()
        .ok()
}

fn seconds_to_us(seconds: i64) -> TsUs {
    TsUs::from_micros(seconds.saturating_mul(1_000_000))
}

/// Venue float → 1e-8 mantissa (one-shot config read; only venue-to-mantissa conversion).
fn mantissa(value: f64) -> i64 {
    (value * FIXED_SCALE as f64).round() as i64
}

fn iso8601_utc(at: TsUs) -> String {
    let civil = at.civil();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        civil.year, civil.month, civil.day, civil.hour, civil.minute, civil.second
    )
}

fn first_market(events: &[RawEvent]) -> Option<&RawMarket> {
    events.first()?.markets.first()
}

#[derive(Deserialize)]
struct RawEvent {
    slug: Option<String>,
    #[serde(default)]
    markets: Vec<RawMarket>,
}

#[derive(Deserialize)]
struct RawMarket {
    #[serde(rename = "conditionId")]
    condition_id: String,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: String,
    #[serde(default)]
    outcomes: String,
    #[serde(rename = "orderPriceMinTickSize")]
    tick_size: f64,
    #[serde(rename = "orderMinSize", default)]
    min_order_size: f64,
}
