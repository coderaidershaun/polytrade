//! One HTTP transport for every Polymarket call this crate makes — the startup gate, the read-only
//! probe and the execution actor all reach the venue through it.
//!
//! Two properties are why it is one client rather than three. The L2 preimage covers the body bytes
//! exactly as sent and the path WITHOUT its query, so a second implementation is a second chance to
//! sign the wrong thing. And a non-2xx status is not a transport failure here: this venue puts its
//! order verdicts in the body of a `400`, so the status travels beside the body and only the caller
//! decides what it means.

use std::time::Duration;

use reqwest::Method;

use crate::adapters::http::{Deadlines, HttpClient, TransportError};

use super::codec::{EncodedRequest, HttpAnswer};
use super::sign::key::SigningKey;
use super::sign::l1::{ClobAuthRequest, DEFAULT_NONCE, clob_auth_headers};
use super::sign::l2::{HttpMethod, RequestSigner, RequestToSign};

pub const CLOB_BASE: &str = "https://clob.polymarket.com";

/// The geoblock check lives on the website host, not an api host.
pub const GEOBLOCK_URL: &str = "https://polymarket.com/api/geoblock";

pub const CHAIN_ID: u64 = 137;

/// Signed, and can go negative once a bulk cancel overdraws the bucket.
const RATE_LIMIT_REMAINING_HEADER: &str = "poly-ratelimit-remaining";
const RATE_LIMIT_WARNING_HEADER: &str = "poly-ratelimit-warning";

/// Enough of a failing body to identify it; venue errors are short and the interesting part leads.
const BODY_EXCERPT_CHARS: usize = 300;

/// What the venue answered, with the status kept rather than converted.
#[derive(Debug, Clone)]
pub struct ClobResponse {
    pub status: u16,
    pub body: String,
    pub rate_limit: RateLimit,
}

impl ClobResponse {
    pub fn answer(&self) -> HttpAnswer<'_> {
        HttpAnswer {
            status: self.status,
            body: &self.body,
        }
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub fn excerpt(&self) -> String {
        self.body.chars().take(BODY_EXCERPT_CHARS).collect()
    }
}

/// The per-signer bucket state the venue reports on mutating calls. Absent on plenty of endpoints,
/// so every field is optional rather than defaulted — a missing header is not a zero budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimit {
    /// Signed: a bulk cancel debits after the fact and can push the bucket below zero.
    pub remaining: Option<i64>,
    pub warning: Option<String>,
}

impl RateLimit {
    pub const ABSENT: RateLimit = RateLimit {
        remaining: None,
        warning: None,
    };

    pub fn is_absent(&self) -> bool {
        self.remaining.is_none() && self.warning.is_none()
    }

    /// The bucket owes more than it holds, so cancels are refused until it recovers.
    pub fn is_overdrawn(&self) -> bool {
        self.remaining.is_some_and(|remaining| remaining < 0)
    }

    fn from_response(response: &reqwest::Response) -> Self {
        Self {
            remaining: header_text(response, RATE_LIMIT_REMAINING_HEADER)
                .and_then(|text| text.trim().parse().ok()),
            warning: header_text(response, RATE_LIMIT_WARNING_HEADER),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClobHttp {
    http: HttpClient,
}

impl ClobHttp {
    /// # Errors
    /// [`TransportError::ClientBuild`] if the TLS backend fails to initialise.
    pub fn new(
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, TransportError> {
        let http = HttpClient::new(Deadlines {
            connect: connect_timeout,
            request: request_timeout,
        })?;
        Ok(Self { http })
    }

    /// Unsigned, at an absolute url — the one check that does not live on the api host.
    pub async fn send_unsigned(&self, url: &str) -> Result<ClobResponse, TransportError> {
        self.send(self.http.request(Method::GET, url), url).await
    }

    pub async fn send_public(
        &self,
        request: &EncodedRequest,
    ) -> Result<ClobResponse, TransportError> {
        let url = url_of(request);
        self.send(self.builder(request, &url), &url).await
    }

    /// L2: the api-key headers every private call carries.
    pub async fn send_signed(
        &self,
        signer: &RequestSigner,
        request: &EncodedRequest,
        timestamp_secs: i64,
    ) -> Result<ClobResponse, TransportError> {
        let headers = signer.headers(
            &RequestToSign {
                method: request.method,
                path: &request.path,
                body: &request.body,
            },
            timestamp_secs,
        );
        self.send_with_headers(request, headers.entries()).await
    }

    /// L1: the wallet key signs directly, and only the two credential endpoints accept it.
    pub async fn send_wallet_signed(
        &self,
        key: &SigningKey,
        request: &EncodedRequest,
        timestamp_secs: i64,
    ) -> Result<ClobResponse, TransportError> {
        let headers = clob_auth_headers(
            key,
            &ClobAuthRequest {
                chain_id: CHAIN_ID,
                timestamp_secs,
                nonce: DEFAULT_NONCE,
            },
        );
        self.send_with_headers(request, headers.entries()).await
    }

    async fn send_with_headers(
        &self,
        request: &EncodedRequest,
        headers: impl IntoIterator<Item = (&'static str, &str)>,
    ) -> Result<ClobResponse, TransportError> {
        let url = url_of(request);
        let mut builder = self.builder(request, &url);
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
        self.send(builder, &url).await
    }

    /// The body is handed over as the exact bytes that were signed; reqwest's own json helper would
    /// re-serialise and invalidate the header.
    fn builder(&self, request: &EncodedRequest, url: &str) -> reqwest::RequestBuilder {
        let method = match request.method {
            HttpMethod::Get => Method::GET,
            HttpMethod::Post => Method::POST,
            HttpMethod::Delete => Method::DELETE,
        };
        let builder = self.http.request(method, url);
        match request.body.is_empty() {
            true => builder,
            false => builder
                .header("Content-Type", "application/json")
                .body(request.body.clone()),
        }
    }

    async fn send(
        &self,
        builder: reqwest::RequestBuilder,
        url: &str,
    ) -> Result<ClobResponse, TransportError> {
        let response = self.http.send(builder, url).await?;
        let status = response.status().as_u16();
        let rate_limit = RateLimit::from_response(&response);
        let body = self.http.body(response, url).await?;
        Ok(ClobResponse {
            status,
            body,
            rate_limit,
        })
    }
}

fn url_of(request: &EncodedRequest) -> String {
    match request.query.is_empty() {
        true => format!("{CLOB_BASE}{}", request.path),
        false => format!("{CLOB_BASE}{}?{}", request.path, request.query),
    }
}

fn header_text(response: &reqwest::Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}
