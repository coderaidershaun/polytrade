//! What every venue's REST client shares: how the client is built, how a request that never got an
//! answer is reported, and how `Retry-After` is read. What a non-2xx status means stays with each
//! venue, because the venues disagree on it.

use std::time::Duration;

use reqwest::header::{HeaderMap, RETRY_AFTER};
use reqwest::{Client, Method, RequestBuilder, Response};

#[derive(thiserror::Error, Debug)]
pub enum TransportError {
    #[error("building the http client failed")]
    ClientBuild {
        #[source]
        source: reqwest::Error,
    },
    #[error("request to {url} failed")]
    Request {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    // The url goes last because a log record cuts a long line off at the end, and a signed url is
    // long enough to hide anything written after it.
    #[error(
        "request timed out after {limit_secs}s while {phase} — slow or blocked route to the venue (vpn, proxy, firewall): {url}"
    )]
    Timeout {
        url: String,
        phase: &'static str,
        limit_secs: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Deadlines {
    pub(crate) connect: Duration,
    pub(crate) request: Duration,
}

impl Deadlines {
    pub(crate) const STANDARD: Deadlines = Deadlines {
        connect: Duration::from_secs(5),
        request: Duration::from_secs(10),
    };
}

#[derive(Debug, Clone)]
pub(crate) struct HttpClient {
    http: Client,
    deadlines: Deadlines,
}

impl HttpClient {
    pub(crate) fn new(deadlines: Deadlines) -> Result<Self, TransportError> {
        let http = Client::builder()
            .connect_timeout(deadlines.connect)
            .timeout(deadlines.request)
            .build()
            .map_err(|source| TransportError::ClientBuild { source })?;
        Ok(Self { http, deadlines })
    }

    pub(crate) fn request(&self, method: Method, url: &str) -> RequestBuilder {
        self.http.request(method, url)
    }

    pub(crate) async fn send(
        &self,
        builder: RequestBuilder,
        url: &str,
    ) -> Result<Response, TransportError> {
        builder
            .send()
            .await
            .map_err(|source| self.failure(url, "waiting for the response", source))
    }

    pub(crate) async fn body(
        &self,
        response: Response,
        url: &str,
    ) -> Result<String, TransportError> {
        response
            .text()
            .await
            .map_err(|source| self.failure(url, "reading the response body", source))
    }

    /// One deadline covers both the wait for headers and the body read, and reqwest reports a slow
    /// body as a timeout wrapped in a decoding error. The phase says which of the two ran out of
    /// time.
    fn failure(&self, url: &str, phase: &'static str, source: reqwest::Error) -> TransportError {
        let url = url.to_owned();
        if !source.is_timeout() {
            return TransportError::Request { url, source };
        }
        if source.is_connect() {
            return TransportError::Timeout {
                url,
                phase: "connecting",
                limit_secs: self.deadlines.connect.as_secs(),
            };
        }
        TransportError::Timeout {
            url,
            phase,
            limit_secs: self.deadlines.request.as_secs(),
        }
    }
}

/// Reads only the delta-seconds form. The HTTP-date form reads as absent, because the dependency
/// list allows no calendar parser.
pub(crate) fn retry_after_secs(headers: &HeaderMap) -> Option<u64> {
    headers.get(RETRY_AFTER)?.to_str().ok()?.trim().parse().ok()
}
