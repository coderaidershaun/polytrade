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
    #[error("request {target} failed")]
    Request {
        target: String,
        #[source]
        source: reqwest::Error,
    },
    // The target goes last because a log record cuts a long line off at the end.
    #[error(
        "request timed out after {limit_secs}s while {phase} — slow or blocked route to the venue (vpn, proxy, firewall): {target}"
    )]
    Timeout {
        target: String,
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

    /// `target` is how an error names the request, so it holds no secret: a venue that signs its
    /// query string passes the method and the path alone.
    pub(crate) async fn send(
        &self,
        builder: RequestBuilder,
        target: &str,
    ) -> Result<Response, TransportError> {
        builder
            .send()
            .await
            .map_err(|source| self.failure(target, "waiting for the response", source))
    }

    pub(crate) async fn body(
        &self,
        response: Response,
        target: &str,
    ) -> Result<String, TransportError> {
        response
            .text()
            .await
            .map_err(|source| self.failure(target, "reading the response body", source))
    }

    /// One deadline covers both the wait for headers and the body read, and reqwest reports a slow
    /// body as a timeout wrapped in a decoding error. The phase says which of the two ran out of
    /// time. The error of reqwest prints the whole url, query string and signature included, so it
    /// loses its url here.
    fn failure(&self, target: &str, phase: &'static str, source: reqwest::Error) -> TransportError {
        let target = target.to_owned();
        if !source.is_timeout() {
            return TransportError::Request {
                target,
                source: source.without_url(),
            };
        }
        if source.is_connect() {
            return TransportError::Timeout {
                target,
                phase: "connecting",
                limit_secs: self.deadlines.connect.as_secs(),
            };
        }
        TransportError::Timeout {
            target,
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
