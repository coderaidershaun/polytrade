//! The loopback server behind the browser workstation: it serves the Trunk bundle and the app
//! descriptor Jet reads, and carries link datagrams between a browser tab and an engine without
//! decoding them. It holds no market state.

mod arguments;
mod bundle;
mod guard;
mod tab;

use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::{ALLOW, CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, HeaderValue};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};

use crate::msg::relay::RELAY_PATH;
use crate::operator::accept::{self, AcceptError};

use bundle::Bundle;
use guard::{Refusal, guard};
use tab::Relay;

pub use arguments::{ArgumentError, WebConfig};

#[derive(Debug, thiserror::Error)]
pub enum WebError {
    #[error("no workstation bundle at {} - run `trunk build` in browser/ first", .dir.display())]
    BundleMissing { dir: PathBuf },
    #[error("cannot read the workstation bundle at {}", .path.display())]
    BundleRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot bind {address}")]
    Bind {
        address: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot listen for ctrl-c")]
    Signal(#[source] std::io::Error),
    #[error(transparent)]
    Accept(#[from] AcceptError),
}

struct Shared {
    bundle: Bundle,
    relay: Relay,
    port: u16,
}

/// Binds `127.0.0.1` only. Port 0 lets the operating system pick, and the line printed on stderr
/// once the server listens names the port it got. Returns when ctrl-c arrives.
///
/// # Errors
/// [`WebError::BundleMissing`] and [`WebError::BundleRead`] when the bundle cannot be loaded,
/// [`WebError::Bind`] when the port is taken, [`WebError::Signal`] when ctrl-c cannot be awaited,
/// and [`WebError::Accept`] once accepts fail more times in a row than `--accept-tries` allows.
/// The relay holds no state that outlives it, so it ends there and its process exits with 1.
pub async fn serve(config: WebConfig) -> Result<(), WebError> {
    let bundle = Bundle::load(&config.bundle_dir, config.mcp_url.as_deref())?;
    let requested = SocketAddr::from((Ipv4Addr::LOCALHOST, config.port));
    let bind_failed = |source| WebError::Bind {
        address: requested,
        source,
    };
    let mut listener = TcpListener::bind(requested).await.map_err(bind_failed)?;
    let address = listener.local_addr().map_err(bind_failed)?;
    let shared = Arc::new(Shared {
        bundle,
        relay: Relay::new(&config),
        port: address.port(),
    });
    eprintln!("polysim-web: listening on http://{address}/");
    let accepting = accept::each_connection(&mut listener, config.accept_tries, |stream| {
        serve_connection(stream, Arc::clone(&shared));
    });
    tokio::select! {
        error = accepting => Err(error.into()),
        signalled = tokio::signal::ctrl_c() => signalled.map_err(WebError::Signal),
    }
}

fn serve_connection(stream: TcpStream, shared: Arc<Shared>) {
    tokio::spawn(async move {
        let service = service_fn(move |request| respond(Arc::clone(&shared), request));
        let connection = http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .with_upgrades();
        // A client that hangs up mid-reply ends its connection with an error, which is routine
        // and leaves nothing to clean up.
        connection.await.ok();
    });
}

async fn respond(
    shared: Arc<Shared>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let mut response = match guard(request.headers(), shared.port) {
        Ok(()) => route(&shared, request).await,
        Err(Refusal { status, reason }) => plain(status, reason),
    };
    let headers = response.headers_mut();
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    // A site that could frame the page could lay a decoy over START or STOP and steer a click onto
    // it. Jet frames the page from its own origin, through its proxy, so that parent still may.
    headers.insert(
        CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("frame-ancestors 'self'"),
    );
    Ok(response)
}

async fn route(shared: &Shared, request: Request<Incoming>) -> Response<Full<Bytes>> {
    let path = request.uri().path();
    if path.strip_prefix('/') == Some(RELAY_PATH) {
        return shared.relay.upgrade(request).await;
    }
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        return method_not_allowed("GET, HEAD");
    }
    match shared.bundle.find(path) {
        Some(file) => reply(StatusCode::OK, file.content_type, file.body.clone()),
        None => plain(
            StatusCode::NOT_FOUND,
            "no such file in the workstation bundle",
        ),
    }
}

fn reply(status: StatusCode, content_type: &'static str, body: Bytes) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

fn plain(status: StatusCode, text: impl Into<Bytes>) -> Response<Full<Bytes>> {
    reply(status, "text/plain; charset=utf-8", text.into())
}

fn method_not_allowed(allowed: &'static str) -> Response<Full<Bytes>> {
    let mut response = plain(
        StatusCode::METHOD_NOT_ALLOWED,
        "this address does not take that method",
    );
    response
        .headers_mut()
        .insert(ALLOW, HeaderValue::from_static(allowed));
    response
}
