//! The loopback server behind the browser workstation: it serves the Trunk bundle and the app
//! descriptor Jet reads, and carries link datagrams between a browser tab and an engine without
//! decoding them. It holds no market state.

mod arguments;
mod bundle;
mod relay;

use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::header::{
    ALLOW, CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, HOST, HeaderValue, ORIGIN,
};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::msg::relay::RELAY_PATH;

use bundle::Bundle;
use relay::Relay;

pub use arguments::{ArgumentError, WebConfig};

const ACCEPT_RETRY_PAUSE: Duration = Duration::from_millis(100);

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
/// [`WebError::Bind`] when the port is taken, and [`WebError::Signal`] when ctrl-c cannot be
/// awaited.
pub async fn serve(config: WebConfig) -> Result<(), WebError> {
    let bundle = Bundle::load(&config.bundle_dir, config.mcp_url.as_deref())?;
    let requested = SocketAddr::from((Ipv4Addr::LOCALHOST, config.port));
    let bind_failed = |source| WebError::Bind {
        address: requested,
        source,
    };
    let listener = TcpListener::bind(requested).await.map_err(bind_failed)?;
    let address = listener.local_addr().map_err(bind_failed)?;
    let shared = Arc::new(Shared {
        bundle,
        relay: Relay::new(&config),
        port: address.port(),
    });
    eprintln!("polysim-web: listening on http://{address}/");
    tokio::select! {
        () = accept_forever(listener, shared) => Ok(()),
        signalled = tokio::signal::ctrl_c() => signalled.map_err(WebError::Signal),
    }
}

// SMELL: the MCP server's HTTP listener keeps its own copy of this accept loop and retry pause, so
// a fix to one leaves the other behind.
async fn accept_forever(listener: TcpListener, shared: Arc<Shared>) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _peer)) => stream,
            Err(error) => {
                // An accept error usually means the process is out of file descriptors, which
                // passes once one is freed. Ending the server over it would drop every tab, and
                // retrying at once would keep a core busy until then.
                eprintln!("polysim-web: cannot accept a connection: {error}");
                tokio::time::sleep(ACCEPT_RETRY_PAUSE).await;
                continue;
            }
        };
        let shared = Arc::clone(&shared);
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

struct Refusal {
    status: StatusCode,
    reason: String,
}

impl Refusal {
    fn new(status: StatusCode, reason: impl Into<String>) -> Refusal {
        Refusal {
            status,
            reason: reason.into(),
        }
    }
}

/// The relay carries run control with no login, so a request must come from this machine's own
/// address and from a page this server served. Checking `Host` stops a rebinding attack, where a
/// foreign name resolves to loopback, and checking `Origin` stops a page on another site from
/// opening the relay. A browser cannot forge either header, and it always sends `Origin` when a
/// page opens a WebSocket. A frame load or a plain link from another site sends none, which is
/// why every response also refuses a frame on another site.
fn guard(headers: &HeaderMap, port: u16) -> Result<(), Refusal> {
    let mut hosts = headers.get_all(HOST).iter();
    let (Some(host), None) = (hosts.next(), hosts.next()) else {
        return Err(Refusal::new(
            StatusCode::BAD_REQUEST,
            "the request needs exactly one Host header",
        ));
    };
    let host = host.to_str().unwrap_or_default();
    if !is_loopback_authority(host, port) {
        return Err(Refusal::new(
            StatusCode::FORBIDDEN,
            format!("this server answers only to 127.0.0.1, localhost and [::1] on port {port}"),
        ));
    }
    let mut origins = headers.get_all(ORIGIN).iter();
    match (origins.next(), origins.next()) {
        (None, _) => Ok(()),
        (Some(origin), None) if *origin == format!("http://{host}") => Ok(()),
        _ => Err(Refusal::new(
            StatusCode::FORBIDDEN,
            "this server refuses requests from a web page it did not serve",
        )),
    }
}

// SMELL: a browser leaves `:80` off both `Host` and `Origin`, so a server started with
// `--port 80` refuses every page. Accepting a bare loopback name when bound to port 80 would fix
// it.
fn is_loopback_authority(authority: &str, port: u16) -> bool {
    let Some((name, number)) = authority.rsplit_once(':') else {
        return false;
    };
    let is_loopback_name = ["127.0.0.1", "localhost", "[::1]"]
        .iter()
        .any(|loopback| name.eq_ignore_ascii_case(loopback));
    is_loopback_name && number == port.to_string()
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
