//! Serves the MCP tools over HTTP on one loopback listener, so every client session drives the same
//! fleet and an engine outlives the client that started it.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

use super::ServeError;
use super::tools::PolysimMcp;
use crate::operator::accept::{self, AcceptTries};

/// Returns only when the listener cannot be bound. On a signal the caller drops this future, and
/// dropping it ends every session and every open connection.
pub(super) async fn serve(
    server: PolysimMcp,
    port: u16,
    accept_tries: AcceptTries,
) -> Result<(), ServeError> {
    let requested = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let bind_failed = |source| ServeError::Bind {
        address: requested,
        source,
    };
    let mut listener = TcpListener::bind(requested).await.map_err(bind_failed)?;
    let address = listener.local_addr().map_err(bind_failed)?;
    // A browser adds an Origin header to the requests a web page's script sends, and a native MCP
    // client sends none, so refusing that header keeps web pages away from the tools.
    let config = StreamableHttpServerConfig::default().enforce_origin_validation();
    let cancel = config.cancellation_token.clone();
    let _end_every_session = cancel.clone().drop_guard();
    // SMELL: every session shares one lock on the fleet, and a stop holds it until the engine has
    // exited, so a list from another client can wait behind that stop for up to the stop deadline.
    let service = TowerToHyperService::new(StreamableHttpService::new(
        move || Ok(server.clone()),
        Arc::new(LocalSessionManager::default()),
        config,
    ));
    eprintln!("polysim-mcp: listening on http://{address}/mcp");
    let error = accept::each_connection(&mut listener, accept_tries, |stream| {
        let connection =
            http1::Builder::new().serve_connection(TokioIo::new(stream), service.clone());
        let cancel = cancel.clone();
        tokio::spawn(async move {
            tokio::select! {
                // A client that hangs up mid-reply ends its connection with an error, which is
                // routine and leaves nothing to clean up.
                _closed = connection => {}
                () = cancel.cancelled() => {}
            }
        });
    })
    .await;
    // Returning would stop every engine that this server started, and an accept error must never
    // stop an engine. So the server closes its port, keeps the sessions it has, and waits for its
    // exit signal.
    drop(listener);
    eprintln!(
        "polysim-mcp: {error}: {}; this server takes no new session, and each engine that it \
         started runs until the server is told to exit",
        error.last()
    );
    std::future::pending().await
}

/// Whatever the parent writes is read and thrown away, because only the end of the pipe matters.
pub(super) async fn wait_until_stdin_closes() -> Result<(), ServeError> {
    let mut stdin = tokio::io::stdin();
    let mut discarded = [0_u8; 256];
    loop {
        match stdin.read(&mut discarded).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(source) => return Err(ServeError::StdinRead(source)),
        }
    }
}
