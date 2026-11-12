//! One WebSocket per browser tab, joined to one UDP socket per tab that is connected to the tab's
//! engine. Every binary message crosses as exactly one datagram, in either direction, so the server
//! never reads a link frame and the tab is an ordinary link subscriber.

use std::io::{self, ErrorKind};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::upgrade::Upgraded;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::UdpSocket;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::handshake::server::create_response_with_body;
use tokio_tungstenite::tungstenite::protocol::{Message, Role, WebSocketConfig};

use super::{WebConfig, method_not_allowed, plain};
use crate::link::{LINK_MAX_DATAGRAM, LinkHash};
use crate::msg::relay::{PEER_KEY, RelayHello};

pub(super) struct Relay {
    peers: Vec<SocketAddr>,
    hello: String,
}

struct Tab {
    browser: WebSocketStream<TokioIo<Upgraded>>,
    engine: UdpSocket,
    counts: Counts,
}

#[derive(Default)]
struct Counts {
    to_engine: u64,
    to_browser: u64,
    /// Everything the relay would not carry: text frames, datagrams longer than the link allows,
    /// and sends or receives that a down engine bounced.
    refused: u64,
}

#[derive(Debug, thiserror::Error)]
enum RelayError {
    #[error("the browser socket failed: {0}")]
    Browser(#[from] WebSocketError),
    #[error("the engine socket failed: {0}")]
    Engine(#[from] io::Error),
}

impl Relay {
    pub(super) fn new(config: &WebConfig) -> Relay {
        let hello = RelayHello {
            strategy: config.strategy_id.as_str().to_owned(),
            peers: config.peers.clone(),
            token_hash: LinkHash::of_name(config.token.as_deref().unwrap_or_default()).0,
        };
        Relay {
            peers: config.peers.clone(),
            hello: serde_json::to_string(&hello)
                .expect("a hello of text and integers always serialises"),
        }
    }

    pub(super) async fn upgrade(&self, mut request: Request<Incoming>) -> Response<Full<Bytes>> {
        if request.method() != Method::GET {
            return method_not_allowed("GET");
        }
        let Some(peer) = self.peer_of(request.uri().query()) else {
            return plain(
                StatusCode::NOT_FOUND,
                format!(
                    "peer must be a number below {}, the count of engines this server relays to",
                    self.peers.len()
                ),
            );
        };
        let response = match create_response_with_body(&request, || Full::new(Bytes::new())) {
            Ok(response) => response,
            Err(error) => {
                return plain(
                    StatusCode::BAD_REQUEST,
                    format!("this address speaks WebSocket only: {error}"),
                );
            }
        };
        let engine = match connect_to(peer).await {
            Ok(engine) => engine,
            Err(error) => {
                eprintln!("polysim-web: cannot open a udp socket to {peer}: {error}");
                return plain(
                    StatusCode::BAD_GATEWAY,
                    "cannot open a socket to the engine",
                );
            }
        };
        let upgrade = hyper::upgrade::on(&mut request);
        let hello = self.hello.clone();
        tokio::spawn(async move {
            match upgrade.await {
                Ok(upgraded) => carry(TokioIo::new(upgraded), engine, peer, hello).await,
                Err(error) => eprintln!("polysim-web: relay {peer} could not upgrade: {error}"),
            }
        });
        response
    }

    fn peer_of(&self, query: Option<&str>) -> Option<SocketAddr> {
        let index = query?
            .split('&')
            .find_map(|pair| pair.strip_prefix(PEER_KEY)?.strip_prefix('='))?;
        self.peers.get(index.parse::<usize>().ok()?).copied()
    }
}

async fn connect_to(peer: SocketAddr) -> io::Result<UdpSocket> {
    let unspecified = match peer {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    };
    let udp = UdpSocket::bind(unspecified).await?;
    udp.connect(peer).await?;
    Ok(udp)
}

async fn carry(upgraded: TokioIo<Upgraded>, engine: UdpSocket, peer: SocketAddr, hello: String) {
    let config = WebSocketConfig::default()
        .max_message_size(Some(LINK_MAX_DATAGRAM))
        .max_frame_size(Some(LINK_MAX_DATAGRAM));
    let browser = WebSocketStream::from_raw_socket(upgraded, Role::Server, Some(config)).await;
    let mut tab = Tab {
        browser,
        engine,
        counts: Counts::default(),
    };
    let cause = tab
        .run(hello)
        .await
        .err()
        .map_or_else(String::new, |error| format!(": {error}"));
    let Counts {
        to_engine,
        to_browser,
        refused,
    } = tab.counts;
    eprintln!(
        "polysim-web: relay {peer} closed (forwarded {to_engine} to engine, {to_browser} to \
         browser, refused {refused}){cause}"
    );
}

impl Tab {
    async fn run(&mut self, hello: String) -> Result<(), RelayError> {
        self.browser.send(Message::text(hello)).await?;
        let mut inbox = [0_u8; LINK_MAX_DATAGRAM + 1];
        loop {
            tokio::select! {
                // A close frame must finish its handshake before more datagrams are written to a
                // socket that is closing.
                biased;
                incoming = self.browser.next() => match incoming {
                    Some(Ok(message)) => self.forward_to_engine(message).await?,
                    Some(Err(WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake)))
                    | None => return Ok(()),
                    Some(Err(error)) => return Err(error.into()),
                },
                received = self.engine.recv(&mut inbox) => {
                    self.forward_to_browser(received, &inbox).await?;
                }
            }
        }
    }

    async fn forward_to_engine(&mut self, message: Message) -> Result<(), RelayError> {
        match message {
            Message::Binary(datagram) => match self.engine.send(&datagram).await {
                Ok(_) => self.counts.to_engine += 1,
                Err(error) => self.count_if_refused(error)?,
            },
            Message::Text(_) => self.counts.refused += 1,
            _ => {}
        }
        Ok(())
    }

    async fn forward_to_browser(
        &mut self,
        received: io::Result<usize>,
        inbox: &[u8],
    ) -> Result<(), RelayError> {
        match received {
            Ok(length) if length > LINK_MAX_DATAGRAM => self.counts.refused += 1,
            Ok(length) => {
                self.browser.send(Message::from(&inbox[..length])).await?;
                self.counts.to_browser += 1;
            }
            Err(error) => self.count_if_refused(error)?,
        }
        Ok(())
    }

    /// A down engine bounces datagrams back as an error on the next send or receive, which is
    /// routine across an engine restart. Any other socket error ends the relay.
    fn count_if_refused(&mut self, error: io::Error) -> Result<(), RelayError> {
        if error.kind() != ErrorKind::ConnectionRefused {
            return Err(error.into());
        }
        self.counts.refused += 1;
        Ok(())
    }
}
