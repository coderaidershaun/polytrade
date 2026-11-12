//! One WebSocket dial for every venue. What differs between venues is the URL, not the handshake.

use futures_util::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{Error as ProtocolError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

pub(crate) type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub(crate) type Writer = SplitSink<Socket, Message>;
pub(crate) type Reader = SplitStream<Socket>;

#[derive(thiserror::Error, Debug)]
pub(crate) enum SocketError {
    #[error("connecting to {url} failed")]
    Connect {
        url: String,
        #[source]
        source: ProtocolError,
    },
}

pub(crate) async fn connect(url: &str) -> Result<Socket, SocketError> {
    let (socket, _response) = connect_async(url)
        .await
        .map_err(|source| SocketError::Connect {
            url: url.to_owned(),
            source,
        })?;
    Ok(socket)
}
