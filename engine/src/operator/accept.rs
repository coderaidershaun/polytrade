//! The one accept loop of the loopback servers: it tries again after an accept error, up to a cap
//! of errors in a row, and then hands the error to its server, which decides what it means.

use std::fmt;
use std::io;
use std::num::{NonZeroU32, ParseIntError};
use std::str::FromStr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};

const FIRST_PAUSE: Duration = Duration::from_millis(100);

/// Where a server takes its connections from. The pause after an error is here too, so a test can
/// stand in for the socket and the clock together.
pub trait Listener {
    type Connection;

    fn accept(&mut self) -> impl Future<Output = io::Result<Self::Connection>>;

    fn pause(&mut self, length: Duration) -> impl Future<Output = ()> {
        tokio::time::sleep(length)
    }
}

impl Listener for TcpListener {
    type Connection = TcpStream;

    async fn accept(&mut self) -> io::Result<TcpStream> {
        let (stream, _peer) = TcpListener::accept(self).await?;
        Ok(stream)
    }
}

/// How many accept errors in a row a server takes before it stops accepting. The flag
/// `--accept-tries` sets it, and it is 3 when the flag is not given.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AcceptTries(NonZeroU32);

impl AcceptTries {
    pub const fn new(tries: NonZeroU32) -> Self {
        Self(tries)
    }
}

impl Default for AcceptTries {
    fn default() -> Self {
        Self(NonZeroU32::MIN.saturating_add(2))
    }
}

impl FromStr for AcceptTries {
    type Err = ParseIntError;

    /// Zero is refused, because a server that takes no accept error would stop at the first one.
    fn from_str(text: &str) -> Result<Self, ParseIntError> {
        text.parse().map(Self)
    }
}

impl fmt::Display for AcceptTries {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{tries} accept errors in a row, the cap of --accept-tries")]
pub struct AcceptError {
    tries: AcceptTries,
    #[source]
    last: io::Error,
}

impl AcceptError {
    pub fn tries(&self) -> AcceptTries {
        self.tries
    }

    pub fn last(&self) -> &io::Error {
        &self.last
    }
}

/// Hands each accepted connection to `serve`, which must return at once: a server spawns a task
/// for it. An accept error usually means that the process is out of file descriptors, which passes
/// once one is freed, so the loop pauses and tries again. The pause doubles from 100 ms with each
/// error in a row, and one good accept sets the count back to 0. Returns only at the cap.
pub async fn each_connection<L: Listener>(
    listener: &mut L,
    tries: AcceptTries,
    mut serve: impl FnMut(L::Connection),
) -> AcceptError {
    let mut errors_in_a_row = 0_u32;
    let mut pause = FIRST_PAUSE;
    loop {
        match listener.accept().await {
            Ok(connection) => {
                errors_in_a_row = 0;
                pause = FIRST_PAUSE;
                serve(connection);
            }
            Err(last) => {
                errors_in_a_row += 1;
                if errors_in_a_row >= tries.0.get() {
                    return AcceptError { tries, last };
                }
                listener.pause(pause).await;
                pause = pause.saturating_mul(2);
            }
        }
    }
}
