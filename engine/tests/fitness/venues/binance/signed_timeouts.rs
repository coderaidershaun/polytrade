//! The signed Binance client sends a request that acts on an order once, whatever its failure, and
//! tries a read again. The error of a signed request holds no signature, which a log would keep.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use engine::venues::binance::rest::{BinanceEnv, RestError, SignedRestClient, SignedRestConfig};
use engine::venues::edge::backoff::BackoffCaps;
use engine::venues::edge::http::TransportError;
use engine::venues::edge::rest_quiet::SharedRestQuiet;

const ORDER: &str = "pd-7ad60b6c-6a64f040000c0001";

/// A venue on this host that takes each request and keeps the first line of each. A silent venue
/// never answers. An answering venue answers each request with one fixed reply.
struct StandInVenue {
    port: u16,
    requests: Arc<Mutex<Vec<String>>>,
}

impl StandInVenue {
    fn silent() -> Self {
        Self::start(None)
    }

    fn answering(reply: &'static str) -> Self {
        Self::start(Some(reply))
    }

    fn start(reply: Option<&'static str>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("the test binds a loopback port");
        let port = listener.local_addr().expect("a bound address").port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        thread::spawn(move || {
            for stream in listener.incoming().map_while(Result::ok) {
                let seen = Arc::clone(&seen);
                thread::spawn(move || {
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_ok() {
                        seen.lock()
                            .expect("no reader panics while it holds the list")
                            .push(line.trim_end().to_owned());
                    }
                    let Some(reply) = reply else {
                        // The connection stays open with no answer until the client gives up on it.
                        reader.read_to_end(&mut Vec::new()).ok();
                        return;
                    };
                    let mut header = String::new();
                    while reader.read_line(&mut header).is_ok_and(|read| read > 2) {
                        header.clear();
                    }
                    reader
                        .get_mut()
                        .write_all(reply.as_bytes())
                        .expect("the answer is written");
                });
            }
        });
        Self { port, requests }
    }

    fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .expect("no reader panics while it holds the list")
            .clone()
    }
}

fn client(venue: &StandInVenue) -> SignedRestClient {
    SignedRestClient::on_loopback(
        venue.port,
        SignedRestConfig {
            env: BinanceEnv::Testnet,
            backoff: BackoffCaps {
                initial: Duration::from_millis(10),
                max: Duration::from_millis(10),
            },
            max_attempts: 2,
            ..SignedRestConfig::default()
        },
        SharedRestQuiet::new(),
    )
    .expect("the loopback client builds")
}

fn is_timeout(error: &RestError) -> bool {
    matches!(error, RestError::Transport(TransportError::Timeout { .. }))
}

/// CONTRACT: the signed client of the Binance edge and the venue meet at the network. A cancel
/// whose answer never comes may have run at the venue, so the client sends it once and gives the
/// timeout back, which the edge reads as an ambiguous outcome and settles with a read. A read
/// whose answer never comes is sent again, because reading twice changes nothing. Each request is
/// signed with a key that is no key, and reaches only a stand-in on this host.
#[test]
fn a_timed_out_order_action_is_sent_once() {
    let cancel_venue = StandInVenue::silent();
    let read_venue = StandInVenue::silent();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test builds a runtime");
    let (cancelled, read) = runtime.block_on(async {
        let mut cancelling = client(&cancel_venue);
        let mut reading = client(&read_venue);
        tokio::join!(
            cancelling.cancel_order("BTCUSDT", ORDER),
            reading.order_status("BTCUSDT", ORDER)
        )
    });

    let cancelled = cancelled.expect_err("no answer is no cancel");
    assert!(is_timeout(&cancelled), "{cancelled}");
    let cancels = cancel_venue.requests();
    assert_eq!(cancels.len(), 1, "the cancel was sent again: {cancels:?}");
    assert!(
        cancels[0].starts_with("DELETE /api/v3/order?"),
        "{cancels:?}"
    );

    let read = read.expect_err("no answer is no read");
    assert!(is_timeout(&read), "{read}");
    let reads = read_venue.requests();
    assert_eq!(reads.len(), 2, "a read is tried again: {reads:?}");
    assert!(
        reads
            .iter()
            .all(|request| request.starts_with("GET /api/v3/order?")),
        "{reads:?}"
    );
}

const KEY_REFUSED: &str = "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: 70\r\nConnection: close\r\n\r\n{\"code\":-2015,\"msg\":\"Invalid API-key, IP, or permissions for action.\"}";

/// A venue on this host that refuses the key of the one request it takes, as Binance did on the
/// first live run, and gives back the request line.
fn key_refusing_venue() -> (u16, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("the test binds a loopback port");
    let port = listener.local_addr().expect("a bound address").port();
    let request_line = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("the client connects");
        let mut reader = BufReader::new(stream);
        let mut request_line = String::new();
        reader
            .read_line(&mut request_line)
            .expect("the request line arrives");
        let mut header = String::new();
        while reader.read_line(&mut header).is_ok_and(|read| read > 2) {
            header.clear();
        }
        reader
            .get_mut()
            .write_all(KEY_REFUSED.as_bytes())
            .expect("the answer is written");
        request_line.trim_end().to_owned()
    });
    (port, request_line)
}

/// A port on this host that nothing listens on, so that each connection to it is refused.
fn closed_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("the test binds a loopback port");
    listener.local_addr().expect("a bound address").port()
}

/// The text of the error and of each error below it, and its debug form: each way a log line can
/// print it.
fn every_text_of(error: &RestError) -> String {
    let mut text = format!("{error:?}");
    let mut cause: Option<&dyn std::error::Error> = Some(error);
    while let Some(current) = cause {
        text.push('\n');
        text.push_str(&current.to_string());
        cause = current.source();
    }
    text
}

/// CONTRACT: the signed client of the Binance edge and the log meet at the error of a request. A
/// signed request carries its signature in its query string, and an error ends in a log line, so
/// the error names the method and the path of the request and nothing of its query. Each request
/// is signed with a key that is no key, and reaches only this host.
#[test]
fn the_error_of_a_signed_request_holds_no_signature() {
    let (refusing_port, request_line) = key_refusing_venue();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test builds a runtime");
    let (refused, unreachable) = runtime.block_on(async {
        let mut refused = SignedRestClient::on_loopback(
            refusing_port,
            SignedRestConfig::default(),
            SharedRestQuiet::new(),
        )
        .expect("the loopback client builds");
        let mut unreachable = SignedRestClient::on_loopback(
            closed_port(),
            SignedRestConfig {
                backoff: BackoffCaps {
                    initial: Duration::from_millis(10),
                    max: Duration::from_millis(10),
                },
                ..SignedRestConfig::default()
            },
            SharedRestQuiet::new(),
        )
        .expect("the loopback client builds");
        tokio::join!(refused.account(), unreachable.account())
    });

    let request_line = request_line.join().expect("the stand-in venue ran");
    assert!(
        request_line.starts_with("GET /api/v3/account?") && request_line.contains("signature="),
        "the request was not signed: {request_line}"
    );

    let refused = refused.expect_err("a refused key reads no account");
    assert!(
        matches!(
            refused,
            RestError::Unauthorized {
                status: 401,
                code: Some(-2015),
                ..
            }
        ),
        "{refused:?}"
    );
    let unreachable = unreachable.expect_err("a refused connection reads no account");
    assert!(
        matches!(
            unreachable,
            RestError::Transport(TransportError::Request { .. })
        ),
        "{unreachable:?}"
    );

    for error in [&refused, &unreachable] {
        let text = every_text_of(error);
        assert!(
            !text.contains("signature=") && !text.contains('?'),
            "the error holds the query of a signed request: {text}"
        );
        assert!(
            text.contains("/api/v3/account"),
            "the error names no path: {text}"
        );
    }
}

/// The answer of Binance when its backend timed out: the request can have run, or not.
const STATUS_UNKNOWN: &str = "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: 119\r\nConnection: close\r\n\r\n{\"code\":-1007,\"msg\":\"Timeout waiting for response from backend server. Send status unknown; execution status unknown.\"}";

fn is_status_unknown(error: &RestError) -> bool {
    matches!(
        error,
        RestError::Status {
            status: 503,
            code: Some(-1007),
            ..
        }
    )
}

/// The method and the path of each request line, so that a failed assertion prints no signature.
fn targets(requests: &[String]) -> Vec<&str> {
    requests
        .iter()
        .map(|line| line.split('?').next().unwrap_or_default())
        .collect()
}

/// CONTRACT: the signed client of the Binance edge and the venue meet at the network. When the
/// venue answers that the execution status of a request is unknown, a cancel can have run, so the
/// client sends it once and gives the answer back to the edge. A read with the same answer is sent
/// again, because reading twice changes nothing. Each request is signed with a key that is no key,
/// and reaches only a stand-in on this host.
#[test]
fn an_order_action_whose_outcome_is_unknown_is_sent_once() {
    let cancel_venue = StandInVenue::answering(STATUS_UNKNOWN);
    let read_venue = StandInVenue::answering(STATUS_UNKNOWN);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the test builds a runtime");
    let (cancelled, read) = runtime.block_on(async {
        let mut cancelling = client(&cancel_venue);
        let mut reading = client(&read_venue);
        tokio::join!(
            cancelling.cancel_order("BTCUSDT", ORDER),
            reading.order_status("BTCUSDT", ORDER)
        )
    });

    let cancelled = cancelled.expect_err("an unknown outcome is no cancel");
    assert!(is_status_unknown(&cancelled), "{cancelled}");
    let cancels = cancel_venue.requests();
    assert_eq!(
        targets(&cancels),
        ["DELETE /api/v3/order"],
        "the cancel goes out once"
    );

    let read = read.expect_err("an unknown outcome is no read");
    assert!(is_status_unknown(&read), "{read}");
    let reads = read_venue.requests();
    assert_eq!(
        targets(&reads),
        ["GET /api/v3/order", "GET /api/v3/order"],
        "a read is tried again"
    );
}
