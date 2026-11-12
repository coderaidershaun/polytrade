//! These tests run the browser workstation's server as its own process, the way a browser tab and
//! Jet's proxy reach it. Only a separate process shows what the bound port, the listening line and
//! the exit status look like to a caller. The guard is the only thing between a web page and an
//! engine's unauthenticated run control, so it is pinned through real sockets with hand-written
//! requests, where a client library would tidy away the very headers under test.

use std::future::Future;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream, UdpSocket};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use polysim::link::{LINK_MAX_DATAGRAM, LinkHash};

use crate::parquet_readback::TempDir;

/// A freshly built binary can sit in the operating system's malware scan for tens of seconds.
/// Every wait returns as soon as its answer arrives, so a fast machine never pays this.
const DEADLINE: Duration = Duration::from_secs(60);

const INDEX: &[u8] = b"<!doctype html><title>workstation</title>";
const WASM: &[u8] = b"\0asm\x01\0\0\0";
const SCRIPT: &[u8] = b"console.log('workstation');";

const DESCRIPTOR: &str = r#"{"version":1,"name":"Polysim","ui":"./","embedding":"relative-paths"}"#;
const MCP_URL: &str = "http://127.0.0.1:8765/mcp";

/// Dropping this kills the server process, so a test that panics never leaves one running.
struct WebServer {
    child: Child,
    stderr: Receiver<String>,
}

struct Exited {
    status: ExitStatus,
    stderr: String,
}

impl WebServer {
    fn start(mut command: Command) -> WebServer {
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the server binary starts");
        let stderr = child.stderr.take().expect("stderr is piped");
        let (sender, lines) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    return;
                }
            }
        });
        WebServer {
            child,
            stderr: lines,
        }
    }

    /// Returns the port named in the listening line, which must be the first line on stderr.
    fn wait_until_listening(&self) -> u16 {
        let line = self
            .stderr
            .recv_timeout(DEADLINE)
            .expect("the server says it is listening in time");
        let port = line
            .strip_suffix('/')
            .and_then(|rest| rest.rsplit_once(':'))
            .and_then(|(_, port)| port.parse::<u16>().ok())
            .filter(|port| *port != 0);
        let Some(port) = port else {
            panic!("the first stderr line is not the ready line: {line:?}");
        };
        assert_eq!(
            line,
            format!("polysim-web: listening on http://127.0.0.1:{port}/"),
            "a client waits for exactly this line"
        );
        port
    }

    fn wait_for_line_containing(&self, needle: &str) -> String {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let line = self
                .stderr
                .recv_timeout(left)
                .unwrap_or_else(|_| panic!("no stderr line containing {needle:?} in time"));
            if line.contains(needle) {
                return line;
            }
        }
    }

    fn wait_for_exit(&mut self) -> Exited {
        let deadline = Instant::now() + DEADLINE;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("the server can be waited on") {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "the server did not exit within {DEADLINE:?}"
            );
            thread::sleep(Duration::from_millis(20));
        };
        let mut stderr = Vec::new();
        loop {
            match self.stderr.recv_timeout(DEADLINE) {
                Ok(line) => stderr.push(line),
                Err(RecvTimeoutError::Disconnected) => break,
                Err(RecvTimeoutError::Timeout) => panic!("stderr stayed open after the exit"),
            }
        }
        Exited {
            status,
            stderr: stderr.join("\n"),
        }
    }
}

impl Drop for WebServer {
    fn drop(&mut self) {
        if self.child.kill().is_ok() {
            self.child.wait().ok();
        }
    }
}

/// A UDP socket standing in for a trading engine's link port.
fn fake_engine() -> (UdpSocket, SocketAddr) {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a free udp port");
    socket
        .set_read_timeout(Some(DEADLINE))
        .expect("a read timeout");
    let address = socket.local_addr().expect("a bound address");
    (socket, address)
}

/// A bundle shaped like Trunk's output: a page, a wasm module and a nested file.
fn bundle(tag: &str) -> TempDir {
    let dir = TempDir::new(tag);
    let files = [
        ("index.html", INDEX),
        ("x.wasm", WASM),
        ("assets/app.js", SCRIPT),
    ];
    for (name, bytes) in files {
        let path = dir.path().join(name);
        std::fs::create_dir_all(path.parent().expect("a file has a folder")).expect("mkdir");
        std::fs::write(path, bytes).expect("write a bundle file");
    }
    dir
}

fn command(bundle: &Path, engines: &[SocketAddr]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_polysim-web"));
    command
        .args(["--strategy", "web-test", "--port", "0", "--bundle"])
        .arg(bundle);
    for engine in engines {
        command.arg("--link").arg(engine.to_string());
    }
    command
}

fn listening(command: Command) -> (WebServer, u16) {
    let server = WebServer::start(command);
    let port = server.wait_until_listening();
    (server, port)
}

struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Reply {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

fn host(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

/// Sends exactly these headers plus `Connection: close`, so the whole answer ends at end of file.
fn exchange(port: u16, method: &str, path: &str, headers: &[(&str, &str)]) -> Reply {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).expect("the server accepts");
    stream
        .set_read_timeout(Some(DEADLINE))
        .expect("a read timeout");
    let mut request = format!("{method} {path} HTTP/1.1\r\n");
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("Connection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .expect("the request is sent");
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .expect("the server answers and closes");
    parse_reply(&raw)
}

fn parse_reply(raw: &[u8]) -> Reply {
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap_or_else(|| panic!("no end of headers in {:?}", String::from_utf8_lossy(raw)));
    let head = std::str::from_utf8(&raw[..split]).expect("headers are text");
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in {head:?}"));
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_owned(), value.trim().to_owned()))
        .collect();
    Reply {
        status,
        headers,
        body: raw[split + 4..].to_vec(),
    }
}

fn get(port: u16, path: &str) -> Reply {
    exchange(port, "GET", path, &[("Host", &host(port))])
}

#[test]
fn the_descriptor_names_the_app_and_leaves_mcp_out_when_none_is_given() {
    let (_engine, engine_address) = fake_engine();
    let site = bundle("web-descriptor");
    let (_server, port) = listening(command(site.path(), &[engine_address]));

    let reply = get(port, "/jet-app.json");
    assert_eq!(reply.status, 200);
    assert_eq!(reply.header("content-type"), Some("application/json"));
    assert_eq!(reply.header("cache-control"), Some("no-store"));
    assert_eq!(reply.body, DESCRIPTOR.as_bytes());

    let head = exchange(port, "HEAD", "/jet-app.json", &[("Host", &host(port))]);
    assert_eq!(head.status, 200);
    assert_eq!(head.header("content-type"), Some("application/json"));
    assert_eq!(
        head.header("content-length"),
        Some(DESCRIPTOR.len().to_string().as_str())
    );
    assert!(head.body.is_empty(), "a HEAD answer carries no body");
}

#[test]
fn the_descriptor_advertises_the_mcp_server_when_one_is_given() {
    let (_engine, engine_address) = fake_engine();
    let site = bundle("web-descriptor-mcp");
    let mut with_mcp = command(site.path(), &[engine_address]);
    with_mcp.args(["--mcp", MCP_URL]);
    let (_server, port) = listening(with_mcp);

    let reply = get(port, "/jet-app.json");
    assert_eq!(reply.status, 200);
    assert_eq!(
        String::from_utf8(reply.body).expect("the descriptor is text"),
        format!(
            r#"{{"version":1,"name":"Polysim","ui":"./","embedding":"relative-paths","mcp":"{MCP_URL}"}}"#
        )
    );
}

#[test]
fn the_bundle_is_served_from_memory_with_its_own_types_and_nothing_outside_it() {
    let (_engine, engine_address) = fake_engine();
    let site = bundle("web-files");
    let (_server, port) = listening(command(site.path(), &[engine_address]));

    let head = exchange(port, "HEAD", "/", &[("Host", &host(port))]);
    assert_eq!(head.status, 200);
    assert!(head.body.is_empty(), "a HEAD answer carries no body");
    assert_eq!(
        head.header("content-length"),
        Some(INDEX.len().to_string().as_str())
    );

    let index = get(port, "/");
    assert_eq!(index.status, 200);
    assert_eq!(index.body, INDEX);
    assert_eq!(index.header("cache-control"), Some("no-store"));
    assert_eq!(
        index.header("content-type"),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(get(port, "/index.html").body, INDEX);

    let wasm = get(port, "/x.wasm");
    assert_eq!(wasm.status, 200);
    assert_eq!(wasm.header("content-type"), Some("application/wasm"));
    assert_eq!(wasm.body, WASM);

    let nested = get(port, "/assets/app.js");
    assert_eq!(nested.status, 200);
    assert_eq!(
        nested.header("content-type"),
        Some("text/javascript; charset=utf-8")
    );
    assert_eq!(nested.body, SCRIPT);

    for missing in [
        "/missing",
        "/assets/",
        "/../Cargo.toml",
        "/%2e%2e/Cargo.toml",
    ] {
        let reply = get(port, missing);
        assert_eq!(reply.status, 404, "{missing} is not in the bundle");
        assert_eq!(reply.header("cache-control"), Some("no-store"));
    }

    let post = exchange(port, "POST", "/", &[("Host", &host(port))]);
    assert_eq!(post.status, 405);
    assert_eq!(post.header("allow"), Some("GET, HEAD"));
}

#[test]
fn a_missing_bundle_stops_the_server_with_the_trunk_hint() {
    let (_engine, engine_address) = fake_engine();
    let empty = TempDir::new("web-no-bundle");
    let never_built = empty.path().join("never-built");
    for dir in [empty.path(), never_built.as_path()] {
        let exited = WebServer::start(command(dir, &[engine_address])).wait_for_exit();
        assert_eq!(exited.status.code(), Some(1), "{}", exited.stderr);
        assert!(
            exited.stderr.contains(&format!(
                "no workstation bundle at {} - run `trunk build` in browser/ first",
                dir.display()
            )),
            "{}",
            exited.stderr
        );
        assert!(!exited.stderr.contains("listening on"), "{}", exited.stderr);
    }
}

const UPGRADE_HEADERS: [(&str, &str); 4] = [
    ("Connection", "Upgrade"),
    ("Upgrade", "websocket"),
    ("Sec-WebSocket-Version", "13"),
    ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
];

struct Refused<'a> {
    what: &'a str,
    path: &'a str,
    headers: Vec<(&'a str, &'a str)>,
    status: u16,
}

/// A page on any website can open a socket to a loopback port, and the browser adds that page's
/// origin, so these are the shapes that must never reach a route.
#[test]
fn a_request_that_is_not_from_this_machines_own_page_is_refused_before_any_route() {
    let (_engine, engine_address) = fake_engine();
    let site = bundle("web-guard");
    let (_server, port) = listening(command(site.path(), &[engine_address]));
    let own = host(port);
    let other_port = host(port.wrapping_add(1));
    let rebinding = format!("evil.example:{port}");
    let own_origin = format!("http://{own}");
    let other_origin = format!("http://{other_port}");
    let descriptor = |what, headers, status| Refused {
        what,
        path: "/jet-app.json",
        headers,
        status,
    };

    let mut upgrade_from_foreign_page = vec![("Host", own.as_str())];
    upgrade_from_foreign_page.extend(UPGRADE_HEADERS);
    upgrade_from_foreign_page.push(("Origin", "http://evil.example"));

    let cases = [
        descriptor(
            "a foreign origin",
            vec![("Host", &own), ("Origin", "http://evil.example")],
            403,
        ),
        descriptor(
            "an origin of null",
            vec![("Host", &own), ("Origin", "null")],
            403,
        ),
        descriptor(
            "an origin on another port of this machine",
            vec![("Host", &own), ("Origin", &other_origin)],
            403,
        ),
        descriptor(
            "two origins",
            vec![
                ("Host", &own),
                ("Origin", &own_origin),
                ("Origin", &own_origin),
            ],
            403,
        ),
        descriptor(
            "a host that resolves to loopback by rebinding",
            vec![("Host", &rebinding)],
            403,
        ),
        descriptor(
            "a loopback host on another port",
            vec![("Host", &other_port)],
            403,
        ),
        descriptor(
            "a loopback host with no port",
            vec![("Host", "127.0.0.1")],
            403,
        ),
        descriptor("no host", vec![], 400),
        descriptor("two hosts", vec![("Host", &own), ("Host", &own)], 400),
        Refused {
            what: "a rebinding host on a file the bundle holds",
            path: "/index.html",
            headers: vec![("Host", &rebinding)],
            status: 403,
        },
        Refused {
            what: "a websocket upgrade from a foreign page",
            path: "/api/link?peer=0",
            headers: upgrade_from_foreign_page,
            status: 403,
        },
    ];
    for case in cases {
        let reply = exchange(port, "GET", case.path, &case.headers);
        assert_eq!(
            reply.status, case.status,
            "{} must get {}",
            case.what, case.status
        );
        assert_eq!(
            reply.header("cache-control"),
            Some("no-store"),
            "{}",
            case.what
        );
    }
}

/// A frame load sends no `Origin`, so the guard passes a page that another site frames. That site
/// could then lay a decoy over START or STOP and steer the operator's click onto it. Jet frames the
/// page from its own origin, through its proxy, so a same-origin parent is the only one needed.
#[test]
fn only_a_page_of_the_same_origin_may_frame_the_workstation() {
    let (_engine, engine_address) = fake_engine();
    let site = bundle("web-framing");
    let (_server, port) = listening(command(site.path(), &[engine_address]));

    for path in ["/", "/index.html"] {
        let page = get(port, path);
        assert_eq!(page.status, 200, "{path}");
        assert_eq!(
            page.header("content-security-policy"),
            Some("frame-ancestors 'self'"),
            "{path} must refuse a frame on another site"
        );
    }
}

#[test]
fn every_loopback_name_and_this_pages_own_origin_pass_the_guard() {
    let (_engine, engine_address) = fake_engine();
    let site = bundle("web-guard-accepts");
    let (_server, port) = listening(command(site.path(), &[engine_address]));

    for authority in [
        host(port),
        format!("localhost:{port}"),
        format!("LocalHost:{port}"),
        format!("[::1]:{port}"),
    ] {
        let own_origin = format!("http://{authority}");
        let without_origin = exchange(port, "GET", "/jet-app.json", &[("Host", &authority)]);
        assert_eq!(without_origin.status, 200, "{authority} without an origin");
        let with_origin = exchange(
            port,
            "GET",
            "/jet-app.json",
            &[("Host", &authority), ("Origin", &own_origin)],
        );
        assert_eq!(with_origin.status, 200, "{authority} from its own page");
    }
}

#[test]
fn a_peer_the_server_does_not_relay_to_is_not_found_before_any_upgrade() {
    let (_engine, engine_address) = fake_engine();
    let site = bundle("web-peer");
    let (_server, port) = listening(command(site.path(), &[engine_address]));
    let own = host(port);
    let mut upgrade = vec![("Host", own.as_str())];
    upgrade.extend(UPGRADE_HEADERS);

    for path in [
        "/api/link?peer=1",
        "/api/link?peer=99999999999999999999",
        "/api/link?peer=-1",
        "/api/link?peer=x",
        "/api/link?peer=",
        "/api/link",
    ] {
        let reply = exchange(port, "GET", path, &upgrade);
        assert_eq!(reply.status, 404, "{path} names no engine");
    }

    let plain_get = exchange(port, "GET", "/api/link?peer=0", &[("Host", &own)]);
    assert_eq!(plain_get.status, 400, "a request that is not an upgrade");
    let post = exchange(port, "POST", "/api/link?peer=0", &upgrade);
    assert_eq!(post.status, 405);
    assert_eq!(post.header("allow"), Some("GET"));
}

type Tab = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, future)
        .await
        .expect("an answer arrives in time")
}

/// Connects the way a browser tab does, with the origin of the page the server served.
async fn open_tab(port: u16, peer: usize) -> Tab {
    let mut request = format!("ws://127.0.0.1:{port}/api/link?peer={peer}")
        .into_client_request()
        .expect("a valid url");
    let origin = HeaderValue::from_str(&format!("http://127.0.0.1:{port}")).expect("a header");
    request.headers_mut().insert("Origin", origin);
    let (tab, response) = within(connect_async(request))
        .await
        .expect("the relay accepts the upgrade");
    assert_eq!(response.status(), 101);
    tab
}

async fn next_message(tab: &mut Tab) -> Message {
    within(tab.next())
        .await
        .expect("the relay keeps the socket open")
        .expect("a well-formed frame")
}

async fn expect_hello(tab: &mut Tab) -> serde_json::Value {
    let Message::Text(text) = next_message(tab).await else {
        panic!("the first message is the hello, as text");
    };
    serde_json::from_str(text.as_str()).expect("the hello is json")
}

fn bytes_of_length(length: usize) -> Vec<u8> {
    (0..=u8::MAX).cycle().take(length).collect()
}

fn receive(socket: &UdpSocket) -> (Vec<u8>, SocketAddr) {
    let mut buffer = [0_u8; 4096];
    let (length, from) = socket.recv_from(&mut buffer).expect("a datagram arrives");
    (buffer[..length].to_vec(), from)
}

fn expect_silence(socket: &UdpSocket) {
    socket
        .set_read_timeout(Some(Duration::from_millis(300)))
        .expect("a read timeout");
    let mut buffer = [0_u8; 4096];
    assert!(
        socket.recv_from(&mut buffer).is_err(),
        "no datagram should have arrived"
    );
}

const TOKEN: &str = "never-printed-token";

#[tokio::test(flavor = "multi_thread")]
async fn a_tab_reads_the_hello_first_and_datagrams_cross_untouched_both_ways() {
    let (first_engine, first_address) = fake_engine();
    let (second_engine, second_address) = fake_engine();
    let site = bundle("web-relay");
    let mut serve = command(site.path(), &[first_address, second_address]);
    serve.args(["--token", TOKEN]);
    let (server, port) = listening(serve);
    let mut tab = open_tab(port, 1).await;

    let hello = expect_hello(&mut tab).await;
    assert_eq!(
        hello,
        json!({
            "strategy": "web-test",
            "peers": [first_address.to_string(), second_address.to_string()],
            "tokenHash": LinkHash::of_name(TOKEN).0,
        })
    );

    let outbound = bytes_of_length(LINK_MAX_DATAGRAM);
    within(tab.send(Message::binary(outbound.clone())))
        .await
        .expect("the frame is sent");
    let (arrived, relay_source) = receive(&second_engine);
    assert_eq!(arrived, outbound, "a full-size datagram crosses untouched");
    expect_silence(&first_engine);

    within(tab.send(Message::text("not a datagram")))
        .await
        .expect("the frame is sent");

    let inbound = bytes_of_length(LINK_MAX_DATAGRAM);
    second_engine
        .send_to(&inbound, relay_source)
        .expect("the engine replies to the relay");
    assert_eq!(
        next_message(&mut tab).await,
        Message::binary(inbound),
        "a full-size datagram reaches the tab untouched"
    );

    let stranger = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a free udp port");
    stranger
        .send_to(b"spoof", relay_source)
        .expect("the stranger writes to the relay");
    second_engine
        .send_to(&bytes_of_length(LINK_MAX_DATAGRAM + 1), relay_source)
        .expect("the engine sends too much");
    second_engine
        .send_to(&[9, 9, 9, 9, 9], relay_source)
        .expect("the engine sends a short one");
    assert_eq!(
        next_message(&mut tab).await,
        Message::binary(vec![9_u8; 5]),
        "only the engine's datagrams, and only link-sized ones, reach the tab"
    );

    within(tab.close(None)).await.expect("the tab closes");
    let closed = server.wait_for_line_containing("closed");
    assert_eq!(
        closed,
        format!(
            "polysim-web: relay {second_address} closed (forwarded 1 to engine, 2 to browser, \
             refused 2)"
        )
    );
    assert!(!closed.contains(TOKEN), "the token is never printed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_longer_than_any_datagram_closes_the_tab_and_reaches_no_engine() {
    let (engine, engine_address) = fake_engine();
    let site = bundle("web-oversize");
    let (server, port) = listening(command(site.path(), &[engine_address]));
    let mut tab = open_tab(port, 0).await;
    expect_hello(&mut tab).await;

    // The send itself can fail once the relay has dropped the socket, so only the outcome counts.
    tab.send(Message::binary(vec![7_u8; LINK_MAX_DATAGRAM + 1]))
        .await
        .ok();
    let outcome = within(tab.next()).await;
    assert!(
        matches!(outcome, None | Some(Err(_) | Ok(Message::Close(_)))),
        "the relay answered an oversized frame with {outcome:?}"
    );
    expect_silence(&engine);
    let closed = server.wait_for_line_containing("closed");
    assert!(
        closed.contains("closed (forwarded 0 to engine, 0 to browser, refused 0): "),
        "an abnormal end names its cause on the same line: {closed}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_down_engine_does_not_end_the_relay_and_the_tab_reaches_it_once_it_is_back() {
    let (engine, engine_address) = fake_engine();
    drop(engine);
    let site = bundle("web-engine-down");
    let (server, port) = listening(command(site.path(), &[engine_address]));
    let mut tab = open_tab(port, 0).await;
    expect_hello(&mut tab).await;

    for _ in 0..3 {
        within(tab.send(Message::binary(vec![1_u8, 2, 3])))
            .await
            .expect("the relay keeps the tab while the engine is down");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let revived = UdpSocket::bind(engine_address).expect("the engine's port is free again");
    revived
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("a read timeout");
    let mut buffer = [0_u8; 64];
    let deadline = Instant::now() + DEADLINE;
    loop {
        within(tab.send(Message::binary(b"after".to_vec())))
            .await
            .expect("the relay still holds the tab");
        if let Ok((length, _)) = revived.recv_from(&mut buffer) {
            assert_eq!(&buffer[..length], b"after");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the revived engine never heard from the tab"
        );
    }

    drop(tab);
    let closed = server.wait_for_line_containing("closed");
    assert!(
        closed.ends_with(')'),
        "a tab that vanishes without a close frame is a quiet end: {closed}"
    );
}
