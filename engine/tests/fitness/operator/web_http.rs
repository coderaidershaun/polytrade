//! These tests run the browser workstation's server as its own process, the way a browser tab and
//! Jet's proxy reach it. Only a separate process shows what the bound port, the listening line and
//! the exit status look like to a caller. Requests are written by hand, because a client library
//! would tidy away the very headers under test.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream, UdpSocket};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use crate::parquet_readback::TempDir;

/// A freshly built binary can sit in the operating system's malware scan for tens of seconds.
/// Every wait returns as soon as its answer arrives, so a fast machine never pays this.
pub(super) const DEADLINE: Duration = Duration::from_secs(60);

const INDEX: &[u8] = b"<!doctype html><title>workstation</title>";
const WASM: &[u8] = b"\0asm\x01\0\0\0";
const SCRIPT: &[u8] = b"console.log('workstation');";

const DESCRIPTOR: &str = r#"{"version":1,"name":"Polysim","ui":"./","embedding":"relative-paths"}"#;
const MCP_URL: &str = "http://127.0.0.1:8765/mcp";

/// Dropping this kills the server process, so a test that panics never leaves one running.
pub(super) struct WebServer {
    child: Child,
    pub(super) stderr: Receiver<String>,
}

pub(super) struct Exited {
    pub(super) status: ExitStatus,
    pub(super) stderr: String,
}

impl WebServer {
    pub(super) fn start(mut command: Command) -> WebServer {
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

    pub(super) fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Returns the port named in the listening line, which must be the first line on stderr.
    pub(super) fn wait_until_listening(&self) -> u16 {
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

    pub(super) fn wait_for_line_containing(&self, needle: &str) -> String {
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

    pub(super) fn wait_for_exit(&mut self) -> Exited {
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
pub(super) fn fake_engine() -> (UdpSocket, SocketAddr) {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("a free udp port");
    socket
        .set_read_timeout(Some(DEADLINE))
        .expect("a read timeout");
    let address = socket.local_addr().expect("a bound address");
    (socket, address)
}

/// A bundle shaped like Trunk's output: a page, a wasm module and a nested file.
pub(super) fn bundle(tag: &str) -> TempDir {
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

pub(super) fn command(bundle: &Path, engines: &[SocketAddr]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_polysim-web"));
    command
        .args(["--strategy", "web-test", "--port", "0", "--bundle"])
        .arg(bundle);
    for engine in engines {
        command.arg("--link").arg(engine.to_string());
    }
    command
}

pub(super) fn listening(command: Command) -> (WebServer, u16) {
    let server = WebServer::start(command);
    let port = server.wait_until_listening();
    (server, port)
}

pub(super) struct Reply {
    pub(super) status: u16,
    headers: Vec<(String, String)>,
    pub(super) body: Vec<u8>,
}

impl Reply {
    pub(super) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

pub(super) fn host(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

/// Sends exactly these headers plus `Connection: close`, so the whole answer ends at end of file.
pub(super) fn exchange(port: u16, method: &str, path: &str, headers: &[(&str, &str)]) -> Reply {
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

pub(super) const UPGRADE_HEADERS: [(&str, &str); 4] = [
    ("Connection", "Upgrade"),
    ("Upgrade", "websocket"),
    ("Sec-WebSocket-Version", "13"),
    ("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="),
];

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
