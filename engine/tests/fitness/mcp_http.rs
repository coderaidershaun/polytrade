//! These tests run the shared HTTP server as its own process, the way a desktop client and every
//! agent reply reach it. Only a separate process can test how the server exits, because a signal
//! handler belongs to the whole process.

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::{RoleClient, ServiceExt};
use rustix::process::{Pid, Signal, kill_process, test_kill_process};
use serde_json::{Value, json};

use crate::mcp_support::{
    EngineTree, FRESH_SCRIPT_DEADLINE, IGNORE_TERM, MODE_OFF, TRAP_TERM, reply_of, text_of,
};

/// Dropping this kills the server process, so a test that panics before the server exits never
/// leaves one running.
struct Server {
    child: Child,
    stderr: Receiver<String>,
}

struct Exited {
    status: ExitStatus,
    stderr: String,
    stdout: String,
}

impl Server {
    fn spawn(tree: &EngineTree, port: u16) -> Server {
        let mut command = Server::command(tree);
        command
            .arg("--http")
            .arg(port.to_string())
            .stdin(Stdio::null());
        Server::start(command)
    }

    /// Returns a command that runs the server on the tree's engines, so a test can add its own
    /// transport arguments and stdin.
    fn command(tree: &EngineTree) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_polysim-mcp"));
        command
            .arg("--root")
            .arg(tree.root())
            .arg("--engines")
            .arg(tree.bin_dir());
        command
    }

    fn start(mut command: Command) -> Server {
        let mut child = command
            .stdout(Stdio::piped())
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
        Server {
            child,
            stderr: lines,
        }
    }

    /// Returns the port named in the listening line, which must be the first line on stderr.
    fn wait_until_listening(&self) -> u16 {
        let line = self
            .stderr
            .recv_timeout(FRESH_SCRIPT_DEADLINE)
            .expect("the server says it is listening in time");
        let port = line
            .rsplit_once(':')
            .and_then(|(_, rest)| rest.strip_suffix("/mcp"))
            .and_then(|port| port.parse::<u16>().ok())
            .filter(|port| *port != 0);
        let Some(port) = port else {
            panic!("the first stderr line is not the ready line: {line:?}");
        };
        assert_eq!(
            line,
            format!("polysim-mcp: listening on http://127.0.0.1:{port}/mcp"),
            "a client waits for exactly this line"
        );
        port
    }

    fn send_terminate(&self) {
        kill_process(pid_of(self.child.id()), Signal::TERM).expect("SIGTERM reaches the server");
    }

    fn terminate(&mut self) -> Exited {
        self.send_terminate();
        self.wait_for_exit()
    }

    fn wait_for_exit(&mut self) -> Exited {
        let deadline = Instant::now() + FRESH_SCRIPT_DEADLINE;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("the server can be waited on") {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "the server did not exit within {FRESH_SCRIPT_DEADLINE:?}"
            );
            thread::sleep(Duration::from_millis(20));
        };
        let mut stderr = Vec::new();
        loop {
            match self.stderr.recv_timeout(FRESH_SCRIPT_DEADLINE) {
                Ok(line) => stderr.push(line),
                Err(RecvTimeoutError::Disconnected) => break,
                Err(RecvTimeoutError::Timeout) => {
                    panic!("stderr stayed open after the server exited")
                }
            }
        }
        let mut stdout = String::new();
        self.child
            .stdout
            .take()
            .expect("stdout is piped")
            .read_to_string(&mut stdout)
            .expect("stdout is utf-8");
        Exited {
            status,
            stderr: stderr.join("\n"),
            stdout,
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if self.child.kill().is_ok() {
            self.child.wait().ok();
        }
    }
}

fn pid_of(raw: u32) -> Pid {
    i32::try_from(raw)
        .ok()
        .and_then(Pid::from_raw)
        .expect("a child's pid is a positive i32")
}

fn is_alive(pid: u32) -> bool {
    test_kill_process(pid_of(pid)).is_ok()
}

/// The operating system can hold a freshly written engine script in a malware scan, and a start
/// waits for that scan, so every call gets the same long deadline as the fixtures.
async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(FRESH_SCRIPT_DEADLINE, future)
        .await
        .expect("the server answered")
}

async fn open_session(port: u16) -> RunningService<RoleClient, ()> {
    let url = format!("http://127.0.0.1:{port}/mcp");
    within(().serve(StreamableHttpClientTransport::from_uri(url)))
        .await
        .expect("the client initializes over http")
}

async fn call(
    session: &RunningService<RoleClient, ()>,
    tool: &str,
    arguments: Value,
) -> CallToolResult {
    let arguments = arguments
        .as_object()
        .expect("arguments are an object")
        .clone();
    let params = CallToolRequestParams::new(tool.to_owned()).with_arguments(arguments);
    within(session.call_tool(params))
        .await
        .expect("a refusal is a tool result, never a protocol error")
}

fn started_pid(started: &Value) -> u32 {
    started["pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("pid is a u32")
}

fn shared_engine() -> Value {
    json!({"strategy_id": "http-shared", "te_id": "te-one"})
}

/// A page on any website can send this to a loopback port, and its browser adds the page's origin.
fn status_of_browser_request(port: u16) -> String {
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"page","version":"1"}}}"#;
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).expect("the server accepts");
    stream
        .set_read_timeout(Some(FRESH_SCRIPT_DEADLINE))
        .expect("a read timeout");
    write!(
        stream,
        "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nOrigin: http://example.com\r\n\
         Content-Type: application/json\r\nAccept: application/json, text/event-stream\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .expect("the request is sent");
    let mut status = String::new();
    BufReader::new(stream)
        .read_line(&mut status)
        .expect("the server answers");
    status
}

/// Opens a connection that sits idle between requests, the way a client's pooled connection waits
/// for its next one. Each request carries an `Origin` header, so the server refuses it at once with
/// a 403, which opens no session and leaves the connection open. Two answers on the same connection
/// prove that it stays open.
fn open_idle_connection(port: u16) -> BufReader<TcpStream> {
    let stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).expect("the server accepts");
    stream
        .set_read_timeout(Some(FRESH_SCRIPT_DEADLINE))
        .expect("a read timeout");
    let mut connection = BufReader::new(stream);
    for _ in 0..2 {
        write!(
            connection.get_mut(),
            "GET /mcp HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nOrigin: http://example.com\r\n\r\n"
        )
        .expect("the request is sent");
        read_response(&mut connection);
    }
    connection
}

/// Reads one whole response, so the next read sees only what the server sends after it.
fn read_response(connection: &mut BufReader<TcpStream>) {
    let mut body_length = 0;
    loop {
        let mut line = String::new();
        let read = connection.read_line(&mut line).expect("the server answers");
        assert_ne!(read, 0, "the server closed the connection mid-response");
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            body_length = value.trim().parse().expect("a numeric content length");
        }
    }
    let mut body = vec![0; body_length];
    connection
        .read_exact(&mut body)
        .expect("the whole body arrives");
}

/// The server exists so an engine started from one agent reply survives that reply and every later
/// session sees it. A server that stopped an engine with the session that started it, or gave each
/// session its own fleet, would still pass every in-process test.
#[tokio::test(flavor = "multi_thread")]
async fn engines_outlive_their_session_and_every_session_shares_them_until_sigterm() {
    let tree = EngineTree::new("mcp-http-shared");
    // No binary of this name is built beside the server, so it finds this one only through
    // `--engines`.
    tree.engine("http-shared", "te-one", MODE_OFF, TRAP_TERM);
    let mut server = Server::spawn(&tree, 0);
    let port = server.wait_until_listening();

    let first = open_session(port).await;
    let tools = within(first.list_all_tools()).await.expect("lists tools");
    let mut names: Vec<String> = tools.iter().map(|tool| tool.name.to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        [
            "list_strategies",
            "set_execution_mode",
            "start_strategy",
            "stop_strategy"
        ]
    );
    let started = reply_of(&call(&first, "start_strategy", shared_engine()).await);
    assert_eq!(started["mode"], "off");
    let pid = started_pid(&started);
    tree.wait_until_ready("http-shared-te-one");
    within(first.cancel())
        .await
        .expect("the first session closes");
    assert!(
        is_alive(pid),
        "closing the session that started an engine must not stop it"
    );

    let second = open_session(port).await;
    let again = call(&second, "start_strategy", shared_engine()).await;
    assert_eq!(again.is_error, Some(true), "{again:?}");
    assert!(
        text_of(&again).contains("already running"),
        "{}",
        text_of(&again)
    );
    let listing = reply_of(&call(&second, "list_strategies", json!({})).await);
    let row = &listing["engines"][0];
    assert_eq!(row["state"], "running");
    assert_eq!(row["pid"].as_u64(), Some(u64::from(pid)));

    let status = status_of_browser_request(port);
    assert!(
        status.starts_with("HTTP/1.1 403"),
        "a request from a browser page is refused: {status:?}"
    );

    let exited = server.terminate();
    drop(second);
    assert_eq!(exited.status.code(), Some(0), "{}", exited.stderr);
    assert!(
        exited
            .stderr
            .contains("polysim-mcp: stopped http-shared-te-one (exit code 0, forced false)"),
        "{}",
        exited.stderr
    );
    assert!(!is_alive(pid), "the engine is gone once the server exits");
    assert_eq!(exited.stdout, "", "stdout stays silent over http too");
}

/// A client must hear that the server is going away while the engines are still being stopped,
/// instead of waiting on a fleet that stays busy for the whole stop. Process exit closes every
/// socket anyway, so only the order of events shows that a signal closes the connections at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_signal_closes_every_open_connection_before_the_engines_are_stopped() {
    let tree = EngineTree::new("mcp-http-closes");
    // It ignores SIGTERM, so the server keeps stopping it until the test kills it.
    tree.engine("http-closes", "te-one", MODE_OFF, IGNORE_TERM);
    let mut server = Server::spawn(&tree, 0);
    let port = server.wait_until_listening();
    let session = open_session(port).await;
    let engine = json!({"strategy_id": "http-closes", "te_id": "te-one"});
    let pid = started_pid(&reply_of(&call(&session, "start_strategy", engine).await));
    tree.wait_until_ready("http-closes-te-one");
    let mut connection = open_idle_connection(port);

    server.send_terminate();
    let mut rest = Vec::new();
    connection
        .read_to_end(&mut rest)
        .expect("the server closes the connection");
    assert!(
        is_alive(pid),
        "the connection must close while the engine is still being stopped"
    );

    kill_process(pid_of(pid), Signal::KILL).expect("the engine can be killed");
    let exited = server.wait_for_exit();
    assert_eq!(exited.status.code(), Some(0), "{}", exited.stderr);
}

#[test]
fn a_taken_port_fails_the_server_with_a_bind_error() {
    let tree = EngineTree::new("mcp-http-taken");
    // The server resolves `--engines` before it binds, so the folder must exist for the bind to be
    // the step that fails.
    std::fs::create_dir_all(tree.bin_dir()).expect("mkdir bin");
    let taken = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("a free port");
    let port = taken.local_addr().expect("a bound address").port();
    let mut server = Server::spawn(&tree, port);
    let exited = server.wait_for_exit();
    assert_eq!(exited.status.code(), Some(1), "{}", exited.stderr);
    assert!(
        exited
            .stderr
            .contains(&format!("cannot bind 127.0.0.1:{port}")),
        "{}",
        exited.stderr
    );
    assert!(!exited.stderr.contains("listening on"), "{}", exited.stderr);
    assert_eq!(exited.stdout, "");
}

/// A parent that starts the server and then dies without stopping it, by a crash or a force quit,
/// must not leave the server and its engines running where the next parent cannot reach them.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_watching_stdin_stops_its_engines_and_exits_when_the_parent_closes_it() {
    let tree = EngineTree::new("mcp-http-parent");
    tree.engine("http-parent", "te-one", MODE_OFF, TRAP_TERM);
    let mut command = Server::command(&tree);
    command
        .args(["--http", "0", "--watch-stdin"])
        .stdin(Stdio::piped());
    let mut server = Server::start(command);
    let port = server.wait_until_listening();
    // Bytes on the pipe must not end the server. Only closing the pipe does.
    let parent = server.child.stdin.as_mut().expect("stdin is piped");
    parent.write_all(b"noise\n").expect("the parent writes");
    let session = open_session(port).await;
    let engine = json!({"strategy_id": "http-parent", "te_id": "te-one"});
    let pid = started_pid(&reply_of(&call(&session, "start_strategy", engine).await));
    tree.wait_until_ready("http-parent-te-one");

    drop(server.child.stdin.take());
    let exited = server.wait_for_exit();
    drop(session);
    assert_eq!(exited.status.code(), Some(0), "{}", exited.stderr);
    assert!(
        exited
            .stderr
            .contains("polysim-mcp: stopped http-parent-te-one (exit code 0, forced false)"),
        "{}",
        exited.stderr
    );
    assert!(!is_alive(pid), "the engine is gone once the server exits");
    assert_eq!(exited.stdout, "", "stdout stays silent over http too");
}

/// A server that can no longer watch its parent must say so and exit, not run on unwatched.
#[test]
fn a_server_that_cannot_read_its_stdin_exits_with_the_cause() {
    let tree = EngineTree::new("mcp-http-stdin-unreadable");
    // Every read from a directory fails, so a directory as stdin stands in for a pipe that breaks.
    let directory = File::open(tree.root()).expect("a directory opens for reading");
    // This skips `Server::command` because the tree has no engine folder, and the defaults let the
    // server get as far as reading stdin.
    let mut command = Command::new(env!("CARGO_BIN_EXE_polysim-mcp"));
    command
        .args(["--http", "0", "--watch-stdin"])
        .stdin(Stdio::from(directory));
    let exited = Server::start(command).wait_for_exit();
    assert_eq!(exited.status.code(), Some(1), "{}", exited.stderr);
    assert!(
        exited.stderr.contains("cannot read stdin"),
        "{}",
        exited.stderr
    );
    assert_eq!(exited.stdout, "");
}

#[test]
fn watching_stdin_without_http_is_refused_with_the_usage() {
    let tree = EngineTree::new("mcp-http-watch-stdio");
    let mut command = Server::command(&tree);
    command.arg("--watch-stdin").stdin(Stdio::null());
    let mut server = Server::start(command);
    let exited = server.wait_for_exit();
    assert_eq!(exited.status.code(), Some(1), "{}", exited.stderr);
    assert!(
        exited.stderr.contains("--watch-stdin needs --http"),
        "{}",
        exited.stderr
    );
    assert!(
        exited.stderr.contains("usage: polysim-mcp"),
        "{}",
        exited.stderr
    );
    assert!(!exited.stderr.contains("listening on"), "{}", exited.stderr);
    assert_eq!(exited.stdout, "");
}
