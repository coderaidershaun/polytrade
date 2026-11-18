//! These tests run the shared HTTP server as its own process, the way a desktop client and every
//! agent reply reach it. Only a separate process can test how the server exits, because a signal
//! handler belongs to the whole process.

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::process::{Command, Stdio};

use rustix::process::{Signal, kill_process};
use serde_json::json;

use crate::operator::fleet::mcp_support::{
    EngineTree, FRESH_SCRIPT_DEADLINE, IGNORE_TERM, MODE_OFF, Server, call, is_alive, open_session,
    pid_of, reply_of, started_pid,
};

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
