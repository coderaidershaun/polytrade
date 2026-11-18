//! The mcp suites share these fixtures: an engine tree on disk, shell scripts that stand in for
//! engine binaries, deadlines short enough for a test, the checks that read a tool's reply, and the
//! server run as its own process with a client session over loopback.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::{RoleClient, ServiceExt};
use rustix::process::{Pid, Signal, kill_process, test_kill_process};
use serde_json::Value;

use engine::operator::fleet::{Fleet, FleetTiming};
use engine::runtime::IdentityLocks;
use engine::runtime::config::RunIdentity;

use crate::parquet_readback::TempDir;

pub const FAST: FleetTiming = FleetTiming {
    start_grace: Duration::from_millis(500),
    stop_deadline: Duration::from_secs(1),
};

/// A freshly written script can sit in the operating system's malware scan for tens of seconds
/// under load. Every wait on one returns as soon as it answers, so a fast machine never pays this.
pub const FRESH_SCRIPT_DEADLINE: Duration = Duration::from_secs(60);

/// Every long-running script ends on its own after about thirty seconds, so a failed assertion
/// never leaves an endless loop behind on the machine. Each one prints `ready` once its trap is
/// set, because the first run of a freshly written script can take longer than the start grace,
/// and a signal that lands before the trap kills the shell outright.
macro_rules! bounded_script {
    ($prelude:literal) => {
        concat!(
            "#!/bin/sh\n",
            $prelude,
            "echo ready\n",
            "i=0; while [ $i -lt 600 ]; do sleep 0.05; i=$((i+1)); done\n"
        )
    };
}

pub const TRAP_TERM: &str = bounded_script!("trap 'exit 0' TERM\n");
pub const IGNORE_TERM: &str = bounded_script!("trap '' TERM\n");
pub const MARKERS: &str =
    bounded_script!("echo stdout-marker\necho stderr-marker >&2\ntrap 'exit 0' TERM\n");
pub const EXITS_THREE: &str = "#!/bin/sh\necho \"config refused\" >&2\nexit 3\n";

pub const LINK_BIND: &str = "127.0.0.1:9399";
pub const MODE_OFF: &str =
    "sources:\n  - execution:\n      mode: off\nlink:\n  bind: \"127.0.0.1:9399\"\n";
pub const MODE_SIM: &str = "sources:\n  - execution:\n      mode: sim\n";
pub const NO_EXECUTION: &str = "engine:\n  spin_interval_us: 1000\n";
pub const UNPARSEABLE: &str = "sources:\n  - execution: [mode: off\n";

pub struct EngineTree {
    dir: TempDir,
}

impl EngineTree {
    pub fn new(tag: &str) -> EngineTree {
        EngineTree {
            dir: TempDir::new(tag),
        }
    }

    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    pub fn bin_dir(&self) -> PathBuf {
        self.root().join("bin")
    }

    pub fn config(&self, strategy: &str, te: &str, text: &str) -> &EngineTree {
        let path = self.config_path(strategy, te);
        std::fs::create_dir_all(path.parent().expect("config has a folder")).expect("mkdir");
        std::fs::write(&path, text).expect("write config");
        self
    }

    pub fn script(&self, strategy: &str, te: &str, body: &str) -> &EngineTree {
        let dir = self.bin_dir();
        std::fs::create_dir_all(&dir).expect("mkdir bin");
        let path = dir.join(format!("{strategy}-{te}"));
        std::fs::write(&path, body).expect("write script");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        self
    }

    pub fn engine(&self, strategy: &str, te: &str, config: &str, body: &str) -> &EngineTree {
        self.config(strategy, te, config).script(strategy, te, body)
    }

    pub fn config_path(&self, strategy: &str, te: &str) -> PathBuf {
        self.root()
            .join("strategies")
            .join(strategy)
            .join(te)
            .join("config.yaml")
    }

    pub fn console_log(&self, stem: &str) -> PathBuf {
        self.root().join("logs").join(format!("{stem}-console.log"))
    }

    /// Returns once the script has set its signal trap, and fails the test if it never does.
    pub fn wait_until_ready(&self, stem: &str) {
        let log = self.console_log(stem);
        let text = wait_for_file_containing(&log, &["ready"]);
        assert!(
            text.contains("ready"),
            "{stem} never set its trap: {text:?}"
        );
    }

    /// The identity locks of the tree, in a folder of its own, so that two tests never share one.
    pub fn locks(&self) -> IdentityLocks {
        IdentityLocks::in_folder(self.root().join("locks"))
    }

    pub fn fleet(&self, timing: FleetTiming) -> Fleet {
        Fleet::new(self.root().to_path_buf(), self.bin_dir(), timing)
            .with_identity_locks(self.locks())
    }
}

pub fn identity(strategy: &str, te: &str) -> RunIdentity {
    RunIdentity::new(strategy, te).expect("a valid identity")
}

/// Polls rather than trusting the start grace, so a loaded parallel run cannot turn a slow child
/// into a false failure.
pub fn wait_for_file_containing(path: &Path, needles: &[&str]) -> String {
    let deadline = Instant::now() + FRESH_SCRIPT_DEADLINE;
    loop {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        if needles.iter().all(|needle| text.contains(needle)) || Instant::now() >= deadline {
            return text;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

pub fn text_of(result: &CallToolResult) -> &str {
    result
        .content
        .first()
        .and_then(|block| block.as_text())
        .map(|text| text.text.as_str())
        .expect("every reply carries a text block")
}

/// A success must carry the same JSON object as structured content and as its first text block.
pub fn reply_of(result: &CallToolResult) -> Value {
    assert_eq!(result.is_error, Some(false), "{}", text_of(result));
    let structured = result
        .structured_content
        .clone()
        .expect("a success carries structured content");
    let text: Value = serde_json::from_str(text_of(result)).expect("the text block is JSON");
    assert_eq!(
        structured, text,
        "the text block mirrors the structured content"
    );
    structured
}

/// Dropping this kills the server process, so a test that panics before the server exits never
/// leaves one running.
pub struct Server {
    pub child: Child,
    pub stderr: Receiver<String>,
}

pub struct Exited {
    pub status: ExitStatus,
    pub stderr: String,
    pub stdout: String,
}

impl Server {
    pub fn spawn(tree: &EngineTree, port: u16) -> Server {
        let mut command = Server::command(tree);
        command
            .arg("--http")
            .arg(port.to_string())
            .stdin(Stdio::null());
        Server::start(command)
    }

    /// Returns a command that runs the server on the tree's engines, so a test can add its own
    /// transport arguments and stdin.
    pub fn command(tree: &EngineTree) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_polysim-mcp"));
        command
            .arg("--root")
            .arg(tree.root())
            .arg("--engines")
            .arg(tree.bin_dir());
        command
    }

    pub fn start(mut command: Command) -> Server {
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
    pub fn wait_until_listening(&self) -> u16 {
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

    pub fn send_terminate(&self) {
        kill_process(pid_of(self.child.id()), Signal::TERM).expect("SIGTERM reaches the server");
    }

    pub fn terminate(&mut self) -> Exited {
        self.send_terminate();
        self.wait_for_exit()
    }

    pub fn wait_for_exit(&mut self) -> Exited {
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

pub fn pid_of(raw: u32) -> Pid {
    i32::try_from(raw)
        .ok()
        .and_then(Pid::from_raw)
        .expect("a child's pid is a positive i32")
}

pub fn is_alive(pid: u32) -> bool {
    test_kill_process(pid_of(pid)).is_ok()
}

/// The operating system can hold a freshly written engine script in a malware scan, and a start
/// waits for that scan, so every call gets the same long deadline as the fixtures.
pub async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(FRESH_SCRIPT_DEADLINE, future)
        .await
        .expect("the server answered")
}

pub async fn open_session(port: u16) -> RunningService<RoleClient, ()> {
    let url = format!("http://127.0.0.1:{port}/mcp");
    within(().serve(StreamableHttpClientTransport::from_uri(url)))
        .await
        .expect("the client initializes over http")
}

pub async fn call(
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

pub fn started_pid(started: &Value) -> u32 {
    started["pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("pid is a u32")
}

/// A page on any website can send this to a loopback port, and its browser adds the page's origin.
pub fn status_of_browser_request(port: u16) -> String {
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
