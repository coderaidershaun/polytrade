//! Contract test of the one accept loop of the relay and the fleet server: errors past the cap end
//! the loop, and each server then does what its own failure class says.

use std::collections::VecDeque;
use std::io;
use std::net::{Ipv4Addr, TcpStream};
use std::process::Command;
use std::time::Duration;

use engine::operator::accept::{self, AcceptTries, Listener};

use super::web_http::{WebServer, bundle, fake_engine, listening};

/// The code of the error that an accept gives when the process is out of file descriptors.
const OUT_OF_FILES: i32 = 24;

/// A listener that gives the accept results of its script in turn, and records each pause in
/// place of sleeping it.
struct ScriptedListener {
    script: VecDeque<io::Result<u32>>,
    pauses: Vec<Duration>,
}

impl Listener for ScriptedListener {
    type Connection = u32;

    async fn accept(&mut self) -> io::Result<u32> {
        self.script
            .pop_front()
            .expect("the loop stops at the cap and never tries again")
    }

    async fn pause(&mut self, length: Duration) {
        self.pauses.push(length);
    }
}

/// Low enough that a few held connections use up every file the server may open, so its accept
/// fails the way it does when a real server runs out of file descriptors.
const OPEN_FILE_LIMIT: u32 = 16;
const HELD_CONNECTIONS: usize = 32;

/// The server runs under a lower limit of open files than the test process, through the shell
/// that sets the limit and then becomes the server.
fn under_a_low_file_limit(binary: &str) -> Command {
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg(format!("ulimit -n {OPEN_FILE_LIMIT} && exec \"$0\" \"$@\""))
        .arg(binary);
    command
}

/// Each connection that the server accepts holds one of its files until the test drops it.
fn hold_connections(port: u16) -> Vec<TcpStream> {
    (0..HELD_CONNECTIONS)
        .filter_map(|_| TcpStream::connect((Ipv4Addr::LOCALHOST, port)).ok())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn accept_errors_past_cap_give_the_class_of_the_caller() {
    the_loop_stops_at_the_cap_and_a_good_accept_sets_the_count_to_0().await;
    the_relay_exits_with_a_code_that_is_not_0();
    #[cfg(feature = "mcp")]
    fleet::the_fleet_server_takes_no_new_session_and_keeps_its_engines().await;
}

async fn the_loop_stops_at_the_cap_and_a_good_accept_sets_the_count_to_0() {
    let refused = || Err(io::Error::from_raw_os_error(OUT_OF_FILES));
    let mut listener = ScriptedListener {
        script: VecDeque::from([refused(), refused(), Ok(1), refused(), refused(), refused()]),
        pauses: Vec::new(),
    };
    let mut served = Vec::new();
    let error = accept::each_connection(&mut listener, AcceptTries::default(), |connection| {
        served.push(connection);
    })
    .await;

    assert_eq!(served, [1], "the good accept between the errors is served");
    let pauses = [100, 200, 100, 200].map(Duration::from_millis);
    assert_eq!(
        listener.pauses, pauses,
        "the pause doubles, and the good accept starts it again"
    );
    assert!(
        listener.script.is_empty(),
        "every scripted accept was tried"
    );
    assert_eq!(error.tries(), AcceptTries::default());
    assert_eq!(error.last().raw_os_error(), Some(OUT_OF_FILES));
    assert_eq!(
        error.to_string(),
        "3 accept errors in a row, the cap of --accept-tries"
    );
}

fn the_relay_exits_with_a_code_that_is_not_0() {
    let (_engine, engine_address) = fake_engine();
    let site = bundle("accept-cap-relay");
    let mut serve = under_a_low_file_limit(env!("CARGO_BIN_EXE_polysim-web"));
    serve
        .args(["--strategy", "web-test", "--port", "0", "--bundle"])
        .arg(site.path())
        .arg("--link")
        .arg(engine_address.to_string());
    let (mut server, port) = listening(serve);

    let _held = hold_connections(port);
    let exited = server.wait_for_exit();
    assert_eq!(exited.status.code(), Some(1), "{}", exited.stderr);
    assert!(
        exited.stderr.contains("3 accept errors in a row"),
        "{}",
        exited.stderr
    );
}

#[cfg(feature = "mcp")]
mod fleet {
    use std::net::{Ipv4Addr, TcpStream};

    use rmcp::model::CallToolRequestParams;
    use rmcp::service::RunningService;
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::{RoleClient, ServiceExt};
    use rustix::process::{Pid, Signal, kill_process, test_kill_process};
    use serde_json::json;

    use super::{WebServer, hold_connections, under_a_low_file_limit};
    use crate::operator::fleet::mcp_support::{
        EngineTree, FRESH_SCRIPT_DEADLINE, MODE_OFF, TRAP_TERM, reply_of,
    };

    fn pid_of(raw: u32) -> Pid {
        i32::try_from(raw)
            .ok()
            .and_then(Pid::from_raw)
            .expect("a child's pid is a positive i32")
    }

    fn is_alive(pid: u32) -> bool {
        test_kill_process(pid_of(pid)).is_ok()
    }

    async fn open_session(port: u16) -> RunningService<RoleClient, ()> {
        let url = format!("http://127.0.0.1:{port}/mcp");
        tokio::time::timeout(
            FRESH_SCRIPT_DEADLINE,
            ().serve(StreamableHttpClientTransport::from_uri(url)),
        )
        .await
        .expect("the server answered")
        .expect("the client initializes over http")
    }

    async fn start_engine(session: &RunningService<RoleClient, ()>) -> u32 {
        let arguments = json!({"strategy_id": "accept-cap", "te_id": "te-one"})
            .as_object()
            .expect("arguments are an object")
            .clone();
        let params = CallToolRequestParams::new("start_strategy").with_arguments(arguments);
        let result = tokio::time::timeout(FRESH_SCRIPT_DEADLINE, session.call_tool(params))
            .await
            .expect("the server answered")
            .expect("a start is a tool result");
        reply_of(&result)["pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok())
            .expect("pid is a u32")
    }

    fn port_of(listening: &str) -> u16 {
        listening
            .rsplit_once(':')
            .and_then(|(_, rest)| rest.strip_suffix("/mcp"))
            .and_then(|port| port.parse().ok())
            .unwrap_or_else(|| panic!("no port in {listening:?}"))
    }

    pub(super) async fn the_fleet_server_takes_no_new_session_and_keeps_its_engines() {
        let tree = EngineTree::new("accept-cap-fleet");
        tree.engine("accept-cap", "te-one", MODE_OFF, TRAP_TERM);
        let mut serve = under_a_low_file_limit(env!("CARGO_BIN_EXE_polysim-mcp"));
        serve
            .arg("--root")
            .arg(tree.root())
            .arg("--engines")
            .arg(tree.bin_dir())
            .args(["--http", "0", "--accept-tries", "3"]);
        let mut server = WebServer::start(serve);
        let port = port_of(&server.wait_for_line_containing("listening on"));

        let session = open_session(port).await;
        let pid = start_engine(&session).await;
        tree.wait_until_ready("accept-cap-te-one");
        session.cancel().await.expect("the session closes");

        let held = hold_connections(port);
        let past_cap = server.wait_for_line_containing("takes no new session");
        assert!(
            past_cap.contains("3 accept errors in a row, the cap of --accept-tries: "),
            "{past_cap}"
        );
        drop(held);
        assert!(
            TcpStream::connect((Ipv4Addr::LOCALHOST, port)).is_err(),
            "a server past its cap takes no new session"
        );
        assert!(
            is_alive(pid),
            "an accept error never stops an engine that the server started"
        );

        kill_process(pid_of(server.pid()), Signal::TERM).expect("SIGTERM reaches it");
        let exited = server.wait_for_exit();
        assert!(
            exited
                .stderr
                .contains("polysim-mcp: stopped accept-cap-te-one (exit code 0, forced false)"),
            "{}",
            exited.stderr
        );
        assert!(!is_alive(pid), "the exit signal still stops each engine");
    }
}
