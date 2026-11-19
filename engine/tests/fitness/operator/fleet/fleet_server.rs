//! Contract tests of the fleet server through its real process over loopback, with stand-in
//! scripts as its engines: what it lists, what it starts and stops, and the one edit it makes.

use std::io::Write;
use std::net::{Ipv4Addr, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use rmcp::RoleClient;
use rmcp::model::CallToolResult;
use rmcp::service::RunningService;
use serde_json::{Value, json};

use engine::runtime::config::{Config, ExecutionMode};

use crate::micro_strategy::MicroRecorderParams;
use crate::operator::fleet::mcp_support::{
    EXITS_THREE, EngineTree, IGNORE_TERM, LINK_BIND, MARKERS, MODE_OFF, MODE_SIM, NO_EXECUTION,
    Server, TRAP_TERM, UNPARSEABLE, call, is_alive, open_session, reply_of, started_pid,
    status_of_browser_request, text_of, wait_for_file_containing, within,
};

const SHIPPED_BINANCE: &str =
    "../strategies/strat-micro-recorder/te-binance-spot-btcusdt/config.yaml";

/// The timing of the server binary, which takes no flag for it.
const START_GRACE: Duration = Duration::from_secs(2);
const STOP_DEADLINE: Duration = Duration::from_secs(20);

type Session = RunningService<RoleClient, ()>;

fn engine(strategy_id: &str, te_id: &str) -> Value {
    json!({"strategy_id": strategy_id, "te_id": te_id})
}

fn mode_change(strategy_id: &str, te_id: &str, mode: &str) -> Value {
    json!({"strategy_id": strategy_id, "te_id": te_id, "mode": mode})
}

fn refusal_of(result: &CallToolResult) -> String {
    assert_eq!(result.is_error, Some(true), "{result:?}");
    text_of(result).to_owned()
}

/// The server resolves its root, so each path in a reply is the canonical one.
fn canonical(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).expect("the path exists")
}

async fn listing(session: &Session) -> Vec<Value> {
    let listed = reply_of(&call(session, "list_strategies", json!({})).await);
    listed["engines"]
        .as_array()
        .expect("the listing holds its engines")
        .clone()
}

fn row_of<'a>(rows: &'a [Value], te_id: &str) -> &'a Value {
    rows.iter()
        .find(|row| row["te_id"] == te_id)
        .unwrap_or_else(|| panic!("no row of {te_id} in {rows:?}"))
}

fn listening(tree: &EngineTree) -> (Server, u16) {
    std::fs::create_dir_all(tree.bin_dir()).expect("mkdir bin");
    let server = Server::spawn(tree, 0);
    let port = server.wait_until_listening();
    (server, port)
}

/// Each behavior below holds through the server that an agent reaches: one process, its tools over
/// loopback, and real child processes. The servers run side by side, because two of them wait out
/// the stop deadline of the binary.
#[tokio::test(flavor = "multi_thread")]
async fn fleet_starts_stops_and_reads_back_its_one_edit() {
    let cases = [
        tokio::spawn(discovery_lists_each_engine_folder_with_its_mode_and_skips_the_rest()),
        tokio::spawn(the_one_edit_rewrites_only_the_mode_value_and_reads_it_back()),
        tokio::spawn(one_engine_walks_its_states_through_the_tools()),
        tokio::spawn(a_stop_forces_only_an_engine_that_ignores_sigterm()),
        tokio::spawn(engines_outlive_their_session_until_the_exit_signal_stops_them_all()),
        tokio::spawn(a_server_watching_stdin_stops_its_engines_when_the_parent_closes_it()),
    ];
    for case in cases {
        case.await.expect("each case passes");
    }
}

async fn discovery_lists_each_engine_folder_with_its_mode_and_skips_the_rest() {
    let tree = EngineTree::new("fleet-discovery");
    tree.config("strat-a", "te-off", MODE_OFF)
        .config("strat-a", "te-sim", MODE_SIM)
        .config("strat-b", "te-bare", NO_EXECUTION)
        .config("strat-b", "te-broken", UNPARSEABLE)
        .config("strat-b", "not-an-engine", MODE_OFF)
        .config("__pycache__", "te-x", MODE_OFF)
        .config("Strat-D", "te-upper", MODE_OFF);
    std::fs::create_dir_all(tree.root().join("strategies/strat-c/te-no-config")).expect("mkdir");
    let (mut server, port) = listening(&tree);
    let session = open_session(port).await;

    let rows = listing(&session).await;
    let seen: Vec<[&str; 5]> = rows
        .iter()
        .map(|row| {
            ["strategy_id", "te_id", "bin", "mode", "state"]
                .map(|key| row[key].as_str().expect("a text field"))
        })
        .collect();
    assert_eq!(
        seen,
        [
            ["strat-a", "te-off", "strat-a-te-off", "off", "stopped"],
            ["strat-a", "te-sim", "strat-a-te-sim", "sim", "stopped"],
            ["strat-b", "te-bare", "strat-b-te-bare", "absent", "stopped"],
            [
                "strat-b",
                "te-broken",
                "strat-b-te-broken",
                "unreadable",
                "stopped"
            ],
        ],
        "only te- folders with a config and an exact valid id are engines, sorted by identity"
    );
    let [off, sim, bare, broken] = rows.as_slice() else {
        panic!("four engines");
    };
    let console_log = |row: &Value| row["console_log"].as_str().expect("a path").to_owned();
    assert!(console_log(off).ends_with("logs/strat-a-te-off-console.log"));
    assert!(
        console_log(sim).ends_with("logs/strat-a-te-sim-sim-console.log"),
        "a sim run writes a physically separate console log"
    );
    assert_eq!(off["link_bind"], LINK_BIND);
    assert_eq!(sim["link_bind"], Value::Null, "no link block reads as null");
    for row in [off, sim, bare] {
        assert_eq!(row["config_error"], Value::Null, "{row}");
        assert_eq!(
            [&row["pid"], &row["started_at"], &row["exit_code"]],
            [&Value::Null; 3]
        );
    }
    assert_eq!(broken["link_bind"], Value::Null);
    let broken_path = canonical(&tree.config_path("strat-b", "te-broken"));
    let config_error = broken["config_error"]
        .as_str()
        .expect("an unreadable row says why");
    assert!(
        config_error.contains(&broken_path.display().to_string()),
        "the error names the file an agent must fix: {config_error}"
    );
    drop(session);
    assert_eq!(server.terminate().status.code(), Some(0));

    let empty = EngineTree::new("fleet-no-strategies");
    let (mut server, port) = listening(&empty);
    let session = open_session(port).await;
    let refused = refusal_of(&call(&session, "list_strategies", json!({})).await);
    let strategies = canonical(empty.root()).join("strategies");
    assert!(
        refused.contains(&format!(
            "cannot read the directory {}",
            strategies.display()
        )),
        "a wrong root is reported by the path it looked at: {refused}"
    );
    drop(session);
    assert_eq!(server.terminate().status.code(), Some(0));
}

async fn the_one_edit_rewrites_only_the_mode_value_and_reads_it_back() {
    let shipped = std::fs::read_to_string(SHIPPED_BINANCE).expect("the shipped config reads");
    let edges = "link:\n  bind: \"127.0.0.1:9399\"\n  mode: x\nsources:\n  - execution:\r\n      \
                 # mode: off\n      nested:\n        mode: off\n      mode: sim # pick\r\n      \
                 max: 1\n";
    let unverified = [
        (
            "te-next-line",
            "sources:\n  - execution:\n      mode:\n        live\n",
            "has no mode: line with a value",
        ),
        (
            "te-comment",
            "sources:\n  - execution:\n      mode: # pick one\n",
            "cannot be read as a config",
        ),
        (
            "te-duplicate",
            "sources:\n  - execution:\n      mode: off\n      mode: sim\n",
            "cannot be read as a config",
        ),
        (
            "te-anchored",
            "sources:\n  - execution:\n      mode: &m sim\n",
            "was not changed",
        ),
    ];
    let tree = EngineTree::new("fleet-edits");
    tree.config("strat-a", "te-x", &shipped)
        .config("strat-a", "te-edges", edges);
    for (te, text, _) in &unverified {
        tree.config("strat-u", te, text);
    }
    let path = tree.config_path("strat-a", "te-x");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).expect("chmod");
    let (mut server, port) = listening(&tree);
    let session = open_session(port).await;

    let change = reply_of(
        &call(
            &session,
            "set_execution_mode",
            mode_change("strat-a", "te-x", "live"),
        )
        .await,
    );
    assert_eq!(
        (&change["previous"], &change["mode"]),
        (&json!("sim"), &json!("live"))
    );
    assert_eq!(
        change["config_path"],
        canonical(&path).display().to_string()
    );
    let permissions = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
    assert_eq!(
        permissions, 0o640,
        "the edit keeps the permission bits the operator set, got {permissions:#o}"
    );
    let rewritten = std::fs::read_to_string(&path).expect("read back");
    let changed: Vec<(&str, &str)> = shipped
        .lines()
        .zip(rewritten.lines())
        .filter(|(before, after)| before != after)
        .collect();
    assert_eq!(
        changed,
        [(
            "      mode: sim # live, sim, off",
            "      mode: live # live, sim, off"
        )],
        "exactly the mode value changes; its indent and trailing comment survive"
    );
    assert_eq!(shipped.lines().count(), rewritten.lines().count());
    let loaded = Config::<MicroRecorderParams>::from_yaml(&rewritten)
        .expect("the sim block is inert under live, so the edited config still loads");
    assert_eq!(
        loaded.sources[0].execution.as_ref().map(|block| block.mode),
        Some(ExecutionMode::Live)
    );
    reply_of(
        &call(
            &session,
            "set_execution_mode",
            mode_change("strat-a", "te-x", "sim"),
        )
        .await,
    );
    assert_eq!(
        std::fs::read(&path).expect("read back"),
        shipped.as_bytes(),
        "a round trip restores the file byte for byte"
    );

    let change = reply_of(
        &call(
            &session,
            "set_execution_mode",
            mode_change("strat-a", "te-edges", "off"),
        )
        .await,
    );
    assert_eq!(change["previous"], "sim");
    assert_eq!(
        std::fs::read_to_string(tree.config_path("strat-a", "te-edges")).expect("read back"),
        edges.replace("      mode: sim # pick\r\n", "      mode: off # pick\r\n"),
        "a commented mode, a nested mode and another block's mode are all left alone, and CRLF \
         endings are read as line ends and kept"
    );

    for (te, text, cause) in unverified {
        let refused = refusal_of(
            &call(
                &session,
                "set_execution_mode",
                mode_change("strat-u", te, "live"),
            )
            .await,
        );
        assert!(refused.contains(cause), "{te}: {refused}");
        assert_eq!(
            std::fs::read_to_string(tree.config_path("strat-u", te)).expect("read back"),
            text,
            "{te}: a refused edit leaves the file untouched"
        );
    }
    let refused = refusal_of(
        &call(
            &session,
            "set_execution_mode",
            mode_change("strat-u", "te-missing", "off"),
        )
        .await,
    );
    assert!(
        refused.contains("no trading engine strat-u-te-missing"),
        "{refused}"
    );
    drop(session);
    assert_eq!(server.terminate().status.code(), Some(0));
}

async fn one_engine_walks_its_states_through_the_tools() {
    let tree = EngineTree::new("fleet-walk");
    let witness = tree.root().join("spawned");
    tree.engine("strat-a", "te-off", MODE_OFF, TRAP_TERM)
        .config("strat-a", "te-nobin", MODE_OFF)
        .config("strat-a", "te-bare", NO_EXECUTION)
        .engine(
            "strat-a",
            "te-broken",
            UNPARSEABLE,
            &format!("#!/bin/sh\ntouch {}\n", witness.display()),
        )
        .engine("strat-a", "te-dies", MODE_OFF, EXITS_THREE)
        .engine("strat-a", "te-markers", MODE_OFF, MARKERS)
        .engine("strat-a", "te-moves", MODE_OFF, TRAP_TERM);
    let (mut server, port) = listening(&tree);
    let session = open_session(port).await;

    start_and_stop_walk_the_state_machine(&tree, &session).await;
    a_start_is_refused_with_the_fix_for_each_cause(&tree, &session, &witness).await;
    child_output_lands_in_the_console_log(&tree, &session).await;
    a_running_engine_keeps_its_row_when_its_folder_moves(&tree, &session).await;

    drop(session);
    let exited = server.terminate();
    assert_eq!(exited.status.code(), Some(0), "{}", exited.stderr);
    assert_eq!(exited.stdout, "", "stdout stays silent over http");
}

async fn start_and_stop_walk_the_state_machine(tree: &EngineTree, session: &Session) {
    let started = reply_of(&call(session, "start_strategy", engine("strat-a", "te-off")).await);
    let pid = started_pid(&started);
    assert_eq!(
        (&started["strategy_id"], &started["te_id"], &started["mode"]),
        (&json!("strat-a"), &json!("te-off"), &json!("off"))
    );
    let console_log = started["console_log"].as_str().expect("a path").to_owned();
    assert!(
        console_log.ends_with("logs/strat-a-te-off-console.log"),
        "{console_log}"
    );

    let rows = listing(session).await;
    let row = row_of(&rows, "te-off");
    assert_eq!(
        (&row["state"], &row["pid"], &row["exit_code"]),
        (&json!("running"), &json!(pid), &Value::Null)
    );
    let started_at = row["started_at"]
        .as_str()
        .expect("a running row has its start");
    assert!(
        is_utc_with_micros(started_at),
        "RFC3339 UTC with six fraction digits, got {started_at}"
    );
    assert_eq!(row["console_log"], console_log);

    tree.wait_until_ready("strat-a-te-off");
    let again = refusal_of(&call(session, "start_strategy", engine("strat-a", "te-off")).await);
    assert!(
        again.contains(&format!("already running as pid {pid}")),
        "one engine, one process: {again}"
    );

    let locked = refusal_of(
        &call(
            session,
            "set_execution_mode",
            mode_change("strat-a", "te-off", "live"),
        )
        .await,
    );
    assert!(
        locked.contains(&format!("running as pid {pid}; stop it first")),
        "the engine reads its mode only at boot: {locked}"
    );
    assert_eq!(
        std::fs::read_to_string(tree.config_path("strat-a", "te-off")).expect("read back"),
        MODE_OFF
    );

    // The engine is a shell script, so a live mode here trades nothing.
    let edited = "sources:\n  - execution:\n      mode: live\nlink:\n  bind: \"127.0.0.1:9400\"\n";
    tree.config("strat-a", "te-off", edited);
    let rows = listing(session).await;
    let row = row_of(&rows, "te-off");
    assert_eq!(
        [
            &row["state"],
            &row["mode"],
            &row["link_bind"],
            &row["config_error"]
        ],
        [
            &json!("running"),
            &json!("off"),
            &json!(LINK_BIND),
            &Value::Null
        ],
        "a hand edit made while the engine runs changes nothing it is running with"
    );

    let stopped = reply_of(&call(session, "stop_strategy", engine("strat-a", "te-off")).await);
    assert_eq!(
        (&stopped["exit_code"], &stopped["forced"]),
        (&json!(0), &json!(false))
    );
    assert_eq!(
        (&stopped["strategy_id"], &stopped["te_id"]),
        (&json!("strat-a"), &json!("te-off"))
    );
    let duration = stopped["duration_ms"].as_u64().expect("a duration");
    assert!(
        Duration::from_millis(duration) < STOP_DEADLINE,
        "SIGTERM alone ended it: {duration} ms"
    );
    let again = refusal_of(&call(session, "stop_strategy", engine("strat-a", "te-off")).await);
    assert!(again.contains("strat-a-te-off is not running"), "{again}");

    let rows = listing(session).await;
    let row = row_of(&rows, "te-off");
    assert_eq!(
        [
            &row["state"],
            &row["pid"],
            &row["started_at"],
            &row["mode"],
            &row["link_bind"]
        ],
        [
            &json!("stopped"),
            &Value::Null,
            &Value::Null,
            &json!("live"),
            &json!("127.0.0.1:9400")
        ],
        "a stopped row reads the file that the next start boots from"
    );
    let change = reply_of(
        &call(
            session,
            "set_execution_mode",
            mode_change("strat-a", "te-off", "sim"),
        )
        .await,
    );
    assert_eq!(
        change["previous"], "live",
        "a stopped engine's mode can change"
    );

    let bare = refusal_of(
        &call(
            session,
            "set_execution_mode",
            mode_change("strat-a", "te-bare", "live"),
        )
        .await,
    );
    assert!(bare.contains("add the block by hand"), "{bare}");
    assert_eq!(
        std::fs::read_to_string(tree.config_path("strat-a", "te-bare")).expect("read back"),
        NO_EXECUTION
    );
}

async fn a_start_is_refused_with_the_fix_for_each_cause(
    tree: &EngineTree,
    session: &Session,
    witness: &Path,
) {
    let unknown =
        refusal_of(&call(session, "start_strategy", engine("strat-a", "te-nowhere")).await);
    assert!(unknown.contains("list_strategies"), "{unknown}");
    let missing = refusal_of(&call(session, "start_strategy", engine("strat-a", "te-nobin")).await);
    assert!(
        missing.contains("cargo build --release --bin strat-a-te-nobin"),
        "the error is the command that fixes it: {missing}"
    );

    let unreadable =
        refusal_of(&call(session, "start_strategy", engine("strat-a", "te-broken")).await);
    assert!(
        unreadable.contains("cannot be read as a config"),
        "{unreadable}"
    );
    let rows = listing(session).await;
    assert_eq!(
        row_of(&rows, "te-broken")["config_error"],
        unreadable,
        "the refusal and the listing say the same thing"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!witness.exists(), "nothing was spawned");
    assert!(
        !tree.console_log("strat-a-te-broken").exists(),
        "no console log was opened"
    );

    let began = Instant::now();
    let died = refusal_of(&call(session, "start_strategy", engine("strat-a", "te-dies")).await);
    assert!(
        began.elapsed() < START_GRACE,
        "the call returns as soon as the child exits, not at the end of the grace"
    );
    assert!(
        died.contains("exited during start-up (exit status: 3)"),
        "{died}"
    );
    assert!(
        died.contains("config refused"),
        "the tail carries the child's own words: {died}"
    );
    let rows = listing(session).await;
    let row = row_of(&rows, "te-dies");
    assert_eq!(
        [&row["state"], &row["exit_code"], &row["pid"]],
        [&json!("exited"), &json!(3), &Value::Null]
    );
}

async fn child_output_lands_in_the_console_log(tree: &EngineTree, session: &Session) {
    let log = tree.console_log("strat-a-te-markers");
    for run in 1..=2 {
        reply_of(&call(session, "start_strategy", engine("strat-a", "te-markers")).await);
        let text = wait_for_file_containing(&log, &["stdout-marker", "stderr-marker"]);
        assert_eq!(
            (
                text.matches("stdout-marker").count(),
                text.matches("stderr-marker").count()
            ),
            (run, run),
            "a child has one stdout and one stderr, so both markers in the file prove neither \
             was inherited, and run {run} appended rather than truncated: {text}"
        );
        reply_of(&call(session, "stop_strategy", engine("strat-a", "te-markers")).await);
    }
}

async fn a_running_engine_keeps_its_row_when_its_folder_moves(
    tree: &EngineTree,
    session: &Session,
) {
    let started = reply_of(&call(session, "start_strategy", engine("strat-a", "te-moves")).await);
    let pid = started_pid(&started);
    tree.wait_until_ready("strat-a-te-moves");
    let folder = tree.root().join("strategies/strat-a/te-moves");
    std::fs::rename(&folder, folder.with_file_name("te-renamed")).expect("rename");

    let rows = listing(session).await;
    let order: Vec<&str> = rows
        .iter()
        .map(|row| row["te_id"].as_str().expect("an id"))
        .filter(|te| ["te-moves", "te-renamed"].contains(te))
        .collect();
    assert_eq!(
        order,
        ["te-moves", "te-renamed"],
        "the moved row keeps its sorted place"
    );
    let moved = row_of(&rows, "te-moves");
    assert_eq!(
        [
            &moved["state"],
            &moved["pid"],
            &moved["bin"],
            &moved["mode"],
            &moved["link_bind"]
        ],
        [
            &json!("running"),
            &json!(pid),
            &json!("strat-a-te-moves"),
            &json!("off"),
            &json!(LINK_BIND)
        ],
        "a running engine stays listed without its folder, so a client can still stop it"
    );
    assert_eq!(moved["config_error"], Value::Null);
    assert_eq!(moved["console_log"], started["console_log"]);
    assert_eq!(row_of(&rows, "te-renamed")["state"], "stopped");

    let stopped = reply_of(&call(session, "stop_strategy", engine("strat-a", "te-moves")).await);
    assert_eq!(
        (&stopped["exit_code"], &stopped["forced"]),
        (&json!(0), &json!(false))
    );
    let rows = listing(session).await;
    assert!(
        rows.iter().all(|row| row["te_id"] != "te-moves"),
        "once stopped, an engine with no folder is gone"
    );
}

async fn a_stop_forces_only_an_engine_that_ignores_sigterm() {
    let tree = EngineTree::new("fleet-force");
    tree.engine("strat-f", "te-stubborn", MODE_OFF, IGNORE_TERM);
    let (mut server, port) = listening(&tree);
    let session = open_session(port).await;
    let stubborn = engine("strat-f", "te-stubborn");
    reply_of(&call(&session, "start_strategy", stubborn.clone()).await);
    tree.wait_until_ready("strat-f-te-stubborn");

    let stopped = reply_of(&call(&session, "stop_strategy", stubborn).await);
    assert_eq!(
        (&stopped["forced"], &stopped["exit_code"]),
        (&json!(true), &Value::Null)
    );
    let duration = stopped["duration_ms"].as_u64().expect("a duration");
    assert!(
        Duration::from_millis(duration) >= STOP_DEADLINE,
        "SIGTERM got the whole deadline first: {duration} ms"
    );
    drop(session);
    assert_eq!(server.terminate().status.code(), Some(0));
}

/// An engine started from one agent reply survives that reply and every later session sees it,
/// until the exit signal stops each engine under one shared deadline. After that the port takes no
/// connection, so no start can follow.
async fn engines_outlive_their_session_until_the_exit_signal_stops_them_all() {
    let tree = EngineTree::new("fleet-exit");
    tree.engine("strat-s", "te-polite", MODE_OFF, TRAP_TERM)
        .engine("strat-s", "te-stubborn", MODE_OFF, IGNORE_TERM);
    let (mut server, port) = listening(&tree);

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
    let polite = started_pid(&reply_of(
        &call(&first, "start_strategy", engine("strat-s", "te-polite")).await,
    ));
    let stubborn = started_pid(&reply_of(
        &call(&first, "start_strategy", engine("strat-s", "te-stubborn")).await,
    ));
    tree.wait_until_ready("strat-s-te-polite");
    tree.wait_until_ready("strat-s-te-stubborn");
    within(first.cancel())
        .await
        .expect("the first session closes");
    assert!(
        is_alive(polite) && is_alive(stubborn),
        "closing the session that started an engine must not stop it"
    );

    let second = open_session(port).await;
    let again = refusal_of(&call(&second, "start_strategy", engine("strat-s", "te-polite")).await);
    assert!(again.contains("already running"), "{again}");
    let rows = listing(&second).await;
    assert_eq!(
        (
            &row_of(&rows, "te-polite")["state"],
            &row_of(&rows, "te-polite")["pid"]
        ),
        (&json!("running"), &json!(polite))
    );
    let status = status_of_browser_request(port);
    assert!(
        status.starts_with("HTTP/1.1 403"),
        "a request from a browser page is refused: {status:?}"
    );

    let began = Instant::now();
    let exited = server.terminate();
    drop(second);
    assert!(
        began.elapsed() < STOP_DEADLINE * 2,
        "one deadline is shared, so two engines cost one deadline, not two"
    );
    assert_eq!(exited.status.code(), Some(0), "{}", exited.stderr);
    for line in [
        "polysim-mcp: stopped strat-s-te-polite (exit code 0, forced false)",
        "polysim-mcp: stopped strat-s-te-stubborn (exit code none, forced true)",
    ] {
        assert!(exited.stderr.contains(line), "{line}: {}", exited.stderr);
    }
    assert!(
        !is_alive(polite) && !is_alive(stubborn),
        "each engine is gone once the server exits"
    );
    assert_eq!(exited.stdout, "", "stdout stays silent over http");
    assert!(
        TcpStream::connect((Ipv4Addr::LOCALHOST, port)).is_err(),
        "a server that has stopped its engines takes no start"
    );
}

/// A parent that starts the server and then dies without stopping it must not leave the server and
/// its engines running where the next parent cannot reach them.
async fn a_server_watching_stdin_stops_its_engines_when_the_parent_closes_it() {
    let tree = EngineTree::new("fleet-parent");
    tree.engine("strat-p", "te-one", MODE_OFF, TRAP_TERM);
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
    let pid = started_pid(&reply_of(
        &call(&session, "start_strategy", engine("strat-p", "te-one")).await,
    ));
    tree.wait_until_ready("strat-p-te-one");

    drop(server.child.stdin.take());
    let exited = server.wait_for_exit();
    drop(session);
    assert_eq!(exited.status.code(), Some(0), "{}", exited.stderr);
    assert!(
        exited
            .stderr
            .contains("polysim-mcp: stopped strat-p-te-one (exit code 0, forced false)"),
        "{}",
        exited.stderr
    );
    assert!(!is_alive(pid), "the engine is gone once the server exits");
    assert_eq!(exited.stdout, "", "stdout stays silent over http too");
}

/// `YYYY-MM-DDTHH:MM:SS.ffffffZ`, checked byte by byte.
fn is_utc_with_micros(text: &str) -> bool {
    let bytes = text.as_bytes();
    let separators = [
        (4, b'-'),
        (7, b'-'),
        (10, b'T'),
        (13, b':'),
        (16, b':'),
        (19, b'.'),
        (26, b'Z'),
    ];
    bytes.len() == 27
        && bytes.iter().enumerate().all(|(index, byte)| {
            separators
                .iter()
                .find(|(at, _)| *at == index)
                .map_or(byte.is_ascii_digit(), |(_, separator)| byte == separator)
        })
}
