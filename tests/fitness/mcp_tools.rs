//! The MCP surface an agent and a desktop client see, pinned by name literals: an expectation
//! built from the server's own types would agree with any rename.

use std::time::Duration;

use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::{QuitReason, RunningService};
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

use polysim::mcp::{Fleet, PolysimMcp};

use crate::mcp_support::{
    EngineTree, FAST, LINK_BIND, MODE_OFF, MODE_SIM, TRAP_TERM, UNPARSEABLE, reply_of, text_of,
};

/// A server that panics inside a handler never answers, so every call is bounded.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(10);

struct Session {
    client: RunningService<RoleClient, ()>,
    server: JoinHandle<Result<QuitReason, tokio::task::JoinError>>,
}

impl Session {
    async fn open(fleet: Fleet) -> Session {
        let (server_side, client_side) = tokio::io::duplex(64 * 1024);
        let server = tokio::spawn(async move {
            PolysimMcp::new(fleet)
                .serve(server_side)
                .await
                .expect("the server initializes")
                .waiting()
                .await
        });
        let client = answered(().serve(client_side))
            .await
            .expect("the client initializes");
        Session { client, server }
    }

    async fn call(&self, tool: &str, arguments: Value) -> CallToolResult {
        let arguments = arguments
            .as_object()
            .expect("arguments are an object")
            .clone();
        let params = CallToolRequestParams::new(tool.to_owned()).with_arguments(arguments);
        answered(self.client.call_tool(params))
            .await
            .expect("a refusal is a tool result, never a protocol error")
    }

    async fn close(self) {
        answered(self.client.cancel())
            .await
            .expect("the client closes");
        answered(self.server)
            .await
            .expect("the server task joins")
            .expect("the server quits cleanly");
    }
}

async fn answered<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(ANSWER_TIMEOUT, future)
        .await
        .expect("the server answered")
}

fn refusal_of(result: &CallToolResult) -> &str {
    assert_eq!(result.is_error, Some(true), "{result:?}");
    text_of(result)
}

fn keys_of(object: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = object
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    keys
}

fn engine_args(strategy_id: &str, te_id: &str) -> Value {
    json!({"strategy_id": strategy_id, "te_id": te_id})
}

#[tokio::test]
async fn the_server_exposes_exactly_four_tools() {
    let tree = EngineTree::new("mcp-tool-names");
    let session = Session::open(tree.fleet(FAST)).await;
    let tools = answered(session.client.list_all_tools())
        .await
        .expect("lists tools");
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
    session.close().await;
}

#[tokio::test]
async fn the_tools_carry_the_annotations_a_ui_confirms_by() {
    let tree = EngineTree::new("mcp-annotations");
    let session = Session::open(tree.fleet(FAST)).await;
    let tools = answered(session.client.list_all_tools())
        .await
        .expect("lists tools");
    let mut hints: Vec<(String, [Option<bool>; 4])> = tools
        .iter()
        .map(|tool| {
            let annotations = tool.annotations.clone().expect("every tool is annotated");
            let hints = [
                annotations.read_only_hint,
                annotations.destructive_hint,
                annotations.idempotent_hint,
                annotations.open_world_hint,
            ];
            (tool.name.to_string(), hints)
        })
        .collect();
    hints.sort();
    assert_eq!(
        hints,
        [
            (
                "list_strategies".to_owned(),
                [Some(true), None, Some(true), None]
            ),
            (
                "set_execution_mode".to_owned(),
                [Some(false), Some(true), Some(true), None]
            ),
            (
                "start_strategy".to_owned(),
                [Some(false), Some(false), Some(false), None]
            ),
            (
                "stop_strategy".to_owned(),
                [Some(false), Some(true), Some(false), None]
            ),
        ],
        "read-only, destructive, idempotent, open-world, in that order"
    );
    session.close().await;
}

#[tokio::test]
async fn the_server_tells_an_agent_that_live_is_not_guarded() {
    let tree = EngineTree::new("mcp-instructions");
    let session = Session::open(tree.fleet(FAST)).await;
    let info = session
        .client
        .peer_info()
        .expect("the handshake carried server info");
    assert_eq!(
        info.server_info.as_ref().map(|server| server.name.as_str()),
        Some("polysim-mcp")
    );
    assert_eq!(
        info.instructions.as_deref(),
        Some(
            "polysim-mcp operates the trading engines checked out at this repo root. Each engine \
             is a separate process named <strategy_id>-<te_id>; build it first with `cargo build \
             --release --bin <strategy_id>-<te_id>`. list_strategies shows every engine with its \
             execution mode and whether it is running. start_strategy spawns the prebuilt binary; \
             stop_strategy sends SIGTERM and waits for the engine's own drain, forcing only past \
             the deadline. set_execution_mode rewrites execution.mode in the engine's config.yaml \
             and is refused while that engine runs; the mode is read at boot, so set it before \
             starting. Mode live trades real money and this server does not guard it: confirm \
             with the operator before setting live or starting a live engine. An engine's stdout \
             and stderr go to its console log path, returned by every call, for diagnosis."
        ),
        "a desktop client and every agent read this text verbatim"
    );
    session.close().await;
}

#[tokio::test]
async fn list_strategies_replies_with_named_fields() {
    let tree = EngineTree::new("mcp-list-fields");
    tree.config("strat-a", "te-off", MODE_OFF)
        .config("strat-b", "te-broken", UNPARSEABLE);
    let session = Session::open(tree.fleet(FAST)).await;

    let listing = reply_of(&session.call("list_strategies", json!({})).await);
    assert_eq!(keys_of(&listing), ["engines"]);
    let off = &listing["engines"][0];
    assert_eq!(
        keys_of(off),
        [
            "bin",
            "config_error",
            "console_log",
            "exit_code",
            "link_bind",
            "mode",
            "pid",
            "started_at",
            "state",
            "strategy_id",
            "te_id"
        ],
        "the engine row is flat, and every field is present even when it is null"
    );
    assert_eq!(off["strategy_id"], "strat-a");
    assert_eq!(off["te_id"], "te-off");
    assert_eq!(off["bin"], "strat-a-te-off");
    assert_eq!(off["mode"], "off");
    assert_eq!(off["state"], "stopped");
    assert_eq!(off["link_bind"], LINK_BIND);
    for nullable in ["pid", "started_at", "exit_code", "config_error"] {
        assert!(
            off[nullable].is_null(),
            "{nullable} is null on a stopped row"
        );
    }
    assert!(
        off["console_log"]
            .as_str()
            .is_some_and(|path| path.ends_with("logs/strat-a-te-off-console.log")),
        "a stopped row names the log its next start writes: {}",
        off["console_log"]
    );

    let broken = &listing["engines"][1];
    assert_eq!(broken["mode"], "unreadable");
    assert!(broken["link_bind"].is_null());
    let config_path = tree
        .config_path("strat-b", "te-broken")
        .display()
        .to_string();
    assert!(
        broken["config_error"]
            .as_str()
            .is_some_and(|error| error.contains(&config_path)),
        "the row says which file to fix: {}",
        broken["config_error"]
    );
    session.close().await;
}

#[tokio::test]
async fn start_stop_and_set_mode_reply_with_named_fields() {
    let tree = EngineTree::new("mcp-reply-fields");
    tree.engine("strat-a", "te-sim", MODE_SIM, TRAP_TERM);
    let session = Session::open(tree.fleet(FAST)).await;
    let engine = engine_args("strat-a", "te-sim");

    let started = reply_of(&session.call("start_strategy", engine.clone()).await);
    assert_eq!(
        keys_of(&started),
        ["console_log", "mode", "pid", "strategy_id", "te_id"]
    );
    assert_eq!(
        (&started["strategy_id"], &started["te_id"], &started["mode"]),
        (&json!("strat-a"), &json!("te-sim"), &json!("sim"))
    );
    assert!(
        started["console_log"]
            .as_str()
            .is_some_and(|path| path.ends_with("logs/strat-a-te-sim-sim-console.log")),
        "{}",
        started["console_log"]
    );
    let pid = started["pid"].as_u64().expect("pid is an integer");
    tree.wait_until_ready("strat-a-te-sim-sim");

    let listing = reply_of(&session.call("list_strategies", json!({})).await);
    let running = &listing["engines"][0];
    assert_eq!(running["state"], "running");
    assert_eq!(running["pid"].as_u64(), Some(pid));
    assert!(
        running["started_at"]
            .as_str()
            .is_some_and(|at| at.ends_with('Z'))
    );
    assert!(running["exit_code"].is_null());
    assert_eq!(running["console_log"], started["console_log"]);

    let stopped = reply_of(&session.call("stop_strategy", engine.clone()).await);
    assert_eq!(
        keys_of(&stopped),
        ["duration_ms", "exit_code", "forced", "strategy_id", "te_id"]
    );
    assert_eq!(
        (&stopped["exit_code"], &stopped["forced"]),
        (&json!(0), &json!(false))
    );
    assert!(stopped["duration_ms"].is_u64());

    let mut arguments = engine;
    arguments["mode"] = json!("off");
    let change = reply_of(&session.call("set_execution_mode", arguments).await);
    assert_eq!(
        keys_of(&change),
        ["config_path", "mode", "previous", "strategy_id", "te_id"]
    );
    assert_eq!(
        (&change["previous"], &change["mode"]),
        (&json!("sim"), &json!("off"))
    );
    assert_eq!(
        change["config_path"],
        tree.config_path("strat-a", "te-sim").display().to_string()
    );
    session.close().await;
}

#[tokio::test]
async fn a_refused_request_is_a_tool_error_with_actionable_text() {
    let tree = EngineTree::new("mcp-refusals");
    tree.engine("strat-a", "te-off", MODE_OFF, TRAP_TERM)
        .config("strat-a", "te-unbuilt", MODE_OFF);
    let session = Session::open(tree.fleet(FAST)).await;
    let engine = engine_args("strat-a", "te-off");

    reply_of(&session.call("start_strategy", engine.clone()).await);
    tree.wait_until_ready("strat-a-te-off");
    let again = session.call("start_strategy", engine.clone()).await;
    assert!(
        refusal_of(&again).contains("already running"),
        "{}",
        text_of(&again)
    );
    reply_of(&session.call("stop_strategy", engine.clone()).await);
    let again = session.call("stop_strategy", engine.clone()).await;
    assert!(
        refusal_of(&again).contains("not running"),
        "{}",
        text_of(&again)
    );

    let unbuilt = session
        .call("start_strategy", engine_args("strat-a", "te-unbuilt"))
        .await;
    assert!(
        refusal_of(&unbuilt).contains("cargo build --release --bin strat-a-te-unbuilt"),
        "{}",
        text_of(&unbuilt)
    );

    let mut paper = engine;
    paper["mode"] = json!("paper");
    let paper = session.call("set_execution_mode", paper).await;
    assert!(refusal_of(&paper).contains("paper"), "{}", text_of(&paper));

    let bad_id = session
        .call("start_strategy", engine_args("Bad Id!", "te-off"))
        .await;
    assert!(
        refusal_of(&bad_id).contains("must match ^[a-z][a-z0-9-]*$"),
        "the identifier rule is the refusal: {}",
        text_of(&bad_id)
    );
    session.close().await;
}
