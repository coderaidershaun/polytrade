//! The supervisor's contract against real files and real processes: what it lists, what it starts
//! and stops, and the one config edit it is allowed to make.

use std::os::unix::fs::PermissionsExt;
use std::time::{Duration, Instant};

use polysim::config::ExecutionMode;
use polysim::mcp::{FleetError, FleetTiming};

use crate::mcp_support::{
    EXITS_THREE, EngineTree, FAST, FRESH_SCRIPT_DEADLINE, IGNORE_TERM, LINK_BIND, MARKERS,
    MODE_OFF, MODE_SIM, NO_EXECUTION, TRAP_TERM, UNPARSEABLE, identity, wait_for_file_containing,
};

const SHIPPED_BINANCE: &str = "strategies/strat-micro-recorder/te-binance-spot-btcusdt/config.yaml";

#[test]
fn discovery_lists_every_engine_folder_with_its_mode_and_skips_the_rest() {
    let tree = EngineTree::new("mcp-discovery");
    tree.config("strat-a", "te-off", MODE_OFF)
        .config("strat-a", "te-sim", MODE_SIM)
        .config("strat-b", "te-bare", NO_EXECUTION)
        .config("strat-b", "te-broken", UNPARSEABLE)
        .config("strat-b", "not-an-engine", MODE_OFF)
        .config("__pycache__", "te-x", MODE_OFF)
        .config("Strat-D", "te-upper", MODE_OFF);
    std::fs::create_dir_all(tree.root().join("strategies/strat-c/te-no-config")).expect("mkdir");

    let listing = tree.fleet(FAST).list().expect("the tree lists");
    let rows: Vec<(&str, &str, &str, &str, &str)> = listing
        .engines
        .iter()
        .map(|row| {
            (
                row.strategy_id.as_str(),
                row.te_id.as_str(),
                row.bin.as_str(),
                row.mode,
                row.state,
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            ("strat-a", "te-off", "strat-a-te-off", "off", "stopped"),
            ("strat-a", "te-sim", "strat-a-te-sim", "sim", "stopped"),
            ("strat-b", "te-bare", "strat-b-te-bare", "absent", "stopped"),
            (
                "strat-b",
                "te-broken",
                "strat-b-te-broken",
                "unreadable",
                "stopped"
            ),
        ],
        "only te- folders with a config and an exact valid id are engines, sorted by identity"
    );

    let [off, sim, bare, broken] = listing.engines.as_slice() else {
        panic!("four engines");
    };
    assert!(
        off.console_log.ends_with("logs/strat-a-te-off-console.log"),
        "{}",
        off.console_log
    );
    assert!(
        sim.console_log
            .ends_with("logs/strat-a-te-sim-sim-console.log"),
        "a sim run writes a physically separate console log: {}",
        sim.console_log
    );
    assert_eq!(off.link_bind.as_deref(), Some(LINK_BIND));
    assert_eq!(sim.link_bind, None, "no link block reads as null");
    for row in [off, sim, bare] {
        assert_eq!(row.config_error, None, "{} parses", row.bin);
        assert_eq!(
            (row.pid, row.started_at.as_deref(), row.exit_code),
            (None, None, None)
        );
    }
    assert_eq!(broken.link_bind, None);
    let broken_path = tree.config_path("strat-b", "te-broken");
    let config_error = broken
        .config_error
        .as_deref()
        .expect("an unreadable row says why");
    assert!(
        config_error.contains(&broken_path.display().to_string()),
        "the error names the file an agent must fix: {config_error}"
    );

    let empty = EngineTree::new("mcp-no-strategies");
    let error = empty.fleet(FAST).list().expect_err("no strategies folder");
    assert!(matches!(error, FleetError::ReadDir { .. }), "{error:?}");
    assert!(
        error
            .to_string()
            .contains(&empty.root().join("strategies").display().to_string()),
        "a wrong root is reported by the path it looked at: {error}"
    );
}

#[test]
fn set_mode_rewrites_only_the_mode_value_and_preserves_every_other_byte() {
    let shipped = std::fs::read_to_string(SHIPPED_BINANCE).expect("the shipped config reads");
    let tree = EngineTree::new("mcp-set-mode");
    tree.config("strat-a", "te-x", &shipped);
    let path = tree.config_path("strat-a", "te-x");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).expect("chmod");
    let mut fleet = tree.fleet(FAST);
    let engine = identity("strat-a", "te-x");

    let change = fleet
        .set_mode(&engine, ExecutionMode::Live)
        .expect("the shipped config has a mode line");
    assert_eq!((change.previous, change.mode), ("sim", "live"));
    assert_eq!(change.config_path, path.display().to_string());
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
        vec![(
            "  mode: sim # live, sim, off",
            "  mode: live # live, sim, off"
        )],
        "exactly the mode value changes; its indent and trailing comment survive"
    );
    assert_eq!(shipped.lines().count(), rewritten.lines().count());
    let loaded = polysim::config::Config::<crate::micro_strategy::MicroRecorderParams>::from_yaml(
        &rewritten,
    )
    .expect("the sim block is inert under live, so the edited config still loads");
    assert_eq!(
        loaded.execution.map(|block| block.mode),
        Some(ExecutionMode::Live)
    );

    fleet
        .set_mode(&engine, ExecutionMode::Sim)
        .expect("back to the shipped mode");
    assert_eq!(
        std::fs::read(&path).expect("read back"),
        shipped.as_bytes(),
        "a round trip restores the file byte for byte"
    );

    let edges = "link:\n  bind: \"127.0.0.1:9399\"\n  mode: x\nexecution:\r\n  # mode: off\n  \
                 nested:\n    mode: off\n  mode: sim # pick\r\n  max: 1\n";
    tree.config("strat-a", "te-edges", edges);
    let change = fleet
        .set_mode(&identity("strat-a", "te-edges"), ExecutionMode::Off)
        .expect("the execution child is found past every decoy");
    assert_eq!(change.previous, "sim");
    assert_eq!(
        std::fs::read_to_string(tree.config_path("strat-a", "te-edges")).expect("read back"),
        edges.replace("  mode: sim # pick\r\n", "  mode: off # pick\r\n"),
        "a commented mode, a nested mode and another block's mode are all left alone, and CRLF \
         endings are read as line ends and kept"
    );
}

#[test]
fn set_mode_never_reports_a_mode_the_file_does_not_read_as() {
    type IsExpected = fn(&FleetError) -> bool;
    let cases: [(&str, &str, IsExpected); 4] = [
        ("te-next-line", "execution:\n  mode:\n    live\n", |error| {
            matches!(error, FleetError::NoModeLine { .. })
        }),
        ("te-comment", "execution:\n  mode: # pick one\n", |error| {
            matches!(error, FleetError::ConfigUnreadable { .. })
        }),
        (
            "te-duplicate",
            "execution:\n  mode: off\n  mode: sim\n",
            |error| matches!(error, FleetError::ConfigUnreadable { .. }),
        ),
        ("te-anchored", "execution:\n  mode: &m sim\n", |error| {
            matches!(error, FleetError::RewriteNotVerified { .. })
        }),
    ];
    let tree = EngineTree::new("mcp-unverified");
    for (te, text, _) in &cases {
        tree.config("strat-a", te, text);
    }
    let mut fleet = tree.fleet(FAST);

    for (te, text, is_expected) in cases {
        let error = fleet
            .set_mode(&identity("strat-a", te), ExecutionMode::Live)
            .expect_err("this file must not be edited");
        assert!(is_expected(&error), "{te}: {error:?}");
        assert_eq!(
            std::fs::read_to_string(tree.config_path("strat-a", te)).expect("read back"),
            text,
            "{te}: a refused edit leaves the file untouched"
        );
    }

    let error = fleet
        .set_mode(&identity("strat-a", "te-missing"), ExecutionMode::Off)
        .expect_err("no such engine");
    assert!(
        matches!(error, FleetError::UnknownEngine { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn start_and_stop_walk_the_state_machine_on_a_real_child() {
    let tree = EngineTree::new("mcp-walk");
    tree.engine("strat-a", "te-off", MODE_OFF, TRAP_TERM);
    let mut fleet = tree.fleet(FAST);
    let engine = identity("strat-a", "te-off");

    let started = fleet
        .start(&engine)
        .await
        .expect("the child outlives the grace");
    assert!(started.pid > 0);
    assert_eq!(
        (
            started.strategy_id.as_str(),
            started.te_id.as_str(),
            started.mode
        ),
        ("strat-a", "te-off", "off")
    );
    assert!(
        started
            .console_log
            .ends_with("logs/strat-a-te-off-console.log"),
        "{}",
        started.console_log
    );

    let listing = fleet.list().expect("lists");
    let row = &listing.engines[0];
    assert_eq!(
        (row.state, row.pid, row.exit_code),
        ("running", Some(started.pid), None)
    );
    let started_at = row
        .started_at
        .as_deref()
        .expect("a running row carries its start time");
    assert!(
        is_utc_with_micros(started_at),
        "RFC3339 UTC with six fraction digits, got {started_at}"
    );
    assert_eq!(row.console_log, started.console_log);

    tree.wait_until_ready("strat-a-te-off");
    let again = fleet
        .start(&engine)
        .await
        .expect_err("one engine, one process");
    assert!(
        matches!(again, FleetError::AlreadyRunning { pid, .. } if pid == started.pid),
        "{again:?}"
    );
    assert!(again.to_string().contains("already running"), "{again}");

    let stopped = fleet
        .stop(&engine)
        .await
        .expect("the child traps TERM and exits 0");
    assert_eq!((stopped.exit_code, stopped.forced), (Some(0), false));
    assert!(
        Duration::from_millis(stopped.duration_ms) < FAST.stop_deadline,
        "SIGTERM alone ended it: {} ms",
        stopped.duration_ms
    );
    assert_eq!(
        (stopped.strategy_id.as_str(), stopped.te_id.as_str()),
        ("strat-a", "te-off")
    );

    let again = fleet.stop(&engine).await.expect_err("already stopped");
    assert!(matches!(again, FleetError::NotRunning { .. }), "{again:?}");
    assert!(again.to_string().contains("not running"), "{again}");

    let row = &fleet.list().expect("lists").engines[0];
    assert_eq!(
        (row.state, row.pid, row.started_at.as_deref()),
        ("stopped", None, None)
    );
}

#[tokio::test]
async fn start_refuses_an_unknown_engine_and_names_the_build_for_a_missing_binary() {
    let tree = EngineTree::new("mcp-refusals");
    tree.config("strat-a", "te-off", MODE_OFF);
    let mut fleet = tree.fleet(FAST);

    let unknown = fleet
        .start(&identity("strat-a", "te-nowhere"))
        .await
        .expect_err("no such folder");
    assert!(
        matches!(unknown, FleetError::UnknownEngine { .. }),
        "{unknown:?}"
    );
    assert!(unknown.to_string().contains("list_strategies"), "{unknown}");

    let missing = fleet
        .start(&identity("strat-a", "te-off"))
        .await
        .expect_err("no binary was built");
    assert!(
        matches!(missing, FleetError::BinaryMissing { .. }),
        "{missing:?}"
    );
    assert!(
        missing
            .to_string()
            .contains("cargo build --release --bin strat-a-te-off"),
        "the error is the command that fixes it: {missing}"
    );
}

#[tokio::test]
async fn start_refuses_an_unreadable_config_before_spawning() {
    let tree = EngineTree::new("mcp-unreadable-start");
    let witness = tree.root().join("spawned");
    let script = format!("#!/bin/sh\ntouch {}\n", witness.display());
    tree.engine("strat-a", "te-broken", UNPARSEABLE, &script);
    let mut fleet = tree.fleet(FAST);

    let error = fleet
        .start(&identity("strat-a", "te-broken"))
        .await
        .expect_err("the config does not parse");
    assert!(
        matches!(error, FleetError::ConfigUnreadable { .. }),
        "{error:?}"
    );
    let listed = fleet.list().expect("lists");
    assert_eq!(
        listed.engines[0].config_error.as_deref(),
        Some(error.to_string().as_str()),
        "the refusal and the listing say the same thing"
    );
    std::thread::sleep(Duration::from_millis(200));
    assert!(!witness.exists(), "nothing was spawned");
    assert!(
        !tree.root().join("logs").exists(),
        "no console log was opened"
    );
}

#[tokio::test]
async fn an_engine_that_dies_during_start_up_reports_its_code_and_output() {
    let tree = EngineTree::new("mcp-early-exit");
    tree.engine("strat-a", "te-off", MODE_OFF, EXITS_THREE);
    let timing = FleetTiming {
        start_grace: FRESH_SCRIPT_DEADLINE,
        ..FAST
    };
    let mut fleet = tree.fleet(timing);

    let began = Instant::now();
    let error = fleet
        .start(&identity("strat-a", "te-off"))
        .await
        .expect_err("the child exits 3");
    assert!(
        began.elapsed() < timing.start_grace,
        "the call returns as soon as the child exits, not at the end of the grace"
    );
    let FleetError::ExitedEarly { status, tail, .. } = &error else {
        panic!("expected an early exit, got {error:?}");
    };
    assert_eq!(status.code(), Some(3));
    assert!(
        tail.contains("config refused"),
        "the tail carries the child's own words: {tail}"
    );

    let row = &fleet.list().expect("lists").engines[0];
    assert_eq!(
        (row.state, row.exit_code, row.pid),
        ("exited", Some(3), None)
    );
}

#[tokio::test]
async fn stop_forces_only_an_engine_that_ignores_sigterm() {
    let tree = EngineTree::new("mcp-force");
    tree.engine("strat-a", "te-off", MODE_OFF, IGNORE_TERM);
    let mut fleet = tree.fleet(FAST);
    let engine = identity("strat-a", "te-off");

    fleet.start(&engine).await.expect("starts");
    tree.wait_until_ready("strat-a-te-off");
    let stopped = fleet.stop(&engine).await.expect("SIGKILL ends it");
    assert_eq!((stopped.forced, stopped.exit_code), (true, None));
    assert!(
        Duration::from_millis(stopped.duration_ms) >= FAST.stop_deadline,
        "SIGTERM got the whole deadline first: {} ms",
        stopped.duration_ms
    );
}

#[tokio::test]
async fn child_output_lands_in_the_console_log_and_never_in_the_server_streams() {
    let tree = EngineTree::new("mcp-console");
    tree.engine("strat-a", "te-off", MODE_OFF, MARKERS);
    let mut fleet = tree.fleet(FAST);
    let engine = identity("strat-a", "te-off");
    let log = tree.console_log("strat-a-te-off");

    for run in 1..=2 {
        fleet.start(&engine).await.expect("starts");
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
        fleet.stop(&engine).await.expect("stops");
    }
}

#[tokio::test]
async fn stop_all_stops_every_child_it_started() {
    let tree = EngineTree::new("mcp-stop-all");
    tree.engine("strat-a", "te-polite", MODE_OFF, TRAP_TERM)
        .engine("strat-a", "te-stubborn", MODE_OFF, IGNORE_TERM);
    let mut fleet = tree.fleet(FAST);
    fleet
        .start(&identity("strat-a", "te-polite"))
        .await
        .expect("starts");
    fleet
        .start(&identity("strat-a", "te-stubborn"))
        .await
        .expect("starts");
    tree.wait_until_ready("strat-a-te-polite");
    tree.wait_until_ready("strat-a-te-stubborn");

    let began = Instant::now();
    let outcomes = fleet.stop_all().await;
    assert!(
        began.elapsed() < FAST.stop_deadline * 2,
        "one deadline is shared, so two engines cost one deadline, not two"
    );
    let forced: Vec<(String, bool)> = outcomes
        .iter()
        .map(|outcome| {
            let stopped = outcome.result.as_ref().expect("each child stops");
            (outcome.identity.to_string(), stopped.forced)
        })
        .collect();
    assert_eq!(
        forced,
        vec![
            ("strat-a-te-polite".to_owned(), false),
            ("strat-a-te-stubborn".to_owned(), true)
        ]
    );
    let states: Vec<&str> = fleet
        .list()
        .expect("lists")
        .engines
        .iter()
        .map(|row| row.state)
        .collect();
    assert_eq!(states, vec!["stopped", "stopped"]);
}

#[tokio::test]
async fn set_mode_refuses_while_running_and_without_a_mode_line() {
    let tree = EngineTree::new("mcp-mode-locked");
    tree.engine("strat-a", "te-off", MODE_OFF, TRAP_TERM)
        .config("strat-a", "te-bare", NO_EXECUTION);
    let mut fleet = tree.fleet(FAST);
    let engine = identity("strat-a", "te-off");

    let started = fleet.start(&engine).await.expect("starts");
    let error = fleet
        .set_mode(&engine, ExecutionMode::Live)
        .expect_err("the engine reads its mode only at boot");
    assert!(
        matches!(error, FleetError::ModeLockedWhileRunning { pid, .. } if pid == started.pid),
        "{error:?}"
    );
    assert!(error.to_string().contains("stop it first"), "{error}");
    assert_eq!(
        std::fs::read_to_string(tree.config_path("strat-a", "te-off")).expect("read back"),
        MODE_OFF
    );
    fleet.stop(&engine).await.expect("stops");
    fleet
        .set_mode(&engine, ExecutionMode::Sim)
        .expect("a stopped engine's mode can change");

    let error = fleet
        .set_mode(&identity("strat-a", "te-bare"), ExecutionMode::Live)
        .expect_err("no execution block");
    assert!(matches!(error, FleetError::NoModeLine { .. }), "{error:?}");
    assert!(
        error.to_string().contains("add the block by hand"),
        "{error}"
    );
    assert_eq!(
        std::fs::read_to_string(tree.config_path("strat-a", "te-bare")).expect("read back"),
        NO_EXECUTION
    );
}

#[tokio::test]
async fn a_running_row_reports_what_the_engine_booted_with() {
    let tree = EngineTree::new("mcp-boot-mode");
    tree.engine("strat-a", "te-off", MODE_OFF, TRAP_TERM);
    let mut fleet = tree.fleet(FAST);
    let engine = identity("strat-a", "te-off");
    fleet.start(&engine).await.expect("starts");
    // The engine is a shell script, so a live mode here trades nothing.
    let edited = "execution:\n  mode: live\nlink:\n  bind: \"127.0.0.1:9400\"\n";
    tree.config("strat-a", "te-off", edited);

    let row = &fleet.list().expect("lists").engines[0];
    assert_eq!(
        (
            row.state,
            row.mode,
            row.link_bind.as_deref(),
            row.config_error.as_deref()
        ),
        ("running", "off", Some(LINK_BIND), None),
        "a hand edit made while the engine runs changes nothing it is running with"
    );

    tree.wait_until_ready("strat-a-te-off");
    fleet.stop(&engine).await.expect("stops");
    let row = &fleet.list().expect("lists").engines[0];
    assert_eq!(
        (row.state, row.mode, row.link_bind.as_deref()),
        ("stopped", "live", Some("127.0.0.1:9400")),
        "a stopped row reads the file that the next start boots from"
    );
}

#[tokio::test]
async fn a_running_engine_keeps_its_row_when_its_folder_moves() {
    let tree = EngineTree::new("mcp-moved-folder");
    tree.engine("strat-a", "te-off", MODE_OFF, TRAP_TERM);
    let mut fleet = tree.fleet(FAST);
    let engine = identity("strat-a", "te-off");
    let started = fleet.start(&engine).await.expect("starts");
    tree.wait_until_ready("strat-a-te-off");
    let folder = tree.root().join("strategies/strat-a/te-off");
    std::fs::rename(&folder, folder.with_file_name("te-renamed")).expect("rename");

    let listing = fleet.list().expect("lists");
    let rows: Vec<(&str, &str, Option<u32>)> = listing
        .engines
        .iter()
        .map(|row| (row.te_id.as_str(), row.state, row.pid))
        .collect();
    assert_eq!(
        rows,
        vec![
            ("te-off", "running", Some(started.pid)),
            ("te-renamed", "stopped", None)
        ],
        "a running engine stays listed without its folder, in sorted order, so a client can \
         still stop it"
    );
    let moved = &listing.engines[0];
    assert_eq!(
        (
            moved.bin.as_str(),
            moved.mode,
            moved.link_bind.as_deref(),
            moved.config_error.as_deref()
        ),
        ("strat-a-te-off", "off", Some(LINK_BIND), None)
    );
    assert_eq!(moved.console_log, started.console_log);

    let stopped = fleet.stop(&engine).await.expect("stops by id");
    assert_eq!((stopped.exit_code, stopped.forced), (Some(0), false));
    let after = fleet.list().expect("lists");
    let remaining: Vec<&str> = after.engines.iter().map(|row| row.te_id.as_str()).collect();
    assert_eq!(
        remaining,
        vec!["te-renamed"],
        "once stopped, an engine with no folder is gone"
    );
}

#[tokio::test]
async fn a_start_after_stop_all_is_refused() {
    let tree = EngineTree::new("mcp-closed");
    tree.engine("strat-a", "te-off", MODE_OFF, TRAP_TERM);
    let mut fleet = tree.fleet(FAST);

    assert!(fleet.stop_all().await.is_empty(), "nothing was running");
    let error = fleet
        .start(&identity("strat-a", "te-off"))
        .await
        .expect_err("the fleet is closed");
    assert!(matches!(error, FleetError::ShuttingDown), "{error:?}");
    assert!(
        !tree.console_log("strat-a-te-off").exists(),
        "a refused start opens no console log"
    );
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
