//! Contract tests of the fleet against real files and real processes: an engine that another
//! process runs, a list that a stop does not hold up, and a config edit that fails.

use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rmcp::model::{CallToolRequestParams, CallToolResult};
use rmcp::service::RunningService;
use rmcp::{RoleClient, ServiceExt};
use serde_json::{Value, json};

use engine::operator::fleet::{FleetError, FleetTiming, PolysimMcp};
use engine::runtime::config::ExecutionMode;
use engine::runtime::durable_file::{DiskFiles, FileSteps, StepFile};

use crate::operator::fleet::mcp_support::{
    EngineTree, FAST, IGNORE_TERM, MODE_OFF, identity, reply_of, text_of,
};

/// A stand-in engine that leaves a mark when it runs and ends on its own after thirty seconds.
fn marking_script(witness: &Path) -> String {
    format!(
        "#!/bin/sh\ntouch {}\ntrap 'exit 0' TERM\necho ready\ni=0; while [ $i -lt 600 ]; do sleep \
         0.05; i=$((i+1)); done\n",
        witness.display()
    )
}

/// CONTRACT: an engine whose identity lock another process holds is running, though this fleet
/// did not start it. The list says so, and a start is refused and makes no second process.
#[tokio::test]
async fn engine_started_elsewhere_is_listed_running_and_start_is_refused() {
    let tree = EngineTree::new("fleet-elsewhere");
    let witness = tree.root().join("spawned");
    tree.engine("strat-l17", "te-held", MODE_OFF, &marking_script(&witness));
    let held_identity = identity("strat-l17", "te-held");
    let _held = tree
        .locks()
        .take(&held_identity)
        .expect("the test holds the identity lock, as an engine started elsewhere does");
    let mut fleet = tree.fleet(FAST);

    let listing = fleet.list().expect("the tree lists");
    let row = &listing.engines[0];
    assert_eq!(
        (row.state, row.pid),
        ("running elsewhere", None),
        "a held identity lock is a running engine"
    );

    let refused = fleet
        .start(&held_identity)
        .await
        .expect_err("one identity, one process");
    assert!(
        matches!(refused, FleetError::RunningElsewhere { .. }),
        "{refused:?}"
    );
    assert!(
        refused.to_string().contains("strat-l17-te-held"),
        "the refusal names the engine: {refused}"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !witness.exists(),
        "the refused start made no second process"
    );
}

/// CONTRACT: the stop of the exit closes the fleet to starts. A stop of one engine waits for it
/// without the lock of the fleet, so this guard is what keeps an engine from starting while the
/// server shuts down and outliving it.
#[tokio::test]
async fn a_start_after_stop_all_is_refused() {
    let tree = EngineTree::new("fleet-closed");
    let witness = tree.root().join("spawned");
    tree.engine(
        "strat-closed",
        "te-off",
        MODE_OFF,
        &marking_script(&witness),
    );
    let mut fleet = tree.fleet(FAST);

    assert!(fleet.stop_all().await.is_empty(), "nothing was running");
    let error = fleet
        .start(&identity("strat-closed", "te-off"))
        .await
        .expect_err("the fleet is closed");
    assert!(matches!(error, FleetError::ShuttingDown), "{error:?}");
    assert!(
        !tree.console_log("strat-closed-te-off").exists(),
        "a refused start opens no console log"
    );
}

/// One client over an in-memory pipe, so two of them can call the same server at once.
struct Session {
    client: RunningService<RoleClient, ()>,
}

impl Session {
    async fn open(server: PolysimMcp) -> Session {
        let (server_side, client_side) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            server
                .serve(server_side)
                .await
                .expect("the server initializes")
                .waiting()
                .await
        });
        let client = ().serve(client_side).await.expect("the client initializes");
        Session { client }
    }

    async fn call(&self, tool: &str, arguments: Value) -> CallToolResult {
        let arguments = arguments
            .as_object()
            .expect("arguments are an object")
            .clone();
        let params = CallToolRequestParams::new(tool.to_owned()).with_arguments(arguments);
        self.client
            .call_tool(params)
            .await
            .expect("a refusal is a tool result, never a protocol error")
    }
}

/// CONTRACT: a stop waits for its engine outside the lock of the fleet, so a list from another
/// client answers at once and shows the engine as stopping, and a start of it is refused.
#[tokio::test(flavor = "multi_thread")]
async fn list_during_a_stop_answers_before_the_stop_ends() {
    let tree = EngineTree::new("fleet-list-in-stop");
    tree.engine("strat-l18", "te-stubborn", MODE_OFF, IGNORE_TERM);
    let timing = FleetTiming {
        stop_deadline: Duration::from_secs(4),
        ..FAST
    };
    let server = PolysimMcp::new(tree.fleet(timing));
    let stopper = Session::open(server.clone()).await;
    let lister = Session::open(server).await;
    let stubborn = json!({"strategy_id": "strat-l18", "te_id": "te-stubborn"});
    let started = reply_of(&stopper.call("start_strategy", stubborn.clone()).await);
    tree.wait_until_ready("strat-l18-te-stubborn");

    let stop_arguments = stubborn.clone();
    let stopping = tokio::spawn(async move { stopper.call("stop_strategy", stop_arguments).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let began = Instant::now();
    let listed = reply_of(&lister.call("list_strategies", json!({})).await);
    let answered_in = began.elapsed();
    assert!(
        !stopping.is_finished() && answered_in < Duration::from_secs(1),
        "the list answered in {answered_in:?}, before the stop ended"
    );
    let row = &listed["engines"][0];
    assert_eq!(
        (&row["state"], &row["pid"]),
        (&json!("stopping"), &started["pid"]),
        "an engine that is being stopped keeps its row"
    );
    let refused = lister.call("start_strategy", stubborn).await;
    assert_eq!(refused.is_error, Some(true), "{refused:?}");
    assert!(
        text_of(&refused).contains("is stopping"),
        "a start waits for the stop: {}",
        text_of(&refused)
    );

    let stopped = reply_of(&stopping.await.expect("the stop task joins"));
    assert_eq!(stopped["forced"], true);
    let listed = reply_of(&lister.call("list_strategies", json!({})).await);
    assert_eq!(listed["engines"][0]["state"], "stopped");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    Create,
    Write,
    Sync,
    Rename,
    SyncFolder,
}

/// The real file system, except that one step of a write fails, as a full or broken disk refuses
/// it. It can refuse to remove a file too, so a cleanup fails beside the step.
struct FailsAt {
    step: Step,
    refuses_remove: bool,
}

impl FailsAt {
    fn check(&self, step: Step) -> io::Result<()> {
        if step == self.step {
            return Err(io::Error::other(format!("the disk refused the {step:?}")));
        }
        Ok(())
    }
}

impl FileSteps for FailsAt {
    fn create_folder(&self, folder: &Path) -> io::Result<()> {
        DiskFiles.create_folder(folder)
    }

    fn create(&self, path: &Path) -> io::Result<Box<dyn StepFile>> {
        self.check(Step::Create)?;
        let file = DiskFiles.create(path)?;
        Ok(Box::new(FailingFile {
            file,
            write: self.check(Step::Write).is_err(),
            sync: self.check(Step::Sync).is_err(),
        }))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.check(Step::Rename)?;
        DiskFiles.rename(from, to)
    }

    fn sync_folder(&self, folder: &Path) -> io::Result<()> {
        self.check(Step::SyncFolder)?;
        DiskFiles.sync_folder(folder)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        if self.refuses_remove {
            return Err(io::Error::other("the disk refused the remove"));
        }
        DiskFiles.remove(path)
    }
}

struct FailingFile {
    file: Box<dyn StepFile>,
    write: bool,
    sync: bool,
}

impl Write for FailingFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.write {
            return Err(io::Error::other("the disk refused the Write"));
        }
        self.file.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl StepFile for FailingFile {
    fn sync(&mut self) -> io::Result<()> {
        if self.sync {
            return Err(io::Error::other("the disk refused the Sync"));
        }
        self.file.sync()
    }
}

fn folder_names(folder: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(folder)
        .expect("the engine folder lists")
        .map(|entry| {
            entry
                .expect("a readable entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

/// CONTRACT: a config edit that fails at any step up to the rename leaves the config and its folder
/// as they were, and a cleanup that fails too is reported beside the step that failed. A folder
/// sync that fails after the rename leaves the edit done, and says that it may not be durable.
#[test]
fn failed_config_edit_changes_nothing() {
    for step in [Step::Create, Step::Write, Step::Sync, Step::Rename] {
        let tree = EngineTree::new("fleet-failed-edit");
        tree.config("strat-l19", "te-x", MODE_OFF);
        let path = tree.config_path("strat-l19", "te-x");
        let folder = path.parent().expect("a config has a folder").to_path_buf();
        let mut fleet = tree.fleet(FAST).with_file_steps(Arc::new(FailsAt {
            step,
            refuses_remove: false,
        }));

        let error = fleet
            .set_mode(&identity("strat-l19", "te-x"), ExecutionMode::Live)
            .expect_err("a step of the write fails");
        assert!(
            matches!(error, FleetError::ConfigWrite { .. }),
            "{step:?}: {error:?}"
        );
        assert_eq!(
            std::fs::read_to_string(&path).expect("read back"),
            MODE_OFF,
            "{step:?}: the config is as it was"
        );
        assert_eq!(
            folder_names(&folder),
            ["config.yaml"],
            "{step:?}: no staging file stays beside the config"
        );
    }

    let tree = EngineTree::new("fleet-failed-cleanup");
    tree.config("strat-l19", "te-x", MODE_OFF);
    let path = tree.config_path("strat-l19", "te-x");
    let mut fleet = tree.fleet(FAST).with_file_steps(Arc::new(FailsAt {
        step: Step::Rename,
        refuses_remove: true,
    }));
    let error = fleet
        .set_mode(&identity("strat-l19", "te-x"), ExecutionMode::Live)
        .expect_err("the rename fails");
    let text = chain_of(&error);
    assert!(
        text.contains("the disk refused the Rename")
            && text.contains("the disk refused the remove"),
        "both errors are reported: {text}"
    );
    assert_eq!(std::fs::read_to_string(&path).expect("read back"), MODE_OFF);

    let tree = EngineTree::new("fleet-unsynced-edit");
    tree.config("strat-l19", "te-x", MODE_OFF);
    let path = tree.config_path("strat-l19", "te-x");
    let mut fleet = tree.fleet(FAST).with_file_steps(Arc::new(FailsAt {
        step: Step::SyncFolder,
        refuses_remove: false,
    }));
    let error = fleet
        .set_mode(&identity("strat-l19", "te-x"), ExecutionMode::Live)
        .expect_err("the folder sync fails after the rename");
    let text = chain_of(&error);
    assert!(
        text.contains("the disk refused the SyncFolder") && text.contains("not durable"),
        "an edit that is done but not synced says so: {text}"
    );
    assert_eq!(
        std::fs::read_to_string(&path).expect("read back"),
        MODE_OFF.replace("mode: off", "mode: live"),
        "the rename was done, so the new mode is on disk"
    );
}

fn chain_of(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(inner) = cause {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        cause = inner.source();
    }
    text
}
