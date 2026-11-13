//! A thread that cannot pin refuses the start before any writer, timer, edge or sender exists, and
//! each thread that started before it has ended when the start returns.

use std::cell::RefCell;
use std::fs::{self, File};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use engine::msg::failure::{FailureClass, Halt};
use engine::runtime::config::{Config, NoParams, RunIdentity};
use engine::runtime::{Engine, PinRefused, Preflight, StartError};

use super::config_guards::{BINANCE_SPOT_SOURCE, SIM_BLOCK, execution_block_with};
use super::preflight::{Scales, StandInVenue, snapshot};
use crate::engine_support::Idle;
use crate::parquet_readback::TempDir;

const STRATEGY: &str = "strat-start-order";
const ENGINE: &str = "te-pins";
const HOT_CORE: usize = 3;
const HOUSEKEEPING_CORE: usize = 5;
const SENDER_CORE: usize = 7;

/// Set in the process that runs the cases. The last case starts the log, which a process starts
/// only once and which then reaches each thread of that process, so no other test may share it.
const CASES_PROCESS: &str = "POLYSIM_START_ORDER_CASES";
const THIS_TEST: &str = "runtime::start_order::pin_failure_refuses_start_before_any_producer";

/// The core that the stand-in refuses. Only this test pins through the stand-in, and it runs its
/// cases one after the other.
static REFUSED_CORE: AtomicUsize = AtomicUsize::new(usize::MAX);
static PIN_REQUESTS: Mutex<Vec<PinRequest>> = Mutex::new(Vec::new());

/// One call of the stand-in: the core, the thread that asked, and whether that thread has ended.
struct PinRequest {
    core: usize,
    thread: String,
    has_ended: Arc<AtomicBool>,
}

thread_local! {
    static END_OF_THREAD: RefCell<Option<EndOfThread>> = const { RefCell::new(None) };
}

/// Dropped with the locals of its thread, which happens before a join of that thread returns.
struct EndOfThread(Arc<AtomicBool>);

impl Drop for EndOfThread {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// The OS call that pins a thread, as this test sees it: it notes who asked and refuses one core.
fn stand_in_pin(core_id: usize) -> Result<(), PinRefused> {
    let has_ended = Arc::new(AtomicBool::new(false));
    END_OF_THREAD.with(|slot| *slot.borrow_mut() = Some(EndOfThread(Arc::clone(&has_ended))));
    let thread = std::thread::current()
        .name()
        .unwrap_or("unnamed")
        .to_owned();
    PIN_REQUESTS
        .lock()
        .expect("no case panics while it holds the requests")
        .push(PinRequest {
            core: core_id,
            thread,
            has_ended,
        });
    if core_id == REFUSED_CORE.load(Ordering::Acquire) {
        return Err(PinRefused { core_id });
    }
    Ok(())
}

fn identity() -> RunIdentity {
    RunIdentity::new(STRATEGY, ENGINE).expect("a valid identity")
}

/// A simulated Binance spot engine whose hot thread, housekeeping threads and senders each have a
/// core, with each folder below `root`.
fn document(root: &Path) -> String {
    let execution = execution_block_with("sim", SIM_BLOCK);
    format!(
        "engine:
  hot_core_id: {HOT_CORE}
  housekeeping_core_id: {HOUSEKEEPING_CORE}
  spin_interval_us: 100000
queues:
  input_capacity: 1024
  persistence_capacity: 1024
source:
{BINANCE_SPOT_SOURCE}strategy:
  instruments: all
persistence:
  dir: \"{root}/tape\"
  retention_hours: 1
{execution}senders:
  placement: pinned
  core_id: {SENDER_CORE}
logging:
  dir: \"{root}/logs\"
  ring_capacity: 16
  drain_deadline_ms: 500
exposure:
  dir: \"{root}/state\"
",
        root = root.display()
    )
}

/// A tape file older than the retention of the document, which the persistence writer deletes as
/// soon as it starts.
fn plant_old_tape(root: &Path) -> std::path::PathBuf {
    let path = root
        .join("tape")
        .join(STRATEGY)
        .join(ENGINE)
        .join("sim/trades/date=1969-12-31/00-old-0.parquet");
    fs::create_dir_all(path.parent().expect("a tape file sits in a date folder"))
        .expect("the test makes the date folder");
    fs::write(&path, b"old tape").expect("the test writes the old tape file");
    File::options()
        .write(true)
        .open(&path)
        .expect("the old tape file opens")
        .set_modified(SystemTime::now() - Duration::from_hours(2))
        .expect("the test backdates the old tape file");
    path
}

/// On Linux, the names of the threads of this process that only an engine starts. `None`
/// elsewhere, where no such list exists without unsafe code.
fn engine_thread_names() -> Option<Vec<String>> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let tasks = fs::read_dir("/proc/self/task").expect("linux lists the threads of a process");
    let mut names: Vec<String> = tasks
        .filter_map(|task| fs::read_to_string(task.ok()?.path().join("comm")).ok())
        .map(|name| name.trim().to_owned())
        .filter(|name| name.starts_with("polysim-") || name == "persist" || name == "exposure")
        .collect();
    names.sort();
    Some(names)
}

/// The cases run in a process of their own, so each engine thread there is one that a case
/// started. A thread that a join has seen end can still be listed for a moment, so the census waits
/// up to 2 s for an added name to go.
fn assert_no_engine_thread_added(before: Option<&[String]>) {
    let Some(before) = before else {
        return;
    };
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let after = engine_thread_names().expect("the census ran before the start");
        let mut left = before.to_vec();
        let added: Vec<String> = after
            .into_iter()
            .filter(|name| match left.iter().position(|seen| seen == name) {
                Some(index) => {
                    left.remove(index);
                    false
                }
                None => true,
            })
            .collect();
        if added.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the refused start left engine threads running: {added:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Preflight on the document, then the start, with the stand-in refusing `refused_core`. Holds what
/// each case must leave: the class of a refused start, each thread that asked for a pin ended, the
/// old tape file in place, no file written to the state folder by the start, and the nonce that
/// preflight spent.
fn refused_start(root: &Path, refused_core: usize) -> (StartError, Vec<(usize, String)>) {
    let config = Config::<NoParams>::from_yaml(&document(root)).expect("the document is valid");
    let state = root.join("state");
    let old_tape = plant_old_tape(root);
    let plan = Preflight {
        identity: &identity(),
        config: &config,
        strategy: &Idle,
    }
    .run_with(&mut StandInVenue {
        scales: Scales::Stamped {
            max_num_orders: 200,
        },
    })
    .expect("preflight passes: a pin is no check of preflight");
    let spent = snapshot(&state);
    assert!(
        spent.keys().any(|name| name.ends_with(".nonce")),
        "preflight took the lease and spent a run nonce: {:?}",
        spent.keys()
    );

    REFUSED_CORE.store(refused_core, Ordering::Release);
    let census_before = engine_thread_names();
    let started = Engine::start(config, Box::new(Idle), plan, stand_in_pin);
    let Err(refused) = started else {
        panic!("a refused pin on core {refused_core} must refuse the start");
    };
    let requests = std::mem::take(
        &mut *PIN_REQUESTS
            .lock()
            .expect("no case panics while it holds the requests"),
    );
    for request in &requests {
        assert!(
            request.has_ended.load(Ordering::Acquire),
            "the {} thread had ended when the start returned {refused}",
            request.thread
        );
    }
    assert_no_engine_thread_added(census_before.as_deref());
    assert_eq!(refused.failure_class(), FailureClass::Halt(Halt::Start));
    assert!(
        old_tape.exists(),
        "no persistence writer started, so the old tape file is still there: {refused}"
    );
    assert_eq!(
        snapshot(&state),
        spent,
        "the refused start wrote no file to the state folder: {refused}"
    );
    let asked = requests
        .into_iter()
        .map(|request| (request.core, request.thread))
        .collect();
    (refused, asked)
}

fn requests(expected: &[(usize, &str)]) -> Vec<(usize, String)> {
    expected
        .iter()
        .map(|(core, thread)| (*core, (*thread).to_owned()))
        .collect()
}

/// CONTRACT: the hot thread pins as its first act and reports the result, so a refused hot pin
/// ends the start before any other thread exists. Each pin that can refuse comes before the first
/// writer, in the order hot, log, metrics, senders, and a refusal after the hot pin stops each
/// thread that started before it. The cases run in a process of their own, which runs this test
/// alone.
#[test]
fn pin_failure_refuses_start_before_any_producer() {
    if std::env::var_os(CASES_PROCESS).is_some() {
        return each_refused_start();
    }
    let suite = std::env::current_exe().expect("the suite knows the path of its own binary");
    let run = Command::new(suite)
        .args(["--exact", THIS_TEST, "--test-threads=1", "--nocapture"])
        .env(CASES_PROCESS, "1")
        .output()
        .expect("the suite starts its own binary again");
    let stdout = String::from_utf8_lossy(&run.stdout);
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(
        run.status.success() && stdout.contains("test result: ok. 1 passed"),
        "the cases of the start order failed in their own process:\n{stdout}\n{stderr}"
    );
}

/// Only the last case starts the log.
fn each_refused_start() {
    let root = TempDir::new("start-order");

    let (hot, asked) = refused_start(root.path(), HOT_CORE);
    assert!(
        matches!(hot, StartError::HotPin { core: HOT_CORE }),
        "the hot pin refuses with its core: {hot}"
    );
    assert_eq!(
        asked,
        requests(&[(HOT_CORE, "polysim-hot")]),
        "the hot thread is the only thread that the refused start began"
    );

    let (log, asked) = refused_start(root.path(), HOUSEKEEPING_CORE);
    assert!(
        matches!(
            log,
            StartError::HousekeepingPin {
                core_id: HOUSEKEEPING_CORE,
                thread: "log"
            }
        ),
        "the log thread pins first of the housekeeping threads: {log}"
    );
    assert_eq!(
        asked,
        requests(&[
            (HOT_CORE, "polysim-hot"),
            (HOUSEKEEPING_CORE, "polysim-log")
        ]),
        "the metrics thread never started"
    );

    let (sender, asked) = refused_start(root.path(), SENDER_CORE);
    assert!(
        matches!(
            sender,
            StartError::SenderPin {
                core_id: SENDER_CORE
            }
        ),
        "the sender thread pins after the log and the metrics thread: {sender}"
    );
    assert_eq!(
        asked,
        requests(&[
            (HOT_CORE, "polysim-hot"),
            (HOUSEKEEPING_CORE, "polysim-log"),
            (HOUSEKEEPING_CORE, "polysim-metrics"),
            (SENDER_CORE, "polysim-sender"),
        ]),
        "each pin that can refuse comes before the first writer, the runtime and the edges"
    );
}
