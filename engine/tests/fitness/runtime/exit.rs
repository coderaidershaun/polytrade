//! How a run ends: each way gives one exit and one code, and only a clean drain after a request to
//! stop gives code 0, so an operator never reads a fatal or a lost order as a clean stop.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow_schema::ArrowError;
use engine::msg::exec::{CancelReason, Provenance};
use engine::msg::failure::{FailureClass, Halt};
use engine::msg::ids::{ClientOrderId, InstrumentId, Price, Qty, QueueId, Side, SourceId, TradeId};
use engine::msg::time::LocalTs;
use engine::runtime::config::{Config, ConfigError, NoParams, RunIdentity};
use engine::runtime::exposure::ExposureError;
#[cfg(feature = "otlp")]
use engine::runtime::otlp::OtlpError;
use engine::runtime::shutdown::{AnswerPart, FatalReason, FatalSignal};
use engine::runtime::{
    DrainDeadline, DrainFailure, DrainOutcome, DrainWatchdog, ExecutionLease, Exit, Preflight,
    StartError, Trigger,
};
use engine::tape::PersistError;
use engine::venues::VenueError;
use engine::venues::binance::exec::ProbeError;
use engine::venues::exec::{
    ExecCore, ExitPlan, MirroredOrder, SessionOutcome, SweepFailure, TeTag,
};
use engine::venues::sim;
use parquet::errors::ParquetError;

use super::preflight::{Scales, StandInVenue, sim_document, snapshot};
use crate::engine_support::Idle;
use crate::parquet_readback::TempDir;

const REQUESTS: [&str; 3] = ["SIGTERM", "SIGINT", "shutdown request"];

/// One value of each variant, listed in the order of `variant_index`.
fn each_fatal_reason() -> [FatalReason; 15] {
    [
        FatalReason::InputQueueFull {
            queue: QueueId::new(0).unwrap(),
            capacity: 65_536,
        },
        FatalReason::MarketTapFull {
            queue: QueueId::new(1).unwrap(),
            capacity: 1_024,
        },
        FatalReason::Panic,
        FatalReason::MirrorRetainFailed { venue: "binance" },
        FatalReason::PlacementFailedClosed {
            venue: "polymarket",
            client_id: ClientOrderId(7),
        },
        FatalReason::SimulatorCommandLaneFull { capacity: 4_096 },
        FatalReason::RestWorkerGone { venue: "binance" },
        FatalReason::UndecodableAnswer {
            venue: "binance",
            part: AnswerPart::Response,
        },
        FatalReason::SubscribeRefused {
            venue: "binance",
            failures: 3,
        },
        FatalReason::EpochExhausted {
            source_id: SourceId(2),
        },
        FatalReason::MarketDataUnreadable {
            source_id: SourceId(0),
        },
        FatalReason::UndecodableOpenOrder {
            venue: "binance",
            venue_order_id: 12_345,
        },
        FatalReason::OrderIndexFull {
            venue: "polymarket",
        },
        FatalReason::SubscribeUnbuildable {
            venue: "polymarket",
        },
        FatalReason::SettlementFailed {
            venue: "polymarket",
            trade: TradeId(99),
        },
    ]
}

/// The match has no catch-all arm, so a new variant does not compile here until the list above
/// holds a value of it.
fn variant_index(reason: FatalReason) -> usize {
    match reason {
        FatalReason::InputQueueFull { .. } => 0,
        FatalReason::MarketTapFull { .. } => 1,
        FatalReason::Panic => 2,
        FatalReason::MirrorRetainFailed { .. } => 3,
        FatalReason::PlacementFailedClosed { .. } => 4,
        FatalReason::SimulatorCommandLaneFull { .. } => 5,
        FatalReason::RestWorkerGone { .. } => 6,
        FatalReason::UndecodableAnswer { .. } => 7,
        FatalReason::SubscribeRefused { .. } => 8,
        FatalReason::EpochExhausted { .. } => 9,
        FatalReason::MarketDataUnreadable { .. } => 10,
        FatalReason::UndecodableOpenOrder { .. } => 11,
        FatalReason::OrderIndexFull { .. } => 12,
        FatalReason::SubscribeUnbuildable { .. } => 13,
        FatalReason::SettlementFailed { .. } => 14,
    }
}

/// One value of each variant of a refused start, listed in the order of `start_variant_index`.
fn each_start_refusal() -> Vec<StartError> {
    let path = || std::path::PathBuf::from("state/te.nonce");
    let io = || std::io::Error::other("refused");
    let refusals = vec![
        StartError::Config(ConfigError::SpinWithoutHotCore),
        StartError::Exposure(ExposureError::Malformed {
            path: path(),
            detail: "not a ledger".into(),
        }),
        StartError::Venue(VenueError::BinanceGate(ProbeError::TradingDisabled)),
        StartError::ExecutionNotWired {
            mode: "live",
            detail: "the startup permission probe was never run",
        },
        StartError::ExecutionCapacity {
            table: "order slots",
            needed: 65_600,
            addressable: 65_536,
        },
        StartError::ExecutionFieldInert {
            field: "quote_stop_margin_ms",
            value: "9000".into(),
            venue_fact: "this venue's instruments never expire",
        },
        StartError::ExecutionBaseFloorOnPosition {
            value: "0.1".into(),
        },
        StartError::ExecutionOrderCapacity {
            instrument: 0,
            symbol: "BTCUSDT".into(),
            configured_per_side: 4,
            required_total: 8,
            venue_max: 2,
        },
        StartError::ExecutionIdentityInUse { path: path() },
        StartError::ExecutionIdentityIo {
            path: path(),
            source: io(),
        },
        StartError::ExecutionIdentityState {
            path: path(),
            value: "garbage".into(),
        },
        StartError::ExecutionIdentityExhausted { path: path() },
        StartError::Runtime(io()),
        StartError::CoreId {
            core_id: 4_096,
            available: 8,
        },
        StartError::HotPin { core: 3 },
        StartError::HousekeepingPin {
            core_id: 5,
            thread: "log",
        },
        StartError::SenderPin { core_id: 7 },
        StartError::LinkBind {
            bind: "127.0.0.1:9000".into(),
            trading_engine: "strat-te".into(),
            source: io(),
        },
        StartError::LinkNameTooLong {
            name: "a feature".into(),
            found: 80,
            max: 64,
        },
        StartError::ExecutionNonceEmpty { path: path() },
        StartError::ExecutionHttpTimeout {
            venue: "binance",
            http_timeout_ms: 4_000,
            inflight_timeout_ms: 4_000,
        },
        StartError::ExecutionOnSeveralSources {
            first: 0,
            second: 1,
        },
        StartError::ExecutionWithSeveralVenues {
            venue: "polymarket",
            other: "binance",
        },
    ];
    #[cfg(feature = "otlp")]
    let refusals = refusals
        .into_iter()
        .chain([StartError::Otlp(OtlpError::CollectorNotLoopback {
            collector: ([192, 0, 2, 10], 4318).into(),
        })])
        .collect();
    refusals
}

/// The match has no catch-all arm, so a new variant does not compile here until the list above
/// holds a value of it.
fn start_variant_index(error: &StartError) -> usize {
    match error {
        StartError::Config(_) => 0,
        StartError::Exposure(_) => 1,
        StartError::Venue(_) => 2,
        StartError::ExecutionNotWired { .. } => 3,
        StartError::ExecutionCapacity { .. } => 4,
        StartError::ExecutionFieldInert { .. } => 5,
        StartError::ExecutionBaseFloorOnPosition { .. } => 6,
        StartError::ExecutionOrderCapacity { .. } => 7,
        StartError::ExecutionIdentityInUse { .. } => 8,
        StartError::ExecutionIdentityIo { .. } => 9,
        StartError::ExecutionIdentityState { .. } => 10,
        StartError::ExecutionIdentityExhausted { .. } => 11,
        StartError::Runtime(_) => 12,
        StartError::CoreId { .. } => 13,
        StartError::HotPin { .. } => 14,
        StartError::HousekeepingPin { .. } => 15,
        StartError::SenderPin { .. } => 16,
        StartError::LinkBind { .. } => 17,
        StartError::LinkNameTooLong { .. } => 18,
        StartError::ExecutionNonceEmpty { .. } => 19,
        StartError::ExecutionHttpTimeout { .. } => 20,
        StartError::ExecutionOnSeveralSources { .. } => 21,
        StartError::ExecutionWithSeveralVenues { .. } => 22,
        #[cfg(feature = "otlp")]
        StartError::Otlp(_) => 23,
    }
}

fn exposure_failed() -> DrainFailure {
    DrainFailure::Exposure {
        cause: "disk full".into(),
    }
}

fn persistence_failed() -> DrainFailure {
    DrainFailure::Persistence {
        cause: "disk full".into(),
    }
}

fn sweep_failed() -> DrainFailure {
    DrainFailure::Sweep(SweepFailure::PastDeadline {
        venue: "binance",
        resting: [ClientOrderId(1), ClientOrderId(2)].into(),
    })
}

/// One value of each variant of each error that a step of the drain wraps, with the class of that
/// error. The exposure and the persistence steps hold their error as text.
fn each_drain_failure() -> Vec<(DrainFailure, FailureClass)> {
    let sweeps = each_sweep_failure().into_iter().map(|failure| {
        let class = failure.failure_class();
        (DrainFailure::Sweep(failure), class)
    });
    let exposures = each_exposure_error().into_iter().map(|error| {
        let cause = error.to_string().into();
        (DrainFailure::Exposure { cause }, error.failure_class())
    });
    let persists = each_persist_error().into_iter().map(|error| {
        let cause = error.to_string().into();
        (DrainFailure::Persistence { cause }, error.failure_class())
    });
    sweeps.chain(exposures).chain(persists).collect()
}

fn each_sweep_failure() -> [SweepFailure; 2] {
    [
        SweepFailure::PastDeadline {
            venue: "binance",
            resting: [ClientOrderId(1)].into(),
        },
        SweepFailure::Unconfirmed {
            venue: "polymarket",
            deadline_ms: 5_000,
        },
    ]
}

fn each_exposure_error() -> [ExposureError; 9] {
    let path = || PathBuf::from("state/te.json");
    let io = || std::io::Error::other("disk full");
    [
        ExposureError::Read {
            path: path(),
            source: io(),
        },
        ExposureError::Malformed {
            path: path(),
            detail: "not a ledger".into(),
        },
        ExposureError::WrongIdentity {
            path: path(),
            found: "strat-other".into(),
            expected: "strat-te".into(),
        },
        ExposureError::UnknownVersion {
            path: path(),
            found: 2,
            expected: 3,
        },
        ExposureError::ScaleMismatch {
            path: path(),
            found: 1_000,
            expected: 100_000_000,
        },
        ExposureError::UnknownInstrument {
            path: path(),
            venue: "binance".into(),
            market: "spot".into(),
            symbol: "ETHUSDT".into(),
            position_base: 5,
            cash_quote: -10,
        },
        ExposureError::Write {
            path: path(),
            source: io(),
        },
        ExposureError::WriterPanicked {
            payload: "index out of bounds".into(),
        },
        ExposureError::DrainInterrupted,
    ]
}

fn each_persist_error() -> [PersistError; 5] {
    [
        PersistError::Io {
            path: PathBuf::from("data/orders.parquet"),
            source: std::io::Error::other("disk full"),
        },
        PersistError::Parquet {
            table: "orders",
            source: ParquetError::General("short write".to_owned()),
        },
        PersistError::Batch {
            table: "fills",
            source: ArrowError::ComputeError("columns of two lengths".to_owned()),
        },
        PersistError::WriterPanicked {
            payload: "index out of bounds".into(),
        },
        PersistError::DrainInterrupted,
    ]
}

/// The three matches below have no catch-all arm, so a new variant does not compile here until a
/// list above holds a value of it.
fn sweep_index(failure: &SweepFailure) -> usize {
    match failure {
        SweepFailure::PastDeadline { .. } => 0,
        SweepFailure::Unconfirmed { .. } => 1,
    }
}

fn exposure_index(error: &ExposureError) -> usize {
    match error {
        ExposureError::Read { .. } => 0,
        ExposureError::Malformed { .. } => 1,
        ExposureError::WrongIdentity { .. } => 2,
        ExposureError::UnknownVersion { .. } => 3,
        ExposureError::ScaleMismatch { .. } => 4,
        ExposureError::UnknownInstrument { .. } => 5,
        ExposureError::Write { .. } => 6,
        ExposureError::WriterPanicked { .. } => 7,
        ExposureError::DrainInterrupted => 8,
    }
}

fn persist_index(error: &PersistError) -> usize {
    match error {
        PersistError::Io { .. } => 0,
        PersistError::Parquet { .. } => 1,
        PersistError::Batch { .. } => 2,
        PersistError::WriterPanicked { .. } => 3,
        PersistError::DrainInterrupted => 4,
    }
}

/// A halt stops the most and a reject the least. An engine halt and a refused start each end the
/// process, so neither is above the other.
fn rank(class: FailureClass) -> u8 {
    match class {
        FailureClass::Reject => 0,
        FailureClass::Retry => 1,
        FailureClass::Reset => 2,
        FailureClass::Halt(Halt::Quoting) => 3,
        FailureClass::Halt(Halt::Start | Halt::Engine) => 4,
    }
}

fn each_failure_set() -> [Vec<DrainFailure>; 6] {
    [
        Vec::new(),
        vec![exposure_failed()],
        vec![persistence_failed()],
        vec![sweep_failed()],
        vec![exposure_failed(), persistence_failed()],
        vec![sweep_failed(), exposure_failed(), persistence_failed()],
    ]
}

fn outcome(
    trigger: Trigger,
    fatal: Option<FatalReason>,
    failures: Vec<DrainFailure>,
    deadline: DrainDeadline,
) -> DrainOutcome {
    DrainOutcome {
        trigger,
        fatal,
        failures,
        deadline,
    }
}

/// FITNESS: across each trigger, each first fatal reason, each set of drain failures and each
/// deadline, a fatal names the exit before a passed deadline, which names it before a drain failure,
/// and only a request with none of the three is graceful and gives code 0.
#[test]
fn fatal_is_never_a_graceful_exit() {
    let reasons = each_fatal_reason();
    let indexes: Vec<usize> = reasons
        .iter()
        .map(|reason| variant_index(*reason))
        .collect();
    assert_eq!(
        indexes,
        (0..15).collect::<Vec<_>>(),
        "one value of each variant, once"
    );
    for reason in reasons {
        assert_eq!(
            reason.failure_class(),
            FailureClass::Halt(Halt::Engine),
            "{reason:?} trips the fatal latch, which halts the engine"
        );
    }

    let mut triggers: Vec<(Trigger, Option<FatalReason>)> = REQUESTS
        .iter()
        .flat_map(|&name| {
            std::iter::once(None)
                .chain(reasons.map(Some))
                .map(move |latch| (Trigger::Request(name), latch))
        })
        .collect();
    // The latch keeps the first reason, so a fatal trigger finds its own reason there.
    triggers.extend(reasons.map(|reason| (Trigger::Fatal(reason), Some(reason))));

    let mut combinations = 0;
    for (trigger, latch) in triggers {
        for failures in each_failure_set() {
            for deadline in [DrainDeadline::Kept, DrainDeadline::Passed] {
                let first_failure = failures.first().cloned();
                let exit = Exit::decide(outcome(trigger, latch, failures.clone(), deadline));
                let expected = match (trigger, latch, deadline, first_failure) {
                    (Trigger::Fatal(reason), _, _, _) | (_, Some(reason), _, _) => {
                        Exit::Halted(reason)
                    }
                    (_, None, DrainDeadline::Passed, _) => Exit::DeadlinePassed,
                    (_, None, DrainDeadline::Kept, Some(failure)) => Exit::DrainFailed(failure),
                    (Trigger::Request(name), None, DrainDeadline::Kept, None) => {
                        Exit::Graceful { trigger: name }
                    }
                };
                assert_eq!(exit, expected, "{trigger:?} {latch:?} {deadline:?}");
                let is_graceful = matches!(exit, Exit::Graceful { .. });
                assert_eq!(
                    exit.code() == 0,
                    is_graceful,
                    "only a graceful exit gives code 0"
                );
                combinations += 1;
            }
        }
    }
    assert_eq!(combinations, (3 * 16 + 15) * 6 * 2);

    // The six cases that the exit decision held before its reasons were typed.
    let old_cases = [
        (
            outcome(
                Trigger::Request("SIGTERM"),
                None,
                Vec::new(),
                DrainDeadline::Kept,
            ),
            Exit::Graceful { trigger: "SIGTERM" },
        ),
        (
            outcome(
                Trigger::Request("SIGTERM"),
                Some(FatalReason::Panic),
                Vec::new(),
                DrainDeadline::Kept,
            ),
            Exit::Halted(FatalReason::Panic),
        ),
        (
            outcome(
                Trigger::Fatal(reasons[0]),
                Some(reasons[0]),
                Vec::new(),
                DrainDeadline::Kept,
            ),
            Exit::Halted(reasons[0]),
        ),
        (
            outcome(
                Trigger::Request("SIGINT"),
                None,
                vec![persistence_failed()],
                DrainDeadline::Kept,
            ),
            Exit::DrainFailed(persistence_failed()),
        ),
        (
            outcome(
                Trigger::Fatal(FatalReason::Panic),
                Some(FatalReason::Panic),
                vec![persistence_failed()],
                DrainDeadline::Kept,
            ),
            Exit::Halted(FatalReason::Panic),
        ),
        (
            outcome(
                Trigger::Request("shutdown request"),
                None,
                Vec::new(),
                DrainDeadline::Kept,
            ),
            Exit::Graceful {
                trigger: "shutdown request",
            },
        ),
    ];
    for (drained, expected) in old_cases {
        assert_eq!(Exit::decide(drained), expected);
    }
    assert_eq!(
        Exit::Graceful { trigger: "SIGTERM" }.to_string(),
        "received SIGTERM"
    );
    assert_eq!(
        Exit::DrainFailed(persistence_failed()).to_string(),
        "persistence drain failed: disk full"
    );

    // Each failed step of the drain halts the engine, never below the class of what it wraps, and
    // with no fatal reason and the deadline kept the first one names the exit.
    let sweeps: Vec<usize> = each_sweep_failure().iter().map(sweep_index).collect();
    let exposures: Vec<usize> = each_exposure_error().iter().map(exposure_index).collect();
    let persists: Vec<usize> = each_persist_error().iter().map(persist_index).collect();
    assert_eq!(
        (sweeps, exposures, persists),
        ((0..2).collect(), (0..9).collect(), (0..5).collect()),
        "one value of each variant, once"
    );
    for (failure, wrapped) in each_drain_failure() {
        let class = failure.failure_class();
        assert_eq!(class, FailureClass::Halt(Halt::Engine), "{failure}");
        assert!(
            rank(class) >= rank(wrapped),
            "{failure} gives {class:?}, below the {wrapped:?} of the error that it wraps"
        );
        let exit = Exit::decide(outcome(
            Trigger::Request("SIGTERM"),
            None,
            vec![failure.clone(), sweep_failed()],
            DrainDeadline::Kept,
        ));
        assert_eq!(exit, Exit::DrainFailed(failure));
        assert_eq!(exit.code(), 1);
    }

    assert_ne!(
        Exit::StartRefused.code(),
        0,
        "a refused start never exits 0"
    );
    let refusals = each_start_refusal();
    let start_indexes: Vec<usize> = refusals.iter().map(start_variant_index).collect();
    assert_eq!(
        start_indexes,
        (0..23 + usize::from(cfg!(feature = "otlp"))).collect::<Vec<_>>(),
        "one refusal of each variant, once"
    );
    for refusal in &refusals {
        assert_eq!(
            refusal.failure_class(),
            FailureClass::Halt(Halt::Start),
            "{refusal} refuses the start, which halts it"
        );
    }
    assert_ne!(Exit::DeadlinePassed.code(), 0);

    for first in reasons {
        for second in reasons.into_iter().filter(|reason| *reason != first) {
            let fatal = FatalSignal::new();
            fatal.trip(first);
            fatal.trip(second);
            assert!(fatal.is_tripped());
            assert_eq!(
                fatal.reason(),
                Some(first),
                "a second trip keeps the first reason"
            );
        }
    }
}

/// CONTRACT: a start that preflight or the config refuses exits by the class of its error: a
/// refused start, with a code that is not 0, and with the run nonce unspent.
#[test]
fn start_refusal_exits_by_its_class() {
    let root = TempDir::new("exit-start-refusal");
    let state = root.path().join("state");
    // A lease locks one identity on the whole host, so no other test takes this identity.
    let identity = RunIdentity::new("strat-exit", "te-start-refusal").expect("a valid identity");
    let held = ExecutionLease::acquire(&state, TeTag::of(&identity), &sim::lease_namespace())
        .expect("the test takes the lease of this engine identity");
    let document = sim_document(&state, "");
    let config = Config::<NoParams>::from_yaml(&document).expect("the document passes config");
    let before = snapshot(&state);

    let refused = Preflight {
        identity: &identity,
        config: &config,
        strategy: &Idle,
    }
    .run_with(&mut StandInVenue {
        scales: Scales::Stamped {
            max_num_orders: 200,
        },
    });
    let Err(refused) = refused else {
        panic!("a held lease refuses the start");
    };
    assert!(
        matches!(refused, StartError::ExecutionIdentityInUse { .. }),
        "{refused}"
    );
    assert_eq!(refused.failure_class(), FailureClass::Halt(Halt::Start));
    let exit = Exit::refused(refused.failure_class());
    assert_eq!(exit, Exit::StartRefused);
    assert_eq!(exit.code(), 1);
    assert_eq!(snapshot(&state), before, "the refusal spent no nonce");
    drop(held);

    let misspelt = document.replacen("heartbeat_ms:", "heartbeat_msec:", 1);
    let Err(unparsed) = Config::<NoParams>::from_yaml(&misspelt) else {
        panic!("a misspelt key refuses the config");
    };
    assert!(
        matches!(&unparsed, ConfigError::Parse { detail } if detail.contains("heartbeat_msec")),
        "{unparsed}"
    );
    assert_eq!(unparsed.failure_class(), FailureClass::Halt(Halt::Start));
    let exit = Exit::refused(unparsed.failure_class());
    assert_eq!(exit, Exit::StartRefused);
    assert_eq!(exit.code(), 1);
}

/// CONTRACT: an exit sweep that passes its deadline with orders still mirrored as resting ends by
/// naming those orders, never as swept, and the drain turns that into an exit that is not graceful.
#[test]
fn sweep_past_deadline_is_not_swept() {
    const VENUE: &str = "test-venue";
    let resting = [ClientOrderId(0x11), ClientOrderId(0x12)];
    let mut core = ExecCore::with_limits(8, 32);
    for client_id in resting {
        core.observe_venue_order(MirroredOrder {
            instrument: InstrumentId(0),
            client_id,
            side: Side::Buy,
            price: Price::from_mantissa(11_800_000_000_000),
            qty: Qty::from_mantissa(8_000),
            provenance: Provenance::Mine,
            has_sent_cancel: false,
            is_ambiguous: false,
        })
        .expect("the mirror has room for two orders");
    }
    // The venue stand-in confirms no cancel, so both orders stay mirrored as resting.
    core.begin_sweep(CancelReason::Shutdown, None, &mut |_| {});
    assert_eq!(core.mirror().len(), 2);

    let plan = ExitPlan::new(
        CancelReason::Shutdown,
        LocalTs::from_micros(0),
        Duration::from_micros(2_000_000),
    );
    assert_eq!(
        plan.past_deadline(LocalTs::from_micros(2_000_000), VENUE, core.mirror()),
        None,
        "the deadline itself still belongs to the sweep"
    );
    let ended = plan
        .past_deadline(LocalTs::from_micros(2_000_001), VENUE, core.mirror())
        .expect("one microsecond past the deadline the session gives up");
    let SessionOutcome::SweepPastDeadline(failure) = ended else {
        panic!("a sweep past its deadline with 2 orders resting ended as {ended:?}");
    };
    assert_eq!(
        failure,
        SweepFailure::PastDeadline {
            venue: VENUE,
            resting: resting.into(),
        }
    );

    let exit = Exit::decide(outcome(
        Trigger::Request("SIGTERM"),
        None,
        vec![DrainFailure::Sweep(failure)],
        DrainDeadline::Kept,
    ));
    assert!(
        matches!(exit, Exit::DrainFailed(DrainFailure::Sweep(_))),
        "{exit:?}"
    );
    assert_ne!(
        exit.code(),
        0,
        "an exit with orders at rest is never graceful"
    );
}

/// Each call of the forced-exit stand-in: its code and when it came. A binary gives
/// `std::process::exit`, which never returns; the stand-in returns, so the drain goes on.
static FORCED_EXITS: Mutex<Vec<(i32, Instant)>> = Mutex::new(Vec::new());

fn record_forced_exit(code: i32) {
    FORCED_EXITS
        .lock()
        .expect("no test panics while it holds the record")
        .push((code, Instant::now()));
}

fn forced_exits() -> Vec<(i32, Instant)> {
    FORCED_EXITS
        .lock()
        .expect("no test panics while it holds the record")
        .clone()
}

/// CONTRACT: a drain that passes its deadline calls the forced exit of the binary one time, with a
/// code that is not 0, and ends as `DeadlinePassed`; a drain that ends inside its deadline never
/// calls it.
#[test]
fn drain_past_deadline_calls_the_forced_exit_once() {
    const DEADLINE: Duration = Duration::from_millis(200);

    let armed_at = Instant::now();
    let watchdog = DrainWatchdog::arm(DEADLINE, record_forced_exit);
    // The drain step that the test holds open for 1,000 ms.
    std::thread::sleep(Duration::from_millis(1_000));
    let deadline = watchdog.deadline();
    watchdog.disarm();
    let calls = forced_exits();
    assert_eq!(calls.len(), 1, "the forced exit is called one time");
    let (code, called_at) = calls[0];
    assert_ne!(code, 0);
    assert!(called_at.duration_since(armed_at) >= DEADLINE);
    let exit = Exit::decide(outcome(
        Trigger::Request("SIGTERM"),
        None,
        Vec::new(),
        deadline,
    ));
    assert_eq!(exit, Exit::DeadlinePassed);

    let watchdog = DrainWatchdog::arm(DEADLINE, record_forced_exit);
    // The held step ends at 50 ms.
    std::thread::sleep(Duration::from_millis(50));
    let deadline = watchdog.deadline();
    watchdog.disarm();
    assert_eq!(
        forced_exits().len(),
        1,
        "a drain inside its deadline never forces an exit"
    );
    let exit = Exit::decide(outcome(
        Trigger::Request("SIGTERM"),
        None,
        Vec::new(),
        deadline,
    ));
    assert_eq!(exit, Exit::Graceful { trigger: "SIGTERM" });
}
