//! How a run ends: each way gives one exit and one code, and only a clean drain after a request to
//! stop gives code 0, so an operator never reads a fatal or a lost order as a clean stop.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use engine::msg::exec::{CancelReason, Provenance};
use engine::msg::failure::{FailureClass, Halt};
use engine::msg::ids::{ClientOrderId, InstrumentId, Price, Qty, QueueId, Side, SourceId, TradeId};
use engine::msg::time::LocalTs;
use engine::runtime::shutdown::{AnswerPart, FatalReason, FatalSignal};
use engine::runtime::{DrainDeadline, DrainFailure, DrainOutcome, DrainWatchdog, Exit, Trigger};
use engine::venues::exec::{ExecCore, ExitPlan, MirroredOrder, SessionOutcome, SweepFailure};

const REQUESTS: [&str; 3] = ["SIGTERM", "SIGINT", "shutdown request"];

/// One value of each variant, listed in the order of `variant_index`.
fn each_fatal_reason() -> [FatalReason; 16] {
    [
        FatalReason::InputQueueFull {
            queue: QueueId::new(0).unwrap(),
            capacity: 65_536,
        },
        FatalReason::MarketTapFull {
            queue: QueueId::new(1).unwrap(),
            capacity: 1_024,
        },
        FatalReason::HotPinRefused { core: 3 },
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
        FatalReason::HotPinRefused { .. } => 2,
        FatalReason::Panic => 3,
        FatalReason::MirrorRetainFailed { .. } => 4,
        FatalReason::PlacementFailedClosed { .. } => 5,
        FatalReason::SimulatorCommandLaneFull { .. } => 6,
        FatalReason::RestWorkerGone { .. } => 7,
        FatalReason::UndecodableAnswer { .. } => 8,
        FatalReason::SubscribeRefused { .. } => 9,
        FatalReason::EpochExhausted { .. } => 10,
        FatalReason::MarketDataUnreadable { .. } => 11,
        FatalReason::UndecodableOpenOrder { .. } => 12,
        FatalReason::OrderIndexFull { .. } => 13,
        FatalReason::SubscribeUnbuildable { .. } => 14,
        FatalReason::SettlementFailed { .. } => 15,
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
        (0..16).collect::<Vec<_>>(),
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
    assert_eq!(combinations, (3 * 17 + 16) * 6 * 2);

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

    assert_ne!(
        Exit::StartRefused.code(),
        0,
        "a refused start never exits 0"
    );
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
