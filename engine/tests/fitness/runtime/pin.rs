//! A thread that cannot pin itself to its core refuses the start on Linux, before any thread that
//! trades exists and before any edge connects. Elsewhere no pin holds for sure, so the thread runs
//! where it is put.

use engine::msg::time::EngineClock;
use engine::runtime::metrics::{MetricsThread, MetricsThreadSetup};
use engine::runtime::{EngineError, PinnedSenders};

use crate::runtime::metrics::Harness;

/// The last core that a Linux cpu set can name, which no host of this suite has.
const UNUSABLE_CORE: usize = 1_023;

#[test]
fn housekeeping_pin_failure_refuses_start() {
    let harness = Harness::new(10);
    let spawned = MetricsThread::spawn(MetricsThreadSetup {
        metrics: harness.metrics,
        clock: EngineClock::start(),
        stamper: harness.counter.stamper.clone(),
        core_id: Some(UNUSABLE_CORE),
    });
    if cfg!(target_os = "linux") {
        assert!(
            matches!(
                spawned,
                Err(EngineError::HousekeepingPin {
                    core_id: UNUSABLE_CORE,
                    thread: "metrics"
                })
            ),
            "linux refuses the start with the core and the thread, and runs no metrics thread"
        );
    } else {
        let thread = spawned.expect("elsewhere a pin is best effort and the thread runs");
        thread.stop();
    }
}

#[test]
fn sender_pin_failure_refuses_start() {
    let spawned = PinnedSenders::spawn(Some(UNUSABLE_CORE));
    if cfg!(target_os = "linux") {
        let refusal = spawned.err();
        assert!(
            matches!(
                refusal,
                Some(EngineError::SenderPin {
                    core_id: UNUSABLE_CORE
                })
            ),
            "linux refuses the start with the core of the sender thread, and runs no task: \
             {refusal:?}"
        );
    } else {
        drop(spawned.expect("elsewhere a pin is best effort and the sender thread runs"));
    }
}
