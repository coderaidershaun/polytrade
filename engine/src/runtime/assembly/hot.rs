//! The bring-up of the hot thread once its pin holds: on its own core it builds and touches its
//! input rings and builds its engine, then hands the producers of its rings back to the start.

use std::sync::mpsc::sync_channel;
use std::thread::JoinHandle;

use crate::hot::dispatch::HotEngine;
use crate::hot::metrics::DriverSamples;
use crate::hot::sink::TapeSink;
use crate::hot::spawn::{HotDriver, HotLoop, IdleWait, PinnedHotThread, QueueProducer};
use crate::runtime::config::{EngineConfig, IdleMode};
use crate::runtime::shutdown::{DrainSignal, FatalSignal};

use super::queues::{InputProducers, RingPlan};

pub(super) struct HotBringUpSetup<'a, E> {
    pub(super) thread: PinnedHotThread,
    pub(super) rings: RingPlan,
    /// Runs on the hot thread after its rings are built, so the engine's memory is first touched
    /// on the hot core too. It takes the producer of the audit status queue, which stays on the
    /// hot thread.
    pub(super) engine: E,
    pub(super) samples: DriverSamples,
    /// `None` with no persistence: the run records no tape.
    pub(super) tape: Option<TapeSink>,
    pub(super) config: &'a EngineConfig,
    pub(super) fatal: &'a FatalSignal,
    pub(super) drain: &'a DrainSignal,
}

pub(super) struct HotBringUp {
    pub(super) thread: JoinHandle<()>,
    pub(super) producers: InputProducers,
}

/// Returns once the hot thread is built, so each producer that the start hands out writes to a ring
/// whose reader exists.
pub(super) fn hot_bring_up<E>(setup: HotBringUpSetup<'_, E>) -> HotBringUp
where
    E: FnOnce(Option<QueueProducer>) -> HotEngine + Send + 'static,
{
    let HotBringUpSetup {
        thread,
        rings,
        engine,
        samples,
        tape,
        config,
        fatal,
        drain,
    } = setup;
    let idle = match config.idle {
        IdleMode::Spin => IdleWait::Spin,
        IdleMode::Park => IdleWait::Park(config.park()),
    };
    let (fatal, drain) = (fatal.clone(), drain.clone());
    let (ready, producers) = sync_channel(1);
    let thread = thread.start(move || {
        let (ingress, mut input_producers) = rings.build(&fatal);
        let engine = engine(input_producers.audit_status.take());
        ready
            .send(input_producers)
            .expect("the start waits for the producers until the hot thread sends them");
        let driver = HotDriver::new(ingress, engine, Some(samples));
        HotLoop {
            driver: match tape {
                Some(tape) => driver.with_tape(tape),
                None => driver,
            },
            idle,
            fatal,
            drain,
        }
    });
    let producers = producers
        .recv()
        .expect("the hot thread hands back its producers unless its build panicked");
    HotBringUp { thread, producers }
}
