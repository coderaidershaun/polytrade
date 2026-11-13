//! The bring-up of the hot thread once its pin holds: on its own core it builds and touches its
//! input rings and builds its engine, then hands the producers of its rings back to the start.

use std::sync::mpsc::sync_channel;
use std::thread::JoinHandle;

use crate::hot::dispatch::HotEngine;
use crate::hot::metrics::DriverSamples;
use crate::hot::spawn::{HotDriver, HotLoop, IdleWait, PinnedHotThread};
use crate::runtime::config::{EngineConfig, IdleMode};
use crate::runtime::shutdown::{DrainSignal, FatalSignal};

use super::queues::{InputProducers, RingPlan};

pub(super) struct HotBringUpSetup<'a, E> {
    pub(super) thread: PinnedHotThread,
    pub(super) rings: RingPlan,
    /// Runs on the hot thread after its rings are built, so the engine's memory is first touched
    /// on the hot core too.
    pub(super) engine: E,
    pub(super) samples: DriverSamples,
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
    E: FnOnce() -> HotEngine + Send + 'static,
{
    let HotBringUpSetup {
        thread,
        rings,
        engine,
        samples,
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
        let (ingress, input_producers) = rings.build(&fatal);
        let engine = engine();
        ready
            .send(input_producers)
            .expect("the start waits for the producers until the hot thread sends them");
        HotLoop {
            driver: HotDriver::new(ingress, engine, Some(samples)),
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
