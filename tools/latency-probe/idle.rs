//! Mode `idle`: messages arrive with gaps, and the hot thread spins or parks between them. The
//! metrics core times how long each message waited in its queue and took to reach its decision.

use std::sync::mpsc::sync_channel;
use std::time::Duration;

use anyhow::Context;
use rtrb::{Consumer, Producer};

use engine::hot::dispatch::HotEngine;
use engine::hot::ingress::IngressQueues;
use engine::hot::metrics::DriverSamples;
use engine::hot::spawn::{
    HotDriver, HotLoop, HotThreadConfig, IdleWait, spawn_hot_thread, touched_input_ring,
};
use engine::msg::inbound::InboundMessage;
use engine::msg::time::DurationUs;
use engine::runtime::metrics::SampleProducers;
use engine::runtime::pin_to_core;
use engine::runtime::shutdown::{DrainSignal, FatalSignal};

use crate::clock;
use crate::rig::{self, MetricsRun};

const INPUT_RING: usize = 4_096;

/// Where the engine and its input ring are built and first touched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildPlace {
    /// On the main thread, before the hot thread starts: the start of the engine before its pin
    /// came first.
    Main,
    /// On the hot thread after its pin, as the engine's own start does.
    Hot,
}

pub struct IdleRun {
    pub wait: IdleWait,
    pub messages: usize,
    pub gap_us: u64,
    pub core: Option<usize>,
    pub build: BuildPlace,
}

impl IdleRun {
    /// # Errors
    /// When the log or the metrics thread cannot start, or the hot thread cannot pin on Linux.
    pub fn run(&self) -> anyhow::Result<()> {
        let clock = clock::start_engine_clock();
        let (metrics, producers, stamper) = MetricsRun::start(&clock, "latency-probe-idle")?;
        let SampleProducers {
            stages, counters, ..
        } = producers;
        let samples = DriverSamples {
            stamper,
            stages,
            counters,
        };
        let hot_thread = spawn_hot_thread(
            HotThreadConfig {
                core_id: self.core,
                tag: "hot",
            },
            pin_to_core,
        )
        .context("the hot thread of the harness cannot pin to --core")?;
        let built_on_main = (self.build == BuildPlace::Main).then(build);
        let (ready, handed_back) = sync_channel(1);
        let drain = DrainSignal::new();
        let hot_drain = drain.clone();
        let wait = self.wait;
        let hot = hot_thread.start(move || {
            let (input, queue, engine) = built_on_main.unwrap_or_else(build);
            ready
                .send(input)
                .expect("the harness waits for the write end of the input ring");
            HotLoop {
                driver: HotDriver::new(IngressQueues::new(vec![queue]), engine, Some(samples)),
                idle: wait,
                fatal: FatalSignal::new(),
                drain: hot_drain,
            }
        });
        let mut input = handed_back
            .recv()
            .context("the hot thread of the harness ended before it was built")?;

        let trades = rig::recorded_trades();
        let gap = DurationUs::from_micros(self.gap_us as i64);
        let mut due = clock::now(&clock);
        for index in 0..self.messages {
            clock::spin_until(&clock, due);
            let mut message = rig::message(&trades, index, &clock);
            while let Err(rtrb::PushError::Full(back)) = input.push(message) {
                message = back;
                std::hint::spin_loop();
            }
            due = due + gap;
        }
        drain.request();
        hot.join()
            .expect("the hot thread of the harness does not panic");
        let build = match self.build {
            BuildPlace::Main => "main",
            BuildPlace::Hot => "hot",
        };
        let label = match self.wait {
            IdleWait::Spin => format!("idle --wait spin --build {build}"),
            IdleWait::Park(nap) => format!(
                "idle --wait park --park-us {} --build {build}",
                nap.as_micros()
            ),
        };
        metrics.finish(&label)?;
        Ok(())
    }
}

/// The input ring, touched slot by slot, and the engine, both on the calling thread.
fn build() -> (
    Producer<InboundMessage>,
    Consumer<InboundMessage>,
    HotEngine,
) {
    let (input, queue) = touched_input_ring(INPUT_RING);
    let instruments = [rig::instrument()];
    (input, queue, rig::engine(&instruments))
}

pub fn park(micros: u64) -> IdleWait {
    IdleWait::Park(Duration::from_micros(micros))
}
