//! Mode `idle`: messages arrive with gaps, and the hot thread spins or parks between them. The
//! metrics core times how long each message waited in its queue and took to reach its decision.

use std::time::Duration;

use rtrb::RingBuffer;

use engine::hot::ingress::IngressQueues;
use engine::hot::metrics::DriverSamples;
use engine::hot::spawn::{HotThreadConfig, IdleWait, spawn_hot_thread};
use engine::msg::time::DurationUs;
use engine::runtime::metrics::SampleProducers;
use engine::runtime::shutdown::{DrainSignal, FatalSignal};

use crate::clock;
use crate::rig::{self, MetricsRun};

const INPUT_RING: usize = 4_096;

pub struct IdleRun {
    pub wait: IdleWait,
    pub messages: usize,
    pub gap_us: u64,
    pub core: Option<usize>,
}

impl IdleRun {
    /// # Errors
    /// When the log or the metrics thread cannot start.
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
        let instruments = [rig::instrument()];
        let engine = rig::engine(&instruments);
        let (mut input, queue) = RingBuffer::new(INPUT_RING);
        let drain = DrainSignal::new();
        let hot = spawn_hot_thread(
            HotThreadConfig {
                core_id: self.core,
                tag: "hot",
                samples: Some(samples),
                idle: self.wait,
            },
            IngressQueues::new(vec![queue]),
            engine,
            FatalSignal::new(),
            drain.clone(),
        );

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
        let label = match self.wait {
            IdleWait::Spin => "idle --wait spin".to_owned(),
            IdleWait::Park(nap) => format!("idle --wait park --park-us {}", nap.as_micros()),
        };
        metrics.finish(&label)?;
        Ok(())
    }
}

pub fn park(micros: u64) -> IdleWait {
    IdleWait::Park(Duration::from_micros(micros))
}
