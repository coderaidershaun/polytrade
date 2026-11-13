//! Mode `step`: times the public step of the hot loop's driver over a real input ring that a
//! producer thread fills at a fixed rate. The timed region holds the whole of the measurement under
//! test: both stamps, the build of the sample and its push, and the bank stamps of the fold. Run it
//! with stamps off and with stamps on, and compare.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rtrb::RingBuffer;

use engine::hot::ingress::IngressQueues;
use engine::hot::metrics::DriverSamples;
use engine::hot::spawn::{HotDriver, Step};
use engine::msg::time::DurationUs;
use engine::runtime::metrics::SampleProducers;

use crate::clock::{self, Probe};
use crate::rig::{self, MetricsRun};
use crate::tally::Tally;

/// Holds about three seconds of the default rate, so a stall of the driver never stops the sender.
const INPUT_RING: usize = 65_536;

/// Room for the tick pairs of a long stall of the thread that tallies them.
const PAIR_RING: usize = 65_536;

pub struct StepRun {
    pub are_stamps_on: bool,
    pub messages: usize,
    pub rate_hz: u64,
    pub core: Option<usize>,
}

impl StepRun {
    /// # Errors
    /// When the run names a core that it cannot pin on Linux.
    pub fn run(&self) -> anyhow::Result<()> {
        rig::pin(self.core)?;
        let probe = Probe::new();
        let probe_cost_ns = probe.cost_nanos();
        let clock = clock::start_engine_clock();
        let rows = rig::instrument();
        let instruments = [rows];
        let engine = rig::engine(&instruments);
        let (metrics, samples) = if self.are_stamps_on {
            let (metrics, producers, stamper) = MetricsRun::start(&clock, "latency-probe-step")?;
            let SampleProducers {
                stages, counters, ..
            } = producers;
            let samples = DriverSamples {
                stamper,
                stages,
                counters,
            };
            (Some(metrics), Some(samples))
        } else {
            (None, None)
        };

        let (mut input, queue) = RingBuffer::new(INPUT_RING);
        let interval = DurationUs::from_micros((1_000_000 / self.rate_hz.max(1)).max(1) as i64);
        let messages = self.messages;
        let sender_clock = clock.clone();
        let sender = std::thread::spawn(move || {
            let trades = rig::recorded_trades();
            let mut due = clock::now(&sender_clock);
            for index in 0..messages {
                clock::spin_until(&sender_clock, due);
                let mut message = rig::message(&trades, index, &sender_clock);
                while let Err(rtrb::PushError::Full(back)) = input.push(message) {
                    message = back;
                    std::hint::spin_loop();
                }
                due = due + interval;
            }
        });

        let (mut pairs, mut pair_reader) = RingBuffer::<(u64, u64)>::new(PAIR_RING);
        let is_done = Arc::new(AtomicBool::new(false));
        let tally_done = Arc::clone(&is_done);
        let tally_probe = Probe::new();
        let send_interval_ns = interval.micros() as u64 * 1_000;
        let tallier = std::thread::spawn(move || {
            let mut tally = Tally::new(probe_cost_ns, send_interval_ns);
            loop {
                let is_finished = tally_done.load(Ordering::Acquire);
                while let Ok((start, end)) = pair_reader.pop() {
                    tally.record(&tally_probe, start, end);
                }
                if is_finished {
                    return tally;
                }
                std::thread::yield_now();
            }
        });

        let mut driver = HotDriver::new(IngressQueues::new(vec![queue]), engine, samples);
        let mut folded = 0;
        let mut dropped = 0u64;
        while folded < messages {
            let start = probe.read();
            let step = black_box(driver.step());
            let end = probe.read();
            if step == Step::Idle {
                std::hint::spin_loop();
                continue;
            }
            folded += 1;
            if pairs.push((start, end)).is_err() {
                dropped += 1;
            }
        }
        sender
            .join()
            .expect("the sender of the harness does not panic");
        is_done.store(true, Ordering::Release);
        let tally = tallier.join().expect("the tally thread does not panic");
        let label = if self.are_stamps_on { "step --stamps on" } else { "step --stamps off" };
        tally.print(label, dropped);
        if let Some(metrics) = metrics {
            metrics.finish(label)?;
        }
        Ok(())
    }
}
