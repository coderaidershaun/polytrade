//! Every clock that the harness reads sits in this file, so the list of permitted clock reads names
//! one place for the whole harness.

use std::time::Duration;

use engine::msg::time::{EngineClock, Stamper, Tick, TickAnchor, TickScale, TsUs};

/// The two anchors of the first scale sit this far apart.
const FIRST_ANCHOR_GAP: Duration = Duration::from_millis(2);

/// Raw reads of the cpu counter that the engine stamps with, around the region the harness times.
pub struct Probe {
    clock: quanta::Clock,
}

impl Probe {
    /// Building the clock calibrates it, so it is built once, before the first region.
    #[expect(
        clippy::disallowed_methods,
        reason = "the harness builds its own counter clock to time its regions, off any engine thread"
    )]
    pub fn new() -> Probe {
        Probe {
            clock: quanta::Clock::new(),
        }
    }

    #[inline]
    #[expect(
        clippy::disallowed_methods,
        reason = "the harness times its own region with the counter, off any engine thread"
    )]
    pub fn read(&self) -> u64 {
        self.clock.raw()
    }

    /// `None` when `end` reads before `start`: the pair ran backward.
    #[expect(
        clippy::disallowed_methods,
        reason = "the harness turns its own counter reads into time to report its regions"
    )]
    pub fn nanos(&self, start: u64, end: u64) -> Option<u64> {
        (end >= start).then(|| self.clock.delta_as_nanos(start, end))
    }

    /// What two reads with nothing between them cost, the median of many, so it can be taken off
    /// each region.
    pub fn cost_nanos(&self) -> u64 {
        const REGIONS: usize = 1_000_000;
        let mut costs: Vec<u64> = (0..REGIONS)
            .map(|_| {
                let start = self.read();
                let end = self.read();
                self.nanos(start, end).unwrap_or(0)
            })
            .collect();
        costs.sort_unstable();
        costs[REGIONS / 2]
    }
}

/// The engine time of the harness, for the stamps that its messages and commands carry.
#[expect(
    clippy::disallowed_methods,
    reason = "the harness is a process of its own, and its one engine clock stamps what it sends"
)]
pub fn start_engine_clock() -> EngineClock {
    EngineClock::start()
}

#[inline]
#[expect(
    clippy::disallowed_methods,
    reason = "the harness stamps each message and command it sends, as an edge of the engine does"
)]
pub fn now(clock: &EngineClock) -> TsUs {
    clock.now()
}

#[inline]
#[expect(
    clippy::disallowed_methods,
    reason = "the harness stamps each command as it enters the lane, as the command sink does"
)]
pub fn stamp(stamper: &Stamper) -> Tick {
    stamper.stamp()
}

/// The counter that the engine stamps with, built and calibrated once for the harness's engine.
#[expect(
    clippy::disallowed_methods,
    reason = "the harness is a process of its own, and its one counter clock stamps what it times"
)]
pub fn counter_stamper() -> Stamper {
    Stamper::Counter(quanta::Clock::new())
}

/// The scale between two anchors a few milliseconds apart, which the metrics core needs before it
/// reads a sample.
pub fn first_scale(stamper: &Stamper, clock: &EngineClock) -> TickScale {
    let first = TickAnchor {
        tick: stamp(stamper),
        at: now(clock),
    };
    std::thread::sleep(FIRST_ANCHOR_GAP);
    let last = TickAnchor {
        tick: stamp(stamper),
        at: now(clock),
    };
    TickScale::between(first, last)
        .expect("the cpu counter and the engine clock both move in two milliseconds")
}

/// Spins until the engine clock reaches `due`, so the sender of the harness keeps a fixed rate.
pub fn spin_until(clock: &EngineClock, due: TsUs) {
    while now(clock) < due {
        std::hint::spin_loop();
    }
}
