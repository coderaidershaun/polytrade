//! Spin-tick timer actor: wall clock as message venue. Drift-free: every tick at start + n*interval; overran wake emits latest.

use std::time::Duration;

use tokio::runtime::Handle;
use tokio::time::{self, Instant};

use crate::hot::spawn::QueueProducer;
use crate::msg::inbound::{InboundMessage, SpinTick};
use crate::shutdown::TaskHandle;
use crate::time::{DurationUs, EngineClock};
use crate::warn;

/// Pushes `SpinTick`s onto `producer`'s input queue, each stamped with its scheduled fire time
/// as `received_ts_us` (the timer is the venue), spaced by `interval`.
pub(super) fn spawn_timer(
    interval: DurationUs,
    mut producer: QueueProducer,
    clock: &EngineClock,
    tokio_handle: &Handle,
) -> TaskHandle {
    let clock = clock.clone();
    let interval_us = interval.micros().max(1) as u64;
    let body = async move {
        let start_instant = Instant::now();
        let start_wall = clock.now();
        let mut seq: u64 = 0;
        loop {
            let offset = Duration::from_micros(interval_us.saturating_mul(seq));
            time::sleep_until(start_instant + offset).await;

            // Which slot has the schedule reached? A late wake lands later — fire it, skip the rest.
            let elapsed = start_instant.elapsed();
            let current = (elapsed.as_micros() / u128::from(interval_us)) as u64;
            if current > seq {
                warn!(
                    "timer skipped {} spin ticks (seq {}..{}) — schedule held, no drift",
                    current - seq,
                    seq,
                    current - 1
                );
            }

            let scheduled_offset =
                DurationUs::from_micros((current as i64).saturating_mul(interval_us as i64));
            let tick = SpinTick {
                seq: current,
                received_ts_us: start_wall + scheduled_offset,
                queued_ts_us: clock.now(),
            };
            producer.push(InboundMessage::SpinTick(tick));
            seq = current + 1;
        }
    };
    let join = tokio_handle.spawn(crate::log::tag_task("timer", body));
    TaskHandle::new(join, "timer")
}
