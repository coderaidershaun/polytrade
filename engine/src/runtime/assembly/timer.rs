//! The timer turns the passing of time into spin-tick messages, so the hot thread reads time only
//! from its input. Each tick carries the time of its slot of the spin schedule, never of the wake.

use std::time::Duration;

use tokio::runtime::Handle;
use tokio::time;

use crate::hot::spawn::QueueProducer;
use crate::msg::inbound::{InboundMessage, SpinTick};
use crate::msg::time::{DurationUs, EngineClock, SpinSchedule};
use crate::runtime::shutdown::TaskHandle;
use crate::warn;

/// Pushes one tick for each slot of a schedule that starts now. A late wake fires only the newest
/// slot that is due, and counts the slots it skipped in one WARN line.
#[expect(
    clippy::disallowed_methods,
    reason = "the timer actor schedules the spin ticks, so time enters the engine as a message"
)]
pub(super) fn spawn_timer(
    interval: DurationUs,
    mut producer: QueueProducer,
    clock: &EngineClock,
    tokio_handle: &Handle,
) -> TaskHandle {
    let clock = clock.clone();
    let body = async move {
        let schedule = SpinSchedule::new(clock.now(), interval);
        let mut next_slot: u64 = 0;
        let mut skipped_slots: u64 = 0;
        loop {
            let wait = schedule.stamp_of(next_slot).diff(clock.now());
            time::sleep(Duration::from_micros(wait.micros().max(0) as u64)).await;

            let woke_ts_us = clock.now();
            // The sleep never ends before its slot, but a slot must never fire twice.
            let slot = schedule.slot_at(woke_ts_us).max(next_slot);
            producer.push(InboundMessage::SpinTick(SpinTick {
                seq: slot,
                received_ts_us: schedule.stamp_of(slot),
                queued_ts_us: woke_ts_us,
            }));

            if slot > next_slot {
                skipped_slots += slot - next_slot;
                warn!(
                    "timer woke late: fired spin slot {} and skipped slots {} to {}, {} skipped since start",
                    slot,
                    next_slot,
                    slot - 1,
                    skipped_slots
                );
            }
            next_slot = slot + 1;
        }
    };
    let join = tokio_handle.spawn(crate::log::tag_task("timer", body));
    TaskHandle::new(join, "timer")
}
