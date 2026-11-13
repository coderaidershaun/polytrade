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
        let mut woke_ts_us = clock.now();
        let schedule = SpinSchedule::new(woke_ts_us, interval);
        let (mut slot, mut skipped) = (0, 0);
        let mut skipped_slots: u64 = 0;
        loop {
            producer.push(InboundMessage::SpinTick(SpinTick {
                seq: slot,
                received_ts_us: schedule.stamp_of(slot),
                queued_ts_us: woke_ts_us,
            }));
            if skipped > 0 {
                skipped_slots += skipped;
                warn!(
                    "timer woke late: fired spin slot {} and skipped the {} slots before it, {} skipped since start",
                    slot, skipped, skipped_slots
                );
            }

            let wait = schedule.stamp_of(slot + 1).diff(clock.now());
            time::sleep(Duration::from_micros(wait.micros().max(0) as u64)).await;
            woke_ts_us = clock.now();
            (slot, skipped) = schedule.slot_to_fire(slot, woke_ts_us);
        }
    };
    let join = tokio_handle.spawn(crate::log::tag_task("timer", body));
    TaskHandle::new(join, "timer")
}
