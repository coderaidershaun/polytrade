//! The schedule of the spin timer. Its stamps decide the time of each spin, so an error in them
//! moves every deadline and every window that a strategy measures in spins.

use engine::msg::time::{DurationUs, SpinSchedule, TsUs};
use proptest::prelude::*;

/// Year 2100 in microseconds since the Unix epoch.
const LAST_START_US: i64 = 4_102_444_800_000_000;
/// The range of `engine.spin_interval_us` that the config accepts.
const SHORTEST_INTERVAL_US: i64 = 1_000;
const LONGEST_INTERVAL_US: i64 = 60_000_000;
/// About three years of spins at the shortest interval.
const LAST_SLOT: u64 = 100_000_000_000;

proptest! {
    /// FITNESS: each stamp is computed from the start of the schedule. A stamp that added one
    /// interval to the stamp before it would carry each rounding and each late wake into every
    /// later slot, and the spins of a long run would drift off their grid without an error.
    #[test]
    fn stamp_of_slot_has_no_drift(
        start_us in 0..=LAST_START_US,
        interval_us in SHORTEST_INTERVAL_US..=LONGEST_INTERVAL_US,
        slot in 0..=LAST_SLOT,
        raw_offset_us in 0..LONGEST_INTERVAL_US,
    ) {
        let start = TsUs::from_micros(start_us);
        let interval = DurationUs::from_micros(interval_us);
        let schedule = SpinSchedule::new(start, interval);

        let stamp = schedule.stamp_of(slot);
        let exact = i128::from(start_us) + i128::from(slot) * i128::from(interval_us);
        prop_assert_eq!(i128::from(stamp.micros()), exact);
        prop_assert_eq!(schedule.stamp_of(slot + 1).diff(stamp), interval);

        let woke = stamp + DurationUs::from_micros(raw_offset_us % interval_us);
        prop_assert_eq!(schedule.slot_at(woke), slot, "a wake inside a slot fires that slot");
    }
}

/// CONTRACT: the timer fires the slot that the schedule names for its wake, stamps the tick with
/// the time of that slot, counts the slots it skipped, and sleeps until the slot after it. A late
/// timer that fired each slot it missed would hand the hot thread a burst of stale spins, and one
/// that stamped its wake would move each later spin.
#[test]
fn late_wake_fires_only_the_latest_slot() {
    let start = TsUs::from_micros(1_000_000);
    let schedule = SpinSchedule::new(start, DurationUs::from_micros(1_000));
    let last_sent = 0;
    let woke = start + DurationUs::from_micros(3_400);

    assert_eq!(schedule.slot_at(woke), 3);
    let (slot, skipped) = schedule.slot_to_fire(last_sent, woke);
    assert_eq!(slot, 3, "the wake fires the latest slot that is due");
    assert_eq!(
        schedule.stamp_of(slot),
        TsUs::from_micros(1_003_000),
        "the tick carries the time of its slot, not the time of the wake"
    );
    assert_eq!(
        skipped, 2,
        "slots 1 and 2 get no tick, and the timer counts them"
    );
    assert_eq!(
        schedule.slot_to_fire(slot, woke),
        (4, 0),
        "a wake before the next slot is due fires that slot, so no slot fires twice"
    );

    let next_wake = schedule.stamp_of(slot + 1);
    assert_eq!(
        next_wake,
        TsUs::from_micros(1_004_000),
        "the next sleep ends on the grid"
    );
    assert!(
        next_wake > woke,
        "the next sleep ends after this wake, so no slot fires twice"
    );
}
