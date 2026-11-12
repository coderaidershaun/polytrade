//! Reconnect backoff exists to spread a correlated failure: when a venue drops, every actor that
//! was on it retries at once, and a schedule they share reconverges them round after round. Pinned
//! here: each call is an independent draw inside the configured caps, the caps hold at any attempt
//! number a supervisor could reach, and no draw comes back as no delay at all.

use std::collections::HashSet;
use std::time::Duration;

use polysim::adapters::backoff::BackoffCaps;
use proptest::prelude::*;

/// Wide enough that two independent draws colliding, or one landing on zero, is a once-in-a-million
/// event rather than an occasional red — the assertions below are about the mechanism, not luck.
const WIDE_DRAW_RANGE: BackoffCaps = BackoffCaps {
    initial: Duration::from_secs(3_600),
    max: Duration::from_secs(3_600),
};

const SAMPLE_DRAWS: usize = 2_048;

/// FITNESS: the wall clock is only readable to about a microsecond, so two actors losing the same
/// venue together can and do sample it identically. The per-draw counter is what still tells them
/// apart; without it they would reconnect in lockstep, which is the failure jitter exists to
/// prevent. Zero is checked in the same loop because it is the same entropy path failing: a run of
/// thousands hitting it means every retry is now immediate, spinning against a venue that just
/// refused the last one.
#[test]
fn draws_the_clock_cannot_tell_apart_still_come_back_different() {
    let mut previous = None;
    let mut distinct = HashSet::new();
    for draw in 0..SAMPLE_DRAWS {
        let delay = WIDE_DRAW_RANGE.delay(0);
        assert_ne!(
            Some(delay),
            previous,
            "draw {draw} repeated its predecessor, so two callers sharing a clock reading share a \
             delay and reconnect together"
        );
        assert_ne!(
            delay,
            Duration::ZERO,
            "draw {draw} asks the caller to retry immediately"
        );
        previous = Some(delay);
        distinct.insert(delay);
    }
    assert!(
        distinct.len() > SAMPLE_DRAWS / 2,
        "only {} of {SAMPLE_DRAWS} draws were distinct, so the delay is closer to a schedule than to a \
         draw",
        distinct.len()
    );
}

/// FITNESS: attempt counts come from supervisors that keep counting while a venue stays down, so
/// the doubling has to stop somewhere the shift can still be taken.
#[test]
fn an_unbounded_attempt_count_saturates_rather_than_overflowing() {
    let caps = BackoffCaps::default();
    for attempt in [16, 17, 31, 32, 33, 64, 1_000, u32::MAX] {
        assert!(
            caps.delay(attempt) <= caps.max,
            "attempt {attempt} drew past the configured maximum"
        );
    }
}

proptest! {
    /// FITNESS: `initial` is the first ceiling and `max` bounds every later one, whatever the two
    /// are set to relative to each other.
    #[test]
    fn every_draw_lands_inside_the_caps(
        initial_ms in 1u64..5_000,
        max_ms in 1u64..120_000,
        attempt in 0u32..24,
    ) {
        let caps = BackoffCaps {
            initial: Duration::from_millis(initial_ms),
            max: Duration::from_millis(max_ms),
        };
        let delay = caps.delay(attempt);
        prop_assert!(delay <= caps.max, "{:?} exceeds max {:?}", delay, caps.max);
        if attempt == 0 {
            prop_assert!(
                delay <= caps.initial.min(caps.max),
                "{:?} exceeds the first ceiling {:?}",
                delay,
                caps.initial
            );
        }
    }
}
