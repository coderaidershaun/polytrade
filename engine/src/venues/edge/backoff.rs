//! Exponential reconnect backoff with full jitter, drawn without a `rand` dependency.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Bounds the doubling shift so it cannot overflow `u32`. Under any caps a caller would configure
/// the ceiling has saturated at `max` long before here.
const MAX_DOUBLINGS: u32 = 16;

/// splitmix64's step constant, used to walk the draw counter across the whole word.
const GOLDEN_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

/// Advances once per draw. Without it the entropy would be a wall-clock reading alone, so two
/// actors losing the same venue in the same instant — the correlated failure jitter exists to
/// spread — would draw delays microseconds apart and reconnect in lockstep, round after round.
static DRAWS: AtomicU64 = AtomicU64::new(0);

/// Bounds on the delay: full jitter is drawn from `[0, min(max, initial · 2^attempt)]`, so `initial`
/// sets the first ceiling and `max` caps every later one however `initial` is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffCaps {
    pub initial: Duration,
    pub max: Duration,
}

impl Default for BackoffCaps {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(250),
            max: Duration::from_secs(30),
        }
    }
}

impl BackoffCaps {
    /// Full jitter for `attempt`, counted from zero for the first retry. Drawn afresh on every
    /// call, so repeating the same `attempt` gives a different answer — the caps bound a
    /// distribution, they are not a schedule.
    ///
    /// Public because the caps are: a configuration type a caller can build but whose behaviour it
    /// cannot reach keeps half its contract to itself.
    #[must_use]
    pub fn delay(&self, attempt: u32) -> Duration {
        let ceiling = self
            .initial
            .saturating_mul(1u32 << attempt.min(MAX_DOUBLINGS))
            .min(self.max);
        let ceiling_us = u64::try_from(ceiling.as_micros()).unwrap_or(u64::MAX);
        if ceiling_us == 0 {
            return Duration::ZERO;
        }
        let Ok(since_epoch) = SystemTime::now().duration_since(UNIX_EPOCH) else {
            // No seed to draw from, so take the safe end of the range rather than the fast one:
            // returning no delay would spin the reconnect loop against the venue at full speed.
            return ceiling;
        };
        let step = DRAWS
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_mul(GOLDEN_GAMMA);
        let entropy = mix(since_epoch.as_nanos() as u64).wrapping_add(step);
        Duration::from_micros(mix(entropy) % (ceiling_us + 1))
    }
}

/// splitmix64's finaliser: inputs one apart come out differing in every bit, which is what makes
/// successive draws independent rather than adjacent.
const fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
