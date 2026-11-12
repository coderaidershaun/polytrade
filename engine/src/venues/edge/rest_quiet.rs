//! REST cool-off: after a 429 (Binance also bans with 418) the clients that earned it hold off,
//! honouring `Retry-After` within bounds, rather than retrying into a harder ban. A window covers
//! one budget, so its owners are whoever spends that budget: Binance's market-data reads and its
//! signed order path count against a single per-IP allowance and share one window through
//! [`SharedRestQuiet`], while Polymarket, whose allowance is its own, holds a plain [`RestQuiet`].

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

const QUIET_FLOOR_SECS: u64 = 2;

// `Retry-After` is whatever answers as the venue, and it reaches here straight off the wire. Past a
// minute it stops being an instruction worth obeying: the run's own reconnect and exit-sweep
// supervision has to keep making progress on a path that carries order placement and cancellation,
// and at the top of the range the deadline arithmetic overflows outright.
const QUIET_CEILING_SECS: u64 = 60;

/// Reads no clock of its own: every method takes `now`, so a cool-off replays exactly.
///
/// The rest of the contract is a promise the order path leans on, which is why it is stated out
/// here rather than left to whichever adapter happens to hold one: the wait is floored so a client
/// cannot answer a rate limit by retrying immediately, capped so a hostile or mistaken header
/// cannot park order placement and cancellation for the rest of the run, and extend-only so a
/// later answer can never cut a window the venue is still enforcing.
#[derive(Debug, Default)]
pub struct RestQuiet {
    until: Option<Instant>,
}

impl RestQuiet {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Opens the cool-off, or extends one already running, and answers how long the caller must now
    /// hold off. Extend-only: a second rate-limited answer arriving mid-window usually carries no
    /// header at all, and overwriting the deadline would cut a live ban back to the floor at the
    /// moment the venue is signalling harder.
    pub fn open(&mut self, retry_after_secs: Option<u64>, now: Instant) -> Duration {
        let secs = retry_after_secs
            .unwrap_or(QUIET_FLOOR_SECS)
            .clamp(QUIET_FLOOR_SECS, QUIET_CEILING_SECS);
        let opens_until = now + Duration::from_secs(secs);
        let until = self.until.map_or(opens_until, |live| live.max(opens_until));
        self.until = Some(until);
        until - now
    }

    #[must_use]
    pub fn is_active(&self, now: Instant) -> bool {
        self.remaining(now).is_some()
    }

    #[must_use]
    pub fn remaining(&self, now: Instant) -> Option<Duration> {
        self.until
            .filter(|until| now < *until)
            .map(|until| until - now)
    }
}

/// One [`RestQuiet`] reachable from several async owners, so a rate limit earned by any of them
/// holds all of them off. Clone to hand out; every clone is the same window, and a fresh `new()` is
/// a fresh budget.
///
/// The lock is an edge-side convenience rather than shared state in the hot-path sense: it guards
/// nothing but the deadline read or write, is never held across an await, and answers the same
/// question the unshared window does.
#[derive(Debug, Clone, Default)]
pub struct SharedRestQuiet {
    window: Arc<Mutex<RestQuiet>>,
}

impl SharedRestQuiet {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// See [`RestQuiet::open`]. Answers the caller's own hold-off, which is the shared deadline: a
    /// client arriving mid-window is told to wait out what someone else's rate limit started.
    pub fn open(&self, retry_after_secs: Option<u64>, now: Instant) -> Duration {
        self.window().open(retry_after_secs, now)
    }

    #[must_use]
    pub fn is_active(&self, now: Instant) -> bool {
        self.window().is_active(now)
    }

    #[must_use]
    pub fn remaining(&self, now: Instant) -> Option<Duration> {
        self.window().remaining(now)
    }

    fn window(&self) -> MutexGuard<'_, RestQuiet> {
        // A deadline written by a thread that then panicked is still a deadline the venue is
        // enforcing, and refusing to read it would fail the order path over a poison flag that
        // nothing under this lock can even raise.
        self.window.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
