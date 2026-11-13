//! Microsecond time types, the engine's monotonic clock, and the counter ticks of latency stamps.
//! A timestamp is microseconds since the Unix epoch in a typed wrapper; a tick becomes a time only
//! through the scale between two anchors.

use core::ops::{Add, Sub};
#[cfg(not(target_arch = "wasm32"))]
use std::time::{Instant, SystemTime, UNIX_EPOCH};
#[cfg(target_arch = "wasm32")]
use web_time::{Instant, SystemTime, UNIX_EPOCH};

use crate::msg::labelled_enum::labelled_enum;

/// Microseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TsUs(i64);

impl TsUs {
    #[inline]
    pub const fn from_micros(us: i64) -> Self {
        Self(us)
    }

    #[inline]
    pub const fn micros(self) -> i64 {
        self.0
    }

    /// Signed span `self - earlier`; negative when `self` precedes `earlier`.
    #[inline]
    pub const fn diff(self, earlier: TsUs) -> DurationUs {
        DurationUs(self.0 - earlier.0)
    }

    pub(crate) fn civil(self) -> CivilTime {
        let seconds = self.0.div_euclid(1_000_000);
        let day_seconds = seconds.rem_euclid(86_400);
        let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
        CivilTime {
            year,
            month,
            day,
            hour: day_seconds / 3_600,
            minute: (day_seconds % 3_600) / 60,
            second: day_seconds % 60,
            micros: self.0.rem_euclid(1_000_000),
        }
    }
}

/// UTC calendar breakdown of an instant, for the display paths that spell a timestamp out. Held
/// apart from [`TsUs`] because nothing downstream of it is a timestamp any more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CivilTime {
    pub(crate) year: i64,
    pub(crate) month: u32,
    pub(crate) day: u32,
    pub(crate) hour: i64,
    pub(crate) minute: i64,
    pub(crate) second: i64,
    pub(crate) micros: i64,
}

/// A signed span in microseconds. Latency uses it too, so there is no third time type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DurationUs(i64);

impl DurationUs {
    pub const ZERO: DurationUs = DurationUs(0);

    /// The engine's smallest representable step.
    pub const RESOLUTION: DurationUs = DurationUs(1);

    #[inline]
    pub const fn from_micros(us: i64) -> Self {
        Self(us)
    }

    #[inline]
    pub const fn from_millis(ms: i64) -> Self {
        Self(ms * 1_000)
    }

    #[inline]
    pub const fn from_secs(secs: i64) -> Self {
        Self(secs * 1_000_000)
    }

    #[inline]
    pub const fn micros(self) -> i64 {
        self.0
    }

    #[inline]
    pub const fn to_secs(self) -> f64 {
        self.0 as f64 / 1e6
    }
}

impl Add<DurationUs> for TsUs {
    type Output = TsUs;

    #[inline]
    fn add(self, rhs: DurationUs) -> TsUs {
        TsUs(self.0 + rhs.0)
    }
}

impl Sub<DurationUs> for TsUs {
    type Output = TsUs;

    #[inline]
    fn sub(self, rhs: DurationUs) -> TsUs {
        TsUs(self.0 - rhs.0)
    }
}

/// One wall-clock reading taken at start, plus the monotonic time elapsed since. A step in the
/// system clock cannot move it, and it stays comparable with venue timestamps.
#[derive(Debug, Clone)]
pub struct EngineClock {
    wall_anchor_ts_us: i64,
    mono_anchor: Instant,
}

impl EngineClock {
    #[expect(
        clippy::disallowed_methods,
        reason = "the clock module wraps the std clocks and the counter, so every other read goes through it"
    )]
    pub fn start() -> Self {
        let wall_anchor_ts_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_micros() as i64)
            .expect("system clock reads before the unix epoch — cannot anchor engine time");
        Self {
            wall_anchor_ts_us,
            mono_anchor: Instant::now(),
        }
    }

    /// Called twice for every hot-path message, and small enough to inline into the caller.
    #[inline]
    #[expect(
        clippy::disallowed_methods,
        reason = "the clock module wraps the std clocks and the counter, so every other read goes through it"
    )]
    pub fn now(&self) -> TsUs {
        let elapsed_us = self.mono_anchor.elapsed().as_micros() as i64;
        TsUs::from_micros(self.wall_anchor_ts_us + elapsed_us)
    }
}

/// A raw reading of the cpu counter, for latency stamps only. It has no order, no arithmetic and no
/// conversion, so no code can decide anything by it. Only the metrics thread may turn a pair of
/// them into nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Tick(u64);

/// The one way to take a latency stamp. `Off` reads no counter, so a replay or a test with
/// measurement off folds exactly what a live run folds.
#[derive(Debug, Clone)]
pub enum Stamper {
    /// Build the clock once at start: building it calibrates the counter.
    #[cfg(not(target_arch = "wasm32"))]
    Counter(quanta::Clock),
    Off,
}

impl Stamper {
    /// `Off` reads nothing and gives the same tick every time.
    #[inline]
    #[cfg_attr(
        not(target_arch = "wasm32"),
        expect(
            clippy::disallowed_methods,
            reason = "the clock module wraps the counter, so every other read goes through it"
        )
    )]
    pub fn stamp(&self) -> Tick {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Stamper::Counter(clock) => Tick(clock.raw()),
            Stamper::Off => Tick(0),
        }
    }
}

/// A tick read beside a reading of the engine clock, so that ticks can be placed in engine time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TickAnchor {
    pub tick: Tick,
    pub at: TsUs,
}

/// The rate of the counter between two anchors. This is the only way to turn ticks into time, and
/// only the metrics thread holds one, so no other code can decide anything by a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TickScale {
    first: TickAnchor,
    last: TickAnchor,
    span_ticks: u64,
    span_ns: u64,
}

impl TickScale {
    /// `None` unless `last` is later than `first` on both the counter and the engine clock.
    pub fn between(first: TickAnchor, last: TickAnchor) -> Option<TickScale> {
        let span_ticks = last
            .tick
            .0
            .checked_sub(first.tick.0)
            .filter(|&ticks| ticks > 0)?;
        let span_us = last
            .at
            .micros()
            .checked_sub(first.at.micros())
            .filter(|&us| us > 0)?;
        Some(TickScale {
            first,
            last,
            span_ticks,
            span_ns: (span_us as u64).saturating_mul(1_000),
        })
    }

    /// The same first anchor with a later last one, which makes the rate more exact as the run
    /// goes on. `None` when `anchor` is not later than the first.
    pub fn extended_to(self, anchor: TickAnchor) -> Option<TickScale> {
        TickScale::between(self.first, anchor)
    }

    /// Nanoseconds from `start` to `end`. `None` when `end` reads before `start`: the pair ran
    /// backward, because a thread moved core or the counter is not invariant.
    pub fn nanos(&self, start: Tick, end: Tick) -> Option<u64> {
        let ticks = end.0.checked_sub(start.0)?;
        let nanos = u128::from(ticks) * u128::from(self.span_ns) / u128::from(self.span_ticks);
        Some(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    /// Nanoseconds from the engine time `since` to `tick`. `None` when the tick reads before it.
    pub fn nanos_since(&self, since: TsUs, tick: Tick) -> Option<u64> {
        let ticks_after_last = i128::from(tick.0) - i128::from(self.last.tick.0);
        let nanos_after_last =
            ticks_after_last.saturating_mul(i128::from(self.span_ns)) / i128::from(self.span_ticks);
        let gap_us = i128::from(self.last.at.micros()) - i128::from(since.micros());
        let nanos = gap_us
            .saturating_mul(1_000)
            .saturating_add(nanos_after_last);
        (nanos >= 0).then(|| u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

/// Whether the cpu has a counter that ticks at one rate on every core, which is what makes two
/// latency stamps from different cores comparable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InvariantCounter {
    Present,
    Absent,
}

impl InvariantCounter {
    /// Asks the cpu by the same rule that the counter crate uses to pick its source, so the start
    /// check names the clock that the stamps really read.
    pub fn detect() -> InvariantCounter {
        #[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
        let is_present = tsc_is_invariant();
        #[cfg(not(all(target_arch = "x86_64", target_feature = "sse2")))]
        let is_present = TARGET_COUNTER.is_some();
        if is_present { InvariantCounter::Present } else { InvariantCounter::Absent }
    }
}

/// The counter crate reads the tsc only when the cpu also has `rdtscp`, so this asks both.
#[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
fn tsc_is_invariant() -> bool {
    let cpu = raw_cpuid::CpuId::new();
    let is_invariant = cpu
        .get_advanced_power_mgmt_info()
        .is_some_and(|power| power.has_invariant_tsc());
    let has_rdtscp = cpu
        .get_extended_processor_and_feature_identifiers()
        .is_some_and(|features| features.has_rdtscp());
    is_invariant && has_rdtscp
}

labelled_enum! {
    /// The clock that latency stamps read. It is named at start, so that nobody takes numbers of
    /// the os clock for numbers of the cpu counter.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum ClockSource {
        /// The time stamp counter of an x86_64 cpu.
        Tsc = "tsc",
        /// The virtual counter of an aarch64 cpu.
        Cntvct = "cntvct_el0",
        /// The monotonic clock of the os, which costs more to read than a cpu counter.
        Monotonic = "monotonic",
    }
    pub fn label;
}

#[cfg(all(target_arch = "x86_64", target_feature = "sse2"))]
const TARGET_COUNTER: Option<ClockSource> = Some(ClockSource::Tsc);
#[cfg(all(target_arch = "aarch64", not(target_os = "ios")))]
const TARGET_COUNTER: Option<ClockSource> = Some(ClockSource::Cntvct);
#[cfg(not(any(
    all(target_arch = "x86_64", target_feature = "sse2"),
    all(target_arch = "aarch64", not(target_os = "ios"))
)))]
const TARGET_COUNTER: Option<ClockSource> = None;

impl ClockSource {
    /// The start check: the clock that latency stamps read on a cpu that reports `counter`. The
    /// counter crate falls back to the os clock without a word, so this is how a run learns it.
    ///
    /// # Errors
    /// [`ClockError::CounterAbsent`] when the cpu has no invariant counter. That refuses the
    /// counter, never the start.
    pub fn check(counter: InvariantCounter) -> Result<ClockSource, ClockError> {
        match (counter, TARGET_COUNTER) {
            (InvariantCounter::Present, Some(source)) => Ok(source),
            _ => Err(ClockError::CounterAbsent),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClockError {
    #[error(
        "the cpu reports no invariant counter, so latency stamps read the {} clock instead",
        ClockError::CounterAbsent.fallback().label()
    )]
    CounterAbsent,
}

impl ClockError {
    /// The clock that latency stamps read once the counter is refused.
    pub const fn fallback(self) -> ClockSource {
        match self {
            ClockError::CounterAbsent => ClockSource::Monotonic,
        }
    }
}

/// The wall clock now, for stamps that never become engine state, so a plain clock read is fine:
/// the link's restart marker, the base of the controller epoch, and log and file timestamps. A
/// clock set before the epoch gives 0 rather than a panic.
#[expect(
    clippy::disallowed_methods,
    reason = "the clock module wraps the std clocks and the counter, so every other read goes through it"
)]
pub(crate) fn boot_stamp_us() -> TsUs {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_micros() as i64);
    TsUs::from_micros(micros)
}

/// Howard Hinnant's `civil_from_days`: the Gregorian year, month and day for a signed count of days
/// since the Unix epoch, exact over the whole range. Every place that spells a date out uses it, so
/// no calendar crate is needed.
pub(crate) fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let shifted = days_since_epoch + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_pivot = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_pivot + 2) / 5 + 1;
    let month = if month_pivot < 10 { month_pivot + 3 } else { month_pivot - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    (year, month as u32, day as u32)
}
