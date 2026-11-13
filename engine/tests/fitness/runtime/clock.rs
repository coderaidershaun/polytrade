//! The clock parts of latency measurement: a counter tick that no code can decide by, and a start
//! check that names the clock the stamps really read.

use engine::msg::time::{
    ClockError, ClockSource, DurationUs, InvariantCounter, Stamper, Tick, TsUs,
};

/// Compiles only while the type lacks the bound. A second impl that the bound switches on would
/// make the call ambiguous, so the build fails on the line that names the bound.
macro_rules! assert_lacks {
    ($type:ty: $($bound:tt)+) => {{
        trait Ambiguous<Marker> {
            fn check() {}
        }
        impl<T> Ambiguous<()> for T {}
        impl<T: $($bound)+> Ambiguous<u8> for T {}
        <$type as Ambiguous<_>>::check();
    }};
}

#[test]
fn tick_has_no_ordering_or_conversion() {
    fn is_plain_data<T: Copy + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync>() {}
    is_plain_data::<Tick>();

    assert_lacks!(Tick: PartialOrd);
    assert_lacks!(Tick: std::ops::Add<Tick>);
    assert_lacks!(Tick: std::ops::Sub<Tick>);
    assert_lacks!(Tick: std::ops::Add<u64>);
    assert_lacks!(Tick: std::ops::Sub<u64>);
    assert_lacks!(Tick: std::ops::Add<DurationUs>);
    assert_lacks!(Tick: Into<u64>);
    assert_lacks!(Tick: Into<i64>);
    assert_lacks!(Tick: Into<TsUs>);
    assert_lacks!(Tick: Into<DurationUs>);
    assert_lacks!(Tick: From<u64>);

    let (clock, mock) = quanta::Clock::mock();
    let counter = Stamper::Counter(clock);
    let first = counter.stamp();
    mock.increment(800_u64);
    assert_ne!(counter.stamp(), first, "each stamp reads the counter again");

    let off = Stamper::Off;
    let first = off.stamp();
    assert_eq!(
        off.stamp(),
        first,
        "with measurement off every stamp is the same tick, so no output can differ by one"
    );
}

#[test]
fn counter_absent_is_named_in_anchor() {
    let refusal = ClockSource::check(InvariantCounter::Absent)
        .expect_err("a cpu with no invariant counter refuses the counter");
    assert!(matches!(refusal, ClockError::CounterAbsent));
    assert_eq!(
        refusal.fallback(),
        ClockSource::Monotonic,
        "the run still starts, and its stamps read the os clock"
    );
    let start_line = refusal.to_string();
    assert!(
        start_line.contains(ClockSource::Monotonic.label()),
        "the start line must name the clock that the stamps read: {start_line}"
    );
    let row_sources = super::metrics::row_clock_sources(refusal.fallback());
    assert!(
        !row_sources.is_empty()
            && row_sources
                .iter()
                .all(|source| *source == ClockSource::Monotonic),
        "each row of the metrics thread names the clock that its stamps read: {row_sources:?}"
    );

    let present = ClockSource::check(InvariantCounter::Present);
    match std::env::consts::ARCH {
        "x86_64" => assert_eq!(present.map(ClockSource::label), Ok("tsc")),
        "aarch64" => assert_eq!(present.map(ClockSource::label), Ok("cntvct_el0")),
        other => assert!(
            matches!(present, Err(ClockError::CounterAbsent)),
            "{other} has no counter that the stamps can read"
        ),
    }
}
