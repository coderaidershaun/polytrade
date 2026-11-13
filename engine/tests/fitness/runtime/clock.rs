//! The clock domains and the latency clock: no operator mixes a venue stamp with a local stamp, no
//! code decides by a counter tick, and the start check names the clock that the stamps read.

use engine::msg::time::{
    ClockError, ClockSource, DurationUs, InvariantCounter, LocalTs, Stamper, Tick, VenueTs,
};

/// A span or a comparison across two clocks compiles silently when both are one type, and its
/// number is then off by the skew between the clocks. Each crossing goes through a named method.
#[test]
fn no_operator_across_clock_domains() {
    assert_lacks!(LocalTs: std::ops::Sub<VenueTs>);
    assert_lacks!(VenueTs: std::ops::Sub<LocalTs>);
    assert_lacks!(LocalTs: std::ops::Add<VenueTs>);
    assert_lacks!(VenueTs: std::ops::Add<LocalTs>);
    assert_lacks!(LocalTs: PartialEq<VenueTs>);
    assert_lacks!(VenueTs: PartialEq<LocalTs>);
    assert_lacks!(LocalTs: PartialOrd<VenueTs>);
    assert_lacks!(VenueTs: PartialOrd<LocalTs>);
    assert_lacks!(LocalTs: From<VenueTs>);
    assert_lacks!(VenueTs: From<LocalTs>);
    assert_lacks!(LocalTs: Into<VenueTs>);
    assert_lacks!(VenueTs: Into<LocalTs>);
    assert_lacks!(LocalTs: std::ops::Sub<LocalTs>);
    assert_lacks!(VenueTs: std::ops::Sub<VenueTs>);

    let local = LocalTs::from_micros(1_000_500);
    assert_eq!(
        local.diff(LocalTs::from_micros(1_000_000)),
        DurationUs::from_micros(500),
        "a span of one domain is the plain difference"
    );
    assert_eq!(
        VenueTs::from_micros(1_000_000).offset_from(local),
        DurationUs::from_micros(-500),
        "the named span from a local stamp to a venue stamp is the same difference, signed"
    );
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
    assert_lacks!(Tick: Into<LocalTs>);
    assert_lacks!(Tick: Into<VenueTs>);
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
