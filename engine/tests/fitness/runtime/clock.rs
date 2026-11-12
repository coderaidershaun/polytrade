//! The clock parts of latency measurement: a counter tick that no code can decide by, and a start
//! check that names the clock the stamps really read.

use engine::msg::time::{DurationUs, Stamper, Tick, TsUs};

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
