//! The REST cool-off sits on the order path, so a venue answer must never be able to talk a client
//! into hammering a ban nor into sleeping through an exit sweep. Pinned here: the wait is bounded
//! at both ends whatever `Retry-After` says, an active window can only ever be extended, the whole
//! thing is a function of the caller's `now`, and the clones handed to the clients that share one
//! venue budget really are one window.

use std::time::{Duration, Instant};

use polysim::adapters::rest_quiet::{RestQuiet, SharedRestQuiet};
use proptest::prelude::*;

const FLOOR: Duration = Duration::from_secs(2);
const CEILING: Duration = Duration::from_secs(60);

/// FITNESS: `Retry-After` arrives straight off the wire, so the value is whatever answers as the
/// venue. At the top of the range the deadline arithmetic overflows outright, which is why the
/// unclamped form is a panic rather than a long sleep. No header means the venue said nothing
/// about how long, not that the client may retry at once, so it clamps to the same floor as a
/// too-small header.
#[test]
fn open_clamps_retry_after_between_floor_and_ceiling() {
    let now = Instant::now();
    let cases: &[(&str, Option<u64>, Duration)] = &[
        ("absurd_max", Some(u64::MAX), CEILING),
        ("absurd_max_minus_one", Some(u64::MAX - 1), CEILING),
        ("absurd_max_half", Some(u64::MAX / 2), CEILING),
        ("absurd_day", Some(86_400), CEILING),
        ("absurd_hour", Some(3_600), CEILING),
        ("just_over_ceiling", Some(61), CEILING),
        ("no_header", None, FLOOR),
        ("zero_seconds", Some(0), FLOOR),
        ("one_second", Some(1), FLOOR),
    ];
    for (name, retry_after, expected) in cases {
        let mut quiet = RestQuiet::new();
        assert_eq!(
            quiet.open(*retry_after, now),
            *expected,
            "{name}: a Retry-After of {retry_after:?}s did not clamp to {expected:?}, so one \
             venue answer can park order placement and cancellation for the rest of the run"
        );
    }
}

/// FITNESS: the window is over at its deadline, not one tick after — `is_active` and `remaining`
/// have to agree on which side of the boundary the caller is standing.
#[test]
fn the_window_closes_exactly_at_its_deadline() {
    let now = Instant::now();
    let mut quiet = RestQuiet::new();
    assert!(!quiet.is_active(now));

    let wait = quiet.open(Some(10), now);
    assert!(quiet.is_active(now + wait - Duration::from_micros(1)));
    assert!(!quiet.is_active(now + wait));
    assert_eq!(quiet.remaining(now + wait), None);
}

/// FITNESS: Binance charges its market-data reads and its signed order path against one per-IP
/// allowance, so a 429 earned by either has to hold the other off — a second client that learned
/// nothing would retry straight into a harder ban. Cloning is how that sharing is expressed, so a
/// clone that quietly carried its own deadline would restore the defect in silence.
#[test]
fn a_window_opened_through_one_clone_holds_off_the_other() {
    let now = Instant::now();
    let market_data = SharedRestQuiet::new();
    let order_path = market_data.clone();
    assert!(!order_path.is_active(now));

    let wait = market_data.open(Some(30), now);
    assert_eq!(
        order_path.remaining(now),
        Some(wait),
        "a rate limit earned by the market-data client left the order path free to send"
    );
    assert!(order_path.is_active(now + wait - Duration::from_micros(1)));
    assert!(!order_path.is_active(now + wait));
}

/// FITNESS: extend-only is what stops a second answer cutting a ban the venue is still enforcing,
/// and the second answer is now as likely to arrive at the other owner as at the one that opened
/// the window. The rule has to hold across owners or sharing hands back the shortening it removed.
#[test]
fn a_second_owners_answer_only_ever_extends_the_shared_window() {
    let now = Instant::now();
    let market_data = SharedRestQuiet::new();
    let order_path = market_data.clone();
    market_data.open(Some(45), now);

    let later = now + Duration::from_secs(1);
    assert_eq!(
        order_path.open(None, later),
        Duration::from_secs(44),
        "the order path's own rate limit replaced the market-data deadline instead of extending it"
    );

    let extended = order_path.open(Some(60), later);
    assert_eq!(
        market_data.remaining(later),
        Some(extended),
        "the extension stayed with the owner that made it, so the other resumes mid-ban"
    );
}

proptest! {
    /// FITNESS: the cool-off reads no clock of its own, so two clients fed the same headers and
    /// the same `now` sequence hold off for the same span — a recorded run replays exactly.
    #[test]
    fn the_answer_is_a_function_of_the_headers_and_now(
        headers in prop::collection::vec(prop::option::of(any::<u64>()), 1..24),
        gaps_secs in prop::collection::vec(0u64..90, 24),
    ) {
        let origin = Instant::now();
        let mut left = RestQuiet::new();
        let mut right = RestQuiet::new();
        let mut elapsed = Duration::ZERO;

        for (header, gap) in headers.iter().zip(&gaps_secs) {
            elapsed += Duration::from_secs(*gap);
            let now = origin + elapsed;
            let waited = left.open(*header, now);
            prop_assert_eq!(waited, right.open(*header, now));
            prop_assert!(waited >= FLOOR, "a cool-off shorter than the floor: {:?}", waited);
            prop_assert!(waited <= CEILING, "a cool-off longer than the ceiling: {:?}", waited);
            prop_assert_eq!(left.remaining(now), Some(waited));
        }
    }

    /// FITNESS: extend-only stated as the general rule — whatever the second answer carries and
    /// however late it lands, the deadline never moves backwards.
    #[test]
    fn a_second_answer_only_ever_extends_the_deadline(
        first in 0u64..3_600,
        second in 0u64..3_600,
        gap_secs in 0u64..120,
    ) {
        let now = Instant::now();
        let mut quiet = RestQuiet::new();
        let first_deadline = now + quiet.open(Some(first), now);

        let later = now + Duration::from_secs(gap_secs);
        let second_deadline = later + quiet.open(Some(second), later);
        prop_assert!(
            second_deadline >= first_deadline,
            "the deadline moved back by {:?}",
            first_deadline - second_deadline
        );
    }
}
