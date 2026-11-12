//! Polymarket discovery arithmetic: the 300s grid and A/B slot parity are the identity the whole
//! rotation machine rests on — a misplaced window boundary or a wrong slot silently mints the wrong
//! market or double-books a slot — plus the T-60s handover the zero-gap rotation subscribes on.

use polysim::adapters::polymarket::discovery::{PolySchedule, Slot};
use proptest::prelude::*;

use crate::engine_support::ts_secs;

const SECOND_US: i64 = 1_000_000;
const WINDOW_SECS: i64 = 300;

#[test]
fn next_window_subscribe_matches_current_nominal_end_minus_lead() {
    let schedule = PolySchedule::BTC_5M;
    let now = ts_secs(1_784_439_123); // mid-window
    let current = schedule.current_window(now);
    let next = schedule.next_window(now);
    // The pinned handover: subscribe N+1 at N's nominal end minus the 60s lead (T-60s).
    assert_eq!(
        schedule.subscribe_at(next.window_start_ts_us),
        current.window_close_ts_us - schedule.subscribe_lead
    );
    assert_eq!(next.window_start_ts_us, current.window_close_ts_us);
}

proptest! {
    /// FITNESS: the window containing any instant is 300s-grid-aligned and brackets that instant, and
    /// its slug seconds are divisible by 300. A drift here would subscribe the wrong 5-minute market.
    #[test]
    fn window_containing_is_grid_aligned_and_brackets_now(now_secs in 0i64..4_000_000_000i64) {
        let schedule = PolySchedule::BTC_5M;
        let now = ts_secs(now_secs);
        let window = schedule.current_window(now);
        let len = schedule.window_len.micros();

        prop_assert_eq!(window.window_start_ts_us.micros() % len, 0);
        prop_assert!(window.window_start_ts_us <= now);
        prop_assert!(now < window.window_close_ts_us);
        prop_assert_eq!(window.window_close_ts_us.micros() - window.window_start_ts_us.micros(), len);

        let slug_seconds = window.window_start_ts_us.micros() / SECOND_US;
        prop_assert_eq!(slug_seconds % WINDOW_SECS, 0);
    }

    /// FITNESS: slot is exactly the window index parity — even windows map to A, odd to B — for any window on the grid.
    #[test]
    fn slot_is_window_index_parity(index in 0i64..20_000_000i64) {
        let schedule = PolySchedule::BTC_5M;
        let start = ts_secs(index * WINDOW_SECS);
        let window = schedule.window_at(start);
        prop_assert_eq!(window.index, index);
        let expected = if index % 2 == 0 { Slot::A } else { Slot::B };
        prop_assert_eq!(window.slot, expected);
    }
}
