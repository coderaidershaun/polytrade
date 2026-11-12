//! Gamma discovery parse goldens: the clobTokenIds double-parse (JSON array inside a JSON string),
//! window bounds taken from the slug's grid timestamp (never `startDate`/`end_date_iso`), and the
//! no-chrono `end_date_min` ISO formatter pinned against a real capture value.

use polysim::adapters::polymarket::rest::{
    GammaError, GammaMarket, fallback_url, parse_events, parse_fallback,
};
use polysim::config::PolySeries;
use polysim::ids::{Price, Qty};
use polysim::time::TsUs;

use crate::engine_support::ts_secs;

const SERIES: PolySeries = PolySeries::BtcUpDown5m;

const FALLBACK: &str = include_str!("../../fixtures/polymarket/gamma_fallback.json");

// Verbatim market object from a real discovery capture; clobTokenIds + outcomes are each JSON inside
// a JSON string.
const EVENT: &str = r#"[{"slug":"btc-updown-5m-1784439600","markets":[{
  "conditionId":"0xf12ae6035011301e567f92d9a72c445ebc0cd004991f66239a6ed02c8e65f7e4",
  "clobTokenIds":"[\"111394444729792877806659658906374744159106626487508364417476902646505424758248\",\"67063025231054467036218378525223409033496849839613617174918904089424584844374\"]",
  "outcomes":"[\"Up\", \"Down\"]",
  "orderPriceMinTickSize":0.01,"orderMinSize":5,"enableOrderBook":true,"negRisk":false}]}]"#;

#[test]
fn double_parses_token_ids_and_window_from_slug() {
    let market = parse_events(SERIES, EVENT, ts_secs(1_784_439_600)).expect("event parses");
    assert_eq!(
        market,
        GammaMarket {
            slug: "btc-updown-5m-1784439600".into(),
            condition_id: "0xf12ae6035011301e567f92d9a72c445ebc0cd004991f66239a6ed02c8e65f7e4"
                .into(),
            token_up:
                "111394444729792877806659658906374744159106626487508364417476902646505424758248"
                    .into(),
            token_down:
                "67063025231054467036218378525223409033496849839613617174918904089424584844374"
                    .into(),
            tick_size: Price(1_000_000),
            min_order_size: Qty(500_000_000),
            // The window bounds come from the slug's grid timestamp, not from startDate (creation
            // time) or end_date_iso (too coarse).
            window_open_ts_us: TsUs::from_micros(1_784_439_600_000_000),
            window_close_ts_us: TsUs::from_micros(1_784_439_900_000_000),
        }
    );
}

#[test]
fn parse_events_rejects_malformed_input() {
    type RejectCase = (&'static str, &'static str, fn(&GammaError) -> bool);
    let cases: &[RejectCase] = &[
        ("empty_events_array", "[]", |e| {
            matches!(e, GammaError::MarketNotFound { .. })
        }),
        (
            "single_element_token_ids",
            r#"[{"slug":"btc-updown-5m-1784439600","markets":[{"conditionId":"0xabc","clobTokenIds":"[\"111\"]","orderPriceMinTickSize":0.01,"orderMinSize":5}]}]"#,
            |e| matches!(e, GammaError::TokenIds { .. }),
        ),
        (
            // Same market, but the venue orders outcomes ["Down","Up"] — assigning tokens by index
            // would silently invert every downstream row, so discovery must reject it.
            "inverted_outcomes",
            r#"[{"slug":"btc-updown-5m-1784439600","markets":[{"conditionId":"0xabc","clobTokenIds":"[\"111\",\"222\"]","outcomes":"[\"Down\", \"Up\"]","orderPriceMinTickSize":0.01,"orderMinSize":5}]}]"#,
            |e| matches!(e, GammaError::InvalidOutcomes { .. }),
        ),
    ];
    for (name, raw, expect) in cases {
        let error = parse_events(SERIES, raw, ts_secs(1_784_439_600)).expect_err(name);
        assert!(expect(&error), "{name}: got {error:?}");
    }
}

#[test]
fn fallback_url_formats_end_date_min_as_iso_utc() {
    // 1784439540 = 2026-07-19T05:39:00Z; pins the no-chrono ISO formatter.
    assert!(
        fallback_url(SERIES, ts_secs(1_784_439_540)).contains("end_date_min=2026-07-19T05:39:00Z"),
        "fallback query must time-filter with the mandatory end_date_min"
    );
}

#[test]
fn fallback_resolves_current_and_next_windows() {
    let (current, next) = parse_fallback(SERIES, FALLBACK).expect("fallback yields two windows");
    assert_eq!(&*current.slug, "btc-updown-5m-1784449200");
    assert_eq!(&*next.slug, "btc-updown-5m-1784449500");
    // Windows come from the slug grid ts, never a body date field.
    assert_eq!(
        current.window_open_ts_us,
        TsUs::from_micros(1_784_449_200_000_000)
    );
    assert_eq!(
        current.window_close_ts_us,
        TsUs::from_micros(1_784_449_500_000_000)
    );
    assert_eq!(
        next.window_open_ts_us,
        TsUs::from_micros(1_784_449_500_000_000)
    );
    assert_eq!(
        &*current.token_up,
        "12935926064588426924078449947177910958215776269411501731627440228801766632772"
    );
}

#[test]
fn parse_fallback_reports_the_shortfall() {
    let cases: &[(&str, &str, usize)] = &[
        (
            // First row's slug is not a BTC 5-min window (filter_map skips it); only one valid
            // window is left, so the pair can't be formed.
            "one_unparseable_row",
            r#"[
              {"slug":"not-a-btc-window","markets":[{"conditionId":"0xa","clobTokenIds":"[\"1\",\"2\"]","outcomes":"[\"Up\", \"Down\"]","orderPriceMinTickSize":0.01,"orderMinSize":5}]},
              {"slug":"btc-updown-5m-1784449200","markets":[{"conditionId":"0xb","clobTokenIds":"[\"1\",\"2\"]","outcomes":"[\"Up\", \"Down\"]","orderPriceMinTickSize":0.01,"orderMinSize":5}]}
            ]"#,
            1,
        ),
        ("empty_array", "[]", 0),
    ];
    for (name, raw, found) in cases {
        let error = parse_fallback(SERIES, raw).expect_err(name);
        assert!(
            matches!(error, GammaError::FallbackTooFew { found: f } if f == *found),
            "{name}: got {error:?}"
        );
    }
}

#[test]
fn rate_limited_display_reads_for_some_and_none() {
    let with = GammaError::RateLimited {
        url: "https://clob.polymarket.com/book".to_owned(),
        status: 429,
        retry_after_secs: Some(5),
    };
    assert_eq!(
        with.to_string(),
        "polymarket rate limited https://clob.polymarket.com/book: http 429, retry after 5s"
    );
    let without = GammaError::RateLimited {
        url: "https://clob.polymarket.com/book".to_owned(),
        status: 429,
        retry_after_secs: None,
    };
    // A missing header must read as plain words, not as a formatted `None`.
    assert!(
        without
            .to_string()
            .ends_with("http 429, no retry-after header"),
        "None must not render as a Debug Option: {without}"
    );
}
