//! REST cross-validation: independently fetch a live window's CLOB `/book` snapshot and compare its
//! touch to the streamed book the observer reconstructed. The window's token is resolved via Gamma
//! (the same public path the adapter uses), so the check shares no state with the adapter under test.

use std::collections::BTreeSet;

use polysim::adapters::polymarket::rest::{PolyRest, book_url};
use polysim::time::TsUs;
use serde::Deserialize;

use crate::observer::RotationObserver;
use crate::{WINDOW_SECS, unix_now_s};

/// The streamed touch and a REST `/book` touch can differ by a few 0.01 ticks for the same book: the
/// venue carries no sequence numbers, re-emits the full book ~150ms, and the REST fetch adds latency,
/// so in-flight updates land between the two reads. 0.05 = five ticks of headroom for that drift.
const REST_TOLERANCE: f64 = 0.05;
/// Cross-validate a window only once it is well clear of both edges — the book is settled and
/// unambiguously the current window's, not a boundary flip.
const MID_WINDOW_LO_S: i64 = 90;
const MID_WINDOW_HI_S: i64 = 240;

/// One streamed-vs-REST top-of-book comparison for a live window's Up token.
pub struct CrossValidation {
    window_start_s: i64,
    leg_label: String,
    streamed_bid: Option<f64>,
    streamed_ask: Option<f64>,
    rest_bid: Option<f64>,
    rest_ask: Option<f64>,
}

impl CrossValidation {
    fn within_tolerance(&self) -> bool {
        let ok = |streamed: Option<f64>, rest: Option<f64>| match (streamed, rest) {
            (Some(a), Some(b)) => (a - b).abs() <= REST_TOLERANCE,
            // A momentarily one-sided book on either read is not a mismatch — the other side carries.
            _ => true,
        };
        // At least one side must be comparable, so an all-empty pair never passes vacuously.
        let comparable = (self.streamed_bid.is_some() && self.rest_bid.is_some())
            || (self.streamed_ask.is_some() && self.rest_ask.is_some());
        comparable && ok(self.streamed_bid, self.rest_bid) && ok(self.streamed_ask, self.rest_ask)
    }

    pub fn print(&self) {
        println!(
            "  REST cross-check [{}] window@{}: streamed bid/ask {}/{} vs REST {}/{} — {}",
            self.leg_label,
            self.window_start_s,
            fmt(self.streamed_bid),
            fmt(self.streamed_ask),
            fmt(self.rest_bid),
            fmt(self.rest_ask),
            if self.within_tolerance() { "within tolerance" } else { "OUT OF TOLERANCE" }
        );
    }
}

/// Once per window, mid-life, fetch the current window's Up-token `/book` snapshot and compare its
/// touch to the streamed book. Returns `None` (skips) outside the mid-window band, for an
/// already-checked window, an invalid leg, or any REST failure (reason printed at the failure point).
pub async fn maybe_cross_validate(
    rest: &PolyRest,
    observer: &RotationObserver,
    slot_up: &[usize; 2],
    validated: &mut BTreeSet<i64>,
) -> Option<CrossValidation> {
    let now_s = unix_now_s();
    let offset = now_s.rem_euclid(WINDOW_SECS);
    if !(MID_WINDOW_LO_S..=MID_WINDOW_HI_S).contains(&offset) {
        return None;
    }
    let window_start_s = now_s - offset;
    if validated.contains(&window_start_s) {
        return None;
    }
    let slot = (window_start_s / WINDOW_SECS).rem_euclid(2) as usize;
    let leg = slot_up[slot];
    if !observer.leg_is_valid(leg) {
        return None;
    }
    validated.insert(window_start_s);

    let market = match rest
        .resolve_slug(TsUs::from_micros(window_start_s * 1_000_000))
        .await
    {
        Ok(market) => market,
        Err(error) => {
            println!("  REST cross-check window@{window_start_s}: gamma resolve failed: {error}");
            return None;
        }
    };
    let (rest_bid, rest_ask) = fetch_rest_touch(rest, &market.token_up, window_start_s).await?;
    let book = observer.leg_book(leg)?;
    Some(CrossValidation {
        window_start_s,
        leg_label: observer.leg_label(leg).to_owned(),
        streamed_bid: book.best_bid().map(|level| level.price.to_f64()),
        streamed_ask: book.best_ask().map(|level| level.price.to_f64()),
        rest_bid,
        rest_ask,
    })
}

/// Best bid = highest bid price, best ask = lowest ask price — from the raw REST arrays without
/// assuming their sort order. `None` (reason printed) on any transport, status, or decode failure,
/// so the caller simply skips this window.
async fn fetch_rest_touch(
    rest: &PolyRest,
    token: &str,
    window_start_s: i64,
) -> Option<(Option<f64>, Option<f64>)> {
    let (status, body) = match rest.fetch_status_and_text(&book_url(token)).await {
        Ok(pair) => pair,
        Err(error) => {
            println!("  REST cross-check window@{window_start_s}: /book transport failed: {error}");
            return None;
        }
    };
    if status != 200 {
        println!("  REST cross-check window@{window_start_s}: /book returned http {status}");
        return None;
    }
    let book: RestBook = match serde_json::from_str(&body) {
        Ok(book) => book,
        Err(error) => {
            println!("  REST cross-check window@{window_start_s}: /book decode failed: {error}");
            return None;
        }
    };
    let best = |levels: &[RestLevel], pick_max: bool| {
        levels
            .iter()
            .filter_map(|level| level.price.parse::<f64>().ok())
            .fold(None, |acc: Option<f64>, price| {
                Some(match acc {
                    Some(current) if pick_max => current.max(price),
                    Some(current) => current.min(price),
                    None => price,
                })
            })
    };
    Some((best(&book.bids, true), best(&book.asks, false)))
}

pub fn print_summary(cross_validations: &[CrossValidation]) {
    let within = cross_validations
        .iter()
        .filter(|validation| validation.within_tolerance())
        .count();
    println!(
        "\nREST cross-validation: {}/{} within {REST_TOLERANCE} tolerance",
        within,
        cross_validations.len()
    );
}

/// A failed fetch never becomes a `CrossValidation`, so every element is a completed comparison —
/// require them ALL within tolerance, not just one.
pub fn assert_all_within_tolerance(cross_validations: &[CrossValidation]) {
    let total = cross_validations.len();
    let within = cross_validations
        .iter()
        .filter(|validation| validation.within_tolerance())
        .count();
    assert!(
        total >= 1,
        "no REST /book cross-validation completed in the run"
    );
    assert!(
        within == total,
        "{} of {total} completed REST comparisons fell outside {REST_TOLERANCE} tolerance — see cross-check lines",
        total - within
    );
}

#[derive(Deserialize)]
struct RestBook {
    #[serde(default)]
    bids: Vec<RestLevel>,
    #[serde(default)]
    asks: Vec<RestLevel>,
}

#[derive(Deserialize)]
struct RestLevel {
    price: String,
}

fn fmt(value: Option<f64>) -> String {
    value.map_or_else(|| "—".to_owned(), |price| format!("{price:.3}"))
}
