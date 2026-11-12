//! The chart scenes, built from fixed waves rather than random numbers, so every run draws the
//! same.

use polysim::ids::{FIXED_SCALE, InstrumentId, Price, Qty, Side};
use polysim::msg::inbound::Level;
use polysim::msg::ui::{UiBookState, UiLatencyCell, UiLatencyRow, UiLatencySummary};
use polysim::time::DurationUs;
use polysim::workstation::chart::model::{BookContinuity, ChartModel};

use crate::fixture::{self, BASE_TS, INSTRUMENT, book, px, unit};

const SPIN: DurationUs = DurationUs::from_micros(1_000_000);
const WINDOW_BUCKETS: i64 = 300;

const COARSE_SPIN: DurationUs = DurationUs::from_micros(15_000_000);
const COARSE_WINDOW_BUCKETS: i64 = 20;

const COMMITS_PER_BUCKET: i64 = 4;

const TICK: Price = Price(FIXED_SCALE);
const BASE_BID: i64 = 65_990;

const LATENCY_SAMPLES: u64 = 600;

/// `tick` is `Price(0)` for an instrument with no venue grid.
pub struct ChartScene {
    pub name: &'static str,
    pub chart: ChartModel,
    pub instrument: InstrumentId,
    pub tick: Price,
}

pub fn chart_scenes() -> Vec<ChartScene> {
    vec![
        growing(),
        full_window(),
        holes(),
        flat(),
        one_sided(),
        no_tick_grid(),
        dense_fills(),
        fill_off_mid(),
        coarse_buckets(),
    ]
}

/// Every cell of the latency grid filled, from a few microseconds to a mean past the grid's bound,
/// so the grid shows its overflow word.
pub fn latency_summary() -> UiLatencySummary {
    UiLatencySummary {
        market_data: latency_row([4_200, 38, 12, 85, 4_340, 8_900]),
        execution: latency_row([21_500, 45, 18, 140, 21_700, 12_400_000]),
        hot_path: latency_row([3_100, 29, 9, 61, 3_200, 6_400]),
        backlog_ema: Some(0.4),
    }
}

/// The means in microseconds, in the grid's column order.
fn latency_row(means_us: [i64; 6]) -> UiLatencyRow {
    let [
        exchange_to_received,
        received_to_queued,
        queue_wait,
        processing,
        end_to_end,
        order_round_trip,
    ] = means_us.map(|mean_us| UiLatencyCell {
        count: LATENCY_SAMPLES,
        sum_us: mean_us * LATENCY_SAMPLES as i64,
    });
    UiLatencyRow {
        exchange_to_received,
        received_to_queued,
        queue_wait,
        processing,
        end_to_end,
        order_round_trip,
    }
}

/// A series still filling its window, so it grows from the left before it slides. The window is
/// fixed from the start, so the candles are as wide as in the full window.
fn growing() -> ChartScene {
    let mut feed = Feed::new(Some(TICK));
    feed.walk(0..90);
    feed.fill(30, Side::Buy, BASE_BID);
    feed.fill(62, Side::Sell, BASE_BID + 2);
    feed.finish("growing — 90 s of 300")
}

/// More than five minutes of data, so the window slides and the line is at its densest.
fn full_window() -> ChartScene {
    let mut feed = Feed::new(Some(TICK));
    feed.walk(0..WINDOW_BUCKETS + 120);
    for step in (40..WINDOW_BUCKETS + 120).step_by(90) {
        feed.fill(step, side_for(step), mid_bid(step));
    }
    feed.finish("full window — sliding")
}

/// Two kinds of break that must look the same: spins with no mid, and snapshots lost on the way.
fn holes() -> ChartScene {
    let mut feed = Feed::new(Some(TICK));
    feed.walk(0..120);
    feed.walk(150..240);
    feed.gap();
    feed.walk(240..300);
    feed.finish("holes — missing seconds + lane gap")
}

/// A flat series, whose equal high and low the bounds must widen rather than divide by zero.
fn flat() -> ChartScene {
    let mut feed = Feed::new(Some(TICK));
    for step in 0..240 {
        feed.still(step, BASE_BID);
    }
    feed.finish("flat series")
}

/// A book with bids only, so there is no mid and the chart says so.
fn one_sided() -> ChartScene {
    let mut feed = Feed::new(Some(TICK));
    for step in 0..120 {
        feed.bid_only(step);
    }
    feed.finish("one-sided book — no mid")
}

/// An instrument with no tick grid, as on Polymarket.
fn no_tick_grid() -> ChartScene {
    let mut feed = Feed::new(None);
    feed.walk(0..120);
    ChartScene {
        name: "no tick grid",
        chart: feed.chart,
        instrument: INSTRUMENT,
        tick: Price(0),
    }
}

/// Fills on both sides every spin, to check the markers stay distinct from the mid line.
fn dense_fills() -> ChartScene {
    let mut feed = Feed::new(Some(TICK));
    feed.walk(0..120);
    for step in 0..120 {
        feed.fill(step, Side::Buy, mid_bid(step) - 3);
        feed.fill(step, Side::Sell, mid_bid(step) + 5);
    }
    feed.finish("dense fills both sides")
}

/// Fills far outside the mid's range. The bounds take them in, so every marker stays on screen.
fn fill_off_mid() -> ChartScene {
    let mut feed = Feed::new(Some(TICK));
    feed.walk(0..200);
    feed.fill(45, Side::Buy, BASE_BID - 60);
    feed.fill(120, Side::Sell, BASE_BID + 70);
    feed.fill(170, Side::Buy, BASE_BID - 40);
    feed.finish("fills outside the mid range")
}

/// A full window at a 15 second spin, so the candle bodies and wicks are wide enough to read.
fn coarse_buckets() -> ChartScene {
    let mut feed = Feed::with_spin(Some(TICK), COARSE_SPIN);
    feed.walk(0..COARSE_WINDOW_BUCKETS);
    feed.fill(11, Side::Buy, mid_bid(11));
    feed.fill(28, Side::Sell, mid_bid(28) + 2);
    feed.finish("coarse 15 s buckets — readable candles")
}

struct Feed {
    chart: ChartModel,
    spin: DurationUs,
    seq: u64,
    is_gap_pending: bool,
}

impl Feed {
    fn new(tick: Option<Price>) -> Self {
        Self::with_spin(tick, SPIN)
    }

    fn with_spin(tick: Option<Price>, spin: DurationUs) -> Self {
        let mut chart = ChartModel::with_capacity(1, spin);
        chart.configure(&[tick], spin);
        Self {
            chart,
            spin,
            seq: 0,
            is_gap_pending: false,
        }
    }

    fn walk(&mut self, steps: std::ops::Range<i64>) {
        for step in steps {
            self.bucket(step, mid_bid(step));
        }
    }

    fn bucket(&mut self, step: i64, bid: i64) {
        for sub in 0..COMMITS_PER_BUCKET {
            let continuity = self.take_continuity();
            let bid = bid + wobble(step * COMMITS_PER_BUCKET + sub);
            self.commit(
                self.at(step, sub),
                &[unit(bid)],
                &[unit(bid + 2)],
                continuity,
            );
        }
    }

    /// A candle whose open, high, low and close are equal, which the bounds must widen.
    fn still(&mut self, step: i64, bid: i64) {
        let continuity = self.take_continuity();
        self.commit(self.at(step, 0), &[unit(bid)], &[unit(bid + 2)], continuity);
    }

    fn bid_only(&mut self, step: i64) {
        let continuity = self.take_continuity();
        self.commit(self.at(step, 0), &[unit(mid_bid(step))], &[], continuity);
    }

    fn at(&self, step: i64, sub: i64) -> i64 {
        BASE_TS + step * self.spin.micros() + sub * self.spin.micros() / COMMITS_PER_BUCKET
    }

    fn gap(&mut self) {
        self.is_gap_pending = true;
    }

    fn take_continuity(&mut self) -> BookContinuity {
        if std::mem::take(&mut self.is_gap_pending) {
            BookContinuity::GapBefore
        } else {
            BookContinuity::Continuous
        }
    }

    fn commit(&mut self, micros: i64, bids: &[Level], asks: &[Level], continuity: BookContinuity) {
        self.seq += 1;
        self.chart.apply_book(
            &book(self.seq, micros, UiBookState::Valid, bids, asks),
            continuity,
        );
    }

    fn fill(&mut self, step: i64, side: Side, tick: i64) {
        self.chart.apply_event(&fixture::fill(
            self.seq,
            self.at(step, 1),
            side,
            px(tick),
            Qty(FIXED_SCALE),
        ));
    }

    fn finish(self, name: &'static str) -> ChartScene {
        ChartScene {
            name,
            chart: self.chart,
            instrument: INSTRUMENT,
            tick: TICK,
        }
    }
}

fn mid_bid(step: i64) -> i64 {
    BASE_BID + triangle(step, 137, 18) + triangle(step, 23, 5)
}

fn wobble(commit: i64) -> i64 {
    triangle(commit, 7, 3) - 1
}

fn triangle(step: i64, period: i64, amplitude: i64) -> i64 {
    let phase = step.rem_euclid(period);
    let rise = phase.min(period - phase);
    rise * amplitude * 2 / period
}

fn side_for(step: i64) -> Side {
    if step % 180 < 90 { Side::Buy } else { Side::Sell }
}
