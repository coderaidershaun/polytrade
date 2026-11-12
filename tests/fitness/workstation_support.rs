//! Fixtures shared by the workstation projection fitness files: the book-snapshot builder, the DOM
//! overlay, the named-case table alias and the chart-model fixtures.

use polysim::ids::{InstrumentId, Price, Qty};
use polysim::msg::inbound::Level;
use polysim::msg::ui::{DomQuote, UI_BOOK_LEVELS, UiBookSnapshot, UiBookState, UiEvent};
use polysim::time::{DurationUs, TsUs};
use polysim::workstation::chart::model::{BookContinuity, ChartModel};
use polysim::workstation::dom_view::DomOverlay;

pub type NamedCase = (&'static str, fn(&str));

/// One-mantissa tick, so a price mantissa reads directly as its tick index and a mid reads as the
/// plain sum of the two best indices (half-ticks).
pub const TICK: Price = Price(1);

/// The shipped one-second spin, so the five-minute window is exactly 300 buckets and a bucket index
/// reads as its second.
pub const SPIN: DurationUs = DurationUs::from_micros(1_000_000);

/// The window at [`SPIN`]: `300_000_000 / 1_000_000`.
pub const CAPACITY: u64 = 300;

pub struct BookFixture<'a> {
    pub instrument: u16,
    pub seq: u64,
    pub event_ts: i64,
    pub state: UiBookState,
    pub bids: &'a [(i64, i64)],
    pub asks: &'a [(i64, i64)],
}

pub fn book_snapshot(fixture: BookFixture) -> UiBookSnapshot {
    let empty = Level {
        price: Price(0),
        qty: Qty(0),
    };
    let mut bid_levels = [empty; UI_BOOK_LEVELS];
    let mut ask_levels = [empty; UI_BOOK_LEVELS];
    for (slot, &(price, qty)) in bid_levels.iter_mut().zip(fixture.bids) {
        *slot = Level {
            price: Price(price),
            qty: Qty(qty),
        };
    }
    for (slot, &(price, qty)) in ask_levels.iter_mut().zip(fixture.asks) {
        *slot = Level {
            price: Price(price),
            qty: Qty(qty),
        };
    }
    UiBookSnapshot {
        instrument: InstrumentId(fixture.instrument),
        seq: fixture.seq,
        event_ts_us: TsUs::from_micros(fixture.event_ts),
        state: fixture.state,
        bid_len: fixture.bids.len() as u16,
        ask_len: fixture.asks.len() as u16,
        bids: bid_levels,
        asks: ask_levels,
    }
}

/// A ladder overlay carrying only the desired quote — the shape every DOM case drives, since real
/// working orders are pinned separately.
pub fn overlay(desired: DomQuote) -> DomOverlay<'static> {
    DomOverlay {
        desired: Some(desired),
        bid_orders: &[],
        ask_orders: &[],
    }
}

pub fn at(bucket: u64) -> TsUs {
    TsUs::from_micros(bucket as i64 * SPIN.micros())
}

pub fn chart_with_ticks(ticks: &[Option<Price>]) -> ChartModel {
    let mut chart = ChartModel::with_capacity(ticks.len(), SPIN);
    chart.configure(ticks, SPIN);
    chart
}

pub fn chart_model(instruments: usize) -> ChartModel {
    chart_with_ticks(&vec![Some(TICK); instruments])
}

pub fn chart_book(
    instrument: u16,
    bucket: u64,
    state: UiBookState,
    bids: &[i64],
    asks: &[i64],
) -> UiBookSnapshot {
    let unit_qty =
        |prices: &[i64]| -> Vec<(i64, i64)> { prices.iter().map(|&price| (price, 1)).collect() };
    book_snapshot(BookFixture {
        instrument,
        seq: 0,
        event_ts: at(bucket).micros(),
        state,
        bids: &unit_qty(bids),
        asks: &unit_qty(asks),
    })
}

pub fn valid_book(instrument: u16, bucket: u64, bid: i64, ask: i64) -> UiBookSnapshot {
    chart_book(instrument, bucket, UiBookState::Valid, &[bid], &[ask])
}

/// Fold one two-sided commit stamped at an arbitrary `event_ts_us` with no snapshot loss before it.
pub fn commit_at(chart: &mut ChartModel, instrument: u16, event_ts_us: TsUs) {
    let snapshot = UiBookSnapshot {
        event_ts_us,
        ..valid_book(instrument, 0, 100, 102)
    };
    chart.apply_book(&snapshot, BookContinuity::Continuous);
}

pub fn rotation_event(instrument: u16, bucket: u64) -> UiEvent {
    UiEvent::Rotation {
        instrument: InstrumentId(instrument),
        seq: 0,
        event_ts_us: at(bucket),
    }
}
