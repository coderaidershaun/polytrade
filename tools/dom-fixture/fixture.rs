//! Builders every scene file shares: prices on the one-unit tick grid, quantities in thousandths,
//! book snapshots and maker fills.

use polysim::ids::{AssetId, FIXED_SCALE, InstrumentId, Price, Qty, Side};
use polysim::msg::exec::Liquidity;
use polysim::msg::inbound::Level;
use polysim::msg::ui::{UI_BOOK_LEVELS, UiBookSnapshot, UiBookState, UiEvent};
use polysim::time::TsUs;

/// A realistic epoch: large stamps are what the charts' f64 normalisation before the f32
/// conversion has to survive.
pub const BASE_TS: i64 = 1_753_300_000_000_000;

const MILLI: i64 = FIXED_SCALE / 1000;

pub const INSTRUMENT: InstrumentId = InstrumentId(0);

pub fn px(tick_index: i64) -> Price {
    Price(tick_index * FIXED_SCALE)
}

pub fn qm(milli: i64) -> Qty {
    Qty(milli * MILLI)
}

pub fn level(tick_index: i64, milli: i64) -> Level {
    Level {
        price: px(tick_index),
        qty: qm(milli),
    }
}

/// One whole unit resting at the tick.
pub fn unit(tick_index: i64) -> Level {
    Level {
        price: px(tick_index),
        qty: Qty(FIXED_SCALE),
    }
}

pub fn levels(rows: &[(i64, i64)]) -> Vec<Level> {
    rows.iter()
        .map(|&(tick_index, milli)| level(tick_index, milli))
        .collect()
}

pub fn book(
    seq: u64,
    event_ts_us: i64,
    state: UiBookState,
    bids: &[Level],
    asks: &[Level],
) -> UiBookSnapshot {
    let empty = Level {
        price: Price(0),
        qty: Qty(0),
    };
    let mut bid_levels = [empty; UI_BOOK_LEVELS];
    let mut ask_levels = [empty; UI_BOOK_LEVELS];
    for (slot, level) in bid_levels.iter_mut().zip(bids) {
        *slot = *level;
    }
    for (slot, level) in ask_levels.iter_mut().zip(asks) {
        *slot = *level;
    }
    UiBookSnapshot {
        instrument: INSTRUMENT,
        seq,
        event_ts_us: TsUs::from_micros(event_ts_us),
        state,
        bid_len: bids.len().min(UI_BOOK_LEVELS) as u16,
        ask_len: asks.len().min(UI_BOOK_LEVELS) as u16,
        bids: bid_levels,
        asks: ask_levels,
    }
}

/// A maker fill with no commission, which is all the chart and position scenes need.
pub fn fill(seq: u64, event_ts_us: i64, side: Side, price: Price, qty: Qty) -> UiEvent {
    UiEvent::Fill {
        instrument: INSTRUMENT,
        seq,
        event_ts_us: TsUs::from_micros(event_ts_us),
        side,
        price,
        qty,
        commission: 0,
        commission_asset: AssetId(1),
        liquidity: Some(Liquidity::Maker),
        quote_level: None,
    }
}
