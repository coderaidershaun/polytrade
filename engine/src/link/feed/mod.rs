//! The feed topics' bodies. `UiBookSnapshot` and `UiEvent` are encoded directly, so no second shape
//! can drift from the one the engine holds.

mod exec;
mod latency;

use crate::hot::exec::QuoteLevel;
use crate::ids::{Price, Qty, Side};
use crate::msg::inbound::Level;
use crate::msg::ui::{
    DomQuote, UI_BOOK_LEVELS, UI_ORDER_SNAPSHOT_CAPACITY, UI_ORDER_SNAPSHOT_MAX_TOTAL,
    UI_QUOTE_LEVELS, UiBookSnapshot, UiBookState, UiEvent, UiWorkingOrder,
};
use crate::time::TsUs;

use super::envelope::{ByteReader, ByteWriter, ENVELOPE_LEN, LINK_MAX_DATAGRAM, LinkDecodeError};
use super::wire::{OPTIONAL_LEVEL_LEN, WireField, wire_array, wire_enum, wire_struct};
use exec::{HALT_LEN, REJECT_ORIGIN_LEN};
use latency::LATENCY_LEN;

const LEVEL_LEN: usize = 16;

pub(super) const BOOK_BODY_LEN: usize = 2 + 8 + 8 + 1 + 2 + 2 + 2 * UI_BOOK_LEVELS * LEVEL_LEN;
pub(super) const EVENT_BODY_LEN: usize = 1 + 8 + 8 + EVENT_TAIL_LEN;

const QUOTE_LEN: usize = 2 + 2 * UI_QUOTE_LEVELS * OPTIONAL_LEVEL_LEN;
const ORDER_UPDATE_LEN: usize = 2 + 8 + 1 + 1 + 1 + 8 + 8 + 8;
const ORDER_SNAPSHOT_CELL_LEN: usize = 8 + 1 + 1 + 8 + 8 + 8;
const ORDER_SNAPSHOT_LEN: usize =
    2 + 1 + 1 + 2 + UI_ORDER_SNAPSHOT_CAPACITY * ORDER_SNAPSHOT_CELL_LEN;
const EVENT_TAIL_LEN: usize = ORDER_SNAPSHOT_LEN;
const FILL_LEN: usize = 2 + 1 + 1 + 8 + 8 + 8 + 2 + 1;
const BALANCE_LEN: usize = 2 + 8 + 8;
const REJECT_LEN: usize = 2 + 1 + REJECT_ORIGIN_LEN;
const EXECUTION_LEN: usize = HALT_LEN;
const POSITION_LEN: usize = 2 + 8 + 8;

// UI_BOOK_LEVELS bump -> compiles fail (not runtime fragmentation). Atomic snapshot width; new field -> tail overrun fails.
const _: () = assert!(ENVELOPE_LEN + BOOK_BODY_LEN <= LINK_MAX_DATAGRAM);
const _: () = assert!(ENVELOPE_LEN + EVENT_BODY_LEN <= LINK_MAX_DATAGRAM);
const _: () = assert!(QUOTE_LEN <= EVENT_TAIL_LEN);
const _: () = assert!(ORDER_UPDATE_LEN <= EVENT_TAIL_LEN);
const _: () = assert!(ORDER_SNAPSHOT_LEN <= EVENT_TAIL_LEN);
const _: () = assert!(FILL_LEN <= EVENT_TAIL_LEN);
const _: () = assert!(BALANCE_LEN <= EVENT_TAIL_LEN);
const _: () = assert!(REJECT_LEN <= EVENT_TAIL_LEN);
const _: () = assert!(EXECUTION_LEN <= EVENT_TAIL_LEN);
const _: () = assert!(POSITION_LEN <= EVENT_TAIL_LEN);
const _: () = assert!(LATENCY_LEN <= EVENT_TAIL_LEN);

// Discriminants 1-based: all-zero invalid.
wire_enum! {
    Side, "side";
    (Side::Buy) = 1,
    (Side::Sell) = 2,
}

wire_enum! {
    UiBookState, "book state";
    (UiBookState::AwaitingSnapshot) = 1,
    (UiBookState::Valid) = 2,
}

const NO_QUOTE_LEVEL: u8 = u8::MAX;

impl WireField for Option<QuoteLevel> {
    fn write(&self, writer: &mut ByteWriter<'_>) {
        self.map_or(NO_QUOTE_LEVEL, QuoteLevel::get).write(writer);
    }

    fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
        let value = u8::read(reader)?;
        if value == NO_QUOTE_LEVEL {
            return Ok(None);
        }
        QuoteLevel::new(value)
            .map(Some)
            .ok_or_else(|| LinkDecodeError::unknown("quote level", value))
    }
}

wire_struct! {
    Level { price, qty }
}

wire_struct! {
    UiWorkingOrder {
        client_id,
        quote_level,
        state,
        price,
        qty,
        filled,
    }
}

wire_array! {
    Level; UI_BOOK_LEVELS; Level { price: Price(0), qty: Qty(0) },
    UiWorkingOrder; UI_ORDER_SNAPSHOT_CAPACITY; UiWorkingOrder::EMPTY,
    Option<(Price, Qty)>; UI_QUOTE_LEVELS; None,
}

wire_struct! {
    DomQuote { bids, asks }
}

wire_struct! {
    UiBookSnapshot {
        instrument,
        seq,
        event_ts_us,
        state,
        bid_len after validate_book_len(bid_len)?,
        ask_len after validate_book_len(ask_len)?,
        bids,
        asks,
    }
}

/// A level count past capacity would index out of bounds and let a remote peer crash the reader, so
/// it is refused here once rather than at every reader.
fn validate_book_len(count: u16) -> Result<(), LinkDecodeError> {
    if usize::from(count) <= UI_BOOK_LEVELS {
        return Ok(());
    }
    Err(LinkDecodeError::BookLevelsExceeded {
        count,
        capacity: UI_BOOK_LEVELS,
    })
}

/// Every kind shares the `seq`/`event_ts_us` head and pads to one width, so a reader can seek the
/// next frame without knowing the kind. Listing a variant's fields once is what stops the writer and
/// the reader disagreeing about which byte is the side and which is the state.
macro_rules! ui_events {
    ( $( $variant:ident = $tag:literal { $($field:ident $(after $checked:expr)?),* $(,)? } ),+ $(,)? ) => {
        impl WireField for UiEvent {
            fn write(&self, writer: &mut ByteWriter<'_>) {
                let end = writer.written() + EVENT_BODY_LEN;
                match self {
                    $( UiEvent::$variant { seq, event_ts_us, $($field),* } => {
                        let kind: u8 = $tag;
                        kind.write(writer);
                        seq.write(writer);
                        event_ts_us.write(writer);
                        $( $field.write(writer); )*
                    } )+
                }
                writer.pad_to(end);
            }

            fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
                let kind = u8::read(reader)?;
                let seq = u64::read(reader)?;
                let event_ts_us = TsUs::read(reader)?;
                match kind {
                    $( $tag => {
                        $(
                            let $field = WireField::read(reader)?;
                            $( $checked; )?
                        )*
                        Ok(UiEvent::$variant { seq, event_ts_us, $($field),* })
                    } )+
                    _ => Err(LinkDecodeError::unknown("event kind", kind)),
                }
            }
        }
    };
}

ui_events! {
    Quote = 1 { instrument, quote },
    Trade = 2 { instrument, aggressor, price, qty },
    OrderUpdate = 3 {
        instrument,
        client_id,
        quote_level,
        state,
        side,
        price,
        qty,
        filled,
    },
    Feature = 4 { instrument, feature, value },
    Fill = 5 {
        instrument,
        quote_level,
        side,
        price,
        qty,
        commission,
        commission_asset,
        liquidity,
    },
    Rotation = 6 { instrument },
    Position = 7 { instrument, exposure_quote, pnl_quote },
    Balance = 8 { asset, free, locked },
    Reject = 9 { instrument, side, origin },
    Execution = 10 { halt },
    OrderSnapshot = 11 {
        instrument,
        side,
        detail_len,
        total_working after validate_order_snapshot_counts(detail_len, total_working)?,
        orders after validate_order_snapshot_detail(detail_len, &orders)?,
    },
    Latency = 12 { summary },
}

fn validate_order_snapshot_counts(
    detail_len: u8,
    total_working: u16,
) -> Result<(), LinkDecodeError> {
    let valid = usize::from(detail_len) <= UI_ORDER_SNAPSHOT_CAPACITY
        && u16::from(detail_len) <= total_working
        && total_working <= UI_ORDER_SNAPSHOT_MAX_TOTAL;
    if valid {
        return Ok(());
    }
    Err(LinkDecodeError::OrderSnapshotCountsInvalid {
        detail_len,
        total_working,
        detail_capacity: UI_ORDER_SNAPSHOT_CAPACITY,
        total_capacity: UI_ORDER_SNAPSHOT_MAX_TOTAL,
    })
}

fn validate_order_snapshot_detail(
    detail_len: u8,
    orders: &[UiWorkingOrder; UI_ORDER_SNAPSHOT_CAPACITY],
) -> Result<(), LinkDecodeError> {
    let detail = &orders[..usize::from(detail_len)];
    for order in detail {
        if !order.state.is_working() {
            return Err(LinkDecodeError::OrderSnapshotTerminalState { state: order.state });
        }
    }
    for (index, order) in detail.iter().enumerate() {
        if detail[..index]
            .iter()
            .any(|held| held.client_id == order.client_id)
        {
            return Err(LinkDecodeError::OrderSnapshotDuplicate {
                client_id: order.client_id,
            });
        }
    }
    Ok(())
}
