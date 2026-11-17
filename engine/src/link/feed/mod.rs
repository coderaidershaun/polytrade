//! The feed topics' bodies. `UiBookSnapshot` and `UiEvent` are encoded directly, so no second shape
//! can drift from the one the engine holds.

mod exec;
mod latency;

use crate::hot::exec::QuoteLevel;
use crate::msg::ids::{Price, Qty, Side};
use crate::msg::inbound::Level;
use crate::msg::time::VenueTs;
use crate::msg::ui::{
    DomQuote, UI_BOOK_LEVELS, UI_ORDER_SNAPSHOT_CAPACITY, UI_ORDER_SNAPSHOT_MAX_TOTAL,
    UI_QUOTE_LEVELS, UiBookSnapshot, UiBookState, UiEvent, UiWorkingOrder,
};

use super::envelope::{ByteReader, ByteWriter, LinkDecodeError};
use super::wire::{
    LayoutDigest, WireField, digest_of, widest, width_of, wire_array, wire_enum, wire_struct,
};

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
    const WIDTH: usize = u8::WIDTH;
    const DIGEST: u64 = LayoutDigest::START
        .text("quote level")
        .number(NO_QUOTE_LEVEL as u64)
        .finish();

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
    Level; UI_BOOK_LEVELS; Level { price: Price::from_mantissa(0), qty: Qty::from_mantissa(0) },
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

/// Every kind shares the `seq`/`event_ts_us` head and pads to the widest kind, so a reader can seek
/// the next frame without knowing the kind. Listing a variant's fields once is what stops the writer
/// and the reader disagreeing about which byte is the side and which is the state. Each probe
/// closure only names a field for its type and is never called.
macro_rules! ui_events {
    ( $( $variant:ident = $tag:literal { $($field:ident $(after $checked:expr)?),* $(,)? } ),+ $(,)? ) => {
        impl WireField for UiEvent {
            const WIDTH: usize = u8::WIDTH + u64::WIDTH + VenueTs::WIDTH + widest(&[
                $( 0 $( + width_of(|event: &UiEvent| match event {
                    UiEvent::$variant { $field, .. } => $field,
                    _ => unreachable!("a width probe is never called"),
                }) )* ),+
            ]);
            const DIGEST: u64 = LayoutDigest::START
                .text("event")
                .number(Self::WIDTH as u64)
                .text("seq")
                .part(u64::WIDTH, u64::DIGEST)
                .text("event_ts_us")
                .part(VenueTs::WIDTH, VenueTs::DIGEST)
                $(
                    .text(stringify!($variant))
                    .number($tag)
                    $(
                        .text(stringify!($field))
                        .part(
                            width_of(|event: &UiEvent| match event {
                                UiEvent::$variant { $field, .. } => $field,
                                _ => unreachable!("a width probe is never called"),
                            }),
                            digest_of(|event: &UiEvent| match event {
                                UiEvent::$variant { $field, .. } => $field,
                                _ => unreachable!("a digest probe is never called"),
                            }),
                        )
                    )*
                )+
                .finish();

            fn write(&self, writer: &mut ByteWriter<'_>) {
                let end = writer.written() + Self::WIDTH;
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
                let event_ts_us = VenueTs::read(reader)?;
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
