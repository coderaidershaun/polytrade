//! The Polymarket quarantine boundary: venue JSON dies here, leaving via `i64@1e-8` mantissas
//! (overflow fatal). Structs stay permissive (the venue may append fields). The venue delivers each
//! book side best-last (inverse of Binance); `parse_book` normalises to engine best-first.

use serde::Deserialize;
use serde_json::Value;

use crate::adapters::decode::{DecimalFault, JsonFrame, levels, price_field, qty_field};
use crate::adapters::venue_clock::clamp_exchange_ts;
use crate::ids::{Price, Qty, Side};
use crate::msg::inbound::Level;
use crate::time::TsUs;

const FRAME: JsonFrame = JsonFrame("polymarket");

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolyFrame {
    Book(PolyBook),
    PriceChange(PolyPriceChange),
    Trade(PolyTrade),
    TickSizeChange(PolyTickSizeChange),
    Batch(Vec<PolyFrame>),
    Ignored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyBook {
    pub asset_id: Box<str>,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyPriceChange {
    pub changes: Vec<PolyDelta>,
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyDelta {
    pub asset_id: Box<str>,
    pub side: Side,
    pub level: Level,
    pub best_bid: Option<Price>,
    pub best_ask: Option<Price>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyTrade {
    pub asset_id: Box<str>,
    pub price: Price,
    pub qty: Qty,
    pub side: Side,
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyTickSizeChange {
    pub asset_id: Option<Box<str>>,
    pub old_tick_size: Option<Box<str>>,
    pub new_tick_size: Option<Box<str>>,
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

/// Text this socket carries that holds no frame: the venue's answer to our keepalive, and the blank
/// filler it sends between real frames. Both are routine traffic, so handing either to the parser
/// would book the venue's own housekeeping as frame loss and make the dropped-frame count useless.
pub fn is_frameless(text: &str) -> bool {
    let trimmed = text.trim();
    trimmed.is_empty() || trimmed == super::ws::PONG
}

/// Text that holds no frame is answered by [`is_frameless`] before it reaches here, so every
/// unreadable text that does reach here is frame loss and says so.
///
/// # Errors
/// [`DecimalFault::MantissaOverflow`] is fatal; others are counted and dropped.
pub fn parse_market_frame(text: &str, received_ts_us: TsUs) -> Result<PolyFrame, ParseError> {
    parse_market_value(FRAME.decode(text)?, received_ts_us)
}

fn parse_market_value(value: Value, received_ts_us: TsUs) -> Result<PolyFrame, ParseError> {
    let Value::Array(elements) = value else {
        return frame_from(FRAME.decode_value(value)?, received_ts_us);
    };
    let mut frames = Vec::with_capacity(elements.len());
    for element in elements {
        frames.push(parse_market_value(element, received_ts_us)?);
    }
    Ok(PolyFrame::Batch(frames))
}

fn frame_from(raw: RawFrame, received_ts_us: TsUs) -> Result<PolyFrame, ParseError> {
    Ok(match raw {
        RawFrame::Book(book) => PolyFrame::Book(parse_book(&book, received_ts_us)?),
        RawFrame::PriceChange(change) => {
            PolyFrame::PriceChange(parse_price_change(&change, received_ts_us)?)
        }
        RawFrame::Trade(trade) => PolyFrame::Trade(parse_trade(&trade, received_ts_us)?),
        RawFrame::TickSizeChange(change) => {
            PolyFrame::TickSizeChange(parse_tick_size(change, received_ts_us)?)
        }
        RawFrame::Unhandled => PolyFrame::Ignored,
    })
}

fn parse_book(raw: &RawBook, received_ts_us: TsUs) -> Result<PolyBook, ParseError> {
    let side = |entries: &[RawLevel]| {
        let texts = entries
            .iter()
            .map(|entry| (entry.price.as_str(), entry.size.as_str()));
        levels("book price", "book size", texts)
    };
    let mut bids = side(&raw.bids)?;
    let mut asks = side(&raw.asks)?;
    // Venue delivers best=last (inverse of Binance); sort to engine best-first.
    bids.sort_unstable_by(|a, b| b.price.cmp(&a.price));
    asks.sort_unstable_by(|a, b| a.price.cmp(&b.price));
    Ok(PolyBook {
        asset_id: raw.asset_id.as_str().into(),
        bids,
        asks,
        exchange_ts_us: parse_stamp(&raw.timestamp, received_ts_us)?,
        received_ts_us,
    })
}

fn parse_price_change(
    raw: &RawPriceChange,
    received_ts_us: TsUs,
) -> Result<PolyPriceChange, ParseError> {
    let exchange_ts_us = parse_stamp(&raw.timestamp, received_ts_us)?;
    let mut changes = Vec::with_capacity(raw.price_changes.len());
    for element in &raw.price_changes {
        changes.push(PolyDelta {
            asset_id: element.asset_id.as_str().into(),
            side: parse_side(&element.side)?,
            level: Level {
                price: price_field("price", &element.price)?,
                qty: qty_field("size", &element.size)?,
            },
            best_bid: optional_price("best_bid", element.best_bid.as_deref())?,
            best_ask: optional_price("best_ask", element.best_ask.as_deref())?,
        });
    }
    Ok(PolyPriceChange {
        changes,
        exchange_ts_us,
        received_ts_us,
    })
}

fn parse_trade(raw: &RawTrade, received_ts_us: TsUs) -> Result<PolyTrade, ParseError> {
    Ok(PolyTrade {
        asset_id: raw.asset_id.as_str().into(),
        price: price_field("price", &raw.price)?,
        qty: qty_field("size", &raw.size)?,
        side: parse_side(&raw.side)?,
        exchange_ts_us: parse_stamp(&raw.timestamp, received_ts_us)?,
        received_ts_us,
    })
}

fn parse_tick_size(
    raw: RawTickSize,
    received_ts_us: TsUs,
) -> Result<PolyTickSizeChange, ParseError> {
    Ok(PolyTickSizeChange {
        asset_id: raw.asset_id.map(Into::into),
        old_tick_size: raw.old_tick_size.map(Into::into),
        new_tick_size: raw.new_tick_size.map(Into::into),
        exchange_ts_us: raw
            .timestamp
            .as_deref()
            .map_or(Ok(received_ts_us), |ts| parse_stamp(ts, received_ts_us))?,
        received_ts_us,
    })
}

fn parse_stamp(s: &str, now: TsUs) -> Result<TsUs, DecimalFault> {
    let venue_ms: i64 = s.parse().map_err(|_| DecimalFault::Decimal {
        field: "timestamp",
        value: s.into(),
        reason: "non-integer millisecond timestamp",
    })?;
    Ok(clamp_exchange_ts(venue_ms, now))
}

fn parse_side(s: &str) -> Result<Side, ParseError> {
    match s {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        _ => Err(ParseError::Side { value: s.into() }),
    }
}

fn optional_price(field: &'static str, s: Option<&str>) -> Result<Option<Price>, DecimalFault> {
    s.map(|value| price_field(field, value)).transpose()
}

/// A shape the engine does not handle is `Unhandled` rather than an error: the venue is free to add
/// event types, and a known-shape frame that is not ours is not frame loss.
#[derive(Deserialize)]
#[serde(tag = "event_type")]
enum RawFrame {
    #[serde(rename = "book")]
    Book(RawBook),
    #[serde(rename = "price_change")]
    PriceChange(RawPriceChange),
    #[serde(rename = "last_trade_price")]
    Trade(RawTrade),
    #[serde(rename = "tick_size_change")]
    TickSizeChange(RawTickSize),
    #[serde(other)]
    Unhandled,
}

#[derive(Deserialize)]
struct RawBook {
    asset_id: String,
    timestamp: String,
    #[serde(default)]
    bids: Vec<RawLevel>,
    #[serde(default)]
    asks: Vec<RawLevel>,
}

#[derive(Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct RawPriceChange {
    timestamp: String,
    #[serde(default)]
    price_changes: Vec<RawDelta>,
}

#[derive(Deserialize)]
struct RawDelta {
    asset_id: String,
    price: String,
    size: String,
    side: String,
    best_bid: Option<String>,
    best_ask: Option<String>,
}

#[derive(Deserialize)]
struct RawTrade {
    asset_id: String,
    price: String,
    size: String,
    side: String,
    timestamp: String,
}

#[derive(Deserialize)]
struct RawTickSize {
    asset_id: Option<String>,
    old_tick_size: Option<String>,
    new_tick_size: Option<String>,
    timestamp: Option<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error(transparent)]
    Decode(#[from] DecimalFault),
    #[error("unknown trade/level side {value:?}, expected BUY or SELL")]
    Side { value: Box<str> },
}

impl ParseError {
    pub fn is_fatal(&self) -> bool {
        matches!(self, ParseError::Decode(fault) if fault.is_fatal())
    }
}
