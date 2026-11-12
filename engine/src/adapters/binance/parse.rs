//! Binance quarantine boundary: JSON -> exact ≤8-dp i64 mantissas (overflow fatal). Stamps
//! window-clamped. Structs permissive (Binance appends fields).

use serde::Deserialize;

use crate::adapters::decode::{
    DecimalFault, JsonFrame, levels, mantissa_field, price_field, qty_field,
};
use crate::adapters::venue_clock::{boundary_ts, clamp_exchange_ts};
use crate::config::KlineInterval;
use crate::ids::{AggregateTradeId, InstrumentId, RawTradeId, Side};
use crate::msg::inbound::{KlineEvent, Level, TradeEvent};
use crate::time::TsUs;

const FRAME: JsonFrame = JsonFrame("binance");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseContext {
    pub instrument: InstrumentId,
    pub received_ts_us: TsUs,
}

/// Depth delta, seq ids (spot U==prev_u+1, perp pu==prev_u), absolute levels, qty==0=removal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthDiff {
    pub instrument: InstrumentId,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub prev_final_update_id: Option<u64>,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthSnapshot {
    pub instrument: InstrumentId,
    pub last_update_id: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub received_ts_us: TsUs,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CombinedFrame {
    pub stream: String,
    pub data: String,
}

/// A normalized trade plus its aggregate and raw sequence identities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggTradeEvent {
    pub trade: TradeEvent,
    pub aggregate_id: AggregateTradeId,
    pub first_trade_id: RawTradeId,
    pub last_trade_id: RawTradeId,
}

/// # Errors
/// [`ParseError`] on malformed data, invalid decimals, or invalid trade identity.
pub fn parse_agg_trade(json: &str, ctx: ParseContext) -> Result<AggTradeEvent, ParseError> {
    let raw: AggTrade = FRAME.decode(json)?;
    let aggregate_id = trade_identity("a", raw.aggregate_id)?;
    let first_trade_id = trade_identity("f", raw.first_trade_id)?;
    let last_trade_id = trade_identity("l", raw.last_trade_id)?;
    if first_trade_id > last_trade_id {
        return Err(ParseError::InvertedTradeRange {
            first_trade_id,
            last_trade_id,
        });
    }
    Ok(AggTradeEvent {
        trade: TradeEvent {
            instrument: ctx.instrument,
            price: price_field("price", &raw.price)?,
            qty: qty_field("qty", &raw.qty)?,
            side: taker_side(raw.buyer_is_maker),
            exchange_ts_us: clamp_exchange_ts(raw.trade_ts_ms, ctx.received_ts_us),
            exchange_sent_ts_us: raw
                .event_ts_ms
                .map(|ms| clamp_exchange_ts(ms, ctx.received_ts_us)),
            received_ts_us: ctx.received_ts_us,
            queued_ts_us: ctx.received_ts_us,
        },
        aggregate_id: AggregateTradeId(aggregate_id),
        first_trade_id: RawTradeId(first_trade_id),
        last_trade_id: RawTradeId(last_trade_id),
    })
}

fn trade_identity(field: &'static str, value: Option<u64>) -> Result<u64, ParseError> {
    value.ok_or(ParseError::MissingTradeIdentity { field })
}

/// # Errors
/// As [`parse_agg_trade`]; volume fields can legitimately trip `MantissaOverflow` on SHIB-class candles.
pub fn parse_kline(json: &str, ctx: ParseContext) -> Result<KlineEvent, ParseError> {
    let raw: KlineFrame = FRAME.decode(json)?;
    let k = raw.kline;
    let event_ts_us = clamp_exchange_ts(raw.event_ts_ms, ctx.received_ts_us);
    Ok(KlineEvent {
        instrument: ctx.instrument,
        interval: k.interval,
        open_ts_us: boundary_ts(k.open_ts_ms),
        open: price_field("open", &k.open)?,
        high: price_field("high", &k.high)?,
        low: price_field("low", &k.low)?,
        close: price_field("close", &k.close)?,
        base_volume: qty_field("base_volume", &k.base_volume)?,
        quote_volume: mantissa_field("quote_volume", &k.quote_volume)?,
        trade_count: k.trade_count,
        is_closed: k.is_closed,
        exchange_ts_us: event_ts_us,
        exchange_sent_ts_us: Some(event_ts_us),
        received_ts_us: ctx.received_ts_us,
        queued_ts_us: ctx.received_ts_us,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestKlineTail {
    OpenCandleForming,
    AllClosed,
}

/// Ascending klines array. Closed-ness structural per RestKlineTail.
pub fn parse_rest_klines(
    json: &str,
    ctx: ParseContext,
    interval: KlineInterval,
    tail: RestKlineTail,
) -> Result<Vec<KlineEvent>, ParseError> {
    let rows: Vec<RestKlineRow> = FRAME.decode(json)?;
    let last_index = rows.len().saturating_sub(1);
    let mut events = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let is_closed = match tail {
            RestKlineTail::AllClosed => true,
            RestKlineTail::OpenCandleForming => index != last_index,
        };
        events.push(KlineEvent {
            instrument: ctx.instrument,
            interval,
            open_ts_us: boundary_ts(row.open_ts_ms),
            open: price_field("open", &row.open)?,
            high: price_field("high", &row.high)?,
            low: price_field("low", &row.low)?,
            close: price_field("close", &row.close)?,
            base_volume: qty_field("base_volume", &row.base_volume)?,
            quote_volume: mantissa_field("quote_volume", &row.quote_volume)?,
            trade_count: row.trade_count,
            is_closed,
            exchange_ts_us: clamp_exchange_ts(row.close_ts_ms, ctx.received_ts_us),
            exchange_sent_ts_us: None,
            received_ts_us: ctx.received_ts_us,
            queued_ts_us: ctx.received_ts_us,
        });
    }
    Ok(events)
}

/// # Errors
/// As [`parse_agg_trade`], over every bid/ask level.
pub fn parse_spot_depth_diff(json: &str, ctx: ParseContext) -> Result<DepthDiff, ParseError> {
    let raw: SpotDepthUpdate = FRAME.decode(json)?;
    Ok(DepthDiff {
        instrument: ctx.instrument,
        first_update_id: raw.first_update_id,
        final_update_id: raw.final_update_id,
        prev_final_update_id: None,
        bids: levels("bid price", "bid qty", raw.bids.iter().map(price_and_qty))?,
        asks: levels("ask price", "ask qty", raw.asks.iter().map(price_and_qty))?,
        exchange_ts_us: clamp_exchange_ts(raw.event_ts_ms, ctx.received_ts_us),
        received_ts_us: ctx.received_ts_us,
    })
}

/// Perp `depthUpdate`: `exchange_ts_us` uses `T`.
///
/// # Errors
/// As [`parse_spot_depth_diff`].
pub fn parse_perp_depth_diff(json: &str, ctx: ParseContext) -> Result<DepthDiff, ParseError> {
    let raw: PerpDepthUpdate = FRAME.decode(json)?;
    Ok(DepthDiff {
        instrument: ctx.instrument,
        first_update_id: raw.first_update_id,
        final_update_id: raw.final_update_id,
        prev_final_update_id: Some(raw.prev_final_update_id),
        bids: levels("bid price", "bid qty", raw.bids.iter().map(price_and_qty))?,
        asks: levels("ask price", "ask qty", raw.asks.iter().map(price_and_qty))?,
        exchange_ts_us: clamp_exchange_ts(raw.transaction_ts_ms, ctx.received_ts_us),
        received_ts_us: ctx.received_ts_us,
    })
}

/// # Errors
/// As [`parse_spot_depth_diff`].
pub fn parse_depth_snapshot(json: &str, ctx: ParseContext) -> Result<DepthSnapshot, ParseError> {
    let raw: RestDepthSnapshot = FRAME.decode(json)?;
    Ok(DepthSnapshot {
        instrument: ctx.instrument,
        last_update_id: raw.last_update_id,
        bids: levels("bid price", "bid qty", raw.bids.iter().map(price_and_qty))?,
        asks: levels("ask price", "ask qty", raw.asks.iter().map(price_and_qty))?,
        received_ts_us: ctx.received_ts_us,
    })
}

/// `data` stays JSON text so the actor can feed a typed parser after resolving the instrument.
///
/// # Errors
/// [`DecimalFault::Json`] on an invalid envelope.
pub fn parse_combined_frame(json: &str) -> Result<CombinedFrame, ParseError> {
    let raw: RawEnvelope = FRAME.decode(json)?;
    let data = serde_json::to_string(&raw.data).map_err(|source| FRAME.fault(source))?;
    Ok(CombinedFrame {
        stream: raw.stream,
        data,
    })
}

fn taker_side(buyer_is_maker: bool) -> Side {
    if buyer_is_maker { Side::Sell } else { Side::Buy }
}

fn price_and_qty([price, qty]: &[String; 2]) -> (&str, &str) {
    (price, qty)
}

#[derive(Deserialize)]
struct AggTrade {
    #[serde(rename = "a")]
    aggregate_id: Option<u64>,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    qty: String,
    #[serde(rename = "f")]
    first_trade_id: Option<u64>,
    #[serde(rename = "l")]
    last_trade_id: Option<u64>,
    #[serde(rename = "E")]
    event_ts_ms: Option<i64>,
    #[serde(rename = "T")]
    trade_ts_ms: i64,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

#[derive(Deserialize)]
struct KlineFrame {
    #[serde(rename = "E")]
    event_ts_ms: i64,
    #[serde(rename = "k")]
    kline: KlineData,
}

#[derive(Deserialize)]
struct KlineData {
    #[serde(rename = "t")]
    open_ts_ms: i64,
    #[serde(rename = "i")]
    interval: KlineInterval,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "v")]
    base_volume: String,
    #[serde(rename = "q")]
    quote_volume: String,
    #[serde(rename = "n")]
    trade_count: u32,
    #[serde(rename = "x")]
    is_closed: bool,
}

struct RestKlineRow {
    open_ts_ms: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    base_volume: String,
    close_ts_ms: i64,
    quote_volume: String,
    trade_count: u32,
}

impl<'de> Deserialize<'de> for RestKlineRow {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RowVisitor;

        impl<'de> serde::de::Visitor<'de> for RowVisitor {
            type Value = RestKlineRow;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a binance kline row array")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<RestKlineRow, A::Error> {
                use serde::de::Error as _;
                macro_rules! field {
                    ($index:expr) => {
                        seq.next_element()?
                            .ok_or_else(|| A::Error::invalid_length($index, &self))?
                    };
                }
                let row = RestKlineRow {
                    open_ts_ms: field!(0),
                    open: field!(1),
                    high: field!(2),
                    low: field!(3),
                    close: field!(4),
                    base_volume: field!(5),
                    close_ts_ms: field!(6),
                    quote_volume: field!(7),
                    trade_count: field!(8),
                };
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(row)
            }
        }

        deserializer.deserialize_seq(RowVisitor)
    }
}

#[derive(Deserialize)]
struct SpotDepthUpdate {
    #[serde(rename = "E")]
    event_ts_ms: i64,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[derive(Deserialize)]
struct PerpDepthUpdate {
    #[serde(rename = "T")]
    transaction_ts_ms: i64,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "pu")]
    prev_final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[derive(Deserialize)]
struct RestDepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Deserialize)]
struct RawEnvelope {
    stream: String,
    data: serde_json::Value,
}

/// Only a [`DecimalFault`] can be fatal (escalate, don't truncate). Rest = counted + dropped.
#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error(transparent)]
    Decode(#[from] DecimalFault),
    #[error("aggregate trade frame missing identity field {field:?}")]
    MissingTradeIdentity { field: &'static str },
    #[error("inverted aggregate trade range: first {first_trade_id} > last {last_trade_id}")]
    InvertedTradeRange {
        first_trade_id: u64,
        last_trade_id: u64,
    },
}

impl ParseError {
    pub fn is_fatal(&self) -> bool {
        matches!(self, ParseError::Decode(fault) if fault.is_fatal())
    }
}
