//! User-stream decoder. It is account-wide and replays nothing after a disconnect; reads rebuild
//! state and only then resume.
//!
//! Maker fills fold from order UPDATE, whose size_matched is cumulative, so duplicate or reordered
//! frames are harmless. Trade frames are never folded. The same trade id arrives once per
//! settlement step carrying lineage, fee, and progress only.

use crate::adapters::decode::{order_side, price_field, qty_field, unknown_enum};
use crate::adapters::venue_clock::clamp_or_receipt;
use crate::ids::{ClientOrderId, InstrumentId, Price, Qty, TradeId};
use crate::msg::exec::{ExecEvent, ExecKind, Liquidity};
use crate::time::TsUs;

use super::correlation::{trade_id_digest, venue_order_id_digest};
use super::response::blank_event;
use super::wire::{OrderEvent, StreamFrame, TradeRecord};
use super::{DecodeContext, FRAME, WireError, optional_qty, status_with_fill, venue_status};

#[derive(Debug, Clone, PartialEq)]
pub enum StreamEvent {
    Order(ExecEvent),
    Trade(TradeLineage),
    Ignored(IgnoredReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IgnoredReason {
    /// Dropped here because an `ExecEvent` must name an instrument. Mid-run such a frame may not be
    /// safe to discard, so it is counted.
    UntrackedToken,
    /// The frame names an order id this run has not recorded. It must not be discarded, because the
    /// placement answer that records the id may still arrive.
    UnknownOrder,
    UnhandledEvent,
}

/// Failed is terminal. An already-folded fill did not settle on-chain, so the position the
/// engine believes it holds does not exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TradeSettlement {
    Matched,
    MatchedNotBroadcast,
    Mined,
    Confirmed,
    Retrying,
    Failed,
}

impl TradeSettlement {
    pub const fn is_terminal(self) -> bool {
        matches!(self, TradeSettlement::Confirmed | TradeSettlement::Failed)
    }

    /// Whether the venue has put the trade on chain. Only then have the balances behind it actually
    /// moved: a match is an off-chain promise, and the balance endpoint answers pre-fill numbers
    /// until the transfer lands.
    pub const fn is_on_chain(self) -> bool {
        matches!(self, TradeSettlement::Mined | TradeSettlement::Confirmed)
    }
}

/// Carries no quantity, because the fill was already folded from the order UPDATE. Deduplicate on
/// `venue_trade_id`, not on the digest.
#[derive(Debug, Clone, PartialEq)]
pub struct TradeLineage {
    pub instrument: InstrumentId,
    pub trade_id: TradeId,
    pub venue_trade_id: Box<str>,
    pub settlement: TradeSettlement,
    /// Whether the venue attributes the trade to this engine's own credential. Decided from the
    /// payload alone, so it still holds for a settlement step arriving long after the order it
    /// names went terminal and stopped being resolvable.
    pub is_ours: bool,
    /// The record's side is the taker's side whoever we are, so only the role says which side we
    /// played.
    pub role: Option<Liquidity>,
    pub price: Price,
    pub size: Qty,
    /// Our own resting orders that this trade filled, filtered by owner. Empty when we took.
    pub maker_fills: Vec<MakerFill>,
    /// Our order, when we took. The taker_order_id is only ours if trader_side says so.
    pub taker_order: Option<ClientOrderId>,
    /// The venue charges takers only, and publishes a rate rather than an amount. Only the rate
    /// travels; turning it into money here would put a float where an exact amount belongs.
    pub fee_rate_bps: i32,
    pub exchange_ts_us: TsUs,
    pub received_ts_us: TsUs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MakerFill {
    pub client_id: ClientOrderId,
    pub price: Price,
    pub matched: Qty,
    pub fee_rate_bps: i32,
}

/// # Errors
/// Malformed JSON, a decimal out of scale, or an unknown status, side or event spelling.
pub fn decode_stream_frame(
    json: &str,
    context: &DecodeContext<'_>,
) -> Result<StreamEvent, WireError> {
    let frame: StreamFrame = FRAME.decode(json)?;
    match frame {
        StreamFrame::Order(event) => decode_order_event(&event, context),
        StreamFrame::Trade(record) => Ok(match trade_lineage(&record, context)? {
            Some(lineage) => StreamEvent::Trade(lineage),
            None => StreamEvent::Ignored(IgnoredReason::UntrackedToken),
        }),
        StreamFrame::Unhandled => Ok(StreamEvent::Ignored(IgnoredReason::UnhandledEvent)),
    }
}

fn decode_order_event(
    event: &OrderEvent,
    context: &DecodeContext<'_>,
) -> Result<StreamEvent, WireError> {
    let Some(instrument) = context.tokens.instrument(&event.asset_id) else {
        return Ok(StreamEvent::Ignored(IgnoredReason::UntrackedToken));
    };
    let Some(known) = context.orders.resolve(&event.id) else {
        return Ok(StreamEvent::Ignored(IgnoredReason::UnknownOrder));
    };

    let kind = order_event_kind(&event.event)?;
    let qty = qty_field("original_size", &event.original_size)?;
    let filled = optional_qty("size_matched", &event.size_matched)?;
    let price = price_field("price", &event.price)?;

    Ok(StreamEvent::Order(ExecEvent {
        venue_order_id: Some(venue_order_id_digest(&event.id)),
        status: Some(status_with_fill(
            venue_status("status", &event.status)?,
            filled,
            qty,
        )),
        // Only UPDATE reports matched quantity, and it does so as a maker. The taker's own fill is
        // reported by placement instead, so we don't double-count.
        liquidity: matches!(kind, ExecKind::ReportTrade).then_some(Liquidity::Maker),
        side: order_side("side", &event.side)?,
        price,
        qty,
        cumulative_qty: filled,
        // A resting order fills at its own limit, exactly.
        cumulative_quote: price.notional(filled),
        exchange_ts_us: venue_stamp(&event.timestamp, 1, context),
        ..blank_event(kind, instrument, known.client_id, context)
    }))
}

fn order_event_kind(event: &str) -> Result<ExecKind, WireError> {
    Ok(match event {
        "PLACEMENT" => ExecKind::ReportNew,
        "UPDATE" => ExecKind::ReportTrade,
        "CANCELLATION" => ExecKind::ReportCanceled,
        unknown => return Err(unknown_enum("type", unknown).into()),
    })
}

/// Shared by the stream and `GET /data/trades` — the payload is the same shape on both.
///
/// `Ok(None)` for a token no binding names.
pub(super) fn trade_lineage(
    record: &TradeRecord,
    context: &DecodeContext<'_>,
) -> Result<Option<TradeLineage>, WireError> {
    let Some(instrument) = context.tokens.instrument(&record.asset_id) else {
        return Ok(None);
    };
    let role = trader_role(&record.trader_side);

    // Filter maker_orders to only those where owner matches our api key. Taking the whole array
    // would credit the counterparty's fills to us on any trade we took.
    let mut maker_fills = Vec::new();
    let mut has_our_maker_leg = false;
    for maker in &record.maker_orders {
        if maker.owner != context.api_key {
            continue;
        }
        // Kept apart from the fills below because the order id stops resolving once the order goes
        // terminal, and the credential on the leg does not.
        has_our_maker_leg = true;
        let Some(known) = context.orders.resolve(&maker.order_id) else {
            continue;
        };
        maker_fills.push(MakerFill {
            client_id: known.client_id,
            price: price_field("maker price", &maker.price)?,
            matched: qty_field("matched_amount", &maker.matched_amount)?,
            fee_rate_bps: bps(&maker.fee_rate_bps),
        });
    }

    // `taker_order_id` names our order only when we took; on a trade we made, it names the
    // counterparty's order.
    let taker_order = match role {
        Some(Liquidity::Taker) => context
            .orders
            .resolve(&record.taker_order_id)
            .map(|known| known.client_id),
        _ => None,
    };

    Ok(Some(TradeLineage {
        instrument,
        trade_id: trade_id_digest(&record.id),
        venue_trade_id: record.id.clone().into(),
        settlement: settlement(&record.status)?,
        is_ours: is_ours(role, has_our_maker_leg),
        role,
        price: price_field("price", &record.price)?,
        size: qty_field("size", &record.size)?,
        maker_fills,
        taker_order,
        fee_rate_bps: bps(&record.fee_rate_bps),
        // match_time is in seconds here, unlike the order event which stamps milliseconds.
        exchange_ts_us: venue_stamp(&record.match_time, 1_000, context),
        received_ts_us: context.received_ts_us,
    }))
}

/// Whether this trade belongs to the credential this engine signs with.
///
/// Every maker leg names the api key that placed it, so a trade the venue says we MADE with none of
/// ours among those legs is the venue stating it belongs to someone else — the same wallet is
/// reachable from the website under different credentials. Nothing names the credential behind the
/// taker leg, and an unstated role names nothing at all, so neither can be shown to belong to a
/// stranger and both count as ours. That asymmetry is deliberate: the answer decides whether a
/// failed settlement stops the run, and a fill this engine cannot place must stop it rather than be
/// waved through as somebody else's.
fn is_ours(role: Option<Liquidity>, has_our_maker_leg: bool) -> bool {
    match role {
        Some(Liquidity::Maker) => has_our_maker_leg,
        Some(Liquidity::Taker) | None => true,
    }
}

fn trader_role(trader_side: &str) -> Option<Liquidity> {
    match trader_side {
        "MAKER" => Some(Liquidity::Maker),
        "TAKER" => Some(Liquidity::Taker),
        _ => None,
    }
}

/// The REST surface sends bare names where the stream and the TypeScript SDK prefix them.
fn settlement(status: &str) -> Result<TradeSettlement, WireError> {
    Ok(
        match status.strip_prefix("TRADE_STATUS_").unwrap_or(status) {
            "MATCHED" => TradeSettlement::Matched,
            "MATCHED_NOT_BROADCASTED" => TradeSettlement::MatchedNotBroadcast,
            "MINED" => TradeSettlement::Mined,
            "CONFIRMED" => TradeSettlement::Confirmed,
            "RETRYING" => TradeSettlement::Retrying,
            "FAILED" => TradeSettlement::Failed,
            unknown => return Err(unknown_enum("trade status", unknown).into()),
        },
    )
}

/// Fee rate, not money. An absent field reads as zero because the venue omits it on free markets.
fn bps(text: &str) -> i32 {
    text.trim().parse::<i32>().unwrap_or(0)
}

/// The venue stamps in text, and in seconds on one surface where another uses milliseconds. An
/// unreadable stamp falls back to the moment this process received the frame.
fn venue_stamp(text: &str, millis_per_unit: i64, context: &DecodeContext<'_>) -> TsUs {
    let venue_ms = text
        .trim()
        .parse::<i64>()
        .ok()
        .map(|value| value.saturating_mul(millis_per_unit));
    clamp_or_receipt(venue_ms, context.received_ts_us)
}
