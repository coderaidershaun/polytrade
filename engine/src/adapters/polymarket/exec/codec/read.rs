//! Turns read answers into events: orders, trades, balances and market metadata, everything the
//! driver asks for rather than causes.
//!
//! The rebuild after a disconnect happens HERE and only here. This venue's user stream replays
//! nothing it dropped, so `GET /data/orders` and `GET /data/trades` are the state of record and the
//! stream may only resume once they have landed.

use crate::adapters::decode::{order_side, price_field, qty_field, unknown_enum};
use crate::adapters::exec::{ChunkStamps, account_chunks};
use crate::adapters::venue_clock::clamp_exchange_ts;
use crate::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use crate::msg::exec::{
    AccountChunk, AccountChunkKind, AssetBalance, ExecEvent, ExecKind, VenueOrderStatus,
};
use crate::time::TsUs;

use super::correlation::venue_order_id_digest;
use super::reject::RejectSubject;
use super::response::{blank_event, decode_failure};
use super::stream::{TradeLineage, trade_lineage};
use super::wire::{
    BalanceAllowance, ClobMarketResponse, ClosedOnlyResponse, HeartbeatResponse, NegRiskResponse,
    OrderRecord, Page, TradeRecord, VersionResponse,
};
use super::{
    DecodeContext, FRAME, HttpAnswer, VenueAnswer, WireError, optional_qty, status_with_fill,
    venue_amount, venue_status,
};

/// The cursor the venue answers on the LAST page — base64 for `-1`. Reported as a page to follow it
/// would send every read walking forever, and the pass it belongs to would never settle.
const END_OF_PAGES: &str = "LTE=";

/// `None` when this page is the last one.
fn page_after(cursor: &str) -> Option<Box<str>> {
    match cursor.is_empty() || cursor == END_OF_PAGES {
        true => None,
        false => Some(cursor.into()),
    }
}

/// Orders read. Each page sorts by whether we know the token and whether the token is bound to an
/// instrument.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DecodedOrders {
    pub events: Vec<ExecEvent>,
    /// Known token, unknown order id. At boot these are prior-run orders to be swept; mid-run they
    /// belong to a placement whose answer hasn't landed, and sweeping is not the same decision as
    /// cancelling.
    pub unmapped: Vec<UnmappedOrder>,
    /// Unknown token. Keep the venue id so boot sweep can reach prior-run orders by id alone.
    pub unattributable: Vec<UnattributableOrder>,
    pub next_cursor: Option<Box<str>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnmappedOrder {
    pub instrument: InstrumentId,
    pub venue_order_id: Box<str>,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub filled: Qty,
    pub status: VenueOrderStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnattributableOrder {
    pub venue_order_id: Box<str>,
    pub side: Side,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrdersRead {
    /// On single-instrument reads only; multi-instrument reads drop the marker.
    pub instrument: InstrumentId,
    pub recon_seq: u64,
}

/// # Errors
/// Malformed JSON, a decimal out of scale, or an unknown status or side spelling.
pub fn decode_orders_page(
    answer: HttpAnswer<'_>,
    read: OrdersRead,
    context: &DecodeContext<'_>,
) -> Result<VenueAnswer<DecodedOrders>, WireError> {
    let OrdersRead {
        instrument,
        recon_seq,
    } = read;
    if let Some(failure) = decode_failure(answer, RejectSubject::Read, context)? {
        return Ok(failure.map(|_| DecodedOrders::default()));
    }
    let page: Page<OrderRecord> = FRAME.decode(answer.body)?;
    let mut decoded = DecodedOrders {
        events: Vec::with_capacity(page.data.len() + 1),
        next_cursor: page_after(&page.next_cursor),
        ..DecodedOrders::default()
    };
    for record in &page.data {
        match decode_order_record(record, ExecKind::SnapshotOrder, recon_seq, context)? {
            Some(event) => decoded.events.push(event),
            None => match unmapped_order(record, context)? {
                Some(unmapped) => decoded.unmapped.push(unmapped),
                None => decoded.unattributable.push(unattributable_order(record)?),
            },
        }
    }
    // Push end marker even on empty page: an account with nothing resting still finishes resync.
    decoded.events.push(ExecEvent {
        recon_seq,
        ..blank_event(ExecKind::SnapshotEnd, instrument, ClientOrderId(0), context)
    });
    Ok(VenueAnswer::Answered(decoded))
}

/// # Errors
/// Malformed JSON, a decimal out of scale, or an unknown status or side spelling.
pub fn decode_single_order(
    answer: HttpAnswer<'_>,
    recon_seq: u64,
    context: &DecodeContext<'_>,
) -> Result<VenueAnswer<Option<ExecEvent>>, WireError> {
    if let Some(failure) = decode_failure(answer, RejectSubject::Read, context)? {
        return Ok(failure.map(Some));
    }
    let record: OrderRecord = FRAME.decode(answer.body)?;
    let event = decode_order_record(&record, ExecKind::SnapshotOrder, recon_seq, context)?;
    Ok(VenueAnswer::Answered(event))
}

fn decode_order_record(
    record: &OrderRecord,
    kind: ExecKind,
    recon_seq: u64,
    context: &DecodeContext<'_>,
) -> Result<Option<ExecEvent>, WireError> {
    let Some(instrument) = context.tokens.instrument(&record.asset_id) else {
        return Ok(None);
    };
    let Some(known) = context.orders.resolve(&record.id) else {
        return Ok(None);
    };
    let qty = qty_field("original_size", &record.original_size)?;
    let filled = optional_qty("size_matched", &record.size_matched)?;
    let price = price_field("price", &record.price)?;
    Ok(Some(ExecEvent {
        venue_order_id: Some(venue_order_id_digest(&record.id)),
        status: Some(status_with_fill(
            venue_status("status", &record.status)?,
            filled,
            qty,
        )),
        side: order_side("side", &record.side)?,
        price,
        qty,
        cumulative_qty: filled,
        // A resting order fills at its own limit, so the quote total is exact.
        cumulative_quote: price.notional(filled),
        recon_seq,
        // Seconds, and an INTEGER on this surface where the stream sends the same idea as a string.
        exchange_ts_us: clamp_exchange_ts(
            record.created_at.saturating_mul(1_000),
            context.received_ts_us,
        ),
        ..blank_event(kind, instrument, known.client_id, context)
    }))
}

/// `Ok(None)` when this run cannot name the order.
fn unmapped_order(
    record: &OrderRecord,
    context: &DecodeContext<'_>,
) -> Result<Option<UnmappedOrder>, WireError> {
    let Some(instrument) = context.tokens.instrument(&record.asset_id) else {
        return Ok(None);
    };
    let qty = qty_field("original_size", &record.original_size)?;
    let filled = optional_qty("size_matched", &record.size_matched)?;
    Ok(Some(UnmappedOrder {
        instrument,
        venue_order_id: record.id.clone().into(),
        side: order_side("side", &record.side)?,
        price: price_field("price", &record.price)?,
        qty,
        filled,
        status: status_with_fill(venue_status("status", &record.status)?, filled, qty),
    }))
}

fn unattributable_order(record: &OrderRecord) -> Result<UnattributableOrder, WireError> {
    Ok(UnattributableOrder {
        venue_order_id: record.id.clone().into(),
        side: order_side("side", &record.side)?,
    })
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct DecodedTrades {
    pub trades: Vec<TradeLineage>,
    pub next_cursor: Option<Box<str>>,
}

/// # Errors
/// Malformed JSON, a decimal out of scale, or an unknown settlement spelling.
pub fn decode_trades_page(
    answer: HttpAnswer<'_>,
    context: &DecodeContext<'_>,
) -> Result<VenueAnswer<DecodedTrades>, WireError> {
    if let Some(failure) = decode_failure(answer, RejectSubject::Read, context)? {
        return Ok(failure.map(|_| DecodedTrades::default()));
    }
    let page: Page<TradeRecord> = FRAME.decode(answer.body)?;
    let mut trades = Vec::with_capacity(page.data.len());
    for record in &page.data {
        if let Some(trade) = trade_lineage(record, context)? {
            trades.push(trade);
        }
    }
    Ok(VenueAnswer::Answered(DecodedTrades {
        trades,
        next_cursor: page_after(&page.next_cursor),
    }))
}

/// # Errors
/// Malformed JSON, or a balance that is not a 6-decimal integer.
pub fn decode_balance(
    answer: HttpAnswer<'_>,
    asset: AssetId,
    context: &DecodeContext<'_>,
) -> Result<VenueAnswer<AssetBalance>, WireError> {
    if let Some(failure) = decode_failure(answer, RejectSubject::Read, context)? {
        return Ok(failure.map(|_| AssetBalance {
            asset,
            free: 0,
            locked: 0,
        }));
    }
    let response: BalanceAllowance = FRAME.decode(answer.body)?;
    Ok(VenueAnswer::Answered(AssetBalance {
        asset,
        free: venue_amount("balance", &response.balance)?,
        // The venue reports no locked or reserved amount; the engine tracks reservations itself.
        locked: 0,
    }))
}

/// How far this run has watched its own fills settle, as the venue's own stamp on the newest such
/// trade in whole milliseconds.
///
/// It exists because this venue publishes no account clock. `/balance-allowance` answers a balance
/// and nothing else, and the answer to a read taken right after a fill is still the pre-fill
/// number. The only thing the venue stamps that moves when money moves is the settlement of the
/// trade that moved it, so that is what a balance chunk carries and what the hot side's release
/// gate compares a reservation against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SettlementWatermark(u64);

impl SettlementWatermark {
    /// Nothing has settled yet. A reservation taken here is held until something does, which is the
    /// safe direction — the alternative frees collateral the venue is still holding.
    pub const NONE: Self = Self(0);

    /// Raises the mark to a settled trade's own venue stamp, and answers whether that was new
    /// evidence. Trades settle out of order and each re-sends once per settlement step, so only a
    /// mark that actually moved is worth a fresh balance read.
    pub fn advance_to(&mut self, settled_ts_us: TsUs) -> bool {
        let millis = (settled_ts_us.micros() / 1_000).max(0) as u64;
        if millis <= self.0 {
            return false;
        }
        self.0 = millis;
        true
    }
}

/// What stamps a balance chunk. The two come from different clocks and mean different things: one
/// is the venue's evidence that these balances moved, the other is when this process read them.
#[derive(Debug, Clone, Copy)]
pub struct AccountStamps {
    pub settled_through: SettlementWatermark,
    pub received_ts_us: TsUs,
}

pub fn account_snapshot(
    balances: &[AssetBalance],
    kind: AccountChunkKind,
    stamps: AccountStamps,
) -> Vec<AccountChunk> {
    let AccountStamps {
        settled_through,
        received_ts_us,
    } = stamps;
    let chunk_stamps = ChunkStamps {
        venue_update_ts_ms: settled_through.0,
        exchange_ts_us: received_ts_us,
        received_ts_us,
    };
    account_chunks(kind, balances, chunk_stamps)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClobMarket {
    pub condition_id: Box<str>,
    pub tokens: Vec<ClobToken>,
    pub min_order_size: Qty,
    pub tick_size: Price,
    pub maker_fee_bps: i32,
    pub taker_fee_bps: i32,
    pub is_accepting_orders: bool,
    /// True on crypto markets, where a taker order is held for 250ms and cannot be cancelled until
    /// the hold lapses.
    pub has_taker_delay: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClobToken {
    pub token_id: Box<str>,
    pub outcome: Box<str>,
}

/// # Errors
/// Malformed JSON, or a body carrying no id at all.
pub fn decode_heartbeat(body: &str) -> Result<Box<str>, WireError> {
    let response: HeartbeatResponse = FRAME.decode(body)?;
    match response.heartbeat_id.is_empty() {
        true => Err(unknown_enum("heartbeat_id", "").into()),
        false => Ok(response.heartbeat_id.into()),
    }
}

/// The venue answers `{"version":2}` where its documentation promises a bare `2`, so both are read.
pub fn decode_protocol_version(body: &str) -> Option<u32> {
    let trimmed = body.trim();
    match serde_json::from_str::<VersionResponse>(trimmed) {
        Ok(response) => Some(response.version),
        Err(_) => trimmed.parse().ok(),
    }
}

/// An absent field reads as None, never false. The gate treats None as a refusal and fails closed.
pub fn decode_closed_only(body: &str) -> Option<bool> {
    FRAME
        .decode::<ClosedOnlyResponse>(body)
        .ok()
        .map(|response| response.closed_only)
}

#[derive(serde::Deserialize)]
pub struct ApiKeyPayload {
    #[serde(rename = "apiKey")]
    pub api_key: String,
    pub secret: String,
    pub passphrase: String,
}

/// Which exchange contract an order on this token is signed for. `/clob-markets` does not say, and
/// the wrong contract invalidates every signature with an unhelpful error.
///
/// # Errors
/// Malformed JSON.
pub fn decode_neg_risk(body: &str) -> Result<bool, WireError> {
    let response: NegRiskResponse = FRAME.decode(body)?;
    Ok(response.neg_risk)
}

/// # Errors
/// Malformed JSON, or a minimum order size or tick size the engine's scale cannot hold.
pub fn decode_clob_market(body: &str) -> Result<ClobMarket, WireError> {
    let response: ClobMarketResponse = FRAME.decode(body)?;
    Ok(ClobMarket {
        condition_id: response.condition_id.into(),
        tokens: response
            .tokens
            .into_iter()
            .map(|token| ClobToken {
                token_id: token.token_id.into(),
                outcome: token.outcome.into(),
            })
            .collect(),
        min_order_size: qty_field("mos", &response.min_order_size.to_string())?,
        tick_size: price_field("mts", &response.min_tick_size.to_string())?,
        maker_fee_bps: response.maker_fee_bps,
        taker_fee_bps: response.taker_fee_bps,
        is_accepting_orders: response.is_accepting_orders,
        has_taker_delay: response.is_taker_order_delay_enabled,
    })
}
