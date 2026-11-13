//! Events and balance chunks that every venue builds the same way. The events raised with no venue
//! message matter most, because without them the hot thread's order slots stay blocked.

use crate::msg::exec::{
    ACCOUNT_CHUNK_ASSETS, AccountChunk, AccountChunkKind, AssetBalance, ExecEvent, ExecKind,
    Provenance, RejectClass,
};
use crate::msg::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty, Side};
use crate::msg::time::{LocalTs, VenueTs};

// A timed-out request may still have reached the venue, so its outcome is ambiguous and only a
// reconciliation can settle it.
pub(crate) fn request_timed_out(
    instrument: InstrumentId,
    client_id: ClientOrderId,
    side: Side,
    at: LocalTs,
) -> ExecEvent {
    ExecEvent {
        reject: Some(RejectClass::Ambiguous),
        side,
        ..blank(ExecKind::AckFailed, Some(instrument), client_id, at)
    }
}

// The order never left this process, so the hot thread unwinds it without counting a venue
// rejection.
pub(crate) fn place_not_sent(
    instrument: InstrumentId,
    client_id: ClientOrderId,
    side: Side,
    at: LocalTs,
) -> ExecEvent {
    ExecEvent {
        side,
        ..blank(ExecKind::PlaceNotSent, Some(instrument), client_id, at)
    }
}

// The amend never left this process, so the order still rests unchanged and the hot thread undoes
// only the amend. The event carries no side because the order's slot already holds its own.
pub(crate) fn amend_not_sent(
    instrument: InstrumentId,
    client_id: ClientOrderId,
    at: LocalTs,
) -> ExecEvent {
    blank(ExecKind::AmendNotSent, Some(instrument), client_id, at)
}

// Ends an open-orders resync. The engine stays unready until it sees one.
pub fn open_orders_snapshot_end(instrument: InstrumentId, at: LocalTs) -> ExecEvent {
    blank(
        ExecKind::SnapshotEnd,
        Some(instrument),
        ClientOrderId(0),
        at,
    )
}

// Live and simulated order streams both raise this once they subscribe.
pub(crate) fn stream_ready(at: LocalTs) -> ExecEvent {
    blank(ExecKind::StreamReady, None, ClientOrderId(0), at)
}

pub(crate) fn stream_reset(at: LocalTs) -> ExecEvent {
    blank(ExecKind::StreamReset, None, ClientOrderId(0), at)
}

/// Sets identity and stamps and leaves every other field at a default, which the caller overrides
/// wherever its event differs.
///
/// `kind` is a parameter rather than a default so no event inherits one silently: the natural
/// default would be `AckFailed`, the one kind that tells the hot thread an order failed. Amends
/// default to unknown, so a venue that cannot amend must say so itself.
pub(crate) fn blank(
    kind: ExecKind,
    instrument: Option<InstrumentId>,
    client_id: ClientOrderId,
    at: LocalTs,
) -> ExecEvent {
    ExecEvent {
        instrument,
        client_id,
        venue_order_id: None,
        trade_id: None,
        kind,
        status: None,
        reject: None,
        provenance: Provenance::Mine,
        side: Side::Buy,
        liquidity: None,
        price: Price(0),
        qty: Qty(0),
        last_price: Price(0),
        last_qty: Qty(0),
        cumulative_qty: Qty(0),
        cumulative_quote: 0,
        commission: 0,
        commission_asset: AssetId::UNKNOWN,
        reject_code: 0,
        amends_remaining: ExecEvent::AMENDS_UNKNOWN,
        recon_seq: 0,
        exchange_ts_us: at.as_venue_calendar(),
        request_sent_ts_us: None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ChunkStamps {
    pub(crate) venue_update_ts_ms: u64,
    pub(crate) exchange_ts_us: VenueTs,
}

/// The engine stays unready until it sees the last chunk of a balance snapshot, so an empty sweep
/// still yields one chunk, marked last.
pub(crate) fn account_chunks(
    kind: AccountChunkKind,
    balances: &[AssetBalance],
    stamps: ChunkStamps,
) -> Vec<AccountChunk> {
    let empty = AccountChunk {
        kind,
        balances: [AssetBalance {
            asset: AssetId::UNKNOWN,
            free: 0,
            locked: 0,
        }; ACCOUNT_CHUNK_ASSETS],
        len: 0,
        is_last_chunk: false,
        venue_update_ts_ms: stamps.venue_update_ts_ms,
        exchange_ts_us: stamps.exchange_ts_us,
    };
    let mut chunks: Vec<AccountChunk> = balances
        .chunks(ACCOUNT_CHUNK_ASSETS)
        .map(|group| {
            let mut chunk = empty;
            chunk.balances[..group.len()].copy_from_slice(group);
            chunk.len = group.len() as u8;
            chunk
        })
        .collect();
    if chunks.is_empty() {
        chunks.push(empty);
    }
    if let Some(last) = chunks.last_mut() {
        last.is_last_chunk = true;
    }
    chunks
}
