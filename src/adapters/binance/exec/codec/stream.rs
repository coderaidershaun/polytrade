//! Decodes the account stream. Its frames arrive unasked and cover the whole account, so each one
//! is first checked for a tracked symbol and an owner.

use crate::adapters::decode::{
    DecimalFault, mantissa_field, order_side, price_field, qty_field, unknown_enum,
};
use crate::adapters::exec::{ChunkStamps, account_chunks, blank};
use crate::adapters::venue_clock::clamp_exchange_ts;
use crate::ids::{AssetId, TradeId, VenueOrderId};
use crate::msg::exec::{
    AccountChunk, AccountChunkKind, AssetBalance, ExecEvent, ExecKind, Liquidity, RejectClass,
};
use crate::registry::AssetDictionary;

use super::client_id::classify_client_order_id;
use super::wire::{ExecutionReport, StreamEnvelope, StreamPayload};
use super::{DecodeContext, FRAME, WireError, money_field, venue_status};

/// What one stream frame became. Nothing is dropped silently: the driver counts every ignored frame
/// so the operator can see it.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamEvent {
    Exec(ExecEvent),
    /// Absolute balances for the assets each chunk names.
    Account(Vec<AccountChunk>),
    /// A balance delta. It is never folded, because one lost frame would leave the balance wrong
    /// forever; a fresh snapshot is needed instead.
    BalanceChanged,
    Ignored(IgnoredReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IgnoredReason {
    /// Dropped here, because an `ExecEvent` promises a configured instrument.
    UntrackedSymbol,
    UnhandledEvent,
}

/// The two payloads that reach here spell the same two numbers differently — the WebSocket event
/// uses `f` and `l`, REST uses `free` and `locked` — so each view carries the names its own source
/// used and a decode failure can name a field the operator will find in the payload.
struct BalanceView<'a> {
    asset: &'a str,
    free: &'a str,
    locked: &'a str,
    free_field: &'static str,
    locked_field: &'static str,
}

/// # Errors
/// Malformed JSON, including an event without its envelope, or a field that is malformed, negated
/// as unavailable, or spelled in a way this engine does not know. Only a mantissa overflow is
/// fatal.
pub fn decode_stream_event(
    json: &str,
    context: &DecodeContext<'_>,
) -> Result<StreamEvent, WireError> {
    let envelope: StreamEnvelope = FRAME.decode(json)?;
    match envelope.event {
        StreamPayload::ExecutionReport(report) => decode_execution_report(&report, context),
        StreamPayload::AccountPosition(position) => {
            let balances = position
                .balances
                .iter()
                .map(|balance| {
                    BalanceView {
                        asset: &balance.asset,
                        free: &balance.free,
                        locked: &balance.locked,
                        free_field: "f",
                        locked_field: "l",
                    }
                    .parse(context.assets)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let stamps = ChunkStamps {
                venue_update_ts_ms: position.last_update_ms.max(0) as u64,
                exchange_ts_us: clamp_exchange_ts(position.event_ts_ms, context.received_ts_us),
                received_ts_us: context.received_ts_us,
            };
            let chunks = account_chunks(AccountChunkKind::Update, &balances, stamps);
            Ok(StreamEvent::Account(chunks))
        }
        StreamPayload::BalanceUpdate => Ok(StreamEvent::BalanceChanged),
        StreamPayload::Unhandled => Ok(StreamEvent::Ignored(IgnoredReason::UnhandledEvent)),
    }
}

/// Splits a full balance read into chunks. The hot side overwrites only the assets a chunk names,
/// and the last chunk of a snapshot is what arms readiness.
///
/// # Errors
/// A decode fault on the first malformed balance. Only a mantissa overflow is fatal.
pub fn account_snapshot_chunks(
    balances: &[crate::adapters::binance::rest::Balance],
    venue_update_ts_ms: i64,
    context: &DecodeContext<'_>,
) -> Result<Vec<AccountChunk>, WireError> {
    let parsed = balances
        .iter()
        .map(|balance| {
            BalanceView {
                asset: &balance.asset,
                free: &balance.free,
                locked: &balance.locked,
                free_field: "free",
                locked_field: "locked",
            }
            .parse(context.assets)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let stamps = ChunkStamps {
        venue_update_ts_ms: venue_update_ts_ms.max(0) as u64,
        exchange_ts_us: clamp_exchange_ts(venue_update_ts_ms, context.received_ts_us),
        received_ts_us: context.received_ts_us,
    };
    Ok(account_chunks(AccountChunkKind::Snapshot, &parsed, stamps))
}

impl BalanceView<'_> {
    fn parse(self, assets: &AssetDictionary) -> Result<AssetBalance, DecimalFault> {
        Ok(AssetBalance {
            // An asset no configured instrument names still crosses, as UNKNOWN, because a balance
            // dropped here is one nobody can later explain.
            asset: assets.id(self.asset),
            free: mantissa_field(self.free_field, self.free)?,
            locked: mantissa_field(self.locked_field, self.locked)?,
        })
    }
}

fn decode_execution_report(
    report: &ExecutionReport,
    context: &DecodeContext<'_>,
) -> Result<StreamEvent, WireError> {
    let Some(instrument) = context.symbols.instrument(&report.symbol) else {
        return Ok(StreamEvent::Ignored(IgnoredReason::UntrackedSymbol));
    };

    let kind = execution_kind(&report.execution_type)?;
    let ownership = classify_client_order_id(subject_id(report, kind), context.identity);

    Ok(StreamEvent::Exec(ExecEvent {
        venue_order_id: (report.order_id >= 0).then_some(VenueOrderId(report.order_id)),
        trade_id: (report.trade_id >= 0).then_some(TradeId(report.trade_id)),
        status: Some(venue_status("X", &report.order_status)?),
        reject: report_reject(kind, &report.reject_reason),
        provenance: ownership.provenance,
        side: order_side("S", &report.side)?,
        liquidity: matches!(kind, ExecKind::ReportTrade).then(|| match report.is_maker {
            true => Liquidity::Maker,
            false => Liquidity::Taker,
        }),
        price: price_field("p", &report.price)?,
        qty: qty_field("q", &report.qty)?,
        last_price: price_field("L", &report.last_price)?,
        last_qty: qty_field("l", &report.last_qty)?,
        cumulative_qty: qty_field("z", &report.cumulative_qty)?,
        cumulative_quote: money_field("Z", &report.cumulative_quote)?,
        commission: commission_field(&report.commission)?,
        commission_asset: commission_asset(report.commission_asset.as_deref(), context.assets),
        exchange_ts_us: clamp_exchange_ts(report.transact_ts_ms, context.received_ts_us),
        // The stream gives a reject reason but no code, and an amend report carries no count of
        // the amends left, so the code and the amend budget keep their unknown defaults.
        ..blank(
            kind,
            instrument,
            ownership.client_id,
            context.received_ts_us,
        )
    }))
}

/// On a cancel or amend report, `c` is the acting request's fresh id, which names no slot, and `C`
/// is the order's own id. The engine addresses the order's slot, so `C` wins there.
fn subject_id(report: &ExecutionReport, kind: ExecKind) -> &str {
    let acts_on_another_order = matches!(kind, ExecKind::ReportCanceled | ExecKind::ReportAmended);
    match acts_on_another_order && !report.orig_client_order_id.is_empty() {
        true => &report.orig_client_order_id,
        false => &report.client_order_id,
    }
}

/// `x` says what happened. `REPLACED` is an amend made through `order.amend.keepPriority`.
fn execution_kind(execution_type: &str) -> Result<ExecKind, WireError> {
    Ok(match execution_type {
        "NEW" => ExecKind::ReportNew,
        "TRADE" => ExecKind::ReportTrade,
        "CANCELED" => ExecKind::ReportCanceled,
        "REPLACED" => ExecKind::ReportAmended,
        "REJECTED" => ExecKind::ReportRejected,
        // Both end the order. Its status then reads `EXPIRED_IN_MATCH` when the account crossed
        // itself.
        "EXPIRED" | "TRADE_PREVENTION" => ExecKind::ReportExpired,
        unknown => return Err(unknown_enum("x", unknown).into()),
    })
}

/// Classifies a rejection by its reason. An expiry is not a rejection: the order existed and the
/// venue took it.
fn report_reject(kind: ExecKind, reason: &str) -> Option<RejectClass> {
    if !matches!(kind, ExecKind::ReportRejected) {
        return None;
    }
    Some(match reason {
        // The post-only outcome when the book moved: the order never rested.
        "WOULD_MATCH_IMMEDIATELY" => RejectClass::Refused,
        // Out of money, so a retry is pointless.
        "INSUFFICIENT_BALANCES" => RejectClass::Fatal,
        // An unknown reason is unhandled, and retrying it would flood duplicates.
        _ => RejectClass::Fatal,
    })
}

/// A rebate is a negative commission. Refusing the sign would drop the fill report with it.
fn commission_field(text: &str) -> Result<i64, DecimalFault> {
    match text.strip_prefix('-') {
        Some(rebate) => mantissa_field("n", rebate).map(|rebate| -rebate),
        None => mantissa_field("n", text),
    }
}

/// An asset no configured instrument names becomes UNKNOWN, so its fee is never booked against
/// one the engine trades. The venue sends null when the fee is zero.
fn commission_asset(asset: Option<&str>, assets: &AssetDictionary) -> AssetId {
    asset.map_or(AssetId::UNKNOWN, |name| assets.id(name))
}
