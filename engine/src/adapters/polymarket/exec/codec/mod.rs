//! Translates Polymarket JSON to and from `ExecEvent` and `AccountChunk`, holding no socket, clock
//! or state. Three venue quirks are normalised here and nowhere else, because each yields a
//! silently wrong answer rather than a parse failure: order status has three spellings (`live`,
//! `LIVE`, `ORDER_STATUS_LIVE`) and trade status two; the read surfaces send decimal strings where
//! the write surface sends 6-decimal integers; and three payloads report fills, of which only two
//! can be trusted.

mod correlation;
mod read;
mod reject;
mod request;
mod response;
mod stream;
mod wire;

use crate::adapters::decode::{DecimalFault, JsonFrame, mantissa_field, qty_field, unknown_enum};
use crate::ids::{InstrumentId, Price, Qty};
use crate::msg::exec::VenueOrderStatus;
use crate::time::TsUs;

pub use correlation::{
    KnownOrder, OrderIndex, OrderIndexFull, trade_id_digest, venue_order_id_digest,
};
pub use read::{
    AccountStamps, ApiKeyPayload, ClobMarket, ClobToken, DecodedOrders, DecodedTrades, OrdersRead,
    SettlementWatermark, UnattributableOrder, UnmappedOrder, account_snapshot, decode_balance,
    decode_clob_market, decode_closed_only, decode_heartbeat, decode_neg_risk, decode_orders_page,
    decode_protocol_version, decode_single_order, decode_trades_page,
};
pub use reject::{RejectSubject, RejectVerdict, VenueAvailability, VenueFailure, classify_error};
pub use request::{
    EncodedRequest, OrderSigner, OrderSignerSetup, cancel_market_orders, cancel_venue_order,
    clob_market_request, closed_only_status, collateral_balance, conditional_allowance_refresh,
    conditional_balance, create_api_key, derive_api_key, encode_request, heartbeat,
    neg_risk_request, open_orders_for_token, open_orders_page, protocol_version, server_time,
    subscribe_user_stream, trades_page,
};
pub use response::{
    PlaceOutcome, PlaceRequestContext, PlacedOrder, PlacementStatus, decode_cancel, decode_place,
};
pub use stream::{
    IgnoredReason, MakerFill, StreamEvent, TradeLineage, TradeSettlement, decode_stream_frame,
};

pub(super) const FRAME: JsonFrame = JsonFrame("polymarket execution");

/// The venue's protocol version. Version 2 is not compatible with version 1, and the venue rejects
/// a payload shaped for another version without saying why.
pub const PROTOCOL_VERSION: u32 = 2;

// Venue scale (6 decimals) to engine scale (8 decimals) conversion factor.
const VENUE_TO_ENGINE_SCALE: i64 = 100;

/// A venue answer before any endpoint reads it. Unavailability is split off at this level so no
/// decoder can forget it: a maintenance window is not a verdict on an order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VenueAnswer<T> {
    Answered(T),
    Unavailable(VenueAvailability),
}

impl<T> VenueAnswer<T> {
    pub fn map<U>(self, transform: impl FnOnce(T) -> U) -> VenueAnswer<U> {
        match self {
            VenueAnswer::Answered(value) => VenueAnswer::Answered(transform(value)),
            VenueAnswer::Unavailable(availability) => VenueAnswer::Unavailable(availability),
        }
    }
}

/// The status and the body travel together because the venue reports failures in both, and neither
/// decides alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpAnswer<'a> {
    pub status: u16,
    pub body: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub struct DecodeContext<'a> {
    pub tokens: &'a TokenTable,
    pub orders: &'a OrderIndex,
    /// The api key every payload names as its owner. It tells our maker fills apart from a
    /// counterparty's within one trade event.
    pub api_key: &'a str,
    /// When this process received the message. Venue stamps are clamped against it; it is not the
    /// venue's clock.
    pub received_ts_us: TsUs,
}

/// Encoder context: the same bindings the decoder reads, plus the account material an order
/// signature needs and the stamp it is signed with.
#[derive(Debug, Clone, Copy)]
pub struct EncodeContext<'a> {
    pub tokens: &'a TokenTable,
    pub orders: &'a OrderIndex,
    pub signer: &'a OrderSigner,
    /// Wire-send stamp. It becomes the signed order's millisecond timestamp, which is this venue's
    /// uniqueness key — the codec reads no clock of its own, so a replay re-signs identically.
    pub sent_ts_us: TsUs,
}

/// One tradeable outcome token, as the engine currently knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenBinding {
    pub instrument: InstrumentId,
    pub token_id: Box<str>,
    pub tick: Price,
    pub is_neg_risk: bool,
}

/// Maps instruments to token ids and back, and remembers retired bindings.
///
/// The up/down markets mint a fresh token pair every five minutes under a fixed [`InstrumentId`],
/// so this table is REBOUND while orders from the previous window may still be settling. Retired
/// bindings are kept precisely so a late fill on a token the engine has stopped quoting still
/// routes to the instrument that owns its position, instead of being dropped as untracked.
///
/// No `Default`: a zero retired capacity evicts nothing and the list grows without bound, which is
/// the opposite of what the type promises. The depth is always a stated decision, as it is on
/// [`OrderIndex`].
#[derive(Debug, Clone)]
pub struct TokenTable {
    live: Vec<TokenBinding>,
    retired: Vec<TokenBinding>,
    retired_capacity: usize,
}

impl TokenTable {
    pub fn with_retired_capacity(retired_capacity: usize) -> Self {
        Self {
            live: Vec::new(),
            retired: Vec::with_capacity(retired_capacity),
            retired_capacity,
        }
    }

    /// Rebinding an instrument retires whatever it pointed at. The oldest retired binding is
    /// dropped once capacity is reached — by then its market has resolved and no event can name it.
    pub fn bind(&mut self, binding: TokenBinding) {
        if let Some(index) = self
            .live
            .iter()
            .position(|live| live.instrument == binding.instrument)
        {
            let previous = self.live.swap_remove(index);
            if previous.token_id != binding.token_id {
                while !self.retired.is_empty() && self.retired.len() >= self.retired_capacity {
                    self.retired.remove(0);
                }
                self.retired.push(previous);
            }
        }
        self.live.push(binding);
    }

    /// Only a live binding may be quoted against; a retired one exists to route answers home.
    pub fn live_binding(&self, instrument: InstrumentId) -> Option<&TokenBinding> {
        self.live
            .iter()
            .find(|binding| binding.instrument == instrument)
    }

    pub fn instrument(&self, token_id: &str) -> Option<InstrumentId> {
        self.live
            .iter()
            .chain(self.retired.iter())
            .find(|binding| &*binding.token_id == token_id)
            .map(|binding| binding.instrument)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WireError {
    #[error(transparent)]
    Decode(#[from] DecimalFault),
    #[error(
        "no live token binding for instrument {instrument} — the rotation has not been applied"
    )]
    UnboundInstrument { instrument: u16 },
    #[error(
        "no venue order id for client order {client_id:x} — polymarket echoes no client id, so an order that has not answered cannot be addressed"
    )]
    UnknownOrder { client_id: u64 },
    #[error("{request} is not a request this venue offers")]
    UnsupportedRequest { request: &'static str },
    #[error("the {what} holds bytes that are not utf-8")]
    CredentialNotUtf8 { what: &'static str },
    #[error("polymarket amount {value:?} in {field} is not a 6-decimal integer")]
    VenueAmount {
        field: &'static str,
        value: Box<str>,
    },
    #[error("could not build the {what} request body: {source}")]
    Encoding {
        what: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    Signing(#[from] super::sign::order::OrderSignError),
    #[error(transparent)]
    Amount(#[from] super::sign::amount::AmountError),
}

impl WireError {
    pub fn is_fatal(&self) -> bool {
        matches!(self, WireError::Decode(fault) if fault.is_fatal())
    }
}

/// Three vocabularies, all live: the place response answers lowercase, REST and the stream answer
/// uppercase, and the OpenAPI enum prefixes both.
///
/// `Delayed` has no engine spelling of its own — the order is accepted and uncancellable for the
/// venue's hold, which is not a resting order and not a fill. It maps to `New`, and the caller
/// learns the difference from [`PlacementStatus`], which is the only place that distinction can be
/// acted on.
fn venue_status(field: &'static str, status: &str) -> Result<VenueOrderStatus, WireError> {
    let bare = status
        .strip_prefix("ORDER_STATUS_")
        .unwrap_or(status)
        .to_ascii_uppercase();
    Ok(match bare.as_str() {
        "LIVE" | "DELAYED" | "UNMATCHED" => VenueOrderStatus::New,
        "MATCHED" => VenueOrderStatus::Filled,
        "CANCELED" | "CANCELED_MARKET_RESOLVED" => VenueOrderStatus::Canceled,
        "INVALID" => VenueOrderStatus::Rejected,
        _ => return Err(unknown_enum(field, status).into()),
    })
}

/// A resting order with a partial fill answers `LIVE`, which the engine spells `PartiallyFilled`.
/// The venue never says so itself; the size is the only evidence.
fn status_with_fill(status: VenueOrderStatus, filled: Qty, total: Qty) -> VenueOrderStatus {
    match status == VenueOrderStatus::New && filled.0 > 0 && filled.0 < total.0 {
        true => VenueOrderStatus::PartiallyFilled,
        false => status,
    }
}

/// A venue amount to an 8-decimal mantissa. Two shapes reach here for the SAME quantity: the write
/// surface sends a 6-decimal integer (`"5200000"` = 5.2), but the place RESPONSE was found live to
/// carry the read surface's decimal-dollar string instead (`"2.549999"`), which the doc-shaped
/// fixtures missed. A decimal point selects the decimal path; its absence is the integer form,
/// scaled by [`VENUE_TO_ENGINE_SCALE`]. Integer-only in both directions — these values are money
/// and share counts, and neither may round-trip through `f64`.
fn venue_amount(field: &'static str, text: &str) -> Result<i64, WireError> {
    let digits = text.trim();
    if digits.contains('.') {
        return Ok(mantissa_field(field, digits)?);
    }
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(WireError::VenueAmount {
            field,
            value: text.into(),
        });
    }
    digits
        .parse::<i64>()
        .ok()
        .and_then(|value| value.checked_mul(VENUE_TO_ENGINE_SCALE))
        .ok_or_else(|| WireError::VenueAmount {
            field,
            value: text.into(),
        })
}

/// The venue leaves an amount EMPTY rather than zero when it has nothing to report, which is the
/// shape a `delayed` or `unmatched` placement answers with.
fn optional_venue_amount(field: &'static str, text: &str) -> Result<i64, WireError> {
    match text.trim().is_empty() {
        true => Ok(0),
        false => venue_amount(field, text),
    }
}

pub(super) fn optional_qty(field: &'static str, text: &str) -> Result<Qty, WireError> {
    match text.trim().is_empty() {
        true => Ok(Qty(0)),
        false => Ok(qty_field(field, text)?),
    }
}
