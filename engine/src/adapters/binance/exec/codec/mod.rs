//! Translates Binance JSON to and from [`ExecEvent`] and [`AccountChunk`], resolving symbols,
//! assets and client ids in one place. It holds no socket, clock or state: every stamp arrives as a
//! parameter, so a replay decodes identically.

mod client_id;
mod reject;
mod request;
mod response;
mod stream;
mod wire;

use crate::adapters::decode::{DecimalFault, JsonFrame, mantissa_field, unknown_enum};
use crate::adapters::exec::EngineIdentity;
use crate::ids::InstrumentId;
use crate::msg::exec::VenueOrderStatus;
use crate::registry::{AssetDictionary, Registry};
use crate::time::TsUs;

pub(super) const FRAME: JsonFrame = JsonFrame("binance execution");

pub use client_id::{
    CLIENT_ORDER_ID_LEN, classify_client_order_id, format_client_order_id, parse_client_order_id,
};
pub use reject::{RejectSubject, classify_error};
pub use request::{EncodedRequest, encode_request};
pub use response::{DecodedResponse, ResponseContext, decode_order_record, decode_response};
pub use stream::{IgnoredReason, StreamEvent, account_snapshot_chunks, decode_stream_event};

#[derive(Debug, Clone, Copy)]
pub struct DecodeContext<'a> {
    pub symbols: &'a SymbolTable,
    pub assets: &'a AssetDictionary,
    pub identity: EngineIdentity,
    /// When this process received the message. Venue stamps are clamped against it; it is not the
    /// venue's clock.
    pub received_ts_us: TsUs,
}

#[derive(Debug, Clone, Copy)]
pub struct EncodeContext<'a> {
    pub symbols: &'a SymbolTable,
    pub identity: EngineIdentity,
}

#[derive(Debug, Clone, Default)]
pub struct SymbolTable {
    rows: Vec<SymbolRow>,
}

#[derive(Debug, Clone)]
struct SymbolRow {
    instrument: InstrumentId,
    wire_symbol: Box<str>,
}

impl SymbolTable {
    /// Symbols are stored upper-case, because Binance rejects lower-case ones.
    pub fn new(symbols: impl IntoIterator<Item = (InstrumentId, Box<str>)>) -> Self {
        Self {
            rows: symbols
                .into_iter()
                .map(|(instrument, symbol)| SymbolRow {
                    instrument,
                    wire_symbol: symbol.to_uppercase().into(),
                })
                .collect(),
        }
    }

    pub fn from_registry(registry: &Registry) -> Self {
        Self::new(
            registry
                .instruments()
                .iter()
                .map(|row| (row.instrument_id, row.venue_symbol.clone())),
        )
    }

    pub fn instrument(&self, symbol: &str) -> Option<InstrumentId> {
        self.rows
            .iter()
            .find(|row| row.wire_symbol.eq_ignore_ascii_case(symbol))
            .map(|row| row.instrument)
    }

    pub fn symbol(&self, instrument: InstrumentId) -> Option<&str> {
        self.rows
            .iter()
            .find(|row| row.instrument == instrument)
            .map(|row| &*row.wire_symbol)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WireError {
    #[error(transparent)]
    Decode(#[from] DecimalFault),
    #[error(
        "binance reports {field} as {value:?}, which it does for a value it cannot supply — refusing to fold an unavailable amount"
    )]
    UnavailableAmount {
        field: &'static str,
        value: Box<str>,
    },
    #[error("binance answered with neither a result nor an error")]
    EmptyResponse,
    #[error(
        "no venue symbol for instrument {instrument} — the symbol table and the request disagree"
    )]
    UnknownInstrument { instrument: u16 },
}

impl WireError {
    pub fn is_fatal(&self) -> bool {
        matches!(self, WireError::Decode(fault) if fault.is_fatal())
    }
}

fn venue_status(field: &'static str, status: &str) -> Result<VenueOrderStatus, WireError> {
    Ok(match status {
        "NEW" => VenueOrderStatus::New,
        "PARTIALLY_FILLED" => VenueOrderStatus::PartiallyFilled,
        "FILLED" => VenueOrderStatus::Filled,
        "CANCELED" => VenueOrderStatus::Canceled,
        "PENDING_CANCEL" => VenueOrderStatus::PendingCancel,
        "REJECTED" => VenueOrderStatus::Rejected,
        "EXPIRED" => VenueOrderStatus::Expired,
        "EXPIRED_IN_MATCH" => VenueOrderStatus::ExpiredInMatch,
        unknown => return Err(unknown_enum(field, unknown).into()),
    })
}

/// Binance negates an amount it cannot supply, so a leading `-` is a refusal, not a number.
fn money_field(field: &'static str, text: &str) -> Result<i64, WireError> {
    if text.starts_with('-') {
        return Err(WireError::UnavailableAmount {
            field,
            value: text.into(),
        });
    }
    Ok(mantissa_field(field, text)?)
}
