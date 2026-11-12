//! Order execution for Binance: placing, cancelling, and tracking orders. The implementation
//! modules stay private; only the items below are re-exported.

mod actor;
mod codec;
mod probe;
mod sign;

use crate::adapters::exec::{LeaseNamespace, VenueCapabilities};
use crate::config::BinanceEnv;
use crate::hot::exec::{FeeModel, OrderBudget};
use crate::secrets::CredentialVariables;

pub(crate) use actor::handle::bring_up;
pub use codec::{
    CLIENT_ORDER_ID_LEN, DecodeContext, DecodedResponse, EncodeContext, EncodedRequest,
    IgnoredReason, RejectSubject, ResponseContext, StreamEvent, SymbolTable, WireError,
    account_snapshot_chunks, classify_client_order_id, classify_error, decode_order_record,
    decode_response, decode_stream_event, encode_request, format_client_order_id,
    parse_client_order_id,
};
pub use probe::{ExecutionPreflight, ProbeError, preflight_execution};
pub use sign::{
    ClockOffset, RecvWindow, RequestParams, RequestSigner, RequestStamp, SignError, Signature,
    SignedRequest,
};

/// Spot fees come out of what a trade RECEIVES rather than being added to what it spends, so a buy
/// reserves the notional and nothing more — which is why no fee curve is named here.
///
/// The placement budget is the one fact this venue does not state as a constant: it publishes its
/// ORDERS buckets in `exchangeInfo`, so the caller brings what the account was actually granted.
pub(crate) fn capabilities(order_budget: OrderBudget) -> VenueCapabilities {
    VenueCapabilities {
        holds_reservations_until_settled: true,
        fee_model: FeeModel::None,
        order_budget,
        rotates_markets: false,
        base_asset_is_position: false,
    }
}

/// Testnet and production are separate order books reached with separate keys, so each deployment
/// carries its own history rather than continuing the other's.
pub fn lease_namespace(env: BinanceEnv, api_key: &[u8]) -> LeaseNamespace<'_> {
    LeaseNamespace {
        venue: env.as_str(),
        account: Some(api_key),
    }
}

pub const BINANCE_CREDENTIAL_VARIABLES: CredentialVariables<'static> = CredentialVariables {
    api_key_env: "BINANCE_API_KEY",
    api_secret_env: "BINANCE_API_SECRET",
};
