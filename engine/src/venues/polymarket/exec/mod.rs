//! The Polymarket execution edge: signatures, codec, signed transport, start gate, and driver.
//! Order ids and token bindings sit beside the codec as pure state, so a test needs no driver.

mod actor;
pub mod assess;
pub mod binding;
pub mod codec;
pub mod correlate;
pub mod handle;
mod preflight;
pub mod probe;
pub mod rest;
pub mod sign;

use std::time::Duration;

use crate::hot::exec::{FeeModel, OrderBudget};
use crate::runtime::secrets::CredentialVariables;
use crate::venues::exec::{LeaseNamespace, VenueCapabilities};

/// A resting order moves nothing in the wallet, so funds stay committed until a trade settles. The
/// tradeable asset is the outcome share itself, its taker fee is charged on top of what a buy
/// spends, and every market expires and is replaced.
///
/// No placement budget: this venue grants no account-wide order count. What it does meter is
/// requests per endpoint, which the edge's own lanes carry.
pub(crate) fn capabilities() -> VenueCapabilities {
    VenueCapabilities {
        holds_reservations_until_settled: false,
        fee_model: FeeModel::BinaryOutcome,
        order_budget: OrderBudget::NONE,
        rotates_markets: true,
        base_asset_is_position: true,
    }
}

/// How long the order client waits for an answer. It stays below the in-flight timeout, so every
/// placement gets an HTTP verdict before the in-flight table reads it back, and that expiry stays
/// the exception rather than the normal path.
pub(crate) const ORDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);

/// One chain and one CLOB, so there is no deployment to separate: the signer address alone tells one
/// account's nonce history from another's.
pub fn lease_namespace(signer_address: &str) -> LeaseNamespace<'_> {
    LeaseNamespace {
        venue: "poly",
        account: Some(signer_address.as_bytes()),
    }
}

/// Unlike Binance, the pair is a wallet key and its address, not an api key and secret: the CLOB's
/// own credentials are derived at boot through [`sign::l1`] and never stored.
pub const POLYMARKET_CREDENTIAL_VARIABLES: CredentialVariables<'static> = CredentialVariables {
    api_key_env: "SIGNER_ADDRESS",
    api_secret_env: "POLYGON_PRIVATE_KEY",
};

/// Non-secret facts the probe discovers and writes back to `.env`, because the account wallet is a
/// different address from the signer on every wallet type but a plain EOA.
pub(crate) const WALLET_ADDRESS_VARIABLE: &str = "POLYMARKET_WALLET_ADDRESS";
pub(crate) const SIGNATURE_TYPE_VARIABLE: &str = "POLYMARKET_SIGNATURE_TYPE";
