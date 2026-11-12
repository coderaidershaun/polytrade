//! Polymarket execution edge. `sign` holds the venue's three signature schemes; `codec` is the
//! wire boundary those signatures are composed into; `rest` is the one signed transport all of them
//! reach the venue through; `probe` is the read-only account report that has to come back clean
//! before any order-mutating code runs; `actor` is the driver; `handle` is the seam the engine
//! wires — its startup gate and the actor setup behind it.
//!
//! `correlate` and `binding` sit beside the codec rather than inside the driver because they are
//! the same kind of thing it is: pure state, no socket and no clock, every decision a function of
//! stamped inputs. What an order id means on a venue that mints its own, and which token a leg is
//! trading this window, are this venue's two hardest questions — and neither needs a driver to
//! answer.

mod actor;
pub mod binding;
pub mod codec;
pub mod correlate;
pub mod handle;
mod preflight;
pub mod probe;
pub mod rest;
pub mod sign;

use crate::adapters::exec::{LeaseNamespace, VenueCapabilities};
use crate::hot::exec::{FeeModel, OrderBudget};
use crate::secrets::CredentialVariables;

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
