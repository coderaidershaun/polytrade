//! The thin half of the start gate of Polymarket: it reads the wallet config and the venue, and
//! the pure checks decide, before any thread starts, so a closed-only account never quotes.

use crate::msg::time::{DurationUs, LocalTs, VenueTs, wall_clock_now};
use crate::runtime::secrets::{EnvFile, SecretError};
use crate::venues::edge::http::TransportError;
use crate::{info, warn};

use super::assess::{AccountStanding, ProbeFacts, WalletConfig, assess};
use super::codec::{PROTOCOL_VERSION, collateral_balance, open_orders_page};
use super::handle::WalletIdentity;
use super::probe::{Probe, ProbeError};
use super::rest::{ClobHttp, ClobResponse};
use super::sign::address::AddressError;
use super::sign::key::{KeyError, SigningKey};
use super::sign::l2::{ApiCredentials, RequestSigner};
use super::{POLYMARKET_CREDENTIAL_VARIABLES, SIGNATURE_TYPE_VARIABLE, WALLET_ADDRESS_VARIABLE};

/// What the gate proved, handed to the adapter so none of it is derived a second time.
pub struct PolymarketPreflight {
    /// Minted with the wallet key at boot and never written to disk.
    pub credentials: ApiCredentials,
    pub key: SigningKey,
    pub wallet: WalletIdentity,
    /// Added to the local clock so `POLY_TIMESTAMP` is stamped in venue time.
    pub venue_clock_offset: DurationUs,
}

/// Prove this wallet may trade before any thread spawns. Each check that needs no signed request
/// runs before the first one goes out.
///
/// Placement is the only ground truth for a region restriction, so a geoblocked host is REPORTED
/// and allowed to continue; the account's own close-only flag is a refusal, because the venue has
/// already decided.
///
/// # Errors
/// [`PolymarketPreflightError`] for absent, contradictory or unproven wallet configuration, a venue
/// that cannot be reached or speaks another protocol version, credentials it refuses to mint, and
/// an account it will not let open a position.
#[expect(
    clippy::disallowed_methods,
    reason = "a start preflight reads the wall clock once, before any message exists"
)]
pub async fn preflight_polymarket(
    secrets: &EnvFile,
) -> Result<PolymarketPreflight, PolymarketPreflightError> {
    let key =
        SigningKey::from_secret(&secrets.resolve(POLYMARKET_CREDENTIAL_VARIABLES.api_secret_env)?)?;
    let wallet_config = read_wallet_config(secrets, &key)?;

    let probe = Probe::new(key)?;
    // The probe answers in text so that an unreadable body comes back verbatim: an operator cannot
    // act on "not 2".
    let protocol_version = probe.protocol_version().await?;
    let server_time_secs = probe.server_time().await?;
    // Read right after the venue's clock answered, so the offset holds no time of the other reads.
    let local = wall_clock_now();
    let facts = ProbeFacts {
        protocol_version: protocol_version.into_boxed_str(),
        server_ts_us: VenueTs::from_micros(0) + DurationUs::from_secs(server_time_secs),
        geoblock: probe.geoblock().await?,
    };
    let admitted = assess(&facts, &wallet_config, local)?;
    let venue_clock_offset = admitted.venue_clock_offset();
    info!(
        "polymarket venue clock offset {}us, protocol v{PROTOCOL_VERSION}",
        venue_clock_offset.micros()
    );

    let credentials = probe.api_credentials(server_time_secs).await?.0;
    let signer = RequestSigner::new(&credentials, admitted.signer())
        .map_err(|source| ProbeError::Credentials { source })?;
    let is_closed_only = probe.is_closed_only(&signer, server_time_secs).await?;
    let standing = if is_closed_only { AccountStanding::ClosedOnly } else { AccountStanding::Open };
    let wallet = admitted.assess_account(standing)?;
    info!(
        "polymarket account ok: may open positions, maker {} signing as {} (signatureType {})",
        wallet.maker.to_checksum_hex(),
        wallet.signer.to_checksum_hex(),
        wallet.signature_type.code()
    );
    report_account(probe.http(), &signer, wallet, server_time_secs).await;

    Ok(PolymarketPreflight {
        credentials,
        key: probe.into_key(),
        wallet,
        venue_clock_offset,
    })
}

#[derive(thiserror::Error, Debug)]
pub enum PolymarketPreflightError {
    #[error(
        "polymarket credentials could not be loaded — execution needs a wallet key it can sign with"
    )]
    Credentials(#[from] SecretError),
    #[error("{variable} holds bytes that are not utf-8")]
    NotUtf8 { variable: &'static str },
    #[error("the polymarket wallet private key could not be read")]
    Key(#[from] KeyError),
    #[error(
        "{} names {configured} but the private key derives {derived} — one of the two is another account's",
        POLYMARKET_CREDENTIAL_VARIABLES.api_key_env
    )]
    SignerMismatch {
        configured: Box<str>,
        derived: Box<str>,
    },
    #[error(
        "{WALLET_ADDRESS_VARIABLE} = {value:?} is not an ethereum address — run tools/poly-probe to discover this account's"
    )]
    WalletAddress {
        value: Box<str>,
        #[source]
        source: AddressError,
    },
    #[error(
        "{SIGNATURE_TYPE_VARIABLE} = {value:?}, expected 0 (eoa), 1 (proxy), 2 (gnosis safe) or 3 (deposit wallet) — run tools/poly-probe to discover this account's"
    )]
    WalletSignatureType { value: Box<str> },
    #[error(
        "this wallet signs orders as signatureType {code}, which needs the erc-7739 wrap — the wrap is implemented and pinned against a vector, but no live order has ever been signed with it, and an unproven signing path fails at the venue as a generic refusal"
    )]
    UnprovenWalletType { code: u8 },
    #[error(transparent)]
    Probe(#[from] ProbeError),
    #[error(
        "the polymarket clob speaks protocol {found}, and every payload this engine signs is shaped for {expected}"
    )]
    ProtocolVersion { found: Box<str>, expected: u32 },
    #[error(
        "this host's clock reads {}us since 1970, as a clock set at or before the epoch does, so the venue would reject each request it signs — set the system clock",
        seen.micros()
    )]
    LocalClockInvalid { seen: LocalTs },
    #[error(
        "this polymarket account is closed-only and may only reduce positions — a market maker on it would quote one side into rejection"
    )]
    ClosedOnly,
}

/// The wallet type and account address are discovered, not configured, so the engine reads back what
/// `tools/poly-probe` wrote rather than guessing at them.
fn read_wallet_config(
    secrets: &EnvFile,
    key: &SigningKey,
) -> Result<WalletConfig, PolymarketPreflightError> {
    Ok(WalletConfig {
        signer: key.address(),
        configured_signer: read_plain(secrets, POLYMARKET_CREDENTIAL_VARIABLES.api_key_env)?,
        maker: read_plain(secrets, WALLET_ADDRESS_VARIABLE)?,
        signature_type: read_plain(secrets, SIGNATURE_TYPE_VARIABLE)?,
    })
}

/// Addresses and wallet types are not secrets; they arrive through the secret loader only because
/// that is where `.env` is read.
fn read_plain(
    secrets: &EnvFile,
    variable: &'static str,
) -> Result<Box<str>, PolymarketPreflightError> {
    let value = secrets.resolve(variable)?;
    str::from_utf8(value.expose_bytes())
        .map(Box::from)
        .map_err(|_| PolymarketPreflightError::NotUtf8 { variable })
}

/// What the operator has to see before a live arm: the collateral this wallet type actually holds,
/// and whether anything is already resting on the credentials.
async fn report_account(
    http: &ClobHttp,
    signer: &RequestSigner,
    wallet: WalletIdentity,
    timestamp_secs: i64,
) {
    report_read(
        "collateral",
        http.send_signed(
            signer,
            &collateral_balance(wallet.signature_type),
            timestamp_secs,
        )
        .await,
    );
    report_read(
        "open orders",
        http.send_signed(signer, &open_orders_page(None), timestamp_secs)
            .await,
    );
}

fn report_read(what: &str, answer: Result<ClobResponse, TransportError>) {
    match answer {
        Ok(response) if response.is_success() => {
            info!("polymarket {what} before arming: {}", response.excerpt());
        }
        Ok(response) => warn!(
            "polymarket {what} read answered http {} — arming against a number it could not confirm: {}",
            response.status,
            response.excerpt()
        ),
        Err(error) => warn!("polymarket {what} read failed: {error}"),
    }
}
