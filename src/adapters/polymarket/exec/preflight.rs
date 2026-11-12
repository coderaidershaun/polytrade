//! Startup gate: prove the wallet can trade before any threads start. Not a formality on Polymarket.
//! Region geoblocks make accounts close-only (not rejected), so unchecked runs appear as strategies
//! that never fire. L2 credentials don't exist until the wallet key mints them, so they can't be
//! configured in advance — only checked at boot.

use std::path::Path;

use crate::adapters::http::TransportError;
use crate::secrets::{EnvFile, SecretError};
use crate::time::DurationUs;
use crate::{info, warn};

use super::codec::{PROTOCOL_VERSION, collateral_balance, open_orders_page};
use super::handle::WalletIdentity;
use super::probe::{Geoblock, Probe, ProbeError};
use super::rest::{ClobHttp, ClobResponse};
use super::sign::address::{Address, AddressError};
use super::sign::key::{KeyError, SigningKey};
use super::sign::l2::{ApiCredentials, RequestSigner};
use super::sign::order::SignatureType;
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

/// Prove this wallet may trade before any thread spawns.
///
/// Placement is the only ground truth for a region restriction, so a geoblocked host is REPORTED
/// and allowed to continue; the account's own close-only flag is a refusal, because the venue has
/// already decided.
///
/// # Errors
/// [`PolymarketPreflightError`] for absent, contradictory or unproven wallet configuration, a venue
/// that cannot be reached or speaks another protocol version, credentials it refuses to mint, and
/// an account it will not let open a position.
pub async fn preflight_polymarket() -> Result<PolymarketPreflight, PolymarketPreflightError> {
    let secrets = EnvFile::load(Path::new(EnvFile::DEFAULT_PATH))?;
    let key =
        SigningKey::from_secret(&secrets.resolve(POLYMARKET_CREDENTIAL_VARIABLES.api_secret_env)?)?;
    let wallet = wallet_identity(&secrets, &key)?;

    let probe = Probe::new(key)?;
    // The probe answers in text so that an unreadable body comes back verbatim: an operator cannot
    // act on "not 2".
    let found = probe.protocol_version().await?;
    if found != PROTOCOL_VERSION.to_string() {
        return Err(PolymarketPreflightError::ProtocolVersion {
            found: found.into(),
            expected: PROTOCOL_VERSION,
        });
    }
    let server_time_secs = probe.server_time().await?;
    let venue_clock_offset = DurationUs::from_secs(server_time_secs - local_unix_secs());
    info!(
        "polymarket venue clock offset {}us, protocol v{PROTOCOL_VERSION}",
        venue_clock_offset.micros()
    );
    report_geoblock(probe.geoblock().await?);

    let credentials = probe.api_credentials(server_time_secs).await?.0;
    let signer = RequestSigner::new(&credentials, wallet.signer)
        .map_err(|source| ProbeError::Credentials { source })?;
    if probe.is_closed_only(&signer, server_time_secs).await? {
        return Err(PolymarketPreflightError::ClosedOnly);
    }
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
        "this polymarket account is closed-only and may only reduce positions — a market maker on it would quote one side into rejection"
    )]
    ClosedOnly,
}

/// The wallet type and account address are discovered, not configured, so the engine reads back what
/// `tools/poly-probe` wrote rather than guessing at them.
fn wallet_identity(
    secrets: &EnvFile,
    key: &SigningKey,
) -> Result<WalletIdentity, PolymarketPreflightError> {
    let signer = key.address();
    let configured = read_plain(secrets, POLYMARKET_CREDENTIAL_VARIABLES.api_key_env)?;
    if !configured.eq_ignore_ascii_case(&signer.to_checksum_hex()) {
        return Err(PolymarketPreflightError::SignerMismatch {
            configured: configured.into_boxed_str(),
            derived: signer.to_checksum_hex().into_boxed_str(),
        });
    }
    let maker_text = read_plain(secrets, WALLET_ADDRESS_VARIABLE)?;
    let maker =
        Address::parse(&maker_text).map_err(|source| PolymarketPreflightError::WalletAddress {
            value: maker_text.into_boxed_str(),
            source,
        })?;
    let code_text = read_plain(secrets, SIGNATURE_TYPE_VARIABLE)?;
    let signature_type = code_text
        .trim()
        .parse::<u8>()
        .ok()
        .and_then(|code| SignatureType::from_code(code).ok())
        .ok_or_else(|| PolymarketPreflightError::WalletSignatureType {
            value: code_text.into_boxed_str(),
        })?;
    if !matches!(
        signature_type,
        SignatureType::Eoa | SignatureType::GnosisSafe
    ) {
        return Err(PolymarketPreflightError::UnprovenWalletType {
            code: signature_type.code(),
        });
    }
    Ok(WalletIdentity {
        maker,
        signer,
        signature_type,
    })
}

/// Addresses and wallet types are not secrets; they arrive through the secret loader only because
/// that is where `.env` is read.
fn read_plain(
    secrets: &EnvFile,
    variable: &'static str,
) -> Result<String, PolymarketPreflightError> {
    let value = secrets.resolve(variable)?;
    str::from_utf8(value.expose_bytes())
        .map(str::to_owned)
        .map_err(|_| PolymarketPreflightError::NotUtf8 { variable })
}

/// Reported, never refused: the flag is a website signal, and the venue's answer to a placement is
/// the only ground truth about whether this host may trade.
fn report_geoblock(geoblock: Geoblock) {
    if !geoblock.blocked {
        return;
    }
    warn!(
        "polymarket reports this host geoblocked (ip {}, country {}, region {}) — placement may be refused at the venue",
        blank_as_dash(&geoblock.ip),
        blank_as_dash(&geoblock.country),
        blank_as_dash(&geoblock.region)
    );
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

fn blank_as_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}

fn local_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs() as i64)
}
