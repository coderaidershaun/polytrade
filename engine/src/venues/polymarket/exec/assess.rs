//! The pure half of the start gate of Polymarket. It decides from the venue's answers, the wallet
//! config, and the local time, so the order of its refusals holds with no key and no network.

use crate::msg::time::{DurationUs, LocalTs, VenueTs};
use crate::warn;

use super::codec::PROTOCOL_VERSION;
use super::handle::WalletIdentity;
use super::preflight::PolymarketPreflightError;
use super::probe::Geoblock;
use super::sign::address::Address;
use super::sign::order::SignatureType;

const ONE_SECOND: DurationUs = DurationUs::from_secs(1);

/// What the public reads of the venue answered, before any signed request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeFacts {
    /// As the venue answered it, so a body that does not read as a version is shown verbatim.
    pub protocol_version: Box<str>,
    /// The venue answers its clock in whole seconds.
    pub server_ts_us: VenueTs,
    pub geoblock: Geoblock,
}

/// The wallet as the start configuration names it. Each text is kept as written, so a refusal
/// shows what the operator wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletConfig {
    /// The address that the wallet key derives.
    pub signer: Address,
    pub configured_signer: Box<str>,
    pub maker: Box<str>,
    pub signature_type: Box<str>,
}

/// A wallet that passed each check of the gate that needs no signed request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmittedWallet {
    wallet: WalletIdentity,
    venue_clock_offset: DurationUs,
}

/// The checks of the gate before any signed request, in a fixed order: the wallet, then the
/// protocol version. A geoblocked host is reported and never refused, because only the venue's
/// answer to a placement says whether this host may trade.
///
/// # Errors
/// [`PolymarketPreflightError`] of the first check that fails, in that order.
pub fn assess(
    facts: &ProbeFacts,
    wallet: &WalletConfig,
    local: LocalTs,
) -> Result<AdmittedWallet, PolymarketPreflightError> {
    let wallet = wallet.identity()?;
    if *facts.protocol_version != PROTOCOL_VERSION.to_string() {
        return Err(PolymarketPreflightError::ProtocolVersion {
            found: facts.protocol_version.clone(),
            expected: PROTOCOL_VERSION,
        });
    }
    let venue_clock_offset = venue_clock_offset(facts.server_ts_us, local);
    report_geoblock(&facts.geoblock);
    Ok(AdmittedWallet {
        wallet,
        venue_clock_offset,
    })
}

impl AdmittedWallet {
    /// The last check of the gate, after the signed read of the account.
    ///
    /// # Errors
    /// [`PolymarketPreflightError::ClosedOnly`] when the venue lets the account only reduce
    /// positions.
    pub fn assess_account(
        self,
        is_closed_only: bool,
    ) -> Result<WalletIdentity, PolymarketPreflightError> {
        if is_closed_only {
            return Err(PolymarketPreflightError::ClosedOnly);
        }
        Ok(self.wallet)
    }

    pub(crate) fn signer(&self) -> Address {
        self.wallet.signer
    }

    /// Added to the local clock so a signed request is stamped in venue time.
    pub(crate) fn venue_clock_offset(&self) -> DurationUs {
        self.venue_clock_offset
    }
}

impl WalletConfig {
    fn identity(&self) -> Result<WalletIdentity, PolymarketPreflightError> {
        let derived = self.signer.to_checksum_hex();
        if !self.configured_signer.eq_ignore_ascii_case(&derived) {
            return Err(PolymarketPreflightError::SignerMismatch {
                configured: self.configured_signer.clone(),
                derived: derived.into_boxed_str(),
            });
        }
        let maker = Address::parse(&self.maker).map_err(|source| {
            PolymarketPreflightError::WalletAddress {
                value: self.maker.clone(),
                source,
            }
        })?;
        let signature_type = self
            .signature_type
            .trim()
            .parse::<u8>()
            .ok()
            .and_then(|code| SignatureType::from_code(code).ok())
            .ok_or_else(|| PolymarketPreflightError::WalletSignatureType {
                value: self.signature_type.clone(),
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
            signer: self.signer,
            signature_type,
        })
    }
}

/// The venue answers its clock in whole seconds, so the local time is cut to whole seconds too
/// before the two are compared.
fn venue_clock_offset(server: VenueTs, local: LocalTs) -> DurationUs {
    let local_second = LocalTs::from_micros(local.micros() - local.micros() % ONE_SECOND.micros());
    server.offset_from(local_second)
}

/// Reported, never refused: the flag is a website signal, and the venue's answer to a placement is
/// the only ground truth about whether this host may trade.
fn report_geoblock(geoblock: &Geoblock) {
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

fn blank_as_dash(value: &str) -> &str {
    if value.is_empty() { "-" } else { value }
}
