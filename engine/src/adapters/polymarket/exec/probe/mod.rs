//! Read-only account report. Nothing here places, amends or cancels an order — it exists so the
//! facts an execution adapter has to assume are MEASURED before any order-mutating code is written.
//!
//! Three of those facts cannot be learned any other way. The account's wallet type decides the
//! `signatureType` and the `maker` address, and it is nowhere in configuration; a close-only region
//! degrades the venue silently, so it reads as a strategy that never fires rather than as an error;
//! and the L2 credentials do not exist until L1 mints them.

/// An implementation partition, not a concept a caller names: everything worth reaching is
/// re-exported below.
mod payload;

use std::time::Duration;

use super::codec::{
    EncodedRequest, closed_only_status, collateral_balance, create_api_key, derive_api_key,
    open_orders_page, protocol_version, server_time,
};
use super::rest::{CLOB_BASE, ClobHttp, GEOBLOCK_URL};
use super::sign::address::Address;
use super::sign::key::SigningKey;
use super::sign::l2::{ApiCredentials, RequestSigner};
use super::sign::order::SignatureType;

/// The per-signer bucket state, read straight off the shared transport.
pub use super::rest::RateLimit;
pub use payload::{BalanceAllowance, Geoblock, OpenOrders, ProbeError, ResponseShape};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Every wallet type the venue supports. All four are asked rather than the one we expect, because
/// the expected one has already been wrong once: this account was planned as a Deposit Wallet and
/// is a Gnosis Safe.
const CANDIDATE_SIGNATURE_TYPES: [SignatureType; 4] = [
    SignatureType::DepositWallet,
    SignatureType::Eoa,
    SignatureType::Proxy,
    SignatureType::GnosisSafe,
];

/// One wallet-type hypothesis and what the venue said about it. A funded answer under exactly one
/// `signatureType` is the evidence that settles which wallet this account is.
#[derive(Debug)]
pub struct WalletCandidate {
    pub signature_type: SignatureType,
    pub collateral: Result<BalanceAllowance, ProbeError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialSource {
    Derived,
    Created,
}

#[derive(Debug)]
pub struct ProbeReport {
    pub geoblock: Geoblock,
    pub protocol_version: String,
    pub server_time_secs: i64,
    pub clock_skew_secs: i64,
    pub signer: Address,
    pub api_key: String,
    pub api_key_source: CredentialSource,
    pub is_closed_only: Result<bool, ProbeError>,
    pub wallet_candidates: Vec<WalletCandidate>,
    pub open_orders: Result<OpenOrders, ProbeError>,
    pub rate_limit: RateLimit,
}

impl ProbeReport {
    /// The candidate the venue funded. `None` means every wallet type read zero, which is either an
    /// empty account or a wrong signer — the report prints both so a reader can tell.
    pub fn funded_wallet(&self) -> Option<&WalletCandidate> {
        self.wallet_candidates.iter().find(|candidate| {
            candidate
                .collateral
                .as_ref()
                .is_ok_and(BalanceAllowance::is_funded)
        })
    }
}

pub struct Probe {
    http: ClobHttp,
    key: SigningKey,
}

impl Probe {
    /// # Errors
    /// The HTTP client cannot be built because its TLS backend failed to start.
    pub fn new(key: SigningKey) -> Result<Self, ProbeError> {
        Ok(Self {
            http: ClobHttp::new(CONNECT_TIMEOUT, REQUEST_TIMEOUT)?,
            key,
        })
    }

    /// Steps whose failure would make the rest meaningless return early; the per-wallet-type probes
    /// and the reads after them keep their failures IN the report, because a refusal there is an
    /// answer rather than an outage.
    ///
    /// # Errors
    /// The venue is unreachable, or it refuses to mint L2 credentials for this wallet.
    pub async fn run(&self) -> Result<ProbeReport, ProbeError> {
        let geoblock = self.geoblock().await?;
        let protocol_version = self.protocol_version().await?;
        let server_time_secs = self.server_time().await?;
        let clock_skew_secs = server_time_secs - local_unix_secs()?;

        let (credentials, api_key_source) = self.api_credentials(server_time_secs).await?;
        let api_key = credentials.api_key().to_owned();
        let signer = self.key.address();
        let request_signer = RequestSigner::new(&credentials, signer)
            .map_err(|source| ProbeError::Credentials { source })?;

        let is_closed_only = self.is_closed_only(&request_signer, server_time_secs).await;
        let mut wallet_candidates = Vec::with_capacity(CANDIDATE_SIGNATURE_TYPES.len());
        for signature_type in CANDIDATE_SIGNATURE_TYPES {
            let collateral = self
                .collateral(&request_signer, server_time_secs, signature_type)
                .await;
            wallet_candidates.push(WalletCandidate {
                signature_type,
                collateral,
            });
        }
        let (open_orders, rate_limit) = self.open_orders(&request_signer, server_time_secs).await;

        Ok(ProbeReport {
            geoblock,
            protocol_version,
            server_time_secs,
            clock_skew_secs,
            signer,
            api_key,
            api_key_source,
            is_closed_only,
            wallet_candidates,
            open_orders,
            rate_limit,
        })
    }

    pub(super) fn http(&self) -> &ClobHttp {
        &self.http
    }

    pub(super) fn into_key(self) -> SigningKey {
        self.key
    }

    pub(super) async fn geoblock(&self) -> Result<Geoblock, ProbeError> {
        let response = self.http.send_unsigned(GEOBLOCK_URL).await?;
        payload::decode(GEOBLOCK_URL, &response)
    }

    pub(super) async fn protocol_version(&self) -> Result<String, ProbeError> {
        let request = protocol_version();
        let response = self.http.send_public(&request).await?;
        payload::read_protocol_version(&request.path, &response)
    }

    /// Signing against the venue's own clock rather than ours: `POLY_TIMESTAMP` has no documented
    /// staleness tolerance, so a skewed host would fail auth with no way to tell why.
    pub(super) async fn server_time(&self) -> Result<i64, ProbeError> {
        let request = server_time();
        let response = self.http.send_public(&request).await?;
        let body = payload::success_body(&request.path, &response)?;
        body.trim().parse().map_err(|_| ProbeError::UnexpectedBody {
            url: format!("{CLOB_BASE}/time"),
            body: body.to_owned(),
        })
    }

    /// Derive first: it is idempotent for an address that already holds credentials, where create
    /// would fail. A fresh wallet has none to derive, so create is the fallback, not the first move.
    pub(super) async fn api_credentials(
        &self,
        timestamp_secs: i64,
    ) -> Result<(ApiCredentials, CredentialSource), ProbeError> {
        let derived = self
            .credential_call(&derive_api_key(), timestamp_secs)
            .await;
        match derived {
            Ok(credentials) => Ok((credentials, CredentialSource::Derived)),
            Err(derive_failure) => {
                let created = self
                    .credential_call(&create_api_key(), timestamp_secs)
                    .await
                    .map_err(|create_failure| ProbeError::NoCredentials {
                        derive: derive_failure.to_string(),
                        create: create_failure.to_string(),
                    })?;
                Ok((created, CredentialSource::Created))
            }
        }
    }

    async fn credential_call(
        &self,
        request: &EncodedRequest,
        timestamp_secs: i64,
    ) -> Result<ApiCredentials, ProbeError> {
        let response = self
            .http
            .send_wallet_signed(&self.key, request, timestamp_secs)
            .await?;
        payload::read_api_credentials(&request.path, &response)
    }

    pub(super) async fn is_closed_only(
        &self,
        signer: &RequestSigner,
        timestamp_secs: i64,
    ) -> Result<bool, ProbeError> {
        let request = closed_only_status();
        let response = self
            .http
            .send_signed(signer, &request, timestamp_secs)
            .await?;
        payload::read_closed_only(&request.path, &response)
    }

    async fn collateral(
        &self,
        signer: &RequestSigner,
        timestamp_secs: i64,
        signature_type: SignatureType,
    ) -> Result<BalanceAllowance, ProbeError> {
        let request = collateral_balance(signature_type);
        let response = self
            .http
            .send_signed(signer, &request, timestamp_secs)
            .await?;
        payload::read_balance_allowance(&request.path, &response)
    }

    async fn open_orders(
        &self,
        signer: &RequestSigner,
        timestamp_secs: i64,
    ) -> (Result<OpenOrders, ProbeError>, RateLimit) {
        let request = open_orders_page(None);
        match self
            .http
            .send_signed(signer, &request, timestamp_secs)
            .await
        {
            Ok(response) => (
                payload::read_open_orders(&request.path, &response),
                response.rate_limit.clone(),
            ),
            Err(failure) => (Err(failure.into()), RateLimit::ABSENT),
        }
    }
}

/// The one condition that can fail here — a host clock before 1970 — is exactly the condition the
/// skew measurement exists to catch, so it is an error rather than a zero that would be reported as
/// a fifty-year skew.
fn local_unix_secs() -> Result<i64, ProbeError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .map_err(|_| ProbeError::HostClock)
}
