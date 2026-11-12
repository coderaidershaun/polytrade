//! The venue's response bodies, and the typed facts read out of them.
//!
//! Every reader here is deliberately lenient about SHAPE and strict about failure: the probe exists
//! to settle questions the documentation gets wrong, so a shape the docs did not promise has to
//! come back as data rather than as a parse error.

use serde::Deserialize;

use crate::adapters::http::TransportError;
use crate::secrets::Secret;

use super::super::codec::{ApiKeyPayload, decode_closed_only, decode_protocol_version};
use super::super::rest::ClobResponse;
use super::super::sign::l2::{ApiCredentials, L2Error};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Geoblock {
    #[serde(default)]
    pub blocked: bool,
    #[serde(default)]
    pub ip: String,
    #[serde(default)]
    pub country: String,
    #[serde(default)]
    pub region: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BalanceAllowance {
    pub balance: String,
    pub allowances: Vec<(String, String)>,
}

impl BalanceAllowance {
    /// The balance is a 1e6 integer as a string, and only its non-zero-ness is being asked here —
    /// parsing it to compare against zero would invent a failure mode for no gain.
    pub fn is_funded(&self) -> bool {
        self.balance
            .chars()
            .any(|digit| digit.is_ascii_digit() && digit != '0')
    }
}

/// Whether `/data/orders` answers `{data:[…]}` or a bare array — the docs disagree with themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResponseShape {
    Wrapped,
    BareArray,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OpenOrders {
    pub count: usize,
    pub shape: ResponseShape,
}

#[derive(thiserror::Error, Debug)]
pub enum ProbeError {
    #[error("the venue could not be reached")]
    Http(#[from] TransportError),
    #[error("{url} answered http {status}: {body}")]
    Status {
        url: String,
        status: u16,
        body: String,
    },
    #[error("{url} answered a body this probe cannot read: {body}")]
    UnexpectedBody { url: String, body: String },
    #[error(
        "the venue minted no l2 credentials for this wallet — derive said: {derive}; create said: {create}"
    )]
    NoCredentials { derive: String, create: String },
    #[error("the minted l2 credentials cannot be used to sign")]
    Credentials {
        #[source]
        source: L2Error,
    },
    #[error("this host's clock reads before 1970, so no signed request it stamps can be valid")]
    HostClock,
}

/// Reported as text, not as a number: the probe exists to say what the venue answered, and a body
/// [`decode_protocol_version`] cannot read is exactly the finding worth carrying back.
pub(super) fn read_protocol_version(
    url: &str,
    response: &ClobResponse,
) -> Result<String, ProbeError> {
    let body = success_body(url, response)?;
    let Some(version) = decode_protocol_version(body) else {
        return Ok(body.trim().to_owned());
    };
    Ok(version.to_string())
}

pub(super) fn read_api_credentials(
    url: &str,
    response: &ClobResponse,
) -> Result<ApiCredentials, ProbeError> {
    let payload: ApiKeyPayload = decode(url, response)?;
    Ok(ApiCredentials::new(
        payload.api_key,
        Secret::new(&payload.secret),
        Secret::new(&payload.passphrase),
    ))
}

/// A body without the `closed_only` flag is an error, never a "may trade": the startup gate reads
/// the flag through here, so it must fail closed.
pub(super) fn read_closed_only(url: &str, response: &ClobResponse) -> Result<bool, ProbeError> {
    let body = success_body(url, response)?;
    decode_closed_only(body).ok_or_else(|| ProbeError::UnexpectedBody {
        url: url.to_owned(),
        body: response.excerpt(),
    })
}

pub(super) fn read_balance_allowance(
    url: &str,
    response: &ClobResponse,
) -> Result<BalanceAllowance, ProbeError> {
    let payload: BalanceAllowancePayload = decode(url, response)?;
    Ok(BalanceAllowance {
        balance: payload.balance,
        allowances: payload.allowances.into_iter().collect(),
    })
}

/// Reads BOTH documented shapes, so which one the venue actually uses comes back as an answer
/// rather than as a parse failure.
pub(super) fn read_open_orders(
    url: &str,
    response: &ClobResponse,
) -> Result<OpenOrders, ProbeError> {
    let body = success_body(url, response)?;
    if let Ok(wrapped) = serde_json::from_str::<WrappedOrders>(body) {
        return Ok(OpenOrders {
            count: wrapped.data.len(),
            shape: ResponseShape::Wrapped,
        });
    }
    let bare: Vec<serde_json::Value> =
        serde_json::from_str(body).map_err(|_| ProbeError::UnexpectedBody {
            url: url.to_owned(),
            body: response.excerpt(),
        })?;
    Ok(OpenOrders {
        count: bare.len(),
        shape: ResponseShape::BareArray,
    })
}

pub(super) fn success_body<'a>(
    url: &str,
    response: &'a ClobResponse,
) -> Result<&'a str, ProbeError> {
    if !response.is_success() {
        return Err(ProbeError::Status {
            url: url.to_owned(),
            status: response.status,
            body: response.excerpt(),
        });
    }
    Ok(&response.body)
}

#[derive(Deserialize)]
struct BalanceAllowancePayload {
    #[serde(default)]
    balance: String,
    #[serde(default)]
    allowances: std::collections::BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct WrappedOrders {
    data: Vec<serde_json::Value>,
}

pub(super) fn decode<T: serde::de::DeserializeOwned>(
    url: &str,
    response: &ClobResponse,
) -> Result<T, ProbeError> {
    let body = success_body(url, response)?;
    serde_json::from_str(body).map_err(|_| ProbeError::UnexpectedBody {
        url: url.to_owned(),
        body: response.excerpt(),
    })
}
