//! Signs every private Binance call with HMAC-SHA256. Parameters become the exact signed bytes,
//! which REST sends in the URL and the WebSocket API sends as JSON, so nothing private reaches the
//! wire unsigned.

use std::fmt;

use ring::hmac;

use crate::secrets::Secret;
use crate::time::{DurationUs, TsUs};

const TIMESTAMP_PARAM: &str = "timestamp";
const SIGNATURE_PARAM: &str = "signature";

const RECV_WINDOW_PARAM: &str = "recvWindow";
const RECV_WINDOW_MAX_MILLIS: u32 = 60_000;

/// How old a request's timestamp may be when Binance receives it. A wider window keeps a captured
/// request replayable for longer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecvWindow(u32);

impl RecvWindow {
    pub const DEFAULT: RecvWindow = RecvWindow(5_000);

    /// # Errors
    /// The window is outside 1..=60000 milliseconds.
    pub fn from_millis(millis: u32) -> Result<Self, SignError> {
        if millis == 0 || millis > RECV_WINDOW_MAX_MILLIS {
            return Err(SignError::RecvWindowOutOfRange { millis });
        }
        Ok(Self(millis))
    }

    pub const fn millis(self) -> u32 {
        self.0
    }
}

/// A request timestamp in milliseconds since the epoch. Only [`ClockOffset::stamp`] mints one, so
/// no request is ever stamped with the uncorrected local clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestStamp(i64);

impl RequestStamp {
    pub const fn millis(self) -> i64 {
        self.0
    }
}

/// The venue's clock minus ours, learned from `GET /api/v3/time`. Without it, a host whose clock
/// runs behind sends requests the venue rejects as outside the receive window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClockOffset {
    correction: DurationUs,
}

impl ClockOffset {
    pub const NONE: ClockOffset = ClockOffset {
        correction: DurationUs::ZERO,
    };

    /// `observed_at` must be when the response arrived. Taken at receipt, the offset lags, which is
    /// safe; taken at send, it would stamp requests ahead of the venue's clock.
    pub fn learn(server_time: TsUs, observed_at: TsUs) -> Self {
        Self {
            correction: server_time.diff(observed_at),
        }
    }

    pub const fn correction(self) -> DurationUs {
        self.correction
    }

    /// Takes the time as a parameter rather than reading a clock, so a replay stamps identically.
    /// Rounds down, which is the safe side.
    pub fn stamp(self, local_now: TsUs) -> RequestStamp {
        RequestStamp((local_now + self.correction).micros().div_euclid(1_000))
    }
}

/// Unsigned parameters. Only [`RequestSigner::sign`] turns them into something that can be sent.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RequestParams {
    entries: Vec<(&'static str, String)>,
}

impl RequestParams {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces a value already set rather than adding a second, because the venue rejects a
    /// duplicated parameter.
    #[must_use]
    pub fn set(mut self, name: &'static str, value: impl Into<String>) -> Self {
        let value = value.into();
        match self.entries.iter_mut().find(|(held, _)| *held == name) {
            Some(entry) => entry.1 = value,
            None => self.entries.push((name, value)),
        }
        self
    }

    #[must_use]
    pub fn set_recv_window(self, window: RecvWindow) -> Self {
        self.set(RECV_WINDOW_PARAM, window.millis().to_string())
    }
}

/// An HMAC-SHA256 tag in lowercase hex.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Signature(String);

impl Signature {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Parameters together with their signature: the only form of a private request that can be sent.
#[derive(Clone, PartialEq, Eq)]
pub struct SignedRequest {
    signed_params: Vec<(&'static str, String)>,
    signature: Signature,
    query: String,
}

impl SignedRequest {
    /// These exact bytes, signature included, go into the URL VERBATIM. Never pass them through
    /// reqwest's `query()`, which would rebuild the string the signature covers.
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Sorted by name, without the signature. A WebSocket request adds the signature as its own
    /// JSON key, and the venue verifies the sorted payload.
    pub fn signed_params(&self) -> &[(&'static str, String)] {
        &self.signed_params
    }

    pub fn signature(&self) -> &Signature {
        &self.signature
    }
}

/// Hand-written because the derived form would leak: the query can be replayed, and on the
/// WebSocket API the api key travels in plain text as a parameter. Only parameter names are shown.
impl fmt::Debug for SignedRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self.signed_params.iter().map(|(name, _)| *name).collect();
        formatter
            .debug_struct("SignedRequest")
            .field("signed_params", &names)
            .field("signature", &"<redacted>")
            .field("query", &"<redacted>")
            .finish()
    }
}

pub struct RequestSigner {
    key: hmac::Key,
}

impl RequestSigner {
    pub fn new(api_secret: &Secret) -> Self {
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, api_secret.expose_bytes()),
        }
    }

    /// The raw HMAC over a payload exactly as given. [`RequestSigner::sign`] is what a request goes
    /// through, because that is where the timestamp and recvWindow gate lives; this one is public so
    /// the venue's own documented payload/signature pair can be pinned verbatim, which is the only
    /// check on this code that does not grade itself.
    pub fn sign_payload(&self, payload: &str) -> Signature {
        let tag = hmac::sign(&self.key, payload.as_bytes());
        Signature(hex_lower(tag.as_ref()))
    }

    /// Sorts by name, stamps and signs. Any timestamp or signature the caller set is dropped.
    ///
    /// # Errors
    /// A parameter holds a character a URL query cannot carry unescaped. Escaping it would make the
    /// bytes sent differ from the bytes signed.
    pub fn sign(
        &self,
        params: RequestParams,
        stamp: RequestStamp,
    ) -> Result<SignedRequest, SignError> {
        let mut signed_params = params.entries;
        signed_params.retain(|(name, _)| *name != TIMESTAMP_PARAM && *name != SIGNATURE_PARAM);
        signed_params.push((TIMESTAMP_PARAM, stamp.millis().to_string()));
        signed_params.sort_by_key(|(name, _)| *name);

        for (name, value) in &signed_params {
            check_query_safe(name, value)?;
        }

        let payload = join_params(&signed_params);
        let signature = self.sign_payload(&payload);
        let query = format!("{payload}&{SIGNATURE_PARAM}={}", signature.as_str());
        Ok(SignedRequest {
            signed_params,
            signature,
            query,
        })
    }
}

/// Hand-written so the key material inside the HMAC key is never rendered.
impl fmt::Debug for RequestSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RequestSigner(<hmac-sha256>)")
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SignError {
    #[error(
        "param {name} holds a character at byte {position} of `name=value` that a url query cannot carry unescaped — binance private params are unreserved characters plus `:` and `/`"
    )]
    NotUrlSafe { name: String, position: usize },
    #[error("recvWindow {millis}ms is out of range — binance accepts 1..=60000")]
    RecvWindowOutOfRange { millis: u32 },
}

/// Binance's private parameters are symbols, enum words and decimals, which a URL carries
/// unescaped. Anything else is refused rather than escaped.
fn check_query_safe(name: &str, value: &str) -> Result<(), SignError> {
    let value_offset = name.len() + 1;
    let offender = name
        .char_indices()
        .chain(
            value
                .char_indices()
                .map(|(position, character)| (position + value_offset, character)),
        )
        .find(|(_, character)| !is_query_safe(*character));
    match offender {
        Some((position, _)) => Err(SignError::NotUrlSafe {
            name: name.to_owned(),
            position,
        }),
        None => Ok(()),
    }
}

fn is_query_safe(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_' | '~' | ':' | '/')
}

fn join_params(entries: &[(&'static str, String)]) -> String {
    entries
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
