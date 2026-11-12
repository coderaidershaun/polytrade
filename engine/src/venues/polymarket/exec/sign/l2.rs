//! L2 auth: HMAC-SHA256 over `timestamp + METHOD + path + body` on every private request.
//!
//! Three details here are each a silent auth failure rather than a loud one, so they are pinned by
//! vector rather than trusted: the preimage EXCLUDES the query string but INCLUDES path parameters;
//! both base64url hops carry padding and use the `-_` alphabet; and `POLY_ADDRESS` is EIP-55
//! checksummed at this layer where L1 sends it lowercase.

use std::fmt;

use ring::hmac;

use crate::secrets::Secret;

use super::address::Address;
use super::{ADDRESS_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER};

const BASE64URL_ALPHABET: [u8; 64] =
    *b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

const API_KEY_HEADER: &str = "POLY_API_KEY";
const PASSPHRASE_HEADER: &str = "POLY_PASSPHRASE";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpMethod {
    Get,
    Post,
    Delete,
}

impl HttpMethod {
    pub const fn as_str(self) -> &'static str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Delete => "DELETE",
        }
    }
}

/// `path` must carry no query string, and `body` must be the exact bytes that reach the socket —
/// serialize once, sign that buffer, send that buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestToSign<'a> {
    pub method: HttpMethod,
    pub path: &'a str,
    pub body: &'a str,
}

/// Derived at boot through L1 and never persisted.
pub struct ApiCredentials {
    api_key: String,
    secret: Secret,
    passphrase: Secret,
}

impl ApiCredentials {
    pub fn new(api_key: String, secret: Secret, passphrase: Secret) -> Self {
        Self {
            api_key,
            secret,
            passphrase,
        }
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// The user websocket authenticates with the RAW secret and passphrase — it is the one channel
    /// with no HMAC, so the two are readable rather than hidden behind the signer.
    pub fn secret(&self) -> &Secret {
        &self.secret
    }

    pub fn passphrase(&self) -> &Secret {
        &self.passphrase
    }
}

/// The api key alone cannot authenticate, but it names the account — redacted with the rest.
impl fmt::Debug for ApiCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiCredentials(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2Headers {
    address: String,
    api_key: String,
    passphrase: String,
    signature: String,
    timestamp: String,
}

impl L2Headers {
    pub fn entries(&self) -> [(&'static str, &str); 5] {
        [
            (ADDRESS_HEADER, &self.address),
            (API_KEY_HEADER, &self.api_key),
            (PASSPHRASE_HEADER, &self.passphrase),
            (SIGNATURE_HEADER, &self.signature),
            (TIMESTAMP_HEADER, &self.timestamp),
        ]
    }
}

pub struct RequestSigner {
    key: hmac::Key,
    api_key: String,
    passphrase: String,
    address: String,
}

impl RequestSigner {
    /// # Errors
    /// The API secret is not base64url text, or the passphrase is not UTF-8.
    pub fn new(credentials: &ApiCredentials, signer: Address) -> Result<Self, L2Error> {
        let mut secret = decode_base64url(&credentials.secret)?;
        let key = hmac::Key::new(hmac::HMAC_SHA256, &secret);
        secret.fill(0);
        std::hint::black_box(&secret);

        let passphrase = str::from_utf8(credentials.passphrase.expose_bytes())
            .map_err(|_| L2Error::PassphraseNotUtf8)?;
        Ok(Self {
            key,
            api_key: credentials.api_key.clone(),
            passphrase: passphrase.to_owned(),
            address: signer.to_checksum_hex(),
        })
    }

    pub fn headers(&self, request: &RequestToSign<'_>, timestamp_secs: i64) -> L2Headers {
        let preimage = format!(
            "{timestamp_secs}{}{}{}",
            request.method.as_str(),
            request.path,
            request.body
        );
        let tag = hmac::sign(&self.key, preimage.as_bytes());
        L2Headers {
            address: self.address.clone(),
            api_key: self.api_key.clone(),
            passphrase: self.passphrase.clone(),
            signature: encode_base64url(tag.as_ref()),
            timestamp: timestamp_secs.to_string(),
        }
    }
}

/// Hand-written: the derived form would render the passphrase and the hmac key.
impl fmt::Debug for RequestSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestSigner")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum L2Error {
    #[error("api secret holds bytes that are not utf-8")]
    SecretNotUtf8,
    #[error("api secret holds a character at position {position} that base64url does not use")]
    SecretNotBase64Url { position: usize },
    #[error("api passphrase holds bytes that are not utf-8")]
    PassphraseNotUtf8,
}

fn encode_base64url(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let triple = u32::from(chunk[0]) << 16
            | chunk.get(1).map_or(0, |byte| u32::from(*byte)) << 8
            | chunk.get(2).map_or(0, |byte| u32::from(*byte));
        out.push(symbol(triple >> 18));
        out.push(symbol(triple >> 12));
        out.push(if chunk.len() > 1 { symbol(triple >> 6) } else { '=' });
        out.push(if chunk.len() > 2 { symbol(triple) } else { '=' });
    }
    out
}

fn decode_base64url(secret: &Secret) -> Result<Vec<u8>, L2Error> {
    let text = str::from_utf8(secret.expose_bytes()).map_err(|_| L2Error::SecretNotUtf8)?;
    let body = text.trim().trim_end_matches('=');
    let mut bytes = Vec::with_capacity(body.len() * 3 / 4);
    let mut accumulator = 0_u32;
    let mut bits = 0_u32;
    for (position, character) in body.bytes().enumerate() {
        let value = BASE64URL_ALPHABET
            .iter()
            .position(|symbol| *symbol == character)
            .ok_or(L2Error::SecretNotBase64Url { position })?;
        accumulator = accumulator << 6 | value as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            bytes.push((accumulator >> bits) as u8);
        }
    }
    Ok(bytes)
}

fn symbol(sextet: u32) -> char {
    BASE64URL_ALPHABET[sextet as usize & 0x3f] as char
}
