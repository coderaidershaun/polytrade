//! L1 auth: an EIP-712 `ClobAuth` signature by the wallet key. It authenticates exactly two calls —
//! create and derive API key — and those are the only way to obtain the L2 credentials every other
//! private request needs.

use super::address::Address;
use super::eip712::{self, Word, keccak256};
use super::key::SigningKey;
use super::{ADDRESS_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER};

const DOMAIN_TYPE: &str = "EIP712Domain(string name,string version,uint256 chainId)";
const DOMAIN_NAME: &str = "ClobAuthDomain";
const DOMAIN_VERSION: &str = "1";

const CLOB_AUTH_TYPE: &str =
    "ClobAuth(address address,string timestamp,uint256 nonce,string message)";

/// Verbatim, no trailing period — it is hashed into the signature, so a typo reads as a bad key.
const ATTESTATION: &str = "This message attests that I control the given wallet";

const NONCE_HEADER: &str = "POLY_NONCE";

/// A credential set per nonce; `0` unless an account deliberately runs several.
pub const DEFAULT_NONCE: u64 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClobAuthRequest {
    pub chain_id: u64,
    pub timestamp_secs: i64,
    pub nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClobAuthHeaders {
    address: String,
    signature: String,
    timestamp: String,
    nonce: String,
}

impl ClobAuthHeaders {
    /// Name/value pairs to apply verbatim, so no call site can rebuild a header name by hand.
    pub fn entries(&self) -> [(&'static str, &str); 4] {
        [
            (ADDRESS_HEADER, &self.address),
            (SIGNATURE_HEADER, &self.signature),
            (TIMESTAMP_HEADER, &self.timestamp),
            (NONCE_HEADER, &self.nonce),
        ]
    }
}

/// `POLY_ADDRESS` goes out LOWERCASE here, where the L2 headers send it checksummed.
pub fn clob_auth_headers(key: &SigningKey, request: &ClobAuthRequest) -> ClobAuthHeaders {
    let digest = signing_digest(key.address(), request);
    ClobAuthHeaders {
        address: key.address().to_lowercase_hex(),
        signature: key.sign_digest(digest).to_hex(),
        timestamp: request.timestamp_secs.to_string(),
        nonce: request.nonce.to_string(),
    }
}

/// The domain carries name/version/chainId only — no `verifyingContract`, no `salt`, and the type
/// string names exactly the fields present.
fn signing_digest(address: Address, request: &ClobAuthRequest) -> Word {
    let domain_separator = eip712::hash_words(&[
        keccak256(DOMAIN_TYPE.as_bytes()),
        keccak256(DOMAIN_NAME.as_bytes()),
        keccak256(DOMAIN_VERSION.as_bytes()),
        eip712::word_from_uint(u128::from(request.chain_id)),
    ]);
    let struct_hash = eip712::hash_words(&[
        keccak256(CLOB_AUTH_TYPE.as_bytes()),
        address.word(),
        keccak256(request.timestamp_secs.to_string().as_bytes()),
        eip712::word_from_uint(u128::from(request.nonce)),
        keccak256(ATTESTATION.as_bytes()),
    ]);
    eip712::signing_digest(domain_separator, struct_hash)
}
