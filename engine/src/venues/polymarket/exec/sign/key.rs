//! The wallet private key and the two things it produces: this account's own address, and 65-byte
//! `r‖s‖v` signatures over a 32-byte digest. Signing is RFC-6979 deterministic, so no RNG enters the
//! stack anywhere — the same digest always yields the same signature, which is what makes recorded
//! sign vectors pinnable at all.

use std::fmt;

use k256::ecdsa::SigningKey as EcdsaSigningKey;

use crate::secrets::Secret;

use super::address::Address;
use super::eip712::{Word, keccak256};
use super::hex;

const SECRET_KEY_BYTES: usize = 32;
const SIGNATURE_BYTES: usize = 65;

/// EIP-712 uses the pre-EIP-155 `v ∈ {27, 28}`, never the chain-tagged form.
const RECOVERY_ID_OFFSET: u8 = 27;

pub struct SigningKey {
    inner: EcdsaSigningKey,
    address: Address,
}

impl SigningKey {
    /// # Errors
    /// The secret is not hex text, does not decode to 32 bytes, or is not a valid secp256k1 key.
    pub fn from_secret(secret: &Secret) -> Result<Self, KeyError> {
        let text = str::from_utf8(secret.expose_bytes()).map_err(|_| KeyError::NotUtf8)?;
        let mut bytes = hex::decode(text.trim()).map_err(|source| KeyError::NotHex { source })?;
        let length = bytes.len();
        let parsed = EcdsaSigningKey::from_slice(&bytes);
        // The decoded key escaped `Secret`'s zeroing for the length of this call; put it back.
        bytes.fill(0);
        std::hint::black_box(&bytes);

        if length != SECRET_KEY_BYTES {
            return Err(KeyError::WrongLength { length });
        }
        let inner = parsed.map_err(|_| KeyError::NotOnCurve)?;
        let address = derive_address(&inner);
        Ok(Self { inner, address })
    }

    pub fn address(&self) -> Address {
        self.address
    }

    pub fn sign_digest(&self, digest: Word) -> Signature {
        let (signature, recovery) = self.inner.sign_prehash_recoverable(&digest);
        let mut bytes = [0_u8; SIGNATURE_BYTES];
        bytes[..SIGNATURE_BYTES - 1].copy_from_slice(&signature.to_bytes());
        bytes[SIGNATURE_BYTES - 1] = RECOVERY_ID_OFFSET + recovery.to_byte();
        Signature(bytes)
    }
}

/// Hand-written: the derived form would render the key material.
impl fmt::Debug for SigningKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SigningKey")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Signature([u8; SIGNATURE_BYTES]);

impl Signature {
    pub(super) fn bytes(&self) -> &[u8; SIGNATURE_BYTES] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::prefixed_lower(&self.0)
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum KeyError {
    #[error("wallet private key holds bytes that are not utf-8")]
    NotUtf8,
    #[error("wallet private key is not hex")]
    NotHex {
        #[source]
        source: hex::HexError,
    },
    #[error("wallet private key holds {length} bytes — a secp256k1 key is 32")]
    WrongLength { length: usize },
    #[error("wallet private key is not a valid secp256k1 scalar")]
    NotOnCurve,
}

/// The low 20 bytes of `keccak256` over the uncompressed public key, minus its `0x04` tag byte.
fn derive_address(key: &EcdsaSigningKey) -> Address {
    let point = key.verifying_key().to_sec1_point(false);
    let hash = keccak256(&point.as_bytes()[1..]);
    let mut bytes = [0_u8; 20];
    bytes.copy_from_slice(&hash[12..]);
    Address::from_bytes(bytes)
}
