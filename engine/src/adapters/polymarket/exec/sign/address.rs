//! An Ethereum address in the two casings this venue demands: L1 sends `POLY_ADDRESS` lowercase, L2
//! sends the same address EIP-55 checksummed. There is deliberately no `Display` and no default
//! rendering — picking the wrong casing fails auth at the venue, not locally, so the call site
//! names which one it means.

use std::fmt;

use super::eip712::{Word, ZERO_WORD, keccak256};
use super::hex;

const ADDRESS_BYTES: usize = 20;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Address([u8; ADDRESS_BYTES]);

impl Address {
    pub const ZERO: Address = Address([0; ADDRESS_BYTES]);

    pub const fn from_bytes(bytes: [u8; ADDRESS_BYTES]) -> Self {
        Self(bytes)
    }

    /// # Errors
    /// The text is not hex, or it does not decode to 20 bytes.
    pub fn parse(text: &str) -> Result<Self, AddressError> {
        let decoded = hex::decode(text.trim()).map_err(|source| AddressError::NotHex { source })?;
        let length = decoded.len();
        let bytes = <[u8; ADDRESS_BYTES]>::try_from(decoded)
            .map_err(|_| AddressError::WrongLength { length })?;
        Ok(Self(bytes))
    }

    pub fn to_lowercase_hex(&self) -> String {
        hex::prefixed_lower(&self.0)
    }

    /// EIP-55: a hex letter uppercases when the matching nibble of `keccak256(lowercase_hex)` is ≥ 8.
    pub fn to_checksum_hex(&self) -> String {
        let lower = hex::encode_lower(&self.0);
        let hash = keccak256(lower.as_bytes());
        let mut out = String::with_capacity(2 + ADDRESS_BYTES * 2);
        out.push_str("0x");
        for (index, character) in lower.chars().enumerate() {
            let byte = hash[index / 2];
            let nibble = if index.is_multiple_of(2) { byte >> 4 } else { byte & 0x0f };
            out.push(if nibble >= 8 { character.to_ascii_uppercase() } else { character });
        }
        out
    }

    pub fn word(&self) -> Word {
        let mut word = ZERO_WORD;
        word[12..].copy_from_slice(&self.0);
        word
    }
}

/// Checksummed, because that is the form a reader compares against a block explorer.
impl fmt::Debug for Address {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_checksum_hex())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum AddressError {
    #[error("address is not hex")]
    NotHex {
        #[source]
        source: hex::HexError,
    },
    #[error("address holds {length} bytes — an ethereum address is 20")]
    WrongLength { length: usize },
}
