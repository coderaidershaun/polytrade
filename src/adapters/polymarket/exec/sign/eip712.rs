//! EIP-712 hashing for the fixed set of structs this venue signs. Hand-rolled rather than reached
//! for from `alloy`, which is an enormous dependency tree for two struct hashes: every field
//! Polymarket signs is a static 32-byte ABI word, so `abi.encode` is concatenation and the whole
//! scheme reduces to two struct hashes and a digest.

use tiny_keccak::{Hasher, Keccak};

pub type Word = [u8; 32];

pub const ZERO_WORD: Word = [0; 32];

pub fn keccak256(bytes: &[u8]) -> Word {
    let mut hasher = Keccak::v256();
    let mut digest = ZERO_WORD;
    hasher.update(bytes);
    hasher.finalize(&mut digest);
    digest
}

/// `keccak256(abi.encode(..))` over static-type fields, where that encoding IS the concatenation.
pub fn hash_words(words: &[Word]) -> Word {
    keccak256(&words.concat())
}

pub fn word_from_uint(value: u128) -> Word {
    let mut word = ZERO_WORD;
    word[16..].copy_from_slice(&value.to_be_bytes());
    word
}

/// Token ids are uint256 decimal strings ~77 digits, which exceed `u128`.
///
/// # Errors
/// The text is empty, holds a character that is not a digit, or does not fit 256 bits.
pub fn word_from_decimal(digits: &str) -> Result<Word, Eip712Error> {
    if digits.is_empty() {
        return Err(Eip712Error::EmptyDecimal);
    }
    let mut word = ZERO_WORD;
    for (position, character) in digits.bytes().enumerate() {
        let digit = character
            .checked_sub(b'0')
            .filter(|digit| *digit < 10)
            .ok_or(Eip712Error::NotDecimal { position })?;
        multiply_ten_add(&mut word, digit).ok_or_else(|| Eip712Error::DecimalOverflow {
            digits: digits.to_owned(),
        })?;
    }
    Ok(word)
}

pub fn signing_digest(domain_separator: Word, struct_hash: Word) -> Word {
    let mut preimage = [0_u8; 66];
    preimage[0] = 0x19;
    preimage[1] = 0x01;
    preimage[2..34].copy_from_slice(&domain_separator);
    preimage[34..].copy_from_slice(&struct_hash);
    keccak256(&preimage)
}

#[derive(thiserror::Error, Debug)]
pub enum Eip712Error {
    #[error("uint256 decimal string is empty")]
    EmptyDecimal,
    #[error("uint256 decimal string holds a non-digit at position {position}")]
    NotDecimal { position: usize },
    #[error("uint256 decimal string {digits} does not fit 256 bits")]
    DecimalOverflow { digits: String },
}

/// `None` on carry out of the top byte, which is the value leaving 256 bits. Big-endian schoolbook:
/// each byte holds at most 255, so `255 * 10 + carry` stays inside `u16` and the carry itself never
/// exceeds 9.
fn multiply_ten_add(word: &mut Word, digit: u8) -> Option<()> {
    let mut carry = u16::from(digit);
    for byte in word.iter_mut().rev() {
        let value = u16::from(*byte) * 10 + carry;
        *byte = value as u8;
        carry = value >> 8;
    }
    (carry == 0).then_some(())
}
