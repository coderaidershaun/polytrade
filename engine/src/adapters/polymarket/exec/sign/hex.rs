//! Lowercase hex both directions. Addresses, digests, signatures and the wallet key all cross this
//! venue as hex strings, and the project deliberately carries no `hex` dependency.

const DIGITS: [u8; 16] = *b"0123456789abcdef";

pub fn push_lower(out: &mut String, bytes: &[u8]) {
    out.reserve(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
}

pub(super) fn encode_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    push_lower(&mut out, bytes);
    out
}

pub(super) fn prefixed_lower(bytes: &[u8]) -> String {
    let mut out = String::from("0x");
    push_lower(&mut out, bytes);
    out
}

/// Optional `0x` prefix accepted; count must be even.
///
/// # Errors
/// An odd number of characters, or a character that is not hex.
pub fn decode(text: &str) -> Result<Vec<u8>, HexError> {
    let body = text.strip_prefix("0x").unwrap_or(text);
    if !body.len().is_multiple_of(2) {
        return Err(HexError::OddLength { length: body.len() });
    }
    // Borrowed, never collected: this parses the wallet private key, and a heap copy of its hex text
    // would outlive the caller's scrub and be handed back to the allocator holding the key.
    let mut bytes = Vec::with_capacity(body.len() / 2);
    for (index, pair) in body.as_bytes().chunks_exact(2).enumerate() {
        let high = nibble(pair[0]).ok_or(HexError::NotHex {
            position: index * 2,
        })?;
        let low = nibble(pair[1]).ok_or(HexError::NotHex {
            position: index * 2 + 1,
        })?;
        bytes.push(high << 4 | low);
    }
    Ok(bytes)
}

/// The offending character is deliberately absent: `decode` parses the wallet private key, and an
/// error message is the one place a fragment of it would escape [`crate::secrets::Secret`].
#[derive(thiserror::Error, Debug)]
pub enum HexError {
    #[error("hex string has odd length {length} — every byte needs two characters")]
    OddLength { length: usize },
    #[error("hex string holds a non-hex character at position {position}")]
    NotHex { position: usize },
}

fn nibble(character: u8) -> Option<u8> {
    match character {
        b'0'..=b'9' => Some(character - b'0'),
        b'a'..=b'f' => Some(character - b'a' + 10),
        b'A'..=b'F' => Some(character - b'A' + 10),
        _ => None,
    }
}
