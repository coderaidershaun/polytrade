//! EIP-712 signing of a CLOB V2 order, including the ERC-7739 wrap a Deposit Wallet needs.
//!
//! Two traps live here and neither fails locally — both come back as a generic venue rejection.
//! The field list below IS the typehash, so reordering it invalidates every signature; and
//! `expiration` is deliberately absent, because V2 moved it to the wire body only.

use crate::time::TsUs;

use super::address::Address;
use super::eip712::{self, Eip712Error, Word, ZERO_WORD, keccak256};
use super::hex;
use super::key::SigningKey;

const CHAIN_ID: u64 = 137;

const DOMAIN_TYPE: &str =
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";

/// Both exchanges share the name; only `verifyingContract` differs.
const DOMAIN_NAME: &str = "Polymarket CTF Exchange";
const DOMAIN_VERSION: &str = "2";

const ORDER_TYPE: &str = concat!(
    "Order(uint256 salt,address maker,address signer,uint256 tokenId,",
    "uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,",
    "uint256 timestamp,bytes32 metadata,bytes32 builder)"
);

/// EIP-712 nested-type encoding: the wrapper's own type string with the referenced `Order` appended.
const TYPED_DATA_SIGN_TYPE: &str = concat!(
    "TypedDataSign(Order contents,string name,string version,uint256 chainId,",
    "address verifyingContract,bytes32 salt)",
    "Order(uint256 salt,address maker,address signer,uint256 tokenId,",
    "uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,",
    "uint256 timestamp,bytes32 metadata,bytes32 builder)"
);

const DEPOSIT_WALLET_NAME: &str = "DepositWallet";
const DEPOSIT_WALLET_VERSION: &str = "1";

const EXCHANGE_STANDARD: Address = Address::from_bytes([
    0xE1, 0x11, 0x18, 0x00, 0x00, 0xd2, 0x66, 0x3C, 0x00, 0x91, 0xe4, 0xf4, 0x00, 0x23, 0x75, 0x45,
    0xB8, 0x7B, 0x99, 0x6B,
]);

const EXCHANGE_NEG_RISK: Address = Address::from_bytes([
    0xe2, 0x22, 0x2d, 0x27, 0x9d, 0x74, 0x40, 0x50, 0xd2, 0x8e, 0x00, 0x52, 0x00, 0x10, 0x52, 0x00,
    0x00, 0x31, 0x0F, 0x59,
]);

/// The backend reads `salt` as a JSON number, so anything past `Number.MAX_SAFE_INTEGER` loses
/// precision there and the signature stops verifying.
const SALT_MASK: u64 = (1 << 53) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    /// `uint8` in the signed struct; the wire body spells it `"BUY"`/`"SELL"` instead.
    pub const fn code(self) -> u8 {
        match self {
            OrderSide::Buy => 0,
            OrderSide::Sell => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignatureType {
    Eoa,
    Proxy,
    GnosisSafe,
    DepositWallet,
}

impl SignatureType {
    pub const fn code(self) -> u8 {
        match self {
            SignatureType::Eoa => 0,
            SignatureType::Proxy => 1,
            SignatureType::GnosisSafe => 2,
            SignatureType::DepositWallet => 3,
        }
    }

    /// # Errors
    /// The code is not one of 0 to 3.
    pub fn from_code(code: u8) -> Result<Self, OrderSignError> {
        match code {
            0 => Ok(SignatureType::Eoa),
            1 => Ok(SignatureType::Proxy),
            2 => Ok(SignatureType::GnosisSafe),
            3 => Ok(SignatureType::DepositWallet),
            _ => Err(OrderSignError::UnknownSignatureType { code }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Exchange {
    Standard,
    NegRisk,
}

impl Exchange {
    pub const fn address(self) -> Address {
        match self {
            Exchange::Standard => EXCHANGE_STANDARD,
            Exchange::NegRisk => EXCHANGE_NEG_RISK,
        }
    }
}

/// Outcome token id: a uint256 decimal string around 77 digits, which fits no integer we own.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenId(String);

impl TokenId {
    /// # Errors
    /// The text is not a decimal number that fits 256 bits.
    pub fn parse(digits: &str) -> Result<Self, Eip712Error> {
        eip712::word_from_decimal(digits)?;
        Ok(Self(digits.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The eleven signed fields, in typehash order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedOrderFields {
    pub salt: u64,
    pub maker: Address,
    pub signer: Address,
    pub token_id: TokenId,
    pub maker_amount: u128,
    pub taker_amount: u128,
    pub side: OrderSide,
    pub signature_type: SignatureType,
    pub timestamp_millis: i64,
    pub metadata: Word,
    pub builder: Word,
}

/// Precomputed once per exchange — the separator is constant for the life of the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExchangeDomain {
    separator: Word,
}

impl ExchangeDomain {
    pub fn new(exchange: Exchange) -> Self {
        Self {
            separator: eip712::hash_words(&[
                keccak256(DOMAIN_TYPE.as_bytes()),
                keccak256(DOMAIN_NAME.as_bytes()),
                keccak256(DOMAIN_VERSION.as_bytes()),
                eip712::word_from_uint(u128::from(CHAIN_ID)),
                exchange.address().word(),
            ]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderSignature(String);

impl OrderSignature {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Uniqueness rides the signed millisecond timestamp; the salt only has to not collide within it,
/// which the microseconds already spread. Derived rather than drawn, so a replay of the same inputs
/// signs the same order and no RNG enters the stack.
pub fn salt(sent_ts_us: TsUs, client_order_id: u64) -> u64 {
    (sent_ts_us.micros() as u64 ^ client_order_id) & SALT_MASK
}

fn order_struct_hash(order: &SignedOrderFields) -> Result<Word, Eip712Error> {
    Ok(eip712::hash_words(&[
        keccak256(ORDER_TYPE.as_bytes()),
        eip712::word_from_uint(u128::from(order.salt)),
        order.maker.word(),
        order.signer.word(),
        eip712::word_from_decimal(order.token_id.as_str())?,
        eip712::word_from_uint(order.maker_amount),
        eip712::word_from_uint(order.taker_amount),
        eip712::word_from_uint(u128::from(order.side.code())),
        eip712::word_from_uint(u128::from(order.signature_type.code())),
        eip712::word_from_uint(order.timestamp_millis as u128),
        order.metadata,
        order.builder,
    ]))
}

/// # Errors
/// The order's token id is not a decimal number that fits 256 bits.
pub fn sign_order(
    key: &SigningKey,
    domain: &ExchangeDomain,
    order: &SignedOrderFields,
) -> Result<OrderSignature, OrderSignError> {
    let contents_hash =
        order_struct_hash(order).map_err(|source| OrderSignError::TokenId { source })?;
    match order.signature_type {
        SignatureType::DepositWallet => Ok(wrap_deposit_wallet(key, domain, order, contents_hash)),
        _ => {
            let digest = eip712::signing_digest(domain.separator, contents_hash);
            Ok(OrderSignature(key.sign_digest(digest).to_hex()))
        }
    }
}

/// ERC-7739 / EIP-1271: the wallet contract verifies the inner signature by rebuilding the digest
/// from the trailer, so every appended field is load-bearing rather than informational.
fn wrap_deposit_wallet(
    key: &SigningKey,
    domain: &ExchangeDomain,
    order: &SignedOrderFields,
    contents_hash: Word,
) -> OrderSignature {
    let typed_data_sign_hash = eip712::hash_words(&[
        keccak256(TYPED_DATA_SIGN_TYPE.as_bytes()),
        contents_hash,
        keccak256(DEPOSIT_WALLET_NAME.as_bytes()),
        keccak256(DEPOSIT_WALLET_VERSION.as_bytes()),
        eip712::word_from_uint(u128::from(CHAIN_ID)),
        order.signer.word(),
        ZERO_WORD,
    ]);
    let digest = eip712::signing_digest(domain.separator, typed_data_sign_hash);

    let mut wrapped = String::with_capacity(2 + 130 + 64 + 64 + ORDER_TYPE.len() * 2 + 4);
    wrapped.push_str("0x");
    hex::push_lower(&mut wrapped, key.sign_digest(digest).bytes());
    hex::push_lower(&mut wrapped, &domain.separator);
    hex::push_lower(&mut wrapped, &contents_hash);
    hex::push_lower(&mut wrapped, ORDER_TYPE.as_bytes());
    hex::push_lower(&mut wrapped, &(ORDER_TYPE.len() as u16).to_be_bytes());
    OrderSignature(wrapped)
}

#[derive(thiserror::Error, Debug)]
pub enum OrderSignError {
    #[error("order token id is not a uint256 decimal string")]
    TokenId {
        #[source]
        source: Eip712Error,
    },
    #[error("signature type {code} is not one of 0 eoa, 1 proxy, 2 safe, 3 deposit wallet")]
    UnknownSignatureType { code: u8 },
}
