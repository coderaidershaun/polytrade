//! Signing fitness: every signature Polymarket accepts, reproduced byte-for-byte against vectors
//! the official SDK minted.
//!
//! This is the suite's only defence against a whole class of failure that is invisible locally. A
//! wrong domain separator, a reordered struct field, standard base64 instead of base64url, or the
//! wrong address casing all produce a perfectly well-formed signature that the venue rejects with a
//! generic error — and the ERC-7739 deposit-wallet wrap has six chances to go wrong before anything
//! reaches a socket. Nothing here can be checked by reasoning about our own code, so all of it is
//! pinned against an oracle we did not write.

use polysim::adapters::polymarket::exec::sign::address::Address;
use polysim::adapters::polymarket::exec::sign::amount::{AmountRequest, order_amounts};
use polysim::adapters::polymarket::exec::sign::eip712::{word_from_decimal, word_from_uint};
use polysim::adapters::polymarket::exec::sign::key::SigningKey;
use polysim::adapters::polymarket::exec::sign::l1::{ClobAuthRequest, clob_auth_headers};
use polysim::adapters::polymarket::exec::sign::l2::{
    ApiCredentials, HttpMethod, RequestSigner, RequestToSign,
};
use polysim::adapters::polymarket::exec::sign::order::{
    Exchange, ExchangeDomain, OrderSide, SignatureType, SignedOrderFields, TokenId, sign_order,
};
use polysim::ids::{Price, Qty};
use polysim::secrets::Secret;
use proptest::prelude::*;
use serde_json::Value;

const VECTORS: &str = include_str!("../../fixtures/polymarket/sign_vectors.json");

fn vectors() -> Value {
    serde_json::from_str(VECTORS).expect("sign vector fixture is valid json")
}

pub(crate) fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field]
        .as_str()
        .unwrap_or_else(|| panic!("vector field {field} is a string"))
}

pub(crate) fn wallet_key(vector: &Value) -> SigningKey {
    SigningKey::from_secret(&Secret::new(text(vector, "private_key")))
        .expect("throwaway vector key parses")
}

/// The L1 `ClobAuth` vector: the signature that `derive-api-key` and every private call after it
/// depends on, plus the address the wallet key derives from the SAME vector — keccak over the
/// uncompressed public key minus its tag byte. A wrong address makes the venue attribute our
/// orders to nobody.
#[test]
fn clob_auth_signature_and_wallet_address_match_the_sdk_vector() {
    let vector = &vectors()["clob_auth"];
    let key = wallet_key(vector);

    let headers = clob_auth_headers(
        &key,
        &ClobAuthRequest {
            chain_id: vector["chain_id"].as_u64().expect("chain id"),
            timestamp_secs: vector["timestamp_secs"].as_i64().expect("timestamp"),
            nonce: vector["nonce"].as_u64().expect("nonce"),
        },
    );
    let sent: Vec<(&str, &str)> = headers.entries().to_vec();

    assert!(
        sent.contains(&("POLY_SIGNATURE", text(vector, "expected_signature"))),
        "l1 clob auth signature diverged from the sdk — derive-api-key would fail at the venue, \
         and every private call after it. sent {sent:?}"
    );
    assert!(
        sent.contains(&("POLY_ADDRESS", text(vector, "expected_address_lowercase"))),
        "l1 sends POLY_ADDRESS lowercase where l2 sends it checksummed; sent {sent:?}"
    );

    assert_eq!(
        key.address().to_lowercase_hex(),
        text(vector, "expected_address_lowercase"),
        "address derivation diverged; a wrong address makes the venue attribute our orders to \
         nobody"
    );
}

#[test]
fn l2_signature_matches_every_recorded_request() {
    let fixture = vectors();
    let requests = fixture["l2_requests"]
        .as_array()
        .expect("l2 request vectors");
    assert!(!requests.is_empty(), "the l2 vector set is empty");

    for vector in requests {
        let credentials = ApiCredentials::new(
            "f4f247b7-4ac7-ff29-a152-04fda0a8755a".to_owned(),
            Secret::new(text(vector, "secret")),
            Secret::new("passphrase"),
        );
        let signer = RequestSigner::new(&credentials, Address::ZERO).expect("secret is base64url");

        let headers = signer.headers(
            &RequestToSign {
                method: method(text(vector, "method")),
                path: text(vector, "path"),
                body: text(vector, "body"),
            },
            vector["timestamp_secs"].as_i64().expect("timestamp"),
        );

        let signature_match = headers
            .entries()
            .contains(&("POLY_SIGNATURE", text(vector, "expected_signature")));
        assert!(signature_match);
    }
}

#[test]
fn address_rendering_matches_the_eip55_reference_vectors() {
    let fixture = vectors();
    let addresses = fixture["checksum_addresses"]
        .as_array()
        .expect("checksum vectors");
    assert!(!addresses.is_empty(), "the checksum vector set is empty");

    for vector in addresses {
        let lowercase = text(vector, "lowercase");
        let address = Address::parse(lowercase).expect("reference address parses");
        assert_eq!(
            address.to_checksum_hex(),
            text(vector, "checksummed"),
            "l2 sends POLY_ADDRESS EIP-55 checksummed; a lowercase one fails auth"
        );
        assert_eq!(address.to_lowercase_hex(), lowercase);
    }
}

#[test]
fn order_signature_matches_the_sdk_for_every_wallet_type() {
    let fixture = vectors();
    let orders = fixture["orders"].as_array().expect("order vectors");
    assert_eq!(orders.len(), 3);

    for vector in orders {
        let key = wallet_key(vector);
        let signature_type = SignatureType::from_code(
            u8::try_from(vector["signature_type"].as_u64().expect("signature type"))
                .expect("signature type fits a byte"),
        )
        .expect("signature type is known");

        let fields = SignedOrderFields {
            salt: text(vector, "salt").parse().expect("salt is a u64"),
            maker: Address::parse(text(vector, "maker")).expect("maker parses"),
            signer: Address::parse(text(vector, "signer")).expect("signer parses"),
            token_id: TokenId::parse(text(vector, "token_id")).expect("token id parses"),
            maker_amount: text(vector, "maker_amount").parse().expect("maker amount"),
            taker_amount: text(vector, "taker_amount").parse().expect("taker amount"),
            side: side(vector["side"].as_u64().expect("side")),
            signature_type,
            timestamp_millis: text(vector, "timestamp_millis")
                .parse()
                .expect("timestamp millis"),
            metadata: word(text(vector, "metadata")),
            builder: word(text(vector, "builder")),
        };
        let domain = ExchangeDomain::new(match vector["neg_risk"].as_bool() {
            Some(true) => Exchange::NegRisk,
            Some(false) => Exchange::Standard,
            None => panic!("every vector states its neg risk flag"),
        });

        let signature = sign_order(&key, &domain, &fields).expect("order signs");
        assert_eq!(
            signature.as_str(),
            text(vector, "expected_signature"),
            "order signature diverged for {}",
            text(vector, "note")
        );
    }
}

/// `order_amounts` against the SDK vector, and the grid it refuses rather than rounds through.
#[test]
fn order_amounts_match_the_sdk_and_refuse_off_grid_prices() {
    let fixture = vectors();
    let vector = &fixture["orders"][0];

    // The vector was minted from a BUY of 10 shares at 0.52 on a 0.01-tick market, which is also
    // the venue's own worked example.
    let amounts = order_amounts(&AmountRequest {
        side: OrderSide::Buy,
        price: Price(52_000_000),
        size: Qty(1_000_000_000),
        tick: Price(1_000_000),
    })
    .expect("a well-formed order sizes");

    assert_eq!(amounts.maker.to_string(), text(vector, "maker_amount"));
    assert_eq!(amounts.taker.to_string(), text(vector, "taker_amount"));

    let sell = order_amounts(&AmountRequest {
        side: OrderSide::Sell,
        price: Price(52_000_000),
        size: Qty(1_000_000_000),
        tick: Price(1_000_000),
    })
    .expect("a well-formed order sizes");
    assert_eq!(
        (sell.maker, sell.taker),
        (amounts.taker, amounts.maker),
        "a sell pays shares and receives money — the same two amounts, swapped"
    );

    let off_grid = order_amounts(&AmountRequest {
        side: OrderSide::Buy,
        price: Price(52_500_000),
        size: Qty(1_000_000_000),
        tick: Price(1_000_000),
    });
    assert!(
        off_grid.is_err(),
        "0.525 is not a multiple of a 0.01 tick; rounding it here would sign one price and intend \
         another"
    );

    let at_par = order_amounts(&AmountRequest {
        side: OrderSide::Buy,
        price: Price(100_000_000),
        size: Qty(1_000_000_000),
        tick: Price(1_000_000),
    });
    assert!(at_par.is_err(), "a share cannot cost the full dollar");
}

proptest! {
    /// The venue's token ids are 77-digit uint256 decimals, so the schoolbook loop that turns one
    /// into an ABI word has no native type to fall back on. Where a value does fit `u128`, the two
    /// must agree — a carry bug would otherwise only ever show up as a rejected order.
    #[test]
    fn uint256_words_agree_with_native_arithmetic(value in any::<u128>()) {
        prop_assert_eq!(
            word_from_decimal(&value.to_string()).expect("a u128 decimal is a uint256"),
            word_from_uint(value)
        );
    }

    /// Both casings must parse back to the same 20 bytes, or the two auth layers would disagree
    /// about which account is signing.
    #[test]
    fn an_address_survives_both_renderings(bytes in any::<[u8; 20]>()) {
        let address = Address::from_bytes(bytes);
        prop_assert_eq!(Address::parse(&address.to_checksum_hex()).expect("checksummed parses"), address);
        prop_assert_eq!(Address::parse(&address.to_lowercase_hex()).expect("lowercase parses"), address);
    }
}

fn method(name: &str) -> HttpMethod {
    match name {
        "GET" => HttpMethod::Get,
        "POST" => HttpMethod::Post,
        "DELETE" => HttpMethod::Delete,
        other => panic!("vector names an unsupported http method {other}"),
    }
}

fn side(code: u64) -> OrderSide {
    match code {
        0 => OrderSide::Buy,
        1 => OrderSide::Sell,
        other => panic!("vector names an unknown side {other}"),
    }
}

fn word(hex: &str) -> [u8; 32] {
    let body = hex.strip_prefix("0x").unwrap_or(hex);
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte =
            u8::from_str_radix(&body[index * 2..index * 2 + 2], 16).expect("vector word is hex");
    }
    bytes
}
