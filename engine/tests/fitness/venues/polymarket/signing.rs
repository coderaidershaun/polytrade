//! Polymarket signing, held against vectors that the vendor SDK minted. A wrong separator, field
//! order, or address casing signs a well-formed order that the venue refuses with no cause.

use std::collections::BTreeSet;

use engine::msg::exec::OrderStyle;
use engine::msg::ids::{ClientOrderId, FIXED_SCALE, InstrumentId, Price, Qty, Side};
use engine::msg::time::LocalTs;
use engine::runtime::secrets::Secret;
use engine::venues::exec::ExecRequest;
use engine::venues::polymarket::exec::assess::{
    AccountStanding, ProvenSignatureType, WalletConfig, assess,
};
use engine::venues::polymarket::exec::codec::{
    EncodeContext, OrderIndex, OrderSigner, OrderSignerSetup, TokenBinding, TokenTable,
    encode_request,
};
use engine::venues::polymarket::exec::handle::{PolymarketPreflightError, WalletIdentity};
use engine::venues::polymarket::exec::sign::address::Address;
use engine::venues::polymarket::exec::sign::amount::{AmountRequest, order_amounts};
use engine::venues::polymarket::exec::sign::eip712::{word_from_decimal, word_from_uint};
use engine::venues::polymarket::exec::sign::key::SigningKey;
use engine::venues::polymarket::exec::sign::l1::{ClobAuthRequest, clob_auth_headers};
use engine::venues::polymarket::exec::sign::l2::{
    ApiCredentials, HttpMethod, RequestSigner, RequestToSign,
};
use engine::venues::polymarket::exec::sign::order::{
    Exchange, ExchangeDomain, OrderSide, SignatureType, SignedOrderFields, TokenId, sign_order,
};
use proptest::prelude::*;
use proptest::test_runner::TestRunner;
use serde_json::Value;

use super::gate::{LOCAL, recorded_facts};
use super::poly_exec_codec::API_KEY;

const SIGN_VECTORS: &str = include_str!("../../../../fixtures/polymarket/sign_vectors.json");
const INSTRUMENT: InstrumentId = InstrumentId(3);
/// The vectors were minted on a market of tick 0.01.
const TICK: Price = Price::from_mantissa(1_000_000);
/// The venue writes money and shares with 6 decimals.
const VENUE_UNIT: i128 = 1_000_000;

pub(super) fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field]
        .as_str()
        .unwrap_or_else(|| panic!("vector field {field} is a string"))
}

pub(super) fn wallet_key(vector: &Value) -> SigningKey {
    SigningKey::from_secret(&Secret::new(text(vector, "private_key")))
        .expect("throwaway vector key parses")
}

pub(super) fn sign_vector() -> Value {
    let vectors: Value =
        serde_json::from_str(SIGN_VECTORS).expect("sign vector fixture is valid json");
    vectors["orders"]
        .as_array()
        .expect("order vectors")
        .iter()
        .find(|vector| vector["signature_type"] == 2)
        .cloned()
        .expect("a gnosis safe order vector")
}

pub(super) fn vector_signature_type(vector: &Value) -> u8 {
    vector["signature_type"]
        .as_u64()
        .expect("vector signature type") as u8
}

/// The signer of a vector's wallet, built from what the start gate admitted for it.
pub(super) fn vector_signer(vector: &Value, api_key: &str) -> OrderSigner {
    let identity = admitted_identity(vector).expect("the start gate admits the vector's wallet");
    signer_of(vector, identity, api_key)
}

/// The start gate over the recorded answers and the vector's wallet, configured as an operator
/// configures it: the signer is the address of the key. A wallet type that the gate does not admit
/// gets no identity, so no signer.
fn admitted_identity(vector: &Value) -> Result<WalletIdentity, PolymarketPreflightError> {
    let key = wallet_key(vector);
    let wallet = WalletConfig {
        signer: key.address(),
        configured_signer: key.address().to_checksum_hex().into(),
        maker: text(vector, "maker").into(),
        signature_type: vector_signature_type(vector).to_string().into(),
    };
    assess(&recorded_facts(), &wallet, LOCAL)?.assess_account(AccountStanding::Open)
}

fn signer_of(vector: &Value, identity: WalletIdentity, api_key: &str) -> OrderSigner {
    OrderSigner::new(OrderSignerSetup {
        key: wallet_key(vector),
        maker: identity.maker,
        signer: identity.signer,
        signature_type: identity.signature_type,
        api_key: api_key.to_owned(),
    })
}

fn vectors() -> Value {
    serde_json::from_str(SIGN_VECTORS).expect("sign vector fixture is valid json")
}

/// The value of one header, so a message shows that header and no other.
fn header<'a>(entries: &[(&'static str, &'a str)], name: &str) -> Option<&'a str> {
    entries
        .iter()
        .find(|(entry, _)| *entry == name)
        .map(|(_, value)| *value)
}

/// FITNESS: each signature that Polymarket checks equals the one that the vendor SDK minted from the
/// same inputs: the first-level signature and the address it derives, the second-level signature of
/// each recorded request, the checksum casing of an address, the order signature of each wallet
/// type, and the amounts of an order with the grid it refuses. A wrong one is still well formed, so
/// only the venue would see it, as a refusal that names no cause. The two properties hold the
/// arithmetic and the address forms for each value, beyond the vectors.
#[test]
fn signatures_equal_the_vendor_vectors() {
    let fixture = vectors();
    first_level_signature_and_address(&fixture["clob_auth"]);
    second_level_signature_of_each_request(&fixture);
    checksum_casing_of_each_address(&fixture);
    order_signature_of_each_wallet_type(&fixture);
    order_amounts_and_the_grid(&fixture["orders"][0]);
    words_and_addresses_hold_for_each_value();
}

/// The signature that `derive-api-key` and each private call after it rest on, and the address
/// the wallet key derives: a wrong address makes the venue credit the orders to nobody.
fn first_level_signature_and_address(vector: &Value) {
    let key = wallet_key(vector);
    let headers = clob_auth_headers(
        &key,
        &ClobAuthRequest {
            chain_id: vector["chain_id"].as_u64().expect("chain id"),
            timestamp_secs: vector["timestamp_secs"].as_i64().expect("timestamp"),
            nonce: vector["nonce"].as_u64().expect("nonce"),
        },
    );
    let entries = headers.entries();
    assert_eq!(
        header(&entries, "POLY_SIGNATURE"),
        Some(text(vector, "expected_signature")),
        "vector clob_auth: the first-level signature diverged from the sdk, so derive-api-key and \
         each private call after it would fail at the venue (seen left, expected right)"
    );
    assert_eq!(
        header(&entries, "POLY_ADDRESS"),
        Some(text(vector, "expected_address_lowercase")),
        "vector clob_auth: the first level sends POLY_ADDRESS lowercase (seen left, expected right)"
    );
    assert_eq!(
        key.address().to_lowercase_hex(),
        text(vector, "expected_address_lowercase"),
        "vector clob_auth: the address the key derives diverged (seen left, expected right)"
    );
}

fn second_level_signature_of_each_request(fixture: &Value) {
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
        let name = vector["note"]
            .as_str()
            .unwrap_or_else(|| text(vector, "path"));
        assert_eq!(
            header(&headers.entries(), "POLY_SIGNATURE"),
            Some(text(vector, "expected_signature")),
            "l2 vector {name}: the second-level signature diverged from the sdk (seen left, \
             expected right)"
        );
    }
}

/// The second level sends POLY_ADDRESS checksummed, and a lowercase one fails its auth.
fn checksum_casing_of_each_address(fixture: &Value) {
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
            "address vector {lowercase}: the checksum casing diverged (seen left, expected right)"
        );
        assert_eq!(address.to_lowercase_hex(), lowercase);
    }
}

fn order_signature_of_each_wallet_type(fixture: &Value) {
    let orders = fixture["orders"].as_array().expect("order vectors");
    assert_eq!(orders.len(), 3);

    for vector in orders {
        let key = wallet_key(vector);
        let signature_type = SignatureType::from_code(vector_signature_type(vector))
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
        let signature = sign_order(&key, &domain(vector), &fields).expect("order signs");
        assert_eq!(
            signature.as_str(),
            text(vector, "expected_signature"),
            "order vector {}: the order signature diverged from the sdk (seen left, expected right)",
            text(vector, "note")
        );
    }
}

/// The vector was minted from a buy of 10 shares at 0.52 on a market of tick 0.01, which is also
/// the venue's own worked example.
fn order_amounts_and_the_grid(vector: &Value) {
    let note = text(vector, "note");
    let amounts = order_amounts(&AmountRequest {
        side: OrderSide::Buy,
        price: Price::from_mantissa(52_000_000),
        size: Qty::from_mantissa(1_000_000_000),
        tick: Price::from_mantissa(1_000_000),
    })
    .expect("a well-formed order sizes");
    assert_eq!(
        amounts.maker.to_string(),
        text(vector, "maker_amount"),
        "order vector {note}: the maker amount diverged (seen left, expected right)"
    );
    assert_eq!(
        amounts.taker.to_string(),
        text(vector, "taker_amount"),
        "order vector {note}: the taker amount diverged (seen left, expected right)"
    );

    let sell = order_amounts(&AmountRequest {
        side: OrderSide::Sell,
        price: Price::from_mantissa(52_000_000),
        size: Qty::from_mantissa(1_000_000_000),
        tick: Price::from_mantissa(1_000_000),
    })
    .expect("a well-formed order sizes");
    assert_eq!(
        (sell.maker, sell.taker),
        (amounts.taker, amounts.maker),
        "a sell pays shares and receives money: the same two amounts, swapped"
    );

    let off_grid = order_amounts(&AmountRequest {
        side: OrderSide::Buy,
        price: Price::from_mantissa(52_500_000),
        size: Qty::from_mantissa(1_000_000_000),
        tick: Price::from_mantissa(1_000_000),
    });
    assert!(
        off_grid.is_err(),
        "0.525 is not a multiple of a 0.01 tick; rounding it here would sign one price and intend \
         another"
    );
    let at_par = order_amounts(&AmountRequest {
        side: OrderSide::Buy,
        price: Price::from_mantissa(100_000_000),
        size: Qty::from_mantissa(1_000_000_000),
        tick: Price::from_mantissa(1_000_000),
    });
    assert!(at_par.is_err(), "a share cannot cost the full dollar");
}

/// A token id is a 77-digit uint256 decimal with no native type to check its word against, so
/// where a value fits `u128` the two must agree. Both casings of an address must parse back to the
/// same 20 bytes, or the two auth levels would disagree about who signs.
fn words_and_addresses_hold_for_each_value() {
    let mut runner = TestRunner::default();
    runner
        .run(&any::<u128>(), |value| {
            prop_assert_eq!(
                word_from_decimal(&value.to_string()).expect("a u128 decimal is a uint256"),
                word_from_uint(value)
            );
            Ok(())
        })
        .unwrap_or_else(|failure| {
            panic!("a uint256 word differs from native arithmetic: {failure}")
        });
    runner
        .run(&any::<[u8; 20]>(), |bytes| {
            let address = Address::from_bytes(bytes);
            prop_assert_eq!(
                Address::parse(&address.to_checksum_hex()).expect("checksummed parses"),
                address
            );
            prop_assert_eq!(
                Address::parse(&address.to_lowercase_hex()).expect("lowercase parses"),
                address
            );
            Ok(())
        })
        .unwrap_or_else(|failure| panic!("an address does not survive both renderings: {failure}"));
}

/// FITNESS: each order vector of a wallet type that the start gate admits runs through the
/// encoder, with the signer built from what the gate admitted, and gives the place body of the
/// vendor SDK byte for byte: the signature, both addresses, the amounts, and each field around them.
/// A vector of a type that the gate does not admit gets no signer, and each type that it admits has
/// one vector or more.
#[test]
fn each_order_vector_equals_sdk_body() {
    let fixture = vectors();
    let orders = fixture["orders"].as_array().expect("order vectors");
    let mut encoded = BTreeSet::new();
    for vector in orders {
        let note = text(vector, "note");
        let code = vector_signature_type(vector);
        let identity = match admitted_identity(vector) {
            Ok(identity) => identity,
            Err(refusal) => {
                assert!(
                    matches!(refusal, PolymarketPreflightError::UnprovenWalletType { code: found } if found == code),
                    "order vector {note}: the gate refuses wallet type {code} for another reason: \
                     {refusal}"
                );
                continue;
            }
        };
        let signer = signer_of(vector, identity, API_KEY);
        let seen = place_body(vector, &signer);
        assert_eq!(
            seen,
            sdk_body(vector),
            "order vector {note}: the place body diverged from the sdk (seen left, expected right)"
        );
        encoded.insert(code);
    }

    let admitted: BTreeSet<u8> = (0..=3)
        .filter(|code| {
            let mut wallet = orders[0].clone();
            wallet["signature_type"] = (*code).into();
            admitted_identity(&wallet).is_ok()
        })
        .collect();
    assert!(!admitted.is_empty(), "the gate admits no wallet type");
    assert_eq!(
        encoded, admitted,
        "each wallet type that the gate admits has one order vector or more (encoded left, \
         admitted right)"
    );
}

/// FITNESS (type test): the signer takes only a wallet type that the start gate proved. A
/// `ProvenSignatureType` comes only from the gate, so a signer with another type does not build;
/// this half holds that the gate proves each type that a live order has proven, and no other one.
#[test]
fn signer_takes_only_proven_wallet_type() {
    let vector = sign_vector();
    for (code, expected) in [
        (0, Some(SignatureType::Eoa)),
        (1, None),
        (2, Some(SignatureType::GnosisSafe)),
        (3, None),
    ] {
        let mut wallet = vector.clone();
        wallet["signature_type"] = code.into();
        match (admitted_identity(&wallet), expected) {
            (Ok(identity), Some(expected)) => {
                let proven: ProvenSignatureType = identity.signature_type;
                assert_eq!(proven.code(), code, "wallet type {code}");
                assert_eq!(SignatureType::from(proven), expected, "wallet type {code}");
                signer_of(&wallet, identity, API_KEY);
            }
            (Err(refusal), None) => assert!(
                matches!(refusal, PolymarketPreflightError::UnprovenWalletType { code: found } if found == code),
                "wallet type {code}: {refusal}"
            ),
            (Ok(identity), None) => panic!("wallet type {code} is proven: {identity:?}"),
            (Err(refusal), Some(_)) => panic!("wallet type {code} is refused: {refusal}"),
        }
    }
}

/// The place body that the encoder writes for the vector's order. The salt comes from the send
/// stamp and the client id, so the client id that lands on the vector's salt is solved for.
fn place_body(vector: &Value, signer: &OrderSigner) -> String {
    let mut tokens = TokenTable::with_retired_capacity(2);
    tokens.bind(TokenBinding {
        instrument: INSTRUMENT,
        token_id: text(vector, "token_id").into(),
        tick: TICK,
        is_neg_risk: vector["neg_risk"].as_bool().expect("vector neg_risk"),
    });
    let orders = OrderIndex::with_capacity(4);
    let timestamp_millis: i64 = text(vector, "timestamp_millis")
        .parse()
        .expect("vector timestamp");
    let sent_ts_us = LocalTs::from_micros(timestamp_millis * 1_000);
    let salt: u64 = text(vector, "salt").parse().expect("vector salt");
    let (side, price, qty) = order_of(vector);
    let context = EncodeContext {
        tokens: &tokens,
        orders: &orders,
        signer,
        sent_ts_us,
    };
    encode_request(
        ExecRequest::Place {
            instrument: INSTRUMENT,
            client_id: ClientOrderId(sent_ts_us.micros() as u64 ^ salt),
            side,
            price,
            qty,
            style: OrderStyle::PostOnly,
        },
        &context,
    )
    .expect("the vector's order is on the venue's grid")
    .body
}

/// The side, price and size that the vector's amounts state: a buy pays money for shares, and a
/// sell pays shares for money.
fn order_of(vector: &Value) -> (Side, Price, Qty) {
    let maker: i128 = text(vector, "maker_amount").parse().expect("maker amount");
    let taker: i128 = text(vector, "taker_amount").parse().expect("taker amount");
    let (side, money, shares) = match side(vector["side"].as_u64().expect("side")) {
        OrderSide::Buy => (Side::Buy, maker, taker),
        OrderSide::Sell => (Side::Sell, taker, maker),
    };
    let scale = i128::from(FIXED_SCALE);
    assert_eq!(
        money * scale % shares,
        0,
        "order vector {}: the price is not exact at the engine scale",
        text(vector, "note")
    );
    let price = i64::try_from(money * scale / shares).expect("a price fits the mantissa");
    let size = i64::try_from(shares * scale / VENUE_UNIT).expect("a size fits the mantissa");
    (side, Price::from_mantissa(price), Qty::from_mantissa(size))
}

/// The body of the vendor SDK for the vector's order, resting post-only.
fn sdk_body(vector: &Value) -> String {
    let side = match side(vector["side"].as_u64().expect("side")) {
        OrderSide::Buy => "BUY",
        OrderSide::Sell => "SELL",
    };
    format!(
        concat!(
            r#"{{"order":{{"salt":{salt},"maker":"{maker}","signer":"{signer}","#,
            r#""tokenId":"{token}","makerAmount":"{maker_amount}","takerAmount":"{taker_amount}","#,
            r#""side":"{side}","expiration":"0","timestamp":"{timestamp}","#,
            r#""signatureType":{signature_type},"#,
            r#""signature":"{signature}","#,
            r#""metadata":"0x0000000000000000000000000000000000000000000000000000000000000000","#,
            r#""builder":"0x0000000000000000000000000000000000000000000000000000000000000000"}},"#,
            r#""owner":"{api_key}","orderType":"GTC","deferExec":false,"postOnly":true}}"#
        ),
        salt = text(vector, "salt"),
        maker = text(vector, "maker"),
        signer = text(vector, "signer"),
        token = text(vector, "token_id"),
        maker_amount = text(vector, "maker_amount"),
        taker_amount = text(vector, "taker_amount"),
        side = side,
        timestamp = text(vector, "timestamp_millis"),
        signature_type = vector_signature_type(vector),
        signature = text(vector, "expected_signature"),
        api_key = API_KEY,
    )
}

fn domain(vector: &Value) -> ExchangeDomain {
    ExchangeDomain::new(match vector["neg_risk"].as_bool() {
        Some(true) => Exchange::NegRisk,
        Some(false) => Exchange::Standard,
        None => panic!("every vector states its neg risk flag"),
    })
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
