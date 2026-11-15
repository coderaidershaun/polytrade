//! Polymarket signing, held against vectors that the vendor SDK minted. A wrong separator, field
//! order, or address casing signs a well-formed order that the venue refuses with no cause.

use engine::runtime::secrets::Secret;
use engine::venues::polymarket::exec::codec::{OrderSigner, OrderSignerSetup};
use engine::venues::polymarket::exec::sign::address::Address;
use engine::venues::polymarket::exec::sign::key::SigningKey;
use engine::venues::polymarket::exec::sign::order::SignatureType;
use serde_json::Value;

const SIGN_VECTORS: &str = include_str!("../../../../fixtures/polymarket/sign_vectors.json");

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

pub(super) fn vector_signer(vector: &Value, api_key: &str) -> OrderSigner {
    OrderSigner::new(OrderSignerSetup {
        key: wallet_key(vector),
        maker: Address::parse(text(vector, "maker")).expect("vector maker parses"),
        signer: Address::parse(text(vector, "signer")).expect("vector signer parses"),
        signature_type: SignatureType::from_code(vector_signature_type(vector))
            .expect("vector names a wallet type this engine knows"),
        api_key: api_key.to_owned(),
    })
}
