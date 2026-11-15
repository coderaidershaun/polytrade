//! The start gate of Polymarket decides from values passed in, so the order of its refusals holds
//! over recorded venue answers with no key of an account, no network, and no `.env`.

use engine::msg::time::{DurationUs, LocalTs, VenueTs};
use engine::runtime::secrets::Secret;
use engine::venues::polymarket::exec::assess::{
    AccountStanding, AdmittedWallet, ProbeFacts, WalletConfig, assess,
};
use engine::venues::polymarket::exec::codec::{
    PROTOCOL_VERSION, decode_closed_only, decode_protocol_version,
};
use engine::venues::polymarket::exec::handle::PolymarketPreflightError;
use engine::venues::polymarket::exec::probe::Geoblock;
use engine::venues::polymarket::exec::sign::key::SigningKey;
use engine::venues::polymarket::exec::sign::order::SignatureType;

const VERSION_ANSWER: &str = include_str!("../../../../fixtures/polymarket/exec/version.json");
const OPEN_HOST: &str = r#"{"blocked":false,"ip":"192.0.2.7","country":"GB","region":"ENG"}"#;
const BLOCKED_HOST: &str = r#"{"blocked":true,"ip":"192.0.2.7","country":"US","region":"NY"}"#;
const SERVER_SECS: i64 = 1_700_000_000;
/// Read 0.4 s after the venue's own clock answered.
pub(super) const LOCAL: LocalTs = LocalTs::from_micros(SERVER_SECS * 1_000_000 + 400_000);

/// A throwaway key of this test: the secp256k1 scalar `scalar`, which holds no funds anywhere.
fn test_key(scalar: u8) -> SigningKey {
    SigningKey::from_secret(&Secret::new(&format!("0x{scalar:064x}")))
        .expect("a small scalar is a valid secp256k1 key")
}

fn geoblock(body: &str) -> Geoblock {
    serde_json::from_str(body).expect("a geoblock answer of the venue's shape")
}

/// The answers of the public reads: the recorded `/version`, the venue clock, an open host.
pub(super) fn recorded_facts() -> ProbeFacts {
    let version = decode_protocol_version(VERSION_ANSWER).expect("the recorded answer names one");
    ProbeFacts {
        protocol_version: version.to_string().into(),
        server_ts_us: VenueTs::from_micros(0) + DurationUs::from_secs(SERVER_SECS),
        geoblock: geoblock(OPEN_HOST),
    }
}

/// A plain wallet whose key signs for itself, configured as `tools/poly-probe` writes it.
fn eoa_wallet(key: &SigningKey) -> WalletConfig {
    let address = key.address().to_checksum_hex();
    WalletConfig {
        signer: key.address(),
        configured_signer: address.to_lowercase().into(),
        maker: address.into(),
        signature_type: "0".into(),
    }
}

fn refused(facts: &ProbeFacts, wallet: &WalletConfig, local: LocalTs) -> PolymarketPreflightError {
    assess(facts, wallet, local).expect_err("this gate must refuse")
}

fn admitted(facts: &ProbeFacts, wallet: &WalletConfig) -> AdmittedWallet {
    assess(facts, wallet, LOCAL).expect("this gate must admit the wallet")
}

/// CONTRACT: the start gate of Polymarket refuses in one fixed order, the wallet first, then the
/// protocol version, then the account after its signed read, and a geoblocked host is only
/// reported. With several faults at once the refusal names the first of that order.
#[test]
fn preflight_gates_run_in_order_over_recorded_answers() {
    // The recorded answer of `/version`, and a bare number, read as the version this engine signs.
    assert_eq!(decode_protocol_version(VERSION_ANSWER), Some(2));
    assert_eq!(
        decode_protocol_version(VERSION_ANSWER),
        Some(PROTOCOL_VERSION)
    );
    assert_eq!(decode_protocol_version("2"), Some(PROTOCOL_VERSION));

    let key = test_key(1);
    let other = test_key(2).address();
    let facts = recorded_facts();
    let wallet = eoa_wallet(&key);

    // A configured signer that the key does not derive.
    let foreign = WalletConfig {
        configured_signer: other.to_checksum_hex().into(),
        ..wallet.clone()
    };
    let mismatch = refused(&facts, &foreign, LOCAL);
    assert!(
        matches!(&mismatch, PolymarketPreflightError::SignerMismatch { configured, derived }
            if **configured == other.to_checksum_hex() && **derived == key.address().to_checksum_hex()),
        "{mismatch}"
    );

    // A maker that is not an address.
    let no_maker = WalletConfig {
        maker: "not-an-address".into(),
        ..wallet.clone()
    };
    let maker = refused(&facts, &no_maker, LOCAL);
    assert!(
        matches!(&maker, PolymarketPreflightError::WalletAddress { value, .. } if **value == *"not-an-address"),
        "{maker}"
    );

    // A wallet type with no live proof, and one that no wallet has.
    for code in [1_u8, 3] {
        let unproven = WalletConfig {
            signature_type: code.to_string().into(),
            ..wallet.clone()
        };
        let refusal = refused(&facts, &unproven, LOCAL);
        assert!(
            matches!(refusal, PolymarketPreflightError::UnprovenWalletType { code: found } if found == code),
            "type {code}: {refusal}"
        );
    }
    let unknown = WalletConfig {
        signature_type: "9".into(),
        ..wallet.clone()
    };
    let type_9 = refused(&facts, &unknown, LOCAL);
    assert!(
        matches!(&type_9, PolymarketPreflightError::WalletSignatureType { value } if **value == *"9"),
        "{type_9}"
    );

    // A venue that speaks another protocol version, named as it answered.
    let version_1 = ProbeFacts {
        protocol_version: "1".into(),
        ..facts.clone()
    };
    let version = refused(&version_1, &wallet, LOCAL);
    assert!(
        matches!(&version, PolymarketPreflightError::ProtocolVersion { found, expected: 2 } if **found == *"1"),
        "{version}"
    );

    // A geoblocked host is reported, never refused: only the venue's answer to a placement says
    // whether this host may trade.
    let blocked = ProbeFacts {
        geoblock: geoblock(BLOCKED_HOST),
        ..facts.clone()
    };
    assert!(blocked.geoblock.blocked);
    let past_the_block = admitted(&blocked, &wallet);

    // The account, after its signed read. A body with no closed-only flag never reads as "may
    // trade": the read refuses it before this check.
    assert_eq!(decode_closed_only(r#"{"closed_only":true}"#), Some(true));
    assert_eq!(decode_closed_only(r#"{"closed_only":false}"#), Some(false));
    for unproven in [
        "{}",
        r#"{"error":"unauthorized"}"#,
        "<html>500 Internal Server Error</html>",
        "",
    ] {
        assert_eq!(decode_closed_only(unproven), None, "{unproven:?}");
    }
    let closed = past_the_block.assess_account(AccountStanding::ClosedOnly);
    assert!(
        matches!(closed, Err(PolymarketPreflightError::ClosedOnly)),
        "{closed:?}"
    );
    let open = past_the_block
        .assess_account(AccountStanding::Open)
        .expect("an account open to new positions passes");
    assert_eq!(open.signature_type, SignatureType::Eoa);
    assert_eq!(open.signature_type.code(), 0);
    assert_eq!(open.signer, key.address());
    assert_eq!(open.maker, key.address());

    // A smart wallet: the maker is the wallet's own address, and the key only signs for it.
    let safe = WalletConfig {
        maker: other.to_checksum_hex().into(),
        signature_type: " 2\n".into(),
        ..wallet.clone()
    };
    let safe_identity = admitted(&facts, &safe)
        .assess_account(AccountStanding::Open)
        .expect("a gnosis safe passes");
    assert_eq!(safe_identity.signature_type, SignatureType::GnosisSafe);
    assert_eq!(safe_identity.signature_type.code(), 2);
    assert_eq!(safe_identity.maker, other);
    assert_eq!(safe_identity.signer, key.address());

    // Several faults at once: the refusal names the first in the order of the gate.
    let all_wrong = refused(&version_1, &foreign, LOCAL);
    assert!(
        matches!(all_wrong, PolymarketPreflightError::SignerMismatch { .. }),
        "{all_wrong}"
    );
    let unproven_and_version = WalletConfig {
        signature_type: "1".into(),
        ..wallet.clone()
    };
    let wallet_first = refused(&version_1, &unproven_and_version, LOCAL);
    assert!(
        matches!(
            wallet_first,
            PolymarketPreflightError::UnprovenWalletType { code: 1 }
        ),
        "{wallet_first}"
    );
    let blocked_version_1 = ProbeFacts {
        geoblock: geoblock(BLOCKED_HOST),
        ..version_1.clone()
    };
    let version_first = refused(&blocked_version_1, &wallet, LOCAL);
    assert!(
        matches!(
            version_first,
            PolymarketPreflightError::ProtocolVersion { .. }
        ),
        "{version_first}"
    );

    // The local clock comes after the wallet and the version, and before the account.
    let before_epoch = LocalTs::from_micros(-1);
    let wallet_before_clock = refused(&facts, &unproven_and_version, before_epoch);
    assert!(
        matches!(
            wallet_before_clock,
            PolymarketPreflightError::UnprovenWalletType { code: 1 }
        ),
        "{wallet_before_clock}"
    );
    let version_before_clock = refused(&version_1, &wallet, before_epoch);
    assert!(
        matches!(
            version_before_clock,
            PolymarketPreflightError::ProtocolVersion { .. }
        ),
        "{version_before_clock}"
    );
    let clock = refused(&blocked, &wallet, before_epoch);
    assert!(
        matches!(clock, PolymarketPreflightError::LocalClockInvalid { seen } if seen == before_epoch),
        "{clock}"
    );
}

/// CONTRACT: a local clock at or before the epoch refuses the arm before any signed request, and
/// the refusal names the value seen. A clock set before 1970 reads as 0 on this host.
#[test]
fn invalid_local_clock_refuses_the_arm() {
    let key = test_key(1);
    let facts = recorded_facts();
    let wallet = eoa_wallet(&key);
    admitted(&facts, &wallet);

    for seen in [
        LocalTs::from_micros(-1_000_000),
        LocalTs::from_micros(-1),
        LocalTs::from_micros(0),
    ] {
        let refusal = refused(&facts, &wallet, seen);
        assert!(
            matches!(refusal, PolymarketPreflightError::LocalClockInvalid { seen: found } if found == seen),
            "local time {seen:?}: {refusal}"
        );
        assert!(
            refusal
                .to_string()
                .contains(&format!("{}us", seen.micros())),
            "the refusal names the value seen: {refusal}"
        );
    }
    let first_microsecond = assess(&facts, &wallet, LocalTs::from_micros(1));
    assert!(first_microsecond.is_ok(), "{first_microsecond:?}");
}
