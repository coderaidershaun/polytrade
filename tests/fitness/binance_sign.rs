//! Credential + signing fitness: a signed Binance request must reproduce the venue's documented
//! HMAC vector, must build the same payload no matter what order the caller set the params in, and
//! must never carry a secret into a log line. Every failure here is silent — a wrong payload order
//! reads as a rejected order, and a leaked secret reads as nothing at all until the account drains.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use polysim::adapters::binance::exec::{
    ClockOffset, RecvWindow, RequestParams, RequestSigner, SignError,
};
use polysim::secrets::{CredentialVariables, EnvFile, Secret, SecretError};
use polysim::time::{DurationUs, TsUs};
use proptest::prelude::*;

/// Binance's own worked example for `POST /api/v3/order`, secret and all.
const DOCUMENTED_SECRET: &str = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
const DOCUMENTED_PAYLOAD: &str = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
const DOCUMENTED_SIGNATURE: &str =
    "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71";

const ORDER_PARAMS: [(&str, &str); 6] = [
    ("symbol", "LTCBTC"),
    ("side", "BUY"),
    ("type", "LIMIT"),
    ("timeInForce", "GTC"),
    ("quantity", "1"),
    ("price", "0.1"),
];

const STAMP: TsUs = TsUs::from_micros(1_499_827_319_559_000);

fn signer() -> RequestSigner {
    RequestSigner::new(&Secret::new(DOCUMENTED_SECRET))
}

fn order_params(order: &[usize]) -> RequestParams {
    order.iter().fold(RequestParams::new(), |params, index| {
        let (name, value) = ORDER_PARAMS[*index];
        params.set(name, value)
    })
}

/// A `.env` under the system temp dir, removed when the test that wrote it ends.
struct TempEnvFile {
    path: PathBuf,
}

impl TempEnvFile {
    fn write(contents: &str) -> Self {
        static NEXT_ID: AtomicU32 = AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "polysim-fitness-{}-{}.env",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, contents).expect("the system temp dir must be writable");
        Self { path }
    }

    fn load(&self) -> EnvFile {
        EnvFile::load(&self.path).expect("the temp env file must load")
    }
}

impl Drop for TempEnvFile {
    fn drop(&mut self) {
        // A leftover temp file is not worth failing an otherwise-green test over.
        fs::remove_file(&self.path).ok();
    }
}

/// Binance's own documented vector: a foreign-oracle pin, not a property we could rederive from
/// our own implementation.
#[test]
fn documented_binance_vector_reproduces() {
    let signature = signer().sign_payload(DOCUMENTED_PAYLOAD);
    assert_eq!(signature.as_str(), DOCUMENTED_SIGNATURE);
}

/// The signed query round trips through its own signature, and a caller-set `signature` or
/// `timestamp` never reaches the payload that gets signed — planting either would let a caller
/// forge or replay the one thing the venue trusts.
#[test]
fn signed_query_is_the_signed_payload_and_rejects_caller_injected_fields() {
    let stamp = ClockOffset::NONE.stamp(STAMP);
    let signed = signer()
        .sign(order_params(&[0, 1, 2, 3, 4, 5]), stamp)
        .expect("the documented params are url safe");

    let (payload, signature) = signed
        .query()
        .rsplit_once("&signature=")
        .expect("a signed query ends with the signature param");
    assert_eq!(signature, signed.signature().as_str());
    assert_eq!(signer().sign_payload(payload).as_str(), signature);

    let polluted = signer()
        .sign(
            order_params(&[0, 1, 2, 3, 4, 5])
                .set("signature", "deadbeef")
                .set("timestamp", "1"),
            stamp,
        )
        .expect("the documented params are url safe");
    assert_eq!(signed.query(), polluted.query());
    assert!(
        polluted
            .signed_params()
            .iter()
            .all(|(name, _)| *name != "signature")
    );
}

/// Credential values must never leak into an error message or a `Debug` rendering. A value
/// carrying `&` or `=` would make the query sent differ from the bytes signed — and a
/// percent-encoder applied after signing is how the two silently diverge, so the request is
/// refused, and the rejection must name the param without echoing its value OR the offending
/// character: the WS API signs `apiKey` as a param, so even one character of it is one character
/// of a credential in a log line. A signed request's `Debug` output must carry no values, or a
/// logged rejection would write a replayable request, and on the WS API the API key itself, into
/// the log file in plaintext.
#[test]
fn credential_values_never_leak_into_an_error_or_a_debug_rendering() {
    // The pattern below binds every field with no `..`, so re-adding a value or character field
    // breaks this test at compile time.
    let refused = signer().sign(
        RequestParams::new().set("apiKey", "secret-looking&value"),
        ClockOffset::NONE.stamp(STAMP),
    );
    let Err(SignError::NotUrlSafe { name, position }) = refused else {
        panic!("a value carrying a query delimiter must be refused, got {refused:?}");
    };
    assert_eq!(name, "apiKey");
    // `apiKey=` is 7 bytes, and the `&` sits 14 bytes into the value.
    assert_eq!(position, 21);
    let rendered = SignError::NotUrlSafe { name, position }.to_string();
    assert!(!rendered.contains("secret-looking"), "leaked: {rendered}");
    assert!(!rendered.contains('&'), "leaked the character: {rendered}");

    let signed = signer()
        .sign(
            order_params(&[0, 1, 2, 3, 4, 5]).set("apiKey", DOCUMENTED_SECRET),
            ClockOffset::NONE.stamp(STAMP),
        )
        .expect("the documented params are url safe");
    let debug_rendered = format!("{signed:?}");
    assert!(
        !debug_rendered.contains(DOCUMENTED_SECRET),
        "a signed param value reached Debug: {debug_rendered}"
    );
    assert!(
        !debug_rendered.contains(signed.signature().as_str()),
        "the signature reached Debug: {debug_rendered}"
    );
    assert!(
        !debug_rendered.contains("LTCBTC"),
        "a signed param value reached Debug: {debug_rendered}"
    );
    // Param NAMES are what a signing failure actually needs, so they must survive.
    assert!(
        debug_rendered.contains("symbol"),
        "names must survive: {debug_rendered}"
    );
    assert!(
        debug_rendered.contains("apiKey"),
        "names must survive: {debug_rendered}"
    );
}

#[test]
fn a_skewed_host_clock_stamps_venue_time() {
    let venue_now = TsUs::from_micros(1_499_827_319_559_000);
    let host_now = venue_now - DurationUs::from_secs(2);
    let offset = ClockOffset::learn(venue_now, host_now);

    assert_eq!(offset.correction(), DurationUs::from_secs(2));
    assert_eq!(offset.stamp(host_now).millis(), 1_499_827_319_559);
    assert_eq!(
        offset
            .stamp(host_now + DurationUs::from_micros(500_000))
            .millis(),
        1_499_827_320_059
    );
}

/// The only guard on the configured window: bring-up builds a [`RecvWindow`] from
/// `execution.recv_window_ms` and refuses to start on the error. Nothing neutral checks the number,
/// because the range belongs to this venue and no other one has to obey it.
#[test]
fn recv_window_range_is_enforced() {
    assert_eq!(RecvWindow::DEFAULT.millis(), 5_000);
    assert_eq!(
        RecvWindow::from_millis(60_000).map(RecvWindow::millis).ok(),
        Some(60_000)
    );
    assert!(matches!(
        RecvWindow::from_millis(60_001),
        Err(SignError::RecvWindowOutOfRange { millis: 60_001 })
    ));
    assert!(matches!(
        RecvWindow::from_millis(0),
        Err(SignError::RecvWindowOutOfRange { millis: 0 })
    ));
}

/// `.env` parsing: the process environment always wins over the file, and comments, blank lines,
/// both quote styles, and surrounding whitespace all parse as documented.
#[test]
fn env_file_parsing_and_process_precedence() {
    let (name, value) = std::env::vars()
        .find(|(name, value)| {
            !value.is_empty()
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
        .expect("the test process carries at least one plainly-named non-empty variable");
    let file = TempEnvFile::write(&format!("{name}=injected-by-the-file\n"));
    let resolved = file.load().resolve(&name).expect("the variable resolves");
    assert_eq!(resolved.expose_bytes(), value.as_bytes());

    let file = TempEnvFile::write(concat!(
        "# a leading comment\n",
        "\n",
        "POLYSIM_FITNESS_PLAIN=plain\n",
        "POLYSIM_FITNESS_DOUBLE=\"double quoted\"\n",
        "POLYSIM_FITNESS_SINGLE='single quoted'\n",
        "   POLYSIM_FITNESS_SPACED   =   spaced   \n",
        "   # an indented comment\n",
    ));
    let loaded = file.load();
    let expected = [
        ("POLYSIM_FITNESS_PLAIN", "plain"),
        ("POLYSIM_FITNESS_DOUBLE", "double quoted"),
        ("POLYSIM_FITNESS_SINGLE", "single quoted"),
        ("POLYSIM_FITNESS_SPACED", "spaced"),
    ];
    for (name, value) in expected {
        let resolved = loaded.resolve(name).expect("the variable resolves");
        assert_eq!(resolved.expose_bytes(), value.as_bytes(), "for {name}");
    }
}

/// `.env` error semantics: a missing file loads as empty rather than erroring, a blank value is
/// distinguished from an absent one, and `export KEY=value` is refused by line number rather than
/// stored as part of the name — accepted loosely it becomes a variable named `"export KEY"`, and
/// the only symptom is `resolve` insisting `KEY` is unset while the operator reads it in the file.
#[test]
fn env_file_error_semantics_distinguish_missing_empty_and_malformed() {
    let absent = std::env::temp_dir().join("polysim-fitness-absent-on-purpose.env");
    fs::remove_file(&absent).ok();
    let loaded = EnvFile::load(&absent).expect("a missing env file loads as empty");
    assert!(matches!(
        loaded.resolve("POLYSIM_FITNESS_NOWHERE"),
        Err(SecretError::Missing { .. })
    ));

    let file = TempEnvFile::write("POLYSIM_FITNESS_BLANK=\n");
    let loaded = file.load();
    assert!(matches!(
        loaded.resolve("POLYSIM_FITNESS_BLANK"),
        Err(SecretError::Empty { .. })
    ));
    assert!(matches!(
        loaded.resolve("POLYSIM_FITNESS_ABSENT"),
        Err(SecretError::Missing { .. })
    ));

    let file = TempEnvFile::write(concat!(
        "POLYSIM_FITNESS_GOOD=value\n",
        "export POLYSIM_FITNESS_KEY=exported\n",
    ));
    let Err(SecretError::MalformedLine { line, .. }) = EnvFile::load(&file.path) else {
        panic!("an `export` prefix must be refused rather than stored as part of the name");
    };
    assert_eq!(line, 2);
}

#[test]
fn a_secret_never_prints_itself() {
    let secret = Secret::new(DOCUMENTED_SECRET);
    assert_eq!(format!("{secret:?}"), "<redacted>");

    let file = TempEnvFile::write(&format!(
        "POLYSIM_FITNESS_KEY=key\nPOLYSIM_FITNESS_SECRET={DOCUMENTED_SECRET}\n"
    ));
    let loaded = file.load();
    let credentials = loaded
        .resolve_credentials(&CredentialVariables {
            api_key_env: "POLYSIM_FITNESS_KEY",
            api_secret_env: "POLYSIM_FITNESS_SECRET",
        })
        .expect("both variables resolve");

    for rendered in [format!("{loaded:?}"), format!("{credentials:?}")] {
        assert!(
            !rendered.contains(DOCUMENTED_SECRET),
            "secret leaked into a debug rendering: {rendered}"
        );
    }
}

proptest! {
    /// The payload the venue signs is order-independent only because we sort it. Lose the sort and
    /// two identical orders sign differently depending on which branch built the params.
    #[test]
    fn insertion_order_never_changes_the_signed_query(
        order in Just((0..ORDER_PARAMS.len()).collect::<Vec<_>>()).prop_shuffle(),
    ) {
        let stamp = ClockOffset::NONE.stamp(STAMP);
        let shuffled = signer()
            .sign(order_params(&order), stamp)
            .expect("the documented params are url safe");
        let canonical = signer()
            .sign(order_params(&[0, 1, 2, 3, 4, 5]), stamp)
            .expect("the documented params are url safe");

        prop_assert_eq!(shuffled.query(), canonical.query());
    }
}
