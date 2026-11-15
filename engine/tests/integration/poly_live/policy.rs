//! What the live policy asks of each signed test of live Polymarket: the caps of one run, the
//! redaction of each request and answer, and the file of the order ids that the tests placed.

use std::fs;
use std::path::{Path, PathBuf};

use engine::msg::exec::OrderStyle;
use engine::msg::ids::{ClientOrderId, FIXED_SCALE, Side};
use engine::venues::polymarket::exec::POLYMARKET_CREDENTIAL_VARIABLES;
use serde_json::Value;

/// One unit at the fixed scale of each amount below.
pub(crate) const ONE: i64 = FIXED_SCALE;

/// The caps of one live run on Polymarket, from the live policy: shares, and USD at the fixed
/// scale. No test sends an order outside them.
pub(crate) const ORDER_SIZE_SHARES: i64 = 5 * ONE;
pub(crate) const ORDER_CEILING_USD: i64 = 6 * ONE;
pub(crate) const EXPOSURE_USD: i64 = 6 * ONE;
pub(crate) const RESERVE_USD: i64 = 65 * ONE;
pub(crate) const ORDERS_EACH_SIDE: usize = 1;

/// The lowest price of the venue: a buy there sits far below each touch that is above 0.02.
pub(crate) const FLOOR_PRICE: i64 = ONE / 100;

const REDACTED: &str = "<redacted>";

/// Fields that would give a key, a signature, the api key, or a wallet address away.
const SECRET_FIELDS: [&str; 16] = [
    "owner",
    "trade_owner",
    "maker_address",
    "maker",
    "signer",
    "funder",
    "address",
    "signature",
    "apiKey",
    "api_key",
    "secret",
    "passphrase",
    "POLY_ADDRESS",
    "POLY_SIGNATURE",
    "POLY_API_KEY",
    "POLY_PASSPHRASE",
];

/// Says whether each key variable is set in the environment, by its name only. A key that the
/// working folder's `.env` holds is never read here.
pub(crate) fn report_key_variables() {
    for name in [
        POLYMARKET_CREDENTIAL_VARIABLES.api_key_env,
        POLYMARKET_CREDENTIAL_VARIABLES.api_secret_env,
    ] {
        let state = if std::env::var_os(name).is_some() { "set" } else { "unset" };
        println!("  {name}: {state} in the environment");
    }
}

/// Shows each secret field as `<redacted>`, and each known secret text inside any other string.
#[derive(Debug, Clone, Default)]
pub(crate) struct Redactor {
    texts: Vec<String>,
}

impl Redactor {
    /// Also hides each of `texts` wherever a string holds it, in each casing an address has.
    pub(crate) fn hiding(texts: &[&str]) -> Self {
        let texts = texts
            .iter()
            .filter(|text| !text.is_empty())
            .flat_map(|text| [(*text).to_owned(), text.to_lowercase()])
            .collect();
        Self { texts }
    }

    pub(crate) fn redacted(&self, value: &Value) -> Value {
        match value {
            Value::Object(fields) => Value::Object(
                fields
                    .iter()
                    .map(|(name, field)| {
                        let shown = if SECRET_FIELDS.contains(&name.as_str()) {
                            Value::String(REDACTED.to_owned())
                        } else {
                            self.redacted(field)
                        };
                        (name.clone(), shown)
                    })
                    .collect(),
            ),
            Value::Array(items) => {
                Value::Array(items.iter().map(|item| self.redacted(item)).collect())
            }
            Value::String(text) => Value::String(self.hidden(text)),
            other => other.clone(),
        }
    }

    fn hidden(&self, text: &str) -> String {
        self.texts.iter().fold(text.to_owned(), |shown, secret| {
            shown.replace(secret.as_str(), REDACTED)
        })
    }
}

/// The file of the run folder that holds the token of the tests and the id of each order that
/// they placed. A placement writes its line before it is sent, and its venue id once the venue
/// answers, so the kill switch reaches each order of the tests and no other.
pub(crate) struct IdFile(PathBuf);

/// One line of the id file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdLine {
    Token(String),
    /// A placement that has no answer yet, named by its client id.
    Pending(u64),
    Placed(String),
}

impl IdFile {
    pub(crate) fn in_folder(folder: &Path) -> Self {
        Self(folder.join("poly-order-ids.txt"))
    }

    pub(crate) fn exists(&self) -> bool {
        self.0.exists()
    }

    pub(crate) fn lines(&self) -> Vec<IdLine> {
        let Ok(text) = fs::read_to_string(&self.0) else {
            return Vec::new();
        };
        text.lines()
            .filter_map(|line| match line.split_once(' ')? {
                ("token", token) => Some(IdLine::Token(token.to_owned())),
                ("pending", client) => client.parse().ok().map(IdLine::Pending),
                ("placed", order) => Some(IdLine::Placed(order.to_owned())),
                _ => None,
            })
            .collect()
    }

    /// The token of the tests, which the reads of `before` chose.
    pub(crate) fn token(&self) -> String {
        self.lines()
            .into_iter()
            .find_map(|line| match line {
                IdLine::Token(token) => Some(token),
                _ => None,
            })
            .unwrap_or_else(|| {
                panic!(
                    "{} names no token: the reads of before come first in a run",
                    self.0.display()
                )
            })
    }

    pub(crate) fn placed(&self) -> Vec<String> {
        self.lines()
            .into_iter()
            .filter_map(|line| match line {
                IdLine::Placed(order) => Some(order),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn pending(&self) -> Vec<u64> {
        self.lines()
            .into_iter()
            .filter_map(|line| match line {
                IdLine::Pending(client) => Some(client),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn start(&self, token: &str) {
        assert!(
            !self.exists(),
            "{} exists, and a run starts one id file",
            self.0.display()
        );
        self.write(&[IdLine::Token(token.to_owned())]);
    }

    pub(crate) fn add_pending(&self, client: ClientOrderId) {
        let mut lines = self.lines();
        lines.push(IdLine::Pending(client.0));
        self.write(&lines);
    }

    /// The pending line of `client` becomes the venue id that the answer gave.
    pub(crate) fn place(&self, client: ClientOrderId, order: &str) {
        let lines: Vec<IdLine> = self
            .lines()
            .into_iter()
            .map(|line| match line {
                IdLine::Pending(pending) if pending == client.0 => IdLine::Placed(order.to_owned()),
                other => other,
            })
            .collect();
        self.write(&lines);
    }

    fn write(&self, lines: &[IdLine]) {
        let text: String = lines
            .iter()
            .map(|line| match line {
                IdLine::Token(token) => format!("token {token}\n"),
                IdLine::Pending(client) => format!("pending {client}\n"),
                IdLine::Placed(order) => format!("placed {order}\n"),
            })
            .collect();
        fs::write(&self.0, text)
            .unwrap_or_else(|error| panic!("cannot write {}: {error}", self.0.display()));
    }
}

/// One order of the tests, which [`Plan::check_inside_caps`] holds inside the caps of the live
/// policy before it is built.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Plan {
    pub(crate) client: ClientOrderId,
    pub(crate) side: Side,
    pub(crate) price: i64,
    pub(crate) size: i64,
    pub(crate) style: OrderStyle,
}

impl Plan {
    /// What the order can cost at most, rounded up.
    pub(crate) fn cost(&self) -> i64 {
        let product = i128::from(self.price) * i128::from(self.size);
        let rounded_up = (product + i128::from(ONE) - 1) / i128::from(ONE);
        i64::try_from(rounded_up).expect("the cost of one small order fits an i64")
    }

    /// Refuses, by a panic that names the cap, an order outside the caps of the live policy: its
    /// size, its ceiling, the exposure of the run, and the reserve, which no trade reaches.
    pub(crate) fn check_inside_caps(&self, collateral: i64, exposure_before: i64) {
        assert!(self.size > 0, "the order holds no shares");
        assert!(
            self.size <= ORDER_SIZE_SHARES,
            "the order of {} shares passes the size cap of 5 shares",
            decimal(self.size)
        );
        assert!(
            self.cost() <= ORDER_CEILING_USD,
            "the order of {} USD passes the ceiling of 6 USD",
            decimal(self.cost())
        );
        if self.side == Side::Buy {
            assert!(
                exposure_before + self.cost() <= EXPOSURE_USD,
                "the order takes the exposure of the run to {} USD, past the cap of 6 USD",
                decimal(exposure_before + self.cost())
            );
            assert!(
                collateral - self.cost() >= RESERVE_USD,
                "the order leaves {} USD, inside the reserve of 65 USD",
                decimal(collateral - self.cost())
            );
        }
    }
}

/// Refuses a run when `wanted` orders of the tests on one side would pass the cap of one each side.
pub(crate) fn check_orders_each_side(wanted: usize) {
    assert!(
        wanted <= ORDERS_EACH_SIDE,
        "the test holds {wanted} orders on one side, and the live policy permits {ORDERS_EACH_SIDE}"
    );
}

/// The run folder of the tests, made if it is missing, never below the fixtures of the repo.
pub(crate) fn run_folder() -> PathBuf {
    crate::binance_live::run_folder()
}

/// A fixed-scale amount as a decimal of the venue.
pub(crate) fn decimal(amount: i64) -> String {
    crate::binance_live::decimal(amount)
}
