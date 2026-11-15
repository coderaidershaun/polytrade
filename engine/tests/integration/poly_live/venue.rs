//! The venue side of the signed tests of live Polymarket: the start gate, a transport that sends
//! each request once, and the reads of the open window, its book, the orders, and the balances.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use engine::msg::ids::{ClientOrderId, InstrumentId, Price, Qty};
use engine::msg::time::{DurationUs, EngineClock, LocalTs, VenueTs};
use engine::runtime::config::PolySeries;
use engine::runtime::secrets::EnvFile;
use engine::venues::exec::ExecRequest;
use engine::venues::polymarket::exec::codec::{
    EncodeContext, EncodedRequest, OrderIndex, OrderSigner, OrderSignerSetup, TokenBinding,
    TokenTable, collateral_balance, conditional_balance, decode_neg_risk, encode_request,
    neg_risk_request, open_orders_for_token,
};
use engine::venues::polymarket::exec::handle::{WalletIdentity, preflight_polymarket};
use engine::venues::polymarket::exec::lanes::{LaneSetup, RestLanes, RestOutcome, spawn_lanes};
use engine::venues::polymarket::exec::rest::ClobHttp;
use engine::venues::polymarket::exec::sign::l2::{RequestSigner, RequestToSign};
use engine::venues::polymarket::rest::{PolyRest, book_url};
use serde_json::Value;
use tokio::sync::mpsc;

use super::policy::{ONE, ORDER_SIZE_SHARES, Plan, Redactor, decimal, report_key_variables};

pub(crate) const SERIES: PolySeries = PolySeries::BtcUpDown5m;

/// A test starts its orders only with this much left of the open window, so its cancel lands
/// before the market closes.
const WINDOW_LEFT: Duration = Duration::from_secs(90);

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// The answer of one request, or none. A request with no answer is never sent again: the caller
/// reads the venue to learn what happened.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The open window of the series: the token of its Up outcome, its grid, and its close.
#[derive(Debug, Clone)]
pub(crate) struct Window {
    pub(crate) token: String,
    pub(crate) tick: Price,
    pub(crate) min_size: Qty,
    pub(crate) close: VenueTs,
}

/// The window of the series that is open now, by public reads that sign nothing.
pub(crate) async fn open_window() -> Window {
    let rest = PolyRest::new(SERIES).expect("a public client of polymarket");
    let now = VenueTs::from_micros(local_now().micros());
    let (current, _) = rest
        .resolve_current_and_next(now)
        .await
        .expect("the venue names the open window of the series");
    Window {
        token: current.token_up.to_string(),
        tick: current.tick_size,
        min_size: current.min_order_size,
        close: current.window_close_ts_us,
    }
}

impl Window {
    /// Refuses a window that closes before an order and its cancel can land, or whose minimum
    /// size is above the size cap.
    pub(crate) fn check_for_an_order(&self, token: &str) {
        assert_eq!(
            self.token, token,
            "the window of the tests has closed: take new reads of before, with a new run folder"
        );
        let left = self.close.diff(VenueTs::from_micros(local_now().micros()));
        assert!(
            left.micros() >= WINDOW_LEFT.as_micros() as i64,
            "the window closes in {} s, and a test needs {} s",
            left.micros() / 1_000_000,
            WINDOW_LEFT.as_secs()
        );
        assert!(
            self.min_size.mantissa() <= ORDER_SIZE_SHARES,
            "the venue's minimum of {} shares passes the size cap of 5 shares",
            decimal(self.min_size.mantissa())
        );
    }
}

/// The best bid and the best ask of `token`, by a public read.
pub(crate) async fn touch(token: &str) -> (i64, i64) {
    let rest = PolyRest::new(SERIES).expect("a public client of polymarket");
    let (status, text) = rest
        .fetch_status_and_text(&book_url(token))
        .await
        .expect("the book of the token answers");
    assert_eq!(status, 200, "the book of the token answers: {text}");
    let book: Value = serde_json::from_str(&text).expect("the book is json");
    let prices = |side: &str| -> Vec<i64> {
        book[side]
            .as_array()
            .map(|levels| {
                levels
                    .iter()
                    .filter_map(|level| level["price"].as_str())
                    .map(|price| Price::parse_decimal(price).expect("a price").mantissa())
                    .collect()
            })
            .unwrap_or_default()
    };
    let bid = prices("bids").into_iter().max().unwrap_or(0);
    let ask = prices("asks").into_iter().min().unwrap_or(ONE);
    (bid, ask)
}

/// An open order of the account, as the venue lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenOrder {
    pub(crate) id: String,
    pub(crate) side: String,
    pub(crate) price: String,
    pub(crate) size: String,
    pub(crate) status: String,
}

impl std::fmt::Display for OpenOrder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} {} {} @ {} ({})",
            self.id, self.side, self.size, self.price, self.status
        )
    }
}

/// The gate of the start, passed: the wallet that it proved, its clock offset, and the signers and
/// transport of the tests. Each request goes out once.
pub(crate) struct Session {
    http: ClobHttp,
    request_signer: Arc<RequestSigner>,
    order_signer: OrderSigner,
    wallet: WalletIdentity,
    venue_clock_offset: DurationUs,
    redactor: Redactor,
}

impl Session {
    /// Loads the keys from the `.env` of the working folder and runs the start gate of the engine:
    /// the wallet, the protocol version, the local clock, and the account. Each of its reads is
    /// public or signed; none is an order action.
    pub(crate) async fn open() -> Self {
        report_key_variables();
        let secrets = EnvFile::load(Path::new(EnvFile::DEFAULT_PATH))
            .expect("the keys load from the environment or the .env of the working folder");
        let gate = preflight_polymarket(&secrets)
            .await
            .expect("the start gate passes: the wallet is proven and the account may trade");
        let api_key = gate.credentials.api_key().to_owned();
        let maker = gate.wallet.maker.to_checksum_hex();
        let signer = gate.wallet.signer.to_checksum_hex();
        let redactor = Redactor::hiding(&[&api_key, &maker, &signer]);
        println!(
            "  start gate passed: signatureType {}, clock offset {} us",
            gate.wallet.signature_type.code(),
            gate.venue_clock_offset.micros()
        );
        let request_signer = RequestSigner::new(&gate.credentials, gate.wallet.signer)
            .expect("the minted credentials sign");
        let order_signer = OrderSigner::new(OrderSignerSetup {
            key: gate.key,
            maker: gate.wallet.maker,
            signer: gate.wallet.signer,
            signature_type: gate.wallet.signature_type,
            api_key,
        });
        Self {
            http: ClobHttp::new(CONNECT_TIMEOUT, REQUEST_TIMEOUT).expect("an http client"),
            request_signer: Arc::new(request_signer),
            order_signer,
            wallet: gate.wallet,
            venue_clock_offset: gate.venue_clock_offset,
            redactor,
        }
    }

    pub(crate) fn redactor(&self) -> &Redactor {
        &self.redactor
    }

    pub(crate) fn venue_clock_offset(&self) -> DurationUs {
        self.venue_clock_offset
    }

    /// The three request lanes of the engine over the transport and the signer of the tests, so a
    /// test sends a placement and a cancel at the same time on lanes of their own.
    pub(crate) fn lanes<P: Send + 'static>(
        &self,
    ) -> (RestLanes<P>, mpsc::Receiver<RestOutcome<P>>) {
        spawn_lanes(
            LaneSetup {
                http: Arc::new(self.http.clone()),
                signer: Arc::clone(&self.request_signer),
                clock: EngineClock::start(),
                venue_clock_offset: self.venue_clock_offset,
            },
            &tokio::runtime::Handle::current(),
        )
    }

    /// A signed read as it would go out, with its headers, each secret shown as `<redacted>`.
    pub(crate) fn shown_signed(&self, request: &EncodedRequest) -> Value {
        let mut view = request_view(request);
        view["headers"] = headers_view(&self.request_signer, request, self.venue_secs());
        self.redactor.redacted(&view)
    }

    pub(crate) fn venue_secs(&self) -> i64 {
        (local_now().micros() + self.venue_clock_offset.micros()) / 1_000_000
    }

    /// The signed body of one order on `token`, built by the encoder of the engine.
    pub(crate) async fn order(&self, token: &str, tick: Price, plan: &Plan) -> EncodedRequest {
        let neg_risk = self
            .http
            .send_public(&neg_risk_request(token))
            .await
            .expect("the neg risk flag of the token answers");
        let is_neg_risk = decode_neg_risk(&neg_risk.body).expect("the neg risk flag decodes");
        let mut tokens = TokenTable::with_retired_capacity(1);
        tokens.bind(TokenBinding {
            instrument: INSTRUMENT,
            token_id: token.into(),
            tick,
            is_neg_risk,
        });
        let orders = OrderIndex::with_capacity(1);
        let context = EncodeContext {
            tokens: &tokens,
            orders: &orders,
            signer: &self.order_signer,
            sent_ts_us: local_now(),
        };
        encode_request(
            ExecRequest::Place {
                instrument: INSTRUMENT,
                client_id: plan.client,
                side: plan.side,
                price: Price::from_mantissa(plan.price),
                qty: Qty::from_mantissa(plan.size),
                style: plan.style,
            },
            &context,
        )
        .expect("the order is on the grid of the venue")
    }

    /// Prints the request with each secret shown as `<redacted>`, and sends it once when the run is
    /// live. `None` when it was not sent, or when no answer came: it is never sent again.
    pub(crate) async fn send_once(
        &self,
        request: &EncodedRequest,
        is_live: bool,
    ) -> Option<(u16, Value)> {
        println!("  request: {}", self.shown_request(request));
        if !is_live {
            return None;
        }
        match self
            .http
            .send_signed(&self.request_signer, request, self.venue_secs())
            .await
        {
            Ok(response) => {
                let body = serde_json::from_str(&response.body)
                    .unwrap_or(Value::String(response.body.clone()));
                println!(
                    "  answer: http {} {}",
                    response.status,
                    self.redactor.redacted(&body)
                );
                Some((response.status, body))
            }
            Err(failure) => {
                println!("  no answer: {failure}; the request is not sent again");
                None
            }
        }
    }

    /// A read: it signs and goes out at once, in a dry run too, and prints nothing of its answer.
    pub(crate) async fn read(&self, request: &EncodedRequest) -> Value {
        let response = self
            .http
            .send_signed(&self.request_signer, request, self.venue_secs())
            .await
            .expect("the signed read answers");
        assert!(
            response.is_success(),
            "the signed read {} answers http {}",
            request.path,
            response.status
        );
        serde_json::from_str(&response.body).expect("the read is json")
    }

    /// Each open order of the account on `token`.
    pub(crate) async fn open_orders(&self, token: &str) -> Vec<OpenOrder> {
        let mut orders = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .read(&open_orders_for_token(token, cursor.as_deref()))
                .await;
            let rows = page["data"].as_array().cloned().unwrap_or_default();
            orders.extend(rows.iter().map(|row| OpenOrder {
                id: text(row, "id"),
                side: text(row, "side"),
                price: text(row, "price"),
                size: text(row, "original_size"),
                status: text(row, "status"),
            }));
            match page["next_cursor"].as_str() {
                Some(next) if !next.is_empty() && next != END_CURSOR => {
                    cursor = Some(next.to_owned())
                }
                _ => return orders,
            }
        }
    }

    /// The collateral of the wallet, in USD at the fixed scale.
    pub(crate) async fn collateral(&self) -> i64 {
        let answer = self
            .read(&collateral_balance(self.wallet.signature_type.into()))
            .await;
        venue_amount(&answer["balance"])
    }

    /// The shares of `token` that the wallet holds, at the fixed scale.
    pub(crate) async fn shares(&self, token: &str) -> i64 {
        let answer = self
            .read(&conditional_balance(
                token,
                self.wallet.signature_type.into(),
            ))
            .await;
        venue_amount(&answer["balance"])
    }

    pub(crate) fn shown_request(&self, request: &EncodedRequest) -> Value {
        self.redactor.redacted(&request_view(request))
    }
}

/// A request as JSON, before redaction: its method, path, query, and body.
pub(crate) fn request_view(request: &EncodedRequest) -> Value {
    let body = serde_json::from_str(&request.body).unwrap_or(Value::Null);
    serde_json::json!({
        "method": request.method.as_str(),
        "path": request.path,
        "query": request.query,
        "body": body,
    })
}

/// The signed headers of a request as JSON, before redaction.
pub(crate) fn headers_view(
    signer: &RequestSigner,
    request: &EncodedRequest,
    venue_secs: i64,
) -> Value {
    let headers = signer.headers(
        &RequestToSign {
            method: request.method,
            path: &request.path,
            body: &request.body,
        },
        venue_secs,
    );
    Value::Object(
        headers
            .entries()
            .iter()
            .map(|(name, value)| ((*name).to_owned(), Value::String((*value).to_owned())))
            .collect(),
    )
}

/// The page cursor that ends a listing of the venue.
const END_CURSOR: &str = "LTE=";

/// The one instrument that the encoder binds the token of the tests to.
pub(crate) const INSTRUMENT: InstrumentId = InstrumentId(0);

/// A client id of the tests: the encoder salts each order with it. The venue never sees it.
pub(crate) fn client_id(slot: u64) -> ClientOrderId {
    let secs = u64::try_from(crate::unix_now_s()).expect("seconds since 1970");
    ClientOrderId(secs << 8 | slot)
}

/// The venue's 6-decimal amount at the fixed scale.
fn venue_amount(value: &Value) -> i64 {
    let units: i64 = value
        .as_str()
        .and_then(|text| text.parse().ok())
        .unwrap_or_else(|| panic!("the venue answers no whole amount: {value}"));
    units * (ONE / 1_000_000)
}

fn text(row: &Value, field: &str) -> String {
    row[field].as_str().unwrap_or_default().to_owned()
}

/// The wall clock of this host, for the stamps that the venue's clock offset corrects.
pub(crate) fn local_now() -> LocalTs {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_micros();
    LocalTs::from_micros(i64::try_from(micros).expect("microseconds since 1970 fit an i64"))
}
