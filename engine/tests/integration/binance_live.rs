//! What each signed test of live Binance spot shares: the caps of the live policy, the check of the
//! acknowledgment, the run folder, the redaction of each request and payload, and an order lane
//! that sends each order action once.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use engine::msg::ids::{ClientOrderId, FIXED_SCALE, InstrumentId};
use engine::msg::time::EngineClock;
use engine::runtime::config::{BinanceMarket, RunIdentity};
use engine::runtime::secrets::Secret;
use engine::venues::binance::exec::{
    BINANCE_CREDENTIAL_VARIABLES, ClockOffset, EncodeContext, EncodedRequest, RecvWindow,
    RequestSigner, SymbolTable, encode_request,
};
use engine::venues::binance::rest::{BinanceEnv, RestClient, RestRequest};
use engine::venues::exec::{EngineIdentity, ExecRequest, OrderOwnership, TeTag};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Map, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

/// The one instrument that a live run on Binance may trade.
pub(crate) const SYMBOL: &str = "BTCUSDT";
pub(crate) const INSTRUMENT: InstrumentId = InstrumentId(0);

/// One unit at the fixed scale of each amount below.
pub(crate) const ONE: i64 = FIXED_SCALE;

/// The caps of one live run on Binance spot, from the live policy, in USDT or BTC at the fixed
/// scale. No test sends an order outside them.
pub(crate) const ORDER_SIZE_USDT: i64 = 10 * ONE;
pub(crate) const ORDER_CEILING_USDT: i64 = 25 * ONE;
pub(crate) const EXPOSURE_USDT: i64 = 50 * ONE;

// An order inside the size cap is inside the ceiling of one order too.
const _: () = assert!(ORDER_SIZE_USDT <= ORDER_CEILING_USDT);
pub(crate) const RESERVE_BTC: i64 = ONE / 5_000;
pub(crate) const RESERVE_USDT: i64 = 20 * ONE;
pub(crate) const RUN_LENGTH: Duration = Duration::from_secs(10 * 60);

/// The answer of one order action, or no answer at all. A request with no answer is never sent
/// again: the caller reads the venue to learn what happened.
const ANSWER_WAIT: Duration = Duration::from_secs(10);

const ACKNOWLEDGMENT: &str = "yes-i-acknowledge-the-risk";
const RUN_FOLDER: &str = "REX_RUN_DIR";
const REDACTED: &str = "<redacted>";

/// Fields that would give a key, a signature, a stream token, or the account away.
const SECRET_FIELDS: [&str; 6] = [
    "apiKey",
    "signature",
    "listenKey",
    "subscriptionId",
    "uid",
    "accountId",
];

/// Stream events that hold balances. A payload file never holds one.
const BALANCE_EVENTS: [&str; 2] = ["outboundAccountPosition", "balanceUpdate"];

/// The first statement of each signed test: a real-funds test runs only with the acknowledgment
/// of its runbook.
pub(crate) fn require_real_funds_ack() {
    let ack = std::env::var("REX_REAL_FUNDS").unwrap_or_default();
    assert_eq!(
        ack, ACKNOWLEDGMENT,
        "a test with real funds runs only with the acknowledgment of its runbook in REX_REAL_FUNDS"
    );
}

/// Whether the order actions of a run go to the venue, or are built and printed only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sending {
    DryRun,
    Live,
}

/// The engine identity of these tests, apart from each engine of the build, so each client order
/// id starts with `pd-` and the tag of the tests. Each run has its own nonce.
pub(crate) fn test_identity() -> EngineIdentity {
    let identity =
        RunIdentity::new("live-test", "binance-recording").expect("a valid identity of the tests");
    let run_nonce = u32::try_from(crate::unix_now_s()).expect("seconds since 1970 fit a u32");
    EngineIdentity {
        te_tag: TeTag::of(&identity),
        run_nonce,
    }
}

/// Whether a client order id passes the ownership rule of the codec with the tag of the tests: an
/// order of this run or of an earlier run of the tests. Each other order belongs to someone else.
pub(crate) fn is_ours(ownership: OrderOwnership) -> bool {
    matches!(
        ownership,
        OrderOwnership::Mine(_) | OrderOwnership::PriorRun(_)
    )
}

/// The client order id of the tests for one side, so a run holds one order at most on each side.
pub(crate) fn client_id(identity: EngineIdentity, side_slot: u64) -> ClientOrderId {
    ClientOrderId(u64::from(identity.run_nonce) << 32 | side_slot)
}

/// Says whether each key variable is set in the environment, by its name only. A key that the
/// working folder's `.env` holds is never read here.
pub(crate) fn report_key_variables() {
    for name in [
        BINANCE_CREDENTIAL_VARIABLES.api_key_env,
        BINANCE_CREDENTIAL_VARIABLES.api_secret_env,
    ] {
        let state = if std::env::var_os(name).is_some() { "set" } else { "unset" };
        println!("  {name}: {state} in the environment");
    }
}

/// The run folder that `REX_RUN_DIR` names, made if it is missing. It is never below the fixtures
/// of the repo, because a payload of a live run enters Git only after a person redacts it again.
pub(crate) fn run_folder() -> PathBuf {
    let folder = std::env::var_os(RUN_FOLDER)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("{RUN_FOLDER} names no run folder"));
    assert!(
        folder.is_absolute(),
        "{RUN_FOLDER} must be an absolute path, got {}",
        folder.display()
    );
    assert!(
        !folder
            .components()
            .any(|part| part.as_os_str() == "fixtures"),
        "a payload of a live run never goes below the fixtures, got {}",
        folder.display()
    );
    fs::create_dir_all(&folder).expect("the run folder can be made");
    folder
}

/// A copy of `value` with each secret field shown as `<redacted>`, at any depth.
pub(crate) fn redacted(value: &Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(name, field)| {
                    let shown = if SECRET_FIELDS.contains(&name.as_str()) {
                        Value::String(REDACTED.to_owned())
                    } else {
                        redacted(field)
                    };
                    (name.clone(), shown)
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redacted).collect()),
        other => other.clone(),
    }
}

/// An append-only file of one JSON value per line, each redacted at write.
pub(crate) struct PayloadFile(PathBuf);

impl PayloadFile {
    pub(crate) fn in_folder(folder: &Path, name: &str) -> Self {
        Self(folder.join(name))
    }

    pub(crate) fn write(&self, value: &Value) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.0)
            .unwrap_or_else(|error| panic!("cannot open {}: {error}", self.0.display()));
        writeln!(file, "{}", redacted(value))
            .unwrap_or_else(|error| panic!("cannot write {}: {error}", self.0.display()));
    }
}

/// The lot, the tick and the minimums that the venue publishes for the symbol, at the fixed scale.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SymbolFacts {
    pub(crate) tick: i64,
    pub(crate) step: i64,
    pub(crate) min_qty: i64,
    pub(crate) min_notional: i64,
}

/// The best bid and ask of the symbol, at the fixed scale.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Touch {
    pub(crate) bid: i64,
    pub(crate) ask: i64,
}

/// Reads the facts and the touch of the symbol by public requests, which sign nothing.
pub(crate) async fn public_market() -> (SymbolFacts, Touch) {
    let mut client = RestClient::new(BinanceMarket::Spot, BinanceEnv::Production)
        .expect("a public client of binance spot");
    let info = client
        .exchange_info(SYMBOL)
        .await
        .expect("exchangeInfo answers for the symbol");
    let filters = &info
        .symbols
        .iter()
        .find(|row| &*row.symbol == SYMBOL)
        .expect("exchangeInfo names the symbol")
        .filters;
    let filter = |kind: &str| {
        filters
            .iter()
            .find(|filter| &*filter.filter_type == kind)
            .unwrap_or_else(|| panic!("{SYMBOL} publishes no {kind} filter"))
    };
    let amount = |text: &Option<Box<str>>, name: &str| {
        mantissa(text.as_deref().unwrap_or_else(|| panic!("no {name}")))
    };
    let facts = SymbolFacts {
        tick: amount(&filter("PRICE_FILTER").tick_size, "tickSize"),
        step: amount(&filter("LOT_SIZE").step_size, "stepSize"),
        min_qty: amount(&filter("LOT_SIZE").min_qty, "minQty"),
        min_notional: amount(&filter("NOTIONAL").min_notional, "minNotional"),
    };
    let depth = client
        .fetch_text(&RestRequest::DepthSnapshot {
            symbol: SYMBOL.to_owned(),
            limit: 5,
        })
        .await
        .expect("the depth of the symbol answers");
    let depth: Value = serde_json::from_str(&depth).expect("the depth is json");
    let best = |side: &str| {
        depth[side][0][0]
            .as_str()
            .map(mantissa)
            .unwrap_or_else(|| panic!("the depth holds no {side}"))
    };
    let touch = Touch {
        bid: best("bids"),
        ask: best("asks"),
    };
    (facts, touch)
}

/// A decimal of the venue at the fixed scale, with no rounding through a float.
pub(crate) fn mantissa(text: &str) -> i64 {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let fraction = format!("{fraction:0<8}");
    let digits = format!("{whole}{}", &fraction[..8]);
    digits
        .parse()
        .unwrap_or_else(|_| panic!("{text} is no decimal of at most 8 places"))
}

/// A fixed-scale amount as the venue writes a decimal.
pub(crate) fn decimal(amount: i64) -> String {
    let fraction = format!("{:08}", amount.rem_euclid(ONE));
    let fraction = fraction.trim_end_matches('0');
    let whole = amount.div_euclid(ONE);
    if fraction.is_empty() { whole.to_string() } else { format!("{whole}.{fraction}") }
}

/// `value` rounded down to a whole number of `step`.
pub(crate) fn floor_to(value: i64, step: i64) -> i64 {
    value - value.rem_euclid(step)
}

/// The notional of `qty` at `price`, rounded up, so a cap is never passed by rounding.
pub(crate) fn notional(price: i64, qty: i64) -> i64 {
    let product = i128::from(price) * i128::from(qty);
    let rounded_up = (product + i128::from(ONE) - 1) / i128::from(ONE);
    i64::try_from(rounded_up).expect("the notional of one small order fits an i64")
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// What the venue said to one order action.
#[derive(Debug)]
pub(crate) enum Answer {
    /// The answer frame, as the venue sent it.
    Given(Value),
    /// A dry run built the request and sent nothing.
    NotSent,
    /// No answer came, or the connection dropped. The action is not sent again.
    Unknown,
}

/// The order lane of a run over the WebSocket API of the venue. Each order action goes out once,
/// never again after a timeout or a dropped connection.
pub(crate) struct OrderLane {
    socket: Option<Socket>,
    signer: RequestSigner,
    api_key: String,
    offset: ClockOffset,
    clock: EngineClock,
    symbols: SymbolTable,
    identity: EngineIdentity,
    next_id: u64,
    requests: Option<PayloadFile>,
    answers: Option<PayloadFile>,
    events: Option<PayloadFile>,
    sent: Vec<Value>,
}

/// The keys that sign each order action, and the clock offset of the venue that stamps it.
pub(crate) struct LaneKeys<'a> {
    pub(crate) api_key: &'a Secret,
    pub(crate) api_secret: &'a Secret,
    pub(crate) offset: ClockOffset,
}

/// Where a lane writes what it sends and what it hears. A dry run writes nothing.
pub(crate) struct LaneFiles {
    pub(crate) requests: PayloadFile,
    pub(crate) answers: PayloadFile,
    pub(crate) events: PayloadFile,
}

impl OrderLane {
    /// A live lane connects and subscribes to the user data stream, so it records each execution
    /// report of the run. A dry lane opens nothing.
    pub(crate) async fn open(
        sending: Sending,
        keys: LaneKeys<'_>,
        identity: EngineIdentity,
        files: Option<LaneFiles>,
    ) -> Self {
        let api_key = std::str::from_utf8(keys.api_key.expose_bytes())
            .expect("the api key is text")
            .to_owned();
        let (requests, answers, events) = match files {
            Some(files) => (
                Some(files.requests),
                Some(files.answers),
                Some(files.events),
            ),
            None => (None, None, None),
        };
        let mut lane = Self {
            socket: None,
            signer: RequestSigner::new(keys.api_secret),
            api_key,
            offset: keys.offset,
            clock: EngineClock::start(),
            symbols: SymbolTable::new([(INSTRUMENT, SYMBOL.into())]),
            identity,
            next_id: 1,
            requests,
            answers,
            events,
            sent: Vec::new(),
        };
        if sending == Sending::Live {
            let (socket, _) = connect_async("wss://ws-api.binance.com:443/ws-api/v3")
                .await
                .expect("the websocket api of binance connects");
            lane.socket = Some(socket);
            let answer = lane.send_once(ExecRequest::SubscribeUserStream).await;
            assert!(
                matches!(&answer, Answer::Given(frame) if frame["status"] == 200),
                "the user data stream subscribes: {answer:?}"
            );
        }
        lane
    }

    /// Signs one request and sends it once. It prints the request with each secret field shown
    /// as `<redacted>`. A dry lane sends nothing.
    pub(crate) async fn send_once(&mut self, request: ExecRequest) -> Answer {
        let id = self.next_id;
        self.next_id += 1;
        let frame = self.frame(id, request);
        if let Some(symbol) = frame["params"].get("symbol") {
            assert_eq!(symbol, SYMBOL, "the tests trade {SYMBOL} alone");
        }
        let shown = redacted(&frame);
        println!("  request: {shown}");
        if let Some(file) = &self.requests {
            file.write(&frame);
        }
        self.sent.push(shown);
        let Some(socket) = self.socket.as_mut() else {
            return Answer::NotSent;
        };
        if socket.send(Message::text(frame.to_string())).await.is_err() {
            return Answer::Unknown;
        }
        let answer = self.answer_to(id).await;
        if let (Answer::Given(frame), Some(file)) = (&answer, &self.answers) {
            file.write(frame);
        }
        answer
    }

    /// Records each stream event that comes within `span`, and returns the execution reports
    /// among them.
    pub(crate) async fn listen(&mut self, span: Duration) -> Vec<Value> {
        let mut reports = Vec::new();
        let deadline = tokio::time::Instant::now() + span;
        while let Ok(Some(frame)) = tokio::time::timeout_at(deadline, self.next_frame()).await {
            if let Some(report) = self.record_event(&frame) {
                reports.push(report);
            }
        }
        reports
    }

    /// Each request that the lane sent or built, with each secret field shown as `<redacted>`.
    pub(crate) fn sent(&self) -> &[Value] {
        &self.sent
    }

    fn frame(&self, id: u64, request: ExecRequest) -> Value {
        let context = EncodeContext {
            symbols: &self.symbols,
            identity: self.identity,
        };
        let EncodedRequest { method, params } =
            encode_request(request, &context).expect("the request names the one symbol");
        let window = RecvWindow::from_millis(5_000).expect("5000 ms is a receive window");
        let signed = self
            .signer
            .sign(
                params
                    .set("apiKey", self.api_key.as_str())
                    .set_recv_window(window),
                self.offset.stamp(self.clock.now()),
            )
            .expect("each parameter of the request is safe in a query");
        let mut fields: Map<String, Value> = signed
            .signed_params()
            .iter()
            .map(|(name, value)| ((*name).to_owned(), Value::String(value.clone())))
            .collect();
        fields.insert(
            "signature".to_owned(),
            Value::String(signed.signature().as_str().to_owned()),
        );
        serde_json::json!({ "id": id, "method": method, "params": fields })
    }

    /// The answer frame of request `id`, recording each stream event that comes before it.
    async fn answer_to(&mut self, id: u64) -> Answer {
        let deadline = tokio::time::Instant::now() + ANSWER_WAIT;
        loop {
            match tokio::time::timeout_at(deadline, self.next_frame()).await {
                Ok(Some(frame)) if frame["id"] == id => return Answer::Given(frame),
                Ok(Some(frame)) => {
                    self.record_event(&frame);
                }
                Ok(None) | Err(_) => return Answer::Unknown,
            }
        }
    }

    async fn next_frame(&mut self) -> Option<Value> {
        let socket = self.socket.as_mut()?;
        loop {
            match socket.next().await? {
                Ok(Message::Text(text)) => {
                    return Some(serde_json::from_str(&text).expect("the venue sends json"));
                }
                Ok(Message::Close(_)) | Err(_) => return None,
                Ok(_) => {}
            }
        }
    }

    /// Writes a stream event to the events file, unless it holds balances.
    fn record_event(&self, frame: &Value) -> Option<Value> {
        let event = frame.get("event")?;
        let kind = event["e"].as_str().unwrap_or_default();
        if BALANCE_EVENTS.contains(&kind) {
            return None;
        }
        if let Some(file) = &self.events {
            file.write(event);
        }
        (kind == "executionReport").then(|| redacted(event))
    }
}
