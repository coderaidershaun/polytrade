//! The signed tests of live Binance spot that record the payloads of one small order, read the
//! account before and after, stop each order of the tests, and sell back what a fill bought. Each
//! one stays inside the caps of the live policy, in code.

use std::path::Path;
use std::time::{Duration, Instant};

use engine::msg::exec::OrderStyle;
use engine::msg::ids::{Price, Qty, Side};
use engine::runtime::secrets::{EnvFile, Secret};
use engine::venues::binance::exec::{
    BINANCE_CREDENTIAL_VARIABLES, ExecutionPreflight, RecvWindow, RequestParams, RequestSigner,
    classify_client_order_id, format_client_order_id, preflight_execution,
};
use engine::venues::binance::rest::{AccountInfo, BinanceEnv, OrderRecord, SignedRestClient};
use engine::venues::edge::rest_quiet::SharedRestQuiet;
use engine::venues::exec::{EngineIdentity, ExecRequest};
use serde_json::{Value, json};

use crate::binance_live::{
    Answer, EXPOSURE_USDT, INSTRUMENT, LaneFiles, LaneKeys, ONE, ORDER_CEILING_USDT,
    ORDER_SIZE_USDT, OrderLane, PayloadFile, RESERVE_BTC, RESERVE_USDT, RUN_LENGTH, SYMBOL,
    Sending, SymbolFacts, Touch, client_id, decimal, floor_to, is_ours, mantissa, notional,
    public_market, redacted, report_key_variables, require_real_funds_ack, run_folder,
    test_identity,
};

/// How long the recording waits for its resting buy to fill, inside the length of one run.
const FILL_WAIT: Duration = Duration::from_secs(5 * 60);

/// The slot of the client order id of each order of a run. The taker buy goes out only after the
/// cancel of the resting buy is confirmed, so one order at most rests on each side.
const BUY_SLOT: u64 = 1;
const SELL_SLOT: u64 = 2;
const TAKER_BUY_SLOT: u64 = 3;

/// The taker buy lives for one match and never rests.
const IMMEDIATE_OR_CANCEL: &[(&str, &str)] = &[("timeInForce", "IOC")];

/// The reads of a run: before the recording, after it, and after a rollback.
const READ_PHASES: [&str; 3] = ["before", "after", "after-rollback"];

/// The rollback sells this many ticks below the best bid, so it takes the bid at once.
const SELL_THROUGH_TICKS: i64 = 20;

/// Places one post-only buy of about 10 USDT at the best bid, records each answer and each
/// execution report of the user data stream, waits for a fill, cancels what rests by its client
/// order id, and prints each request with each secret field shown as `<redacted>`. IF the buy has
/// no fill, because its time ends or because the venue refuses it as a buy that would cross, THEN
/// after the venue shows that no buy of the tests rests, one buy of the same size at the best ask,
/// which lives for one match, gives the trade report that U-M3 needs.
#[ignore = "real funds on live binance: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn records_a_small_order_lifecycle_on_binance() {
    require_real_funds_ack();
    record_order_lifecycle(Sending::Live).await;
}

/// The recording with each order action built and printed, and none sent.
#[ignore = "real funds on live binance: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn records_a_small_order_lifecycle_on_binance_dry_run() {
    require_real_funds_ack();
    record_order_lifecycle(Sending::DryRun).await;
}

/// The kill switch: cancels each open order of the symbol whose client order id passes the
/// ownership rule of the codec with the tag of the tests, one cancel by client order id for each,
/// and prints each order that it left. It never cancels by symbol alone.
#[ignore = "real funds on live binance: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn cancels_only_the_orders_of_the_tests() {
    require_real_funds_ack();
    cancel_orders_of_the_tests(Sending::Live).await;
}

/// The kill switch with each cancel built and printed, and none sent.
#[ignore = "real funds on live binance: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn cancels_only_the_orders_of_the_tests_dry_run() {
    require_real_funds_ack();
    cancel_orders_of_the_tests(Sending::DryRun).await;
}

/// The rollback: sends one sell of at most the free BTC above `before`, inside the caps and never
/// into the reserve, and prints what it sent.
#[ignore = "real funds on live binance: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn sells_back_the_base_above_before() {
    require_real_funds_ack();
    sell_back_the_base_above_before(Sending::Live).await;
}

/// The rollback with its sell built and printed, and none sent.
#[ignore = "real funds on live binance: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn sells_back_the_base_above_before_dry_run() {
    require_real_funds_ack();
    sell_back_the_base_above_before(Sending::DryRun).await;
}

/// Reads the clock skew, the open orders of the symbol and the balances, and writes them to the
/// file of the run folder that `REX_READ_PHASE` names: `before.json`, `after.json`, or
/// `after-rollback.json`.
#[ignore = "real funds on live binance: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn writes_the_venue_reads() {
    require_real_funds_ack();
    write_venue_reads().await;
}

/// The reads with one account read signed and printed, and nothing sent or written: the key
/// loads and the redaction holds before the first signed request of a run.
#[ignore = "real funds on live binance: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[test]
fn writes_the_venue_reads_dry_run() {
    require_real_funds_ack();
    report_key_variables();
    let credentials = EnvFile::load(Path::new(EnvFile::DEFAULT_PATH))
        .and_then(|secrets| secrets.resolve_credentials(&BINANCE_CREDENTIAL_VARIABLES))
        .expect("the keys load from the environment or the .env of the working folder");
    println!(
        "  request: {}",
        signed_read_shown(credentials.api_key(), credentials.api_secret())
    );
    println!("  nothing sent, nothing written");
}

async fn record_order_lifecycle(sending: Sending) {
    report_key_variables();
    let folder = run_folder();
    let before = Reads::from_file(&folder.join("before.json"));
    let identity = test_identity();
    let ExecutionPreflight {
        rest: mut client,
        credentials,
        ..
    } = live_venue().await;
    let (facts, touch) = public_market().await;
    println!(
        "  touch: bid {} ask {}",
        decimal(touch.bid),
        decimal(touch.ask)
    );
    let account = client.account().await.expect("the account read answers");
    refuse_a_second_order_on(Side::Buy, &mut client, identity).await;

    let qty = floor_to(ORDER_SIZE_USDT * ONE / touch.bid, facts.step);
    let buy = Order {
        side: Side::Buy,
        price: touch.bid,
        qty,
    };
    buy.check_inside_caps(&facts, &account, &before);

    let recording = folder.join("recording");
    std::fs::create_dir_all(&recording).expect("the recording folder can be made");
    let files = (sending == Sending::Live).then(|| LaneFiles {
        requests: PayloadFile::in_folder(&recording, "requests.jsonl"),
        answers: PayloadFile::in_folder(&recording, "answers.jsonl"),
        events: PayloadFile::in_folder(&recording, "events.jsonl"),
    });
    let started = Instant::now();
    let keys = LaneKeys {
        api_key: credentials.api_key(),
        api_secret: credentials.api_secret(),
        offset: client.clock_offset(),
    };
    let mut lane = OrderLane::open(sending, keys, identity, files).await;
    let buy_id = client_id(identity, BUY_SLOT);
    let placed = lane
        .send_once(buy.request(buy_id, OrderStyle::PostOnly))
        .await;
    let taker_id = client_id(identity, TAKER_BUY_SLOT);
    if sending == Sending::DryRun {
        lane.send_once(cancel(buy_id)).await;
        let taker = taker_buy(&facts, touch, &account, &before);
        lane.send_once_with(
            taker.request(taker_id, OrderStyle::Immediate),
            IMMEDIATE_OR_CANCEL,
        )
        .await;
        println!("  dry run: {} requests built, none sent", lane.sent().len());
        return;
    }
    let client_order_id = format_client_order_id(identity.te_tag, buy_id);
    let placed_status = status_of(&placed, &client_order_id, &mut client).await;
    println!("  placed: {placed_status}");
    let refused_for_crossing = is_refused_for_crossing(&placed);
    if refused_for_crossing {
        println!("  the venue refused the post-only buy, because it would cross: no fill");
    }

    if placed_status.is_open() {
        lane.listen(FILL_WAIT).await;
    }
    let now = status_of(&Answer::Unknown, &client_order_id, &mut client).await;
    if now.is_open() {
        let answer = lane.send_once(cancel(buy_id)).await;
        println!("  cancel: {}", answer_text(&answer));
        lane.listen(Duration::from_secs(2)).await;
    }
    let after = status_of(&Answer::Unknown, &client_order_id, &mut client).await;
    let mut reports = lane.take_reports();
    let has_traded = reports.iter().any(|report| report["x"] == "TRADE");
    let mut taker_after = None;
    if !has_traded && (after.is_cancelled() || refused_for_crossing) {
        let taker_order_id = format_client_order_id(identity.te_tag, taker_id);
        let status = take_the_ask(&mut lane, &mut client, identity, &before, taker_id).await;
        println!("    taker buy {taker_order_id}: {status}");
        reports.extend(lane.take_reports());
        taker_after = Some(status);
    }

    println!("  what the venue gave:");
    println!("    order {client_order_id}: {after}");
    println!("    execution reports: {}", reports.len());
    for report in reports.iter().filter(|report| report["x"] == "TRADE") {
        println!(
            "    trade t={} l={} z={} n={} N={}",
            report["t"], report["l"], report["z"], report["n"], report["N"]
        );
    }
    assert!(
        !after.is_open() && !taker_after.as_ref().is_some_and(OrderStatus::is_open),
        "no order of the tests rests at the end of the run: {after}"
    );
    assert!(
        started.elapsed() < RUN_LENGTH,
        "the run took {:?}, past the length of one run",
        started.elapsed()
    );
}

async fn cancel_orders_of_the_tests(sending: Sending) {
    report_key_variables();
    let identity = test_identity();
    let ExecutionPreflight {
        rest: mut client, ..
    } = live_venue().await;
    let open = client
        .open_orders(SYMBOL)
        .await
        .expect("the open orders of the symbol read");
    let mut left = Vec::new();
    for order in open {
        assert_eq!(&*order.symbol, SYMBOL, "the read names one symbol");
        if !is_ours(classify_client_order_id(&order.client_order_id, identity)) {
            left.push(order);
            continue;
        }
        let id = &*order.client_order_id;
        println!("  request: DELETE /api/v3/order symbol={SYMBOL} origClientOrderId={id}");
        if sending == Sending::Live {
            let answer = client.cancel_order(SYMBOL, id).await;
            match answer {
                Ok(record) => println!("  cancelled {id}: {}", record.status),
                Err(failure) => println!("  cancel of {id} failed: {failure}"),
            }
        }
    }
    for order in &left {
        println!("  left: {}", order_line(order));
    }
    println!("  orders left that are not the tests': {}", left.len());
    if sending == Sending::Live {
        let rest = client
            .open_orders(SYMBOL)
            .await
            .expect("the open orders read again");
        let ours = rest
            .iter()
            .filter(|order| is_ours(classify_client_order_id(&order.client_order_id, identity)));
        for order in ours {
            println!(
                "  still open, cancel by hand at the venue: {}",
                order_line(order)
            );
        }
    }
}

async fn sell_back_the_base_above_before(sending: Sending) {
    report_key_variables();
    let folder = run_folder();
    let before = Reads::from_file(&folder.join("before.json"));
    let identity = test_identity();
    let ExecutionPreflight {
        rest: mut client,
        credentials,
        ..
    } = live_venue().await;
    let (facts, touch) = public_market().await;
    let account = client.account().await.expect("the account read answers");
    let free_btc = free(&account, "BTC");
    let above_before = free_btc - before.free_btc;
    if above_before <= 0 {
        println!(
            "  nothing to roll back: free BTC {} is not above before {}",
            decimal(free_btc),
            decimal(before.free_btc)
        );
        return;
    }
    let size_cap = floor_to(ORDER_SIZE_USDT * ONE / touch.bid, facts.step);
    let qty = floor_to(above_before, facts.step).min(size_cap);
    if qty < facts.min_qty || notional(touch.bid, qty) < facts.min_notional {
        println!(
            "  dust left: {} BTC above before, below the minimum order of the venue",
            decimal(above_before)
        );
        return;
    }
    refuse_a_second_order_on(Side::Sell, &mut client, identity).await;
    let price = floor_to(touch.bid - SELL_THROUGH_TICKS * facts.tick, facts.tick);
    let sell = Order {
        side: Side::Sell,
        price,
        qty,
    };
    sell.check_inside_caps(&facts, &account, &before);

    let rollback = folder.join("rollback");
    std::fs::create_dir_all(&rollback).expect("the rollback folder can be made");
    let files = (sending == Sending::Live).then(|| LaneFiles {
        requests: PayloadFile::in_folder(&rollback, "requests.jsonl"),
        answers: PayloadFile::in_folder(&rollback, "answers.jsonl"),
        events: PayloadFile::in_folder(&rollback, "events.jsonl"),
    });
    let keys = LaneKeys {
        api_key: credentials.api_key(),
        api_secret: credentials.api_secret(),
        offset: client.clock_offset(),
    };
    let mut lane = OrderLane::open(sending, keys, identity, files).await;
    let sell_id = client_id(identity, SELL_SLOT);
    let answer = lane
        .send_once(sell.request(sell_id, OrderStyle::Immediate))
        .await;
    if sending == Sending::DryRun {
        println!("  dry run: the sell is built, and none sent");
        return;
    }
    let client_order_id = format_client_order_id(identity.te_tag, sell_id);
    let status = status_of(&answer, &client_order_id, &mut client).await;
    println!("  sell {client_order_id}: {status}");
    if status.is_open() {
        let answer = lane.send_once(cancel(sell_id)).await;
        println!(
            "  the rest of the sell is cancelled: {}",
            answer_text(&answer)
        );
    }
}

async fn write_venue_reads() {
    report_key_variables();
    let phase = std::env::var("REX_READ_PHASE").unwrap_or_default();
    assert!(
        READ_PHASES.contains(&phase.as_str()),
        "REX_READ_PHASE names one of {READ_PHASES:?}, got {phase:?}"
    );
    let path = run_folder().join(format!("{phase}.json"));
    assert!(
        !path.exists(),
        "{} exists, and a read never writes over another",
        path.display()
    );
    let identity = test_identity();
    let ExecutionPreflight {
        rest: mut client, ..
    } = live_venue().await;
    let skew_us = client
        .sync_clock()
        .await
        .expect("the venue answers its time")
        .correction()
        .micros();
    let open = client
        .open_orders(SYMBOL)
        .await
        .expect("the open orders of the symbol read");
    let account = client.account().await.expect("the account read answers");
    let ours = open
        .iter()
        .filter(|order| is_ours(classify_client_order_id(&order.client_order_id, identity)))
        .count();
    let orders: Vec<Value> = open
        .iter()
        .map(|order| {
            json!({
                "client_order_id": &*order.client_order_id,
                "side": &*order.side,
                "price": &*order.price,
                "orig_qty": &*order.orig_qty,
                "executed_qty": &*order.executed_qty,
                "status": &*order.status,
            })
        })
        .collect();
    let balances: Vec<Value> = ["BTC", "USDT"]
        .iter()
        .map(|asset| {
            json!({
                "asset": asset,
                "free": decimal(free(&account, asset)),
                "locked": decimal(locked(&account, asset)),
            })
        })
        .collect();
    let reads = json!({ "skew_us": skew_us, "open_orders": orders, "balances": balances });
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&redacted(&reads)).expect("json"),
    )
    .expect("the reads file is written");
    println!(
        "  open orders of {SYMBOL}: {}, of the tests: {ours}",
        open.len()
    );
    println!("  clock skew against the venue: {skew_us} us");
    println!("  written: {}", path.display());
}

/// One order of the tests, which [`Order::check_inside_caps`] holds inside the caps of the live
/// policy before it is built.
#[derive(Debug, Clone, Copy)]
struct Order {
    side: Side,
    price: i64,
    qty: i64,
}

impl Order {
    /// Refuses, by a panic that names the cap, an order outside the caps of the live policy: its
    /// size, its ceiling, the minimums of the venue, and the reserve, which no trade reaches.
    fn check_inside_caps(&self, facts: &SymbolFacts, account: &AccountInfo, before: &Reads) {
        let value = notional(self.price, self.qty);
        assert!(self.qty > 0, "the order holds no quantity");
        assert!(
            value <= ORDER_SIZE_USDT,
            "the order of {} USDT passes the size cap of {} USDT, inside the ceiling of {} USDT",
            decimal(value),
            decimal(ORDER_SIZE_USDT),
            decimal(ORDER_CEILING_USDT)
        );
        assert!(
            self.qty >= facts.min_qty && value >= facts.min_notional,
            "the order of {} BTC, {} USDT, is below the minimum of the venue",
            decimal(self.qty),
            decimal(value)
        );
        match self.side {
            Side::Buy => {
                let left = free(account, "USDT") - value;
                assert!(
                    left >= RESERVE_USDT,
                    "the buy leaves {} USDT, inside the reserve of {} USDT",
                    decimal(left),
                    decimal(RESERVE_USDT)
                );
                let held = free(account, "BTC") - before.free_btc + self.qty;
                let exposure = notional(self.price, held.max(0));
                assert!(
                    exposure <= EXPOSURE_USDT,
                    "the buy takes the exposure of the run to {} USDT, past the cap of {} USDT",
                    decimal(exposure),
                    decimal(EXPOSURE_USDT)
                );
            }
            Side::Sell => {
                let left = free(account, "BTC") - self.qty;
                assert!(
                    left >= RESERVE_BTC && left >= before.free_btc,
                    "the sell leaves {} BTC, below the reserve of {} BTC or the {} BTC of before",
                    decimal(left),
                    decimal(RESERVE_BTC),
                    decimal(before.free_btc)
                );
            }
        }
    }

    fn request(&self, id: engine::msg::ids::ClientOrderId, style: OrderStyle) -> ExecRequest {
        ExecRequest::Place {
            instrument: INSTRUMENT,
            client_id: id,
            side: self.side,
            price: Price::from_mantissa(self.price),
            qty: Qty::from_mantissa(self.qty),
            style,
        }
    }
}

/// A buy of the size of the resting buy at the best ask, inside the caps.
fn taker_buy(facts: &SymbolFacts, touch: Touch, account: &AccountInfo, before: &Reads) -> Order {
    let order = Order {
        side: Side::Buy,
        price: touch.ask,
        qty: floor_to(ORDER_SIZE_USDT * ONE / touch.ask, facts.step),
    };
    order.check_inside_caps(facts, account, before);
    order
}

/// The fill of last resort: one buy at the best ask that lives for one match, sent once, after a
/// new read of the touch and the account, and a new check that no buy of the tests rests.
async fn take_the_ask(
    lane: &mut OrderLane,
    client: &mut SignedRestClient,
    identity: EngineIdentity,
    before: &Reads,
    taker_id: engine::msg::ids::ClientOrderId,
) -> OrderStatus {
    let (facts, touch) = public_market().await;
    let account = client.account().await.expect("the account read answers");
    refuse_a_second_order_on(Side::Buy, client, identity).await;
    let taker = taker_buy(&facts, touch, &account, before);
    let answer = lane
        .send_once_with(
            taker.request(taker_id, OrderStyle::Immediate),
            IMMEDIATE_OR_CANCEL,
        )
        .await;
    println!("  taker buy: {}", answer_text(&answer));
    lane.listen(Duration::from_secs(3)).await;
    let taker_order_id = format_client_order_id(identity.te_tag, taker_id);
    status_of(&answer, &taker_order_id, client).await
}

/// Whether the venue refused a post-only buy because it would match at once: HTTP 400 with code
/// -2010 and the message of a match. A -2010 with any other message, such as a short balance, is
/// not this refusal, and gets no taker buy.
fn is_refused_for_crossing(answer: &Answer) -> bool {
    let Answer::Given(frame) = answer else { return false };
    let error = &frame["error"];
    frame["status"] == 400
        && error["code"] == -2010
        && error["msg"]
            .as_str()
            .is_some_and(|message| message.contains("immediately match"))
}

fn cancel(id: engine::msg::ids::ClientOrderId) -> ExecRequest {
    ExecRequest::Cancel {
        instrument: INSTRUMENT,
        client_id: id,
    }
}

/// The free BTC of the `before` file of the run folder, which the read test wrote.
struct Reads {
    free_btc: i64,
}

impl Reads {
    fn from_file(path: &Path) -> Self {
        let text = std::fs::read_to_string(path).unwrap_or_else(|_| {
            panic!(
                "{} is missing: the reads of before come first in a run",
                path.display()
            )
        });
        let reads: Value = serde_json::from_str(&text).expect("the reads file is json");
        let btc = reads["balances"]
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["asset"] == "BTC"))
            .and_then(|row| row["free"].as_str())
            .expect("the reads file holds the free BTC");
        Self {
            free_btc: mantissa(btc),
        }
    }
}

/// The state of one order of the tests at the venue.
enum OrderStatus {
    Known(Box<str>),
    Unknown(String),
}

impl OrderStatus {
    fn is_cancelled(&self) -> bool {
        matches!(self, OrderStatus::Known(status) if &**status == "CANCELED")
    }

    fn is_open(&self) -> bool {
        matches!(self, OrderStatus::Known(status) if &**status == "NEW" || &**status == "PARTIALLY_FILLED")
    }
}

impl std::fmt::Display for OrderStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderStatus::Known(status) => formatter.write_str(status),
            OrderStatus::Unknown(cause) => write!(formatter, "unknown ({cause})"),
        }
    }
}

/// The state of an order from its answer, or from a read of the venue when the answer is
/// missing: a request with no answer is read, never sent again.
async fn status_of(
    answer: &Answer,
    client_order_id: &str,
    client: &mut SignedRestClient,
) -> OrderStatus {
    if let Answer::Given(frame) = answer
        && let Some(status) = frame["result"]["status"].as_str()
    {
        return OrderStatus::Known(status.into());
    }
    match client.order_status(SYMBOL, client_order_id).await {
        Ok(record) => OrderStatus::Known(record.status),
        Err(failure) => OrderStatus::Unknown(failure.to_string()),
    }
}

fn answer_text(answer: &Answer) -> String {
    match answer {
        Answer::Given(frame) => redacted(frame).to_string(),
        Answer::NotSent => "not sent".to_owned(),
        Answer::Unknown => {
            "no answer: the venue is read, and the request is not sent again".to_owned()
        }
    }
}

/// Refuses a run when an order of the tests already rests on `side`: one order each side.
async fn refuse_a_second_order_on(
    side: Side,
    client: &mut SignedRestClient,
    identity: EngineIdentity,
) {
    let open = client
        .open_orders(SYMBOL)
        .await
        .expect("the open orders of the symbol read");
    let wire_side = match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };
    let resting = open.iter().find(|order| {
        &*order.side == wire_side
            && is_ours(classify_client_order_id(&order.client_order_id, identity))
    });
    assert!(
        resting.is_none(),
        "an order of the tests already rests on the {wire_side} side, and a run holds one each side: {}",
        resting.map(order_line).unwrap_or_default()
    );
}

/// The probed venue: the key loads, the clock syncs, and the key can trade spot.
async fn live_venue() -> ExecutionPreflight {
    let venue = preflight_execution(BinanceEnv::Production, SharedRestQuiet::new())
        .await
        .expect("the key loads, signs, and passes the permission probe");
    println!(
        "  clock skew against the venue: {} us",
        venue.rest.clock_offset().correction().micros()
    );
    venue
}

fn free(account: &AccountInfo, asset: &str) -> i64 {
    balance(account, asset, |row| &row.free)
}

fn locked(account: &AccountInfo, asset: &str) -> i64 {
    balance(account, asset, |row| &row.locked)
}

fn balance(
    account: &AccountInfo,
    asset: &str,
    amount: impl Fn(&engine::venues::binance::rest::Balance) -> &Box<str>,
) -> i64 {
    account
        .balances
        .iter()
        .find(|row| &*row.asset == asset)
        .map_or(0, |row| mantissa(amount(row)))
}

fn order_line(order: &OrderRecord) -> String {
    format!(
        "{} {} {} @ {} ({})",
        order.client_order_id, order.side, order.orig_qty, order.price, order.status
    )
}

/// A signed account read, shown with each secret field as `<redacted>`.
fn signed_read_shown(api_key: &Secret, api_secret: &Secret) -> Value {
    let signer = RequestSigner::new(api_secret);
    signed_shown(
        "GET /api/v3/account",
        RequestParams::new(),
        api_key,
        &signer,
    )
}

fn signed_shown(
    endpoint: &str,
    params: RequestParams,
    api_key: &Secret,
    signer: &RequestSigner,
) -> Value {
    let window = RecvWindow::from_millis(5_000).expect("5000 ms is a receive window");
    let stamp = engine::venues::binance::exec::ClockOffset::NONE
        .stamp(engine::msg::time::EngineClock::start().now());
    let signed = signer
        .sign(params.set_recv_window(window), stamp)
        .expect("each parameter is safe in a query");
    let mut fields: serde_json::Map<String, Value> = signed
        .signed_params()
        .iter()
        .map(|(name, value)| ((*name).to_owned(), Value::String(value.clone())))
        .collect();
    fields.insert(
        "signature".to_owned(),
        Value::String(signed.signature().as_str().to_owned()),
    );
    let key = std::str::from_utf8(api_key.expose_bytes()).unwrap_or_default();
    redacted(&json!({ "endpoint": endpoint, "apiKey": key, "params": fields }))
}
