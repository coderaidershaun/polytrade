//! The signed test of live Binance spot that asks whether a read on the order session still answers
//! while the user stream of that session is silent, with one resting buy far below the touch.

use std::time::{Duration, Instant};

use engine::msg::exec::OrderStyle;
use engine::msg::ids::{ClientOrderId, Price, Qty, Side};
use engine::venues::binance::exec::{
    ExecutionPreflight, classify_client_order_id, format_client_order_id, preflight_execution,
};
use engine::venues::binance::rest::{AccountInfo, BinanceEnv};
use engine::venues::edge::rest_quiet::SharedRestQuiet;
use engine::venues::exec::ExecRequest;
use serde_json::Value;

use crate::binance_live::{
    Answer, EXPOSURE_USDT, INSTRUMENT, LaneFiles, LaneKeys, ONE, ORDER_CEILING_USDT,
    ORDER_SIZE_USDT, OrderLane, PayloadFile, RESERVE_USDT, RUN_LENGTH, SYMBOL, Sending, client_id,
    decimal, floor_to, is_ours, mantissa, notional, public_market, redacted, report_key_variables,
    require_real_funds_ack, run_folder, test_identity,
};

/// The silence after which the shipped config escalates (`execution.stream_silence_ms`, 30 s), and
/// 5 s more.
const SILENCE: Duration = Duration::from_secs(35);

/// A read that answers inside this, while the stream is silent, answers U-V2 with yes.
const READ_WAIT: Duration = Duration::from_secs(10);

/// The resting buy sits this far below the best bid, so it never sits at the touch.
const BELOW_THE_BID_PERCENT: i64 = 2;

/// The slot of the client order id of the resting buy, apart from the slots of the recording.
const BUY_SLOT: u64 = 4;

/// Places one post-only buy of about 10 USDT 2 % below the best bid, waits 35 s while counting the
/// events of the user stream, reads the open orders on the same session, cancels the buy by its
/// client order id, and prints the answer to U-V2.
#[ignore = "real funds on live binance: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn silent_user_stream_still_answers_reads() {
    require_real_funds_ack();
    read_through_a_silent_stream(Sending::Live).await;
}

/// The buy, the read, and the cancel built and printed, and none sent.
#[ignore = "real funds on live binance: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn silent_user_stream_still_answers_reads_dry_run() {
    require_real_funds_ack();
    read_through_a_silent_stream(Sending::DryRun).await;
}

async fn read_through_a_silent_stream(sending: Sending) {
    report_key_variables();
    let started = Instant::now();
    let folder = run_folder();
    let identity = test_identity();
    let ExecutionPreflight {
        rest: mut client,
        credentials,
        ..
    } = preflight_execution(BinanceEnv::Production, SharedRestQuiet::new())
        .await
        .expect("the key loads, signs, and passes the permission probe");
    println!(
        "  clock skew against the venue: {} us",
        client.clock_offset().correction().micros()
    );
    let (facts, touch) = public_market().await;
    let account = client.account().await.expect("the account read answers");
    let open = client
        .open_orders(SYMBOL)
        .await
        .expect("the open orders of the symbol read");
    let resting = open.iter().find(|order| {
        &*order.side == "BUY" && is_ours(classify_client_order_id(&order.client_order_id, identity))
    });
    assert!(
        resting.is_none(),
        "a buy of the tests already rests, and a run holds one order each side: {}",
        resting
            .map(|order| &*order.client_order_id)
            .unwrap_or_default()
    );

    let price = floor_to(touch.bid * (100 - BELOW_THE_BID_PERCENT) / 100, facts.tick);
    let qty = floor_to(ORDER_SIZE_USDT * ONE / price, facts.step);
    check_inside_caps(price, qty, &facts, &account);
    let buy_id = client_id(identity, BUY_SLOT);
    let client_order_id = format_client_order_id(identity.te_tag, buy_id);

    let silence = folder.join("silence");
    std::fs::create_dir_all(&silence).expect("the silence folder can be made");
    let files = (sending == Sending::Live).then(|| LaneFiles {
        requests: PayloadFile::in_folder(&silence, "requests.jsonl"),
        answers: PayloadFile::in_folder(&silence, "answers.jsonl"),
        events: PayloadFile::in_folder(&silence, "events.jsonl"),
    });
    let keys = LaneKeys {
        api_key: credentials.api_key(),
        api_secret: credentials.api_secret(),
        offset: client.clock_offset(),
    };
    let mut lane = OrderLane::open(sending, keys, identity, files).await;
    let placed = lane.send_once(buy(buy_id, price, qty)).await;
    if sending == Sending::DryRun {
        lane.send_once(open_orders()).await;
        lane.send_once(cancel(buy_id)).await;
        println!("  dry run: {} requests built, none sent", lane.sent().len());
        return;
    }
    println!("  placed: {}", answer_text(&placed));
    let is_new = matches!(&placed, Answer::Given(frame) if frame["result"]["status"] == "NEW");
    assert!(
        is_new,
        "the venue did not rest the buy: read the venue, then run the kill switch of part 3"
    );
    lane.listen(Duration::from_secs(2)).await;
    let reports = lane.take_reports();
    println!(
        "  stream reports of the placement: {}",
        reports
            .iter()
            .filter(|report| report["c"] == client_order_id.as_str())
            .count()
    );

    lane.listen(SILENCE).await;
    let heard = lane.take_reports().len();
    println!(
        "  stream events in the {} s of silence: {heard}",
        SILENCE.as_secs()
    );
    let asked = Instant::now();
    let read = lane.send_once(open_orders()).await;
    let waited = asked.elapsed();
    let names_the_order =
        matches!(&read, Answer::Given(frame) if names_order(frame, &client_order_id));
    println!(
        "  read answered after {} ms, naming the buy: {names_the_order}",
        waited.as_millis()
    );

    let cancelled = lane.send_once(cancel(buy_id)).await;
    println!("  cancel: {}", answer_text(&cancelled));
    lane.listen(Duration::from_secs(2)).await;
    let status = client
        .order_status(SYMBOL, &client_order_id)
        .await
        .map(|record| record.status.to_string())
        .unwrap_or_else(|failure| format!("unknown ({failure})"));
    println!("  the buy at the end: {status}");

    let is_yes = heard == 0 && names_the_order && waited <= READ_WAIT;
    println!(
        "  U-V2: {}",
        match is_yes {
            true => "YES: the read answered while the stream was silent",
            false => "NO: the read did not answer in time, or the stream was not silent",
        }
    );
    assert!(
        status != "NEW" && status != "PARTIALLY_FILLED",
        "the buy of the test still rests: run the kill switch of part 3"
    );
    assert!(
        started.elapsed() < RUN_LENGTH,
        "the run took {:?}, past the length of one run",
        started.elapsed()
    );
}

/// Refuses, by a panic that names the cap, a buy outside the caps of the live policy: its size, its
/// ceiling, its exposure, the minimums of the venue, and the reserve, which no trade reaches.
fn check_inside_caps(
    price: i64,
    qty: i64,
    facts: &crate::binance_live::SymbolFacts,
    account: &AccountInfo,
) {
    let value = notional(price, qty);
    assert!(qty > 0, "the buy holds no quantity");
    assert!(
        value <= ORDER_SIZE_USDT,
        "the buy of {} USDT passes the size cap of {} USDT, inside the ceiling of {} USDT",
        decimal(value),
        decimal(ORDER_SIZE_USDT),
        decimal(ORDER_CEILING_USDT)
    );
    assert!(
        value <= EXPOSURE_USDT,
        "the buy of {} USDT passes the exposure cap of {} USDT",
        decimal(value),
        decimal(EXPOSURE_USDT)
    );
    assert!(
        qty >= facts.min_qty && value >= facts.min_notional,
        "the buy of {} BTC, {} USDT, is below the minimum of the venue",
        decimal(qty),
        decimal(value)
    );
    let free_usdt = account
        .balances
        .iter()
        .find(|row| &*row.asset == "USDT")
        .map_or(0, |row| mantissa(&row.free));
    assert!(
        free_usdt - value >= RESERVE_USDT,
        "the buy leaves {} USDT, inside the reserve of {} USDT",
        decimal(free_usdt - value),
        decimal(RESERVE_USDT)
    );
}

fn buy(id: ClientOrderId, price: i64, qty: i64) -> ExecRequest {
    ExecRequest::Place {
        instrument: INSTRUMENT,
        client_id: id,
        side: Side::Buy,
        price: Price::from_mantissa(price),
        qty: Qty::from_mantissa(qty),
        style: OrderStyle::PostOnly,
    }
}

fn open_orders() -> ExecRequest {
    ExecRequest::OpenOrders {
        instrument: INSTRUMENT,
    }
}

fn cancel(id: ClientOrderId) -> ExecRequest {
    ExecRequest::Cancel {
        instrument: INSTRUMENT,
        client_id: id,
    }
}

fn names_order(frame: &Value, client_order_id: &str) -> bool {
    frame["result"].as_array().is_some_and(|rows| {
        rows.iter()
            .any(|row| row["clientOrderId"] == client_order_id)
    })
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
