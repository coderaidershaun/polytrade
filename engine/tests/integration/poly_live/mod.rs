//! The signed tests of live Polymarket: one post-only order far below the touch and its cancel, a
//! cancel beside a held marketable buy, the kill switch, the rollback, and the reads of a run.

mod policy;
mod venue;

use std::path::Path;

use engine::msg::exec::OrderStyle;
use engine::msg::ids::Side;
use engine::venues::polymarket::exec::codec::{cancel_venue_order, open_orders_for_token};
use engine::venues::polymarket::exec::lanes::{Lane, RestJob, RestOutcome};
use serde_json::{Value, json};

use crate::binance_live::{Sending, require_real_funds_ack};
use policy::{
    FLOOR_PRICE, IdFile, ONE, ORDER_SIZE_SHARES, Plan, check_orders_each_side, decimal, run_folder,
};
use venue::{OpenOrder, Session, client_id, open_window, touch};

/// The slot of the client id of each order of a run, which salts its signed order.
const RESTING_BUY_SLOT: u64 = 1;
const MARKETABLE_BUY_SLOT: u64 = 2;
const SELL_BACK_SLOT: u64 = 3;

/// The lot of the venue: a size carries at most 2 decimal places.
const SHARE_LOT: i64 = ONE / 100;

/// The reads of a run: before the orders, after them, and after a rollback.
const READ_PHASES: [&str; 3] = ["before", "after", "after-rollback"];

/// Places one post-only buy of 5 shares at 0.01, far below the touch of the token of the tests,
/// writes its venue id to the id file, reads it back resting, and cancels it by that id.
#[ignore = "real funds on live polymarket: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn venue_accepts_one_signed_order() {
    require_real_funds_ack();
    place_and_cancel_one_order(Sending::Live).await;
}

/// The order and its cancel built and printed, and none sent.
#[ignore = "real funds on live polymarket: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn venue_accepts_one_signed_order_dry_run() {
    require_real_funds_ack();
    place_and_cancel_one_order(Sending::DryRun).await;
}

/// Sends a marketable buy and, on another lane at the same time, the cancel of a resting buy of the
/// tests, and prints which answer came first. It holds two buys on one side, so the cap of one
/// order for each side refuses it in code before it sends an order.
#[ignore = "real funds on live polymarket: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn cancel_finishes_while_a_marketable_place_is_held() {
    require_real_funds_ack();
    cancel_beside_a_held_place(Sending::Live).await;
}

/// The resting buy, the marketable buy, and the cancel built and printed, and none sent.
#[ignore = "real funds on live polymarket: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn cancel_finishes_while_a_marketable_place_is_held_dry_run() {
    require_real_funds_ack();
    cancel_beside_a_held_place(Sending::DryRun).await;
}

/// The kill switch: one cancel by venue order id for each order of the id file, then it prints each
/// open order of the token that it left, and each placement that has no venue id. It never cancels
/// by market, token, or account.
#[ignore = "real funds on live polymarket: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn cancels_only_the_orders_of_the_polymarket_tests() {
    require_real_funds_ack();
    cancel_orders_of_the_tests(Sending::Live).await;
}

/// The kill switch with each cancel built and printed, and none sent.
#[ignore = "real funds on live polymarket: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn cancels_only_the_orders_of_the_polymarket_tests_dry_run() {
    require_real_funds_ack();
    cancel_orders_of_the_tests(Sending::DryRun).await;
}

/// The rollback of the marketable buy: one sell of the shares of the token above `before`, at most
/// 5, at the best bid, for one match.
#[ignore = "real funds on live polymarket: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn sells_back_the_shares_of_the_held_placement() {
    require_real_funds_ack();
    sell_back_the_shares(Sending::Live).await;
}

/// The rollback with its sell built and printed, and none sent.
#[ignore = "real funds on live polymarket: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn sells_back_the_shares_of_the_held_placement_dry_run() {
    require_real_funds_ack();
    sell_back_the_shares(Sending::DryRun).await;
}

/// Reads the clock offset, the open orders of the token of the tests, the collateral, and the
/// shares of the token, and writes them to the file of the run folder that `REX_READ_PHASE` names.
/// The reads of `before` choose the token: the Up token of the open window.
#[ignore = "signed reads of live polymarket: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn writes_the_polymarket_reads() {
    require_real_funds_ack();
    write_the_reads().await;
}

/// The reads with one read printed with its signed headers, and nothing written: the key loads and
/// the redaction holds before the first signed request of a run.
#[ignore = "signed reads of live polymarket: only the live-runner role of a job runs it, by the runbook of its job and the live policy"]
#[tokio::test]
async fn writes_the_polymarket_reads_dry_run() {
    require_real_funds_ack();
    let session = Session::open().await;
    let window = open_window().await;
    let read = open_orders_for_token(&window.token, None);
    println!("  request: {}", session.shown_signed(&read));
    println!("  nothing sent by this request, nothing written");
}

async fn place_and_cancel_one_order(sending: Sending) {
    let folder = run_folder();
    let ids = IdFile::in_folder(&folder);
    let token = ids.token();
    let session = Session::open().await;
    let window = open_window().await;
    window.check_for_an_order(&token);
    let (bid, _) = touch(&token).await;
    assert!(
        bid > 2 * FLOOR_PRICE,
        "the best bid {} is at or below 0.02, so a buy at 0.01 would sit at the touch",
        decimal(bid)
    );
    check_orders_each_side(1);
    refuse_an_order_of_the_tests_on("BUY", &session.open_orders(&token).await, &ids);
    let plan = Plan {
        client: client_id(RESTING_BUY_SLOT),
        side: Side::Buy,
        price: FLOOR_PRICE,
        size: ORDER_SIZE_SHARES,
        style: OrderStyle::PostOnly,
    };
    plan.check_inside_caps(session.collateral().await, 0);
    session.check_may_place(sending);
    let place = session.order(&token, window.tick, &plan).await;
    if sending == Sending::DryRun {
        session.send_once(&place, false).await;
        let cancel = cancel_venue_order("<the venue id of the placement>").expect("a cancel body");
        session.send_once(&cancel, false).await;
        println!("  dry run: 2 requests built, none sent");
        return;
    }

    ids.add_pending(plan.client);
    let answer = session.send_once(&place, true).await;
    let order = placed_id(answer.as_ref()).unwrap_or_else(|| {
        panic!(
            "the placement has no venue order id: its pending line stays in the id file. Read the \
             venue (the reads), then run the kill switch"
        )
    });
    ids.place(plan.client, &order);
    println!("  placed: {order}");

    let resting = session.open_orders(&token).await;
    let rests = resting.iter().find(|open| open.id == order);
    println!(
        "  at the venue: {}",
        rests.map_or_else(|| "not listed".to_owned(), ToString::to_string)
    );
    let cancel = cancel_venue_order(&order).expect("a cancel body");
    let answer = session.send_once(&cancel, true).await;
    println!(
        "  cancelled: {}",
        answer
            .as_ref()
            .is_some_and(|(_, body)| is_cancelled(body, &order))
    );
    let left = session.open_orders(&token).await;
    for open in &left {
        println!("  left: {open}");
    }
    assert!(
        rests.is_some(),
        "the venue did not list the order of the test as resting"
    );
    assert!(
        !left.iter().any(|open| open.id == order),
        "the order of the test still rests: run the kill switch"
    );
}

/// What a request of a lane was for: its answer comes back with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Purpose {
    MarketableBuy,
    CancelOfTheRestingBuy,
}

async fn cancel_beside_a_held_place(sending: Sending) {
    let folder = run_folder();
    let ids = IdFile::in_folder(&folder);
    let token = ids.token();
    let session = Session::open().await;
    let window = open_window().await;
    window.check_for_an_order(&token);
    let (bid, ask) = touch(&token).await;
    assert!(
        bid > 2 * FLOOR_PRICE && ask <= ONE,
        "the touch {} / {} leaves no room for a resting buy at 0.01 and a buy at the ask",
        decimal(bid),
        decimal(ask)
    );
    let resting = Plan {
        client: client_id(RESTING_BUY_SLOT),
        side: Side::Buy,
        price: FLOOR_PRICE,
        size: ORDER_SIZE_SHARES,
        style: OrderStyle::PostOnly,
    };
    let marketable = Plan {
        client: client_id(MARKETABLE_BUY_SLOT),
        side: Side::Buy,
        price: ask,
        size: ORDER_SIZE_SHARES,
        style: OrderStyle::Immediate,
    };
    let collateral = session.collateral().await;
    resting.check_inside_caps(collateral, 0);
    marketable.check_inside_caps(collateral, resting.cost());
    session.check_may_place(sending);
    let resting_body = session.order(&token, window.tick, &resting).await;
    let marketable_body = session.order(&token, window.tick, &marketable).await;
    if sending == Sending::DryRun {
        session.send_once(&resting_body, false).await;
        session.send_once(&marketable_body, false).await;
        let cancel =
            cancel_venue_order("<the venue id of the resting buy>").expect("a cancel body");
        session.send_once(&cancel, false).await;
        println!("  dry run: 3 requests built, none sent");
        return;
    }
    check_orders_each_side(2);

    refuse_an_order_of_the_tests_on("BUY", &session.open_orders(&token).await, &ids);
    ids.add_pending(resting.client);
    let answer = session.send_once(&resting_body, true).await;
    let resting_id = placed_id(answer.as_ref()).expect("the resting buy has a venue order id");
    ids.place(resting.client, &resting_id);

    let (lanes, mut outcomes) = session.lanes::<Purpose>();
    ids.add_pending(marketable.client);
    let cancel = cancel_venue_order(&resting_id).expect("a cancel body");
    println!("  request: {}", session.shown_request(&marketable_body));
    println!("  request: {}", session.shown_request(&cancel));
    let held = lanes.submit(
        Lane::Place,
        RestJob::signed(Purpose::MarketableBuy, marketable_body),
    );
    let cancelled = lanes.submit(
        Lane::Control,
        RestJob::signed(Purpose::CancelOfTheRestingBuy, cancel),
    );
    assert!(
        held.is_queued() && cancelled.is_queued(),
        "both lanes take their request"
    );
    let first = outcomes.recv().await.expect("a lane answers");
    let second = outcomes.recv().await.expect("the other lane answers");
    for outcome in [&first, &second] {
        println!(
            "  answer of {:?}: {}",
            outcome.purpose,
            shown_outcome(&session, outcome)
        );
        if outcome.purpose == Purpose::MarketableBuy {
            let order = outcome.answer.as_ref().ok().and_then(|response| {
                let body: Value = serde_json::from_str(&response.body).ok()?;
                placed_id(Some(&(response.status, body)))
            });
            match order {
                Some(order) => ids.place(marketable.client, &order),
                None => println!("  the marketable buy has no venue id: read the venue"),
            }
        }
    }
    let is_cancel_first = first.purpose == Purpose::CancelOfTheRestingBuy;
    println!(
        "  U-V3: {}",
        if is_cancel_first {
            "YES, the cancel answered first"
        } else {
            "NO, the held buy answered first"
        }
    );
}

async fn cancel_orders_of_the_tests(sending: Sending) {
    let folder = run_folder();
    let ids = IdFile::in_folder(&folder);
    let token = ids.token();
    let session = Session::open().await;
    let placed = ids.placed();
    for order in &placed {
        let cancel = cancel_venue_order(order).expect("a cancel body");
        if let Some((_, body)) = session.send_once(&cancel, sending == Sending::Live).await {
            println!("  cancelled {order}: {}", is_cancelled(&body, order));
        }
    }
    for client in ids.pending() {
        println!(
            "  pending: the placement of client {client} has no venue id; read the venue for it"
        );
    }
    let open = session.open_orders(&token).await;
    for order in &open {
        match placed.contains(&order.id) {
            true => println!("  still open, run the kill switch again, at most 3 times: {order}"),
            false => println!("  left: {order}"),
        }
    }
    println!(
        "  orders left that are not the tests': {}",
        open.iter()
            .filter(|order| !placed.contains(&order.id))
            .count()
    );
}

async fn sell_back_the_shares(sending: Sending) {
    let folder = run_folder();
    let ids = IdFile::in_folder(&folder);
    let token = ids.token();
    let before = before_shares(&folder.join("before.json"));
    let session = Session::open().await;
    let above = session.shares(&token).await - before;
    if above <= 0 {
        println!("  nothing to roll back: the shares are not above before");
        return;
    }
    let window = open_window().await;
    let size = (above - above.rem_euclid(SHARE_LOT)).min(ORDER_SIZE_SHARES);
    if size < window.min_size.mantissa() {
        println!(
            "  dust left: {} shares above before, below the minimum order of the venue",
            decimal(above)
        );
        return;
    }
    let (bid, _) = touch(&token).await;
    check_orders_each_side(1);
    refuse_an_order_of_the_tests_on("SELL", &session.open_orders(&token).await, &ids);
    let plan = Plan {
        client: client_id(SELL_BACK_SLOT),
        side: Side::Sell,
        price: bid,
        size,
        style: OrderStyle::Immediate,
    };
    plan.check_inside_caps(session.collateral().await, 0);
    session.check_may_place(sending);
    let sell = session.order(&token, window.tick, &plan).await;
    if sending == Sending::DryRun {
        session.send_once(&sell, false).await;
        println!("  dry run: the sell is built, and none sent");
        return;
    }
    ids.add_pending(plan.client);
    let answer = session.send_once(&sell, true).await;
    match placed_id(answer.as_ref()) {
        Some(order) => ids.place(plan.client, &order),
        None => println!("  the sell has no venue order id: read the venue, then the kill switch"),
    }
}

async fn write_the_reads() {
    let phase = std::env::var("REX_READ_PHASE").unwrap_or_default();
    assert!(
        READ_PHASES.contains(&phase.as_str()),
        "REX_READ_PHASE names one of {READ_PHASES:?}, got {phase:?}"
    );
    let folder = run_folder();
    let path = folder.join(format!("{phase}.json"));
    assert!(
        !path.exists(),
        "{} exists, and a read never writes over another",
        path.display()
    );
    let ids = IdFile::in_folder(&folder);
    let session = Session::open().await;
    if phase == "before" {
        ids.start(&open_window().await.token);
    }
    let token = ids.token();
    let open = session.open_orders(&token).await;
    let placed = ids.placed();
    let reads = json!({
        "skew_us": session.venue_clock_offset().micros(),
        "open_orders": open.iter().map(order_row).collect::<Vec<Value>>(),
        "balances": {
            "collateral": decimal(session.collateral().await),
            "shares": decimal(session.shares(&token).await),
        },
    });
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&session.redactor().redacted(&reads)).expect("json"),
    )
    .expect("the reads file is written");
    println!(
        "  open orders of the token: {}, of the tests: {}",
        open.len(),
        open.iter()
            .filter(|order| placed.contains(&order.id))
            .count()
    );
    println!(
        "  clock offset against the venue: {} us",
        session.venue_clock_offset().micros()
    );
    println!("  written: {}", path.display());
}

/// Refuses a run when an order of the tests already rests on `side`: one order each side.
fn refuse_an_order_of_the_tests_on(side: &str, open: &[OpenOrder], ids: &IdFile) {
    let placed = ids.placed();
    let resting = open
        .iter()
        .find(|order| order.side == side && placed.contains(&order.id));
    assert!(
        resting.is_none(),
        "an order of the tests already rests on the {side} side, and a run holds one each side: {}",
        resting.map(ToString::to_string).unwrap_or_default()
    );
}

/// The venue order id of an accepted placement.
fn placed_id(answer: Option<&(u16, Value)>) -> Option<String> {
    let (status, body) = answer?;
    let is_accepted = (200..300).contains(status) && body["success"] == true;
    let order = body["orderID"].as_str().filter(|order| !order.is_empty())?;
    is_accepted.then(|| order.to_owned())
}

fn is_cancelled(body: &Value, order: &str) -> bool {
    body["canceled"]
        .as_array()
        .is_some_and(|ids| ids.iter().any(|id| id == order))
}

fn shown_outcome(session: &Session, outcome: &RestOutcome<Purpose>) -> String {
    match &outcome.answer {
        Ok(response) => {
            let body = serde_json::from_str(&response.body).unwrap_or(Value::Null);
            format!(
                "http {} {}",
                response.status,
                session.redactor().redacted(&body)
            )
        }
        Err(failure) => format!("no answer: {failure}; the request is not sent again"),
    }
}

fn order_row(order: &OpenOrder) -> Value {
    json!({
        "order_id": order.id,
        "side": order.side,
        "price": order.price,
        "size": order.size,
        "status": order.status,
    })
}

/// The shares of the token of the tests in the `before` file of the run folder.
fn before_shares(path: &Path) -> i64 {
    let text = std::fs::read_to_string(path).unwrap_or_else(|_| {
        panic!(
            "{} is missing: the reads of before come first in a run",
            path.display()
        )
    });
    let reads: Value = serde_json::from_str(&text).expect("the reads file is json");
    let shares = reads["balances"]["shares"]
        .as_str()
        .expect("the reads file holds the shares of the token");
    crate::binance_live::mantissa(shares)
}
