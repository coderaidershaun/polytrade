//! Live signed-REST check against production Binance: prove the whole private path — credential
//! load, clock sync, HMAC signature, verbatim query, API-key header — actually authenticates, which
//! no offline test can. Read-only by construction.
//!
//! DELETE IS DELIBERATELY ABSENT. `SignedRestClient::cancel_order` is built and shipped in this
//! chunk, but exercising it here would cancel whatever is resting on the real account, and the
//! operator may have placed those orders by hand. A cancel is not idempotent from their point of
//! view: the order does not come back. The cancel path gets its proof against the deterministic fake
//! venue instead, where a destroyed order costs nothing.
//!
//! Run: `cargo test --test integration -- --ignored --nocapture`

use polysim::adapters::binance::exec::{ExecutionPreflight, preflight_execution};
use polysim::adapters::binance::rest::BinanceEnv;
use polysim::adapters::rest_quiet::SharedRestQuiet;

const SYMBOL: &str = "BTCUSDT";

/// The gate C8 will call at startup, run end to end against production. Asserts what the key must be
/// able to do; logs, never asserts, what is account-specific (permissions, balances, fee tier).
#[ignore = "live network, signed production call — agent-run: cargo test --test integration -- --ignored --nocapture"]
#[tokio::test]
async fn the_permission_probe_authenticates_against_production() {
    let ExecutionPreflight {
        rest: client,
        probe,
        ..
    } = preflight_execution(BinanceEnv::Production, SharedRestQuiet::new())
        .await
        .expect("the configured key must load, sign and pass the permission probe");

    println!(
        "  clock offset: {}us",
        client.clock_offset().correction().micros()
    );
    println!(
        "  commission: maker {} taker {}",
        probe.commission_rates.maker, probe.commission_rates.taker
    );
    println!("  permissions: {:?}", probe.permissions);
    for balance in probe.funded_balances() {
        println!(
            "  balance {}: {} free, {} locked",
            balance.asset, balance.free, balance.locked
        );
    }

    assert!(probe.can_trade, "the key must be able to trade");
    assert_eq!(&*probe.account_type, "SPOT");
    assert!(
        !probe.commission_rates.maker.is_empty(),
        "commission rates must be present — position sizing prices against them"
    );
}

/// The three read-only private endpoints, over one client, so a signature failure on any of them
/// surfaces here rather than mid-session. Existence asserts only: the account's contents are the
/// operator's business and change between runs.
#[ignore = "live network, signed production call — agent-run: cargo test --test integration -- --ignored --nocapture"]
#[tokio::test]
async fn signed_reads_return_well_formed_payloads() {
    let ExecutionPreflight {
        rest: mut client, ..
    } = preflight_execution(BinanceEnv::Production, SharedRestQuiet::new())
        .await
        .expect("the configured key must pass the permission probe");

    let open_orders = client
        .open_orders(SYMBOL)
        .await
        .expect("openOrders must authenticate");
    println!("  {SYMBOL} open orders: {}", open_orders.len());
    for order in &open_orders {
        println!(
            "    {} {} {} @ {} ({})",
            order.client_order_id, order.side, order.orig_qty, order.price, order.status
        );
        assert_eq!(&*order.symbol, SYMBOL);
    }

    let trades = client
        .my_trades(SYMBOL, None, 10)
        .await
        .expect("myTrades must authenticate");
    println!("  {SYMBOL} recent fills: {}", trades.len());
    for trade in trades.iter().take(3) {
        println!(
            "    #{} {} @ {} fee {} {}",
            trade.id, trade.qty, trade.price, trade.commission, trade.commission_asset
        );
        assert_eq!(&*trade.symbol, SYMBOL);
        assert!(trade.time_ms > 0, "a fill must carry an exchange timestamp");
    }
}

/// A signed request for an order that cannot exist must come back as a clean `Routine` verdict, not
/// as an authentication failure — this is the distinction the reconciler will lean on to tell "the
/// order is gone" from "the key stopped working".
#[ignore = "live network, signed production call — agent-run: cargo test --test integration -- --ignored --nocapture"]
#[tokio::test]
async fn a_missing_order_reads_as_routine_not_as_a_credential_failure() {
    use polysim::adapters::binance::rest::FailureVerdict;

    let ExecutionPreflight {
        rest: mut client, ..
    } = preflight_execution(BinanceEnv::Production, SharedRestQuiet::new())
        .await
        .expect("the configured key must pass the permission probe");

    let absent = client
        .order_status(SYMBOL, "polysim-integration-no-such-order")
        .await;

    let Err(failure) = absent else {
        panic!("an invented client order id must not resolve to a real order");
    };
    println!("  absent order -> {failure}");
    assert_eq!(
        failure.verdict(),
        FailureVerdict::Routine,
        "a missing order is routine; a Fatal verdict here would halt a reconciler that should carry on"
    );
}
