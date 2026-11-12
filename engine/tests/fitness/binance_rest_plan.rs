//! Signed-REST fitness: the documented IP weight, HTTP method and authentication of every request
//! this build can issue, and the verdict it draws from a venue failure. Both fail silently if wrong —
//! an under-charged weight earns an IP ban mid-session, and a mis-classified failure either retries a
//! rejected order or gives up on a transient one. The live integration suite never runs in CI, so
//! this is the only standing guard on the table.

use polysim::adapters::binance::rest::{
    AccountInfo, BinanceEnv, FailureVerdict, HttpMethod, KlineQuery, OrderCountWindow, OrderRecord,
    RequestAuth, RestClient, RestError, RestRequest,
};
use polysim::adapters::http::TransportError;
use polysim::config::{BinanceMarket, KlineInterval};

/// Weights verified against <https://developers.binance.com/docs/binance-spot-api-docs/rest-api>
/// on 2026-07-27 — account 20, myTrades 20 without `orderId`, openOrders 6 for one symbol,
/// order 4, cancel 1. The public market-data half must stay exactly as it was too — a weight
/// change there would silently re-tune the resync-storm guard the depth adapter leans on.
#[test]
fn every_request_plans_its_documented_weight_method_and_auth() {
    let cases = [
        (
            RestRequest::AccountInfo,
            20,
            HttpMethod::Get,
            RequestAuth::Signed,
            "account",
        ),
        (
            RestRequest::MyTrades {
                symbol: "btcusdt".to_owned(),
                from_id: Some(42),
                limit: 500,
            },
            20,
            HttpMethod::Get,
            RequestAuth::Signed,
            "myTrades",
        ),
        (
            RestRequest::OpenOrders {
                symbol: "btcusdt".to_owned(),
            },
            6,
            HttpMethod::Get,
            RequestAuth::Signed,
            "openOrders",
        ),
        (
            RestRequest::OrderStatus {
                symbol: "btcusdt".to_owned(),
                orig_client_order_id: "poly-1".to_owned(),
            },
            4,
            HttpMethod::Get,
            RequestAuth::Signed,
            "order",
        ),
        (
            RestRequest::CancelOrder {
                symbol: "btcusdt".to_owned(),
                orig_client_order_id: "poly-1".to_owned(),
            },
            1,
            HttpMethod::Delete,
            RequestAuth::Signed,
            "cancelOrder",
        ),
        (
            RestRequest::DepthSnapshot {
                symbol: "btcusdt".to_owned(),
                limit: 1000,
            },
            50,
            HttpMethod::Get,
            RequestAuth::Public,
            "depth",
        ),
        (
            RestRequest::Klines(KlineQuery {
                symbol: "btcusdt".to_owned(),
                interval: KlineInterval::OneMinute,
                limit: 500,
                start_ts_ms: None,
                end_ts_ms: None,
            }),
            2,
            HttpMethod::Get,
            RequestAuth::Public,
            "klines",
        ),
        (
            RestRequest::ExchangeInfo {
                symbol: "btcusdt".to_owned(),
            },
            20,
            HttpMethod::Get,
            RequestAuth::Public,
            "exchangeInfo",
        ),
        (
            RestRequest::ServerTime,
            1,
            HttpMethod::Get,
            RequestAuth::Public,
            "time",
        ),
    ];

    for (request, weight, method, auth, endpoint) in cases {
        let plan = request.plan(BinanceMarket::Spot);
        assert_eq!(plan.weight, weight, "weight for {endpoint}");
        assert_eq!(plan.method, method, "method for {endpoint}");
        assert_eq!(plan.auth, auth, "auth for {endpoint}");
        assert_eq!(plan.endpoint, endpoint);
    }
}

/// Spot's unfiltered catalog is about 17 MB, enough to time out a startup on a slow link, so a
/// spot request must always name its one symbol. Perpetual ignores the parameter, so none is sent.
#[test]
fn exchange_info_asks_the_venue_for_one_symbol_on_spot_and_none_on_perp() {
    let request = RestRequest::ExchangeInfo {
        symbol: "btcusdt".to_owned(),
    };

    assert_eq!(
        request.plan(BinanceMarket::Spot).query,
        vec![("symbol", "BTCUSDT".to_owned())]
    );
    assert!(request.plan(BinanceMarket::Perpetual).query.is_empty());
}

/// The public client must refuse a private request rather than send it unsigned. Reached without a
/// socket: the guard fires before any connection is opened.
#[tokio::test]
async fn the_public_client_refuses_a_private_request() {
    let mut client = RestClient::new(BinanceMarket::Spot, BinanceEnv::Production)
        .expect("the tls backend builds");

    let refused = client.fetch_text(&RestRequest::AccountInfo).await;

    assert!(
        matches!(
            refused,
            Err(RestError::RequiresSignature {
                endpoint: "account"
            })
        ),
        "expected a signature refusal, got {refused:?}"
    );
}

/// Every `RestError` shape this build can construct, mapped to its verdict. `-2011`/`-2013` are
/// routine (a cancel losing a race to a fill is normal traffic for a market maker), an unrecognised
/// or absent code is never retried (retrying an unhandled failure against an order endpoint is how
/// one bug becomes a burst of duplicate orders), and `-2010 NEW_ORDER_REJECTED` is TWO failures
/// wearing one code — only the message tells a post-only cross (routine) from real insufficient
/// balance (fatal), and an unrecognised -2010 message must not be filed as routine either.
#[test]
fn venue_failure_classification() {
    let status = |code: Option<i64>, message: Option<&str>| RestError::Status {
        url: "https://api.binance.com/api/v3/order".to_owned(),
        status: 400,
        code,
        message: message.map(str::to_owned),
    };

    let cases: Vec<(&str, RestError, FailureVerdict)> = vec![
        (
            "-1021 recvWindow",
            status(Some(-1021), None),
            FailureVerdict::Retry,
        ),
        (
            "-1003 too many requests",
            status(Some(-1003), None),
            FailureVerdict::Retry,
        ),
        (
            "-1006 unexpected response",
            status(Some(-1006), None),
            FailureVerdict::Retry,
        ),
        (
            "-1007 backend timeout",
            status(Some(-1007), None),
            FailureVerdict::Retry,
        ),
        (
            "-1000 unknown",
            status(Some(-1000), None),
            FailureVerdict::Retry,
        ),
        (
            "-1001 disconnected",
            status(Some(-1001), None),
            FailureVerdict::Retry,
        ),
        (
            "-2013 order does not exist",
            status(Some(-2013), None),
            FailureVerdict::Routine,
        ),
        (
            "-2011 unknown order",
            status(Some(-2011), None),
            FailureVerdict::Routine,
        ),
        (
            "-1022 bad signature",
            status(Some(-1022), None),
            FailureVerdict::Fatal,
        ),
        (
            "-2014 bad api key format",
            status(Some(-2014), None),
            FailureVerdict::Fatal,
        ),
        (
            "-2015 invalid key/ip/permissions",
            status(Some(-2015), None),
            FailureVerdict::Fatal,
        ),
        (
            "-1100 illegal characters",
            status(Some(-1100), None),
            FailureVerdict::Fatal,
        ),
        (
            "-1101 too many parameters",
            status(Some(-1101), None),
            FailureVerdict::Fatal,
        ),
        (
            "-1102 mandatory parameter missing",
            status(Some(-1102), None),
            FailureVerdict::Fatal,
        ),
        (
            "unrecognised code",
            status(Some(-9999), Some("something the docs never listed")),
            FailureVerdict::Fatal,
        ),
        ("absent code", status(None, None), FailureVerdict::Fatal),
        (
            "rejected key (Unauthorized variant)",
            RestError::Unauthorized {
                url: "https://api.binance.com/api/v3/account".to_owned(),
                status: 401,
                code: Some(-2015),
                message: None,
            },
            FailureVerdict::Fatal,
        ),
        (
            "timed out reading the body (Timeout variant)",
            RestError::Transport(TransportError::Timeout {
                url: "https://api.binance.com/api/v3/exchangeInfo".to_owned(),
                phase: "reading the response body",
                limit_secs: 10,
            }),
            FailureVerdict::Retry,
        ),
        (
            "rate limited (RateLimited variant)",
            RestError::RateLimited {
                url: "https://api.binance.com/api/v3/account".to_owned(),
                status: 429,
                retry_after_secs: Some(5),
                code: None,
                message: None,
            },
            FailureVerdict::Retry,
        ),
        (
            "2010 post-only cross",
            status(Some(-2010), Some("Order would immediately match and take.")),
            FailureVerdict::Routine,
        ),
        (
            "2010 insufficient balance",
            status(
                Some(-2010),
                Some("Account has insufficient balance for requested action."),
            ),
            FailureVerdict::Fatal,
        ),
        (
            "2010 unrecognised message",
            status(Some(-2010), Some("Some reason the docs never listed.")),
            FailureVerdict::Fatal,
        ),
        (
            "2010 no message",
            status(Some(-2010), None),
            FailureVerdict::Fatal,
        ),
    ];

    for (name, error, want) in cases {
        assert_eq!(error.verdict(), want, "{name}");
    }
}

/// Order counts arrive over whatever intervals the venue chooses, so the interval is read from the
/// header name. Matching a fixed `-1m` the way IP weight does would silently observe nothing.
#[test]
fn order_count_intervals_come_from_the_header_name() {
    assert_eq!(
        OrderCountWindow::interval_of_header("x-mbx-order-count-10s"),
        Some("10s")
    );
    assert_eq!(
        OrderCountWindow::interval_of_header("x-mbx-order-count-1d"),
        Some("1d")
    );
    assert_eq!(
        OrderCountWindow::interval_of_header("x-mbx-order-count-"),
        None
    );
    assert_eq!(
        OrderCountWindow::interval_of_header("x-mbx-used-weight-1m"),
        None
    );
}

/// Shape recorded from a live `GET /api/v3/account` on 2026-07-27, trimmed to the fields the engine
/// reads plus enough neighbours to prove unknown ones are ignored rather than fatal.
const LIVE_ACCOUNT: &str = r#"{
  "makerCommission": 10,
  "takerCommission": 10,
  "buyerCommission": 0,
  "sellerCommission": 0,
  "commissionRates": {
    "maker": "0.00100000",
    "taker": "0.00100000",
    "buyer": "0.00000000",
    "seller": "0.00000000"
  },
  "canTrade": true,
  "canWithdraw": true,
  "canDeposit": true,
  "brokered": false,
  "requireSelfTradePrevention": false,
  "preventSor": false,
  "updateTime": 1769500000000,
  "accountType": "SPOT",
  "balances": [
    { "asset": "BTC", "free": "0.00135871", "locked": "0.00000000" },
    { "asset": "USDT", "free": "171.14535000", "locked": "0.00000000" },
    { "asset": "EDG", "free": "0.00000000", "locked": "0.00000000" },
    { "asset": "EON", "free": "0.00000000", "locked": "0.00000000" }
  ],
  "permissions": ["TRD_GRP_015"],
  "uid": 123456789
}"#;

/// Two REST response shapes decoded directly: the live account report, and a cancel
/// acknowledgement — which is an order record plus the id it cancelled, the field that tells the
/// engine WHICH of its orders is gone.
#[test]
fn rest_json_shapes_decode_and_ignore_unknown_fields() {
    let account: AccountInfo = serde_json::from_str(LIVE_ACCOUNT).expect("the live shape decodes");
    assert!(account.can_trade);
    assert_eq!(&*account.account_type, "SPOT");
    assert_eq!(&*account.commission_rates.maker, "0.00100000");
    assert_eq!(&*account.commission_rates.taker, "0.00100000");
    assert_eq!(account.permissions.len(), 1);
    assert_eq!(account.balances.len(), 4);

    let raw = r#"{
      "symbol": "BTCUSDT",
      "origClientOrderId": "poly-1",
      "orderId": 28457,
      "orderListId": -1,
      "clientOrderId": "cancel-abc",
      "transactTime": 1769500001000,
      "price": "118000.00",
      "origQty": "0.00008000",
      "executedQty": "0.00000000",
      "cummulativeQuoteQty": "0.00000000",
      "status": "CANCELED",
      "timeInForce": "GTC",
      "type": "LIMIT",
      "side": "BUY"
    }"#;
    let order: OrderRecord = serde_json::from_str(raw).expect("a cancel ack decodes");
    assert_eq!(order.orig_client_order_id.as_deref(), Some("poly-1"));
    assert_eq!(&*order.client_order_id, "cancel-abc");
    assert_eq!(order.transact_time_ms, Some(1769500001000));
    assert_eq!(order.time_ms, None);
}
