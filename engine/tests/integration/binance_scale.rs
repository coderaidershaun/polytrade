//! Live check of two Binance behaviours the startup scale check depends on and no offline test can
//! prove: an `exchangeInfo` request that names a symbol returns only that symbol, and an unlisted
//! symbol is refused with the invalid-symbol code. Both calls are public and read-only, so nothing
//! is placed or signed.
//!
//! An agent runs these reads only where a plan names them, by their exact names and with an empty
//! environment.

use engine::config::BinanceMarket;
use engine::venues::binance::preflight::check_order_rate_limits;
use engine::venues::binance::rest::{BinanceEnv, RestClient, RestError};

#[ignore = "live network, public production call: an agent runs it only where a plan names it, by its exact name and with an empty environment"]
#[tokio::test]
async fn a_filtered_exchange_info_returns_only_the_requested_symbol() {
    let mut client = RestClient::new(BinanceMarket::Spot, BinanceEnv::Production)
        .expect("the tls backend builds");

    let info = client
        .exchange_info("btcusdt")
        .await
        .expect("production spot answers a filtered exchangeInfo");

    assert_eq!(
        info.symbols.len(),
        1,
        "an unfiltered request would return the whole catalog"
    );
    assert!(info.symbols[0].symbol.eq_ignore_ascii_case("BTCUSDT"));
    check_order_rate_limits(BinanceMarket::Spot, &info)
        .expect("a filtered payload still carries the ORDERS rate limits");
}

#[ignore = "live network, public production call: an agent runs it only where a plan names it, by its exact name and with an empty environment"]
#[tokio::test]
async fn an_unlisted_symbol_is_rejected_with_the_invalid_symbol_code() {
    let mut client = RestClient::new(BinanceMarket::Spot, BinanceEnv::Production)
        .expect("the tls backend builds");

    let refused = client.exchange_info("zznotlistedzz").await;

    assert!(
        matches!(
            refused,
            Err(RestError::Status {
                code: Some(-1121),
                ..
            })
        ),
        "expected an invalid-symbol rejection, got {refused:?}"
    );
}
