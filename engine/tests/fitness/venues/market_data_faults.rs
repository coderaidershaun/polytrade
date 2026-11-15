//! The faults that end a market-data feed. Each one halts the engine with a reason of its own, and
//! only a decimal too large for the fixed-point range turns a parse fault into one.

use engine::msg::failure::{FailureClass, Halt};
use engine::msg::ids::{InstrumentId, SourceId};
use engine::msg::time::LocalTs;
use engine::runtime::shutdown::{FatalReason, FatalSignal};
use engine::venues::binance::parse::{
    ParseContext, ParseError as BinanceParseError, parse_agg_trade, parse_combined_frame,
};
use engine::venues::edge::decode::DecimalFault;
use engine::venues::edge::fault::{MarketDataError, halt_on_market_data_error};
use engine::venues::polymarket::parse::{ParseError as PolymarketParseError, parse_market_frame};

const SPOT_TRADES: &str = include_str!("../../../fixtures/binance/spot_aggTrade.jsonl");
const POLYMARKET_MARKET: &str = include_str!("../../../fixtures/polymarket/poly_market.jsonl");
const OVERFLOW: &str = "99999999999.00000000";
const RECEIVED_US: i64 = 1_784_410_630_200_000;

fn received() -> LocalTs {
    LocalTs::from_micros(RECEIVED_US)
}

/// The first line of a recording, with the first string value of `key` replaced.
fn first_line_with(recording: &str, key: &str, value: &str) -> String {
    let line = recording
        .lines()
        .next()
        .expect("the recording holds a line");
    let opening = format!("\"{key}\":\"");
    let start = line.find(&opening).expect("the line holds the key") + opening.len();
    let end = start + line[start..].find('"').expect("the value is closed");
    format!("{}{value}{}", &line[..start], &line[end..])
}

fn binance_trade(frame: &str) -> Result<(), BinanceParseError> {
    let frame = parse_combined_frame(frame)?;
    let context = ParseContext {
        instrument: InstrumentId(0),
        received_ts_us: received(),
    };
    parse_agg_trade(&frame.data, context).map(drop)
}

/// CONTRACT: a fault of a market-data feed halts the engine with its own reason. The feed returns
/// the fault and never trips; its reconnect loop reads the class, writes one ERROR line, and trips
/// the latch with the reason of the same name. The latch keeps the first reason, so a second fault
/// changes nothing.
#[test]
fn market_data_faults_halt_the_engine_with_their_reason() {
    let unreadable = MarketDataError::Unreadable {
        source_id: SourceId(1),
        field: "price",
        value: "99999999999.0".into(),
    };
    let exhausted = MarketDataError::EpochExhausted {
        source_id: SourceId(2),
    };
    let cases = [
        (
            &unreadable,
            FatalReason::MarketDataUnreadable {
                source_id: SourceId(1),
            },
        ),
        (
            &exhausted,
            FatalReason::EpochExhausted {
                source_id: SourceId(2),
            },
        ),
    ];
    for (error, reason) in cases {
        assert_eq!(
            error.failure_class(),
            FailureClass::Halt(Halt::Engine),
            "{error}"
        );
        assert_eq!(error.fatal_reason(), reason, "{error}");
        let fatal = FatalSignal::new();
        halt_on_market_data_error(error, &fatal);
        assert!(fatal.is_tripped(), "{error}");
        assert_eq!(fatal.reason(), Some(reason), "{error}");
    }

    let fatal = FatalSignal::new();
    halt_on_market_data_error(&unreadable, &fatal);
    halt_on_market_data_error(&exhausted, &fatal);
    assert_eq!(
        fatal.reason(),
        Some(FatalReason::MarketDataUnreadable {
            source_id: SourceId(1)
        }),
        "the latch keeps the first reason"
    );
}

/// CONTRACT: a decimal too large for the fixed-point range ends the feed of either venue with a
/// market-data fault, because a truncated number would be a wrong price. Each other parse fault
/// gives none: the feed drops the frame and counts it.
#[test]
fn an_overflowing_decimal_ends_the_feed_with_a_market_data_fault() {
    let binance = binance_trade(&first_line_with(SPOT_TRADES, "p", OVERFLOW))
        .expect_err("a price above the fixed-point range does not parse");
    assert!(binance.is_fatal(), "{binance}");
    let BinanceParseError::Decode(binance) = binance else {
        panic!("a decimal fault, got {binance:?}");
    };
    let polymarket = parse_market_frame(
        &first_line_with(POLYMARKET_MARKET, "size", OVERFLOW),
        received(),
    )
    .expect_err("a size above the fixed-point range does not parse");
    assert!(polymarket.is_fatal(), "{polymarket}");
    let PolymarketParseError::Decode(polymarket) = polymarket else {
        panic!("a decimal fault, got {polymarket:?}");
    };

    for (source_id, field, fault) in [
        (SourceId(1), "price", &binance),
        (SourceId(2), "book size", &polymarket),
    ] {
        assert!(
            matches!(fault, DecimalFault::MantissaOverflow { field: seen, value } if *seen == field && &**value == OVERFLOW),
            "{fault:?}"
        );
        let error = MarketDataError::of_decimal_fault(source_id, fault);
        assert_eq!(
            error,
            Some(MarketDataError::Unreadable {
                source_id,
                field,
                value: OVERFLOW.into(),
            })
        );
        let fatal = FatalSignal::new();
        halt_on_market_data_error(&error.expect("a market-data fault"), &fatal);
        assert_eq!(
            fatal.reason(),
            Some(FatalReason::MarketDataUnreadable { source_id })
        );
    }

    let unknown_side = parse_market_frame(
        r#"{"event_type":"last_trade_price","asset_id":"T","price":"0.5","size":"1","side":"MAYBE","timestamp":"1000"}"#,
        received(),
    )
    .expect_err("an unknown side does not parse");
    assert!(
        !unknown_side.is_fatal() && matches!(unknown_side, PolymarketParseError::Side { .. }),
        "an unknown side holds no decimal fault, so it cannot become a market-data fault: {unknown_side:?}"
    );
    let Err(BinanceParseError::Decode(not_numeric)) =
        binance_trade(&first_line_with(SPOT_TRADES, "p", "abc"))
    else {
        panic!("a price that is not a number is a decimal fault");
    };
    let Err(BinanceParseError::Decode(not_json)) = binance_trade("{") else {
        panic!("text that is not JSON is a decimal fault");
    };
    let spelled_side = DecimalFault::UnknownEnum {
        field: "side",
        value: "MAYBE".into(),
    };
    for fault in [&not_numeric, &not_json, &spelled_side] {
        assert!(!fault.is_fatal(), "{fault:?}");
        assert_eq!(
            MarketDataError::of_decimal_fault(SourceId(1), fault),
            None,
            "{fault:?}"
        );
    }
}
