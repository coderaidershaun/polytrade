//! Binance parse goldens: normalised output pinned against real venue payloads — the recorded
//! aggTrade replays, the venue-stamp clamp over the whole i64 space, the perp depth pu/T
//! normalisation, and the REST-kline structural tail rule.
//!
//! No recorder tool exists for `fixtures/`, so these committed goldens are the sole source of
//! truth: to change them, re-record by hand from the live venue.

use polysim::adapters::binance::parse::{
    DepthDiff, ParseContext, RestKlineTail, parse_agg_trade, parse_combined_frame,
    parse_perp_depth_diff, parse_rest_klines,
};
use polysim::adapters::venue_clock::MAX_VENUE_SKEW_US;
use polysim::config::KlineInterval;
use polysim::ids::{InstrumentId, Price, Side};
use polysim::msg::inbound::TradeEvent;
use polysim::time::TsUs;
use proptest::prelude::*;

use crate::engine_support::level;

const INSTRUMENT: InstrumentId = InstrumentId(7);

fn ctx(received_us: i64) -> ParseContext {
    ParseContext {
        instrument: INSTRUMENT,
        received_ts_us: TsUs::from_micros(received_us),
    }
}

mod timestamp_clamp {
    use super::*;

    fn agg_trade_at(trade_ts_ms: i64, received_us: i64) -> TradeEvent {
        let json = format!(r#"{{"a":1,"p":"1","q":"1","f":1,"l":1,"T":{trade_ts_ms},"m":true}}"#);
        parse_agg_trade(&json, ctx(received_us))
            .expect("parses")
            .trade
    }

    proptest! {
        /// Over the entire `i64` venue-stamp space, parsing never panics and the stamp never
        /// escapes the window (parse defends `TsUs::diff`).
        #[test]
        fn clamp_stays_inside_window(ms in any::<i64>(), now_us in 0i64..=4_000_000_000_000_000i64) {
            let clamped = agg_trade_at(ms, now_us).exchange_ts_us.micros();
            prop_assert!(clamped >= now_us.saturating_sub(MAX_VENUE_SKEW_US));
            prop_assert!(clamped <= now_us.saturating_add(MAX_VENUE_SKEW_US));
        }
    }
}

mod venue_send_stamp {
    //! `T` (match time) and `E` (send time) are distinct venue clocks, so `E` needs its own field
    //! without displacing `T`, and must stay optional since not every frame carries it.
    use super::*;

    const RECEIVED_US: i64 = 1_784_410_700_000_000;
    const MATCH_MS: i64 = 1_784_410_699_900;
    const SENT_MS: i64 = 1_784_410_699_950;

    fn agg_trade(stamps: &str) -> TradeEvent {
        let json = format!(r#"{{"a":1,"p":"1","q":"1","f":1,"l":1,{stamps},"m":true}}"#);
        parse_agg_trade(&json, ctx(RECEIVED_US))
            .expect("parses")
            .trade
    }

    #[test]
    fn the_send_stamp_is_optional_clamped_and_never_displaces_the_match_stamp() {
        let both = agg_trade(&format!(r#""E":{SENT_MS},"T":{MATCH_MS}"#));
        assert_eq!(
            both.exchange_ts_us,
            TsUs::from_micros(MATCH_MS * 1_000),
            "the match stamp still comes from T"
        );
        assert_eq!(
            both.exchange_sent_ts_us,
            Some(TsUs::from_micros(SENT_MS * 1_000))
        );

        let absurd = agg_trade(&format!(r#""E":{},"T":{MATCH_MS}"#, i64::MAX));
        assert_eq!(
            absurd.exchange_sent_ts_us,
            Some(TsUs::from_micros(RECEIVED_US + MAX_VENUE_SKEW_US)),
            "E clamps like every other venue stamp"
        );

        let absent = agg_trade(&format!(r#""T":{MATCH_MS}"#));
        assert_eq!(absent.exchange_sent_ts_us, None);
    }
}

mod rest_klines {
    use super::*;

    // 12-tuple rows, two consecutive minutes; trailing taker/ignore slots drained.
    const ROW0: &str = r#"[1672515780000,"0.0010","0.0025","0.0015","0.0020","1000",1672515839999,"1.0000",100,"500","0.500","0"]"#;
    const ROW1: &str = r#"[1672515840000,"0.0020","0.0030","0.0018","0.0028","900",1672515899999,"0.9000",90,"400","0.400","0"]"#;

    fn two_rows() -> String {
        format!("[{ROW0},{ROW1}]")
    }

    // Row 0's close time (…839999 ms).
    const ROW0_CLOSE_US: i64 = 1_672_515_839_999_000;

    #[test]
    fn all_closed_tail_marks_every_row_closed() {
        // Receipt BEFORE both close times: a clock rule would call both still-open. AllClosed is
        // structural — every row of a bounded past window is a genuinely-closed candle.
        let received = ROW0_CLOSE_US - 1;
        let events = parse_rest_klines(
            &two_rows(),
            ctx(received),
            KlineInterval::OneMinute,
            RestKlineTail::AllClosed,
        )
        .expect("parses");
        assert_eq!(events.len(), 2);
        assert!(
            events.iter().all(|event| event.is_closed),
            "AllClosed marks every row closed regardless of the local clock"
        );
        // Fields still decode exactly (locks the decimal and millisecond-to-microsecond boundary).
        assert_eq!(
            events[0].open_ts_us,
            TsUs::from_micros(1_672_515_780_000_000)
        );
        assert_eq!(events[0].close, Price(200_000));
        assert_eq!(events[0].quote_volume, 100_000_000);
        assert_eq!(events[0].trade_count, 100);
    }

    /// A backfill row's only venue time is the candle's own close, which is deliberately far in the
    /// past — research reads it as candle time. Handing it a send stamp it never had would let the
    /// engine measure that age as wire latency, so the absence is load-bearing, not incidental.
    #[test]
    fn a_backfill_row_carries_the_candle_close_and_no_send_stamp() {
        let received = ROW0_CLOSE_US + 6 * 60 * 60 * 1_000_000;
        let events = parse_rest_klines(
            &two_rows(),
            ctx(received),
            KlineInterval::OneMinute,
            RestKlineTail::AllClosed,
        )
        .expect("parses");
        assert_eq!(
            events[0].exchange_ts_us,
            TsUs::from_micros(ROW0_CLOSE_US),
            "hours behind the fetch that carried it"
        );
        assert_eq!(events[0].exchange_sent_ts_us, None);
    }
}

mod depth {
    use super::*;

    // Perp depthUpdate: carries `pu`, `T`, and the post-CM `ps`/`st`.
    const PERP: &str = r#"{"e":"depthUpdate","E":1672515782136,"T":1672515782130,"s":"BTCUSDT","U":157,"u":160,"pu":149,"ps":"BTCUSDT","st":1,"b":[["0.0024","10"]],"a":[["0.0026","100"]]}"#;

    #[test]
    fn perp_carries_pu_and_uses_transaction_time() {
        let received = 1_672_515_782_200_000;
        let diff = parse_perp_depth_diff(PERP, ctx(received)).expect("perp depth parses");
        assert_eq!(
            diff,
            DepthDiff {
                instrument: INSTRUMENT,
                first_update_id: 157,
                final_update_id: 160,
                prev_final_update_id: Some(149),
                bids: vec![level(240_000, 1_000_000_000)],
                asks: vec![level(260_000, 10_000_000_000)],
                exchange_ts_us: TsUs::from_micros(1_672_515_782_130_000),
                received_ts_us: TsUs::from_micros(received),
            }
        );
    }
}

mod recorded_agg_trade {
    //! Every recorded aggTrade frame parses to a sane normalised trade — the regression lock for
    //! the wire shape trades depend on, the way the depth/kline goldens lock theirs.
    use super::*;

    const SPOT: &str = include_str!("../../fixtures/binance/spot_aggTrade.jsonl");
    const PERP: &str = include_str!("../../fixtures/binance/perp_aggTrade.jsonl");

    // The recording ran in a ~60 s window; every venue trade time converts into this µs bracket
    // (proof the ms→µs conversion neither clamped nor mangled the stamps).
    const EPOCH_LO_US: i64 = 1_784_000_000_000_000;
    const EPOCH_HI_US: i64 = 1_785_000_000_000_000;
    const RECORDED_RECEIVED_US: i64 = 1_784_410_700_000_000;

    #[test]
    fn every_recorded_agg_trade_fixture_replays() {
        for (market, fixture) in [("spot", SPOT), ("perp", PERP)] {
            assert_fixture_replays(fixture, market);
        }
    }

    fn assert_fixture_replays(fixture: &str, market: &str) {
        let received = TsUs::from_micros(RECORDED_RECEIVED_US);
        let context = ParseContext {
            instrument: INSTRUMENT,
            received_ts_us: received,
        };
        let mut count = 0usize;
        let mut saw_buy = false;
        let mut saw_sell = false;
        for line in fixture.lines().filter(|line| !line.trim().is_empty()) {
            let frame = parse_combined_frame(line).expect("combined envelope");
            let parsed = parse_agg_trade(&frame.data, context).expect("recorded aggTrade parses");
            assert!(
                parsed.first_trade_id <= parsed.last_trade_id,
                "recorded aggregate covers a forward raw range"
            );
            let event = parsed.trade;
            assert!(event.price.0 > 0, "price mantissa positive");
            assert!(event.qty.0 > 0, "qty mantissa positive");
            assert!(
                (EPOCH_LO_US..EPOCH_HI_US).contains(&event.exchange_ts_us.micros()),
                "exchange stamp within the recording epoch"
            );
            assert!(
                event.exchange_sent_ts_us.is_some(),
                "real venue frames carry the E send stamp"
            );
            assert_eq!(event.received_ts_us, received);
            assert_eq!(event.queued_ts_us, received);
            match event.side {
                Side::Buy => saw_buy = true,
                Side::Sell => saw_sell = true,
            }
            count += 1;
        }
        assert!(
            count > 100,
            "the {market} fixture locks the full recorded window: {count} rows"
        );
        assert!(
            saw_buy && saw_sell,
            "both aggressor sides present in {market}"
        );
    }
}
