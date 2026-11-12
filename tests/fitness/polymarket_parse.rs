//! Polymarket parse + normalise goldens: the ascending-order trap (venue best = last, engine best =
//! first), absolute-size delta application vs a `BTreeMap` model, decimal exactness, the ms→µs
//! stamp clamp, and that control text never masquerades as a data frame.

use std::collections::BTreeMap;

use polysim::adapters::decode::DecimalFault;
use polysim::adapters::polymarket::book::{BookNormaliser, ChunkStamps};
use polysim::adapters::polymarket::parse::{ParseError, PolyFrame, parse_market_frame};
use polysim::adapters::venue_clock::MAX_VENUE_SKEW_US;
use polysim::hot::book::{Book, SnapshotOutcome};
use polysim::ids::{InstrumentId, Price, Qty, Side};
use polysim::msg::inbound::{BookChunkKind, InboundMessage, Level};
use polysim::time::TsUs;
use proptest::prelude::*;

use crate::engine_support::level;

const INSTRUMENT: InstrumentId = InstrumentId(9);
const BOOK_CAPACITY: usize = 128;
const RECEIVED_US: i64 = 1_784_439_700_000_000;

fn received() -> TsUs {
    TsUs::from_micros(RECEIVED_US)
}

fn stamps(exchange_ts_us: TsUs) -> ChunkStamps {
    ChunkStamps {
        exchange_ts_us,
        received_ts_us: received(),
    }
}

fn parse(text: &str) -> PolyFrame {
    parse_market_frame(text, received()).expect("frame parses")
}

pub(crate) fn apply(book: &mut Book, messages: &[InboundMessage]) {
    for message in messages {
        let InboundMessage::Book(chunk) = message else {
            continue;
        };
        match chunk.kind {
            // A re-emitted snapshot on a still-valid book is a legitimate ImplicitReset; the replays
            // using this helper track no book-derived state, so either outcome is fine.
            BookChunkKind::Snapshot => match book.apply_snapshot_chunk(chunk) {
                SnapshotOutcome::Clean | SnapshotOutcome::ImplicitReset => {}
            },
            BookChunkKind::Delta => book.apply_delta_chunk(chunk),
        }
    }
}

// Venue sends bids ascending (0.48,0.49,0.50) and asks in the inverse order (0.53,0.52,0.51); after
// normalisation the touch is bids[0]=0.50 / asks[0]=0.51 regardless of wire order.
const BOOK: &str = r#"{"event_type":"book","market":"0xMKT","asset_id":"TOKEN_UP","timestamp":"1784439695963","hash":"7483277d",
 "bids":[{"price":"0.48","size":"100"},{"price":"0.49","size":"200"},{"price":"0.50","size":"300"}],
 "asks":[{"price":"0.53","size":"60"},{"price":"0.52","size":"40"},{"price":"0.51","size":"20"}]}"#;

mod book_snapshot {
    use super::*;

    #[test]
    fn normalises_ascending_arrays_to_best_first() {
        let PolyFrame::Book(book) = parse(BOOK) else {
            panic!("book frame classifies as Book");
        };
        assert_eq!(&*book.asset_id, "TOKEN_UP");
        assert_eq!(
            book.bids.first().copied(),
            Some(level(50_000_000, 30_000_000_000))
        );
        assert_eq!(
            book.asks.first().copied(),
            Some(level(51_000_000, 2_000_000_000))
        );
        // full ordering: bids from high to low, asks from low to high
        assert_eq!(
            book.bids,
            vec![
                level(50_000_000, 30_000_000_000),
                level(49_000_000, 20_000_000_000),
                level(48_000_000, 10_000_000_000),
            ]
        );
        assert_eq!(
            book.asks,
            vec![
                level(51_000_000, 2_000_000_000),
                level(52_000_000, 4_000_000_000),
                level(53_000_000, 6_000_000_000),
            ]
        );
        assert_eq!(
            book.exchange_ts_us,
            TsUs::from_micros(1_784_439_695_963_000)
        );
    }
}

mod price_change {
    use super::*;

    #[test]
    fn batches_both_tokens_with_absolute_size_and_side() {
        let frame = r#"{"event_type":"price_change","market":"0xMKT","timestamp":"1784439695997",
         "price_changes":[
           {"asset_id":"TOKEN_UP","price":"0.01","size":"18099.33","side":"BUY","hash":"933e","best_bid":"0.5","best_ask":"0.51"},
           {"asset_id":"TOKEN_DOWN","price":"0.99","size":"18099.33","side":"SELL","hash":"316c","best_bid":"0.49","best_ask":"0.5"}]}"#;
        let PolyFrame::PriceChange(change) = parse(frame) else {
            panic!("price_change frame");
        };
        assert_eq!(change.changes.len(), 2);
        assert_eq!(&*change.changes[0].asset_id, "TOKEN_UP");
        assert_eq!(change.changes[0].side, Side::Buy);
        assert_eq!(change.changes[0].level, level(1_000_000, 1_809_933_000_000));
        assert_eq!(change.changes[0].best_bid, Some(Price(50_000_000)));
        assert_eq!(change.changes[1].side, Side::Sell);
        assert_eq!(change.changes[1].best_ask, Some(Price(50_000_000)));
    }

    #[test]
    fn zero_size_removes_the_level_against_a_model() {
        let PolyFrame::Book(snapshot) = parse(BOOK) else {
            panic!("book frame");
        };
        // remove the 0.50 bid, add a 0.47 bid, remove the 0.51 ask
        let frame = r#"{"event_type":"price_change","market":"0xMKT","timestamp":"1784439696100",
         "price_changes":[
           {"asset_id":"TOKEN_UP","price":"0.50","size":"0","side":"BUY","best_bid":"0.49","best_ask":"0.52"},
           {"asset_id":"TOKEN_UP","price":"0.47","size":"500","side":"BUY","best_bid":"0.49","best_ask":"0.52"},
           {"asset_id":"TOKEN_UP","price":"0.51","size":"0","side":"SELL","best_bid":"0.49","best_ask":"0.52"}]}"#;
        let PolyFrame::PriceChange(change) = parse(frame) else {
            panic!("price_change frame");
        };

        let mut normaliser = BookNormaliser::new(INSTRUMENT);
        let mut book = Book::new(BOOK_CAPACITY);
        let mut out = Vec::new();
        normaliser.emit_snapshot(&snapshot, &mut |m| out.push(m));
        normaliser.emit_price_change(&change.changes, stamps(change.exchange_ts_us), &mut |m| {
            out.push(m)
        });
        apply(&mut book, &out);

        assert_eq!(
            book.bids().to_vec(),
            vec![
                level(49_000_000, 20_000_000_000),
                level(48_000_000, 10_000_000_000),
                level(47_000_000, 50_000_000_000),
            ]
        );
        assert_eq!(
            book.asks().to_vec(),
            vec![
                level(52_000_000, 4_000_000_000),
                level(53_000_000, 6_000_000_000),
            ]
        );
    }
}

mod chunk_stamp {
    //! The frame `timestamp` must reach every chunk it produced without a snapshot inheriting a
    //! later delta's stamp, or exchange-to-receipt latency silently reads blank for Polymarket.
    use super::*;

    const CHANGE: &str = r#"{"event_type":"price_change","market":"0xMKT","timestamp":"1784439696100",
     "price_changes":[{"asset_id":"TOKEN_UP","price":"0.47","size":"500","side":"BUY","best_bid":"0.50","best_ask":"0.51"}]}"#;

    fn emitted_stamps(messages: &[InboundMessage], kind: BookChunkKind) -> Vec<Option<TsUs>> {
        messages
            .iter()
            .filter_map(|message| match message {
                InboundMessage::Book(chunk) if chunk.kind == kind => Some(chunk.exchange_ts_us),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn each_chunk_carries_the_stamp_of_the_frame_that_produced_it() {
        let PolyFrame::Book(snapshot) = parse(BOOK) else {
            panic!("book frame");
        };
        let PolyFrame::PriceChange(change) = parse(CHANGE) else {
            panic!("price_change frame");
        };
        assert_ne!(snapshot.exchange_ts_us, change.exchange_ts_us);

        let mut normaliser = BookNormaliser::new(INSTRUMENT);
        let mut out = Vec::new();
        normaliser.emit_snapshot(&snapshot, &mut |m| out.push(m));
        normaliser.emit_price_change(&change.changes, stamps(change.exchange_ts_us), &mut |m| {
            out.push(m)
        });

        let from_snapshot = emitted_stamps(&out, BookChunkKind::Snapshot);
        let from_delta = emitted_stamps(&out, BookChunkKind::Delta);
        assert!(!from_snapshot.is_empty() && !from_delta.is_empty());
        assert!(
            from_snapshot
                .iter()
                .all(|stamp| *stamp == Some(snapshot.exchange_ts_us)),
            "snapshot chunks: {from_snapshot:?}"
        );
        assert!(
            from_delta
                .iter()
                .all(|stamp| *stamp == Some(change.exchange_ts_us)),
            "delta chunks: {from_delta:?}"
        );
    }
}

mod trade {
    use super::*;

    #[test]
    fn last_trade_price_maps_to_a_trade() {
        let frame = r#"{"event_type":"last_trade_price","market":"0xMKT","asset_id":"TOKEN_UP","price":"0.51","size":"7.843136","fee_rate_bps":"0","side":"BUY","timestamp":"1784439696531","transaction_hash":"0xd36c"}"#;
        let PolyFrame::Trade(trade) = parse(frame) else {
            panic!("trade frame");
        };
        assert_eq!(&*trade.asset_id, "TOKEN_UP");
        assert_eq!(trade.price, Price(51_000_000));
        assert_eq!(trade.qty, Qty(784_313_600));
        assert_eq!(trade.side, Side::Buy);
        assert_eq!(
            trade.exchange_ts_us,
            TsUs::from_micros(1_784_439_696_531_000)
        );
    }
}

mod control_and_unknown {
    use super::*;

    // The driver matches the venue's `PONG` keepalive itself, before the text ever reaches the
    // parser. Nothing else on this socket is allowed to vanish quietly: a frame the parser cannot
    // read is frame loss, and the drop counter exists to make exactly that visible.
    #[test]
    fn text_that_does_not_parse_is_a_countable_error() {
        for text in ["PONG", "INVALID OPERATION", "{\"event_type\":"] {
            let error = parse_market_frame(text, received())
                .expect_err("unreadable text must reach the drop counter");
            assert!(!error.is_fatal(), "{text} should drop and count, not halt");
        }
    }

    #[test]
    fn tolerated_or_ignored_frame_shapes() {
        type FrameCase = (&'static str, &'static str, fn(&PolyFrame) -> bool);
        let cases: &[FrameCase] = &[
            (
                "unknown_event_type_is_ignored",
                r#"{"event_type":"some_future_type","asset_id":"T"}"#,
                |f| *f == PolyFrame::Ignored,
            ),
            (
                "appended_fields_are_tolerated",
                r#"{"event_type":"last_trade_price","asset_id":"T","price":"0.5","size":"1","side":"SELL","timestamp":"1784439696531","future_field_2099":{"nested":true}}"#,
                |f| matches!(f, PolyFrame::Trade(_)),
            ),
        ];
        for (name, frame, expect) in cases {
            let parsed = parse(frame);
            assert!(expect(&parsed), "{name}: got {parsed:?}");
        }
    }
}

mod tick_size_change {
    use super::*;

    #[test]
    fn parses_documented_and_bare_shapes() {
        let full = r#"{"event_type":"tick_size_change","asset_id":"T","market":"0xM","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1784439695963"}"#;
        let PolyFrame::TickSizeChange(change) = parse(full) else {
            panic!("tick_size_change frame");
        };
        assert_eq!(change.asset_id.as_deref(), Some("T"), "documented_shape");
        assert_eq!(
            change.new_tick_size.as_deref(),
            Some("0.001"),
            "documented_shape"
        );

        let bare = r#"{"event_type":"tick_size_change"}"#;
        let PolyFrame::TickSizeChange(change) = parse(bare) else {
            panic!("tick_size_change frame");
        };
        assert_eq!(change.asset_id, None, "bare_shape");
        assert_eq!(change.exchange_ts_us, received(), "bare_shape");
    }
}

mod decimals {
    use super::*;

    fn trade_size(size: &str) -> Result<PolyFrame, ParseError> {
        let frame = format!(
            r#"{{"event_type":"last_trade_price","asset_id":"T","price":"0.5","size":"{size}","side":"BUY","timestamp":"1000"}}"#
        );
        parse_market_frame(&frame, received())
    }

    #[test]
    fn six_decimal_size_is_exact() {
        let PolyFrame::Trade(trade) = trade_size("112122.430000").expect("parses") else {
            panic!("trade");
        };
        assert_eq!(trade.qty, Qty(11_212_243_000_000));
    }

    #[test]
    fn malformed_trade_fields_are_fatal_or_counted() {
        struct Case {
            name: &'static str,
            size: &'static str,
            side: &'static str,
            fatal: bool,
            matches: fn(&ParseError) -> bool,
        }
        let cases = [
            Case {
                name: "oversized_size_overflows_i64_at_1e-8",
                size: "1000000000000",
                side: "BUY",
                fatal: true,
                matches: |e| matches!(e, ParseError::Decode(DecimalFault::MantissaOverflow { .. })),
            },
            Case {
                name: "non_numeric_size",
                size: "abc",
                side: "BUY",
                fatal: false,
                matches: |e| matches!(e, ParseError::Decode(DecimalFault::Decimal { .. })),
            },
            Case {
                name: "size_with_9_decimal_places",
                size: "1.123456789",
                side: "BUY",
                fatal: false,
                matches: |e| matches!(e, ParseError::Decode(DecimalFault::Decimal { .. })),
            },
            Case {
                name: "unknown_side",
                size: "1",
                side: "MAYBE",
                fatal: false,
                matches: |e| matches!(e, ParseError::Side { .. }),
            },
        ];
        for case in cases {
            let frame = format!(
                r#"{{"event_type":"last_trade_price","asset_id":"T","price":"0.5","size":"{}","side":"{}","timestamp":"1000"}}"#,
                case.size, case.side
            );
            let error = parse_market_frame(&frame, received()).expect_err(case.name);
            assert_eq!(error.is_fatal(), case.fatal, "{}", case.name);
            assert!((case.matches)(&error), "{}: got {error:?}", case.name);
        }
    }
}

mod stamp_clamp {
    use super::*;

    fn trade_at(ts_ms: &str) -> TsUs {
        let frame = format!(
            r#"{{"event_type":"last_trade_price","asset_id":"T","price":"0.5","size":"1","side":"BUY","timestamp":"{ts_ms}"}}"#
        );
        let PolyFrame::Trade(trade) = parse_market_frame(&frame, received()).expect("parses")
        else {
            panic!("trade");
        };
        trade.exchange_ts_us
    }

    #[test]
    fn absurd_future_stamp_clamps_to_upper_edge() {
        assert_eq!(
            trade_at("999999999999999"),
            TsUs::from_micros(RECEIVED_US.saturating_add(MAX_VENUE_SKEW_US))
        );
    }

    proptest! {
        /// Any millisecond stamp string stays inside the receipt window, because the parse defends
        /// `TsUs::diff`.
        #[test]
        fn clamp_stays_inside_window(ms in 0i64..=99_999_999_999_999i64) {
            let clamped = trade_at(&ms.to_string()).micros();
            prop_assert!(clamped >= RECEIVED_US.saturating_sub(MAX_VENUE_SKEW_US));
            prop_assert!(clamped <= RECEIVED_US.saturating_add(MAX_VENUE_SKEW_US));
        }
    }
}

/// FITNESS: a book reconstructed from a snapshot + absolute-size deltas equals a naive `BTreeMap`
/// model applying the same levels (replace, `size == 0` removes). Any divergence is silent book
/// corruption.
mod delta_model {
    use super::*;

    // Cent prices 1..=98, sizes 1..=5000 shares. `min` is 1 for the snapshot's leading side so the
    // book reaches Valid — a live market's first frame always has levels; deltas before Valid are
    // (correctly) dropped, which the naive model doesn't express.
    fn any_levels(min: usize) -> impl Strategy<Value = Vec<(i64, i64)>> {
        prop::collection::vec((1i64..=98, 1i64..=5_000), min..40)
    }

    fn any_deltas() -> impl Strategy<Value = Vec<(bool, i64, i64)>> {
        // (is_bid, cent price, size where 0 removes)
        prop::collection::vec((any::<bool>(), 1i64..=98, 0i64..=5_000), 0..60)
    }

    fn model_apply(model: &mut BTreeMap<i64, i64>, price: i64, qty: i64) {
        if qty == 0 {
            model.remove(&price);
        } else {
            model.insert(price, qty);
        }
    }

    fn side_levels(model: &BTreeMap<i64, i64>, descending: bool) -> Vec<Level> {
        let mut prices: Vec<i64> = model.keys().copied().collect();
        if descending {
            prices.sort_unstable_by(|a, b| b.cmp(a));
        } else {
            prices.sort_unstable();
        }
        prices
            .into_iter()
            .map(|price| level(price * 1_000_000, model[&price] * 100_000_000))
            .collect()
    }

    proptest! {
        #[test]
        fn book_matches_model(
            bid_seed in any_levels(1),
            ask_seed in any_levels(0),
            deltas in any_deltas(),
        ) {
            let mut bid_model = BTreeMap::new();
            let mut ask_model = BTreeMap::new();
            for (price, qty) in &bid_seed {
                bid_model.insert(*price, *qty);
            }
            for (price, qty) in &ask_seed {
                ask_model.insert(*price, *qty);
            }

            let snapshot = poly_book(&bid_model, &ask_model);
            let mut normaliser = BookNormaliser::new(INSTRUMENT);
            let mut book = Book::new(BOOK_CAPACITY);
            let mut out = Vec::new();
            normaliser.emit_snapshot(&snapshot, &mut |m| out.push(m));

            let mut bids = Vec::new();
            let mut asks = Vec::new();
            for (is_bid, price, qty) in &deltas {
                let level = level(price * 1_000_000, qty * 100_000_000);
                if *is_bid {
                    model_apply(&mut bid_model, *price, *qty);
                    bids.push(level);
                } else {
                    model_apply(&mut ask_model, *price, *qty);
                    asks.push(level);
                }
            }
            normaliser.emit_delta(&bids, &asks, stamps(snapshot.exchange_ts_us), &mut |m| {
                out.push(m)
            });
            apply(&mut book, &out);

            prop_assert_eq!(book.bids().to_vec(), side_levels(&bid_model, true));
            prop_assert_eq!(book.asks().to_vec(), side_levels(&ask_model, false));
        }
    }

    fn poly_book(
        bids: &BTreeMap<i64, i64>,
        asks: &BTreeMap<i64, i64>,
    ) -> polysim::adapters::polymarket::parse::PolyBook {
        let render = |model: &BTreeMap<i64, i64>| {
            model
                .iter()
                .map(|(price, qty)| format!(r#"{{"price":"0.{price:02}","size":"{qty}"}}"#))
                .collect::<Vec<_>>()
                .join(",")
        };
        let frame = format!(
            r#"{{"event_type":"book","asset_id":"T","timestamp":"1784439695963","bids":[{}],"asks":[{}]}}"#,
            render(bids),
            render(asks)
        );
        match parse_market_frame(&frame, received()).expect("book parses") {
            PolyFrame::Book(book) => book,
            _ => panic!("book frame"),
        }
    }
}
