//! Binance kline sequencing goldens: the kline sequencer replayed over a recorded prod fixture
//! (live WS closes) and REST backfill, plus the gap-repair receipt-stamp monotonicity edge.

use polysim::adapters::binance::kline::{KlineOutcome, KlineSequencer};
use polysim::adapters::binance::parse::{
    RestKlineTail, parse_combined_frame, parse_kline, parse_rest_klines,
};
use polysim::config::KlineInterval;
use polysim::ids::{Price, Qty};
use polysim::msg::inbound::{InboundMessage, KlineEvent};
use polysim::time::TsUs;

use crate::binance_depth_sequencing::{INSTRUMENT, ctx};

const PERP_KLINE: &str = include_str!("../../fixtures/binance/perp_kline_1m.jsonl");
const PERP_REST_KLINES: &str = include_str!("../../fixtures/binance/perp_klines.json");

const MINUTE_US: i64 = 60_000_000;
const FIRST_OPEN_US: i64 = 1_784_410_620_000_000;

fn parse_live(fixture: &str) -> Vec<KlineEvent> {
    fixture
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let frame = parse_combined_frame(line).expect("envelope");
            parse_kline(&frame.data, ctx()).expect("kline")
        })
        .collect()
}

fn candle_at(open_ts_us: i64, is_closed: bool, received: TsUs) -> KlineEvent {
    KlineEvent {
        instrument: INSTRUMENT,
        interval: KlineInterval::OneMinute,
        open_ts_us: TsUs::from_micros(open_ts_us),
        open: Price(1),
        high: Price(1),
        low: Price(1),
        close: Price(1),
        base_volume: Qty(1),
        quote_volume: 1,
        trade_count: 1,
        is_closed,
        exchange_ts_us: received,
        exchange_sent_ts_us: None,
        received_ts_us: received,
        queued_ts_us: received,
    }
}

#[test]
fn perp_live_stream_forwards() {
    let events = parse_live(PERP_KLINE);
    // A streamed frame carries a send time; the backfill rows beside it do not, and that is the only
    // thing separating a measurable transport reading from a candle's age.
    assert!(
        events
            .iter()
            .all(|event| event.exchange_sent_ts_us == Some(event.exchange_ts_us)),
        "every live frame's event time fills the send stamp"
    );
    let mut sequencer = KlineSequencer::new(KlineInterval::OneMinute);
    let mut emitted = Vec::new();
    for event in events {
        let outcome = sequencer.on_live(event, &mut |message| emitted.push(message));
        assert_ne!(outcome, KlineOutcome::Duplicate);
        assert!(
            !matches!(outcome, KlineOutcome::Gap { .. }),
            "recorded stream is contiguous"
        );
    }
    let closes: Vec<&KlineEvent> = emitted
        .iter()
        .filter_map(|message| match message {
            InboundMessage::Kline(event) if event.is_closed => Some(event),
            _ => None,
        })
        .collect();
    assert_eq!(closes.len(), 1, "exactly one candle closes in the window");
    assert_eq!(closes[0].open_ts_us, TsUs::from_micros(FIRST_OPEN_US));
}

#[test]
fn rest_backfill_emits_closed_then_dedupes_on_overlap() {
    let backfill = parse_rest_klines(
        PERP_REST_KLINES,
        ctx(),
        KlineInterval::OneMinute,
        RestKlineTail::OpenCandleForming,
    )
    .expect("rest klines");
    let closed_count = backfill.iter().filter(|event| event.is_closed).count();
    assert!(closed_count >= 2, "backfill carries closed history");

    let mut sequencer = KlineSequencer::new(KlineInterval::OneMinute);
    let mut emitted = Vec::new();
    sequencer.on_backfill(&backfill, &mut |message| emitted.push(message));
    assert_eq!(emitted.len(), closed_count);

    // Re-fetching the same window on reconnect must add nothing (dedupe by open time).
    let before = emitted.len();
    sequencer.on_backfill(&backfill, &mut |message| emitted.push(message));
    assert_eq!(emitted.len(), before, "overlapping backfill is deduped");
}

#[test]
fn gap_repair_never_emits_a_decreasing_receipt_stamp() {
    // The held live close (stamp tL) triggers a REST gap-fill whose rows arrive LATER (tR > tL).
    // Emission order is backfill-then-close, so the re-fed close would carry a receipt stamp
    // BEHIND the backfill rows it follows — a decreasing received_ts_us on the queue, breaking
    // the ingress non-decreasing precondition. The sequencer clamps it forward.
    let mut sequencer = KlineSequencer::new(KlineInterval::OneMinute);
    let mut emitted = Vec::new();

    let early = TsUs::from_micros(1_000_000);
    let live_stamp = TsUs::from_micros(2_000_000);
    let repair_stamp = TsUs::from_micros(9_000_000);

    sequencer.on_backfill(&[candle_at(FIRST_OPEN_US, true, early)], &mut |m| {
        emitted.push(m)
    });

    let gap_close = candle_at(FIRST_OPEN_US + 2 * MINUTE_US, true, live_stamp);
    let outcome = sequencer.on_live(gap_close, &mut |m| emitted.push(m));
    assert!(matches!(outcome, KlineOutcome::Gap { .. }));

    sequencer.on_backfill(
        &[candle_at(FIRST_OPEN_US + MINUTE_US, true, repair_stamp)],
        &mut |m| emitted.push(m),
    );
    sequencer.on_live(gap_close, &mut |m| emitted.push(m));

    let stamps: Vec<i64> = emitted
        .iter()
        .map(|m| m.received_ts_us().micros())
        .collect();
    assert!(
        stamps.windows(2).all(|w| w[0] <= w[1]),
        "receipt stamps must be non-decreasing across the repair boundary: {stamps:?}"
    );
}
