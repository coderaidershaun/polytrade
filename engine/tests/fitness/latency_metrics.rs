//! Pins the three things that can't be re-derived: which stamps each stage subtracts, that the
//! ten-minute window truly ages out rather than accumulating forever, and which cells of the
//! snapshot the UI summary folds into which display row.

use std::time::{SystemTime, UNIX_EPOCH};

use polysim::config::{KlineInterval, TrackerSpec};
use polysim::hot::metrics::{Category, HotMetrics, MetricsSnapshot, Stage};
use polysim::hot::strategy::Strategy;
use polysim::ids::{ClientOrderId, InstrumentId, QueueId, Side};
use polysim::msg::exec::{ExecEvent, ExecKind};
use polysim::msg::inbound::{BookChunk, InboundMessage, KlineEvent, TradeEvent};
use polysim::msg::ui::{UiLatencyCell, UiLatencySummary};
use polysim::time::TsUs;

use crate::engine_support::{
    ONE, delta_chunk, engine_without_warmup, exec_event, instrument_row, kline, metrics_ring,
    persist_ring, pop, spin, spin_pop, strategy_log_ring, trade, ts,
};

/// Distinct by construction — a crossed wiring lands on another stage's number instead of
/// coincidentally matching its own.
const SEND_TO_RECV_US: i64 = 3_000;
const MATCH_TO_SEND_US: i64 = 2_000;
const ROUND_TRIP_US: i64 = 8_000;
const BOOK_SEND_TO_RECV_US: i64 = 1_500;
const KLINE_SEND_TO_RECV_US: i64 = 2_500;
const EXEC_VENUE_TO_RECV_US: i64 = 4_000;
const SYNTHESISED_LOCAL_GAP_US: i64 = 60_000;

/// Far enough back that admitting it moves every number it touches by four orders of magnitude: a
/// market-data mean over healthy 113ms traffic would read as several seconds.
const BACKFILLED_CANDLE_AGE_US: i64 = 6 * 60 * 60 * 1_000_000;

const ELEVEN_MINUTES_US: i64 = 11 * 60 * 1_000_000;
const BACKLOG_HALF_LIFE_US: i64 = 60 * 1_000_000;

/// Queue and depth stamped on each dispatch below, in message order. Two queues with distinct
/// depths, so the per-queue gauge cannot pass by reading whichever number it saw last.
const DISPATCH_DEPTHS: [(u8, usize); 7] = [(0, 4), (0, 2), (1, 3), (1, 0), (1, 5), (1, 4), (0, 0)];

/// Deliberately unlike every entry in [`DISPATCH_DEPTHS`]: the panel's figure is the total across
/// all rings, and a fold that reused the popped queue's own depth would land on one of those.
const SPIN_BACKLOG: usize = 6;

/// Rollup keeps only buckets within ten minutes of now — a fixture stamped from an arbitrary epoch
/// would be filtered out, and every assertion below would read zero.
fn wall_now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock reads after the unix epoch")
        .as_micros() as i64
}

struct Silent;

impl Strategy for Silent {}

fn dispatch_stamped_traffic() -> MetricsSnapshot {
    let instruments = [instrument_row(0, TrackerSpec::default(), 64)];
    let (persistence, _records) = persist_ring(256);
    let (strategy_log_sink, _logs) = strategy_log_ring(64);
    let (metrics_sink, mut snapshots) = metrics_ring(64);
    let mut engine = engine_without_warmup(
        &instruments,
        Box::new(Silent),
        persistence,
        strategy_log_sink,
        metrics_sink,
    );

    let base = wall_now_us();
    let messages = [
        InboundMessage::Trade(TradeEvent {
            exchange_ts_us: ts(base - MATCH_TO_SEND_US - SEND_TO_RECV_US),
            exchange_sent_ts_us: Some(ts(base - SEND_TO_RECV_US)),
            received_ts_us: ts(base),
            queued_ts_us: ts(base),
            ..trade(0, 100 * ONE, ONE, Side::Buy, base)
        }),
        InboundMessage::Book(BookChunk {
            exchange_ts_us: Some(ts(base - BOOK_SEND_TO_RECV_US)),
            received_ts_us: ts(base),
            queued_ts_us: ts(base),
            ..delta_chunk(0, Side::Buy, &[(100 * ONE, ONE)], base)
        }),
        InboundMessage::Exec(venue_answer(base)),
        InboundMessage::Exec(never_sent(base)),
        InboundMessage::Kline(streamed_candle(base)),
        InboundMessage::Kline(backfilled_candle(base)),
        InboundMessage::SpinTick(spin(1, base)),
    ];
    for (message, (queue, depth)) in messages.iter().zip(DISPATCH_DEPTHS) {
        let sample = match message {
            InboundMessage::SpinTick(_) => spin_pop(queue, SPIN_BACKLOG),
            _ => pop(queue, depth),
        };
        engine.dispatch(sample, message);
    }
    snapshots
        .pop()
        .expect("the spin tick emits exactly one metrics snapshot")
}

fn venue_answer(base: i64) -> ExecEvent {
    ExecEvent {
        kind: ExecKind::ReportNew,
        exchange_ts_us: ts(base - EXEC_VENUE_TO_RECV_US),
        request_sent_ts_us: Some(ts(base - ROUND_TRIP_US)),
        received_ts_us: ts(base),
        queued_ts_us: ts(base),
        ..exec_event(
            InstrumentId(0),
            ClientOrderId(1),
            Side::Buy,
            100 * ONE,
            base,
        )
    }
}

/// Edge-synthesised: `exchange_ts_us` here is OUR clock wearing the venue's field, stamped far back
/// enough that admitting it would move both count and max.
fn never_sent(base: i64) -> ExecEvent {
    ExecEvent {
        kind: ExecKind::PlaceNotSent,
        exchange_ts_us: ts(base - SYNTHESISED_LOCAL_GAP_US),
        request_sent_ts_us: None,
        received_ts_us: ts(base),
        queued_ts_us: ts(base),
        ..exec_event(
            InstrumentId(0),
            ClientOrderId(2),
            Side::Buy,
            100 * ONE,
            base,
        )
    }
}

/// A live WS frame: its event time is the venue's send, milliseconds behind the receive.
fn streamed_candle(base: i64) -> KlineEvent {
    KlineEvent {
        exchange_ts_us: ts(base - KLINE_SEND_TO_RECV_US),
        exchange_sent_ts_us: Some(ts(base - KLINE_SEND_TO_RECV_US)),
        received_ts_us: ts(base),
        queued_ts_us: ts(base),
        ..flat_candle(base, false)
    }
}

/// A REST backfill row: `exchange_ts_us` is the candle's own close, which is how old the CANDLE is,
/// not how long its bytes took to arrive.
fn backfilled_candle(base: i64) -> KlineEvent {
    KlineEvent {
        exchange_ts_us: ts(base - BACKFILLED_CANDLE_AGE_US),
        exchange_sent_ts_us: None,
        received_ts_us: ts(base),
        queued_ts_us: ts(base),
        ..flat_candle(base, true)
    }
}

fn flat_candle(base: i64, is_closed: bool) -> KlineEvent {
    let price = 100 * ONE;
    kline(
        0,
        KlineInterval::OneMinute,
        (price, price, price, price),
        is_closed,
        base,
    )
}

fn assert_single_sample(
    snapshot: &MetricsSnapshot,
    category: Category,
    stage: Stage,
    expected_us: i64,
) {
    let stat = snapshot.stage(category, stage);
    let label = format!("{} {}", category.label(), stage.label());
    assert_eq!(stat.count, 1, "{label}: exactly one sample recorded");
    assert_eq!(stat.min_us, expected_us, "{label}: min");
    assert_eq!(stat.max_us, expected_us, "{label}: max");
}

/// Both a lone-stamp venue (book) and a synthesised local event (the second exec message, which
/// never touched a venue) share the same failure mode: fabricating a stamp the message never
/// carried. The exec case is the sharper one — only the venue-decoded event may reach exch->recv, or
/// the engine ends up timing its own clock against itself.
#[test]
fn each_stage_subtracts_the_stamps_its_name_claims() {
    let snapshot = dispatch_stamped_traffic();

    // Anchored on the venue's SEND stamp, so the venue's internal gap is NOT counted twice.
    assert_single_sample(
        &snapshot,
        Category::Trade,
        Stage::ExchangeToReceived,
        SEND_TO_RECV_US,
    );
    assert_single_sample(
        &snapshot,
        Category::Trade,
        Stage::ExchangeInternal,
        MATCH_TO_SEND_US,
    );
    assert_single_sample(
        &snapshot,
        Category::Exec,
        Stage::OrderRoundTrip,
        ROUND_TRIP_US,
    );
    assert_single_sample(
        &snapshot,
        Category::BookDelta,
        Stage::ExchangeToReceived,
        BOOK_SEND_TO_RECV_US,
    );

    // One stamp only: a single-stamp venue must never fabricate an internal gap out of it.
    assert_eq!(
        snapshot
            .stage(Category::BookDelta, Stage::ExchangeInternal)
            .count,
        0,
        "a lone venue stamp yields no match->send gap"
    );

    // The synthesised exec event was dispatched too — only the venue-decoded one may reach
    // exch->recv.
    assert_single_sample(
        &snapshot,
        Category::Exec,
        Stage::ExchangeToReceived,
        EXEC_VENUE_TO_RECV_US,
    );
    assert_eq!(
        snapshot.stage(Category::Exec, Stage::EndToEnd).count,
        2,
        "both exec events were dispatched — the guard drops the stamp, not the message"
    );
}

/// FITNESS: a REST backfill row stamps `exchange_ts_us` from the candle it closes, hours behind the
/// fetch that carried it, and that is deliberate — the research columns read it as candle time. Feed
/// it to exch->recv and the engine records the candle's AGE as a transport latency. The damage is
/// silent and unbounded: the market-data row sums its categories before it divides, so a single
/// backfilled hour buries every healthy reading beside it, and the display an operator judges engine
/// health by reads seconds where the wire is delivering microseconds.
#[test]
fn a_backfilled_candle_contributes_no_exchange_latency() {
    let snapshot = dispatch_stamped_traffic();

    // Only the streamed candle carries a send stamp, so a second sample here could only be the
    // backfill row's six-hour age — which would move max, not just count.
    assert_single_sample(
        &snapshot,
        Category::Kline,
        Stage::ExchangeToReceived,
        KLINE_SEND_TO_RECV_US,
    );
    assert_eq!(
        snapshot
            .stage(Category::Kline, Stage::ExchangeInternal)
            .count,
        0,
        "a candle has no match stamp to gap against its send"
    );

    // The row is dropped from ONE stage, not from the engine: its wall-clock stages are honest
    // measurements of a message that really did queue and really was processed.
    for stage in [
        Stage::ReceivedToQueued,
        Stage::QueueWait,
        Stage::Processing,
        Stage::EndToEnd,
    ] {
        assert_eq!(
            snapshot.stage(Category::Kline, stage).count,
            2,
            "both candles were dispatched: {}",
            stage.label()
        );
    }
}

#[test]
fn queue_depth_reports_min_mean_and_max_over_the_window_it_ages_out() {
    let mut metrics = HotMetrics::new();
    let at = TsUs::from_micros(1_700_000_000_000_000);
    for depth in [3, 7, 2] {
        metrics.record_occupancy(QueueId(0), depth, at);
    }

    let depth = metrics.snapshot(at).occupancy[0];
    assert_eq!(depth.count, 3);
    assert_eq!(depth.min, 2);
    assert_eq!(depth.max, 7);
    assert_eq!(depth.mean(), 4.0);

    let later = TsUs::from_micros(at.micros() + ELEVEN_MINUTES_US);
    metrics.record_occupancy(QueueId(0), 5, later);
    let depth = metrics.snapshot(later).occupancy[0];
    assert_eq!(
        depth.count, 1,
        "the three older samples aged out of the window"
    );
    assert_eq!(depth.min, 5);
    assert_eq!(depth.max, 5);
}

/// FITNESS: before the first spin there is no backlog reading at all, and the panel must say so
/// rather than draw a zero. Once a spin lands, a caught-up engine reads exactly 0.0 — the reading an
/// operator checks against. A per-dispatch sampler cannot produce it, because a quiet queue
/// contributes nothing and the whole column goes blank.
#[test]
fn the_backlog_is_absent_until_the_first_spin_and_zero_once_one_lands() {
    let mut metrics = HotMetrics::new();
    let at = TsUs::from_micros(1_700_000_000_000_000);
    metrics.record_occupancy(QueueId(0), 3, at);

    assert_eq!(
        metrics.snapshot(at).backlog_ema,
        None,
        "occupancy traffic alone conjured a backlog reading"
    );

    metrics.record_spin_backlog(0, at);
    assert_eq!(metrics.snapshot(at).backlog_ema, Some(0.0));
}

/// FITNESS: the reading is an EWMA on the tick's own event time, not the latest sample. One
/// half-life after a backlog of eight clears, the panel still shows half of it — a fold that decayed
/// on elapsed wall time instead would drift with how long the test itself took, and a replay would
/// no longer reproduce the panel the live run showed.
#[test]
fn a_cleared_backlog_decays_by_half_over_one_half_life() {
    let mut metrics = HotMetrics::new();
    let at = TsUs::from_micros(1_700_000_000_000_000);
    metrics.record_spin_backlog(8, at);
    metrics.record_spin_backlog(0, TsUs::from_micros(at.micros() + BACKLOG_HALF_LIFE_US));

    let ema = metrics
        .snapshot(at)
        .backlog_ema
        .expect("two spins were sampled");
    assert!(
        (ema - 4.0).abs() < 1e-9,
        "half a half-life-old backlog of eight is 4.0, read {ema}"
    );
}

/// FITNESS: the summary the UI folds is a distillation of the snapshot, and WHICH cell lands in
/// which row is the whole of it. Every mistake available here is silent: a market-data row that
/// quietly included exec would report a round-trip as though it were a book update's age, and a
/// slot total that read one queue would say the engine was idle while another queue backed up.
#[test]
fn the_ui_summary_distils_the_snapshot_it_is_built_from() {
    let snapshot = dispatch_stamped_traffic();
    let summary = UiLatencySummary::from_snapshot(&snapshot);

    // The trade, the book delta and the streamed candle, added before dividing. The backfilled
    // candle is the fourth market-data message and contributes nothing here — a fold that counted it
    // would report six hours as the venue's transport time.
    assert_eq!(summary.market_data.exchange_to_received.count, 3);
    assert_eq!(
        summary.market_data.exchange_to_received.mean_us(),
        Some((SEND_TO_RECV_US + BOOK_SEND_TO_RECV_US + KLINE_SEND_TO_RECV_US) as f64 / 3.0),
        "the market-data row folds every venue-facing category into one mean"
    );
    assert_eq!(
        summary.execution.order_round_trip.mean_us(),
        Some(ROUND_TRIP_US as f64),
        "the exec row carries the round trip the exec category recorded"
    );
    assert_eq!(
        summary.market_data.order_round_trip.mean_us(),
        None,
        "no market-data message has a round trip — absent must not read as zero"
    );

    // Spanning EVERY category is the hot-path row's reason to exist: the spin tick is a processed
    // message that neither display row can see, so a row that only re-added the two would miss it.
    let spin_processing = snapshot.stage(Category::Spin, Stage::Processing);
    assert!(
        spin_processing.count > 0,
        "the fixture's spin tick is itself a processed message"
    );
    assert_eq!(
        summary.hot_path.processing.count,
        summary.market_data.processing.count
            + summary.execution.processing.count
            + spin_processing.count,
    );
    assert_eq!(
        summary.hot_path.queue_wait.count, summary.hot_path.processing.count,
        "every dispatched message contributes to both engine stages"
    );

    // A carried cell with no reader is a defect: the hot-path row answers only the two questions
    // that are meaningful across categories, and the other four stay visibly empty.
    let empty = UiLatencyCell::default();
    assert_eq!(summary.hot_path.exchange_to_received, empty);
    assert_eq!(summary.hot_path.received_to_queued, empty);
    assert_eq!(summary.hot_path.end_to_end, empty);
    assert_eq!(summary.hot_path.order_round_trip, empty);

    // Seeded, not blended: the fixture's single spin is the first sample the EWMA ever saw, so it
    // carries the raw backlog. Any weight applied to a first sample would show up here as a fraction.
    assert_eq!(
        summary.backlog_ema,
        Some(SPIN_BACKLOG as f64),
        "the panel's backlog comes from the spin's own reading, not from the queue it popped"
    );
}
