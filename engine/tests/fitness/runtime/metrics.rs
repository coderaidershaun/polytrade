//! The metrics thread through what it makes public: samples go into its rings, message time is
//! passed in, and rows, reports and the workstation's panel come out. Every failure here is silent:
//! a stage that subtracts the wrong stamps, a window that never forgets, or a drop that nobody
//! counts all still print plausible numbers.

use std::sync::Arc;

use engine::hot::metrics::EngineCounters;
use engine::link::{
    Envelope, LINK_MAX_DATAGRAM, LinkBody, LinkDatagram, LinkHash, LinkIdentity, TopicId,
};
use engine::msg::ids::{ClientOrderId, InstrumentId, QueueId};
use engine::msg::latency::{Category, CommandKind, CounterSample, Stage, StageSample, WireSample};
use engine::msg::time::{ClockSource, DurationUs, Stamper, Tick, TickAnchor, TickScale, TsUs};
use engine::msg::ui::{LatencyFlow, LatencyReading, LatencyReport, Percentile, UiEvent};
use engine::operator::workstation::link::session::{Delivery, LinkSession, LinkTarget};
use engine::operator::workstation::model::UiModel;
use engine::runtime::config::{LatencyConfig, StrategyId};
use engine::runtime::metrics::{
    LatencyRow, Metrics, MetricsParts, MetricsSetup, RollupRow, RollupSink, SampleProducers,
    SinkKind,
};
use proptest::prelude::*;
use rtrb::Consumer;

const INTERVAL_US: i64 = 60_000_000;
const DISPLAY: DurationUs = DurationUs::from_millis(1_000);
const SMALLEST_RING: u64 = 1_024;
const NO_STAMP: TsUs = StageSample::ABSENT;

/// Distinct by construction, so a stage that subtracts the wrong pair lands on another stage's
/// number instead of on its own.
const VENUE_SENT_US: i64 = 997_000;
const VENUE_MATCH_US: i64 = 995_000;
const RECEIVED_US: i64 = 1_000_000;
const QUEUED_US: i64 = 1_000_010;
const POP_TICK: u64 = 1_000_050_000;
const DONE_TICK: u64 = 1_000_050_800;

#[derive(Default)]
struct Rows(Vec<RollupRow>);

impl RollupSink for Rows {
    fn write(&mut self, row: RollupRow) {
        self.0.push(row);
    }
}

impl Rows {
    fn latency(&self) -> Vec<LatencyRow> {
        self.0
            .iter()
            .filter_map(|row| match row {
                RollupRow::Latency(row) => Some(*row),
                _ => None,
            })
            .collect()
    }

    fn stage(&self, category: Category, stage: Stage) -> Option<LatencyRow> {
        self.latency()
            .into_iter()
            .find(|row| row.category == category && row.stage == stage)
    }

    fn dropped_total(&self, sink: SinkKind) -> Option<u64> {
        self.0.iter().rev().find_map(|row| match row {
            RollupRow::Drop(row) if row.sink == sink => Some(row.dropped_total),
            _ => None,
        })
    }
}

/// A counter that the test sets, so each tick is a literal. One tick is one nanosecond.
struct Counter {
    stamper: Stamper,
    mock: Arc<quanta::Mock>,
}

impl Counter {
    fn new() -> Counter {
        let (clock, mock) = quanta::Clock::mock();
        Counter {
            stamper: Stamper::Counter(clock),
            mock,
        }
    }

    fn at(&self, value: u64) -> Tick {
        let current = self.mock.value();
        if value >= current {
            self.mock.increment(value - current);
        } else {
            self.mock.decrement(current - value);
        }
        self.stamper.stamp()
    }

    /// Tick 0 is engine time 0 and tick 1e9 is one second later.
    fn scale(&self) -> TickScale {
        let first = TickAnchor {
            tick: self.at(0),
            at: ts(0),
        };
        let last = TickAnchor {
            tick: self.at(1_000_000_000),
            at: ts(1_000_000),
        };
        TickScale::between(first, last).expect("both anchors move forward")
    }
}

fn ts(micros: i64) -> TsUs {
    TsUs::from_micros(micros)
}

fn config(window_intervals: u64) -> LatencyConfig {
    LatencyConfig {
        interval_secs: (INTERVAL_US / 1_000_000) as u64,
        window_intervals,
        display_ms: DISPLAY.micros() as u64 / 1_000,
        sample_ring: SMALLEST_RING,
    }
}

struct Harness {
    counter: Counter,
    metrics: Metrics,
    samples: SampleProducers,
    reports: Consumer<LatencyReport>,
    rows: Rows,
}

impl Harness {
    fn new(window_intervals: u64) -> Harness {
        Harness::with_source(window_intervals, ClockSource::Tsc)
    }

    fn with_source(window_intervals: u64, clock_source: ClockSource) -> Harness {
        let counter = Counter::new();
        let MetricsParts {
            metrics,
            samples,
            reports,
        } = Metrics::with_rings(MetricsSetup {
            latency: config(window_intervals),
            clock_source,
            queue_count: 2,
            scale: counter.scale(),
            start_ts_us: ts(0),
        });
        Harness {
            counter,
            metrics,
            samples,
            reports,
            rows: Rows::default(),
        }
    }

    fn pass(&mut self, now_us: i64) {
        self.metrics.pass(ts(now_us), &mut self.rows);
    }

    /// The newest report on the ring, once every older one is read off.
    fn latest_report(&mut self) -> Option<LatencyReport> {
        let mut latest = None;
        while let Ok(report) = self.reports.pop() {
            latest = Some(report);
        }
        latest
    }

    fn trade(&self) -> StageSample {
        self.message(Category::Trade, POP_TICK, DONE_TICK)
    }

    fn message(&self, category: Category, pop: u64, done: u64) -> StageSample {
        StageSample {
            pop_tick: self.counter.at(pop),
            done_tick: self.counter.at(done),
            received_ts_us: ts(RECEIVED_US),
            queued_ts_us: ts(QUEUED_US),
            exchange_sent_ts_us: ts(VENUE_SENT_US),
            exchange_ts_us: ts(VENUE_MATCH_US),
            request_sent_ts_us: NO_STAMP,
            category,
            queue: QueueId(0),
            pop_seq: 0,
        }
    }

    fn command(&self, issue: u64, pop: u64, wire: u64) -> WireSample {
        WireSample {
            issue_tick: self.counter.at(issue),
            pop_tick: self.counter.at(pop),
            wire_tick: self.counter.at(wire),
            issued_ts_us: ts(RECEIVED_US),
            client_id: ClientOrderId(7),
            kind: CommandKind::Place,
            queue: QueueId(1),
        }
    }
}

fn spin_counters(spin_us: i64, backlog: [u32; 2]) -> CounterSample {
    let mut waiting = [0; 20];
    waiting[..2].copy_from_slice(&backlog);
    CounterSample {
        spin_ts_us: ts(spin_us),
        spin_seq: 0,
        backlog: waiting,
        counters: EngineCounters::default(),
    }
}

fn assert_exact(rows: &Rows, category: Category, stage: Stage, nanos: u64) {
    let row = rows
        .stage(category, stage)
        .unwrap_or_else(|| panic!("{category:?} has no {stage:?} row: {:#?}", rows.latency()));
    assert_eq!(row.count, 1, "{category:?} {stage:?} took one sample");
    assert_eq!(
        row.max_ns, nanos,
        "{category:?} {stage:?} subtracts the stamps its name claims"
    );
}

#[test]
fn each_stage_subtracts_the_stamps_its_name_claims() {
    let mut harness = Harness::new(10);
    let trade = harness.trade();
    harness.samples.stages.push(trade);
    let exec = StageSample {
        exchange_sent_ts_us: NO_STAMP,
        exchange_ts_us: NO_STAMP,
        request_sent_ts_us: ts(RECEIVED_US - 8_000),
        ..harness.message(Category::Exec, POP_TICK, DONE_TICK)
    };
    harness.samples.stages.push(exec);
    harness.pass(QUEUED_US + 100);
    harness.pass(INTERVAL_US);

    let rows = &harness.rows;
    assert_exact(rows, Category::Trade, Stage::VenueToEngine, 3_000_000);
    assert_exact(rows, Category::Trade, Stage::VenueInternal, 2_000_000);
    assert_exact(rows, Category::Trade, Stage::WireToMessage, 10_000);
    assert_exact(rows, Category::Trade, Stage::QueueWait, 40_000);
    assert_exact(rows, Category::Trade, Stage::Processing, 800);
    assert_exact(rows, Category::Trade, Stage::MessageToDecision, 40_800);
    let trade_stages = rows
        .latency()
        .iter()
        .filter(|row| row.category == Category::Trade)
        .count();
    assert_eq!(trade_stages, 6, "no other stage of a trade takes a sample");

    assert_exact(rows, Category::Exec, Stage::OrderRoundTrip, 8_000_000);
    assert!(
        rows.stage(Category::Exec, Stage::VenueToEngine).is_none(),
        "an answer with no venue stamp, such as one the edge made itself, times no venue stage"
    );
}

#[test]
fn full_sample_ring_drops_and_counts_without_wait() {
    let mut harness = Harness::new(10);
    let trade = harness.trade();
    for _ in 0..SMALLEST_RING + 5 {
        harness.samples.stages.push(trade);
    }
    assert_eq!(
        harness.samples.stages.dropped(),
        5,
        "each push past a full ring returned at once and counted its sample"
    );

    harness.pass(QUEUED_US + 100);
    harness.pass(INTERVAL_US);
    let row = harness
        .rows
        .stage(Category::Trade, Stage::Processing)
        .expect("the samples that fit were timed");
    assert_eq!(row.count, SMALLEST_RING);
    assert_eq!(row.dropped, 5, "the row says its tail reads low");
    assert_eq!(harness.rows.dropped_total(SinkKind::StageSamples), Some(5));
}

#[test]
fn backward_pair_is_counted_not_recorded() {
    let mut harness = Harness::new(10);
    let backward = harness.message(Category::Trade, POP_TICK, POP_TICK - 10_000);
    harness.samples.stages.push(backward);
    let venue_ahead = StageSample {
        exchange_sent_ts_us: ts(RECEIVED_US + 500),
        ..harness.trade()
    };
    harness.samples.stages.push(venue_ahead);
    let command = harness.command(2_000, 1_000, 3_000);
    harness.samples.wires.push(command);
    harness.pass(QUEUED_US + 100);
    harness.pass(INTERVAL_US);

    let rows = &harness.rows;
    let processing = rows
        .stage(Category::Trade, Stage::Processing)
        .expect("a backward pair still has its row");
    assert_eq!(processing.backward, 1);
    assert_eq!(
        processing.count, 1,
        "only the pair that ran forward is in the histogram"
    );
    assert_eq!(processing.max_ns, 800);

    let venue = rows
        .stage(Category::Trade, Stage::VenueToEngine)
        .expect("a negative venue stage still has its row");
    assert_eq!(venue.negative, 1);
    assert_eq!(venue.count, 1);

    let bank = rows
        .stage(Category::Command, Stage::BankAndLaneWait)
        .expect("a command that popped before it was banked still has its row");
    assert_eq!((bank.count, bank.backward), (0, 1));
    assert_exact(rows, Category::Command, Stage::Send, 2_000);
    assert_exact(rows, Category::Command, Stage::DecisionToWire, 1_000);
}

fn engine_identity() -> LinkIdentity {
    LinkIdentity {
        token_hash: LinkHash::of_name(""),
        strategy_hash: LinkHash::of_name("strat-fitness"),
        sender_te_hash: LinkHash::of_name("te-fitness"),
        boot_ts_us: ts(1_000),
    }
}

fn workstation(now: TsUs) -> LinkSession {
    LinkSession::new(
        LinkTarget {
            strategy: StrategyId::new("strat-fitness").expect("the strategy id is a literal"),
            peers: vec!["127.0.0.1:9310".parse().expect("the peer is a literal")],
            token_hash: LinkHash::of_name(""),
            sender_nonce: 7,
        },
        now,
    )
    .expect("the target names a peer")
}

fn report_frame(report: LatencyReport) -> Vec<u8> {
    let mut buffer = [0u8; LINK_MAX_DATAGRAM];
    let written = LinkDatagram {
        envelope: Envelope::new(engine_identity(), TopicId::LATENCY, 1),
        body: LinkBody::Latency(report),
    }
    .encode(&mut buffer);
    buffer[..written].to_vec()
}

#[test]
fn latency_report_reaches_the_workstation_feed() {
    let mut harness = Harness::new(10);
    let trade = harness.trade();
    for _ in 0..300 {
        harness.samples.stages.push(trade);
    }
    const FIRST_PASS_US: i64 = 500_000;
    harness.pass(FIRST_PASS_US);
    assert!(
        harness.latest_report().is_none(),
        "no report before the display period has passed"
    );
    harness.pass(FIRST_PASS_US + DISPLAY.micros());
    let report = harness
        .latest_report()
        .expect("one report once the display period has passed");

    let processing = report
        .cell(LatencyFlow::MarketData, Stage::Processing)
        .expect("the market data row shows processing");
    assert_eq!(processing.count, 300);
    assert_eq!(processing.max_ns, 800);
    assert!(
        matches!(
            processing.reading(Percentile::P50),
            LatencyReading::Micros(micros) if (micros - 0.8).abs() < 0.01
        ),
        "300 samples carry a median: {processing:?}"
    );
    assert_eq!(
        processing.reading(Percentile::P99),
        LatencyReading::Thin { count: 300 },
        "300 samples cannot carry a 99th percentile"
    );
    assert_eq!(
        report.age(LatencyFlow::MarketData),
        Some(DurationUs::from_micros(DISPLAY.micros()))
    );
    assert_eq!(
        report.age(LatencyFlow::Execution),
        None,
        "no execution sample yet"
    );

    let now = report.built_ts_us;
    let mut session = workstation(now);
    let delivery = session.on_datagram(&report_frame(report), now);
    let Some(Delivery::Latency(delivered)) = delivery else {
        panic!("the report frame reaches the workstation as a report: {delivery:?}");
    };
    let mut model = UiModel::with_capacity(1, DurationUs::from_millis(100));
    model.apply_latency_report(delivered);
    assert_eq!(
        model.latency_report(),
        Some(&report),
        "the panel reads what the thread built"
    );
}

#[test]
fn window_drops_samples_older_than_its_intervals() {
    let mut harness = Harness::new(10);
    let trade = harness.trade();
    for _ in 0..3 {
        harness.samples.stages.push(trade);
    }
    harness.pass(QUEUED_US + 100);

    let window_count = |harness: &mut Harness, interval: i64| {
        let start_us = interval * INTERVAL_US;
        harness.pass(start_us);
        harness.samples.stages.push(trade);
        harness.pass(start_us + DISPLAY.micros());
        harness
            .latest_report()
            .and_then(|report| report.cell(LatencyFlow::MarketData, Stage::Processing))
            .map_or(0, |cell| cell.count)
    };
    assert_eq!(
        window_count(&mut harness, 10),
        4,
        "ten intervals ago is still inside a ten-interval window"
    );
    assert_eq!(
        window_count(&mut harness, 11),
        2,
        "eleven intervals ago has aged out, and only the two newer samples remain"
    );
}

#[test]
fn backlog_is_absent_until_the_first_spin_and_zero_once_one_lands() {
    let mut harness = Harness::new(10);
    let trade = harness.trade();
    harness.samples.stages.push(trade);
    harness.pass(DISPLAY.micros());
    let before = harness
        .latest_report()
        .expect("a report once the display period passed");
    assert_eq!(
        before.backlog_ema, None,
        "message traffic alone makes no backlog reading"
    );

    harness
        .samples
        .counters
        .push(spin_counters(1_000_000, [0, 0]));
    harness.pass(2 * DISPLAY.micros());
    let after = harness.latest_report().expect("the next report");
    assert_eq!(
        after.backlog_ema,
        Some(0.0),
        "an engine that keeps up reads exactly zero"
    );
}

#[test]
fn cleared_backlog_halves_over_one_half_life_of_message_time() {
    const HALF_LIFE_US: i64 = 60_000_000;
    let mut harness = Harness::new(10);
    harness
        .samples
        .counters
        .push(spin_counters(1_000_000, [5, 3]));
    harness
        .samples
        .counters
        .push(spin_counters(1_000_000 + HALF_LIFE_US, [0, 0]));
    harness.pass(DISPLAY.micros());

    let ema = harness
        .latest_report()
        .and_then(|report| report.backlog_ema)
        .expect("two spins were counted");
    assert!(
        (ema - 4.0).abs() < 1e-9,
        "a backlog of eight that cleared one half-life of message time ago reads 4.0, read {ema}"
    );
}

#[test]
fn stale_latency_report_shows_its_age() {
    let mut harness = Harness::new(10);
    harness.pass(DISPLAY.micros());
    let report = harness
        .latest_report()
        .expect("a report once the display period passed");
    let built = report.built_ts_us;

    let mut model = UiModel::with_capacity(1, DurationUs::from_millis(100));
    model.apply_latency_report(report);
    let rotation = |seq: u64, at: TsUs| UiEvent::Rotation {
        instrument: InstrumentId(0),
        seq,
        event_ts_us: at,
    };
    model.apply_event(rotation(0, built + DurationUs::from_secs(1)));
    assert_eq!(
        model.latency_report_staleness(),
        None,
        "a report one second behind the feed is fresh"
    );

    model.apply_event(rotation(1, built + DurationUs::from_secs(10)));
    assert_eq!(
        model.latency_report_staleness(),
        Some(DurationUs::from_secs(10)),
        "a metrics thread that stopped sends nothing, and the feed shows how old its last report is"
    );
}

/// The clock source that each row of a closed interval names, for a thread started with
/// `clock_source`.
pub(super) fn row_clock_sources(clock_source: ClockSource) -> Vec<ClockSource> {
    let mut harness = Harness::with_source(10, clock_source);
    let trade = harness.trade();
    harness.samples.stages.push(trade);
    harness.pass(INTERVAL_US);
    harness
        .rows
        .latency()
        .iter()
        .map(|row| row.clock_source)
        .collect()
}

fn arb_stamp() -> impl Strategy<Value = TsUs> {
    prop_oneof![
        Just(TsUs::from_micros(i64::MIN)),
        Just(TsUs::from_micros(i64::MAX)),
        Just(TsUs::from_micros(0)),
        any::<i64>().prop_map(TsUs::from_micros),
    ]
}

fn arb_tick() -> impl Strategy<Value = u64> {
    prop_oneof![Just(0), Just(u64::MAX), any::<u64>()]
}

fn arb_category() -> impl Strategy<Value = Category> {
    prop::sample::select(Category::ALL.to_vec())
}

#[derive(Debug, Clone)]
struct RawStage {
    pop: u64,
    done: u64,
    stamps: [TsUs; 5],
    category: Category,
    queue: u8,
    pop_seq: u32,
}

fn arb_stage() -> impl Strategy<Value = RawStage> {
    (
        arb_tick(),
        arb_tick(),
        prop::array::uniform5(arb_stamp()),
        arb_category(),
        any::<u8>(),
        any::<u32>(),
    )
        .prop_map(|(pop, done, stamps, category, queue, pop_seq)| RawStage {
            pop,
            done,
            stamps,
            category,
            queue,
            pop_seq,
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// FITNESS: a sample is data from another thread, and the metrics thread trusts none of it. Any
    /// tick, any stamp and any category is timed, or counted as backward or negative, and a panic
    /// there would end the run for a fault of measurement alone.
    #[test]
    fn arbitrary_samples_are_counted_and_never_panic(
        stages in prop::collection::vec(arb_stage(), 0..48),
        wires in prop::collection::vec((arb_tick(), arb_tick(), arb_tick()), 0..16),
        spins in prop::collection::vec((arb_stamp(), prop::array::uniform20(any::<u32>())), 0..8),
    ) {
        let mut harness = Harness::new(3);
        for raw in &stages {
            let sample = StageSample {
                pop_tick: harness.counter.at(raw.pop),
                done_tick: harness.counter.at(raw.done),
                received_ts_us: raw.stamps[0],
                queued_ts_us: raw.stamps[1],
                exchange_sent_ts_us: raw.stamps[2],
                exchange_ts_us: raw.stamps[3],
                request_sent_ts_us: raw.stamps[4],
                category: raw.category,
                queue: QueueId(raw.queue),
                pop_seq: raw.pop_seq,
            };
            harness.samples.stages.push(sample);
        }
        for &(issue, pop, wire) in &wires {
            let sample = harness.command(issue, pop, wire);
            harness.samples.wires.push(sample);
        }
        for &(spin_ts_us, backlog) in &spins {
            harness.samples.counters.push(CounterSample {
                spin_ts_us,
                spin_seq: 0,
                backlog,
                counters: EngineCounters::default(),
            });
        }
        harness.pass(DISPLAY.micros());
        prop_assert!(harness.latest_report().is_some(), "the report is built from any samples");
        harness.pass(INTERVAL_US);

        let timed_messages = stages.iter().filter(|raw| raw.category != Category::Command).count();
        let processing: u64 = harness
            .rows
            .latency()
            .iter()
            .filter(|row| row.stage == Stage::Processing)
            .map(|row| row.count + row.backward)
            .sum();
        prop_assert_eq!(
            processing,
            timed_messages as u64,
            "each message sample is timed or counted"
        );
        let sends: u64 = harness
            .rows
            .latency()
            .iter()
            .filter(|row| row.stage == Stage::Send)
            .map(|row| row.count + row.backward)
            .sum();
        prop_assert_eq!(sends, wires.len() as u64, "each wire sample is timed or counted");
    }
}
