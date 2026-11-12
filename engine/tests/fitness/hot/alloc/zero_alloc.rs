//! Zero-allocation fitness: after construction, the hot loop's own driver and the quant fit never
//! touch the allocator. The falsifiable proof of the steady-state no-alloc rule, green forever.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use engine::hot::book::{Book, SnapshotOutcome};
use engine::hot::dispatch::{ExecWiring, ExposureWiring, HotEngine, HotEngineSetup, LinkWiring};
use engine::hot::exec::MAX_ORDER_INSTRUMENTS;
use engine::hot::ingress::IngressQueues;
use engine::hot::metrics::DriverSamples;
use engine::hot::quant::intensity::IntensityFit;
use engine::hot::sink::{ExecSink, LinkSink};
use engine::hot::spawn::{HotDriver, Step};
use engine::hot::strategy::{Strategy, StrategyConfig};
use engine::hot::tracker::MicroTracker;
use engine::link::{
    InboundLink, LinkFrame, LinkHash, LinkOrigin, LinkPayload, OutboundLink, TopicId,
    schema_hash_of_fields,
};
use engine::log::{HotRecord, LaneReader, TextRecord, open_hot_lane, open_text_lane};
use engine::msg::exec::{
    AccountChunk, ExecCommand, ExecEvent, ExecKind, ExecLaneItem, VenueOrderStatus,
};
use engine::msg::ids::{AssetId, FIXED_SCALE, Price, Qty, Side};
use engine::msg::inbound::InboundMessage;
use engine::msg::persist::{FeatureId, PersistRecord};
use engine::msg::time::{DurationUs, Stamper, TsUs};
use engine::msg::ui::{UiBookSnapshot, UiEvent};
use engine::runtime::config::{
    BinanceMarket, Instruments, IntensitySpec, KlineInterval, PolySeries, RecordedTables,
    StrategySpec, Subscriptions, TableKind, TrackerSpec, VenueMarket,
};
use engine::runtime::exposure::ExposureSnapshot;
use engine::runtime::metrics::{SampleProducers, SinkKind};
use engine::runtime::registry::InstrumentRow;
use engine::runtime::shutdown::RunControlGate;
use engine::venues::exec::open_orders_snapshot_end;
use rtrb::{Consumer, Producer, RingBuffer};

use crate::engine_support::{
    ONE, account_chunk, book_reset, delta_chunk, detached_setup, engine_view, exec_event,
    exposure_ring, idle_at, instrument_row, kline, persist_ring_for, recorder_feature_id,
    recorder_spec, rotation, run_control, running_at, snapshot_pair, spin, strategy_log_ring,
    tracker_spec_all, trade, ts, ui_book_ring, ui_event_ring,
};
use crate::hot::driver::{Shipped, exec_settings, readiness};
use crate::micro_strategy::{MicroRecorder, MicroRecorderParams};
use crate::poly_strategy::{PolyUpParams, PolyUpPublisher, link_schema};
use crate::runtime::metrics::Harness;

fn seeded_book() -> Book {
    let (bids, asks) = snapshot_pair(0, &[(100 * ONE, 2 * ONE)], &[(101 * ONE, ONE)], 0);
    let mut book = Book::new(64);
    let first = book.apply_snapshot_chunk(&bids);
    let second = book.apply_snapshot_chunk(&asks);
    assert_eq!(
        (first, second),
        (SnapshotOutcome::Clean, SnapshotOutcome::Clean)
    );
    book
}

fn intensity_spec() -> TrackerSpec {
    TrackerSpec {
        intensity: Some(IntensitySpec {
            max_depth_ticks: 16,
            half_life_secs: 600.0,
            min_events: 5.0,
        }),
        ..TrackerSpec::default()
    }
}

/// Intensity must stay allocation-free on both halves: inside the measured window the accumulator
/// (decay, bucket increments and rotation clear) AND the strategy's pull-based MLE `fit`
/// (warm-started Nelder-Mead) must touch the allocator not at all. `estimates` proves the fit actually ran and produced output,
/// so a dead no-op cannot pass the allocation assertion by doing nothing.
#[test]
fn intensity_accumulator_and_fit_do_not_allocate() {
    let tick = ONE / 100;
    let mut tracker = MicroTracker::new(&intensity_spec(), &[], Some(Price(tick)));
    let book = seeded_book();
    tracker.on_book(&book);

    // Buy aggressors 0..3 ticks above the ask populate the ask histogram; distinct timestamps make
    // each its own group, so every step also advances the decay clock.
    let feed = |tracker: &mut MicroTracker, i: i64| {
        tracker.on_trade(&trade(
            0,
            101 * ONE + (i % 4) * tick,
            ONE,
            Side::Buy,
            i * 1_000,
        ));
    };
    let mut fit = IntensityFit::default();
    for i in 0..1_000i64 {
        feed(&mut tracker, i);
    }
    // Prime the warm-start cache before the counted window so the in-window fits refit warm.
    fit.fit(
        tracker.intensity().expect("configured"),
        TsUs::from_micros(1_000_000),
    );

    let mut estimates = 0u64;
    let before = crate::alloc_count();
    for i in 1_000..101_000i64 {
        feed(&mut tracker, i);
        if i % 1_000 == 0 {
            let now = TsUs::from_micros(i * 1_000);
            if fit
                .fit(tracker.intensity().expect("configured"), now)
                .ask
                .is_some()
            {
                estimates += 1;
            }
        }
        if i % 5_000 == 0 {
            tracker.on_rotation();
            tracker.on_book(&book);
        }
    }
    let after = crate::alloc_count();
    assert_eq!(
        after, before,
        "intensity accumulator or fit allocated in steady state"
    );
    assert!(
        estimates > 0,
        "the pull-based fit must run inside the measured window"
    );
}

const SECOND_US: i64 = 1_000_000;

/// The [`MicroRecorder`] columns the measured window counts, resolved by name so a feature inserted
/// above one of them cannot silently repoint it at its neighbour.
struct RecorderColumns {
    egarch: FeatureId,
    realised_st: FeatureId,
    realised_st_bps: FeatureId,
    obi: FeatureId,
    resilience_median: FeatureId,
    resilience_mean: FeatureId,
    /// The ask side alone: `micro_step` sends buy aggressors, so no sell print ever reaches the bid
    /// histogram.
    intensity_ask: FeatureId,
    /// The Guéant quote the ask-side (A, k) and the σ rescale that feeds both sides. The price
    /// column is the end of the whole side path — coefficients, half-spread, grid snap — so
    /// counting it proves every step of that path ran.
    gueant_ask_price: FeatureId,
    gueant_sigma: FeatureId,
    /// The volume-clock pair, emitted from `on_volume` rather than the spin: the ~1.01-USD prints
    /// against the fixed 1-USD target close at least one bar per trade, so the fan-out runs — and
    /// must emit — inside the measured window.
    volume_imbalance: FeatureId,
    volume_duration: FeatureId,
    /// The two VPIN toxicity windows and their signed-flow companions, emitted from `on_volume`
    /// once each window has its bucket count. The long window only fills because the instrument row
    /// below retains more closed bars than the shipped fixture keep — see the `keep` override.
    vpin_st: FeatureId,
    vpin_lt: FeatureId,
    vpin_signed_flow_st: FeatureId,
    vpin_signed_flow_lt: FeatureId,
    /// The per-side Hawkes columns emitted from the spin. Only the ask side is asserted live:
    /// `micro_step` sends buy aggressors alone, which hit the ask side, so the bid side never banks
    /// an arrival and its columns stay null, as with `intensity_ask`. λ is the resident live
    /// intensity, the other four the fitted kernel.
    hawkes_lambda_ask: FeatureId,
    hawkes_mu_ask: FeatureId,
    hawkes_alpha_ask: FeatureId,
    hawkes_beta_ask: FeatureId,
    hawkes_branching_ask: FeatureId,
}

impl RecorderColumns {
    /// Allocates, so every measured window resolves its columns before opening the counter.
    fn resolve() -> Self {
        Self {
            egarch: recorder_feature_id("egarch_vol_lt"),
            realised_st: recorder_feature_id("realised_vol_st"),
            realised_st_bps: recorder_feature_id("realised_vol_st_bps"),
            obi: recorder_feature_id("obi_half_bp"),
            resilience_median: recorder_feature_id("resilience_median_1m"),
            resilience_mean: recorder_feature_id("resilience_mean_1m"),
            intensity_ask: recorder_feature_id("intensity_a_ask_per_sec"),
            gueant_ask_price: recorder_feature_id("gueant_ask_price"),
            gueant_sigma: recorder_feature_id("gueant_sigma_bps"),
            volume_imbalance: recorder_feature_id("volume_bar_imbalance"),
            volume_duration: recorder_feature_id("volume_bar_duration_secs"),
            vpin_st: recorder_feature_id("vpin_st"),
            vpin_lt: recorder_feature_id("vpin_lt"),
            vpin_signed_flow_st: recorder_feature_id("vpin_signed_flow_st"),
            vpin_signed_flow_lt: recorder_feature_id("vpin_signed_flow_lt"),
            hawkes_lambda_ask: recorder_feature_id("hawkes_lambda_ask_per_sec"),
            hawkes_mu_ask: recorder_feature_id("hawkes_mu_ask_per_sec"),
            hawkes_alpha_ask: recorder_feature_id("hawkes_alpha_ask_per_sec"),
            hawkes_beta_ask: recorder_feature_id("hawkes_beta_ask_per_sec"),
            hawkes_branching_ask: recorder_feature_id("hawkes_branching_ask"),
        }
    }
}

/// Counts the feature rows the measured window produced, so a silently dead per-venue path
/// cannot pass the allocation assertion by doing nothing.
#[derive(Default)]
struct FeatureEmissions {
    egarch: u64,
    realised_st: u64,
    realised_st_bps: u64,
    obi: u64,
    resilience_median: u64,
    resilience_mean: u64,
    intensity_ask: u64,
    gueant_ask_price: u64,
    gueant_sigma: u64,
    volume_imbalance: u64,
    volume_duration: u64,
    vpin_st: u64,
    vpin_lt: u64,
    vpin_signed_flow_st: u64,
    vpin_signed_flow_lt: u64,
    hawkes_lambda_ask: u64,
    hawkes_mu_ask: u64,
    hawkes_alpha_ask: u64,
    hawkes_beta_ask: u64,
    hawkes_branching_ask: u64,
}

impl FeatureEmissions {
    fn count(&mut self, columns: &RecorderColumns, feature: FeatureId, value: f64) {
        if feature == columns.egarch {
            self.egarch += 1;
        } else if feature == columns.realised_st {
            self.realised_st += 1;
        } else if feature == columns.realised_st_bps {
            self.realised_st_bps += 1;
        } else if feature == columns.obi {
            assert!(
                (-1.0..=1.0).contains(&value),
                "obi_half_bp out of [-1,1]: {value}"
            );
            self.obi += 1;
        } else if feature == columns.resilience_median {
            self.resilience_median += 1;
        } else if feature == columns.resilience_mean {
            self.resilience_mean += 1;
        } else if feature == columns.intensity_ask {
            self.intensity_ask += 1;
        } else if feature == columns.gueant_ask_price {
            self.gueant_ask_price += 1;
        } else if feature == columns.gueant_sigma {
            self.gueant_sigma += 1;
        } else if feature == columns.volume_imbalance {
            self.volume_imbalance += 1;
        } else if feature == columns.volume_duration {
            self.volume_duration += 1;
        } else if feature == columns.vpin_st {
            assert!(
                (0.0..=1.0).contains(&value),
                "vpin_st out of [0,1]: {value}"
            );
            self.vpin_st += 1;
        } else if feature == columns.vpin_lt {
            assert!(
                (0.0..=1.0).contains(&value),
                "vpin_lt out of [0,1]: {value}"
            );
            self.vpin_lt += 1;
        } else if feature == columns.vpin_signed_flow_st {
            assert!(
                (-1.0..=1.0).contains(&value),
                "vpin_signed_flow_st out of [-1,1]: {value}"
            );
            self.vpin_signed_flow_st += 1;
        } else if feature == columns.vpin_signed_flow_lt {
            assert!(
                (-1.0..=1.0).contains(&value),
                "vpin_signed_flow_lt out of [-1,1]: {value}"
            );
            self.vpin_signed_flow_lt += 1;
        } else if feature == columns.hawkes_lambda_ask {
            // λ = μ + excitation ≥ μ > 0, and the package reports instability rather than
            // clamping, so these bound each column's own domain, not a stationarity window.
            assert!(value > 0.0, "hawkes_lambda_ask not positive: {value}");
            self.hawkes_lambda_ask += 1;
        } else if feature == columns.hawkes_mu_ask {
            assert!(value > 0.0, "hawkes_mu_ask not positive: {value}");
            self.hawkes_mu_ask += 1;
        } else if feature == columns.hawkes_alpha_ask {
            assert!(value >= 0.0, "hawkes_alpha_ask negative: {value}");
            self.hawkes_alpha_ask += 1;
        } else if feature == columns.hawkes_beta_ask {
            assert!(value > 0.0, "hawkes_beta_ask not positive: {value}");
            self.hawkes_beta_ask += 1;
        } else if feature == columns.hawkes_branching_ask {
            assert!(value >= 0.0, "hawkes_branching_ask negative: {value}");
            self.hawkes_branching_ask += 1;
        }
    }

    /// The recorder's per-spin emission, its spin-mid realised vol, its per-closed-bar `on_volume`
    /// rows, its per-side Hawkes arrival refit and an EGARCH refit all ran inside the window. No
    /// `kyle_lambda` column: the mix prints buy aggressors alone, and the strategy's 0.2-per-sign
    /// floor refuses a one-sided window forever, though its fold and rolling OLS still run.
    fn assert_live(&self) {
        let paths = [
            (
                self.egarch,
                "the binance EGARCH long-horizon path must have run",
            ),
            (
                self.realised_st,
                "the binance spin-mid realised path must have run",
            ),
            (
                self.realised_st_bps,
                "the binance realised-vol bps companion must have run",
            ),
            (
                self.obi,
                "the banded order-book imbalance path must have run",
            ),
            (
                self.resilience_median,
                "the binance resilience median path must have run",
            ),
            (
                self.resilience_mean,
                "the binance resilience mean path must have run",
            ),
            (
                self.intensity_ask,
                "the binance per-side intensity fit must have run",
            ),
            (
                self.gueant_ask_price,
                "the binance Guéant ask-side depth and grid snap must have run",
            ),
            (
                self.gueant_sigma,
                "the binance Guéant σ log→bps rescale must have run",
            ),
            (
                self.volume_imbalance,
                "the volume-bar imbalance row must reach the sink via on_volume",
            ),
            (
                self.volume_duration,
                "the volume-bar duration row must reach the sink via on_volume",
            ),
            (
                self.vpin_st,
                "the short VPIN toxicity window must reach the sink via on_volume",
            ),
            (
                self.vpin_lt,
                "the long VPIN toxicity window must reach the sink via on_volume",
            ),
            (
                self.vpin_signed_flow_st,
                "the short VPIN signed-flow companion must reach the sink",
            ),
            (
                self.vpin_signed_flow_lt,
                "the long VPIN signed-flow companion must reach the sink",
            ),
            (
                self.hawkes_lambda_ask,
                "the ask-side Hawkes resident live intensity must reach the sink",
            ),
            (
                self.hawkes_mu_ask,
                "the ask-side Hawkes baseline must reach the sink",
            ),
            (
                self.hawkes_alpha_ask,
                "the ask-side Hawkes excitation jump must reach the sink",
            ),
            (
                self.hawkes_beta_ask,
                "the ask-side Hawkes decay must reach the sink",
            ),
            (
                self.hawkes_branching_ask,
                "the ask-side Hawkes branching ratio must reach the sink",
            ),
        ];
        for (count, claim) in paths {
            assert!(count > 0, "{claim}");
        }
    }
}

/// Closed-candle steps (indexed by `i / 6`) that carry a CLOSED kline: enough to push the EGARCH
/// close history past its 300 floor early, so the first (cold) fit runs before the measured window.
const EARLY_CLOSED_KLINES: i64 = 320;

/// First loop index of the measured window.
const MEASURED_START: i64 = 10_000;

/// Inside the measured window, one candle in this many closes. Closed candles must keep arriving so
/// an EGARCH REFIT fires under the counting allocator, but rarely enough that the fit does not run
/// on every spin — a bound on the test's cost, not on what it proves.
const SPARSE_KLINE_STRIDE: i64 = 400;

/// Event-time gap between the spins of the mix: one message in six, three seconds apart.
const MICRO_SPIN: DurationUs = DurationUs::from_secs(18);

/// The binance grid instrument 0 is stamped with, and the step its buy prints walk out from the ask.
const MICRO_TICK: i64 = ONE / 100;

/// The polymarket grid, and the touch of each traded leg on it.
const POLY_TICK: i64 = FIXED_SCALE / 100;
const POLY_BID: i64 = 50 * POLY_TICK;
const POLY_ASK: i64 = 52 * POLY_TICK;
const POLY_SHARES: i64 = 5 * FIXED_SCALE;

/// Calls of `step` that the counter watches, for each shipped strategy.
const MEASURED_STEPS: u64 = 100_000;

/// A resume wipes the derived state that the recorder's columns are built from, so the engine
/// parks and resumes only once those columns have had the window.
const PARK_AT_STEP: u64 = 99_000;

/// Each shipped strategy runs with as many instruments as the order table holds.
const INSTRUMENTS: u16 = MAX_ORDER_INSTRUMENTS as u16;

/// The instrument that takes the book resets and rotations, so the traded ones keep their state.
const SPARE: u16 = INSTRUMENTS - 1;

/// Small, so each input ring wraps many times before the window opens.
const INPUT_RING: usize = 64;

const OUTPUT_RING: usize = 65_536;

/// The stage sample ring is read only in the first half of the window, so it is full in the second.
const STAGE_RING: u64 = 4_096;

/// Once in this many loop indexes the spare instrument resets and rotates, the account reports,
/// and a peer frame arrives.
const EXTRAS_EVERY: i64 = 50;

/// One name for each kind of message, in the order of [`kind_of`].
const KINDS: [&str; 10] = [
    "trade",
    "book",
    "book reset",
    "market rotation",
    "kline",
    "spin",
    "link",
    "run control",
    "exec",
    "account",
];

/// No arm catches the rest, so a new kind of message stops this file compiling until the mix sends
/// it.
fn kind_of(message: &InboundMessage) -> usize {
    match message {
        InboundMessage::Trade(_) => 0,
        InboundMessage::Book(_) => 1,
        InboundMessage::BookReset(_) => 2,
        InboundMessage::MarketRotation(_) => 3,
        InboundMessage::Kline(_) => 4,
        InboundMessage::SpinTick(_) => 5,
        InboundMessage::Link(_) => 6,
        InboundMessage::RunControl(_) => 7,
        InboundMessage::Exec(_) => 8,
        InboundMessage::Account(_) => 9,
    }
}

/// The input queues of a trading engine: market data, the timer, the venue's answers, the link.
#[derive(Debug, Clone, Copy)]
enum Queue {
    Market,
    Timer,
    Venue,
    Link,
}

const QUEUES: usize = 4;

/// The write ends of the input rings, and the count of each kind of message pushed into them.
struct Inputs {
    rings: Vec<Producer<InboundMessage>>,
    kinds: [u64; KINDS.len()],
}

impl Inputs {
    fn new() -> (Inputs, IngressQueues) {
        let (rings, consumers) = (0..QUEUES).map(|_| RingBuffer::new(INPUT_RING)).unzip();
        let inputs = Inputs {
            rings,
            kinds: [0; KINDS.len()],
        };
        (inputs, IngressQueues::new(consumers))
    }

    fn push(&mut self, queue: Queue, message: InboundMessage) {
        self.kinds[kind_of(&message)] += 1;
        self.rings[queue as usize]
            .push(message)
            .expect("the driver empties each input ring before the next push");
    }
}

fn when_of(i: i64) -> i64 {
    i * 3 * SECOND_US
}

/// One binance instrument: closed klines feed the EGARCH close history, spins sample the mids for
/// realised vol. The cycle stays six phases wide so the spin — and the per-side Hawkes refit it
/// drives, which dominates this test's runtime — keeps a fixed share of the measured window.
fn recorder_message(i: i64) -> (Queue, InboundMessage) {
    let when = when_of(i);
    match i % 6 {
        // Buy aggressors reaching 0..3 ticks past the seeded 101-unit ask. Depth must actually
        // vary: prints that all land at the touch drive the fitted k to its search-box ceiling,
        // where re-anchoring A from touch to mid overflows and the Guéant block gates off.
        0 => (
            Queue::Market,
            InboundMessage::Trade(trade(
                0,
                101 * ONE + (i % 4) * MICRO_TICK,
                1_000_000,
                Side::Buy,
                when,
            )),
        ),
        // Whole-unit bid qty against the seeded 1-unit ask: a real top-of-book imbalance that
        // clears the resilience dust floor (≥5% TOB imbalance) so a rate is produced. The
        // `i % 5 == 0` deltas set qty == ask qty, so mid == microprice anchors x0 = 0 and the
        // NEXT spin's update dust-skips. Price is untouched, so mid — and thus realised vol —
        // is unaffected.
        1 => (
            Queue::Market,
            InboundMessage::Book(delta_chunk(
                0,
                Side::Buy,
                &[(100 * ONE, (1 + i % 5) * ONE)],
                when,
            )),
        ),
        // Churn on the second level of each side. Deliberately off the touch: the arm above tunes a
        // TOB interaction the resilience gate depends on, and moving the touch here would perturb
        // it while proving nothing extra about allocation.
        2 => (
            Queue::Market,
            InboundMessage::Book(delta_chunk(
                0,
                Side::Buy,
                &[(99 * ONE, (1 + i % 5) * ONE)],
                when,
            )),
        ),
        3 => (
            Queue::Market,
            InboundMessage::Book(delta_chunk(
                0,
                Side::Sell,
                &[(102 * ONE, (1 + i % 5) * ONE)],
                when,
            )),
        ),
        4 => {
            let step = i / 6;
            let is_closed = step < EARLY_CLOSED_KLINES
                || (i >= MEASURED_START && step % SPARSE_KLINE_STRIDE == 0);
            let candle = kline(
                0,
                KlineInterval::OneMinute,
                (100 * ONE, 101 * ONE, 99 * ONE, (100 + i % 5) * ONE),
                is_closed,
                when,
            );
            (Queue::Market, InboundMessage::Kline(candle))
        }
        _ => (Queue::Timer, InboundMessage::SpinTick(spin(i as u64, when))),
    }
}

/// Prints and touch churn on both up legs, with sides that alternate from cycle to cycle.
fn publisher_message(i: i64) -> (Queue, InboundMessage) {
    let when = when_of(i);
    let side = if (i / 6) % 2 == 0 { Side::Buy } else { Side::Sell };
    let depth = (1 + i % 5) * POLY_SHARES;
    match i % 6 {
        0 => (
            Queue::Market,
            InboundMessage::Trade(trade(0, POLY_ASK, POLY_SHARES, side, when)),
        ),
        1 => (
            Queue::Market,
            InboundMessage::Book(delta_chunk(0, Side::Buy, &[(POLY_BID, depth)], when)),
        ),
        2 => (
            Queue::Market,
            InboundMessage::Book(delta_chunk(2, Side::Sell, &[(POLY_ASK, depth)], when)),
        ),
        3 => (
            Queue::Market,
            InboundMessage::Trade(trade(2, POLY_BID, POLY_SHARES, side, when)),
        ),
        4 => {
            let candle = kline(
                SPARE,
                KlineInterval::OneMinute,
                (POLY_BID, POLY_ASK, POLY_BID, POLY_ASK),
                i % 12 == 4,
                when,
            );
            (Queue::Market, InboundMessage::Kline(candle))
        }
        _ => (Queue::Timer, InboundMessage::SpinTick(spin(i as u64, when))),
    }
}

/// Two price levels of one side of a book, each a price and a quantity.
type Levels = [(i64, i64); 2];

/// The two levels on each side that every book starts from, on the grid of `shipped`.
fn touch_levels(shipped: Shipped) -> (Levels, Levels) {
    match shipped {
        Shipped::Recorder => (
            [(100 * ONE, 2 * ONE), (99 * ONE, ONE)],
            [(101 * ONE, ONE), (102 * ONE, 2 * ONE)],
        ),
        Shipped::Publisher => (
            [(POLY_BID, POLY_SHARES), (POLY_BID - POLY_TICK, POLY_SHARES)],
            [(POLY_ASK, POLY_SHARES), (POLY_ASK + POLY_TICK, POLY_SHARES)],
        ),
    }
}

fn push_snapshot(inputs: &mut Inputs, shipped: Shipped, instrument: u16, when: i64) {
    let (bids, asks) = touch_levels(shipped);
    let (bids, asks) = snapshot_pair(instrument, &bids, &asks, when);
    inputs.push(Queue::Market, InboundMessage::Book(bids));
    inputs.push(Queue::Market, InboundMessage::Book(asks));
}

/// The open window of the `a` legs runs past the whole tape, and the `b` legs host the next one.
fn push_windows(inputs: &mut Inputs, when: i64) {
    const DAY_US: i64 = 86_400 * SECOND_US;
    let (open, close) = (when - 60 * SECOND_US, when + 10 * DAY_US);
    for (instrument, (open, close)) in [
        (0, (open, close)),
        (1, (open, close)),
        (2, (close, close + 300 * SECOND_US)),
        (3, (close, close + 300 * SECOND_US)),
    ] {
        let window = rotation(instrument, open, close, when);
        inputs.push(Queue::Market, InboundMessage::MarketRotation(window));
    }
}

/// A frame of the polymarket engine's own schema, with a value in each slot.
struct PeerFrames {
    schema_hash: LinkHash,
    values: [f64; link_schema::LINK_FIELDS.len()],
}

impl PeerFrames {
    fn new() -> PeerFrames {
        PeerFrames {
            schema_hash: schema_hash_of_fields(&link_schema::LINK_FIELDS),
            values: [0.5; link_schema::LINK_FIELDS.len()],
        }
    }

    fn frame(&self, seq: u64, when: i64) -> InboundMessage {
        InboundMessage::Link(InboundLink {
            frame: LinkFrame {
                origin: LinkOrigin {
                    sender_te_hash: LinkHash::of_name("peer-te"),
                    boot_ts_us: TsUs::from_micros(1),
                    topic: TopicId::FIRST_STRATEGY,
                    seq,
                },
                payload: LinkPayload::new(
                    self.schema_hash,
                    TsUs::from_micros(when - 1),
                    &self.values,
                ),
            },
            received_ts_us: TsUs::from_micros(when),
            queued_ts_us: TsUs::from_micros(when),
        })
    }
}

/// The spare instrument resets and rotates, the account reports, and a peer frame arrives.
fn push_extras(inputs: &mut Inputs, shipped: Shipped, peers: &PeerFrames, i: i64) {
    let when = when_of(i);
    match i % EXTRAS_EVERY {
        7 => {
            inputs.push(
                Queue::Market,
                InboundMessage::BookReset(book_reset(SPARE, when)),
            );
            push_snapshot(inputs, shipped, SPARE, when);
        }
        17 => {
            let window = rotation(SPARE, when, when + 300 * SECOND_US, when);
            inputs.push(Queue::Market, InboundMessage::MarketRotation(window));
        }
        27 => {
            let balances = [(AssetId(0), 1_000 * ONE), (AssetId(1), 1_000_000 * ONE)];
            // Each report carries its own venue time, or the engine sets it aside as stale.
            let report = AccountChunk {
                venue_update_ts_ms: u64::try_from(when / 1_000).expect("message time is positive"),
                ..account_chunk(&balances, when)
            };
            inputs.push(Queue::Venue, InboundMessage::Account(report));
        }
        37 => inputs.push(Queue::Link, peers.frame(i as u64, when)),
        _ => {}
    }
}

/// Answers each command as a venue would: each placement is acknowledged and then fills in full, so
/// the next spin places again, and each cancel, amend and reconcile gets its answer.
fn answer(inputs: &mut Inputs, command: ExecCommand, when: i64) {
    let answer = match command {
        ExecCommand::Place {
            instrument,
            client_id,
            side,
            price,
            qty,
            ..
        } => {
            let placed = ExecEvent {
                kind: ExecKind::AckPlaced,
                status: Some(VenueOrderStatus::New),
                qty,
                ..exec_event(instrument, client_id, side, price.0, when)
            };
            inputs.push(Queue::Venue, InboundMessage::Exec(placed));
            ExecEvent {
                kind: ExecKind::ReportTrade,
                status: Some(VenueOrderStatus::Filled),
                qty,
                last_price: price,
                last_qty: qty,
                cumulative_qty: qty,
                cumulative_quote: price.notional(qty),
                ..exec_event(instrument, client_id, side, price.0, when)
            }
        }
        ExecCommand::Cancel {
            instrument,
            client_id,
        } => ExecEvent {
            kind: ExecKind::AckCanceled,
            status: Some(VenueOrderStatus::Canceled),
            ..exec_event(instrument, client_id, Side::Buy, 0, when)
        },
        ExecCommand::AmendQty {
            instrument,
            client_id,
            qty,
        } => ExecEvent {
            kind: ExecKind::AckAmended,
            status: Some(VenueOrderStatus::New),
            qty,
            ..exec_event(instrument, client_id, Side::Buy, 0, when)
        },
        ExecCommand::ReconcileOrder {
            instrument,
            recon_seq,
            ..
        }
        | ExecCommand::ReconcileOpenOrders {
            instrument,
            recon_seq,
        } => ExecEvent {
            recon_seq,
            ..open_orders_snapshot_end(instrument, ts(when))
        },
        ExecCommand::CancelOurs { .. } | ExecCommand::CancelPriorRun { .. } => return,
    };
    inputs.push(Queue::Venue, InboundMessage::Exec(answer));
}

fn recorder_row_at(id: u16) -> InstrumentRow {
    let mut row = instrument_row(id, tracker_spec_all(100), 128);
    row.market = VenueMarket::Binance(BinanceMarket::Spot);
    row.venue_symbol = format!("btcusdt-{id}").into();
    row.tick_size = Some(Price(MICRO_TICK));
    row.lot_size = Some(Qty(1_000));
    row.min_qty = Some(Qty(1_000));
    row.min_notional = Some(5 * ONE);
    if id == 0 {
        row.tracker.intensity = intensity_spec().intensity;
        // The long VPIN window is 250 buckets, but tracker_spec_all retains only 64 closed bars — a
        // cap no amount of trading can beat. Retaining more closed bars lets vpin_lt fill and go
        // live under the counter; the larger backing is a one-time init alloc, steady-state pushes
        // still evict.
        row.tracker
            .volume_bars
            .as_mut()
            .expect("tracker_spec_all configures a fixed volume clock")
            .keep = 300;
    }
    row
}

/// Both up legs and their down legs, then rows that no window names.
fn publisher_row_at(id: u16) -> InstrumentRow {
    let leg = match id {
        0 => "a-up".to_owned(),
        1 => "a-down".to_owned(),
        2 => "b-up".to_owned(),
        3 => "b-down".to_owned(),
        _ => format!("pad-{id}"),
    };
    InstrumentRow {
        market: VenueMarket::Polymarket(PolySeries::BtcUpDown5m),
        venue_symbol: format!("btc-updown-5m-{leg}").into(),
        display: format!("btc-updown-5m-{leg}").into(),
        base: "BTC".into(),
        quote: "USDC".into(),
        tick_size: Some(Price(POLY_TICK)),
        lot_size: Some(Qty(FIXED_SCALE / 100)),
        min_qty: Some(Qty(POLY_SHARES)),
        max_price: Some(Price(FIXED_SCALE * 99 / 100)),
        max_num_orders: Some(32),
        subscriptions: Subscriptions {
            klines: false,
            ..Subscriptions::default()
        },
        kline_intervals: Vec::new(),
        max_exposure_quote: 500 * FIXED_SCALE,
        tracker: TrackerSpec {
            intensity: Some(IntensitySpec {
                max_depth_ticks: 32,
                half_life_secs: 120.0,
                min_events: 3.0,
            }),
            ..TrackerSpec::default()
        },
        ..instrument_row(id, TrackerSpec::default(), 128)
    }
}

fn strategy_of(shipped: Shipped) -> Box<dyn Strategy> {
    let view = engine_view(MICRO_SPIN);
    match shipped {
        Shipped::Recorder => {
            let spec = recorder_spec::<MicroRecorderParams>(vec![TableKind::Features]);
            Box::new(MicroRecorder::from_spec(&spec, view))
        }
        Shipped::Publisher => Box::new(PolyUpPublisher::from_spec(
            &StrategySpec {
                instruments: Instruments::All,
                tables: Vec::new(),
                params: PolyUpParams {
                    enabled: true,
                    ..PolyUpParams::default()
                },
            },
            view,
        )),
    }
}

/// The readers of each lane that the engine and its thread write, drained after each step.
struct Readers {
    persist: Option<Consumer<PersistRecord>>,
    commands: Consumer<ExecLaneItem>,
    ui_books: Consumer<UiBookSnapshot>,
    ui_events: Consumer<UiEvent>,
    exposure: Consumer<ExposureSnapshot>,
    strategy_log: Consumer<HotRecord>,
    strategy_log_drops: Arc<AtomicU64>,
    link: Consumer<OutboundLink>,
    hot_log: LaneReader<HotRecord>,
    text_log: LaneReader<TextRecord>,
}

/// The engine of `shipped` at the order table's capacity, armed with execution and the link. The
/// recorder writes each table, and the publisher records nothing, as each ships.
fn armed_at_capacity(
    shipped: Shipped,
    rows: &[InstrumentRow],
    stamper: Stamper,
) -> (HotEngine, Readers) {
    let (persistence, persist) = match shipped {
        Shipped::Recorder => {
            let (wiring, reader) = persist_ring_for(OUTPUT_RING, every_table());
            (Some(wiring), Some(reader))
        }
        Shipped::Publisher => (None, None),
    };
    let (strategy_log_sink, strategy_log) = strategy_log_ring(OUTPUT_RING);
    let strategy_log_drops = strategy_log_sink.drops_handle();
    let (ui_book_sink, ui_books) = ui_book_ring(OUTPUT_RING);
    let (ui_event_sink, ui_events) = ui_event_ring(OUTPUT_RING);
    let (exposure_sink, exposure) = exposure_ring(OUTPUT_RING);
    let (command_producer, commands) = RingBuffer::<ExecLaneItem>::new(OUTPUT_RING);
    let (link_producer, link) = RingBuffer::<OutboundLink>::new(OUTPUT_RING);
    let control = RunControlGate::new();
    let engine = HotEngine::new(HotEngineSetup {
        persistence,
        strategy_log_sink,
        ui_book_sink,
        ui_event_sink,
        link: Some(LinkWiring {
            sink: LinkSink::new(link_producer),
            acknowledged: control.acknowledged().clone(),
        }),
        exec: Some(ExecWiring {
            sink: ExecSink::new(command_producer, stamper),
            settings: exec_settings(shipped),
            run_nonce: 0,
        }),
        exposure: ExposureWiring {
            restored: &[],
            sink: exposure_sink,
        },
        ..detached_setup(rows, strategy_of(shipped))
    });
    let readers = Readers {
        persist,
        commands,
        ui_books,
        ui_events,
        exposure,
        strategy_log,
        strategy_log_drops,
        link,
        hot_log: open_hot_lane(OUTPUT_RING),
        text_log: open_text_lane(OUTPUT_RING),
    };
    (engine, readers)
}

fn every_table() -> RecordedTables {
    RecordedTables::new(&[
        TableKind::Trades,
        TableKind::BookEvents,
        TableKind::Klines,
        TableKind::Features,
        TableKind::LinkFrames,
        TableKind::Orders,
        TableKind::Fills,
    ])
}

/// What the measured window of one run did, so a path that ran dead cannot pass the allocation
/// assertion by doing nothing.
#[derive(Default)]
struct Window {
    heap_calls: u64,
    steps: u64,
    kinds: [u64; KINDS.len()],
    commands: u64,
    tape_rows: u64,
    link_frames: u64,
    hot_records: u64,
    strategy_records: u64,
    strategy_drops: u64,
    stage_drops: u64,
    open_klines: u64,
    closed_klines: u64,
    tees: TeeKinds,
    emissions: FeatureEmissions,
}

impl Readers {
    /// Empties each lane. `window` counts only what the measured window wrote.
    fn drain(
        &mut self,
        window: &mut Window,
        columns: &RecorderColumns,
        inputs: &mut Inputs,
        when: i64,
    ) {
        if let Some(persist) = self.persist.as_mut() {
            while let Ok(record) = persist.pop() {
                window.tape_rows += 1;
                if let PersistRecord::Feature(row) = record {
                    window.emissions.count(columns, row.feature, row.value);
                }
            }
        }
        while let Ok(item) = self.commands.pop() {
            let ExecLaneItem::Command(stamped) = item else {
                continue;
            };
            window.commands += 1;
            answer(inputs, stamped.command, when);
        }
        while let Ok(snapshot) = self.ui_books.pop() {
            std::hint::black_box(snapshot);
        }
        drain_ui_kinds(&mut self.ui_events, &mut window.tees);
        while let Ok(snapshot) = self.exposure.pop() {
            std::hint::black_box(snapshot);
        }
        while let Ok(record) = self.strategy_log.pop() {
            std::hint::black_box(record);
            window.strategy_records += 1;
        }
        while let Ok(frame) = self.link.pop() {
            std::hint::black_box(frame);
            window.link_frames += 1;
        }
        while let Some(record) = self.hot_log.pop() {
            std::hint::black_box(record);
            window.hot_records += 1;
        }
        while let Some(record) = self.text_log.pop() {
            std::hint::black_box(record);
        }
    }
}

/// One run of `shipped` through the hot loop's driver, on a thread of its own so its lanes and its
/// count of heap calls are its own.
fn drive(shipped: Shipped) -> Window {
    std::thread::Builder::new()
        .stack_size(16 << 20)
        .spawn(move || drive_on_this_thread(shipped))
        .expect("the test can spawn a thread")
        .join()
        .expect("the fold does not panic")
}

fn drive_on_this_thread(shipped: Shipped) -> Window {
    const INTERVAL_US: i64 = 60_000_000;
    let rows: Vec<InstrumentRow> = (0..INSTRUMENTS)
        .map(|id| match shipped {
            Shipped::Recorder => recorder_row_at(id),
            Shipped::Publisher => publisher_row_at(id),
        })
        .collect();
    let clock = quanta::Clock::new();
    let (engine, mut readers) = armed_at_capacity(shipped, &rows, Stamper::Counter(clock.clone()));
    let Harness {
        mut metrics,
        samples,
        rows: mut sample_rows,
        ..
    } = Harness::with_stage_ring(STAGE_RING);
    let SampleProducers {
        stages, counters, ..
    } = samples;
    let samples = DriverSamples {
        stamper: Stamper::Counter(clock),
        stages,
        counters,
    };
    let (mut inputs, queues) = Inputs::new();
    let mut driver = HotDriver::new(queues, engine, Some(samples));
    let columns = RecorderColumns::resolve();
    let peers = PeerFrames::new();
    let mut window = Window::default();

    // A rotation names a new market to execution, which then waits for its readiness again, so the
    // windows land first and the readiness last.
    if shipped == Shipped::Publisher {
        push_windows(&mut inputs, 0);
    }
    for instrument in 0..INSTRUMENTS {
        step_to_idle(
            &mut driver,
            &mut readers,
            &mut window,
            &columns,
            &mut inputs,
            0,
        );
        push_snapshot(&mut inputs, shipped, instrument, 0);
    }
    step_to_idle(
        &mut driver,
        &mut readers,
        &mut window,
        &columns,
        &mut inputs,
        0,
    );
    for message in readiness(0) {
        inputs.push(Queue::Venue, message);
    }

    let mut parked_at = None;
    'window: for i in 0.. {
        if i == MEASURED_START {
            window = Window::default();
            inputs.kinds = [0; KINDS.len()];
        }
        let is_measured = i >= MEASURED_START;
        let when = when_of(i);
        let (queue, message) = match shipped {
            Shipped::Recorder => recorder_message(i),
            Shipped::Publisher => publisher_message(i),
        };
        if let (true, InboundMessage::Kline(candle)) = (is_measured, &message) {
            if candle.is_closed {
                window.closed_klines += 1;
            } else {
                window.open_klines += 1;
            }
        }
        inputs.push(queue, message);
        push_extras(&mut inputs, shipped, &peers, i);
        match parked_at {
            None if window.steps >= PARK_AT_STEP => {
                inputs.push(Queue::Link, run_control(idle_at(1), when));
                parked_at = Some(i);
            }
            Some(parked) if i == parked + 5 => {
                inputs.push(Queue::Link, run_control(running_at(2), when));
            }
            _ => {}
        }
        loop {
            let before = crate::alloc_count();
            let step = driver.step();
            let heap_calls = crate::alloc_count() - before;
            if is_measured {
                window.heap_calls += heap_calls;
            }
            if step == Step::Idle {
                break;
            }
            if is_measured {
                window.steps += 1;
            }
            readers.drain(&mut window, &columns, &mut inputs, when);
            if !is_measured || window.steps < MEASURED_STEPS / 2 {
                metrics.pass(ts(0), &mut sample_rows);
            }
            if window.steps == MEASURED_STEPS {
                break 'window;
            }
        }
    }
    window.kinds = inputs.kinds;
    window.strategy_drops = readers.strategy_log_drops.load(Ordering::Relaxed);
    metrics.finish(ts(INTERVAL_US), &mut sample_rows);
    window.stage_drops = sample_rows
        .dropped_total(SinkKind::StageSamples)
        .unwrap_or(0);
    window
}

/// Steps outside the measured window until each queue is empty, so the seeds land before the mix.
fn step_to_idle(
    driver: &mut HotDriver,
    readers: &mut Readers,
    window: &mut Window,
    columns: &RecorderColumns,
    inputs: &mut Inputs,
    when: i64,
) {
    while driver.step() == Step::Folded {
        readers.drain(window, columns, inputs, when);
    }
}

impl Window {
    fn assert_live(&self, shipped: Shipped) {
        for (kind, count) in KINDS.iter().zip(self.kinds) {
            assert!(count > 0, "the {shipped:?} window sent no {kind} message");
        }
        assert!(
            self.commands > 0,
            "the {shipped:?} command sink banked commands"
        );
        assert!(
            self.hot_records > 0,
            "the {shipped:?} hot lane took records"
        );
        assert!(
            self.stage_drops > 0,
            "the {shipped:?} stage sample ring was full"
        );
        assert_eq!(
            self.strategy_drops, 0,
            "the drained {shipped:?} log lane never dropped a record"
        );
        assert!(
            self.tees.trade > 0,
            "the {shipped:?} per-print Trade tee must have run"
        );
        assert!(
            self.tees.execution > 0,
            "the {shipped:?} per-spin Execution tee must have run — the halt latch is absolute \
             state and a panel that stopped hearing it would keep showing ARMED"
        );
        assert!(
            self.tees.fill > 0,
            "the {shipped:?} venue-fill Fill tee must have run — and with it the whole inbound \
             exec fold, which moves the ledger on the hot thread and must allocate nothing doing it"
        );
        assert!(
            self.tees.rotation > 0,
            "the {shipped:?} rotation Rotation tee must have run"
        );
        assert!(
            self.tees.quote > 0,
            "the {shipped:?} simulated Quote tee must have run"
        );
        assert!(
            self.tees.position > 0,
            "the {shipped:?} per-spin Position tee must have run"
        );
        match shipped {
            Shipped::Recorder => self.assert_recorder_live(),
            Shipped::Publisher => assert!(
                self.link_frames > 0,
                "the publisher's link_send reached the outbound ring"
            ),
        }
    }

    fn assert_recorder_live(&self) {
        assert!(
            self.tape_rows > 0,
            "the tape and the feature lane both produced rows"
        );
        assert!(
            self.strategy_records > 0,
            "the recorder's log lines reached the strategy lane"
        );
        assert!(self.tees.feature > 0, "the FeatureRow tee must have run");
        assert!(
            self.open_klines > 0 && self.closed_klines > 0,
            "both open and closed klines must flow: open={} closed={}",
            self.open_klines,
            self.closed_klines
        );
        self.emissions.assert_live();
    }
}

/// FITNESS: after construction, each step of the hot loop's own driver, over real input rings,
/// calls the allocator not once, for each kind of message and each shipped strategy at the order
/// table's capacity. The measurement, the log lanes and the command sink are all inside the window,
/// and one sample ring is full for half of it.
#[test]
fn step_allocates_nothing_for_each_message_kind() {
    for shipped in [Shipped::Recorder, Shipped::Publisher] {
        let window = drive(shipped);
        assert_eq!(
            window.heap_calls, 0,
            "the {shipped:?} driver called the allocator {} times in {} steps",
            window.heap_calls, window.steps
        );
        assert_eq!(window.steps, MEASURED_STEPS);
        window.assert_live(shipped);
    }
}

#[derive(Default)]
struct TeeKinds {
    trade: u64,
    feature: u64,
    fill: u64,
    rotation: u64,
    quote: u64,
    position: u64,
    execution: u64,
}

fn drain_ui_kinds(events: &mut Consumer<UiEvent>, kinds: &mut TeeKinds) {
    while let Ok(event) = events.pop() {
        match event {
            UiEvent::Trade { .. } => kinds.trade += 1,
            UiEvent::Feature { .. } => kinds.feature += 1,
            UiEvent::Fill { .. } => kinds.fill += 1,
            UiEvent::Rotation { .. } => kinds.rotation += 1,
            UiEvent::Quote { .. } => kinds.quote += 1,
            UiEvent::Position { .. } => kinds.position += 1,
            UiEvent::Execution { .. } => kinds.execution += 1,
            UiEvent::OrderUpdate { .. }
            | UiEvent::OrderSnapshot { .. }
            | UiEvent::Reject { .. }
            | UiEvent::Balance { .. } => {}
        }
    }
}
