//! A park and a resume leave no estimator value across the hole: after the resume, a run writes
//! what a cold run of the same suffix writes, for each shipped strategy and for a probe of the
//! engine's own derived state, whatever the length of the park. Only the position is carried.

use engine::hot::strategy::{Registration, Strategy, StrategyConfig, StrategyCtx};
use engine::msg::ids::{InstrumentId, Price, Side};
use engine::msg::inbound::{
    BookChunk, InboundMessage, KlineEvent, MessageBody, SpinTick, TradeEvent,
};
use engine::msg::persist::{FeatureId, FeatureRow, PersistRecord};
use engine::msg::time::DurationUs;
use engine::runtime::config::{IntensitySpec, KlineInterval, RecordedTables, TableKind};
use engine::runtime::registry::InstrumentRow;

use crate::engine_support::{
    FillPen, LinkedEngine, LinkedSetup, ONE, book_reset, delta_chunk, engine_view,
    engine_with_link, idle_at, instrument_row, kline, recorder_spec, rotation, run_control,
    running_at, snapshot_pair, spin, tracker_spec_all, trade, ts,
};
use crate::micro_strategy::features::FEATURE_NAMES;
use crate::micro_strategy::{MicroRecorder, MicroRecorderParams};
use crate::poly_strategy::{PolyUpParams, PolyUpPublisher};

/// One message every half second over a six-phase cycle, so a spin lands every third second, and
/// the engine view agrees, or each window that a strategy sizes from the cadence would be wrong.
const STEP_US: i64 = 500_000;
const SPIN_INTERVAL: DurationUs = DurationUs::from_secs(3);

/// The grid of the row. Prints walk out from the ask in whole ticks of it.
const TICK: i64 = ONE / 100;

/// Long enough to carry each estimator past its floor: 400 prints past the 100-arrival Hawkes
/// floor, and 400 closed candles past the 300-close EGARCH floor.
const PREFIX_STEPS: i64 = 2_400;

/// Walks the EGARCH close floor from below it to past it, 310 candles against the fit's 300, so a
/// close history carried across the hole would fit earlier than a cold one, and the fit itself runs.
const SUFFIX_STEPS: i64 = 6 * 310;

/// The park lengths: one spin, ten minutes, and an hour. Nothing arrives during a park.
const HOLES_US: [i64; 3] = [3_000_000, 600_000_000, 3_600_000_000];

/// The warmups: none, so each run calls back from its first message, and a span from the first
/// market message of each run, so the gates of the resumed run must warm again for the two to agree.
const WARMUPS: [DurationUs; 2] = [DurationUs::ZERO, DurationUs::from_secs(10)];

const PARK_TS_US: i64 = PREFIX_STEPS * STEP_US;

/// The prefix buys a clip this often, so the park has a real position to keep. Only the prefix
/// fills: the suffix is the same tape for both runs.
const FILL_EVERY_STEPS: i64 = 600;

/// A hundredth of a base unit for each fill: a position that is plainly not zero and far from the
/// exposure ceiling of the row, so it cannot withdraw a quote side and read as a leak.
const FILL_QTY: i64 = ONE / 100;

/// A strategy whose rows name what the engine derives: the code of each market callback, and on
/// each spin the EWMA volatility and the position of market 0.
#[derive(Default)]
struct GapProbe {
    call: Option<FeatureId>,
    ewma: Option<FeatureId>,
    position: Option<FeatureId>,
}

const PROBE_FEATURES: [&str; 3] = ["call", "ewma", "position"];

impl GapProbe {
    fn call(&self, ctx: &mut StrategyCtx<'_>, instrument: InstrumentId, code: f64) {
        ctx.emit(self.call.expect("registered"), instrument, code);
    }
}

impl Strategy for GapProbe {
    fn features(&self) -> &'static [&'static str] {
        &PROBE_FEATURES
    }

    fn register(&mut self, registration: Registration<'_>) {
        [self.call, self.ewma, self.position] =
            registration.feature_ids_of(&PROBE_FEATURES).map(Some);
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        self.call(ctx, event.instrument, 1.0);
    }

    fn on_book_update(&mut self, ctx: &mut StrategyCtx<'_>, chunk: &BookChunk) {
        self.call(ctx, chunk.instrument, 2.0);
    }

    fn on_book_reset(&mut self, ctx: &mut StrategyCtx<'_>, instrument: InstrumentId) {
        self.call(ctx, instrument, 3.0);
    }

    fn on_kline(&mut self, ctx: &mut StrategyCtx<'_>, event: &KlineEvent) {
        self.call(ctx, event.instrument, 4.0);
    }

    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, _tick: &SpinTick) {
        let market = InstrumentId(0);
        self.call(ctx, market, 6.0);
        let ewma = ctx.ewma_vol(market);
        ctx.emit_present(self.ewma.expect("registered"), market, ewma);
        let position = ctx.position_base(market).mantissa() as f64;
        ctx.emit(self.position.expect("registered"), market, position);
    }
}

/// The window of each rotation: longer than any suffix, so one rotation at the resync gives the
/// publisher its role for the whole suffix.
const WINDOW_US: i64 = 36_000_000_000;

/// The strategies that F26 holds to a cold run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Subject {
    BinanceRecorder,
    /// Writes no feature: each spin it publishes one link frame for the leg whose window is open.
    PolymarketRecorder,
    Probe,
}

impl Subject {
    fn strategy(self) -> Box<dyn Strategy> {
        match self {
            Subject::BinanceRecorder => {
                let spec = recorder_spec::<MicroRecorderParams>(vec![TableKind::Features]);
                Box::new(MicroRecorder::from_spec(&spec, engine_view(SPIN_INTERVAL)))
            }
            Subject::PolymarketRecorder => {
                let spec = recorder_spec::<PolyUpParams>(Vec::new());
                Box::new(PolyUpPublisher::from_spec(
                    &spec,
                    engine_view(SPIN_INTERVAL),
                ))
            }
            Subject::Probe => Box::new(GapProbe::default()),
        }
    }

    /// The Polymarket publisher reads a leg by its `-up` symbol, and its windows come from the
    /// rotation that the edge sends when it subscribes.
    fn row(self) -> InstrumentRow {
        let mut row = recorded_row();
        if self == Subject::PolymarketRecorder {
            row.venue_symbol = "btc-updown-a-up".into();
        }
        row
    }

    fn columns(self) -> &'static [&'static str] {
        match self {
            Subject::BinanceRecorder => FEATURE_NAMES,
            Subject::PolymarketRecorder => &[],
            Subject::Probe => &PROBE_FEATURES,
        }
    }

    /// The column that a park carries on purpose: the engine owns the position and keeps it, so a
    /// resumed run enters the suffix holding what the prefix bought while a cold run starts flat.
    /// The publisher writes no position, and its rotation starts a new market with none.
    fn position_column(self) -> Option<&'static str> {
        match self {
            Subject::BinanceRecorder => Some("inventory_quote"),
            Subject::PolymarketRecorder => None,
            Subject::Probe => Some("position"),
        }
    }

    /// Counts since the start of the process. A rotation keeps them too, and a researcher reads
    /// them by the difference of two rows, so they compare by how they move after the resume.
    fn lifetime_columns(self) -> &'static [&'static str] {
        match self {
            Subject::BinanceRecorder => &["markout_bid_fills", "markout_ask_fills"],
            Subject::PolymarketRecorder | Subject::Probe => &[],
        }
    }

    /// The columns that the prefix must have written, or the comparison would hold nothing.
    fn warmed_columns(self) -> &'static [&'static str] {
        match self {
            Subject::BinanceRecorder => {
                &["egarch_vol_lt", "hawkes_mu_ask_per_sec", "inventory_quote"]
            }
            Subject::PolymarketRecorder => &[],
            Subject::Probe => &["call", "ewma", "position"],
        }
    }

    /// The column whose rows a cold suffix must reach, so the estimator behind it really ran.
    fn fitted_column(self) -> Option<&'static str> {
        match self {
            Subject::BinanceRecorder => Some("egarch_omega"),
            Subject::PolymarketRecorder => None,
            Subject::Probe => Some("ewma"),
        }
    }

    /// Fewer rows than this from a cold suffix would compare nothing that matters.
    fn least_cold_rows(self) -> usize {
        match self {
            Subject::BinanceRecorder | Subject::Probe => 500,
            Subject::PolymarketRecorder => 300,
        }
    }

    fn column(self, name: &str) -> FeatureId {
        let index = self
            .columns()
            .iter()
            .position(|column| *column == name)
            .unwrap_or_else(|| panic!("{name} is not a column of {self:?}"));
        FeatureId(index as u16)
    }

    fn name_of(self, feature: FeatureId) -> &'static str {
        self.columns()[usize::from(feature.0)]
    }
}

/// The row that lights each path of the recorder: a tick grid for the snap of its quotes and for
/// Kyle, and a reach histogram for the (A, k) fit.
fn recorded_row() -> InstrumentRow {
    let mut row = instrument_row(0, tracker_spec_all(100), 128);
    row.tick_size = Some(Price::from_mantissa(TICK));
    row.tracker.intensity = Some(IntensitySpec {
        max_depth_ticks: 16,
        half_life_secs: 600.0,
        min_events: 5.0,
    });
    row
}

/// What a run wrote: its feature rows, and each frame that it published, as text so that a NaN slot
/// equals a NaN slot.
#[derive(Default)]
struct Written {
    features: Vec<FeatureRow>,
    frames: Vec<String>,
}

fn engine(subject: Subject, rows: &[InstrumentRow], warmup: DurationUs) -> LinkedEngine {
    engine_with_link(LinkedSetup {
        instruments: rows,
        strategy: subject.strategy(),
        tables: RecordedTables::new(&[TableKind::Features]),
        warmup,
    })
}

/// One phase of the six-message cycle that a Binance feed shows a strategy: a print walking out
/// from the ask, depth churn either side of it, a closed candle, and the spin.
fn message(index: i64, when: i64) -> InboundMessage {
    let level = (1 + index % 5) * ONE;
    let body = match index % 6 {
        0 => MessageBody::Trade(trade(
            0,
            101 * ONE + (index % 4) * TICK,
            1_000_000,
            Side::Buy,
            when,
        )),
        1 => MessageBody::Book(delta_chunk(0, Side::Buy, &[(100 * ONE, level)], when)),
        2 => MessageBody::Book(delta_chunk(0, Side::Buy, &[(99 * ONE, level)], when)),
        3 => MessageBody::Book(delta_chunk(0, Side::Sell, &[(102 * ONE, level)], when)),
        4 => MessageBody::Kline(kline(
            0,
            KlineInterval::OneMinute,
            (100 * ONE, 101 * ONE, 99 * ONE, (100 + index % 5) * ONE),
            true,
            when,
        )),
        _ => return spin(index as u64, when),
    };
    InboundMessage::new(ts(when), body)
}

/// A book reset and a fresh snapshot: what an edge sends when it connects, and what puts the book
/// of each run in the same state whatever it held before. A Polymarket edge also states the window
/// of the leg when it subscribes.
fn resync(subject: Subject, when: i64) -> Vec<InboundMessage> {
    let (bids, asks) = snapshot_pair(
        0,
        &[(100 * ONE, 2 * ONE), (99 * ONE, ONE)],
        &[(101 * ONE, ONE), (102 * ONE, 2 * ONE)],
        when + 1,
    );
    let mut messages = Vec::new();
    if subject == Subject::PolymarketRecorder {
        messages.push(rotation(0, when, when + WINDOW_US, when));
    }
    messages.extend([
        book_reset(0, when),
        InboundMessage::new(ts(when + 1), MessageBody::Book(bids)),
        InboundMessage::new(ts(when + 1), MessageBody::Book(asks)),
    ]);
    messages
}

fn fold(linked: &mut LinkedEngine, message: &InboundMessage, written: &mut Written) {
    linked.engine.dispatch(message);
    while let Ok(record) = linked.persist.pop() {
        if let PersistRecord::Feature(row) = record {
            written.features.push(row);
        }
    }
    while let Ok(frame) = linked.outbound.pop() {
        written.frames.push(format!("{frame:?}"));
    }
}

/// The prefix: the resync, then the tape, with a fill of the prior order every so often.
fn fold_prefix(subject: Subject, linked: &mut LinkedEngine) -> Written {
    let mut rows = Written::default();
    let mut pen = FillPen::new(0);
    for message in resync(subject, 0) {
        fold(linked, &message, &mut rows);
    }
    for index in 0..PREFIX_STEPS {
        let when = index * STEP_US;
        fold(linked, &message(index, when), &mut rows);
        if index % FILL_EVERY_STEPS == 0 {
            for message in pen.fill(Side::Buy, 101 * ONE, FILL_QTY, when + 1) {
                fold(linked, &message, &mut rows);
            }
        }
    }
    rows
}

/// The suffix that both runs fold from `resume_ts_us` on: the resync of a reconnecting edge, then
/// the tape.
fn fold_suffix(subject: Subject, linked: &mut LinkedEngine, resume_ts_us: i64) -> Written {
    let mut rows = Written::default();
    for message in resync(subject, resume_ts_us + 1) {
        fold(linked, &message, &mut rows);
    }
    for index in 0..SUFFIX_STEPS {
        let when = resume_ts_us + 3 + index * STEP_US;
        fold(linked, &message(index, when), &mut rows);
    }
    rows
}

fn counts(subject: Subject, rows: &[FeatureRow]) -> Vec<usize> {
    let mut counts = vec![0; subject.columns().len()];
    for row in rows {
        counts[usize::from(row.feature.0)] += 1;
    }
    counts
}

/// Each lifetime column less its first row after the resume, so only how it moves compares.
fn rebase_lifetime_columns(subject: Subject, rows: &mut [FeatureRow]) {
    for name in subject.lifetime_columns() {
        let feature = subject.column(name);
        let base = rows
            .iter()
            .find(|row| row.feature == feature)
            .map_or(0.0, |row| row.value);
        for row in rows.iter_mut().filter(|row| row.feature == feature) {
            row.value -= base;
        }
    }
}

/// `(position rows, each other row)`.
fn split_position(subject: Subject, rows: &[FeatureRow]) -> (Vec<FeatureRow>, Vec<FeatureRow>) {
    let position = subject.position_column().map(|name| subject.column(name));
    rows.iter()
        .copied()
        .partition(|row| Some(row.feature) == position)
}

/// The names of the columns whose counts or rows differ, so a failure names `egarch_vol_lt`.
fn differing_columns(
    subject: Subject,
    resumed: &[FeatureRow],
    cold: &[FeatureRow],
) -> Vec<&'static str> {
    let mut names: Vec<&'static str> = Vec::new();
    let resumed_counts = counts(subject, resumed);
    let cold_counts = counts(subject, cold);
    for (index, (left, right)) in resumed_counts.iter().zip(&cold_counts).enumerate() {
        if left != right {
            names.push(subject.name_of(FeatureId(index as u16)));
        }
    }
    for (left, right) in resumed.iter().zip(cold) {
        if left != right {
            names.push(subject.name_of(left.feature));
        }
    }
    names.sort_unstable();
    names.dedup();
    names
}

/// Run A folds the prefix, parks, lies idle for `hole_us`, resumes and folds the suffix. Run B
/// folds only the suffix, cold. Each row of A after the resume equals the row of B, but the
/// position, and the two write each column the same number of times.
fn resumed_equals_cold(subject: Subject, hole_us: i64, warmup: DurationUs) {
    let case = format!("{subject:?} after a hole of {hole_us} us with a warmup of {warmup:?}");
    let rows = [subject.row()];
    let mut resumed = engine(subject, &rows, warmup);
    let prefix = fold_prefix(subject, &mut resumed);
    let prefix_counts = counts(subject, &prefix.features);
    for name in subject.warmed_columns() {
        assert!(
            prefix_counts[usize::from(subject.column(name).0)] > 0,
            "{case}: the prefix never wrote {name}, so the comparison would hold nothing"
        );
    }
    assert!(
        !prefix.features.is_empty() || !prefix.frames.is_empty(),
        "{case}: the prefix wrote nothing, so the comparison would hold nothing"
    );

    let resume_ts_us = PARK_TS_US + hole_us;
    let before = crate::alloc_count();
    for marker in [
        run_control(idle_at(1), PARK_TS_US),
        run_control(running_at(2), resume_ts_us),
    ] {
        resumed.engine.dispatch(&marker);
    }
    assert_eq!(
        crate::alloc_count(),
        before,
        "{case}: the park and the resume allocated: each reset clears its state in place"
    );
    while resumed.persist.pop().is_ok() {}
    while resumed.outbound.pop().is_ok() {}

    let mut written = fold_suffix(subject, &mut resumed, resume_ts_us);
    let mut cold = engine(subject, &rows, warmup);
    let mut cold_written = fold_suffix(subject, &mut cold, resume_ts_us);
    let cold_rows = cold_written.features.len() + cold_written.frames.len();
    assert!(
        cold_rows > subject.least_cold_rows(),
        "{case}: the cold suffix wrote only {cold_rows} rows, so the comparison holds little"
    );
    let first_frame_apart = written
        .frames
        .iter()
        .zip(&cold_written.frames)
        .position(|(resumed, cold)| resumed != cold);
    assert!(
        first_frame_apart.is_none() && written.frames.len() == cold_written.frames.len(),
        "{case}: link frames after the resume differ from a cold run's: {} against {} frames, \
         first apart at {first_frame_apart:?}: {:?} against {:?}",
        written.frames.len(),
        cold_written.frames.len(),
        first_frame_apart.map(|index| &written.frames[index]),
        first_frame_apart.map(|index| &cold_written.frames[index]),
    );
    let after_resume = &mut written.features;
    let from_cold = &mut cold_written.features;
    rebase_lifetime_columns(subject, after_resume);
    rebase_lifetime_columns(subject, from_cold);

    let cold_counts = counts(subject, from_cold);
    if let Some(fitted) = subject.fitted_column() {
        assert!(
            cold_counts[usize::from(subject.column(fitted).0)] > 0,
            "{case}: the cold suffix never wrote {fitted}, so its estimator never ran"
        );
    }
    assert_eq!(
        counts(subject, after_resume),
        cold_counts,
        "{case}: columns written a different number of times after the resume: {:?}",
        differing_columns(subject, after_resume, from_cold)
    );
    let (resumed_position, resumed_rest) = split_position(subject, after_resume);
    let (cold_position, cold_rest) = split_position(subject, from_cold);
    if subject.position_column().is_some() {
        assert!(
            resumed_position.first().is_some_and(|row| row.value != 0.0),
            "{case}: the resumed run carries no position, so the park kept nothing"
        );
        assert_ne!(
            resumed_position, cold_position,
            "{case}: the park keeps the position, so the resumed run's differs from a cold run's"
        );
    }
    assert_eq!(
        resumed_rest,
        cold_rest,
        "{case}: rows after the resume differ from a cold run's: {:?} carried a value across the hole",
        differing_columns(subject, &resumed_rest, &cold_rest)
    );
}

/// FITNESS: a resumed run equals a cold run, for each strategy and each park length. A value that
/// a strategy or the engine folds across the hole would write rows that look ordinary and describe a
/// market that the run never saw: research data corrupted in silence.
#[test]
fn resumed_run_equals_cold_run_for_each_strategy() {
    for subject in [
        Subject::BinanceRecorder,
        Subject::PolymarketRecorder,
        Subject::Probe,
    ] {
        for hole_us in HOLES_US {
            for warmup in WARMUPS {
                resumed_equals_cold(subject, hole_us, warmup);
            }
        }
    }
}
