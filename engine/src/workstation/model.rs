//! The workstation's model of the engine feed: the catalog and each instrument's latest book and
//! quote, plus the panels' own models. It folds events in event time and never reads the wall clock.

use crate::ids::{FIXED_SCALE, InstrumentId, Price, Side};
use crate::msg::ui::{
    DomQuote, UiBookSnapshot, UiCatalog, UiEvent, UiInstrument, UiLatencySummary,
};
use crate::time::{DurationUs, TsUs};

use super::chart::model::{BookContinuity, ChartModel};
use super::chart::position_model::PositionModel;
use super::chart::position_view::RiskSeries;
use super::chart::view::ChartMode;
use super::dom_view::{
    DEFAULT_ROWS_PER_SIDE, DomGrouping, DomOverlay, DomUnit, FeedStatus, MAX_ROWS_PER_SIDE,
    MIN_ROWS_PER_SIDE,
};
use super::exec_model::ExecModel;
use super::format::qty_decimals;
use super::monitor::model::{MonitorModel, SystemNote};

/// The selected instrument's price grid and quantity precision. Every panel that draws a number
/// needs all three, so they travel together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InstrumentScales {
    pub tick: Price,
    pub qty_scale: i64,
    pub qty_decimals: usize,
}

/// What the selected instrument's ladder reads besides its book, built in one place so every reader
/// agrees on which orders it shows and when it reads stale.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DomSource<'a> {
    pub(crate) overlay: DomOverlay<'a>,
    pub(crate) feed: FeedStatus,
    pub(crate) stale_age: Option<DurationUs>,
}

/// How far a book may trail the engine's own clock before the ladder reads as stale.
const BOOK_STALE_AFTER: DurationUs = DurationUs::from_millis(2_000);

/// One engine run's state. Per-instrument storage grows on demand.
pub struct UiModel {
    catalog: Option<UiCatalog>,
    latest_book: Vec<Option<UiBookSnapshot>>,
    latest_quote: Vec<Option<(DomQuote, TsUs)>>,
    latest_latency: Option<UiLatencySummary>,
    book_last_seq: Vec<Option<u64>>,
    selected: InstrumentId,
    chart_mode: ChartMode,
    risk_series: RiskSeries,
    /// Each unit remembers its own grouping, so flipping between ticks and bps keeps both.
    dom_unit: DomUnit,
    dom_ticks_per_bucket: i64,
    dom_bps: (i64, i64),
    dom_levels: usize,
    spin_interval: DurationUs,
    /// Newest engine stamp seen on a book or an event. Both carry the engine's own clock, so an age
    /// measured against this needs no agreement with the workstation's clock, which may be on
    /// another machine.
    freshest_feed_ts: Option<TsUs>,
    event_last_seq: Option<u64>,
    book_gaps: u64,
    event_gaps: u64,
    monitor: MonitorModel,
    chart: ChartModel,
    positions: PositionModel,
    exec: ExecModel,
}

impl UiModel {
    pub(crate) fn new() -> Self {
        Self::with_capacity(0, DurationUs::ZERO)
    }

    pub fn with_capacity(instrument_count: usize, spin_interval: DurationUs) -> Self {
        Self::with_monitor_capacity(instrument_count, 0, spin_interval)
    }

    pub fn with_monitor_capacity(
        instrument_count: usize,
        feature_count: usize,
        spin_interval: DurationUs,
    ) -> Self {
        Self {
            catalog: None,
            latest_book: vec![None; instrument_count],
            latest_quote: vec![None; instrument_count],
            latest_latency: None,
            book_last_seq: vec![None; instrument_count],
            selected: InstrumentId(0),
            chart_mode: ChartMode::default(),
            risk_series: RiskSeries::default(),
            dom_unit: DomUnit::Bps,
            dom_ticks_per_bucket: 1,
            dom_bps: (1, 20),
            dom_levels: DEFAULT_ROWS_PER_SIDE,
            spin_interval,
            freshest_feed_ts: None,
            event_last_seq: None,
            book_gaps: 0,
            event_gaps: 0,
            monitor: MonitorModel::with_capacity(instrument_count, feature_count, spin_interval),
            chart: ChartModel::with_capacity(instrument_count, spin_interval),
            positions: PositionModel::with_capacity(instrument_count, spin_interval),
            exec: ExecModel::with_capacity(instrument_count),
        }
    }

    pub fn set_catalog(&mut self, catalog: UiCatalog) {
        // The catalog crosses the link as a bare microsecond count; this is where it becomes a span.
        let spin_interval = DurationUs::from_micros(catalog.spin_interval_us as i64);
        self.spin_interval = spin_interval;
        self.ensure_instruments(catalog.instruments.len());
        self.monitor.configure(
            catalog.instruments.len(),
            catalog.feature_names.len(),
            spin_interval,
        );
        let tick_sizes: Vec<Option<Price>> = catalog
            .instruments
            .iter()
            .map(|instrument| instrument.tick_size)
            .collect();
        self.chart.configure(&tick_sizes, spin_interval);
        self.positions
            .configure(catalog.instruments.len(), spin_interval);
        self.exec.configure(catalog.instruments.len());
        self.catalog = Some(catalog);
    }

    /// The newest book per instrument wins. A jump in the sequence counts lost snapshots, and the
    /// chart breaks its line on the same jump.
    pub fn apply_book(&mut self, snapshot: UiBookSnapshot) {
        let index = snapshot.instrument.0 as usize;
        self.ensure_instruments(index + 1);
        let lost = self.book_last_seq[index]
            .map_or(0, |previous| snapshot.seq.saturating_sub(previous + 1));
        if lost > 0 {
            self.book_gaps += lost;
            self.monitor.note_books_lost(lost, snapshot.event_ts_us);
        }
        let continuity = match lost {
            0 => BookContinuity::Continuous,
            _ => BookContinuity::GapBefore,
        };
        self.book_last_seq[index] = Some(snapshot.seq);
        self.note_feed_stamp(snapshot.event_ts_us);
        self.monitor
            .observe_book_state(snapshot.instrument, snapshot.state, snapshot.event_ts_us);
        self.chart.apply_book(&snapshot, continuity);
        self.latest_book[index] = Some(snapshot);
    }

    /// A jump in the sequence counts lost events. Each panel's model takes the kinds it cares
    /// about.
    pub fn apply_event(&mut self, event: UiEvent) {
        let seq = event.seq();
        let at = event.event_ts_us();
        if let Some(previous) = self.event_last_seq
            && seq > previous + 1
        {
            let lost = seq - previous - 1;
            self.event_gaps += lost;
            self.monitor.note_events_lost(lost, at);
            // A lost event may have closed an order, so every working order is marked lost: still
            // shown, but no longer trusted.
            self.exec.note_events_lost(at);
        }
        self.event_last_seq = Some(seq);
        self.note_feed_stamp(at);
        self.monitor.apply_event(&event);
        self.chart.apply_event(&event);
        self.positions.apply_event(&event);
        self.exec.apply_event(&event);
        if let UiEvent::Latency { summary, .. } = event {
            self.latest_latency = Some(summary);
        }
        let UiEvent::Quote {
            instrument,
            event_ts_us,
            quote,
            ..
        } = event
        else {
            return;
        };
        let index = instrument.0 as usize;
        self.ensure_instruments(index + 1);
        self.latest_quote[index] = Some((quote, event_ts_us));
    }

    pub fn book(&self, instrument: InstrumentId) -> Option<&UiBookSnapshot> {
        self.latest_book.get(instrument.0 as usize)?.as_ref()
    }

    pub fn quote(&self, instrument: InstrumentId) -> Option<(DomQuote, TsUs)> {
        *self.latest_quote.get(instrument.0 as usize)?
    }

    /// `None` until the first spin after this workstation attached.
    pub fn latency(&self) -> Option<UiLatencySummary> {
        self.latest_latency
    }

    /// Live while the book is no more than two and a half spins newer than the quote, in event
    /// time. A quote at or ahead of the book is live.
    pub fn is_quote_live(&self, instrument: InstrumentId) -> bool {
        let Some((_, quote_ts)) = self.quote(instrument) else {
            return false;
        };
        let Some(book) = self.book(instrument) else {
            return false;
        };
        let threshold = self.spin_interval.micros().saturating_mul(5) / 2;
        book.event_ts_us.diff(quote_ts).micros() <= threshold
    }

    /// How far the instrument's book trails the freshest stamp the engine has sent on anything.
    /// `None` until a book has arrived. Never negative: an out-of-order stamp reads as no lag
    /// rather than as time running backwards.
    pub fn book_lag(&self, instrument: InstrumentId) -> Option<DurationUs> {
        let book = self.book(instrument)?;
        let freshest = self.freshest_feed_ts?;
        Some(freshest.diff(book.event_ts_us).max(DurationUs::ZERO))
    }

    /// How far behind the ladder is, once that is far enough to say so. Both sides of the
    /// subtraction are the engine's clock. The workstation may run on another machine, and reading
    /// its own clock here would let a few seconds of skew paint a frozen ladder as live, which is
    /// the very reading this badge exists to prevent.
    pub(crate) fn book_staleness(&self, instrument: InstrumentId) -> Option<DurationUs> {
        self.book_lag(instrument)
            .filter(|lag| *lag > BOOK_STALE_AFTER)
    }

    /// The desired quote shows only while it is live, and a live feed over a stale book reads stale.
    pub(crate) fn dom_source(&self, feed: FeedStatus) -> DomSource<'_> {
        let selected = self.selected;
        let desired = self
            .is_quote_live(selected)
            .then(|| self.quote(selected).map(|(quote, _)| quote))
            .flatten();
        let stale_age = self.book_staleness(selected);
        let feed = match (feed, stale_age) {
            (FeedStatus::Live, Some(_)) => FeedStatus::Stale,
            (feed, _) => feed,
        };
        DomSource {
            overlay: DomOverlay {
                desired,
                bid_orders: self.exec.working(selected, Side::Buy),
                ask_orders: self.exec.working(selected, Side::Sell),
            },
            feed,
            stale_age,
        }
    }

    pub(crate) fn selected(&self) -> InstrumentId {
        self.selected
    }

    pub(crate) fn select(&mut self, instrument: InstrumentId) {
        self.selected = instrument;
    }

    /// The selected instrument's grid, as every panel that renders a number needs it. `Price(0)` and
    /// the default scale are the reading before a catalog lands, not a venue's real grid.
    pub(crate) fn instrument_scales(&self) -> InstrumentScales {
        let Some(instrument) = self.selected_instrument() else {
            return InstrumentScales {
                tick: Price(0),
                qty_scale: FIXED_SCALE,
                qty_decimals: qty_decimals(None),
            };
        };
        InstrumentScales {
            tick: instrument.tick_size.unwrap_or(Price(0)),
            qty_scale: instrument.qty_scale,
            qty_decimals: qty_decimals(instrument.lot_size),
        }
    }

    fn selected_instrument(&self) -> Option<&UiInstrument> {
        self.catalog.as_ref()?.instrument(self.selected)
    }

    pub fn chart_mode(&self) -> ChartMode {
        self.chart_mode
    }

    pub(crate) fn set_chart_mode(&mut self, mode: ChartMode) {
        self.chart_mode = mode;
    }

    pub fn dom_unit(&self) -> DomUnit {
        self.dom_unit
    }

    /// Switch units; the unit left behind keeps its remembered grouping.
    pub fn set_dom_unit(&mut self, unit: DomUnit) {
        self.dom_unit = unit;
    }

    pub fn dom_grouping(&self) -> DomGrouping {
        match self.dom_unit {
            DomUnit::Ticks => DomGrouping::Ticks {
                per_bucket: self.dom_ticks_per_bucket,
            },
            DomUnit::Bps => DomGrouping::Bps {
                numerator: self.dom_bps.0,
                denominator: self.dom_bps.1,
            },
        }
    }

    /// Rows the ladder shows per side. The panel clamps it to what fits; this is what was asked for.
    pub fn dom_levels(&self) -> usize {
        self.dom_levels
    }

    /// Clamped to `MIN_ROWS_PER_SIDE..=MAX_ROWS_PER_SIDE` — the control's range is the model's too.
    pub fn set_dom_levels(&mut self, levels: usize) {
        self.dom_levels = levels.clamp(MIN_ROWS_PER_SIDE, MAX_ROWS_PER_SIDE);
    }

    /// Store grouping in its unit's slot and make that unit active; the other slot is untouched.
    pub fn set_dom_grouping(&mut self, grouping: DomGrouping) {
        match grouping {
            DomGrouping::Ticks { per_bucket } => self.dom_ticks_per_bucket = per_bucket,
            DomGrouping::Bps {
                numerator,
                denominator,
            } => self.dom_bps = (numerator, denominator),
        }
        self.dom_unit = grouping.unit();
    }

    /// Book snapshots lost on the way here, summed from sequence jumps. The System channel shows each
    /// loss as a row; no panel shows this total.
    pub fn book_gaps(&self) -> u64 {
        self.book_gaps
    }

    /// Events lost on the way here, summed from sequence jumps.
    pub fn event_gaps(&self) -> u64 {
        self.event_gaps
    }

    pub(crate) fn catalog(&self) -> Option<&UiCatalog> {
        self.catalog.as_ref()
    }

    pub fn monitor(&self) -> &MonitorModel {
        &self.monitor
    }

    pub fn chart(&self) -> &ChartModel {
        &self.chart
    }

    pub fn positions(&self) -> &PositionModel {
        &self.positions
    }

    pub fn exec(&self) -> &ExecModel {
        &self.exec
    }

    pub fn risk_series(&self) -> RiskSeries {
        self.risk_series
    }

    pub(crate) fn set_risk_series(&mut self, series: RiskSeries) {
        self.risk_series = series;
    }

    pub fn note_lifecycle(&mut self, note: SystemNote) {
        self.monitor.note_lifecycle(note);
    }

    fn note_feed_stamp(&mut self, at: TsUs) {
        self.freshest_feed_ts = self.freshest_feed_ts.max(Some(at));
    }

    fn ensure_instruments(&mut self, len: usize) {
        if self.latest_book.len() >= len {
            return;
        }
        self.latest_book.resize(len, None);
        self.latest_quote.resize(len, None);
        self.book_last_seq.resize(len, None);
    }
}
