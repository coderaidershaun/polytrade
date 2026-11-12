//! Strategy-facing surface: post-event state read + emission banking. Split for readability.

use core::fmt;

use crate::hot::book::Book;
use crate::hot::exec::{Balance, DesiredQuote, OrderView, QuoteLevel, TickGrid};
use crate::hot::quant::volatility::EwmaVol;
use crate::hot::tracker::MicroTracker;
use crate::ids::{AssetId, InstrumentId, Qty, Side};
use crate::link::TopicId;
use crate::log::{Level, LogRecord};
use crate::msg::persist::{BookEventRow, FeatureId, FeatureRow, KlineRow, PersistRecord, TradeRow};
use crate::time::TsUs;

use super::{StrategyCtx, WindowInfo};

impl StrategyCtx<'_> {
    /// Declare resting quote this side SHOULD have; `None` withdraws. Declare every spin: absent = expire.
    /// Engine tees declaration to DOM → ladder shows engine state, not strategy paint.
    /// Strategy cannot transmit; engine gates all decisions (price, size, existence).
    pub fn quote(
        &mut self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
        desired: Option<DesiredQuote>,
    ) {
        self.desired
            .declare(instrument, side, level, desired, self.spin_seq);
    }

    /// Declare that this instrument's position should be closed at market. Level-triggered like
    /// [`Self::quote`]: declare it every spin while flat is wanted, and stop declaring to stop
    /// wanting it. The engine sends at most one marketable order per spin, sized by what is
    /// actually held, so a partial fill simply re-fires on the next spin until nothing is left.
    ///
    /// Declaring it does not withdraw the ladder. A resting quote occupies the same side and the
    /// same order budget, so a strategy that wants OUT withdraws its quotes as well.
    pub fn flatten(&mut self, instrument: InstrumentId) {
        self.desired.declare_flatten(instrument, self.spin_seq);
    }

    /// The working order occupying this stable ladder level, if any.
    #[inline]
    pub fn resting(
        &self,
        instrument: InstrumentId,
        side: Side,
        level: QuoteLevel,
    ) -> Option<OrderView> {
        self.exec.resting(instrument, side, level)
    }

    /// Free, locked, reserved per asset. READ only (not event): moves on every fill; engine funds-gates.
    #[inline]
    pub fn balance(&self, asset: AssetId) -> Balance {
        self.exec.balance(asset)
    }

    #[inline]
    pub fn tick_grid(&self, instrument: InstrumentId) -> TickGrid {
        self.exec.tick_grid(instrument)
    }

    #[inline]
    pub fn tracker(&self, instrument: InstrumentId) -> &MicroTracker {
        &self.trackers[usize::from(instrument.0)]
    }

    #[inline]
    pub fn book(&self, instrument: InstrumentId) -> &Book {
        &self.books[usize::from(instrument.0)]
    }

    #[inline]
    pub fn ewma_vol(&self, instrument: InstrumentId) -> Option<f64> {
        self.ewma[usize::from(instrument.0)]
            .as_ref()
            .and_then(EwmaVol::volatility)
    }

    /// Current window. `None` until first MarketRotation.
    #[inline]
    pub fn window(&self, instrument: InstrumentId) -> Option<WindowInfo> {
        self.windows[usize::from(instrument.0)]
    }

    /// Signed base position from REAL fills (positive = long). Applied before fill callback fires.
    #[inline]
    pub fn position_base(&self, instrument: InstrumentId) -> Qty {
        self.ledger.row(instrument).position_base()
    }

    /// MTM signed notional (1e-8 units); 0 until has_mark or flat.
    #[inline]
    pub fn exposure_quote(&self, instrument: InstrumentId) -> i64 {
        self.ledger.row(instrument).exposure_quote()
    }

    /// Total PnL (realized + unrealized) in quote mantissa (1e-8 units).
    /// Before has_mark: cash + cost basis. After: includes MTM.
    #[inline]
    pub fn pnl_quote(&self, instrument: InstrumentId) -> i64 {
        self.ledger.row(instrument).pnl_quote()
    }

    /// Two-sided book ever committed (→ valuation available). Held over resets/parks; cleared by rotation.
    #[inline]
    pub fn has_mark(&self, instrument: InstrumentId) -> bool {
        self.ledger.row(instrument).has_mark()
    }

    /// `received_ts_us` of the message being processed — the deterministic stamp for emitted features.
    #[inline]
    pub fn event_ts(&self) -> TsUs {
        self.event_ts
    }

    /// Always reaches the UI monitor; reaches Parquet only when `strategy.tables` names `features`.
    pub fn emit(&mut self, feature: FeatureId, instrument: InstrumentId, value: f64) {
        self.actions.push_feature(FeatureRow {
            instrument,
            feature,
            value,
            event_ts_us: self.event_ts,
        });
    }

    /// [`Self::emit`] for a value a calculator may not have yet: absent emits no row, so the column
    /// reads null for that event rather than carrying a stand-in.
    pub fn emit_present(
        &mut self,
        feature: FeatureId,
        instrument: InstrumentId,
        value: Option<f64>,
    ) {
        if let Some(value) = value {
            self.emit(feature, instrument, value);
        }
    }

    pub fn emit_trade_row(&mut self, row: TradeRow) {
        self.actions.push_persist(PersistRecord::Trade(row));
    }

    pub fn emit_book_event_row(&mut self, row: BookEventRow) {
        self.actions.push_persist(PersistRecord::BookEvent(row));
    }

    pub fn emit_kline_row(&mut self, row: KlineRow) {
        self.actions.push_persist(PersistRecord::Kline(row));
    }

    /// Bank link payload: topic + values (Strategy::link_fields prefix). Stamped w/ event time (pure fn).
    /// **STATE only, never deltas/events**: far side drops rather than kills engine; only absolute values work.
    /// # Panics
    /// More than LINK_MAX_FIELDS values, an engine-reserved topic, or a topic past the declared
    /// count — all code-level facts, so each is an invariant breach, not a market condition.
    pub fn link_send(&mut self, topic: TopicId, values: &[f64]) {
        assert!(
            topic.is_strategy_topic(),
            "link_send on engine-reserved topic {topic:?} — pass a TopicId from registration.link_topics"
        );
        // The link actor sizes its per-topic sequence array from the same declared count and
        // indexes it by raw id, so an id nobody declared kills that task rather than this one.
        assert!(
            usize::from(topic.0) < TopicId::space_len(self.declared_link_topics),
            "link_send on undeclared link topic {topic:?} — the strategy declared {} — pass a TopicId from registration.link_topics",
            self.declared_link_topics
        );
        self.actions.push_link(topic, values, self.event_ts);
    }

    /// Bank log line (event time, not wall clock → pure fn of sequence). Use strategy_info!/warn!/error! macros.
    #[doc(hidden)]
    pub fn log_record(
        &mut self,
        level: Level,
        module: &'static str,
        file: &'static str,
        line: u32,
        args: fmt::Arguments<'_>,
    ) {
        self.actions.push_log(LogRecord::strategy_at(
            self.event_ts,
            level,
            module,
            file,
            line,
            args,
        ));
    }
}
