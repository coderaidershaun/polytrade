//! Strategy-facing surface: post-event state read + emission banking. Split for readability.

use crate::hot::book::Book;
use crate::hot::exec::{Balance, DesiredQuote, OrderView, QuoteLevel, TickGrid};
use crate::hot::quant::volatility::EwmaVol;
use crate::hot::tracker::MicroTracker;
use crate::link::TopicId;
use crate::log::{Callsite, HotRecord, LogArgs};
use crate::msg::ids::{AssetId, InstrumentId, Notional, Qty, Side};
use crate::msg::persist::{BookEventRow, FeatureId, FeatureRow, KlineRow, PersistRecord, TradeRow};
use crate::msg::time::{FoldTime, LocalTs};

use super::{StrategyCtx, StrategyGate, WindowInfo};

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

    /// Whether the engine calls this market back and lets its quotes out yet. While it warms, a
    /// quote declared for it is held, and the spin callback still runs for the markets that are live.
    #[inline]
    pub fn gate(&self, instrument: InstrumentId) -> StrategyGate {
        self.gates[usize::from(instrument.0)]
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

    /// The signed value of the position at the mark, zero while there is no mark or no position.
    #[inline]
    pub fn exposure_quote(&self, instrument: InstrumentId) -> Notional {
        self.ledger.row(instrument).exposure_quote()
    }

    /// Realised and unrealised PnL. Before the first mark it is cash plus cost basis.
    #[inline]
    pub fn pnl_quote(&self, instrument: InstrumentId) -> Notional {
        self.ledger.row(instrument).pnl_quote()
    }

    /// Two-sided book ever committed (→ valuation available). Held over resets/parks; cleared by rotation.
    #[inline]
    pub fn has_mark(&self, instrument: InstrumentId) -> bool {
        self.ledger.row(instrument).has_mark()
    }

    /// The received stamp of the message under fold: the stamp of a row that the strategy builds
    /// itself, as [`Self::emit`] stamps its rows. To decide by time, read [`Self::now`].
    #[inline]
    pub fn event_ts(&self) -> LocalTs {
        self.event_ts
    }

    /// The time of the fold, the latest received stamp so far. Unlike [`Self::event_ts`] it never
    /// steps back, so a strategy that paces or ages by it decides as the engine's own deadlines do.
    #[inline]
    pub fn now(&self) -> FoldTime {
        self.fold_time
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

    /// Banks one raw log record, stamped with the message time rather than a clock read, so the
    /// strategy lane is a pure function of the message sequence. Call it through `note!` or `alert!`.
    #[doc(hidden)]
    pub fn bank_log<A: LogArgs>(&mut self, callsite: &'static Callsite, args: A) {
        self.actions
            .push_log(HotRecord::new(callsite, self.event_ts, args));
    }
}
