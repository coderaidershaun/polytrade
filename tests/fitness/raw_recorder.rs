//! Probe strategy that persists every event it sees into the configured raw tables, so the replay
//! and zero-alloc tests exercise every persist emit lane.

use polysim::config::{Instruments, NoParams, StrategySpec, TableKind};
use polysim::hot::strategy::{
    EngineView, InstrumentMask, Registration, Strategy, StrategyConfig, StrategyCtx, resolve_filter,
};
use polysim::ids::{InstrumentId, Price, Qty};
use polysim::msg::inbound::{BookChunk, KlineEvent, TradeEvent};
use polysim::msg::persist::{BookEventKind, BookEventRow, FeatureId, KlineRow, TradeRow};

/// Persists raw market data per the configured table set, plus `ewma_vol` as a feature when the
/// features table is on.
pub struct RecorderStrategy {
    tables: Tables,
    ewma_feature: Option<FeatureId>,
    filter: Instruments,
    /// Resolved from `filter` against the id order in [`RecorderStrategy::register`]. `None`
    /// (records nothing) until that runs, which the engine always does before dispatch.
    recorded: Option<InstrumentMask>,
}

#[derive(Debug, Clone, Copy, Default)]
struct Tables {
    trades: bool,
    book_events: bool,
    klines: bool,
    features: bool,
}

impl StrategyConfig for RecorderStrategy {
    type Params = NoParams;

    fn from_spec(spec: &StrategySpec<NoParams>, _engine: EngineView) -> Self {
        let mut tables = Tables::default();
        for table in &spec.tables {
            match table {
                TableKind::Trades => tables.trades = true,
                TableKind::BookEvents => tables.book_events = true,
                TableKind::Klines => tables.klines = true,
                TableKind::Features => tables.features = true,
                // The link tape is teed by dispatch, and the execution tables are banked by the
                // engine's own order state, so a recorder has nothing to switch on for any of them.
                TableKind::LinkFrames | TableKind::Orders | TableKind::Fills => {}
            }
        }
        Self {
            tables,
            ewma_feature: None,
            filter: spec.instruments.clone(),
            recorded: None,
        }
    }
}

impl RecorderStrategy {
    /// Whether this instrument is in the recorded set (`instruments: all`, or its venue symbol is
    /// in the explicit list). A single index into the preresolved mask — no work on the hot path.
    #[inline]
    fn records(&self, instrument: InstrumentId) -> bool {
        self.recorded
            .as_ref()
            .is_some_and(|mask| mask.contains(instrument))
    }
}

impl Strategy for RecorderStrategy {
    fn features(&self) -> &'static [&'static str] {
        &["ewma_vol"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.ewma_feature = registration.features.first().copied();
        self.recorded = Some(resolve_filter(&self.filter, registration.instruments));
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        if !self.records(event.instrument) {
            return;
        }
        if self.tables.trades {
            ctx.emit_trade_row(TradeRow {
                instrument: event.instrument,
                price: event.price,
                qty: event.qty,
                side: event.side,
                exchange_ts_us: event.exchange_ts_us,
                received_ts_us: event.received_ts_us,
            });
        }
    }

    fn on_book_update(&mut self, ctx: &mut StrategyCtx<'_>, chunk: &BookChunk) {
        if !self.records(chunk.instrument) {
            return;
        }
        if self.tables.book_events {
            for level in chunk.active_levels() {
                ctx.emit_book_event_row(BookEventRow {
                    instrument: chunk.instrument,
                    kind: chunk.kind.into(),
                    side: Some(chunk.side),
                    price: level.price,
                    qty: level.qty,
                    update_id: chunk.update_id,
                    received_ts_us: chunk.received_ts_us,
                });
            }
        }
        if self.tables.features
            && let Some(feature) = self.ewma_feature
            && let Some(vol) = ctx.ewma_vol(chunk.instrument)
        {
            ctx.emit(feature, chunk.instrument, vol);
        }
    }

    fn on_book_reset(&mut self, ctx: &mut StrategyCtx<'_>, instrument: InstrumentId) {
        if !self.records(instrument) {
            return;
        }
        if self.tables.book_events {
            let event_ts = ctx.event_ts();
            ctx.emit_book_event_row(BookEventRow {
                instrument,
                kind: BookEventKind::Reset,
                side: None,
                price: Price(0),
                qty: Qty(0),
                update_id: 0,
                received_ts_us: event_ts,
            });
        }
    }

    fn on_kline(&mut self, ctx: &mut StrategyCtx<'_>, event: &KlineEvent) {
        if !self.records(event.instrument) {
            return;
        }
        if self.tables.klines {
            ctx.emit_kline_row(KlineRow {
                instrument: event.instrument,
                interval: event.interval,
                open_ts_us: event.open_ts_us,
                open: event.open,
                high: event.high,
                low: event.low,
                close: event.close,
                base_volume: event.base_volume,
                quote_volume: event.quote_volume,
                trade_count: event.trade_count,
                is_closed: event.is_closed,
                exchange_ts_us: event.exchange_ts_us,
                received_ts_us: event.received_ts_us,
            });
        }
    }
}
