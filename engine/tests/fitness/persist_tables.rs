//! `strategy.tables` is the LIB's authority over which tables a run has, not advice a strategy may
//! forget: a row for a table the config never named reaches no ring and gets no sink, so no
//! directory of it can appear on disk. Gating that lived only in strategy code would let a strategy
//! that skipped its own check silently record tables the operator never asked for — the quietest
//! kind of wrong, because a file nobody expects still reads back perfectly.
//!
//! The empty set is also how persistence-off is expressed (a config with no `persistence:` block
//! cannot name tables), so this is the same test for both configurations.
//!
//! Gating here is the RING's: a discarded row never reaches the writer. The disk half — a table
//! nobody named opening no file at all — is one `RecordedTables` check for every kind alike, pinned
//! once over the execution tables in `persist_exec`.

use polysim::config::{KlineInterval, RecordedTables, TableKind, TrackerSpec};
use polysim::hot::strategy::{Registration, Strategy, StrategyCtx};
use polysim::ids::{Price, Qty, Side};
use polysim::msg::inbound::{InboundMessage, TradeEvent};
use polysim::msg::persist::{BookEventKind, BookEventRow, FeatureId, KlineRow, TradeRow};

use crate::engine_support::{
    ALL_TABLES, ONE, engine_without_warmup, instrument_row, metrics_ring, persist_ring_for, pop,
    strategy_log_ring, trade,
};
use crate::parquet_readback::BASE_TS_US;

/// Every configuration worth distinguishing: none, one, a subset, all.
const CONFIGURATIONS: [&[TableKind]; 4] = [
    &[],
    &[TableKind::Features],
    &[TableKind::Trades, TableKind::Klines],
    &ALL_TABLES,
];

/// The misbehaving strategy the gate exists to contain: it emits into all four tables on every
/// trade and never once consults `spec.tables`.
struct EmitsEveryTable {
    feature: Option<FeatureId>,
}

impl Strategy for EmitsEveryTable {
    fn features(&self) -> &'static [&'static str] {
        &["probe"]
    }

    fn register(&mut self, registration: Registration<'_>) {
        self.feature = registration.features.first().copied();
    }

    fn on_trade(&mut self, ctx: &mut StrategyCtx<'_>, event: &TradeEvent) {
        ctx.emit(
            self.feature.expect("registered"),
            event.instrument,
            event.price.to_f64(),
        );
        ctx.emit_trade_row(TradeRow {
            instrument: event.instrument,
            price: event.price,
            qty: event.qty,
            side: event.side,
            exchange_ts_us: event.exchange_ts_us,
            received_ts_us: event.received_ts_us,
        });
        ctx.emit_book_event_row(BookEventRow {
            instrument: event.instrument,
            kind: BookEventKind::Reset,
            side: None,
            price: Price(0),
            qty: Qty(0),
            update_id: 0,
            received_ts_us: event.received_ts_us,
        });
        ctx.emit_kline_row(KlineRow {
            instrument: event.instrument,
            interval: KlineInterval::OneMinute,
            open_ts_us: event.received_ts_us,
            open: event.price,
            high: event.price,
            low: event.price,
            close: event.price,
            base_volume: event.qty,
            quote_volume: 0,
            trade_count: 1,
            is_closed: true,
            exchange_ts_us: event.exchange_ts_us,
            received_ts_us: event.received_ts_us,
        });
    }
}

#[test]
fn only_the_tables_the_config_names_receive_rows() {
    for configured in CONFIGURATIONS {
        let instruments = [instrument_row(0, TrackerSpec::default(), 32)];
        let (persistence, mut consumer) = persist_ring_for(64, RecordedTables::new(configured));
        let (log_sink, _logs) = strategy_log_ring(64);
        let (metrics, _metrics) = metrics_ring(64);
        let mut engine = engine_without_warmup(
            &instruments,
            Box::new(EmitsEveryTable { feature: None }),
            persistence,
            log_sink,
            metrics,
        );

        engine.dispatch(
            pop(0, 0),
            &InboundMessage::Trade(trade(0, 100 * ONE, ONE, Side::Buy, BASE_TS_US)),
        );

        let mut landed: Vec<&str> = Vec::new();
        while let Ok(record) = consumer.pop() {
            landed.extend(record.table().map(TableKind::as_str));
        }
        landed.sort_unstable();
        let mut expected: Vec<&str> = configured.iter().copied().map(TableKind::as_str).collect();
        expected.sort_unstable();
        assert_eq!(
            landed, expected,
            "a strategy emitting into every table lands rows in exactly the configured ones"
        );
        assert_eq!(
            engine.dropped_persist_records(),
            0,
            "a gated row is discarded, never counted as a full-ring drop"
        );
    }
}
