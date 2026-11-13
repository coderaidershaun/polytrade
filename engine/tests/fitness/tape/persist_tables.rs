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

use engine::hot::strategy::{Registration, Strategy, StrategyCtx};
use engine::msg::ids::{Price, Qty, Side};
use engine::msg::inbound::{InboundMessage, MessageBody, TradeEvent};
use engine::msg::persist::{BookEventKind, BookEventRow, FeatureId, KlineRow, TradeRow};
use engine::runtime::config::{KlineInterval, RecordedTables, TableKind, TrackerSpec};

use crate::engine_support::{
    ALL_TABLES, ONE, engine_without_persistence, engine_without_warmup, instrument_row,
    persist_ring_for, strategy_log_ring, trade, ts,
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
            received_ts_us: ctx.event_ts(),
        });
        ctx.emit_book_event_row(BookEventRow {
            instrument: event.instrument,
            kind: BookEventKind::Reset,
            side: None,
            price: Price::from_mantissa(0),
            qty: Qty::from_mantissa(0),
            update_id: 0,
            received_ts_us: ctx.event_ts(),
        });
        ctx.emit_kline_row(KlineRow {
            instrument: event.instrument,
            interval: KlineInterval::OneMinute,
            open_ts_us: ctx.event_ts().as_venue_calendar(),
            open: event.price,
            high: event.price,
            low: event.price,
            close: event.price,
            base_volume: event.qty,
            quote_volume: 0,
            trade_count: 1,
            is_closed: true,
            exchange_ts_us: event.exchange_ts_us,
            received_ts_us: ctx.event_ts(),
        });
    }
}

#[test]
fn only_the_tables_the_config_names_receive_rows() {
    for configured in CONFIGURATIONS {
        let instruments = [instrument_row(0, TrackerSpec::default(), 32)];
        let (persistence, mut consumer) = persist_ring_for(64, RecordedTables::new(configured));
        let (log_sink, _logs) = strategy_log_ring(64);
        let mut engine = engine_without_warmup(
            &instruments,
            Box::new(EmitsEveryTable { feature: None }),
            persistence,
            log_sink,
        );

        engine.dispatch(&InboundMessage::new(
            ts(BASE_TS_US),
            MessageBody::Trade(trade(0, 100 * ONE, ONE, Side::Buy, BASE_TS_US)),
        ));

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

/// With persistence off no ring exists, so a row that every table refuses is never a drop: the
/// rows never exist. A thousand trades would overrun any ring this file builds.
#[test]
fn rows_without_a_persistence_ring_count_as_no_drop() {
    let instruments = [instrument_row(0, TrackerSpec::default(), 32)];
    let (log_sink, _logs) = strategy_log_ring(64);
    let mut engine = engine_without_persistence(
        &instruments,
        Box::new(EmitsEveryTable { feature: None }),
        log_sink,
    );
    for offset in 0..1_000 {
        engine.dispatch(&InboundMessage::new(
            ts(BASE_TS_US + offset),
            MessageBody::Trade(trade(0, 100 * ONE, ONE, Side::Buy, BASE_TS_US + offset)),
        ));
    }
    assert_eq!(
        engine.dropped_persist_records(),
        0,
        "with no ring there is nothing to drop into — the rows never exist"
    );
}
