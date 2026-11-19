//! The rows of one golden boot, each pushed for the writer and spelled by hand as the cells that a
//! reader must decode from its file, so the cells in `expected.json` come from no column list of
//! the engine.

use std::collections::BTreeMap;

use engine::msg::kinds::FeatureKind;
use engine::msg::persist::{
    AuditRecord, BookEventRow, FeatureRow, FillRow, IncidentRow, KlineRow, LedgerEntry,
    LinkFrameRow, MidRow, NoQuoteReason, NoQuoteRow, OrderRow, ResearchRecord, RotationRow,
    TradeRow,
};
use engine::runtime::manifest::RunId;
use engine::runtime::metrics::{BacklogRow, DropRow, LatencyRow, RollupRow, SenderDropRow};
use serde_json::{Value, json};

use crate::columns::{
    BOOK_EVENTS, FEATURES, FILLS, INCIDENTS, KLINES, LEDGER, LINK_FRAMES, METRICS_BACKLOG,
    METRICS_DROPS, METRICS_LATENCY, METRICS_SENDER_DROPS, MIDS, NO_QUOTES, ORDERS, ROTATIONS,
    TRADES,
};

/// The rows of one table as `expected.json` holds them.
pub struct ExpectedTable {
    pub columns: &'static [&'static str],
    pub rows: Vec<Value>,
}

/// One boot of the golden engine: what goes to each input of the writer, and what each table, the
/// footer and each metric must read back as.
pub struct Run {
    pub run_id: RunId,
    pub feature_names: &'static [&'static str],
    pub feature_kinds: &'static [FeatureKind],
    pub audit: Vec<AuditRecord>,
    pub research: Vec<ResearchRecord>,
    pub rollups: Vec<RollupRow>,
    pub rotations: Vec<RotationRow>,
    pub tables: BTreeMap<&'static str, ExpectedTable>,
    pub metrics: Value,
}

impl Run {
    pub fn new(
        run_id: RunId,
        feature_names: &'static [&'static str],
        feature_kinds: &'static [FeatureKind],
    ) -> Run {
        Run {
            run_id,
            feature_names,
            feature_kinds,
            audit: Vec::new(),
            research: Vec::new(),
            rollups: Vec::new(),
            rotations: Vec::new(),
            tables: BTreeMap::new(),
            metrics: Value::Null,
        }
    }

    fn expect(&mut self, table: &'static str, columns: &'static [&'static str], cells: Value) {
        self.tables
            .entry(table)
            .or_insert_with(|| ExpectedTable {
                columns,
                rows: Vec::new(),
            })
            .rows
            .push(cells);
    }

    pub fn order(&mut self, row: OrderRow) {
        self.audit.push(AuditRecord::Order(row));
        let cells = json!([
            row.exchange_ts_us.micros(),
            row.received_ts_us.micros(),
            row.instrument.0,
            row.client_id.0,
            row.quote_level.map(|level| level.get()),
            row.venue_order_id.map(|id| id.0),
            row.transition.as_str(),
            row.state.as_str(),
            row.previous_state.as_str(),
            row.provenance.as_str(),
            row.side.as_str(),
            row.style.map_or("unknown", |style| style.as_str()),
            row.price.mantissa(),
            row.qty.mantissa(),
            row.filled_qty.mantissa(),
            row.filled_quote,
            row.reject.map_or("none", |reject| reject.as_str()),
            row.reject_code,
            row.depth_ahead.map(|qty| qty.mantissa()),
        ]);
        self.expect("orders", ORDERS, cells);
    }

    pub fn fill(&mut self, row: FillRow) {
        self.audit.push(AuditRecord::Fill(row));
        let cells = json!([
            row.exchange_ts_us.micros(),
            row.received_ts_us.micros(),
            row.instrument.0,
            row.trade_id.map(|id| id.0),
            row.venue_order_id.map(|id| id.0),
            row.client_id.0,
            row.quote_level.map(|level| level.get()),
            row.provenance.as_str(),
            row.side.as_str(),
            row.liquidity.map_or("none", |liquidity| liquidity.as_str()),
            row.last_price.mantissa(),
            row.last_qty.mantissa(),
            row.booked_qty.mantissa(),
            row.booked_quote,
            row.commission,
            row.commission_asset.0,
        ]);
        self.expect("fills", FILLS, cells);
    }

    pub fn incident(&mut self, row: IncidentRow) {
        self.audit.push(AuditRecord::Incident(row));
        let cells = json!([
            row.exchange_ts_us.micros(),
            row.received_ts_us.micros(),
            row.kind.as_str(),
            row.instrument.0,
            row.client_id.map(|id| id.0),
            row.trade_id.map(|id| id.0),
            row.qty.mantissa(),
            row.amount,
            row.window_close_ts_us.map(|at| at.micros()),
        ]);
        self.expect("incidents", INCIDENTS, cells);
    }

    pub fn ledger(&mut self, row: LedgerEntry) {
        self.audit.push(AuditRecord::Ledger(row));
        let cells = json!([
            row.received_ts_us.micros(),
            row.kind.as_str(),
            row.instrument.0,
            row.position_base.mantissa(),
            row.cash_quote.mantissa(),
            row.basis_quote.mantissa(),
            row.realised_quote.mantissa(),
            row.mark.map(|mark| mark.mantissa()),
            row.exposure_quote.mantissa(),
            row.pnl_quote.mantissa(),
            row.client_id.map(|id| id.0),
            row.trade_id.map(|id| id.0),
        ]);
        self.expect("ledger", LEDGER, cells);
    }

    pub fn rotation(&mut self, row: RotationRow) {
        let cells = json!([
            row.received_ts_us.micros(),
            row.instrument.0,
            row.window_open_ts_us.micros(),
            row.window_close_ts_us.micros(),
            row.token_id_up.as_ref(),
            row.token_id_down.as_ref(),
            row.condition_id.as_ref(),
        ]);
        self.rotations.push(row);
        self.expect("rotations", ROTATIONS, cells);
    }

    pub fn feature(&mut self, row: FeatureRow) {
        self.research.push(ResearchRecord::Feature(row));
        let cells = json!([
            row.event_ts_us.micros(),
            row.instrument.0,
            row.feature.0,
            row.value
        ]);
        self.expect("features", FEATURES, cells);
    }

    pub fn trade(&mut self, row: TradeRow) {
        self.research.push(ResearchRecord::Trade(row));
        let cells = json!([
            row.exchange_ts_us.micros(),
            row.received_ts_us.micros(),
            row.instrument.0,
            row.price.mantissa(),
            row.qty.mantissa(),
            row.side.as_str(),
        ]);
        self.expect("trades", TRADES, cells);
    }

    pub fn book_event(&mut self, row: BookEventRow) {
        self.research.push(ResearchRecord::BookEvent(row));
        let cells = json!([
            row.received_ts_us.micros(),
            row.instrument.0,
            row.kind.as_str(),
            row.side.map_or("none", |side| side.as_str()),
            row.price.mantissa(),
            row.qty.mantissa(),
            row.update_id,
        ]);
        self.expect("book_events", BOOK_EVENTS, cells);
    }

    pub fn kline(&mut self, row: KlineRow) {
        self.research.push(ResearchRecord::Kline(row));
        let cells = json!([
            row.exchange_ts_us.micros(),
            row.received_ts_us.micros(),
            row.instrument.0,
            row.interval.as_str(),
            row.open_ts_us.micros(),
            row.open.mantissa(),
            row.high.mantissa(),
            row.low.mantissa(),
            row.close.mantissa(),
            row.base_volume.mantissa(),
            row.quote_volume,
            row.trade_count,
            row.is_closed,
        ]);
        self.expect("klines", KLINES, cells);
    }

    pub fn link_frame(&mut self, row: LinkFrameRow) {
        self.research.push(ResearchRecord::LinkFrame(row));
        let cells = json!([
            row.received_ts_us.micros(),
            row.event_ts_us.micros(),
            row.kind.as_str(),
            row.sender_te_hash,
            row.topic,
            row.seq,
            row.slot,
            row.count,
            row.value,
        ]);
        self.expect("link_frames", LINK_FRAMES, cells);
    }

    pub fn mid(&mut self, row: MidRow) {
        self.research.push(ResearchRecord::Mid(row));
        let cells = json!([
            row.event_ts_us.micros(),
            row.spin_seq,
            row.instrument.0,
            row.bid.map(|price| price.mantissa()),
            row.ask.map(|price| price.mantissa()),
            row.mid.map(|price| price.mantissa()),
        ]);
        self.expect("mids", MIDS, cells);
    }

    pub fn no_quote(&mut self, row: NoQuoteRow) {
        self.research.push(ResearchRecord::NoQuote(row));
        let cells = json!([
            row.event_ts_us.micros(),
            row.spin_seq,
            row.instrument.0,
            row.side.as_str(),
            row.reason.as_str(),
        ]);
        self.expect("no_quotes", NO_QUOTES, cells);
    }

    pub fn latency(&mut self, row: LatencyRow) {
        self.rollups.push(RollupRow::Latency(row));
        let cells = json!([
            row.interval_start_ts_us.micros(),
            row.interval_end_ts_us.micros(),
            row.category.label(),
            row.stage.label(),
            row.count,
            row.dropped,
            row.backward,
            row.negative,
            row.p50_ns,
            row.p99_ns,
            row.p999_ns,
            row.max_ns,
            row.budget_p99_ns,
            row.budget_p999_ns,
            row.is_over_budget,
            row.clock_source.label(),
        ]);
        self.expect("metrics_latency", METRICS_LATENCY, cells);
    }

    pub fn drops(&mut self, row: DropRow) {
        self.rollups.push(RollupRow::Drop(row));
        let cells = json!([
            row.interval_end_ts_us.micros(),
            row.sink.label(),
            row.dropped_total
        ]);
        self.expect("metrics_drops", METRICS_DROPS, cells);
    }

    pub fn sender_drops(&mut self, row: SenderDropRow, sender: &str) {
        self.rollups.push(RollupRow::SenderDrop(row));
        let cells = json!([
            row.interval_end_ts_us.micros(),
            sender,
            row.queue_full_total,
            row.over_rate_total,
        ]);
        self.expect("metrics_sender_drops", METRICS_SENDER_DROPS, cells);
    }

    pub fn backlog(&mut self, row: BacklogRow) {
        self.rollups.push(RollupRow::Backlog(row));
        let cells = json!([
            row.interval_end_ts_us.micros(),
            row.queue.index(),
            row.max,
            row.ema
        ]);
        self.expect("metrics_backlog", METRICS_BACKLOG, cells);
    }

    /// The table as `expected.json` holds it.
    pub fn tables_json(&self) -> Value {
        let tables: serde_json::Map<String, Value> = self
            .tables
            .iter()
            .map(|(name, table)| {
                let value = json!({ "columns": table.columns, "rows": table.rows });
                ((*name).to_owned(), value)
            })
            .collect();
        Value::Object(tables)
    }
}

/// Each reason with `count` spins, zero included.
pub fn not_quoting_spins(count: u64) -> serde_json::Value {
    let counts: serde_json::Map<String, serde_json::Value> = NoQuoteReason::ALL
        .into_iter()
        .map(|reason| (reason.as_str().to_owned(), count.into()))
        .collect();
    serde_json::Value::Object(counts)
}
