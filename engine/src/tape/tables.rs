//! The tables of one run on the writer thread, grouped by the input that feeds them, so each input
//! routes its records to its own tables and seals only those.

use crate::msg::persist::{
    AuditRecord, BookEventRow, FeatureRow, FillRow, IncidentRow, KlineRow, LedgerEntry,
    LinkFrameRow, MidRow, NoQuoteRow, OrderRow, ResearchRecord, RotationRow, TradeRow,
};
use crate::runtime::config::{RecordedTables, TableKind};
use crate::runtime::metrics::{BacklogRow, DropRow, LatencyRow, RollupRow, SenderDropRow};

use super::fault::{Faults, Table};
use super::schema::TableRow;
use super::table::{TableFiles, TableWriter};

/// Where one record went.
pub(super) enum Routed {
    Landed,
    /// `strategy.tables` does not name its table, so it has no file.
    Unnamed(TableKind),
    Seal,
}

/// Opens the writer of each table that the run writes.
pub(super) struct TableWriterBuilder {
    pub(super) tables: RecordedTables,
    pub(super) files: TableFiles,
}

impl TableWriterBuilder {
    fn named<R: TableRow>(&self, table: TableKind) -> Table<R> {
        if self.tables.contains(table) { self.always_written() } else { Table::Unnamed }
    }

    /// For each table that no switch of `strategy.tables` names.
    fn always_written<R: TableRow>(&self) -> Table<R> {
        Table::Writing(Box::new(TableWriter::new(self.files.clone())))
    }
}

/// The record of money: written whole while quoting runs, or quoting halts.
pub(super) struct AuditTables {
    orders: Table<OrderRow>,
    fills: Table<FillRow>,
    incidents: Table<IncidentRow>,
    ledger: Table<LedgerEntry>,
    /// The venue lineage, without which no order row says which market its instrument slot stood
    /// for. It comes by a channel of its own and seals with the audit tables.
    rotations: Table<RotationRow>,
}

impl AuditTables {
    pub(super) fn open(builder: &TableWriterBuilder) -> Self {
        Self {
            orders: builder.named(TableKind::Orders),
            fills: builder.named(TableKind::Fills),
            incidents: builder.always_written(),
            ledger: builder.always_written(),
            rotations: builder.always_written(),
        }
    }

    pub(super) fn route(&mut self, record: AuditRecord, faults: &mut Faults) -> Routed {
        let landed = match record {
            AuditRecord::Order(row) => self.orders.push(row, faults),
            AuditRecord::Fill(row) => self.fills.push(row, faults),
            AuditRecord::Incident(row) => self.incidents.push(row, faults),
            AuditRecord::Ledger(row) => self.ledger.push(row, faults),
            AuditRecord::SealAll => return Routed::Seal,
        };
        routed(landed, record.table())
    }

    pub(super) fn push_rotation(&mut self, row: RotationRow, faults: &mut Faults) {
        self.rotations.push(row, faults);
    }

    pub(super) fn seal(&mut self, faults: &mut Faults) {
        self.orders.seal(faults);
        self.fills.seal(faults);
        self.incidents.seal(faults);
        self.ledger.seal(faults);
        self.rotations.seal(faults);
    }
}

/// What research reads. A fault stops one table and the others go on.
pub(super) struct ResearchTables {
    features: Table<FeatureRow>,
    trades: Table<TradeRow>,
    book_events: Table<BookEventRow>,
    klines: Table<KlineRow>,
    link_frames: Table<LinkFrameRow>,
    mids: Table<MidRow>,
    no_quotes: Table<NoQuoteRow>,
}

impl ResearchTables {
    pub(super) fn open(builder: &TableWriterBuilder) -> Self {
        Self {
            features: builder.named(TableKind::Features),
            trades: builder.named(TableKind::Trades),
            book_events: builder.named(TableKind::BookEvents),
            klines: builder.named(TableKind::Klines),
            link_frames: builder.named(TableKind::LinkFrames),
            mids: builder.always_written(),
            no_quotes: builder.always_written(),
        }
    }

    pub(super) fn route(&mut self, record: ResearchRecord, faults: &mut Faults) -> Routed {
        let landed = match record {
            ResearchRecord::Feature(row) => self.features.push(row, faults),
            ResearchRecord::Trade(row) => self.trades.push(row, faults),
            ResearchRecord::BookEvent(row) => self.book_events.push(row, faults),
            ResearchRecord::Kline(row) => self.klines.push(row, faults),
            ResearchRecord::LinkFrame(row) => self.link_frames.push(row, faults),
            ResearchRecord::Mid(row) => self.mids.push(row, faults),
            ResearchRecord::NoQuote(row) => self.no_quotes.push(row, faults),
            ResearchRecord::SealAll => return Routed::Seal,
        };
        routed(landed, record.table())
    }

    pub(super) fn seal(&mut self, faults: &mut Faults) {
        self.features.seal(faults);
        self.trades.seal(faults);
        self.book_events.seal(faults);
        self.klines.seal(faults);
        self.link_frames.seal(faults);
        self.mids.seal(faults);
        self.no_quotes.seal(faults);
    }
}

/// The rows of the metrics thread. They seal with the research tables.
pub(super) struct MetricTables {
    latency: Table<LatencyRow>,
    drops: Table<DropRow>,
    sender_drops: Table<SenderDropRow>,
    backlog: Table<BacklogRow>,
}

impl MetricTables {
    pub(super) fn open(builder: &TableWriterBuilder) -> Self {
        Self {
            latency: builder.always_written(),
            drops: builder.always_written(),
            sender_drops: builder.always_written(),
            backlog: builder.always_written(),
        }
    }

    pub(super) fn push(&mut self, row: RollupRow, faults: &mut Faults) {
        match row {
            RollupRow::Latency(row) => self.latency.push(row, faults),
            RollupRow::Drop(row) => self.drops.push(row, faults),
            RollupRow::SenderDrop(row) => self.sender_drops.push(row, faults),
            RollupRow::Backlog(row) => self.backlog.push(row, faults),
        };
    }

    pub(super) fn seal(&mut self, faults: &mut Faults) {
        self.latency.seal(faults);
        self.drops.seal(faults);
        self.sender_drops.seal(faults);
        self.backlog.seal(faults);
    }
}

fn routed(landed: bool, table: Option<TableKind>) -> Routed {
    match (landed, table) {
        (false, Some(table)) => Routed::Unnamed(table),
        _ => Routed::Landed,
    }
}
