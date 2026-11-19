//! What the callbacks of one message bank, in lanes of a fixed size, drained after the callbacks in
//! a fixed order. A strategy declares quotes into engine state, so no lane carries an order, and an
//! audit row passes no lane at all: it goes straight to the audit sink, which never drops one.

use crate::hot::digest::digest_fields;
use crate::hot::sink::{AuditSink, LinkSink, ResearchSink, StrategyLogSink, UiEventSink};
use crate::link::{LinkHash, LinkPayload, OutboundLink, TopicId};
use crate::log::HotRecord;
use crate::msg::latency::LaneDrops;
use crate::msg::persist::{AuditRecord, FeatureRow, ResearchRecord};
use crate::msg::time::LocalTs;
use crate::msg::ui::UiEvent;
use crate::runtime::config::{RecordedTables, TableKind};

pub use crate::msg::ids::ClientOrderId;

/// One sweep of 64 features over 64 instruments, and a sixteenth of the persist ring, so one drain
/// cannot swamp it. About 96 KB.
pub(crate) const FEATURE_LANE_CAPACITY: usize = 4096;

/// Raw rows of trades, book events and klines, link frames, mids and the sides that did not quote.
/// A book chunk is the widest callback. About 384 KB.
pub(crate) const RESEARCH_LANE_CAPACITY: usize = 4096;

/// Log records that one callback can bank before the drain: 64 of 64 bytes, 4 KB.
pub(crate) const LOG_LANE_CAPACITY: usize = 64;

/// A callback sends few topics, and the far side reads only the newest state, so a backlog buys
/// nothing. About 9 KB.
pub(crate) const LINK_LANE_CAPACITY: usize = 32;

/// The tables that the config lets the bank write, the hash of the strategy's link fields that
/// each payload carries, and the audit sink. `None` with no `persistence:` block.
pub(crate) struct ActionsSetup {
    pub tables: RecordedTables,
    pub link_schema_hash: LinkHash,
    pub audit: Option<AuditSink>,
}

/// Where the drain sends each lane.
pub(crate) struct DrainSinks<'a> {
    /// `None` with no `persistence:` block configured.
    pub research: Option<&'a mut ResearchSink>,
    pub log_sink: &'a mut StrategyLogSink,
    pub event_sink: &'a mut UiEventSink,
    pub event_seq: &'a mut u64,
    /// `None` with no `link:` block configured — banked payloads are then discarded at the drain.
    pub link_sink: Option<&'a mut LinkSink>,
}

pub(crate) struct Actions {
    /// Features only, each also sent to the workstation. A lane of its own, so a feature that only
    /// the workstation reads never takes the place of a row for the tape.
    features: Vec<FeatureRow>,
    research: Vec<ResearchRecord>,
    logs: Vec<HotRecord>,
    link: Vec<OutboundLink>,
    /// The tables that the config names. Empty with persistence off.
    tables: RecordedTables,
    link_schema_hash: LinkHash,
    dropped: LaneDrops,
    audit: Option<AuditSink>,
}

// The lanes hold what the execution pass banked after the last drain, until the next message.
digest_fields! {
    Actions { features, research, logs, link, tables, link_schema_hash, dropped }
    not_state {
        // What its bank holds and what it posts depend on how fast the writer reads.
        audit,
    }
}

impl Actions {
    pub(crate) fn new(setup: ActionsSetup) -> Self {
        Self {
            features: Vec::with_capacity(FEATURE_LANE_CAPACITY),
            research: Vec::with_capacity(RESEARCH_LANE_CAPACITY),
            logs: Vec::with_capacity(LOG_LANE_CAPACITY),
            link: Vec::with_capacity(LINK_LANE_CAPACITY),
            tables: setup.tables,
            link_schema_hash: setup.link_schema_hash,
            dropped: LaneDrops::default(),
            audit: setup.audit,
        }
    }

    /// Not filtered here: the drain decides whether the tape takes it. A feature row comes only
    /// through here, because through the research lane it would never reach the workstation.
    #[inline]
    pub(crate) fn push_feature(&mut self, row: FeatureRow) {
        if self.features.len() == FEATURE_LANE_CAPACITY {
            self.dropped.features += 1;
            return;
        }
        self.features.push(row);
    }

    /// A row of a table that `strategy.tables` does not name is filtered, which is not a drop and is
    /// not counted. A mid and a side that did not quote pass whatever the config names.
    #[inline]
    pub(crate) fn push_research(&mut self, record: ResearchRecord) {
        if !self.records(record.table()) {
            return;
        }
        if self.research.len() == RESEARCH_LANE_CAPACITY {
            self.dropped.persist += 1;
            return;
        }
        self.research.push(record);
    }

    /// Straight to the audit sink, with no lane between that could drop it. An order or a fill row
    /// is filtered as a research row is; an incident and a ledger row pass whatever the config
    /// names.
    #[inline]
    pub(crate) fn push_audit(&mut self, record: AuditRecord) {
        if !self.records(record.table()) {
            return;
        }
        if let Some(audit) = self.audit.as_mut() {
            audit.push(record);
        }
    }

    #[inline]
    fn records(&self, table: Option<TableKind>) -> bool {
        table.is_none_or(|table| self.tables.contains(table))
    }

    /// The audit sink, `None` with no `persistence:` block.
    pub(crate) fn audit(&self) -> Option<&AuditSink> {
        self.audit.as_ref()
    }

    pub(crate) fn audit_mut(&mut self) -> Option<&mut AuditSink> {
        self.audit.as_mut()
    }

    #[inline]
    pub(crate) fn push_log(&mut self, record: HotRecord) {
        if self.logs.len() == LOG_LANE_CAPACITY {
            self.dropped.logs += 1;
            return;
        }
        self.logs.push(record);
    }

    /// # Panics
    /// When `values` holds more than `LINK_MAX_FIELDS` values.
    #[inline]
    pub(crate) fn push_link(&mut self, topic: TopicId, values: &[f64], event_ts: LocalTs) {
        if self.link.len() == LINK_LANE_CAPACITY {
            self.dropped.link += 1;
            return;
        }
        self.link.push(OutboundLink {
            topic,
            payload: LinkPayload::new(self.link_schema_hash, event_ts.to_peer(), values),
        });
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.features.is_empty()
            && self.research.is_empty()
            && self.logs.is_empty()
            && self.link.is_empty()
    }

    pub(crate) fn dropped(&self) -> LaneDrops {
        self.dropped
    }

    /// Empties each lane into its sink and keeps its capacity. The order is fixed and no clock is
    /// read, so a replay drains the same records. Each feature goes to the workstation, and to the
    /// tape only when the config names the features table.
    pub(crate) fn drain(&mut self, sinks: DrainSinks<'_>) {
        let DrainSinks {
            research: mut sink,
            log_sink,
            event_sink,
            event_seq,
            mut link_sink,
        } = sinks;
        let records_features = self.tables.contains(TableKind::Features);
        for row in self.features.drain(..) {
            event_sink.push_stamped(event_seq, |seq| UiEvent::Feature {
                instrument: row.instrument,
                seq,
                event_ts_us: row.event_ts_us.to_peer(),
                feature: row.feature,
                value: row.value,
            });
            if records_features && let Some(sink) = sink.as_mut() {
                sink.push(ResearchRecord::Feature(row));
            }
        }
        for record in self.research.drain(..) {
            if let Some(sink) = sink.as_mut() {
                sink.push(record);
            }
        }
        for record in self.logs.drain(..) {
            log_sink.push(record);
        }
        for outbound in self.link.drain(..) {
            if let Some(sink) = link_sink.as_mut() {
                sink.push(outbound);
            }
        }
    }
}
