//! Per-message instruction buffer: callbacks bank into fixed-capacity lanes (features, persist,
//! logs, link), drained after callback. One message fans out to multiple callbacks all banking to
//! same buffer before any drain. No resize, pure function of message sequence → replay-exact.
//! Order load-bearing: UI tee + engine tees need consistent sequence.
//! No ORDER/QUOTE lane: strategy declares desired quote into engine-owned level-triggered state.

use crate::config::{RecordedTables, TableKind};
use crate::link::{LinkHash, LinkPayload, OutboundLink, TopicId};
use crate::log::LogRecord;
use crate::msg::persist::{FeatureRow, PersistRecord};
use crate::msg::ui::UiEvent;
use crate::sink::{LinkSink, PersistSink, StrategyLogSink, UiEventSink};
use crate::time::TsUs;

pub use crate::ids::ClientOrderId;

/// 64 features × 64 instruments sweep (1/16 persist ring so flush can't swamp). ~96 KB.
pub(crate) const FEATURE_LANE_CAPACITY: usize = 4096;

/// Raw rows (trades, book events, klines); book chunk widest callback. Deep headroom legacy. ~384 KB.
pub(crate) const PERSIST_LANE_CAPACITY: usize = 4096;

/// Lines-per-tick, ~15 KB.
pub(crate) const LOG_LANE_CAPACITY: usize = 64;

/// Few topics/callback; backlog pointless (far side sees only newest STATE). ~9 KB.
pub(crate) const LINK_LANE_CAPACITY: usize = 32;

/// Dropped records per lane: lane full on push (output drop+count, never fatal).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct LaneDrops {
    pub features: u64,
    pub persist: u64,
    pub logs: u64,
    pub link: u64,
}

/// Bank config: disk permissions + strategy link_fields digest (every payload carries it).
pub(crate) struct ActionsSetup {
    pub tables: RecordedTables,
    pub link_schema_hash: LinkHash,
}

/// Drain destinations. Struct avoids opaque positional args.
pub(crate) struct DrainSinks<'a> {
    /// `None` with no `persistence:` block configured.
    pub persist: Option<&'a mut PersistSink>,
    pub log_sink: &'a mut StrategyLogSink,
    pub event_sink: &'a mut UiEventSink,
    pub event_seq: &'a mut u64,
    /// `None` with no `link:` block configured — banked payloads are then discarded at the drain.
    pub link_sink: Option<&'a mut LinkSink>,
}

pub(crate) struct Actions {
    /// Features only lane with UI tee. Separate from persist: tee-only rows don't displace Parquet rows.
    features: Vec<FeatureRow>,
    persist: Vec<PersistRecord>,
    logs: Vec<LogRecord>,
    link: Vec<OutboundLink>,
    /// Config authority over persist lane; empty = persistence-off.
    tables: RecordedTables,
    link_schema_hash: LinkHash,
    dropped: LaneDrops,
}

impl Actions {
    pub(crate) fn new(setup: ActionsSetup) -> Self {
        Self {
            features: Vec::with_capacity(FEATURE_LANE_CAPACITY),
            persist: Vec::with_capacity(PERSIST_LANE_CAPACITY),
            logs: Vec::with_capacity(LOG_LANE_CAPACITY),
            link: Vec::with_capacity(LINK_LANE_CAPACITY),
            tables: setup.tables,
            link_schema_hash: setup.link_schema_hash,
            dropped: LaneDrops::default(),
        }
    }

    /// Ungated (Parquet gated at drain). Feature rows only here, never via push_persist (lose tee).
    #[inline]
    pub(crate) fn push_feature(&mut self, row: FeatureRow) {
        if self.features.len() == FEATURE_LANE_CAPACITY {
            self.dropped.features += 1;
            return;
        }
        self.features.push(row);
    }

    /// Gate non-tee rows: config authority. Filtered row ≠ capacity drop, uncounted.
    #[inline]
    pub(crate) fn push_persist(&mut self, record: PersistRecord) {
        if !record
            .table()
            .is_some_and(|table| self.tables.contains(table))
        {
            return;
        }
        if self.persist.len() == PERSIST_LANE_CAPACITY {
            self.dropped.persist += 1;
            return;
        }
        self.persist.push(record);
    }

    #[inline]
    pub(crate) fn push_log(&mut self, record: LogRecord) {
        if self.logs.len() == LOG_LANE_CAPACITY {
            self.dropped.logs += 1;
            return;
        }
        self.logs.push(record);
    }

    /// # Panics
    /// >LINK_MAX_FIELDS values (see StrategyCtx::link_send).
    #[inline]
    pub(crate) fn push_link(&mut self, topic: TopicId, values: &[f64], event_ts: TsUs) {
        if self.link.len() == LINK_LANE_CAPACITY {
            self.dropped.link += 1;
            return;
        }
        self.link.push(OutboundLink {
            topic,
            payload: LinkPayload::new(self.link_schema_hash, event_ts, values),
        });
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.features.is_empty()
            && self.persist.is_empty()
            && self.logs.is_empty()
            && self.link.is_empty()
    }

    pub(crate) fn dropped(&self) -> LaneDrops {
        self.dropped
    }

    /// Drain lanes into sinks, keep capacity. Features tee to UI (push_stamped); others to own lanes.
    /// Fixed order + no clock → replay-exact. Every feature tees; only config-named features → Parquet.
    /// Persistence-off: monitor fed, Parquet empty.
    pub(crate) fn drain(&mut self, sinks: DrainSinks<'_>) {
        let DrainSinks {
            persist: mut sink,
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
                event_ts_us: row.event_ts_us,
                feature: row.feature,
                value: row.value,
            });
            if records_features && let Some(sink) = sink.as_mut() {
                sink.push(PersistRecord::Feature(row));
            }
        }
        for record in self.persist.drain(..) {
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
