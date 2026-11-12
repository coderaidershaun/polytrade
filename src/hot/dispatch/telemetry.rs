//! Per-message latency stamps, per-spin metrics snapshot, drop counters per sink. Machine reads only, never market.

use crate::hot::book::Book;
use crate::hot::metrics::{EngineCounters, MessageMeta, Stage};
use crate::hot::quant::intensity::IntensityCounts;
use crate::hot::tracker::MicroTracker;
use crate::msg::ui::UiLatencySummary;
use crate::sink::LinkSink;
use crate::time::TsUs;

use super::HotEngine;

impl HotEngine {
    /// Output queue full = drop (never fatal).
    pub fn dropped_persist_records(&self) -> u64 {
        self.state.persist_dropped()
    }

    /// Metrics queue full = drop (never fatal).
    pub fn dropped_metrics_snapshots(&self) -> u64 {
        self.metrics_sink.dropped()
    }

    /// Event ring full = drop (never fatal).
    pub fn dropped_ui_events(&self) -> u64 {
        self.ui.dropped_events()
    }

    /// Closed volume bars the tracker evicted before the fan-out could hand them over — the clock
    /// cutting faster than `tracker.volume_bars.keep` retains, not a ring drop.
    pub fn unretained_volume_bars(&self) -> u64 {
        self.unretained_volume_bars
    }

    /// Link ring full = drop (never fatal).
    pub fn dropped_link_frames(&self) -> u64 {
        self.link_sink.as_ref().map_or(0, LinkSink::dropped)
    }

    pub(super) fn record_ingress(&mut self, meta: MessageMeta, dequeued_at: TsUs) {
        // Prefer the SEND stamp: match->send gap is the venue's own latency, measured separately below.
        if let Some(venue_ts) = meta.exchange_sent_ts.or(meta.exchange_ts) {
            let latency = meta.received_ts.diff(venue_ts);
            self.metrics.record(
                meta.category,
                Stage::ExchangeToReceived,
                meta.received_ts,
                latency,
            );
        }
        if let (Some(sent), Some(matched)) = (meta.exchange_sent_ts, meta.exchange_ts) {
            self.metrics.record(
                meta.category,
                Stage::ExchangeInternal,
                meta.received_ts,
                sent.diff(matched),
            );
        }
        if let Some(request_sent) = meta.request_sent_ts {
            self.metrics.record(
                meta.category,
                Stage::OrderRoundTrip,
                meta.received_ts,
                meta.received_ts.diff(request_sent),
            );
        }
        let queued_latency = meta.queued_ts.diff(meta.received_ts);
        self.metrics.record(
            meta.category,
            Stage::ReceivedToQueued,
            meta.queued_ts,
            queued_latency,
        );
        let wait = dequeued_at.diff(meta.queued_ts);
        self.metrics
            .record(meta.category, Stage::QueueWait, dequeued_at, wait);
    }

    pub(super) fn record_processing(
        &mut self,
        meta: MessageMeta,
        dequeued_at: TsUs,
        processed_at: TsUs,
    ) {
        let processing = processed_at.diff(dequeued_at);
        self.metrics
            .record(meta.category, Stage::Processing, processed_at, processing);
        let end_to_end = processed_at.diff(meta.received_ts);
        self.metrics
            .record(meta.category, Stage::EndToEnd, processed_at, end_to_end);
    }

    fn intensity_counts_sum(&self, metric: fn(&IntensityCounts) -> u64) -> u64 {
        self.state
            .trackers
            .iter()
            .filter_map(MicroTracker::intensity)
            .map(metric)
            .sum()
    }

    pub(super) fn emit_snapshot(&mut self, now: TsUs) {
        let counters = EngineCounters {
            persist_dropped: self.state.persist_dropped(),
            book_trimmed: self.state.books.iter().map(Book::trimmed_count).sum(),
            book_remove_missing: self
                .state
                .books
                .iter()
                .map(Book::remove_missing_count)
                .sum(),
            klines_unconfigured: self
                .state
                .trackers
                .iter()
                .map(MicroTracker::unconfigured_kline_count)
                .sum(),
            intensity_inside_spread: self
                .intensity_counts_sum(IntensityCounts::inside_spread_count),
            intensity_without_book: self.intensity_counts_sum(IntensityCounts::without_book_count),
            snapshots_dropped: self.metrics_sink.dropped(),
            orders_submitted: self.state.exec.commands_banked(),
            bank_drops: self.state.actions.dropped(),
            ui_books_dropped: self.ui.dropped_books(),
            ui_events_dropped: self.ui.dropped_events(),
            link_dropped: self.dropped_link_frames(),
            exposure_superseded: self.state.exposure.superseded(),
        };
        self.metrics.record_counters(counters);
        let snapshot = self.metrics.snapshot(now);
        self.metrics_sink.push(snapshot);
        self.ui
            .emit_latency(UiLatencySummary::from_snapshot(&snapshot), now);
    }
}
