//! What the fold counts about itself: the drops of each sink and the events it refused. These
//! counts describe the engine, never the market, and the driver times each fold from outside.

use crate::hot::book::Book;
use crate::hot::quant::intensity::IntensityCounts;
use crate::hot::sink::LinkSink;
use crate::hot::tracker::MicroTracker;
use crate::msg::latency::EngineCounters;

use super::HotEngine;

impl HotEngine {
    /// A full output ring drops the record, never fatal.
    pub fn dropped_persist_records(&self) -> u64 {
        self.state.persist_dropped()
    }

    /// A full event ring drops the event, never fatal.
    pub fn dropped_ui_events(&self) -> u64 {
        self.ui.dropped_events()
    }

    /// Closed volume bars the tracker evicted before the fan-out could hand them over — the clock
    /// cutting faster than `tracker.volume_bars.keep` retains, not a ring drop.
    pub fn unretained_volume_bars(&self) -> u64 {
        self.unretained_volume_bars
    }

    /// A full link ring drops the frame, never fatal.
    pub fn dropped_link_frames(&self) -> u64 {
        self.link_sink.as_ref().map_or(0, LinkSink::dropped)
    }

    fn intensity_counts_sum(&self, metric: fn(&IntensityCounts) -> u64) -> u64 {
        self.state
            .derived
            .trackers
            .iter()
            .filter_map(MicroTracker::intensity)
            .map(metric)
            .sum()
    }

    /// The drop and event counts since the start of the run, which the hot loop reads once a spin.
    pub fn counters(&self) -> EngineCounters {
        let incidents = self.state.exec.incident_counts();
        EngineCounters {
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
                .derived
                .trackers
                .iter()
                .map(MicroTracker::unconfigured_kline_count)
                .sum(),
            intensity_inside_spread: self
                .intensity_counts_sum(IntensityCounts::inside_spread_count),
            intensity_without_book: self.intensity_counts_sum(IntensityCounts::without_book_count),
            orders_submitted: self.state.exec.commands_banked(),
            bank_drops: self.state.actions.dropped(),
            ui_books_dropped: self.ui.dropped_books(),
            ui_events_dropped: self.ui.dropped_events(),
            link_dropped: self.dropped_link_frames(),
            exposure_superseded: self.state.exposure.superseded(),
            fee_gaps: incidents.fee_gaps,
            stranded_positions: incidents.stranded_positions,
            orphan_fills: incidents.orphan_fills,
            unbooked_fees: incidents.unbooked_fees,
            orphan_events_without_fill: incidents.orphan_events_without_fill,
        }
    }
}
