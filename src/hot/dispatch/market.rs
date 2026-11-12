//! Market handlers: trade/book/reset/rotation/kline state transitions. Separate from dispatch loop body for readability.

use crate::hot::book::{BookState, SnapshotOutcome};
use crate::hot::strategy::WindowInfo;
use crate::ids::{InstrumentId, Price};
use crate::msg::inbound::{
    BookChunk, BookChunkKind, BookReset, KlineEvent, MarketRotation, TradeEvent,
};
use crate::warn;

use super::HotEngine;

impl HotEngine {
    pub(super) fn on_trade(&mut self, event: &TradeEvent) {
        let index = usize::from(event.instrument.0);
        let closed = self.state.trackers[index].on_trade(event);
        // Tape ahead of warmup (operators see prints).
        self.ui.emit_trade(event);
        if !self.is_strategy_live() {
            return;
        }
        let mut ctx = self.state.ctx(event.received_ts_us);
        self.strategy.on_trade(&mut ctx, event);
        if closed > 0 {
            self.fan_out_volume_bars(event, index, closed);
        }
    }

    /// Bars copied before ctx built (series borrow + &mut Actions coexist limit).
    fn fan_out_volume_bars(&mut self, event: &TradeEvent, index: usize, closed: usize) {
        let retained = self.state.trackers[index]
            .volume_bars()
            .map_or(0, |series| series.closed.len());
        let deliverable = closed.min(retained);
        if deliverable < closed {
            self.record_unretained_volume_bars(closed - deliverable);
        }
        for offset in (0..deliverable).rev() {
            let bar = self.state.trackers[index]
                .volume_bars()
                .and_then(|series| series.closed.get(series.closed.len() - 1 - offset));
            if let Some(bar) = bar {
                let mut ctx = self.state.ctx(event.received_ts_us);
                self.strategy.on_volume(&mut ctx, event.instrument, &bar);
            }
        }
    }

    /// Keep overflow -> bars evicted. Warned once, counted after.
    #[cold]
    fn record_unretained_volume_bars(&mut self, count: usize) {
        if self.unretained_volume_bars == 0 {
            warn!(
                "volume bars closing faster than tracker.volume_bars.keep retains — {count} bar(s) never reached the strategy; raise keep or the threshold"
            );
        }
        self.unretained_volume_bars += count as u64;
    }

    pub(super) fn on_book(&mut self, chunk: &BookChunk) {
        let index = usize::from(chunk.instrument.0);
        match chunk.kind {
            BookChunkKind::Snapshot => {
                if self.state.books[index].apply_snapshot_chunk(chunk)
                    == SnapshotOutcome::ImplicitReset
                {
                    self.reset_derived_state(index);
                }
            }
            BookChunkKind::Delta => self.state.books[index].apply_delta_chunk(chunk),
        }
        // Commit boundary only (mid-update microprices decay EWMA). One-sided = None.
        if chunk.is_last_chunk && self.state.books[index].state() == BookState::Valid {
            let microprice = self.state.trackers[index].on_book(&self.state.books[index]);
            if let (Some(ewma), Some(microprice)) = (self.state.ewma[index].as_mut(), microprice) {
                ewma.on_microprice(microprice);
            }
            self.mark_ledger(chunk.instrument);
            // Message stamp (replay-exact, not clock).
            self.state
                .exec
                .on_book_commit(chunk.instrument, chunk.received_ts_us);
            self.ui
                .emit_book(chunk.instrument, &self.state.books, chunk.received_ts_us);
        }
        if !self.is_strategy_live() {
            return;
        }
        let mut ctx = self.state.ctx(chunk.received_ts_us);
        self.strategy.on_book_update(&mut ctx, chunk);
    }

    /// Mid = bid + half-spread (no overflow). Crossed books accepted (warned anomaly already).
    #[inline]
    fn mark_ledger(&mut self, instrument: InstrumentId) {
        let book = &self.state.books[usize::from(instrument.0)];
        let Some((bid, ask)) = book.best_bid().zip(book.best_ask()) else {
            return;
        };
        let mid = Price(bid.price.0 + (ask.price.0 - bid.price.0) / 2);
        self.state.ledger.set_mark(instrument, mid);
    }

    /// Fires on proven loss only (rare).
    #[cold]
    pub(super) fn on_book_reset(&mut self, reset: &BookReset) {
        let index = usize::from(reset.instrument.0);
        self.state.books[index].apply_reset();
        self.reset_derived_state(index);
        // Dims UI (dropped levels not live). Ahead of warmup.
        self.ui
            .emit_book(reset.instrument, &self.state.books, reset.received_ts_us);
        if !self.is_strategy_live() {
            return;
        }
        let mut ctx = self.state.ctx(reset.received_ts_us);
        self.strategy.on_book_reset(&mut ctx, reset.instrument);
    }

    /// Reset or implicit snapshot reset (rare).
    #[cold]
    fn reset_derived_state(&mut self, index: usize) {
        self.state.trackers[index].on_book_reset();
        if let Some(ewma) = self.state.ewma[index].as_mut() {
            ewma.reset_continuity();
        }
    }

    /// New window -> wipe tracker/EwmaVol/ledger (new distribution). Only place ledger zeroed.
    #[cold]
    pub(super) fn on_market_rotation(&mut self, rotation: &MarketRotation) {
        let index = usize::from(rotation.instrument.0);
        self.state.windows[index] = Some(WindowInfo {
            open_ts_us: rotation.window_open_ts_us,
            close_ts_us: rotation.window_close_ts_us,
        });
        self.state.trackers[index].on_rotation();
        // Before the reset, which is what erases the realised leg it carries away.
        self.state
            .exec
            .on_market_rotation(&self.state.ledger, rotation.instrument);
        self.state.ledger.reset_instrument(rotation.instrument);
        if let Some(ewma) = self.state.ewma[index].as_mut() {
            ewma.reset();
        }
        // Refresh UI (re-stamps identity). Ahead of warmup.
        self.ui.emit_book(
            rotation.instrument,
            &self.state.books,
            rotation.received_ts_us,
        );
        self.ui.emit_rotation(rotation);
        if !self.is_strategy_live() {
            return;
        }
        let mut ctx = self.state.ctx(rotation.received_ts_us);
        self.strategy.on_market_rotation(&mut ctx, rotation);
    }

    pub(super) fn on_kline(&mut self, event: &KlineEvent) {
        let index = usize::from(event.instrument.0);
        self.state.trackers[index].on_kline(event);
        if !self.is_strategy_live() {
            return;
        }
        let mut ctx = self.state.ctx(event.received_ts_us);
        self.strategy.on_kline(&mut ctx, event);
    }
}
