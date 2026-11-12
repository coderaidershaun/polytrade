//! Outcome leg: Up/Down token. Validates book vs shadow, detects collapse, emits normalized.

use crate::adapters::polymarket::book::{BookNormaliser, ChunkStamps};
use crate::adapters::polymarket::parse::{PolyBook, PolyDelta};
use crate::adapters::polymarket::rotation::TokenId;
use crate::adapters::polymarket::shadow::{BookFrameOutcome, BookMismatch, ShadowValidator};
use crate::adapters::polymarket::teardown::{CollapseDetector, LevelUpdate};
use crate::ids::{InstrumentId, Qty, Side};
use crate::msg::inbound::{BookReset, InboundMessage, Level};
use crate::time::TsUs;

pub(super) struct Leg {
    instrument: InstrumentId,
    token: Option<TokenId>,
    shadow: ShadowValidator,
    collapse: CollapseDetector,
    normaliser: BookNormaliser,
}

impl Leg {
    pub(super) fn new(instrument: InstrumentId) -> Self {
        Self {
            instrument,
            token: None,
            shadow: ShadowValidator::default(),
            collapse: CollapseDetector::default(),
            normaliser: BookNormaliser::new(instrument),
        }
    }

    pub(super) fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    pub(super) fn token_matches(&self, asset_id: &str) -> bool {
        self.token
            .as_ref()
            .is_some_and(|token| token.as_str() == asset_id)
    }

    pub(super) fn live_token(&self) -> Option<&TokenId> {
        self.token.as_ref()
    }

    pub(super) fn is_live(&self) -> bool {
        self.token.is_some()
    }

    pub(super) fn assign(&mut self, token: TokenId, now: TsUs) {
        self.token = Some(token);
        self.shadow.resubscribe();
        self.collapse.reset();
        self.normaliser.note_emit_floor(now);
    }

    pub(super) fn resubscribe(&mut self) {
        self.shadow.resubscribe();
        self.collapse.reset();
    }

    pub(super) fn clear(&mut self) {
        self.token = None;
    }

    pub(super) fn on_venue_book(
        &mut self,
        book: &PolyBook,
        now: TsUs,
        emit: &mut impl FnMut(InboundMessage),
    ) -> Option<BookMismatch> {
        let bids_ascending: Vec<Level> = book.bids.iter().rev().copied().collect();
        match self.shadow.on_venue_book(&bids_ascending, &book.asks) {
            BookFrameOutcome::Validated => None,
            BookFrameOutcome::ForwardSnapshot => {
                self.normaliser.emit_snapshot(book, emit);
                self.reseed_collapse(book);
                None
            }
            BookFrameOutcome::Diverged(mismatch) => {
                emit(InboundMessage::BookReset(BookReset {
                    instrument: self.instrument,
                    received_ts_us: now,
                    queued_ts_us: now,
                }));
                self.normaliser.emit_snapshot(book, emit);
                self.reseed_collapse(book);
                Some(mismatch)
            }
        }
    }

    /// Forward leg's price-change slice as Delta chunks, mirror into shadow, feed collapse detector.
    pub(super) fn on_deltas(
        &mut self,
        deltas: &[&PolyDelta],
        received_ts_us: TsUs,
        exchange_ts_us: TsUs,
        emit: &mut impl FnMut(InboundMessage),
    ) -> bool {
        self.normaliser.emit_price_change(
            deltas,
            ChunkStamps {
                exchange_ts_us,
                received_ts_us,
            },
            emit,
        );
        for delta in deltas {
            self.shadow
                .on_forwarded_delta(delta.side, delta.level.price, delta.level.qty);
            self.collapse.observe(LevelUpdate {
                side: delta.side,
                price: delta.level.price,
                qty: delta.level.qty,
                exchange_ts_us,
            });
        }
        self.collapse.has_collapsed()
    }

    /// Rebuild collapse detector from full snapshot so two-sided view survives re-baseline.
    fn reseed_collapse(&mut self, book: &PolyBook) {
        self.collapse.reset();
        for level in &book.bids {
            self.observe_level(Side::Buy, level, book.exchange_ts_us);
        }
        for level in &book.asks {
            self.observe_level(Side::Sell, level, book.exchange_ts_us);
        }
    }

    fn observe_level(&mut self, side: Side, level: &Level, exchange_ts_us: TsUs) {
        if level.qty == Qty(0) {
            return;
        }
        self.collapse.observe(LevelUpdate {
            side,
            price: level.price,
            qty: level.qty,
            exchange_ts_us,
        });
    }
}
