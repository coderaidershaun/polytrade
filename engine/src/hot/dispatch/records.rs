//! The rows that the fold writes from its own state for a reader after the run: the ledger at the
//! boot, at each interval while a position is open, and before a rotation clears a row; the touch
//! and the mid of each book at each spin; and each traded side that holds no quote after the
//! execution pass, with its reason. Each takes the stamp of its message, never a clock.

use std::num::NonZeroU64;

use crate::hot::book::Book;
use crate::msg::ids::{InstrumentId, Price};
use crate::msg::persist::{AuditRecord, LedgerKind, MidRow, NoQuoteRow, ResearchRecord};
use crate::msg::time::LocalTs;

use super::seal::SealBorders;
use super::{HotEngine, HotState};

/// The mid of the touch as an exact mantissa, half the spread above the bid, so it never overflows.
/// `None` when the book lacks a side.
pub(super) fn exact_mid(book: &Book) -> Option<Price> {
    let (bid, ask) = book.best_bid().zip(book.best_ask())?;
    let (bid, ask) = (bid.price.mantissa(), ask.price.mantissa());
    Some(Price::from_mantissa(bid + (ask - bid) / 2))
}

impl HotEngine {
    /// Writes a ledger row of each open position at the first spin past each multiple of
    /// `interval_spins`, so a reader sees the mark move between fills. With no interval the fold
    /// writes ledger rows only at the boot, at fills and at rotations.
    pub fn writing_ledger_every(mut self, interval_spins: NonZeroU64) -> HotEngine {
        self.state.ledger_borders = Some(SealBorders::every(interval_spins));
        self
    }
}

impl HotState {
    pub(super) fn bank_ledger_row(
        &mut self,
        kind: LedgerKind,
        instrument: InstrumentId,
        at: LocalTs,
    ) {
        let entry = self.ledger.row(instrument).entry(kind, instrument, at);
        self.actions.push_audit(AuditRecord::Ledger(entry));
    }

    /// The start state of the fold is no message, so the first message writes it: one row of each
    /// instrument whose restored row holds money, in instrument order, before any other row.
    #[inline]
    pub(super) fn write_boot_rows(&mut self, at: LocalTs) {
        if self.is_booted {
            return;
        }
        self.is_booted = true;
        for index in 0..self.books.len() {
            let instrument = InstrumentId(index as u16);
            if self.ledger.row(instrument).holds_money() {
                self.bank_ledger_row(LedgerKind::Boot, instrument, at);
            }
        }
    }

    /// The rows of each spin that come before the strategy: the ledger of each open position at an
    /// interval border, and the touch of each book.
    pub(super) fn write_spin_rows(&mut self, spin_seq: u64, at: LocalTs) {
        if self.sink.is_none() {
            return;
        }
        let is_border = self
            .ledger_borders
            .as_mut()
            .is_some_and(|borders| borders.is_crossed_by(spin_seq));
        for index in 0..self.books.len() {
            let instrument = InstrumentId(index as u16);
            if is_border && self.ledger.row(instrument).position_base().mantissa() != 0 {
                self.bank_ledger_row(LedgerKind::Interval, instrument, at);
            }
            let book = &self.books[index];
            let row = MidRow {
                instrument,
                spin_seq,
                bid: book.best_bid().map(|level| level.price),
                ask: book.best_ask().map(|level| level.price),
                mid: exact_mid(book),
                event_ts_us: at,
            };
            self.actions.push_research(ResearchRecord::Mid(row));
        }
    }

    /// After the execution pass: each side of each instrument of a venue that trades and that holds
    /// no working order of this run, with the reason the pass recorded.
    pub(super) fn write_no_quote_rows(&mut self, spin_seq: u64, at: LocalTs) {
        if self.sink.is_none() {
            return;
        }
        let HotState { exec, actions, .. } = self;
        exec.for_each_unquoted_side(|instrument, side, reason| {
            actions.push_research(ResearchRecord::NoQuote(NoQuoteRow {
                instrument,
                spin_seq,
                side,
                reason: reason.into(),
                event_ts_us: at,
            }));
        });
    }
}
