//! What belongs to one venue of the engine: its account, rate meter, settings, readiness, reject
//! streak, run nonce, the silence of its order stream, and its command bank and lane.

use std::ops::Range;

use crate::hot::digest::digest_fields;
use crate::hot::sink::{ExecSink, PendingCommands};
use crate::msg::exec::ExecEvent;
use crate::msg::ids::{InstrumentId, Side, VenueId};
use crate::runtime::registry::InstrumentRow;

use super::account::AccountTable;
use super::budget::BudgetMeter;
use super::capacity::ExecCapacity;
use super::engine::{ExecEngine, ExecSettings};
use super::gates::{Readiness, ReadinessGap};
use super::order::side_base;
use super::silence::StreamSilence;

/// How the engine reaches one venue. A venue with no sink has execution off, so it never places.
pub struct ExecVenueSetup {
    pub run_nonce: u32,
    pub settings: ExecSettings,
    pub sink: Option<ExecSink>,
}

impl ExecVenueSetup {
    /// Settings that refuse every order, and a nonce nothing reads because minting a client id
    /// needs a sink to send it through.
    pub fn disabled() -> Self {
        Self {
            run_nonce: 0,
            settings: ExecSettings::disabled(),
            sink: None,
        }
    }
}

pub(super) struct VenueExec {
    pub(super) account: AccountTable,
    pub(super) pending: PendingCommands,
    pub(super) sink: Option<ExecSink>,
    pub(super) readiness: Readiness,
    pub(super) reject_streak: u32,
    pub(super) budget: BudgetMeter,
    pub(super) settings: ExecSettings,
    pub(super) silence: StreamSilence,
    /// The edge of this venue echoes each number, and only the orders of this venue are compared
    /// against it, so each venue numbers its own reconcile passes.
    pub(super) recon_seq: u64,
    pub(super) run_nonce: u32,
    /// The rows of this venue, which sit together in source order, and the order slots they own.
    pub(super) instruments: Range<usize>,
    pub(super) slots: Range<usize>,
}

digest_fields! {
    VenueExec {
        account, pending, sink, readiness, reject_streak, budget, settings, silence, recon_seq,
        run_nonce,
    }
    not_state {
        // Where the rows of the venue sit: the rows give it, and the digest holds each row's venue.
        instruments, slots,
    }
}

impl VenueExec {
    fn new(setup: ExecVenueSetup, instruments: Range<usize>, capacity: &ExecCapacity) -> Self {
        let first = InstrumentId(instruments.start as u16);
        let end = InstrumentId(instruments.end as u16);
        Self {
            account: AccountTable::new(capacity),
            pending: PendingCommands::for_sink(setup.sink.as_ref(), capacity.command_bank()),
            sink: setup.sink,
            readiness: Readiness::default(),
            reject_streak: 0,
            budget: BudgetMeter::new(setup.settings.order_budget),
            settings: setup.settings,
            silence: StreamSilence::default(),
            recon_seq: 0,
            run_nonce: setup.run_nonce,
            slots: side_base(first, Side::Buy)..side_base(end, Side::Buy),
            instruments,
        }
    }

    pub(super) fn instrument_ids(&self) -> impl Iterator<Item = InstrumentId> + use<> {
        self.instruments
            .clone()
            .map(|index| InstrumentId(index as u16))
    }

    /// The first fact an unready venue still waits for, joined across its two owners: the stream
    /// and open-order facts of the readiness latch, and the balance snapshot of the account.
    /// Ordered furthest-upstream first, so a diagnostic names the cause rather than a consequence.
    #[inline]
    pub(super) fn readiness_gap(&self) -> Option<ReadinessGap> {
        if !self.readiness.is_stream_ready() {
            return Some(ReadinessGap::Stream);
        }
        if !self.account.has_snapshot() {
            return Some(ReadinessGap::Balances);
        }
        (!self.readiness.has_open_orders_snapshot()).then_some(ReadinessGap::OpenOrders)
    }
}

impl ExecEngine {
    #[inline]
    pub(crate) fn commands_banked(&self) -> u64 {
        self.venues
            .iter()
            .map(|venue| venue.pending.banked_total())
            .sum()
    }

    #[inline]
    pub(super) fn venue_of(&self, instrument: InstrumentId) -> usize {
        usize::from(self.instruments[usize::from(instrument.0)].venue.0)
    }

    #[inline]
    pub(super) fn venue(&self, instrument: InstrumentId) -> &VenueExec {
        &self.venues[self.venue_of(instrument)]
    }

    #[inline]
    pub(super) fn venue_mut(&mut self, instrument: InstrumentId) -> &mut VenueExec {
        let venue = self.venue_of(instrument);
        &mut self.venues[venue]
    }

    #[inline]
    pub(super) fn settings_of(&self, instrument: InstrumentId) -> &ExecSettings {
        &self.venue(instrument).settings
    }

    /// The fold reads the venue from the message. An event that also names an instrument names that
    /// instrument's venue: one fact in two places, checked.
    pub(super) fn venue_of_event(&self, event: &ExecEvent) -> usize {
        if let Some(instrument) = event.instrument {
            let row_venue = self.instruments[usize::from(instrument.0)].venue;
            assert_eq!(
                event.venue, row_venue,
                "an event of venue {} names instrument {} of venue {}",
                event.venue.0, instrument.0, row_venue.0
            );
        }
        let venue = usize::from(event.venue.0);
        assert!(
            venue < self.venues.len(),
            "an event names venue {venue}, and this engine has {} venues",
            self.venues.len()
        );
        venue
    }

    #[inline]
    pub(super) fn has_sink(&self) -> bool {
        self.venues.iter().any(|venue| venue.sink.is_some())
    }
}

/// One state for each venue that a row names or a setup wires, in venue order. The rows of one venue
/// sit together, because the registry builds them source by source.
///
/// # Panics
/// When the rows of a venue do not sit together, or more venues are wired than the 256 that the id
/// holds.
pub(super) fn venues_for(
    rows: &[InstrumentRow],
    setups: Vec<ExecVenueSetup>,
    capacity: &ExecCapacity,
) -> Vec<VenueExec> {
    let named = rows
        .iter()
        .map(|row| usize::from(row.venue.0) + 1)
        .max()
        .unwrap_or(0);
    let count = named.max(setups.len()).max(1);
    assert!(
        count <= usize::from(u8::MAX) + 1,
        "{count} venues are wired, and a venue id holds 256"
    );
    let mut setups = setups.into_iter();
    (0..count)
        .map(|venue| {
            let instruments = rows_of(rows, VenueId(venue as u8));
            let setup = setups.next().unwrap_or_else(ExecVenueSetup::disabled);
            VenueExec::new(setup, instruments, capacity)
        })
        .collect()
}

fn rows_of(rows: &[InstrumentRow], venue: VenueId) -> Range<usize> {
    let Some(start) = rows.iter().position(|row| row.venue == venue) else {
        return rows.len()..rows.len();
    };
    let end = rows[start..]
        .iter()
        .position(|row| row.venue != venue)
        .map_or(rows.len(), |offset| start + offset);
    assert!(
        rows[end..].iter().all(|row| row.venue != venue),
        "the rows of venue {} do not sit together: each source gives its rows in one run",
        venue.0
    );
    start..end
}
