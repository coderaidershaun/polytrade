//! Puts REST backfill and live klines into one series by open time, with no gap or duplicate. It
//! reports each hole, and keeps received stamps from running back across a repair.

use crate::msg::inbound::{InboundMessage, KlineEvent, MessageBody};
use crate::msg::time::{DurationUs, LocalTs, VenueTs};
use crate::runtime::config::KlineInterval;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KlineOutcome {
    Forwarded,
    Duplicate,
    Gap {
        missing_from_open_ts_us: VenueTs,
        next_open_ts_us: VenueTs,
    },
}

/// Last finalized open time (anchor for dedupe + gap detection).
pub struct KlineSequencer {
    interval_us: Option<DurationUs>,
    last_closed_open_ts_us: Option<VenueTs>,
    last_emitted_received_ts_us: Option<LocalTs>,
}

impl KlineSequencer {
    pub fn new(interval: KlineInterval) -> Self {
        Self {
            interval_us: interval.fixed_duration(),
            last_closed_open_ts_us: None,
            last_emitted_received_ts_us: None,
        }
    }

    /// Next closed candle's expected open (driver backfills from here, not jumping gaps).
    /// None before first close or on 1M (no fixed span).
    pub fn next_expected_open_ts_us(&self) -> Option<VenueTs> {
        match (self.last_closed_open_ts_us, self.interval_us) {
            (Some(last), Some(interval_us)) => Some(last + interval_us),
            _ => None,
        }
    }

    /// Emit closed rows oldest-first (skip already-final, forming). Stop at hole, emit contiguous
    /// prefix only (anchor never jumps unfetched).
    pub fn on_backfill(
        &mut self,
        events: &[KlineEvent],
        received_ts_us: LocalTs,
        emit: &mut impl FnMut(InboundMessage),
    ) {
        for event in events {
            if !event.is_closed || self.is_stale_open(event.open_ts_us) {
                continue;
            }
            if self.gap_start(event.open_ts_us).is_some() {
                return;
            }
            self.emit_closed(*event, received_ts_us, emit);
        }
    }

    pub fn on_live(
        &mut self,
        event: KlineEvent,
        received_ts_us: LocalTs,
        emit: &mut impl FnMut(InboundMessage),
    ) -> KlineOutcome {
        let open_ts_us = event.open_ts_us;
        if self.is_stale_open(open_ts_us) {
            return KlineOutcome::Duplicate;
        }
        if event.is_closed {
            if let Some(expected) = self.gap_start(open_ts_us) {
                return KlineOutcome::Gap {
                    missing_from_open_ts_us: expected,
                    next_open_ts_us: open_ts_us,
                };
            }
            self.emit_closed(event, received_ts_us, emit);
            return KlineOutcome::Forwarded;
        }
        self.emit_clamped(event, received_ts_us, emit);
        KlineOutcome::Forwarded
    }

    fn is_stale_open(&self, open_ts_us: VenueTs) -> bool {
        self.last_closed_open_ts_us
            .is_some_and(|last| open_ts_us <= last)
    }

    /// Answers the expected open when `open_ts_us` lies past it, since that is where the hole
    /// starts. A monthly interval has no expected open, so it is never gap-checked.
    fn gap_start(&self, open_ts_us: VenueTs) -> Option<VenueTs> {
        self.next_expected_open_ts_us()
            .filter(|&expected| open_ts_us > expected)
    }

    fn emit_closed(
        &mut self,
        event: KlineEvent,
        received_ts_us: LocalTs,
        emit: &mut impl FnMut(InboundMessage),
    ) {
        let open_ts_us = event.open_ts_us;
        self.emit_clamped(event, received_ts_us, emit);
        self.last_closed_open_ts_us = Some(open_ts_us);
    }

    fn emit_clamped(
        &mut self,
        event: KlineEvent,
        received_ts_us: LocalTs,
        emit: &mut impl FnMut(InboundMessage),
    ) {
        let received_ts_us = self
            .last_emitted_received_ts_us
            .map_or(received_ts_us, |floor_ts_us| {
                received_ts_us.max(floor_ts_us)
            });
        self.last_emitted_received_ts_us = Some(received_ts_us);
        emit(InboundMessage::new(
            received_ts_us,
            MessageBody::Kline(event),
        ));
    }
}
