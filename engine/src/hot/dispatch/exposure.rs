//! Exposure persistence write half: banked ledger snapshot to disk on change.

use crate::exposure::{ExposureSnapshot, InstrumentExposure, MAX_EXPOSURE_INSTRUMENTS};
use crate::msg::inbound::InboundMessage;
use crate::sink::ExposureSink;
use crate::time::TsUs;
use crate::warn;

use super::HotState;

/// Wiring both halves (can't wire apart); restored position not rewritten = silent loss of fills at boot.
pub struct ExposureWiring<'a> {
    /// Boot-restored basis (empty on cold start).
    pub restored: &'a [InstrumentExposure],
    pub sink: ExposureSink,
}

/// Only Exec + MarketRotation move durable basis; book moves position worth (not durable).
#[inline]
pub(super) fn moves_position(message: &InboundMessage) -> bool {
    matches!(
        message,
        InboundMessage::Exec(_) | InboundMessage::MarketRotation(_)
    )
}

/// Boot state as publishable snapshot (same fn as publisher); mismatched seed -> spurious first write.
pub(super) fn restored_snapshot(restored: &[InstrumentExposure]) -> ExposureSnapshot {
    snapshot_of(restored.iter().copied())
}

/// Build snapshot from rows (drop empty, apply capacity).
fn snapshot_of(rows: impl Iterator<Item = InstrumentExposure>) -> ExposureSnapshot {
    let mut snapshot = ExposureSnapshot::EMPTY;
    let mut len = 0;
    for exposure in rows.filter(|exposure| !exposure.is_empty()) {
        let Some(slot) = snapshot.instruments.get_mut(len) else {
            break;
        };
        *slot = exposure;
        len += 1;
    }
    snapshot.len = len as u8;
    snapshot
}

/// Warn if instruments exceed snapshot capacity (fixed-size POD, said once at startup).
#[cold]
pub(super) fn warn_uncovered_instruments(instrument_count: usize) {
    warn!(
        "exposure: {instrument_count} instruments exceed the {MAX_EXPOSURE_INSTRUMENTS} one snapshot carries — only the first {MAX_EXPOSURE_INSTRUMENTS} holding a position survive a restart"
    );
}

impl HotState {
    /// Publish basis snapshot (only where changed). Compare against last published, not ledger flag (can't drift).
    pub(super) fn publish_exposure(&mut self, at: TsUs) {
        let durable = self
            .ledger
            .rows()
            .map(|(instrument, row)| InstrumentExposure {
                instrument,
                position_base: row.position_base(),
                cash_quote: row.cash_quote(),
                basis_quote: row.basis_quote(),
            });
        let mut snapshot = snapshot_of(durable);
        if snapshot.active() == self.published_exposure.active() {
            return;
        }
        // Seq monotone from seed's 0 (writer's disk state). Mark informational only, saturate on overflow.
        snapshot.seq = self.published_exposure.seq + 1;
        snapshot.exposure_quote = self.ledger.rows().fold(0i64, |sum, (_, row)| {
            sum.saturating_add(row.exposure_quote())
        });
        snapshot.emitted_ts_us = at;
        self.published_exposure = snapshot;
        self.exposure.push(snapshot);
    }
}
