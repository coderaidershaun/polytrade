//! Exposure persistence write half: banked ledger snapshot to disk on change.

use crate::hot::sink::ExposureSink;
use crate::msg::inbound::{InboundMessage, MessageKind};
use crate::msg::time::LocalTs;
use crate::runtime::exposure::{ExposureSnapshot, InstrumentExposure};

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
        message.kind(),
        MessageKind::Exec | MessageKind::MarketRotation
    )
}

/// The restored position as a snapshot with the rows that the publisher writes. A seed with other
/// rows would make the first publish write a change that never happened. It has room for each
/// instrument, so a publish fills it in place.
pub(super) fn restored_snapshot(
    instruments: usize,
    restored: &[InstrumentExposure],
) -> ExposureSnapshot {
    let mut snapshot = ExposureSnapshot::for_instruments(instruments);
    snapshot.set_rows(restored.iter().copied());
    snapshot
}

impl HotState {
    /// Publish basis snapshot (only where changed). Compare against last published, not ledger flag (can't drift).
    pub(super) fn publish_exposure(&mut self, at: LocalTs) {
        let ledger = &self.ledger;
        let durable = || {
            ledger.rows().map(|(instrument, row)| InstrumentExposure {
                instrument,
                position_base: row.position_base(),
                cash_quote: row.cash_quote().mantissa(),
                basis_quote: row.basis_quote().mantissa(),
            })
        };
        let published = &mut self.published_exposure;
        if durable()
            .filter(|exposure| !exposure.is_empty())
            .eq(published.active().iter().copied())
        {
            return;
        }
        // Seq monotone from seed's 0 (writer's disk state). Mark informational only, saturate on overflow.
        published.set_rows(durable());
        published.seq += 1;
        published.exposure_quote = ledger.rows().fold(0i64, |sum, (_, row)| {
            sum.saturating_add(row.exposure_quote().mantissa())
        });
        published.emitted_ts_us = at;
        self.exposure.push(published);
    }
}
