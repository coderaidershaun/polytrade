//! The venue metadata that the Binance depth sequencer stamps beside each message, and the made-up
//! diffs and snapshots that the depth tests share.

use engine::msg::ids::InstrumentId;
use engine::msg::inbound::{BookChunkKind, BookReset, InboundMessage, MessageBody};
use engine::msg::time::{LocalTs, VenueTs};
use engine::venues::binance::depth::{ChainRule, DepthSequencer, DiffOutcome, SnapshotOutcome};
use engine::venues::binance::parse::{DepthDiff, DepthSnapshot, ParseContext};

use crate::engine_support::level;

pub(crate) const INSTRUMENT: InstrumentId = InstrumentId(3);
pub(crate) const RECEIVED: LocalTs = LocalTs::from_micros(1_784_410_630_000_000);

pub(crate) fn ctx() -> ParseContext {
    ParseContext {
        instrument: INSTRUMENT,
        received_ts_us: RECEIVED,
    }
}

pub(crate) fn synthetic_snapshot(last_update_id: u64) -> DepthSnapshot {
    DepthSnapshot {
        instrument: INSTRUMENT,
        last_update_id,
        bids: vec![level(64_600_000_000, 100_000_000)],
        asks: vec![level(64_700_000_000, 100_000_000)],
        received_ts_us: RECEIVED,
    }
}

pub(crate) fn synthetic_diff(
    first_update_id: u64,
    final_update_id: u64,
    prev_final_update_id: Option<u64>,
) -> DepthDiff {
    DepthDiff {
        instrument: INSTRUMENT,
        first_update_id,
        final_update_id,
        prev_final_update_id,
        bids: vec![level(64_600_000_000, 50_000_000)],
        asks: vec![],
        exchange_ts_us: RECEIVED.as_venue_calendar(),
        received_ts_us: RECEIVED,
    }
}

/// The simulated venue reconstructs continuity from the venue stamps that each message carries, so
/// a message that leaves the sequencer without its stamp would place a delta or a reset at the wrong
/// venue time. The stamps are pinned here, where a fixture can prove them.
mod venue_stamps {
    use super::*;

    const RESET_EXCHANGE_TS: VenueTs = VenueTs::from_micros(1_784_410_631_500_000);

    fn live_spot_sequencer() -> DepthSequencer {
        let mut sequencer = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 8);
        sequencer.on_diff(synthetic_diff(490, 500, None), &mut |_| {});
        assert_eq!(
            sequencer.apply_snapshot(synthetic_snapshot(490), &mut |_| {}),
            SnapshotOutcome::Applied
        );
        assert!(sequencer.is_live());
        sequencer
    }

    /// The reset's own stamp is the BROKEN diff's exchange time, not the receipt the message
    /// carries — the receipt is clamped to the emitted floor and the venue clock is not.
    #[test]
    fn each_message_carries_the_venue_stamp_of_its_kind() {
        let mut sequencer = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 8);
        let mut stamped = Vec::new();
        sequencer.apply_snapshot(synthetic_snapshot(490), &mut |message| {
            stamped.push(message)
        });
        assert!(!stamped.is_empty(), "snapshot: a snapshot emits its book");
        for message in &stamped {
            assert!(
                matches!(&message.body, MessageBody::Book(chunk)
                    if chunk.kind == BookChunkKind::Snapshot && chunk.exchange_ts_us.is_none()),
                "snapshot: a REST snapshot carries no venue stamp, got {message:?}"
            );
        }

        let mut sequencer = live_spot_sequencer();
        let diff = synthetic_diff(501, 510, None);
        let mut stamped = Vec::new();
        assert_eq!(
            sequencer.on_diff(diff.clone(), &mut |message| stamped.push(message)),
            DiffOutcome::Applied,
            "delta"
        );
        assert!(
            !stamped.is_empty(),
            "delta: a chaining diff emits its levels"
        );
        for message in &stamped {
            assert!(
                matches!(&message.body, MessageBody::Book(chunk)
                    if chunk.kind == BookChunkKind::Delta
                        && chunk.exchange_ts_us == Some(diff.exchange_ts_us)),
                "delta: each chunk carries the stamp of its diff, got {message:?}"
            );
        }

        let mut sequencer = live_spot_sequencer();
        let mut broken = synthetic_diff(600, 610, None);
        broken.exchange_ts_us = RESET_EXCHANGE_TS;
        let mut stamped = Vec::new();
        assert_eq!(
            sequencer.on_diff(broken, &mut |message| stamped.push(message)),
            DiffOutcome::Resync,
            "reset"
        );
        assert!(
            matches!(
                stamped.as_slice(),
                [InboundMessage {
                    body: MessageBody::BookReset(BookReset {
                        exchange_ts_us: RESET_EXCHANGE_TS,
                        ..
                    }),
                    ..
                }]
            ),
            "reset: got {stamped:?}"
        );
    }
}
