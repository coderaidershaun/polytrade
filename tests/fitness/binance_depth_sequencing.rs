//! Binance depth sequencing goldens: the depth state machines over recorded prod fixtures plus
//! the synthetic spot/perp equal-boundary asymmetry a fixture can't force.

use polysim::adapters::binance::depth::{ChainRule, DepthSequencer, DiffOutcome, SnapshotOutcome};
use polysim::adapters::binance::parse::{
    DepthDiff, DepthSnapshot, ParseContext, parse_combined_frame, parse_perp_depth_diff,
    parse_spot_depth_diff,
};
use polysim::ids::{InstrumentId, Side};
use polysim::msg::inbound::{BOOK_CHUNK_LEVELS, BookChunkKind, InboundMessage};
use polysim::time::TsUs;

use crate::engine_support::level;

pub(crate) const INSTRUMENT: InstrumentId = InstrumentId(3);
pub(crate) const RECEIVED: TsUs = TsUs::from_micros(1_784_410_630_000_000);

pub(crate) fn ctx() -> ParseContext {
    ParseContext {
        instrument: INSTRUMENT,
        received_ts_us: RECEIVED,
    }
}

fn count_delta_chunks(messages: &[InboundMessage], update_id: u64) -> usize {
    messages
        .iter()
        .filter(|message| match message {
            InboundMessage::Book(chunk) => {
                chunk.kind == BookChunkKind::Delta && chunk.update_id == update_id
            }
            _ => false,
        })
        .count()
}

fn assert_event_chunks(
    emitted: &[InboundMessage],
    kind: BookChunkKind,
    update_id: u64,
    bid_levels: usize,
    ask_levels: usize,
) {
    let mut buy = 0usize;
    let mut sell = 0usize;
    let mut last_flags = 0usize;
    for message in emitted {
        let InboundMessage::Book(chunk) = message else {
            panic!("expected a book chunk, got {message:?}");
        };
        assert_eq!(chunk.kind, kind);
        assert_eq!(chunk.update_id, update_id);
        assert!(chunk.len as usize <= BOOK_CHUNK_LEVELS);
        match chunk.side {
            Side::Buy => buy += chunk.len as usize,
            Side::Sell => sell += chunk.len as usize,
        }
        if chunk.is_last_chunk {
            last_flags += 1;
        }
    }
    assert_eq!(buy, bid_levels, "bid levels conserved");
    assert_eq!(sell, ask_levels, "ask levels conserved");
    if bid_levels + ask_levels == 0 {
        assert!(emitted.is_empty(), "an empty event emits nothing");
    } else {
        assert_eq!(last_flags, 1, "exactly one is_last_chunk per event");
        let last = emitted.last().expect("non-empty event");
        let InboundMessage::Book(chunk) = last else { unreachable!() };
        assert!(chunk.is_last_chunk, "is_last_chunk sits on the final chunk");
    }
}

fn parse_depth_diffs(fixture: &str, rule: ChainRule) -> Vec<DepthDiff> {
    fixture
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let frame = parse_combined_frame(line).expect("combined envelope");
            match rule {
                ChainRule::Spot => parse_spot_depth_diff(&frame.data, ctx()),
                ChainRule::Perp => parse_perp_depth_diff(&frame.data, ctx()),
            }
            .expect("depth diff")
        })
        .collect()
}

fn synthetic_snapshot(last_update_id: u64) -> DepthSnapshot {
    DepthSnapshot {
        instrument: INSTRUMENT,
        last_update_id,
        bids: vec![level(64_600_000_000, 100_000_000)],
        asks: vec![level(64_700_000_000, 100_000_000)],
        received_ts_us: RECEIVED,
    }
}

fn synthetic_diff(
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
        exchange_ts_us: RECEIVED,
        received_ts_us: RECEIVED,
    }
}

mod depth_golden {
    use super::*;
    use polysim::msg::inbound::VenueMeta;

    const PERP_DEPTH: &str = include_str!("../../fixtures/binance/perp_depth.jsonl");
    const SPOT_DEPTH: &str = include_str!("../../fixtures/binance/spot_depth.jsonl");

    fn assert_chain_holds(fixture: &str, rule: ChainRule) {
        let diffs = parse_depth_diffs(fixture, rule);
        assert!(diffs.len() > 100, "fixture should carry the full window");

        let mut sequencer = DepthSequencer::new(rule, INSTRUMENT, 512);
        assert_eq!(
            sequencer.on_diff(diffs[0].clone(), &mut |_, _| {}),
            DiffOutcome::Buffered
        );
        // A snapshot whose final id equals the first diff's first id spans it (both venue rules).
        let outcome =
            sequencer.apply_snapshot(synthetic_snapshot(diffs[0].first_update_id), &mut |_, _| {});
        assert_eq!(outcome, SnapshotOutcome::Applied);
        assert!(sequencer.is_live());

        for diff in &diffs[1..] {
            let mut emitted = Vec::new();
            let mut stamps = Vec::new();
            let outcome = sequencer.on_diff(diff.clone(), &mut |message, venue_meta| {
                emitted.push(message);
                stamps.push(venue_meta);
            });
            assert_eq!(
                outcome,
                DiffOutcome::Applied,
                "chain broke at update id {}",
                diff.final_update_id
            );
            for venue_meta in stamps {
                assert_eq!(
                    venue_meta,
                    VenueMeta::DepthDelta {
                        exchange_ts_us: diff.exchange_ts_us
                    },
                    "delta stamped with the wrong exchange time at update id {}",
                    diff.final_update_id
                );
            }
            assert_event_chunks(
                &emitted,
                BookChunkKind::Delta,
                diff.final_update_id,
                diff.bids.len(),
                diff.asks.len(),
            );
        }
    }

    #[test]
    fn perp_chain_holds_across_recorded_stream() {
        assert_chain_holds(PERP_DEPTH, ChainRule::Perp);
    }

    #[test]
    fn spot_chain_holds_across_recorded_stream() {
        assert_chain_holds(SPOT_DEPTH, ChainRule::Spot);
    }
}

mod depth_synthetic {
    use super::*;

    /// The asymmetry trap: an event whose `u` exactly equals `lastUpdateId` is DROPPED by spot
    /// (`u <= lastUpdateId`) but KEPT by perp (`u < lastUpdateId`). Same buffer, opposite fate.
    #[test]
    fn equal_boundary_is_dropped_on_spot_kept_on_perp() {
        let boundary = 500;

        let mut spot = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 8);
        spot.on_diff(synthetic_diff(490, boundary, None), &mut |_, _| {});
        let mut spot_out = Vec::new();
        assert_eq!(
            spot.apply_snapshot(synthetic_snapshot(boundary), &mut |message, _| spot_out
                .push(message)),
            SnapshotOutcome::Applied
        );
        assert_eq!(
            count_delta_chunks(&spot_out, boundary),
            0,
            "spot drops the equal-boundary event"
        );

        let mut perp = DepthSequencer::new(ChainRule::Perp, INSTRUMENT, 8);
        perp.on_diff(synthetic_diff(490, boundary, Some(400)), &mut |_, _| {});
        let mut perp_out = Vec::new();
        assert_eq!(
            perp.apply_snapshot(synthetic_snapshot(boundary), &mut |message, _| perp_out
                .push(message)),
            SnapshotOutcome::Applied
        );
        assert_eq!(
            count_delta_chunks(&perp_out, boundary),
            1,
            "perp keeps and applies the equal-boundary event"
        );
    }

    #[test]
    fn an_exhausted_spot_sequence_id_resets_instead_of_wrapping() {
        let mut sequencer = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 8);
        sequencer.on_diff(synthetic_diff(u64::MAX, u64::MAX, None), &mut |_, _| {});
        assert_eq!(
            sequencer.apply_snapshot(synthetic_snapshot(u64::MAX - 1), &mut |_, _| {}),
            SnapshotOutcome::Applied,
            "the final representable diff can bridge a snapshot"
        );

        let mut emitted = Vec::new();
        assert_eq!(
            sequencer.on_diff(
                synthetic_diff(u64::MAX, u64::MAX, None),
                &mut |message, _| emitted.push(message)
            ),
            DiffOutcome::Resync,
            "there is no sequence id after u64::MAX"
        );
        assert!(
            matches!(emitted.as_slice(), [InboundMessage::BookReset(_)]),
            "exhaustion is an explicit loss of sequencing, not a wrapped duplicate"
        );
    }

    #[test]
    fn a_snapshot_past_the_exhausted_id_is_stale() {
        struct Case {
            name: &'static str,
            prime: fn(&mut DepthSequencer),
            snapshot_at: u64,
        }
        let cases = [
            Case {
                name: "no_prior_diff_at_the_ceiling",
                prime: |_| {},
                snapshot_at: u64::MAX,
            },
            Case {
                name: "buffered_chain_cannot_wrap_its_successor_to_zero",
                prime: |seq| {
                    seq.on_diff(synthetic_diff(u64::MAX - 1, u64::MAX, None), &mut |_, _| {});
                    seq.on_diff(synthetic_diff(u64::MAX, u64::MAX, None), &mut |_, _| {});
                },
                snapshot_at: u64::MAX - 2,
            },
        ];
        for case in cases {
            let mut sequencer = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 8);
            (case.prime)(&mut sequencer);
            assert_eq!(
                sequencer.apply_snapshot(synthetic_snapshot(case.snapshot_at), &mut |_, _| {}),
                SnapshotOutcome::Stale,
                "{}: a snapshot with no representable successor cannot establish a live chain",
                case.name
            );
            assert!(!sequencer.is_live(), "{}", case.name);
        }
    }

    /// Spot re-sends events the book already holds, and the sequencer throws them away. The outcome
    /// it reports for that is the one a caller counts applied diffs by, so it must not claim an
    /// application: the natural reading "Applied implies levels reached the book" has to stay true.
    #[test]
    fn a_spot_diff_the_book_already_holds_is_not_reported_as_applied() {
        let mut sequencer = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 8);
        sequencer.on_diff(synthetic_diff(490, 500, None), &mut |_, _| {});
        assert_eq!(
            sequencer.apply_snapshot(synthetic_snapshot(490), &mut |_, _| {}),
            SnapshotOutcome::Applied
        );
        assert!(sequencer.is_live());

        let mut emitted = Vec::new();
        let outcome = sequencer.on_diff(synthetic_diff(495, 500, None), &mut |message, _| {
            emitted.push(message);
        });
        assert!(
            emitted.is_empty(),
            "a diff the book has already absorbed changes no level"
        );
        assert_eq!(outcome, DiffOutcome::AlreadyApplied);
    }
}

/// The simulated venue reconstructs continuity from the metadata riding beside each message and
/// asserts its presence on intake, so a message that reaches the tap stripped of its evidence kills
/// a sim run. The pairing is pinned here, where a fixture can prove it, rather than at that panic.
mod venue_metadata {
    use super::*;
    use polysim::msg::inbound::VenueMeta;

    const RESET_EXCHANGE_TS: TsUs = TsUs::from_micros(1_784_410_631_500_000);

    fn live_spot_sequencer() -> DepthSequencer {
        let mut sequencer = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 8);
        sequencer.on_diff(synthetic_diff(490, 500, None), &mut |_, _| {});
        assert_eq!(
            sequencer.apply_snapshot(synthetic_snapshot(490), &mut |_, _| {}),
            SnapshotOutcome::Applied
        );
        assert!(sequencer.is_live());
        sequencer
    }

    /// The reset's own stamp is the BROKEN diff's exchange time, not the receipt the message
    /// carries — the receipt is clamped to the emitted floor and the venue clock is not.
    #[test]
    fn venue_meta_matches_the_event_kind_it_stamps() {
        let mut sequencer = DepthSequencer::new(ChainRule::Spot, INSTRUMENT, 8);
        let mut stamped = Vec::new();
        sequencer.apply_snapshot(synthetic_snapshot(490), &mut |message, venue_meta| {
            stamped.push((message, venue_meta));
        });
        assert!(!stamped.is_empty(), "snapshot: a snapshot emits its book");
        for (message, venue_meta) in &stamped {
            assert!(
                matches!(message, InboundMessage::Book(chunk) if chunk.kind == BookChunkKind::Snapshot),
                "snapshot"
            );
            assert_eq!(
                *venue_meta,
                VenueMeta::None,
                "snapshot: a snapshot replaces the book, so it chains onto nothing"
            );
        }

        let mut sequencer = live_spot_sequencer();
        let diff = synthetic_diff(501, 510, None);
        let mut stamped = Vec::new();
        assert_eq!(
            sequencer.on_diff(diff.clone(), &mut |message, venue_meta| {
                stamped.push((message, venue_meta));
            }),
            DiffOutcome::Applied,
            "delta"
        );
        assert!(
            !stamped.is_empty(),
            "delta: a chaining diff emits its levels"
        );
        for (message, venue_meta) in &stamped {
            assert!(
                matches!(message, InboundMessage::Book(chunk) if chunk.kind == BookChunkKind::Delta),
                "delta"
            );
            assert_eq!(
                *venue_meta,
                VenueMeta::DepthDelta {
                    exchange_ts_us: diff.exchange_ts_us
                },
                "delta"
            );
        }

        let mut sequencer = live_spot_sequencer();
        let mut broken = synthetic_diff(600, 610, None);
        broken.exchange_ts_us = RESET_EXCHANGE_TS;
        let mut stamped = Vec::new();
        assert_eq!(
            sequencer.on_diff(broken, &mut |message, venue_meta| {
                stamped.push((message, venue_meta));
            }),
            DiffOutcome::Resync,
            "reset"
        );
        assert!(
            matches!(
                stamped.as_slice(),
                [(
                    InboundMessage::BookReset(_),
                    VenueMeta::DepthReset {
                        exchange_ts_us: RESET_EXCHANGE_TS
                    }
                )]
            ),
            "reset: got {stamped:?}"
        );
    }
}
