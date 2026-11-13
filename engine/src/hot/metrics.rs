//! What the hot loop records about each message it folds: a counter read on each side of the fold
//! and the stamps that the message carries, pushed raw for the metrics thread to turn into numbers.

use crate::hot::sink::SampleSink;
use crate::msg::exec::ExecKind;
use crate::msg::ids::QueueId;
use crate::msg::inbound::{BookChunkKind, InboundMessage, MessageBody};
use crate::msg::latency::{Category, CounterSample, StageSample};
use crate::msg::time::{LocalTs, Stamper, Tick, VenueTs};

/// The counter that the hot loop reads on each side of a fold, and the rings that its samples go
/// to. A loop built without them reads no counter and pushes nothing.
pub struct DriverSamples {
    pub stamper: Stamper,
    pub stages: SampleSink<StageSample>,
    pub counters: SampleSink<CounterSample>,
}

/// One message as the hot loop folded it.
pub(crate) struct Folded<'a> {
    pub(crate) message: &'a InboundMessage,
    pub(crate) queue: QueueId,
    pub(crate) pop_tick: Tick,
    pub(crate) done_tick: Tick,
    pub(crate) pop_seq: u32,
}

impl DriverSamples {
    #[inline]
    #[expect(
        clippy::disallowed_methods,
        reason = "the driver loop stamps each message twice for its latency, and the ticks never reach the fold"
    )]
    pub(crate) fn stamp(&self) -> Tick {
        self.stamper.stamp()
    }

    /// A message of a kind that no stage times pushes nothing.
    #[inline]
    pub(crate) fn record_stage(&mut self, folded: &Folded<'_>) {
        if let Some(sample) = stage_sample(folded) {
            self.stages.push(sample);
        }
    }
}

/// The stamps of one body that a stage can subtract. The received and queued stamps are in the
/// header of every message.
struct BodyStamps {
    category: Category,
    exchange_sent_ts_us: VenueTs,
    exchange_ts_us: VenueTs,
    request_sent_ts_us: LocalTs,
}

#[inline]
fn stage_sample(folded: &Folded<'_>) -> Option<StageSample> {
    let stamps = body_stamps(&folded.message.body)?;
    let header = folded.message.header;
    Some(StageSample {
        pop_tick: folded.pop_tick,
        done_tick: folded.done_tick,
        received_ts_us: header.received_ts_us,
        queued_ts_us: header.queued_ts_us,
        exchange_sent_ts_us: stamps.exchange_sent_ts_us,
        exchange_ts_us: stamps.exchange_ts_us,
        request_sent_ts_us: stamps.request_sent_ts_us,
        category: stamps.category,
        queue: folded.queue,
        pop_seq: folded.pop_seq,
    })
}

/// `None` for a kind of message that no stage times.
fn body_stamps(body: &MessageBody) -> Option<BodyStamps> {
    const ABSENT: LocalTs = StageSample::ABSENT;
    const ABSENT_VENUE: VenueTs = StageSample::ABSENT_VENUE;
    let stamps = match body {
        MessageBody::Trade(event) => BodyStamps {
            category: Category::Trade,
            exchange_sent_ts_us: event.exchange_sent_ts_us.unwrap_or(ABSENT_VENUE),
            exchange_ts_us: event.exchange_ts_us,
            request_sent_ts_us: ABSENT,
        },
        MessageBody::Book(chunk) => BodyStamps {
            category: match chunk.kind {
                BookChunkKind::Snapshot => Category::BookSnapshot,
                BookChunkKind::Delta => Category::BookDelta,
            },
            exchange_sent_ts_us: ABSENT_VENUE,
            exchange_ts_us: chunk.exchange_ts_us.unwrap_or(ABSENT_VENUE),
            request_sent_ts_us: ABSENT,
        },
        // A candle's `exchange_ts_us` is the CANDLE's time, not the frame's: on REST backfill it is
        // the close of a row hours old, and measuring against it would record that age as latency.
        // Only the send stamp is a transport fact, so a backfill row measures no wire time at all.
        MessageBody::Kline(event) => BodyStamps {
            category: Category::Kline,
            exchange_sent_ts_us: event.exchange_sent_ts_us.unwrap_or(ABSENT_VENUE),
            exchange_ts_us: ABSENT_VENUE,
            request_sent_ts_us: ABSENT,
        },
        MessageBody::SpinTick(_) => BodyStamps {
            category: Category::Spin,
            exchange_sent_ts_us: ABSENT_VENUE,
            exchange_ts_us: ABSENT_VENUE,
            request_sent_ts_us: ABSENT,
        },
        MessageBody::Exec(event) => BodyStamps {
            category: Category::Exec,
            exchange_sent_ts_us: ABSENT_VENUE,
            exchange_ts_us: if is_venue_stamped(event.kind) {
                event.exchange_ts_us
            } else {
                ABSENT_VENUE
            },
            request_sent_ts_us: event.request_sent_ts_us.unwrap_or(ABSENT),
        },
        MessageBody::BookReset(_)
        | MessageBody::MarketRotation(_)
        | MessageBody::Link(_)
        | MessageBody::RunControl(_)
        | MessageBody::Account(_) => return None,
    };
    Some(stamps)
}

/// Whether `exchange_ts_us` came off the wire rather than our own clock: feeding a locally-stamped
/// kind into the venue-to-engine stage would measure our clock against itself. `AckFailed` is
/// excluded on both of its origins — a synthesised timeout and a venue rejection — because neither
/// carries a transact time.
fn is_venue_stamped(kind: ExecKind) -> bool {
    !matches!(
        kind,
        ExecKind::PlaceNotSent
            | ExecKind::AmendNotSent
            | ExecKind::AckFailed
            | ExecKind::SnapshotEnd
            | ExecKind::StreamReset
            | ExecKind::StreamReady
    )
}
