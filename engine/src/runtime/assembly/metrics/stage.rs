//! Which stages each category times, and which two stamps each stage subtracts. A pair that a
//! category does not list has no histogram, and a stamp that a sample lacks gives no value.

use crate::msg::latency::{Category, Stage, StageSample, WireSample};
use crate::msg::time::{TickScale, TsUs};

/// Each stage that a sample of each category can time. A pair that is not listed has no
/// histogram, so the memory of the thread stays a fixed few megabytes.
pub(super) const PAIRS: [(Category, Stage); 34] = [
    (Category::Trade, Stage::VenueToEngine),
    (Category::Trade, Stage::VenueInternal),
    (Category::Trade, Stage::WireToMessage),
    (Category::Trade, Stage::QueueWait),
    (Category::Trade, Stage::Processing),
    (Category::Trade, Stage::MessageToDecision),
    (Category::BookDelta, Stage::VenueToEngine),
    (Category::BookDelta, Stage::WireToMessage),
    (Category::BookDelta, Stage::QueueWait),
    (Category::BookDelta, Stage::Processing),
    (Category::BookDelta, Stage::MessageToDecision),
    (Category::BookSnapshot, Stage::VenueToEngine),
    (Category::BookSnapshot, Stage::WireToMessage),
    (Category::BookSnapshot, Stage::QueueWait),
    (Category::BookSnapshot, Stage::Processing),
    (Category::BookSnapshot, Stage::MessageToDecision),
    (Category::Kline, Stage::VenueToEngine),
    (Category::Kline, Stage::WireToMessage),
    (Category::Kline, Stage::QueueWait),
    (Category::Kline, Stage::Processing),
    (Category::Kline, Stage::MessageToDecision),
    (Category::Spin, Stage::WireToMessage),
    (Category::Spin, Stage::QueueWait),
    (Category::Spin, Stage::Processing),
    (Category::Spin, Stage::MessageToDecision),
    (Category::Exec, Stage::VenueToEngine),
    (Category::Exec, Stage::WireToMessage),
    (Category::Exec, Stage::QueueWait),
    (Category::Exec, Stage::Processing),
    (Category::Exec, Stage::MessageToDecision),
    (Category::Exec, Stage::OrderRoundTrip),
    (Category::Command, Stage::DecisionToWire),
    (Category::Command, Stage::BankAndLaneWait),
    (Category::Command, Stage::Send),
];

/// Why a sample gave no value for one stage.
pub(super) enum Fault {
    Backward,
    Negative,
}

fn present(stamp: TsUs) -> Option<TsUs> {
    (stamp != StageSample::ABSENT).then_some(stamp)
}

/// From a venue stamp to one of ours. Negative means that the venue's clock runs ahead.
fn venue_span(venue: TsUs, ours: TsUs) -> Result<u64, Fault> {
    micros_span(venue, ours).ok_or(Fault::Negative)
}

/// Between two stamps of our own clock, which never runs backward unless something is wrong.
fn own_span(start: TsUs, end: TsUs) -> Result<u64, Fault> {
    micros_span(start, end).ok_or(Fault::Backward)
}

fn micros_span(start: TsUs, end: TsUs) -> Option<u64> {
    let micros = i128::from(end.micros()) - i128::from(start.micros());
    (micros >= 0).then(|| u64::try_from(micros.saturating_mul(1_000)).unwrap_or(u64::MAX))
}

/// `None` when the sample does not carry the stamps of `stage`.
pub(super) fn stage_nanos(
    sample: &StageSample,
    stage: Stage,
    scale: &TickScale,
) -> Option<Result<u64, Fault>> {
    let ticked = |nanos: Option<u64>| Some(nanos.ok_or(Fault::Backward));
    match stage {
        Stage::VenueToEngine => {
            let venue = present(sample.exchange_sent_ts_us).or(present(sample.exchange_ts_us))?;
            Some(venue_span(venue, sample.received_ts_us))
        }
        Stage::VenueInternal => {
            let sent = present(sample.exchange_sent_ts_us)?;
            let matched = present(sample.exchange_ts_us)?;
            Some(venue_span(matched, sent))
        }
        Stage::WireToMessage => Some(own_span(sample.received_ts_us, sample.queued_ts_us)),
        Stage::QueueWait => ticked(scale.nanos_since(sample.queued_ts_us, sample.pop_tick)),
        Stage::Processing => ticked(scale.nanos(sample.pop_tick, sample.done_tick)),
        Stage::MessageToDecision => {
            ticked(scale.nanos_since(sample.queued_ts_us, sample.done_tick))
        }
        Stage::OrderRoundTrip => {
            let sent = present(sample.request_sent_ts_us)?;
            Some(own_span(sent, sample.received_ts_us))
        }
        Stage::DecisionToWire | Stage::BankAndLaneWait | Stage::Send => None,
    }
}

pub(super) fn wire_nanos(
    sample: &WireSample,
    stage: Stage,
    scale: &TickScale,
) -> Option<Result<u64, Fault>> {
    let nanos = match stage {
        Stage::DecisionToWire => scale.nanos(sample.issue_tick, sample.wire_tick),
        Stage::BankAndLaneWait => scale.nanos(sample.issue_tick, sample.pop_tick),
        Stage::Send => scale.nanos(sample.pop_tick, sample.wire_tick),
        Stage::VenueToEngine
        | Stage::VenueInternal
        | Stage::WireToMessage
        | Stage::MessageToDecision
        | Stage::QueueWait
        | Stage::Processing
        | Stage::OrderRoundTrip => return None,
    };
    Some(nanos.ok_or(Fault::Backward))
}
