//! Adapter between the simulated venue and the production execution lifecycle.

mod account;
mod correlation;
mod handshake;
mod summary;

use account::has_account_update;
use correlation::request_matches_answer;
use summary::SimRunSummary;

use crate::msg::exec::{CancelReason, StampedExecCommand};
use crate::msg::ids::{AssetId, ClientOrderId, InstrumentId};
use crate::msg::inbound::{InboundMessage, MessageBody};
use crate::msg::time::{DurationUs, LocalTs};
use crate::runtime::registry::AssetDictionary;
use crate::runtime::shutdown::FatalSignal;
use crate::venues::binance::exec::{DecodeContext, ResponseContext};
use crate::venues::exec::{
    ExecCore, ExecEffect, ExecRequest, ExecSession, InFlightRequest, Outgoing, RequestId,
    SessionSetup, place_not_sent, stream_reset,
};

use super::core::schedule::{
    DeliverySchedule, DueAnswer, SynthesisedEvent, VenueAnswer, VenueReport, VenueVoice,
};
use super::core::{SimEmission, SimVenue};
use super::wire::{SimFill, VenueWire, response_messages, stream_messages};

/// The two timestamps an effect needs are both venue time and both `LocalTs`, so only distinct field
/// names stop a caller swapping them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EffectStamp {
    /// When a request the effect sends reaches the matching model.
    effective_ts_us: LocalTs,
    /// What the driver stamps on events it synthesises for the engine.
    reported_ts_us: LocalTs,
}

impl EffectStamp {
    const fn at(ts_us: LocalTs) -> Self {
        Self {
            effective_ts_us: ts_us,
            reported_ts_us: ts_us,
        }
    }
}

pub struct SimExecDriver {
    session: ExecSession<Vec<InboundMessage>>,
    venue: SimVenue,
    schedule: DeliverySchedule,
    wire: VenueWire,
    instrument: InstrumentId,
    emissions: Vec<SimEmission>,
    due: Vec<DueAnswer>,
    has_swept: bool,
    summary: SimRunSummary,
}

impl SimExecDriver {
    pub fn new(setup: SimExecDriverSetup) -> Self {
        Self {
            session: ExecSession::new(SessionSetup {
                venue: "simulated",
                core: setup.core,
                instruments: vec![setup.instrument],
                events: Vec::new(),
                fatal: setup.fatal,
                inflight_timeout: setup.inflight_timeout,
            }),
            venue: setup.venue,
            schedule: setup.schedule,
            wire: setup.wire,
            instrument: setup.instrument,
            emissions: Vec::new(),
            due: Vec::new(),
            has_swept: false,
            summary: SimRunSummary::default(),
        }
    }

    pub fn on_command(&mut self, stamped: StampedExecCommand, effective_ts_us: LocalTs) {
        let command = stamped.command;
        let mut outgoing = Vec::new();
        self.session.core.on_command(
            command,
            &mut Outgoing::sink(&mut outgoing, command.recon_seq()),
        );
        self.apply(
            &outgoing,
            EffectStamp {
                effective_ts_us,
                reported_ts_us: stamped.issued_ts_us,
            },
        );
    }

    pub fn advance_to(
        &mut self,
        horizon: LocalTs,
        effect_ts_us: LocalTs,
        context: DecodeContext<'_>,
    ) -> Vec<InboundMessage> {
        let Self {
            venue,
            schedule,
            summary,
            emissions,
            ..
        } = self;
        venue.advance_to_watermark(horizon, emissions);
        for emission in emissions.iter() {
            summary.observe(emission);
            schedule.accept(emission);
        }
        let mut forwarded = std::mem::take(&mut self.session.events);
        let mut due = std::mem::take(&mut self.due);
        self.schedule.advance_to(horizon, &mut due);
        for answer in &due {
            let decode = DecodeContext {
                received_ts_us: answer.due_ts_us,
                ..context
            };
            let has_account_update = has_account_update(&answer.voice);
            let messages = self.decode(&answer.voice, answer.event_ts_us, decode);
            self.fold(
                &messages,
                EffectStamp {
                    effective_ts_us: effect_ts_us,
                    reported_ts_us: answer.due_ts_us,
                },
            );
            forwarded.extend(messages);
            if has_account_update {
                forwarded.extend(self.settlement_messages(answer.due_ts_us, decode));
            }
        }
        self.due = due;
        forwarded
    }

    pub fn begin_sweep(&mut self, reason: CancelReason, at_ts_us: LocalTs) {
        let outgoing = self.session.sweep(reason, None);
        let cancelling: Vec<ClientOrderId> = outgoing
            .iter()
            .filter_map(|sent| match sent.effect {
                ExecEffect::Send {
                    request: ExecRequest::Cancel { client_id, .. },
                    ..
                } => Some(client_id),
                _ => None,
            })
            .collect();
        self.apply(&outgoing, EffectStamp::at(at_ts_us));
        let exited = self.venue.force_exit_open_orders(at_ts_us, &cancelling);
        self.schedule.force_sweep(at_ts_us, &exited);
    }

    pub fn venue(&self) -> &SimVenue {
        &self.venue
    }

    pub fn venue_mut(&mut self) -> &mut SimVenue {
        &mut self.venue
    }

    pub fn is_swept(&self) -> bool {
        self.has_swept
    }

    fn apply(&mut self, outgoing: &[Outgoing], stamp: EffectStamp) {
        for &Outgoing { effect, recon_seq } in outgoing {
            match effect {
                ExecEffect::Send {
                    request_id,
                    request,
                } => self.send(request_id, request, recon_seq, stamp.effective_ts_us),
                ExecEffect::PlaceNotSent { .. } | ExecEffect::AmendNotSent { .. } => {
                    self.session.on_unsent(&effect, stamp.reported_ts_us);
                }
                ExecEffect::SweepComplete { .. } => self.has_swept = true,
                ExecEffect::Skipped { .. } => {}
            }
        }
    }

    fn send(
        &mut self,
        request_id: RequestId,
        request: ExecRequest,
        recon_seq: u64,
        effective_ts_us: LocalTs,
    ) {
        if !matches!(request, ExecRequest::SubscribeUserStream) {
            // `sent_at` is venue time, never a clock reading: replaying the same tape has to
            // reproduce the same run.
            self.session.inflight.record(InFlightRequest {
                request_id,
                request,
                sent_at: effective_ts_us,
                recon_seq,
            });
        }
        self.venue.on_request(&request, effective_ts_us);
    }

    fn fold(&mut self, messages: &[InboundMessage], stamp: EffectStamp) {
        let mut outgoing = Vec::new();
        for message in messages {
            let MessageBody::Exec(event) = &message.body else {
                continue;
            };
            self.session.fold(event, 0, &mut outgoing);
        }
        self.apply(&outgoing, stamp);
    }

    fn decode(
        &mut self,
        voice: &VenueVoice,
        event_ts_us: LocalTs,
        decode: DecodeContext<'_>,
    ) -> Vec<InboundMessage> {
        let at_ts_us = decode.received_ts_us;
        match voice {
            VenueVoice::Response(answer) => self.answer_messages(answer, event_ts_us, decode),
            VenueVoice::Report(report) => {
                let payload = self.mint_report(report, event_ts_us, decode.assets);
                stream_messages(&[payload], decode)
            }
            VenueVoice::Synthesised(SynthesisedEvent::StreamReset) => {
                vec![InboundMessage::new(
                    at_ts_us,
                    MessageBody::Exec(stream_reset(at_ts_us)),
                )]
            }
            VenueVoice::Synthesised(SynthesisedEvent::PlaceNotSent(order)) => {
                let event = place_not_sent(self.instrument, order.client_id, order.side, at_ts_us);
                vec![InboundMessage::new(at_ts_us, MessageBody::Exec(event))]
            }
            VenueVoice::Synthesised(SynthesisedEvent::StreamSubscribed) => Vec::new(),
        }
    }

    /// The venue answers from the order it holds rather than from the request, so the oldest
    /// outstanding request of the answer's shape is the one it answers.
    fn answer_messages(
        &mut self,
        answer: &VenueAnswer,
        event_ts_us: LocalTs,
        decode: DecodeContext<'_>,
    ) -> Vec<InboundMessage> {
        let Some(held) = self
            .session
            .inflight
            .take_where(|held| request_matches_answer(held.request, answer))
        else {
            return Vec::new();
        };
        let payload = self.mint_answer(answer, event_ts_us);
        let mut messages = response_messages(
            &[payload],
            &ResponseContext {
                decode,
                request: held.request,
                recon_seq: held.recon_seq,
            },
        );
        for message in &mut messages {
            if let MessageBody::Exec(event) = &mut message.body {
                event.request_sent_ts_us = Some(held.sent_at);
            }
        }
        messages
    }

    fn mint_answer(&self, answer: &VenueAnswer, event_ts_us: LocalTs) -> String {
        let wire = &self.wire;
        match answer {
            VenueAnswer::PlaceAccepted(order) => wire.place_ack(order, event_ts_us),
            VenueAnswer::CancelAccepted(order) => wire.cancel_ack(order, event_ts_us),
            VenueAnswer::AmendAccepted(order) => wire.amend_ack(order, event_ts_us),
            VenueAnswer::Status { order, status } => {
                wire.order_status_as(order, *status, event_ts_us)
            }
            VenueAnswer::OpenOrders(rows) => wire.open_orders(rows, event_ts_us),
            VenueAnswer::Refused { rejection, .. } => wire.refusal(*rejection),
        }
    }

    fn mint_report(
        &self,
        report: &VenueReport,
        event_ts_us: LocalTs,
        assets: &AssetDictionary,
    ) -> String {
        let wire = &self.wire;
        match report {
            VenueReport::New(order) => wire.new_report(order, event_ts_us),
            VenueReport::Trade {
                order,
                trade_id,
                settlement,
            } => wire.trade_report(
                order,
                SimFill {
                    trade_id: *trade_id,
                    settlement,
                    fee_asset: match settlement.fee {
                        0 => "",
                        _ => asset_name(assets, settlement.fee_asset),
                    },
                },
                event_ts_us,
            ),
            VenueReport::Canceled(order) => wire.cancel_report(order, event_ts_us),
            VenueReport::Rejected(order) => wire.rejected_report(order, event_ts_us),
        }
    }
}

pub struct SimExecDriverSetup {
    pub core: ExecCore,
    pub venue: SimVenue,
    pub schedule: DeliverySchedule,
    pub wire: VenueWire,
    pub instrument: InstrumentId,
    pub fatal: FatalSignal,
    pub inflight_timeout: DurationUs,
}

fn asset_name(assets: &AssetDictionary, asset: AssetId) -> &str {
    assets
        .name(asset)
        .unwrap_or_else(|| panic!("simulated wallet names unknown asset {}", asset.0))
}

fn venue_millis(at_ts_us: LocalTs) -> u64 {
    u64::try_from(at_ts_us.micros() / 1_000).unwrap_or_default()
}
