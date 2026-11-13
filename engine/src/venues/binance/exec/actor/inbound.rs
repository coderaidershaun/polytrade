//! Routes what the venue sends: a request's answer by its id, an account event by its client order
//! id. They part before parsing, so a stream event can never answer a request.

use tokio_tungstenite::tungstenite::{Error as ProtocolError, Message};

use crate::msg::exec::ExecEvent;
use crate::msg::time::LocalTs;
use crate::runtime::shutdown::AnswerPart;
use crate::venues::binance::ws;
use crate::venues::edge::EdgeDriver;
use crate::venues::exec::{
    EventSink, ExecRequest, ExecSessionError, InFlightRequest, RequestId, SessionOutcome,
    TimeoutFallout,
};
use crate::{error, warn};

use super::super::{
    DecodedResponse, IgnoredReason, ResponseContext, StreamEvent, decode_response,
    decode_stream_event,
};
use super::frame::InboundFrame;
use super::{Actor, Writer, frame};

const MAX_SUBSCRIBE_FAILURES: u32 = 3;

impl Actor {
    pub(super) async fn on_frame(
        &mut self,
        frame: Option<Result<Message, ProtocolError>>,
        writer: &mut Writer,
    ) -> Option<SessionOutcome> {
        match frame {
            Some(Ok(Message::Text(text))) => self.on_text(text.as_str(), writer).await,
            Some(Ok(Message::Ping(payload))) => match ws::reply_to_ping(writer, payload).await {
                Ok(()) => None,
                Err(_) => Some(SessionOutcome::Reconnect),
            },
            Some(Ok(Message::Close(_))) | None => Some(SessionOutcome::Reconnect),
            Some(Ok(Message::Pong(_) | Message::Binary(_) | Message::Frame(_))) => None,
            Some(Err(error)) => {
                warn!("binance execution stream: {error}");
                Some(SessionOutcome::Reconnect)
            }
        }
    }

    async fn on_text(&mut self, text: &str, writer: &mut Writer) -> Option<SessionOutcome> {
        match frame::route(text) {
            Ok(InboundFrame::Response(request_id)) => {
                self.on_response(request_id, text, writer).await
            }
            Ok(InboundFrame::StreamEvent) => self.on_stream_frame(text, writer).await,
            Ok(InboundFrame::Unroutable) => {
                self.counts.unroutable_frames += 1;
                warn!(
                    "binance execution frame carried neither an event nor a request id ({} so far)",
                    self.counts.unroutable_frames
                );
                None
            }
            Err(error) => {
                self.counts.dropped_frames += 1;
                warn!("binance execution could not read a frame: {error}");
                None
            }
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "an edge actor stamps received and queued time on each message and paces its own I/O"
    )]
    async fn on_response(
        &mut self,
        request_id: RequestId,
        text: &str,
        writer: &mut Writer,
    ) -> Option<SessionOutcome> {
        let Some(entry) = self.session.inflight.take(request_id) else {
            self.counts.unmatched_responses += 1;
            return None;
        };
        let now = self.control.clock.now();
        let context = ResponseContext {
            decode: self.decode_context(now),
            request: entry.request,
            recon_seq: entry.recon_seq,
        };
        match decode_response(text, &context) {
            Ok(decoded) => self.on_decoded(decoded, entry, now, writer).await,
            Err(error) if error.is_fatal() => {
                error!("binance execution fatal decode: {error}");
                self.session.mark_in_doubt(entry.request, now);
                self.control.fail(ExecSessionError::UndecodableAnswer {
                    venue: self.venue(),
                    part: AnswerPart::Response,
                });
                Some(SessionOutcome::Reconnect)
            }
            Err(error) => {
                self.counts.dropped_frames += 1;
                warn!(
                    "binance execution could not decode an answer: {error} — the request is ambiguous until reconnect reconciliation"
                );
                match entry.request.timeout_fallout() {
                    TimeoutFallout::OrderInDoubt { .. } => {
                        self.session.mark_in_doubt(entry.request, now);
                        Some(SessionOutcome::Reconnect)
                    }
                    TimeoutFallout::StreamUnusable => Some(SessionOutcome::Reconnect),
                    TimeoutFallout::ReadAbandoned => None,
                }
            }
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "an edge actor stamps received and queued time on each message and paces its own I/O"
    )]
    async fn on_decoded(
        &mut self,
        decoded: DecodedResponse,
        entry: InFlightRequest,
        received_ts_us: LocalTs,
        writer: &mut Writer,
    ) -> Option<SessionOutcome> {
        if matches!(entry.request, ExecRequest::SubscribeUserStream) {
            return self.on_subscribe_answer(&decoded, received_ts_us);
        }
        let mut outgoing = Vec::new();
        for event in &decoded.events {
            let event = ExecEvent {
                request_sent_ts_us: Some(entry.sent_at),
                ..*event
            };
            self.note_event(&event);
            if let Err(error) = self.session.fold(&event, entry.recon_seq, &mut outgoing) {
                self.control.fail(error);
            }
            // An order-status probe, not a full snapshot, is what settles an ambiguous cancel here.
            if matches!(
                entry.request,
                ExecRequest::OrderStatus { client_id, .. } if client_id == event.client_id
            ) && event.status.is_some_and(|status| !status.is_terminal())
            {
                self.session.core.re_arm_cancel(event.client_id);
            }
            self.session.events.send_exec(received_ts_us, event);
        }
        let ended = self.dispatch(outgoing, Some(writer)).await;
        self.session.arm_readiness(self.control.clock.now());
        ended
    }

    fn on_subscribe_answer(
        &mut self,
        decoded: &DecodedResponse,
        received_ts_us: LocalTs,
    ) -> Option<SessionOutcome> {
        if decoded.events.is_empty() {
            self.subscribe_failures += 1;
            if self.subscribe_failures >= MAX_SUBSCRIBE_FAILURES {
                error!(
                    "binance execution could not subscribe to the account stream {} times — refusing to quote without one",
                    self.subscribe_failures
                );
                self.control.fail(ExecSessionError::SubscribeRefused {
                    venue: self.venue(),
                    failures: self.subscribe_failures,
                });
            } else {
                warn!("binance execution subscribe rejected — reconnecting");
            }
            return Some(SessionOutcome::Reconnect);
        }
        self.subscribe_failures = 0;
        for event in &decoded.events {
            self.session.events.send_exec(received_ts_us, *event);
        }
        self.start_resync();
        None
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "an edge actor stamps received and queued time on each message and paces its own I/O"
    )]
    async fn on_stream_frame(&mut self, text: &str, writer: &mut Writer) -> Option<SessionOutcome> {
        let now = self.control.clock.now();
        match decode_stream_event(text, &self.decode_context(now)) {
            Ok(StreamEvent::Exec(event)) => {
                let mut outgoing = Vec::new();
                self.note_event(&event);
                if let Err(error) = self.session.fold(&event, 0, &mut outgoing) {
                    self.control.fail(error);
                }
                self.session.events.send_exec(now, event);
                let ended = self.dispatch(outgoing, Some(writer)).await;
                self.session.arm_readiness(self.control.clock.now());
                ended
            }
            Ok(StreamEvent::Account(chunks)) => {
                self.session.events.send_account(now, chunks);
                None
            }
            // A lost delta is resynced by taking a fresh snapshot.
            Ok(StreamEvent::BalanceChanged) => {
                self.request_balance_snapshot();
                None
            }
            Ok(StreamEvent::Ignored(reason)) => {
                self.counts.ignored_events += 1;
                if matches!(reason, IgnoredReason::UntrackedSymbol) {
                    self.counts.untracked_events += 1;
                }
                None
            }
            Err(error) if error.is_fatal() => {
                error!("binance execution fatal stream decode: {error}");
                self.control.fail(ExecSessionError::UndecodableAnswer {
                    venue: self.venue(),
                    part: AnswerPart::Stream,
                });
                Some(SessionOutcome::Reconnect)
            }
            Err(error) => {
                self.counts.dropped_frames += 1;
                warn!("binance execution could not decode an account event: {error}");
                None
            }
        }
    }
}
