//! Turns effects into HTTP requests; the socket is only ever used to open the user stream.
//! Three withholding policies live here, each preventing a request from being sent under a
//! specific condition. A placement is withheld when the venue reports unavailability, since a
//! 425 or 503 describes the venue's own state, not a problem with the order. A cancel is
//! withheld during the venue's taker hold window, to avoid a refusal or a hard reject. A sell
//! is withheld until its token's allowance cache is warm, because the venue treats a cold
//! cache as an empty wallet.

use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::Message;

use crate::adapters::exec::{
    EventSink, ExecEffect, ExecRequest, InFlightRequest, Outgoing, PlaceNotSentReason, RequestId,
    SessionOutcome, SkipReason, stream_ready,
};
use crate::ids::{ClientOrderId, Side};
use crate::msg::exec::{CancelReason, ExecEvent};
use crate::time::DurationUs;
use crate::{error, info, warn};

use super::super::codec::{
    EncodeContext, PlaceRequestContext, VenueAvailability, conditional_allowance_refresh,
    encode_request, subscribe_user_stream,
};
use super::super::correlate::{WithheldCancel, restates_balances};
use super::rest::{Lane, RestJob, RestPurpose, Submitted};
use super::{Actor, Writer};

impl Actor {
    pub(super) async fn dispatch(
        &mut self,
        outgoing: Vec<Outgoing>,
        mut writer: Option<&mut Writer>,
    ) -> Option<SessionOutcome> {
        let mut ended = None;
        for Outgoing { effect, recon_seq } in outgoing {
            match effect {
                ExecEffect::Send {
                    request_id,
                    request,
                } => {
                    if let Some(next) = self
                        .send(request_id, request, recon_seq, writer.as_deref_mut())
                        .await
                    {
                        ended = Some(next);
                    }
                }
                ExecEffect::Skipped { client_id, reason } => self.on_skipped(client_id, reason),
                ExecEffect::PlaceNotSent { .. } | ExecEffect::AmendNotSent { .. } => {
                    self.session.on_unsent(&effect, self.control.clock.now());
                }
                ExecEffect::SweepComplete { reason } => self.on_sweep_complete(reason),
            }
        }
        ended
    }

    async fn send(
        &mut self,
        request_id: RequestId,
        request: ExecRequest,
        recon_seq: u64,
        writer: Option<&mut Writer>,
    ) -> Option<SessionOutcome> {
        if matches!(request, ExecRequest::SubscribeUserStream) {
            return self.subscribe(writer).await;
        }
        if let Some(refusal) = self.decide_send(&request) {
            self.session
                .refuse(request, refusal, self.control.clock.now());
            return None;
        }
        if let Some(release_at) = self.cancel_hold(&request) {
            self.delayed.withhold(WithheldCancel {
                request_id,
                request,
                recon_seq,
                release_at,
            });
            return None;
        }
        let encoded = {
            let context = EncodeContext {
                tokens: &self.tokens,
                orders: &self.orders,
                signer: &self.signer,
                sent_ts_us: self.control.clock.now(),
            };
            encode_request(request, &context)
        };
        let encoded = match encoded {
            Ok(encoded) => encoded,
            Err(error) => {
                warn!("polymarket execution could not build a request: {error}");
                self.session.refuse(
                    request,
                    PlaceNotSentReason::Encoding,
                    self.control.clock.now(),
                );
                return None;
            }
        };
        let (lane, place) = match request {
            ExecRequest::Place {
                instrument,
                client_id,
                side,
                price,
                qty,
                ..
            } => (
                Lane::Place,
                Some(PlaceRequestContext {
                    instrument,
                    client_id,
                    side,
                    price,
                    qty,
                }),
            ),
            _ => (Lane::Control, None),
        };
        let purpose = RestPurpose::Core {
            request_id,
            request,
            recon_seq,
            place,
        };
        if self.submit(lane, RestJob::signed(purpose, encoded)) == Submitted::LaneFull {
            self.session.refuse(
                request,
                PlaceNotSentReason::NoTransport,
                self.control.clock.now(),
            );
            return None;
        }
        self.session.inflight.record(InFlightRequest {
            request_id,
            request,
            sent_at: self.control.clock.now(),
            recon_seq,
        });
        None
    }

    /// The user stream is authenticated by the subscribe frame itself and answers no ack, so the
    /// stream is ready the moment the frame is on the wire. A re-arm after a scoped sweep reaches
    /// here with the socket already subscribed — sending a second auth frame would be a guess about
    /// behaviour the venue does not document, so it is skipped rather than repeated.
    async fn subscribe(&mut self, writer: Option<&mut Writer>) -> Option<SessionOutcome> {
        if !self.is_subscribed {
            let Some(writer) = writer else {
                return Some(SessionOutcome::Reconnect);
            };
            let frame = subscribe_user_stream(
                self.signer.api_key(),
                self.credentials.secret(),
                self.credentials.passphrase(),
            );
            let frame = match frame {
                Ok(frame) => frame,
                Err(error) => {
                    error!("polymarket execution cannot build its stream subscription: {error}");
                    self.control
                        .fatal
                        .trip(format!("polymarket execution subscribe: {error}"));
                    return Some(SessionOutcome::Reconnect);
                }
            };
            if writer.send(Message::Text(frame.into())).await.is_err() {
                warn!("polymarket execution could not send its stream subscription — reconnecting");
                return Some(SessionOutcome::Reconnect);
            }
            self.is_subscribed = true;
        }
        let now = self.control.clock.now();
        self.forward_exec(stream_ready(now));
        self.start_resync();
        None
    }

    /// Whether this request may leave now, and the work that makes it sendable next time. It is a
    /// verb because it is not a query: an availability refusal is counted here, and a cold token's
    /// allowance refresh is issued here.
    fn decide_send(&mut self, request: &ExecRequest) -> Option<PlaceNotSentReason> {
        let now = self.control.clock.now();
        if let Some(parked) = self.parked
            && now < parked.until
        {
            let is_blocked = match request {
                ExecRequest::Place { .. } => true,
                ExecRequest::Cancel { .. } => !parked.availability.allows_cancel(),
                _ => false,
            };
            if is_blocked {
                self.counts.availability_refusals += 1;
                return Some(PlaceNotSentReason::NoTransport);
            }
        }
        let ExecRequest::Place {
            instrument, side, ..
        } = request
        else {
            return None;
        };
        // A sell against a token whose allowance cache the CLOB has not refreshed is rejected as an
        // empty wallet, which the reject table classifies Fatal — refusing here keeps that
        // classification honest and costs one spin.
        if *side == Side::Sell {
            let cold_token = self
                .tokens
                .live_binding(*instrument)
                .map(|binding| binding.token_id.clone())
                .filter(|token| !self.bindings.is_allowance_warm(token));
            if let Some(token) = cold_token {
                self.warm_allowance(&token);
                return Some(PlaceNotSentReason::NoTransport);
            }
        }
        None
    }

    /// The venue's taker hold, during which a cancel is refused rather than queued.
    fn cancel_hold(&self, request: &ExecRequest) -> Option<crate::time::TsUs> {
        let ExecRequest::Cancel { client_id, .. } = request else {
            return None;
        };
        self.delayed
            .held_until(*client_id, self.control.clock.now())
    }

    pub(super) async fn release_withheld_cancels(
        &mut self,
        writer: Option<&mut Writer>,
    ) -> Option<SessionOutcome> {
        let released = self.delayed.released(self.control.clock.now());
        if released.is_empty() {
            return None;
        }
        let outgoing = released
            .into_iter()
            .map(|cancel| Outgoing {
                recon_seq: cancel.recon_seq,
                effect: cancel.into_effect(),
            })
            .collect();
        self.dispatch(outgoing, writer).await
    }

    /// The CLOB caches allowances, and the cache is cold for a token minted five minutes ago —
    /// however approved the chain is, a sell against a cold cache is refused as an empty wallet.
    pub(super) fn warm_allowance(&mut self, token_id: &str) {
        if !self.bindings.claim_allowance_refresh(token_id) {
            return;
        }
        let purpose = RestPurpose::Allowance {
            token_id: token_id.into(),
        };
        let request = conditional_allowance_refresh(token_id, self.signature_type);
        let submitted = self.submit(Lane::Control, RestJob::signed(purpose, request));
        // Only an answer clears the claim, and no answer is coming for a request that never left.
        // Left claimed, the token reads as cold forever and every sell on it is withheld for the
        // rest of the run.
        if submitted == Submitted::LaneFull {
            self.bindings.release_allowance_refresh(token_id);
        }
    }

    /// The venue said "come back later". Placement parks; the hot side is told its pending slot
    /// closed so the declaration loop can decide again rather than hanging on an answer that will
    /// never arrive.
    pub(super) fn on_unavailable(&mut self, availability: VenueAvailability) {
        let wait = match availability {
            VenueAvailability::Restarting => DurationUs::from_secs(2),
            VenueAvailability::PostOnlyMode { retry_after_secs }
            | VenueAvailability::RateLimited { retry_after_secs } => {
                DurationUs::from_secs(retry_after_secs.unwrap_or(1).clamp(1, 60))
            }
            VenueAvailability::CancelOnly | VenueAvailability::TradingDisabled => {
                DurationUs::from_secs(5)
            }
        };
        let until = self.control.clock.now() + wait;
        let is_new = self
            .parked
            .is_none_or(|parked| parked.availability != availability);
        self.parked = Some(super::Parked {
            availability,
            until,
        });
        if is_new {
            warn!(
                "polymarket execution parked on venue state {availability:?} for {}ms — placements withheld, this is not an order rejection",
                wait.micros() / 1_000
            );
        }
    }

    fn on_skipped(&mut self, client_id: Option<ClientOrderId>, reason: SkipReason) {
        match reason {
            SkipReason::ForeignOrder => warn!(
                "polymarket execution left an order it did not place alone: {:016x}",
                client_id.unwrap_or(ClientOrderId(0)).0
            ),
            SkipReason::AlreadyCancelling => {}
        }
    }

    /// A sweep the hot side asked for is not an exit: the next window has to be quotable. The core
    /// leaves Cancelling for Down when a scoped sweep settles, and only a fresh resync walks it back
    /// to Quoting.
    fn on_sweep_complete(&mut self, reason: CancelReason) {
        info!("polymarket execution swept every order it owns: {reason:?}");
        match self.control.exit.is_some() {
            true => self.control.is_swept = true,
            false => self.re_arm_after_sweep(),
        }
    }

    fn re_arm_after_sweep(&mut self) {
        // Connecting the core again asks only for the stream subscription, or for nothing once the
        // core has stopped for good. The socket is already subscribed, so this re-reads state and
        // re-opens quoting without touching the wire.
        if self.session.connect().is_empty() {
            return;
        }
        self.forward_exec(stream_ready(self.control.clock.now()));
        self.start_resync();
    }

    pub(super) fn forward_exec(&mut self, event: ExecEvent) {
        // A fill changes the account, so the read is triggered where the report is rather than
        // where a caller remembers to ask for it. It does not release the reservation the fill
        // took: this venue answers a read taken now with the pre-fill number, and only the trade's
        // own settlement frees it.
        if restates_balances(&event) {
            self.restate_balances();
        }
        self.session.events.send_exec(event);
    }
}
