//! Effect execution + REST task spawning.

use std::sync::Arc;
use std::time::Instant;

use futures_util::SinkExt;
use tokio_tungstenite::tungstenite::Message;

use crate::ids::{InstrumentId, Side};
use crate::msg::inbound::Level;
use crate::time::TsUs;
use crate::{warn, warn_repeating};

use crate::adapters::lane::Flow;
use crate::adapters::polymarket::parse::ParseError;
use crate::adapters::polymarket::rest::{BookProbe, GammaError, GammaMarket, PolyRest};
use crate::adapters::polymarket::rotation::{
    ForceTeardownFacts, ProbeOutcome, WindowAssignment, WindowTokens,
};
use crate::adapters::polymarket::shadow::BookMismatch;
use crate::adapters::polymarket::ws;
use crate::adapters::socket::Writer;

use super::Actor;
use super::core::DriverEffect;

/// REST task result fed back to single-owner loop.
pub(super) enum CoreEvent {
    ResolveOk {
        start: TsUs,
        market: GammaMarket,
    },
    ResolveErr {
        start: TsUs,
    },
    Probed {
        tokens: WindowTokens,
        /// `None` when the probe never reached the venue. The slot's in-flight mark still clears,
        /// and the FSM asks again on its own cadence.
        outcome: Option<ProbeOutcome>,
    },
    RateLimited {
        pending: Pending,
        retry_after_secs: Option<u64>,
    },
}

/// A REST call in flight, keyed so the same call is never stacked twice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Pending {
    Resolve(TsUs),
    Probe(WindowTokens),
}

impl Pending {
    pub(super) fn api(&self) -> &'static str {
        match self {
            Pending::Resolve(_) => "gamma",
            Pending::Probe(_) => "clob",
        }
    }
}

impl Actor {
    pub(super) async fn execute(
        &mut self,
        effects: Vec<DriverEffect>,
        writer: &mut Writer,
    ) -> Flow {
        let mut effects = effects.into_iter();
        for effect in effects.by_ref() {
            if let Some(command) = self.handle_effect(effect) {
                let text = ws::operation_message(&command);
                if writer.send(Message::Text(text.into())).await.is_err() {
                    warn!(
                        "polymarket adapter {} operation send failed — draining then reconnecting",
                        self.label
                    );
                    self.execute_offline(effects.collect());
                    return Flow::Reconnect;
                }
            }
        }
        Flow::Continue
    }

    pub(super) fn execute_offline(&mut self, effects: Vec<DriverEffect>) {
        for effect in effects {
            drop(self.handle_effect(effect));
        }
    }

    fn handle_effect(&mut self, effect: DriverEffect) -> Option<ws::WsCommand> {
        match effect {
            DriverEffect::Emit(mut message) => {
                message.set_queued_ts_us(self.clock.now());
                self.producer.push(message);
                None
            }
            DriverEffect::PersistRotation(row) => {
                // Full/closed → drop + count + WARN, never block.
                if self.rotations_tx.try_send(row).is_err() {
                    warn_repeating!(
                        self.dropped_rotations,
                        "polymarket adapter {} dropped {} rotation lineage rows — persistence side-channel full or closed",
                        self.label,
                        self.dropped_rotations
                    );
                }
                None
            }
            DriverEffect::Resolve(start) => {
                let call = resolve_window(Arc::clone(&self.rest), start);
                self.spawn_rest(Pending::Resolve(start), call);
                None
            }
            DriverEffect::Probe(tokens) => {
                let call = probe_window(Arc::clone(&self.rest), tokens.clone());
                self.spawn_rest(Pending::Probe(tokens), call);
                None
            }
            DriverEffect::Subscribe(tokens) => Some(ws::WsCommand::Subscribe(tokens)),
            DriverEffect::Unsubscribe(tokens) => Some(ws::WsCommand::Unsubscribe(tokens)),
            DriverEffect::ForcedTeardown(facts) => {
                self.warn_forced_teardown(&facts);
                None
            }
            DriverEffect::Diverged {
                instrument,
                mismatch,
            } => {
                self.warn_diverged(instrument, &mismatch);
                None
            }
            DriverEffect::BindExecution(assignment) => {
                self.bind_execution(*assignment);
                None
            }
            DriverEffect::TickSizeChange(change) => {
                warn!(
                    "polymarket adapter {} tick_size_change (unverified shape) asset {} tick {} -> {} — counted",
                    self.label,
                    change.asset_id.as_deref().unwrap_or("?"),
                    change.old_tick_size.as_deref().unwrap_or("?"),
                    change.new_tick_size.as_deref().unwrap_or("?")
                );
                None
            }
        }
    }

    /// The execution edge is optional — a mode-off run has none — and a full channel means the
    /// edge is not draining, which the edge itself reports far better than a market-data warning.
    fn bind_execution(&mut self, assignment: WindowAssignment) {
        let Some(sender) = &self.window_assignments else {
            return;
        };
        if sender.try_send(assignment).is_err() {
            warn_repeating!(
                self.dropped_bindings,
                "polymarket adapter {} could not hand {} rotation bindings to the execution edge",
                self.label,
                self.dropped_bindings
            );
        }
    }

    /// The core asks again on its own cadence, a probe every five seconds, so without the in-flight
    /// guard a slow venue would stack calls for the same window until it answered.
    fn spawn_rest(
        &mut self,
        pending: Pending,
        call: impl Future<Output = CoreEvent> + Send + 'static,
    ) {
        if self.inflight.contains(&pending) || self.withhold_while_rest_quiet() {
            return;
        }
        self.inflight.push(pending);
        let tx = self.results_tx.clone();
        tokio::spawn(async move {
            if tx.send(call.await).await.is_err() {
                // The actor has shut down; nothing consumes the result.
            }
        });
    }

    /// Whether the REST quiet period withholds this call, counting what the quiet costs. It is a
    /// verb because it is not a query: a second ask counts a second suppression, and a window that
    /// rolls past unresolved is otherwise invisible — the quiet logs once when it opens and nothing
    /// after.
    fn withhold_while_rest_quiet(&mut self) -> bool {
        if !self.rest_quiet.is_active(Instant::now()) {
            return false;
        }
        warn_repeating!(
            self.suppressed_rest,
            "polymarket adapter {} suppressed {} window resolves and probes while its rest quiet period runs",
            self.label,
            self.suppressed_rest
        );
        true
    }

    pub(super) fn clear_inflight(&mut self, settled: &Pending) {
        self.inflight.retain(|pending| pending != settled);
    }

    /// Venue drops `price_change` → shadow diverges until resnapshot. Venue property, not incident:
    /// count always, WARN on power-of-two for rate visibility without flood.
    #[cold]
    fn warn_diverged(&mut self, instrument: InstrumentId, mismatch: &BookMismatch) {
        warn_repeating!(
            self.divergences,
            "polymarket adapter {} resnapshotted book {} after {} shadow divergences — latest at the {} rank: shadow {} of {} levels, venue {} of {}",
            self.label,
            instrument.0,
            self.divergences,
            side_word(mismatch.side),
            level_word(mismatch.shadow),
            mismatch.shadow_levels,
            level_word(mismatch.venue),
            mismatch.venue_levels
        );
    }

    fn warn_forced_teardown(&self, facts: &ForceTeardownFacts) {
        warn!(
            "polymarket adapter {} force-teardown window {} (grace age {}us) — evicted for the next window",
            self.label,
            facts.window_open_ts_us.micros(),
            facts.grace_age.micros()
        );
    }

    /// Mantissa overflow fatal; other parse errors drop + count, WARN on power-of-two.
    #[cold]
    pub(super) fn on_parse_error(&mut self, error: ParseError) {
        warn_repeating!(
            self.dropped_frames,
            "polymarket adapter {} dropped {} malformed frames (latest: {error})",
            self.label,
            self.dropped_frames
        );
    }
}

fn side_word(side: Side) -> &'static str {
    match side {
        Side::Buy => "bid",
        Side::Sell => "ask",
    }
}

fn level_word(level: Option<Level>) -> String {
    level.map_or_else(
        || "none".to_owned(),
        |level| format!("{}@{}", level.price.to_f64(), level.qty.to_f64()),
    )
}

async fn resolve_window(rest: Arc<PolyRest>, start: TsUs) -> CoreEvent {
    match rest.resolve_slug(start).await {
        Ok(market) => CoreEvent::ResolveOk { start, market },
        Err(GammaError::RateLimited {
            retry_after_secs, ..
        }) => CoreEvent::RateLimited {
            pending: Pending::Resolve(start),
            retry_after_secs,
        },
        Err(_) => CoreEvent::ResolveErr { start },
    }
}

/// Double 404 confirms teardown (single transient 404 with live sibling is not resolution). Both
/// legs are asked at once so one probe costs one timeout rather than two.
async fn probe_window(rest: Arc<PolyRest>, tokens: WindowTokens) -> CoreEvent {
    let (up, down) = tokio::join!(
        rest.probe_book(tokens.up.as_str()),
        rest.probe_book(tokens.down.as_str())
    );
    match (up, down) {
        (Ok(up), Ok(down)) => {
            let outcome = if up == BookProbe::TornDown && down == BookProbe::TornDown {
                ProbeOutcome::NotFound
            } else {
                ProbeOutcome::BookExists
            };
            CoreEvent::Probed {
                tokens,
                outcome: Some(outcome),
            }
        }
        (
            Err(GammaError::RateLimited {
                retry_after_secs, ..
            }),
            _,
        )
        | (
            _,
            Err(GammaError::RateLimited {
                retry_after_secs, ..
            }),
        ) => CoreEvent::RateLimited {
            pending: Pending::Probe(tokens),
            retry_after_secs,
        },
        _ => CoreEvent::Probed {
            tokens,
            outcome: None,
        },
    }
}
