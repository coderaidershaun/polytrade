//! Simulator taps at the Binance normalization boundary.

use std::time::Duration;

use crate::adapters::binance::parse::ParseContext;
use crate::adapters::binance::rest::RestRequest;
use crate::error;
use crate::ids::InstrumentId;
use crate::msg::inbound::TappedMessage;

use super::{Actor, liveness};

impl Actor {
    // Both consumers must observe the same prefix.
    pub(super) fn flush_tapped(&mut self, out: Vec<TappedMessage>) {
        for tapped in out {
            let mut message = tapped.message;
            message.set_queued_ts_us(self.clock.now());
            self.producer.push_tapped(message, tapped.venue_meta);
        }
    }

    /// Uses the simulator heartbeat for tapped lanes.
    pub(super) fn poll_period(&self) -> Duration {
        if self.producer.has_tap() { self.tap_heartbeat } else { liveness::LIVENESS_POLL }
    }

    /// Every REST read this actor makes goes through here, because a fetch that outlives the
    /// heartbeat would emit no watermark while it runs and so stall the simulator's ordering gate.
    /// A lane with no tap fetches plainly. `None` means the failure has already been logged.
    pub(super) async fn read_rest(
        &mut self,
        instrument: InstrumentId,
        what: &'static str,
        plan: impl FnOnce(String) -> RestRequest,
    ) -> Option<(String, ParseContext)> {
        let Some(symbol) = self.venue_symbol(instrument) else {
            self.on_unknown_symbol(instrument, &format!("its {what}"));
            return None;
        };
        let request = plan(symbol);
        let fetched = if self.producer.has_tap() {
            let mut fetch = std::pin::pin!(self.rest.fetch_text(&request));
            loop {
                match tokio::time::timeout(self.tap_heartbeat, &mut fetch).await {
                    Ok(result) => break result,
                    Err(_elapsed) => self.producer.push_tap_watermark(self.clock.now()),
                }
            }
        } else {
            self.rest.fetch_text(&request).await
        };
        let json = fetched
            .map_err(|error| self.on_rest_error(&error, &format!("{what} {}", instrument.0)))
            .ok()?;
        let ctx = ParseContext {
            instrument,
            received_ts_us: self.clock.now(),
        };
        Some((json, ctx))
    }

    // Never reuse a sequence generation.
    #[cold]
    pub(super) fn on_epoch_exhausted(&self) {
        error!(
            "binance adapter {} exhausted its connection epoch counter",
            self.label
        );
        self.fatal.trip(format!(
            "binance adapter {} exhausted its connection epoch counter",
            self.label
        ));
    }
}
