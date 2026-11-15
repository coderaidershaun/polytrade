//! What the actor does with a frame it cannot use: it drops and counts a malformed or unroutable
//! frame, and keeps the one fault that ends its session.

use crate::venues::binance::parse::ParseError;
use crate::venues::edge::fault::MarketDataError;
use crate::warn_repeating;

use super::Actor;

impl Actor {
    pub(super) fn parsed<T>(&mut self, result: Result<T, ParseError>) -> Option<T> {
        result.map_err(|error| self.on_parse_error(error)).ok()
    }

    #[cold]
    pub(super) fn on_parse_error(&mut self, error: ParseError) {
        if let ParseError::Decode(fault) = &error
            && let Some(fault) = MarketDataError::of_decimal_fault(self.producer.source_id(), fault)
        {
            self.fault.get_or_insert(fault);
            return;
        }
        warn_repeating!(
            self.malformed_frames,
            "binance adapter {} dropped {} malformed frames (latest: {error})",
            self.label,
            self.malformed_frames
        );
    }

    /// Counted apart from malformed frames: the venue sent something well-formed and this actor has
    /// nowhere to put it, which is a subscription or registry fault rather than a wire fault.
    #[cold]
    pub(super) fn on_unroutable_frame(&mut self, subject: std::fmt::Arguments<'_>) {
        warn_repeating!(
            self.unroutable_frames,
            "binance adapter {} has no sequencer for {subject} — {} such frames dropped",
            self.label,
            self.unroutable_frames
        );
    }
}
