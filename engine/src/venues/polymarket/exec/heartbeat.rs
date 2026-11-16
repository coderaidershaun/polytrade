//! When the edge sends the heartbeat that keeps the venue from cancelling the book, and what it does
//! when beats go unanswered. Pure state: each instant is a parameter.

use crate::msg::time::{DurationUs, LocalTs};
use crate::venues::edge::http::TransportError;

use super::codec::decode_heartbeat;
use super::facts::POLYMARKET_FACTS;
use super::rest::ClobResponse;

/// What the edge does at one poll.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatStep {
    Wait,
    /// Send a beat that echoes `id`, the id that the last answer named.
    Send {
        id: Box<str>,
    },
    /// No beat was answered since the deadline: rebuild the order mirror from a read of the venue,
    /// and place nothing until a beat is answered again.
    ResetMirror,
    /// The venue limit passed with no beat answered, so the venue has cancelled the book: read the
    /// open orders again.
    ReadAfterVenueCancel,
}

/// How the venue answered one beat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeatAnswer {
    /// The venue took the beat and named the id that the next beat echoes.
    Accepted,
    /// The venue refused the beat, as it does for a stale id, and named the id that the next beat
    /// must echo.
    Refused,
    /// No answer came, or the answer named no id.
    Lost,
}

#[derive(Debug, Default)]
pub struct HeartbeatSchedule {
    id: Option<Box<str>>,
    /// When the beat in flight went out. One beat at a time: the next one waits for its answer.
    in_flight_since: Option<LocalTs>,
    /// When the last beat that the venue accepted went out, or the first beat before any was
    /// accepted. The venue limit runs from here.
    alive_since: Option<LocalTs>,
    /// When the next beat is due. `None` is at once: the first beat.
    next_due: Option<LocalTs>,
    has_reset_mirror: bool,
    has_read_after_cancel: bool,
}

impl HeartbeatSchedule {
    pub fn new() -> Self {
        Self::default()
    }

    /// The deadline and the venue limit come first, each one time while no beat is answered.
    pub fn on_poll(&mut self, now: LocalTs) -> HeartbeatStep {
        if let Some(alive_since) = self.alive_since {
            let silence = now.diff(alive_since);
            if !self.has_reset_mirror && silence >= deadline() {
                self.has_reset_mirror = true;
                return HeartbeatStep::ResetMirror;
            }
            if !self.has_read_after_cancel && silence >= POLYMARKET_FACTS.heartbeat_limit {
                self.has_read_after_cancel = true;
                return HeartbeatStep::ReadAfterVenueCancel;
            }
        }
        let is_due = self.next_due.is_none_or(|due| now >= due);
        if self.in_flight_since.is_some() || !is_due {
            return HeartbeatStep::Wait;
        }
        HeartbeatStep::Send {
            id: self.id.clone().unwrap_or_default(),
        }
    }

    /// The beat is on its way. Stamped only once it is: stamping the attempt would hold the next
    /// beat off against the venue limit.
    pub fn on_sent(&mut self, now: LocalTs) {
        self.in_flight_since = Some(now);
        self.alive_since.get_or_insert(now);
    }

    /// The venue names the id of the next beat in a refusal as in a success, so either answer
    /// continues the chain. Only an accepted beat keeps the book alive, and it counts from when it
    /// went out, because the venue heard it before it answered. A beat that failed is tried again
    /// after the retry interval, not at the next tick, so a venue that refuses each beat is not sent
    /// one at each tick.
    pub fn on_answer(&mut self, answer: &Result<ClobResponse, TransportError>) -> BeatAnswer {
        let sent_at = self.in_flight_since.take();
        let verdict = self.read_answer(answer);
        match (verdict, sent_at) {
            (BeatAnswer::Accepted, Some(sent_at)) => {
                self.alive_since = Some(sent_at);
                self.next_due = Some(sent_at + POLYMARKET_FACTS.heartbeat_period);
                self.has_reset_mirror = false;
                self.has_read_after_cancel = false;
            }
            _ => self.next_due = sent_at.map(|sent_at| sent_at + POLYMARKET_FACTS.heartbeat_retry),
        }
        verdict
    }

    /// After the mirror was reset, until a beat is answered: the venue may cancel the book at any
    /// moment, so an order placed now could be cancelled with no report.
    pub fn is_placing_held(&self) -> bool {
        self.has_reset_mirror
    }

    fn read_answer(&mut self, answer: &Result<ClobResponse, TransportError>) -> BeatAnswer {
        let Ok(response) = answer else {
            return BeatAnswer::Lost;
        };
        let Ok(id) = decode_heartbeat(&response.body) else {
            return BeatAnswer::Lost;
        };
        self.id = Some(id);
        match response.is_success() {
            true => BeatAnswer::Accepted,
            false => BeatAnswer::Refused,
        }
    }
}

/// The venue limit less the margin, so that the read of the mirror comes before the venue cancels
/// the book.
fn deadline() -> DurationUs {
    let facts = POLYMARKET_FACTS;
    DurationUs::from_micros(facts.heartbeat_limit.micros() - facts.heartbeat_margin.micros())
}
