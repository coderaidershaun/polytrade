//! When the edge sends the heartbeat that keeps the venue from cancelling the book, and what it does
//! when beats go unanswered. Pure state: each instant is a parameter.

use crate::msg::time::LocalTs;
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
    last_sent: Option<LocalTs>,
}

impl HeartbeatSchedule {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_poll(&mut self, now: LocalTs) -> HeartbeatStep {
        let is_due = self
            .last_sent
            .is_none_or(|sent| now.diff(sent) >= POLYMARKET_FACTS.heartbeat_period);
        match is_due {
            true => HeartbeatStep::Send {
                id: self.id.clone().unwrap_or_default(),
            },
            false => HeartbeatStep::Wait,
        }
    }

    /// The beat is on its way. Stamped only once it is: stamping the attempt would hold the next
    /// beat off for a whole period against the venue limit.
    pub fn on_sent(&mut self, now: LocalTs) {
        self.last_sent = Some(now);
    }

    /// The venue names the id of the next beat in a refusal as in a success, so either answer
    /// continues the chain.
    pub fn on_answer(&mut self, answer: &Result<ClobResponse, TransportError>) -> BeatAnswer {
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

    pub fn is_placing_held(&self) -> bool {
        false
    }
}
