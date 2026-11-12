//! The connection strip's words: each chip in a full and a short form, and what the run button says
//! before and after it is armed.

use crate::link::{RunPhase, RunState};
use crate::workstation::format::MISSING;
use crate::workstation::link_model::{ConnectionState, ControlVerdict, LinkStatus};

use super::tone::Tone;

pub(crate) const NO_ENGINE: &str = "no engine attached";
pub(crate) const PEER_HOVER: &str = "attach to the next trading engine";

/// One chip's text. A short form is never longer than the full one, and equals it when the chip has
/// nothing to cut.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ChipWords {
    pub(crate) full: String,
    pub(crate) short: String,
    pub(crate) tone: Tone,
}

impl ChipWords {
    fn new(full: impl Into<String>, short: impl Into<String>, tone: Tone) -> Self {
        Self {
            full: full.into(),
            short: short.into(),
            tone,
        }
    }

    fn same(text: impl Into<String>, tone: Tone) -> Self {
        let text = text.into();
        Self::new(text.clone(), text, tone)
    }
}

pub(crate) const fn button_label(target: RunState) -> &'static str {
    match target {
        RunState::Idle => "STOP",
        RunState::Running => "START",
    }
}

pub(crate) const fn confirm_label(target: RunState) -> &'static str {
    match target {
        RunState::Idle => "CONFIRM STOP",
        RunState::Running => "CONFIRM START",
    }
}

pub(crate) fn is_peer_clickable(status: &LinkStatus) -> bool {
    status.peer_count >= 2
}

pub(crate) fn peer_words(status: &LinkStatus) -> ChipWords {
    let full = format!("engine {}", status.peer);
    let short = format!(":{}", status.peer.port());
    if !is_peer_clickable(status) {
        return ChipWords::new(full, short, Tone::Text);
    }
    let place = format!("[{}/{}]", status.peer_index + 1, status.peer_count);
    ChipWords::new(
        format!("{full} {place}"),
        format!("{short} {place}"),
        Tone::Text,
    )
}

pub(crate) fn connection_words(status: &LinkStatus) -> ChipWords {
    match status.connection {
        ConnectionState::Connecting => ChipWords::new("CONNECTING", "CONN", Tone::Text2),
        ConnectionState::Live => ChipWords::same("LIVE", Tone::Positive),
        ConnectionState::Stale => ChipWords::same("STALE", Tone::Stale),
    }
}

pub(crate) fn phase_words(status: &LinkStatus) -> Option<ChipWords> {
    let (word, tone) = match status.phase? {
        RunPhase::Starting => ("starting", Tone::Text2),
        RunPhase::Ready => ("ready", Tone::Text2),
        RunPhase::Draining => ("draining", Tone::Warning),
        RunPhase::Stopped => ("stopped", Tone::Invalid),
    };
    Some(ChipWords::same(word, tone))
}

pub(crate) fn run_words(status: &LinkStatus) -> ChipWords {
    let Some(state) = status.reported_state else {
        return ChipWords::same(format!("run {MISSING}"), Tone::Text2);
    };
    let word = state_word(state);
    let tone = match state {
        RunState::Running => Tone::Positive,
        RunState::Idle => Tone::Warning,
    };
    ChipWords::new(format!("run {word}"), word, tone)
}

pub(crate) fn control_words(status: &LinkStatus) -> ChipWords {
    let tone = match status.control {
        ControlVerdict::NoOpinion => Tone::Text2,
        ControlVerdict::Pending => Tone::Warning,
        ControlVerdict::Applied => Tone::Positive,
        ControlVerdict::Lost { .. } => Tone::Invalid,
    };
    match (status.control, status.asserted_state) {
        (ControlVerdict::Lost { holder_epoch }, _) => ChipWords::new(
            format!("control LOST to epoch {holder_epoch}"),
            "control LOST",
            tone,
        ),
        (ControlVerdict::NoOpinion, _) | (_, None) => ChipWords::same("control none", tone),
        (ControlVerdict::Applied, Some(state)) => control_verb_words("applied", state, tone),
        (ControlVerdict::Pending, Some(state)) => control_verb_words("asserting", state, tone),
    }
}

fn control_verb_words(verb: &str, state: RunState, tone: Tone) -> ChipWords {
    let short = format!("{verb} {}", state_word(state));
    ChipWords::new(format!("control {short}"), short, tone)
}

fn state_word(state: RunState) -> &'static str {
    match state {
        RunState::Running => "RUNNING",
        RunState::Idle => "IDLE",
    }
}
