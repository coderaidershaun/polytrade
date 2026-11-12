//! What the link bar shows at a given width, and the run button's two-press confirm. Both stay free
//! of egui so that native tests can check them; the painter only measures one character's width.

use std::net::SocketAddr;

use crate::link::RunState;
use crate::time::DurationUs;
use crate::workstation::link_model::LinkStatus;
use crate::workstation::text::{self, ChipWords, Tone};

/// The bar shows chips left to right in the order these variants are declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChipKind {
    Peer,
    Connection,
    Phase,
    Run,
    Control,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacedChip {
    pub kind: ChipKind,
    pub text: String,
    /// Counted in monospace character cells from the bar's left edge.
    pub column: usize,
    pub(crate) tone: Tone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkBarLayout {
    pub chips: Vec<PlacedChip>,
    /// The run button runs from this column to the bar's right edge.
    pub button_column: usize,
}

/// Most important first. A narrow bar shortens chips and then drops them from the end of this list,
/// so the peer is the last thing to give way and is never dropped.
const PRIORITY: [ChipKind; 5] = [
    ChipKind::Peer,
    ChipKind::Connection,
    ChipKind::Run,
    ChipKind::Control,
    ChipKind::Phase,
];

pub(crate) const LEFT_PAD: usize = 2;
const GAP: usize = 2;
const BUTTON_PAD: usize = 1;

/// Wide enough for the longer confirm label, so arming never moves a chip.
const BUTTON_COLUMNS: usize = {
    let stop = text::confirm_label(RunState::Idle).len();
    let start = text::confirm_label(RunState::Running).len();
    (if stop > start { stop } else { start }) + 2 * BUTTON_PAD
};

/// Step 0 shows every chip in full. Each later step shortens one more kind, lowest priority first,
/// until every chip is short. After that, each step drops one more kind, but never the peer.
const LAST_STEP: usize = 2 * PRIORITY.len() - 1;

impl LinkBarLayout {
    /// Picks the first step whose chips fit, so a narrower bar never shows more than a wider one.
    /// When no step fits, the peer alone is chosen and the painter clips it.
    pub fn fit(status: &LinkStatus, columns: usize) -> Self {
        let button_column = columns.saturating_sub(BUTTON_COLUMNS);
        let budget = button_column.saturating_sub(GAP + LEFT_PAD);
        let candidates = candidates(status);
        let step = (0..=LAST_STEP)
            .find(|&step| width_at(&candidates, step) <= budget)
            .unwrap_or(LAST_STEP);
        Self {
            chips: place(&candidates, step),
            button_column,
        }
    }
}

struct Candidate {
    kind: ChipKind,
    /// 0 for the first kind to be shortened and dropped.
    rank: usize,
    words: ChipWords,
}

impl Candidate {
    fn text_at(&self, step: usize) -> Option<&str> {
        let dropped_kinds = step.saturating_sub(PRIORITY.len());
        if self.rank < dropped_kinds {
            return None;
        }
        let is_short = self.rank < step;
        Some(if is_short { &self.words.short } else { &self.words.full })
    }
}

fn candidates(status: &LinkStatus) -> Vec<Candidate> {
    let mut candidates: Vec<Candidate> = PRIORITY
        .iter()
        .rev()
        .enumerate()
        .filter_map(|(rank, &kind)| {
            let words = words_of(kind, status)?;
            Some(Candidate { kind, rank, words })
        })
        .collect();
    candidates.sort_by_key(|candidate| candidate.kind as usize);
    candidates
}

fn words_of(kind: ChipKind, status: &LinkStatus) -> Option<ChipWords> {
    match kind {
        ChipKind::Peer => Some(text::peer_words(status)),
        ChipKind::Connection => Some(text::connection_words(status)),
        ChipKind::Phase => text::phase_words(status),
        ChipKind::Run => Some(text::run_words(status)),
        ChipKind::Control => Some(text::control_words(status)),
    }
}

fn width_at(candidates: &[Candidate], step: usize) -> usize {
    let with_trailing_gap: usize = candidates
        .iter()
        .filter_map(|candidate| candidate.text_at(step))
        .map(|text| text.chars().count() + GAP)
        .sum();
    with_trailing_gap.saturating_sub(GAP)
}

fn place(candidates: &[Candidate], step: usize) -> Vec<PlacedChip> {
    let mut chips = Vec::with_capacity(candidates.len());
    let mut column = LEFT_PAD;
    for candidate in candidates {
        let Some(text) = candidate.text_at(step) else { continue };
        chips.push(PlacedChip {
            kind: candidate.kind,
            text: text.to_owned(),
            column,
            tone: candidate.words.tone,
        });
        column += text.chars().count() + GAP;
    }
    chips
}

const CONFIRM_WINDOW: DurationUs = DurationUs::from_secs(3);

/// The run button's two-press confirm. An arming is forgotten by the first press or refresh that
/// finds its window closed or its target changed. Every `ui_time` is a reading of one monotonic
/// clock: the time elapsed since that clock started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct RunConfirm {
    armed: Option<Armed>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Armed {
    target: Target,
    armed_at: DurationUs,
}

/// What one press stands for: this assertion, on this engine, in this session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Target {
    state: RunState,
    peer: SocketAddr,
    session: u64,
}

impl Target {
    fn of(status: &LinkStatus) -> Self {
        Self {
            state: status.next_assertion(),
            peer: status.peer,
            session: status.session,
        }
    }
}

impl Armed {
    fn time_left(self, target: Target, ui_time: DurationUs) -> Option<DurationUs> {
        if self.target != target {
            return None;
        }
        let elapsed = ui_time.micros().saturating_sub(self.armed_at.micros());
        let window = CONFIRM_WINDOW.micros();
        (0..window)
            .contains(&elapsed)
            .then(|| DurationUs::from_micros(window - elapsed))
    }
}

impl RunConfirm {
    /// `Some(state)` only on a second press, inside the window, for the same target. Any other
    /// press arms and sends nothing.
    pub fn press(&mut self, status: &LinkStatus, ui_time: DurationUs) -> Option<RunState> {
        let target = Target::of(status);
        if self.refresh(status, ui_time).is_some() {
            self.armed = None;
            return Some(target.state);
        }
        self.armed = Some(Armed {
            target,
            armed_at: ui_time,
        });
        None
    }

    /// Returns the time left while the button should read CONFIRM. Call it on every frame, so that
    /// a status that changes and then changes back cannot bring an old arming back.
    pub(crate) fn refresh(
        &mut self,
        status: &LinkStatus,
        ui_time: DurationUs,
    ) -> Option<DurationUs> {
        let time_left = self
            .armed
            .and_then(|armed| armed.time_left(Target::of(status), ui_time));
        if time_left.is_none() {
            self.armed = None;
        }
        time_left
    }
}
