//! Per-slot lifecycle FSM: `Idle → Prefetched → Subscribed → Active → Grace →
//! TornDown → Idle`. Consumes ticks, gamma assignments, frame liveness, collapse signals and `/book`
//! probe results; emits subscribe/unsubscribe ops, the rotation + reset messages, probe requests and
//! force-teardown facts. Pure: every input carries `now`, the machine never reads a clock.
//!
//! A slot hosts BOTH outcome instruments (up + down) of one window — the registry registers
//! `{a,b}×{up,down}` and `MarketRotation`/`BookReset` are per-instrument (msg.rs), so every handover
//! and teardown fans out to both legs or a sibling instrument is silently left stale.
//!
//! Teardown never depends on witnessing the collapse burst — a reconnect that missed it still tears
//! down via the grace `/book` 404 probe, so a slot cannot wedge. Gamma/CLOB status
//! flags are never consulted: only frame evidence, 404 probes and the wall clock decide.

use std::sync::Arc;

use crate::ids::InstrumentId;
use crate::time::{DurationUs, TsUs};

use super::discovery::PolySchedule;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenId(Arc<str>);

impl TokenId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for TokenId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for TokenId {
    fn from(value: &str) -> Self {
        TokenId(Arc::from(value))
    }
}

impl From<String> for TokenId {
    fn from(value: String) -> Self {
        TokenId(Arc::from(value.as_str()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowTokens {
    pub up: TokenId,
    pub down: TokenId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OutcomeLeg {
    pub instrument: InstrumentId,
    pub token: TokenId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowAssignment {
    pub up: OutcomeLeg,
    pub down: OutcomeLeg,
    pub window_open_ts_us: TsUs,
    pub window_close_ts_us: TsUs,
    pub condition_id: Arc<str>,
}

impl WindowAssignment {
    pub fn tokens(&self) -> WindowTokens {
        WindowTokens {
            up: self.up.token.clone(),
            down: self.down.token.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProbeOutcome {
    BookExists,
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotInput {
    Tick,
    Assign(WindowAssignment),
    Frame,
    Collapsed,
    ProbeResult {
        outcome: ProbeOutcome,
        tokens: WindowTokens,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RotationFacts {
    pub instrument: InstrumentId,
    pub window_open_ts_us: TsUs,
    pub window_close_ts_us: TsUs,
    pub tokens: WindowTokens,
    pub condition_id: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ForceTeardownFacts {
    pub up_instrument: InstrumentId,
    pub down_instrument: InstrumentId,
    pub window_open_ts_us: TsUs,
    pub grace_age: DurationUs,
    pub tokens: WindowTokens,
}

/// Ordering: per-instrument `Rotation → BookReset → Snapshot`; teardown is `Unsubscribe → BookReset`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotAction {
    Subscribe(WindowAssignment),
    Unsubscribe(WindowTokens),
    Rotation(RotationFacts),
    BookReset(InstrumentId),
    Probe(WindowTokens),
    ForceTeardown(ForceTeardownFacts),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotState {
    Idle,
    Prefetched,
    Subscribed,
    Active,
    Grace,
    TornDown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveWindow {
    assignment: WindowAssignment,
    last_frame_ts_us: TsUs,
    next_probe_ts_us: TsUs,
    collapse_seen: bool,
}

/// The machine's own state. The four occupied phases carry the window they are about, so a phase
/// that needs one cannot be entered without it; [`SlotState`] is the same phase without the data,
/// which is all a caller ever wants to name.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Phase {
    Idle,
    Prefetched(LiveWindow),
    Subscribed(LiveWindow),
    Active(LiveWindow),
    Grace(LiveWindow),
    TornDown,
}

impl Phase {
    fn state(&self) -> SlotState {
        match self {
            Phase::Idle => SlotState::Idle,
            Phase::Prefetched(_) => SlotState::Prefetched,
            Phase::Subscribed(_) => SlotState::Subscribed,
            Phase::Active(_) => SlotState::Active,
            Phase::Grace(_) => SlotState::Grace,
            Phase::TornDown => SlotState::TornDown,
        }
    }

    fn window(&self) -> Option<&LiveWindow> {
        match self {
            Phase::Prefetched(window)
            | Phase::Subscribed(window)
            | Phase::Active(window)
            | Phase::Grace(window) => Some(window),
            Phase::Idle | Phase::TornDown => None,
        }
    }

    fn window_mut(&mut self) -> Option<&mut LiveWindow> {
        match self {
            Phase::Prefetched(window)
            | Phase::Subscribed(window)
            | Phase::Active(window)
            | Phase::Grace(window) => Some(window),
            Phase::Idle | Phase::TornDown => None,
        }
    }
}

enum GraceAction {
    Teardown,
    Probe(WindowTokens),
    Wait,
}

/// Deterministic: identical inputs → identical state (fitness-verified).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotMachine {
    schedule: PolySchedule,
    phase: Phase,
}

impl SlotMachine {
    pub fn new(schedule: PolySchedule) -> Self {
        Self {
            schedule,
            phase: Phase::Idle,
        }
    }

    pub fn state(&self) -> SlotState {
        self.phase.state()
    }

    pub fn on_input(&mut self, now: TsUs, input: SlotInput, emit: &mut impl FnMut(SlotAction)) {
        if self.phase == Phase::TornDown {
            self.phase = Phase::Idle;
        }
        match input {
            SlotInput::Tick => {}
            SlotInput::Assign(assignment) => self.on_assign(now, assignment, emit),
            SlotInput::Frame => self.on_frame(now),
            SlotInput::Collapsed => self.on_collapsed(),
            SlotInput::ProbeResult { outcome, tokens } => self.on_probe(outcome, &tokens, emit),
        }
        self.advance(now, emit);
    }

    fn on_assign(
        &mut self,
        now: TsUs,
        assignment: WindowAssignment,
        emit: &mut impl FnMut(SlotAction),
    ) {
        if self.is_occupied() {
            self.force_teardown(now, emit);
        }
        self.phase = Phase::Prefetched(LiveWindow {
            assignment,
            last_frame_ts_us: now,
            next_probe_ts_us: now,
            collapse_seen: false,
        });
    }

    /// Frame → book alive; disproves collapse (collapse = silence).
    fn on_frame(&mut self, now: TsUs) {
        if let Some(window) = self.phase.window_mut() {
            window.last_frame_ts_us = now;
            window.collapse_seen = false;
        }
    }

    /// Burst = resolution only in grace tail; mid-window is a blip.
    fn on_collapsed(&mut self) {
        if let Phase::Grace(window) = &mut self.phase {
            window.collapse_seen = true;
        }
    }

    fn on_probe(
        &mut self,
        outcome: ProbeOutcome,
        tokens: &WindowTokens,
        emit: &mut impl FnMut(SlotAction),
    ) {
        if outcome != ProbeOutcome::NotFound {
            return;
        }
        let Phase::Grace(window) = &self.phase else {
            return;
        };
        if &window.assignment.tokens() == tokens {
            self.teardown(emit);
        }
    }

    fn advance(&mut self, now: TsUs, emit: &mut impl FnMut(SlotAction)) {
        while self.step(now, emit) {}
        if matches!(self.phase, Phase::Grace(_)) {
            self.service_grace(now, emit);
        }
    }

    /// One phase transition, or `false` when the schedule says this phase is not due to move.
    fn step(&mut self, now: TsUs, emit: &mut impl FnMut(SlotAction)) -> bool {
        let schedule = self.schedule;
        let is_due = match &self.phase {
            Phase::Prefetched(window) => {
                now >= schedule.subscribe_at(window.assignment.window_open_ts_us)
            }
            Phase::Subscribed(window) => now >= window.assignment.window_open_ts_us,
            Phase::Active(window) => now >= window.assignment.window_close_ts_us,
            Phase::Idle | Phase::Grace(_) | Phase::TornDown => false,
        };
        if !is_due {
            return false;
        }
        self.phase = match std::mem::replace(&mut self.phase, Phase::Idle) {
            Phase::Prefetched(window) => {
                emit_subscribe(&window.assignment, emit);
                Phase::Subscribed(window)
            }
            Phase::Subscribed(window) => Phase::Active(window),
            Phase::Active(mut window) => {
                // Rate-limit starts at nominal end to allow silence-triggered probes earlier.
                window.next_probe_ts_us = window.assignment.window_close_ts_us;
                Phase::Grace(window)
            }
            phase => phase,
        };
        true
    }

    fn service_grace(&mut self, now: TsUs, emit: &mut impl FnMut(SlotAction)) {
        let schedule = self.schedule;
        let decision = match &mut self.phase {
            Phase::Grace(window) => grace_decision(window, now, schedule),
            _ => GraceAction::Wait,
        };
        match decision {
            GraceAction::Teardown => self.teardown(emit),
            GraceAction::Probe(tokens) => emit(SlotAction::Probe(tokens)),
            GraceAction::Wait => {}
        }
    }

    /// Empties the slot. A slot with no window has nothing to tear down — unreachable, because
    /// every caller has just read the window it is acting on.
    fn teardown(&mut self, emit: &mut impl FnMut(SlotAction)) {
        let Some(window) = self.phase.window() else {
            return;
        };
        emit(SlotAction::Unsubscribe(window.assignment.tokens()));
        emit(SlotAction::BookReset(window.assignment.up.instrument));
        emit(SlotAction::BookReset(window.assignment.down.instrument));
        self.phase = Phase::TornDown;
    }

    fn force_teardown(&mut self, now: TsUs, emit: &mut impl FnMut(SlotAction)) {
        let Some(window) = self.phase.window() else {
            return;
        };
        let facts = ForceTeardownFacts {
            up_instrument: window.assignment.up.instrument,
            down_instrument: window.assignment.down.instrument,
            window_open_ts_us: window.assignment.window_open_ts_us,
            grace_age: now.diff(window.assignment.window_close_ts_us),
            tokens: window.assignment.tokens(),
        };
        self.teardown(emit);
        emit(SlotAction::ForceTeardown(facts));
    }

    fn is_occupied(&self) -> bool {
        matches!(
            self.phase,
            Phase::Subscribed(_) | Phase::Active(_) | Phase::Grace(_)
        )
    }
}

fn emit_subscribe(assignment: &WindowAssignment, emit: &mut impl FnMut(SlotAction)) {
    let tokens = assignment.tokens();
    emit(SlotAction::Subscribe(assignment.clone()));
    for instrument in [assignment.up.instrument, assignment.down.instrument] {
        emit(SlotAction::Rotation(RotationFacts {
            instrument,
            window_open_ts_us: assignment.window_open_ts_us,
            window_close_ts_us: assignment.window_close_ts_us,
            tokens: tokens.clone(),
            condition_id: Arc::clone(&assignment.condition_id),
        }));
        emit(SlotAction::BookReset(instrument));
    }
}

fn grace_decision(window: &mut LiveWindow, now: TsUs, schedule: PolySchedule) -> GraceAction {
    let silent = now.diff(window.last_frame_ts_us) > schedule.silence_threshold;
    if window.collapse_seen && silent {
        return GraceAction::Teardown;
    }
    // Probe after nominal_end+delay OR on silence; reconnect still confirms via 404.
    let armed = now >= schedule.grace_probe_start(window.assignment.window_close_ts_us);
    if !(armed || silent) || now < window.next_probe_ts_us {
        return GraceAction::Wait;
    }
    window.next_probe_ts_us = now + schedule.probe_cadence;
    GraceAction::Probe(window.assignment.tokens())
}
