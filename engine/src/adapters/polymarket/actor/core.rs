//! Pure driver core: two A/B slots, each an FSM + Up/Down legs. Schedules prefetch + subscribe,
//! routes frames by token, translates actions → effects. No sockets/REST/clock — replays identically.
//! Async shell performs effects and feeds results back.

use std::sync::Arc;

use crate::adapters::polymarket::discovery::{PolySchedule, Slot};
use crate::adapters::polymarket::parse::{
    PolyBook, PolyDelta, PolyFrame, PolyPriceChange, PolyTickSizeChange, PolyTrade,
};
use crate::adapters::polymarket::rest::GammaMarket;
use crate::adapters::polymarket::rotation::{
    ForceTeardownFacts, OutcomeLeg, ProbeOutcome, SlotAction, SlotInput, SlotMachine, TokenId,
    WindowAssignment, WindowTokens,
};
use crate::adapters::polymarket::shadow::BookMismatch;
use crate::ids::InstrumentId;
use crate::msg::inbound::{BookReset, InboundMessage, MarketRotation, TradeEvent};
use crate::msg::persist::RotationRow;
use crate::time::TsUs;

use super::leg::Leg;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotLegs {
    pub up: InstrumentId,
    pub down: InstrumentId,
}

/// Work shell must perform. Order matters: `Rotation → BookReset → Snapshot` per slot,
/// `Unsubscribe → BookReset` on teardown.
// `Emit` carries a whole `InboundMessage`; boxing it would allocate per book chunk on the
// adapter's steady path to shrink a Vec the shell drains immediately.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum DriverEffect {
    /// Idempotent resolve (shell de-dupes in-flight).
    Resolve(TsUs),
    Subscribe(WindowTokens),
    Unsubscribe(WindowTokens),
    /// Grace-tail `/book` 404 probe on both tokens.
    Probe(WindowTokens),
    /// Pushed to hot ring; shell stamps `queued_ts_us`.
    Emit(InboundMessage),
    /// Venue lineage → rotations side-channel (off hot ring).
    PersistRotation(RotationRow),
    /// Slot occupied at next subscribe — WARN + counted.
    ForcedTeardown(ForceTeardownFacts),
    /// Book diverged from shadow — WARN + counted. Carries cut point so WARN names cause.
    Diverged {
        instrument: InstrumentId,
        mismatch: BookMismatch,
    },
    /// `tick_size_change` frame; WARN records old→new shape.
    TickSizeChange(PolyTickSizeChange),
    /// The window an execution edge may now trade. Emitted at SUBSCRIBE rather than at resolve:
    /// a slot resolves its next window while its previous one is still live, and binding then
    /// would re-point the instrument the edge has orders resting on.
    BindExecution(Box<WindowAssignment>),
}

#[derive(Clone, Copy)]
enum LegSide {
    Up,
    Down,
}

struct SlotRuntime {
    machine: SlotMachine,
    up: Leg,
    down: Leg,
    /// Resolved window awaiting subscribe instant.
    resolved: Option<WindowAssignment>,
    /// Highest handled index — guards re-resolve/re-assign.
    handled_index: Option<i64>,
}

impl SlotRuntime {
    fn leg(&self, side: LegSide) -> &Leg {
        match side {
            LegSide::Up => &self.up,
            LegSide::Down => &self.down,
        }
    }

    fn leg_mut(&mut self, side: LegSide) -> &mut Leg {
        match side {
            LegSide::Up => &mut self.up,
            LegSide::Down => &mut self.down,
        }
    }
}

pub struct PolyDriverCore {
    schedule: PolySchedule,
    slots: [SlotRuntime; 2],
    force_teardowns: u64,
}

impl PolyDriverCore {
    pub fn new(slot_legs: [SlotLegs; 2], schedule: PolySchedule) -> Self {
        let slot = |legs: SlotLegs| SlotRuntime {
            machine: SlotMachine::new(schedule),
            up: Leg::new(legs.up),
            down: Leg::new(legs.down),
            resolved: None,
            handled_index: None,
        };
        Self {
            schedule,
            slots: [slot(slot_legs[0]), slot(slot_legs[1])],
            force_teardowns: 0,
        }
    }

    pub fn on_tick(&mut self, now: TsUs, emit: &mut dyn FnMut(DriverEffect)) {
        for slot in 0..2 {
            self.prefetch(slot, now, emit);
            self.try_assign(slot, now, emit);
            self.feed(slot, now, SlotInput::Tick, emit);
        }
    }

    pub fn on_frame(&mut self, now: TsUs, frame: &PolyFrame, emit: &mut dyn FnMut(DriverEffect)) {
        match frame {
            PolyFrame::Book(book) => self.on_book(now, book, emit),
            PolyFrame::PriceChange(change) => self.on_price_change(now, change, emit),
            PolyFrame::Trade(trade) => self.on_trade(now, trade, emit),
            PolyFrame::TickSizeChange(change) => emit(DriverEffect::TickSizeChange(change.clone())),
            PolyFrame::Batch(frames) => {
                for inner in frames {
                    self.on_frame(now, inner, emit);
                }
            }
            PolyFrame::Ignored => {}
        }
    }

    pub fn on_window_resolved(
        &mut self,
        now: TsUs,
        assignment: WindowAssignment,
        emit: &mut dyn FnMut(DriverEffect),
    ) {
        let index = self
            .schedule
            .window_index_containing(assignment.window_open_ts_us);
        let slot = Slot::from_window_index(index).as_usize();
        if self.slots[slot]
            .handled_index
            .is_some_and(|handled| index <= handled)
        {
            return;
        }
        self.slots[slot].resolved = Some(assignment);
        self.try_assign(slot, now, emit);
    }

    /// Pair a resolved market's tokens with the legs of the slot hosting the window at `start` —
    /// the driver's single leg/parity authority, so shell-side instrument picks can never disagree
    /// with the slot the core routes to. `start` is schedule-minted at every call site, so
    /// [`PolySchedule::window_at`]'s grid assert holds by construction.
    pub(super) fn assignment_from_market(
        &self,
        start: TsUs,
        market: &GammaMarket,
    ) -> WindowAssignment {
        let legs = &self.slots[self.schedule.window_at(start).slot.as_usize()];
        WindowAssignment {
            up: OutcomeLeg {
                instrument: legs.up.instrument(),
                token: TokenId::from(market.token_up.as_ref()),
            },
            down: OutcomeLeg {
                instrument: legs.down.instrument(),
                token: TokenId::from(market.token_down.as_ref()),
            },
            window_open_ts_us: market.window_open_ts_us,
            window_close_ts_us: market.window_close_ts_us,
            condition_id: Arc::from(market.condition_id.as_ref()),
        }
    }

    /// A `/book` probe returned: the FSM self-defends against a stale window, so feeding both slots is
    /// safe — only the one whose live window matches the probed tokens acts.
    pub fn on_probe_result(
        &mut self,
        now: TsUs,
        tokens: WindowTokens,
        outcome: ProbeOutcome,
        emit: &mut dyn FnMut(DriverEffect),
    ) {
        for slot in 0..2 {
            self.feed(
                slot,
                now,
                SlotInput::ProbeResult {
                    outcome,
                    tokens: tokens.clone(),
                },
                emit,
            );
        }
    }

    /// A fresh socket: every live leg re-baselines (the venue resends full books), so emit a BookReset
    /// per live instrument now and forward the coming book as a Snapshot.
    pub fn on_reconnect(&mut self, now: TsUs, emit: &mut dyn FnMut(DriverEffect)) {
        for slot in &mut self.slots {
            for side in [LegSide::Up, LegSide::Down] {
                let leg = slot.leg_mut(side);
                if !leg.is_live() {
                    continue;
                }
                leg.resubscribe();
                let instrument = leg.instrument();
                emit(DriverEffect::Emit(InboundMessage::BookReset(BookReset {
                    instrument,
                    received_ts_us: now,
                    queued_ts_us: now,
                })));
            }
        }
    }

    /// Every token currently subscribed — the plain-subscribe set the shell replays on each connect.
    pub fn live_tokens(&self) -> Vec<TokenId> {
        let mut tokens = Vec::new();
        for slot in &self.slots {
            for leg in [&slot.up, &slot.down] {
                if let Some(token) = leg.live_token() {
                    tokens.push(token.clone());
                }
            }
        }
        tokens
    }

    pub fn force_teardown_count(&self) -> u64 {
        self.force_teardowns
    }

    fn prefetch(&mut self, slot: usize, now: TsUs, emit: &mut dyn FnMut(DriverEffect)) {
        let target = self.target_index(slot, now);
        if self.slots[slot].handled_index == Some(target) {
            return;
        }
        let have = self.slots[slot]
            .resolved
            .as_ref()
            .is_some_and(|assignment| {
                self.schedule
                    .window_index_containing(assignment.window_open_ts_us)
                    == target
            });
        if !have {
            emit(DriverEffect::Resolve(self.schedule.window_start(target)));
        }
    }

    fn try_assign(&mut self, slot: usize, now: TsUs, emit: &mut dyn FnMut(DriverEffect)) {
        let Some(assignment) = self.slots[slot].resolved.as_ref() else {
            return;
        };
        let index = self
            .schedule
            .window_index_containing(assignment.window_open_ts_us);
        let subscribe_at = self.schedule.subscribe_at(assignment.window_open_ts_us);
        if self.slots[slot].handled_index == Some(index) || now < subscribe_at {
            return;
        }
        let assignment = self.slots[slot].resolved.take().expect("resolved present");
        self.slots[slot].handled_index = Some(index);
        self.feed(slot, now, SlotInput::Assign(assignment), emit);
    }

    /// The next window index destined for this slot: the nearest at-or-after `now` with the slot's
    /// parity, stepped one full slot period past a window already handed over.
    fn target_index(&self, slot: usize, now: TsUs) -> i64 {
        let current = self.schedule.current_window(now);
        let base = if current.slot.as_usize() == slot { current.index } else { current.index + 1 };
        if self.slots[slot].handled_index == Some(base) { base + 2 } else { base }
    }

    fn feed(
        &mut self,
        slot: usize,
        now: TsUs,
        input: SlotInput,
        emit: &mut dyn FnMut(DriverEffect),
    ) {
        let mut actions = Vec::new();
        self.slots[slot]
            .machine
            .on_input(now, input, &mut |action| actions.push(action));
        for action in actions {
            self.exec_action(slot, now, action, emit);
        }
    }

    fn exec_action(
        &mut self,
        slot: usize,
        now: TsUs,
        action: SlotAction,
        emit: &mut dyn FnMut(DriverEffect),
    ) {
        match action {
            SlotAction::Subscribe(assignment) => {
                let tokens = assignment.tokens();
                emit(DriverEffect::BindExecution(Box::new(assignment.clone())));
                self.slots[slot].up.assign(assignment.up.token, now);
                self.slots[slot].down.assign(assignment.down.token, now);
                emit(DriverEffect::Subscribe(tokens));
            }
            SlotAction::Rotation(facts) => {
                emit(DriverEffect::Emit(InboundMessage::MarketRotation(
                    MarketRotation {
                        instrument: facts.instrument,
                        window_open_ts_us: facts.window_open_ts_us,
                        window_close_ts_us: facts.window_close_ts_us,
                        received_ts_us: now,
                        queued_ts_us: now,
                    },
                )));
                emit(DriverEffect::PersistRotation(RotationRow {
                    instrument: facts.instrument,
                    window_open_ts_us: facts.window_open_ts_us,
                    window_close_ts_us: facts.window_close_ts_us,
                    token_id_up: facts.tokens.up.as_str().into(),
                    token_id_down: facts.tokens.down.as_str().into(),
                    condition_id: facts.condition_id.as_ref().into(),
                    received_ts_us: now,
                }));
            }
            SlotAction::BookReset(instrument) => {
                emit(DriverEffect::Emit(InboundMessage::BookReset(BookReset {
                    instrument,
                    received_ts_us: now,
                    queued_ts_us: now,
                })));
            }
            SlotAction::Unsubscribe(tokens) => {
                self.slots[slot].up.clear();
                self.slots[slot].down.clear();
                emit(DriverEffect::Unsubscribe(tokens));
            }
            SlotAction::Probe(tokens) => emit(DriverEffect::Probe(tokens)),
            SlotAction::ForceTeardown(facts) => {
                self.force_teardowns += 1;
                emit(DriverEffect::ForcedTeardown(facts));
            }
        }
    }

    fn on_book(&mut self, now: TsUs, book: &PolyBook, emit: &mut dyn FnMut(DriverEffect)) {
        let Some((slot, side)) = self.route(&book.asset_id) else {
            return;
        };
        self.feed(slot, now, SlotInput::Frame, emit);
        let diverged = self.slots[slot]
            .leg_mut(side)
            .on_venue_book(book, now, &mut |message| emit(DriverEffect::Emit(message)));
        if let Some(mismatch) = diverged {
            let instrument = self.slots[slot].leg(side).instrument();
            emit(DriverEffect::Diverged {
                instrument,
                mismatch,
            });
        }
    }

    fn on_price_change(
        &mut self,
        now: TsUs,
        change: &PolyPriceChange,
        emit: &mut dyn FnMut(DriverEffect),
    ) {
        for slot in 0..2 {
            let mut framed = false;
            let mut collapsed = false;
            for side in [LegSide::Up, LegSide::Down] {
                let deltas: Vec<&PolyDelta> = change
                    .changes
                    .iter()
                    .filter(|delta| self.slots[slot].leg(side).token_matches(&delta.asset_id))
                    .collect();
                if deltas.is_empty() {
                    continue;
                }
                if !framed {
                    self.feed(slot, now, SlotInput::Frame, emit);
                    framed = true;
                }
                let leg_collapsed = self.slots[slot].leg_mut(side).on_deltas(
                    &deltas,
                    now,
                    change.exchange_ts_us,
                    &mut |message| emit(DriverEffect::Emit(message)),
                );
                collapsed = collapsed || leg_collapsed;
            }
            if collapsed {
                self.feed(slot, now, SlotInput::Collapsed, emit);
            }
        }
    }

    fn on_trade(&mut self, now: TsUs, trade: &PolyTrade, emit: &mut dyn FnMut(DriverEffect)) {
        let Some((slot, side)) = self.route(&trade.asset_id) else {
            return;
        };
        self.feed(slot, now, SlotInput::Frame, emit);
        let instrument = self.slots[slot].leg(side).instrument();
        emit(DriverEffect::Emit(InboundMessage::Trade(TradeEvent {
            instrument,
            price: trade.price,
            qty: trade.qty,
            side: trade.side,
            exchange_ts_us: trade.exchange_ts_us,
            exchange_sent_ts_us: None,
            received_ts_us: trade.received_ts_us,
            queued_ts_us: trade.received_ts_us,
        })));
    }

    fn route(&self, asset_id: &str) -> Option<(usize, LegSide)> {
        for (slot, state) in self.slots.iter().enumerate() {
            if state.up.token_matches(asset_id) {
                return Some((slot, LegSide::Up));
            }
            if state.down.token_matches(asset_id) {
                return Some((slot, LegSide::Down));
            }
        }
        None
    }
}
