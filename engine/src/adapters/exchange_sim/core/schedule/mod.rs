//! Delivery ordering for simulated venue answers.

mod transition;
mod voice;

use crate::ids::{ClientOrderId, Qty};
use crate::time::{DurationUs, TsUs};

use super::latency::shifted;
use super::resting::OrderSnapshot;
use super::{ForcedOrderExit, SimEmission, VenueEvent};

use voice::TerminalHalf;
pub use voice::{
    AnswerSubject, DueAnswer, Rejection, SynthesisedEvent, VenueAnswer, VenueReport, VenueVoice,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GenerationKey {
    pub client_id: ClientOrderId,
    pub generation: u32,
}

impl GenerationKey {
    fn of(snapshot: OrderSnapshot) -> Self {
        Self {
            client_id: snapshot.order.client_id,
            generation: snapshot.generation,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeliveryLimits {
    pub ack_latency: DurationUs,
    pub answer_capacity: usize,
}

const fn rank_of(kind: AnswerKind) -> u8 {
    match kind {
        AnswerKind::PlaceAck => 0,
        AnswerKind::ReportNew => 1,
        AnswerKind::Refusal
        | AnswerKind::NotSent
        | AnswerKind::Transition
        | AnswerKind::Observation => 2,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AnswerKind {
    PlaceAck,
    ReportNew,
    Refusal,
    NotSent,
    Transition,
    Observation,
}

struct Scheduled {
    key: Option<GenerationKey>,
    kind: AnswerKind,
    earned_ts_us: TsUs,
    due_ts_us: TsUs,
    sequence: u64,
    voice: VenueVoice,
}

struct Spoken {
    key: GenerationKey,
    has_said_new: bool,
    has_said_terminal_ack: bool,
    has_said_terminal_report: bool,
    cumulative_qty: Qty,
    new_barrier_ts_us: Option<TsUs>,
}

pub struct DeliverySchedule {
    pending: Vec<Scheduled>,
    spoken: Vec<Spoken>,
    limits: DeliveryLimits,
    spoken_through_ts_us: TsUs,
}

impl DeliverySchedule {
    pub fn new(limits: DeliveryLimits) -> Self {
        Self {
            pending: Vec::with_capacity(limits.answer_capacity),
            spoken: Vec::new(),
            limits,
            spoken_through_ts_us: TsUs::from_micros(i64::MIN),
        }
    }

    pub fn accept(&mut self, emission: &SimEmission) {
        let due_ts_us = shifted(emission.at_ts_us, [self.limits.ack_latency]);
        let at = Landing {
            earned_ts_us: emission.at_ts_us,
            due_ts_us,
            sequence: emission.sequence,
        };
        match emission.event {
            VenueEvent::Rested { snapshot, .. } => self.announce(snapshot, at),
            VenueEvent::PostOnlyCrossed { snapshot } => {
                self.refuse_place(snapshot, Rejection::WouldMatchImmediately, at)
            }
            VenueEvent::PlaceRefused { snapshot, reason } => {
                self.refuse_place(snapshot, Rejection::of(reason), at)
            }
            VenueEvent::Filled {
                snapshot,
                trade_id,
                settlement,
                ..
            } => self.fill(snapshot, settlement, trade_id, at),
            VenueEvent::Canceled { snapshot } => self.cancel(snapshot, at),
            VenueEvent::Amended { snapshot, .. } => self.amend(snapshot, at),
            VenueEvent::CancelRefused { client_id, reason } => {
                self.refuse(client_id, Rejection::of(reason), AnswerSubject::Cancel, at)
            }
            VenueEvent::AmendRefused { client_id, reason } => {
                self.refuse(client_id, Rejection::of(reason), AnswerSubject::Amend, at)
            }
            VenueEvent::OrderStatus { snapshot } => self.status(snapshot, at),
            VenueEvent::NoSuchOrder { client_id } => self.no_such_order(client_id, at),
            VenueEvent::OpenOrders { ref rows } => self.snapshot(rows, at),
            VenueEvent::StreamSubscribed => {
                self.observe(SynthesisedEvent::StreamSubscribed, at);
            }
            VenueEvent::MarketReset { .. } => self.observe(SynthesisedEvent::StreamReset, at),
        }
    }

    pub fn advance_to(&mut self, horizon: TsUs, due: &mut Vec<DueAnswer>) {
        due.clear();
        while let Some(index) = self.next_speakable(horizon) {
            let entry = self.pending.remove(index);
            self.speak(entry, due);
        }
    }

    pub fn force_sweep(&mut self, at_ts_us: TsUs, exited: &[ForcedOrderExit]) {
        for entry in &mut self.pending {
            entry.due_ts_us = entry.due_ts_us.min(at_ts_us);
        }
        for record in &mut self.spoken {
            record.new_barrier_ts_us = record.new_barrier_ts_us.map(|due| due.min(at_ts_us));
        }
        for exit in exited {
            let at = Landing::at(at_ts_us);
            match exit.was_pending {
                true => self.withdraw(exit.snapshot, at),
                false => self.pull(exit.snapshot, at),
            }
        }
    }

    pub fn owes_nothing(&self) -> bool {
        self.pending.is_empty()
    }

    fn open_barrier(&mut self, key: GenerationKey, due_ts_us: TsUs) {
        self.ensure_spoken(key).new_barrier_ts_us = Some(due_ts_us);
    }

    fn push(
        &mut self,
        key: Option<GenerationKey>,
        kind: AnswerKind,
        at: Landing,
        voice: VenueVoice,
    ) {
        assert!(
            self.pending.len() < self.limits.answer_capacity,
            "the simulated venue owes {} answers and cannot buffer another",
            self.pending.len()
        );
        if let Some(key) = key {
            self.ensure_spoken(key);
        }
        self.pending.push(Scheduled {
            key,
            kind,
            earned_ts_us: at.earned_ts_us,
            due_ts_us: at.due_ts_us,
            sequence: at.sequence,
            voice,
        });
    }

    fn ensure_spoken(&mut self, key: GenerationKey) -> &mut Spoken {
        let at = match self.spoken.iter().position(|spoken| spoken.key == key) {
            Some(at) => at,
            None => {
                self.spoken.push(Spoken::new(key));
                self.spoken.len() - 1
            }
        };
        &mut self.spoken[at]
    }

    fn next_speakable(&self, horizon: TsUs) -> Option<usize> {
        self.pending
            .iter()
            .enumerate()
            .filter(|(_, entry)| self.release_of(entry) <= horizon)
            .min_by_key(|(_, entry)| {
                (
                    self.release_of(entry),
                    rank_of(entry.kind),
                    entry.earned_ts_us,
                    entry.sequence,
                )
            })
            .map(|(index, _)| index)
    }

    fn release_of(&self, entry: &Scheduled) -> TsUs {
        if matches!(entry.kind, AnswerKind::PlaceAck | AnswerKind::ReportNew) {
            return entry.due_ts_us;
        }
        let barrier = entry
            .key
            .and_then(|key| self.spoken_of(key))
            .and_then(|spoken| spoken.new_barrier_ts_us);
        match barrier {
            Some(barrier) => entry.due_ts_us.max(barrier),
            None => entry.due_ts_us,
        }
    }

    fn speak(&mut self, entry: Scheduled, due: &mut Vec<DueAnswer>) {
        let at = self.release_of(&entry).max(self.spoken_through_ts_us);
        self.spoken_through_ts_us = at;
        if let Some(key) = entry.key {
            let spoken = self
                .spoken_mut(key)
                .expect("a scheduled entry outlived its generation");
            spoken.record(entry.kind, &entry.voice);
        }
        due.push(DueAnswer {
            event_ts_us: entry.earned_ts_us,
            due_ts_us: at,
            voice: entry.voice,
        });
    }

    fn spoken_of(&self, key: GenerationKey) -> Option<&Spoken> {
        self.spoken.iter().find(|spoken| spoken.key == key)
    }

    fn spoken_mut(&mut self, key: GenerationKey) -> Option<&mut Spoken> {
        self.spoken.iter_mut().find(|spoken| spoken.key == key)
    }
}

impl Spoken {
    fn new(key: GenerationKey) -> Self {
        Self {
            key,
            has_said_new: false,
            has_said_terminal_ack: false,
            has_said_terminal_report: false,
            cumulative_qty: Qty(0),
            new_barrier_ts_us: None,
        }
    }

    fn record(&mut self, kind: AnswerKind, voice: &VenueVoice) {
        assert!(
            !self.has_said_terminal_report,
            "the simulated venue spoke about {:?} after its terminal report: {kind:?}",
            self.key
        );
        if let Some(cumulative) = voice.cumulative_qty() {
            assert!(
                cumulative.0 >= self.cumulative_qty.0,
                "cumulative quantity on {:?} went backwards, {} then {}",
                self.key,
                self.cumulative_qty.0,
                cumulative.0
            );
            self.cumulative_qty = cumulative;
        }
        self.record_kind(kind);
        match voice.terminal_half() {
            Some(TerminalHalf::Ack) => {
                assert!(
                    !self.has_said_terminal_ack,
                    "a second terminal transition for {:?}",
                    self.key
                );
                self.has_said_terminal_ack = true;
            }
            Some(TerminalHalf::Report) => self.has_said_terminal_report = true,
            None => {}
        }
    }

    fn record_kind(&mut self, kind: AnswerKind) {
        match kind {
            AnswerKind::ReportNew => {
                assert!(
                    !self.has_said_new,
                    "a second NEW for {:?} would open a second hot slot",
                    self.key
                );
                self.has_said_new = true;
                self.new_barrier_ts_us = None;
            }
            AnswerKind::Transition => assert!(
                self.has_said_new,
                "the simulated venue changed {:?} before saying it existed",
                self.key
            ),
            AnswerKind::Refusal | AnswerKind::NotSent => assert!(
                !self.has_said_new,
                "{:?} rested and then took the never-rested exit, so the engine heard two \
                 lifecycles for one order",
                self.key
            ),
            AnswerKind::PlaceAck | AnswerKind::Observation => {}
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Landing {
    earned_ts_us: TsUs,
    due_ts_us: TsUs,
    sequence: u64,
}

impl Landing {
    const fn at(ts_us: TsUs) -> Self {
        Self {
            earned_ts_us: ts_us,
            due_ts_us: ts_us,
            sequence: 0,
        }
    }
}
