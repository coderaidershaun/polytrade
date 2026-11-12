//! Readiness proofs for starting and recovering simulated execution.

use super::lanes::SimLane;
use crate::time::TsUs;

const PROOF_COUNT: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadinessProof {
    DepthBridged,
    BookSnapshotComplete,
    CommandLaneAdvancing,
    TradeLaneAdvancing,
    DepthLaneAdvancing,
}

impl ReadinessProof {
    const fn of_lane(lane: SimLane) -> ReadinessProof {
        match lane {
            SimLane::Command => ReadinessProof::CommandLaneAdvancing,
            SimLane::Trade => ReadinessProof::TradeLaneAdvancing,
            SimLane::Depth => ReadinessProof::DepthLaneAdvancing,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimReadiness {
    held: [bool; PROOF_COUNT],
    armed_at: [TsUs; SimLane::ALL.len()],
    latest_lane: [TsUs; SimLane::ALL.len()],
    has_announced: bool,
}

impl SimReadiness {
    pub fn unseeded() -> Self {
        let unseeded = TsUs::from_micros(i64::MIN);
        Self {
            held: [false; PROOF_COUNT],
            armed_at: [unseeded; SimLane::ALL.len()],
            latest_lane: [unseeded; SimLane::ALL.len()],
            has_announced: false,
        }
    }

    pub fn prove(&mut self, proof: ReadinessProof) {
        self.held[proof as usize] = true;
    }

    pub fn observe_lane(&mut self, lane: SimLane, watermark: TsUs) {
        let latest = &mut self.latest_lane[lane as usize];
        *latest = (*latest).max(watermark);
        if watermark > self.armed_at[lane as usize] {
            self.prove(ReadinessProof::of_lane(lane));
        }
    }

    pub fn withdraw(&mut self) {
        self.held = [false; PROOF_COUNT];
        self.armed_at = self.latest_lane;
        self.has_announced = false;
    }

    fn is_complete(&self) -> bool {
        self.held.iter().all(|held| *held)
    }

    pub fn take_announcement(&mut self) -> bool {
        if self.has_announced || !self.is_complete() {
            return false;
        }
        self.has_announced = true;
        true
    }
}
