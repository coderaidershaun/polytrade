//! The workstation's view of the engine it is attached to: which run its frames come from, whether
//! the heartbeat is still arriving, what run state the engine reports against the one this
//! workstation asserts, and the run catalog rebuilt from the loose per-item frames the link
//! announces. No socket and no clock of its own, so the link session and the link bar read one
//! source of truth rather than two.

use std::net::SocketAddr;

use crate::link::{
    CatalogFeature, CatalogInstrument, Envelope, LINK_SUBSCRIPTION_TTL, Lifecycle, LinkHash,
    RunPhase, RunState,
};
use crate::msg::ui::{UiCatalog, UiInstrument};
use crate::shutdown::RunAssertion;
use crate::time::{DurationUs, TsUs};

/// Silence past which a peer's feed reads as stale rather than merely quiet. The engine announces
/// its lifecycle once a second, and this matches the far side's own subscription TTL — so "I have
/// stopped hearing the engine" and "the engine has forgotten me" surface at the same moment.
pub(crate) const PEER_SILENCE_LIMIT: DurationUs = LINK_SUBSCRIPTION_TTL;

/// One configured socket serves one trading engine run. The envelope names that run on every
/// topic, so it is the earliest authoritative reset signal — waiting for the first catalog frame
/// would let a new run's events fold into the previous run's working-order model in the meantime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RemoteRun {
    sender_te_hash: LinkHash,
    boot_ts_us: TsUs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RunObservation {
    First,
    Same,
    Restarted,
    Stale,
}

#[derive(Debug, Default)]
pub(super) struct RemoteRunGate {
    current: Option<RemoteRun>,
}

impl RemoteRunGate {
    pub(super) fn observe(&mut self, envelope: &Envelope) -> RunObservation {
        let incoming = RemoteRun {
            sender_te_hash: envelope.sender_te_hash,
            boot_ts_us: envelope.boot_ts_us,
        };
        let Some(current) = self.current else {
            self.current = Some(incoming);
            return RunObservation::First;
        };
        if incoming.boot_ts_us < current.boot_ts_us {
            return RunObservation::Stale;
        }
        if incoming == current {
            return RunObservation::Same;
        }
        self.current = Some(incoming);
        RunObservation::Restarted
    }

    pub(super) fn clear(&mut self) {
        self.current = None;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionState {
    /// Subscribed, nothing heard back. A trading engine that is not running looks exactly like this.
    Connecting,
    Live,
    /// Heard once, then nothing for [`PEER_SILENCE_LIMIT`]. UDP silence is otherwise
    /// indistinguishable from an engine with nothing to say, which is the whole reason this state
    /// is on screen.
    Stale,
}

impl ConnectionState {
    pub(crate) fn from_silence(since_last_frame: Option<DurationUs>) -> Self {
        match since_last_frame {
            None => Self::Connecting,
            Some(silence) if silence >= PEER_SILENCE_LIMIT => Self::Stale,
            Some(_) => Self::Live,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlVerdict {
    NoOpinion,
    Pending,
    Applied,
    Lost { holder_epoch: u64 },
}

#[derive(Debug)]
pub(crate) struct Controller {
    boot_epoch: u64,
    assertions: u64,
    asserted_state: Option<RunState>,
}

impl Controller {
    pub(crate) fn new(boot_ts_us: TsUs) -> Self {
        Self {
            boot_epoch: boot_ts_us.micros().max(0) as u64,
            assertions: 0,
            asserted_state: None,
        }
    }

    pub(crate) fn assert(&mut self, state: RunState) {
        self.assertions += 1;
        self.asserted_state = Some(state);
    }

    pub(crate) fn release(&mut self) {
        self.asserted_state = None;
    }

    pub(crate) fn assertion(&self) -> RunAssertion {
        match self.asserted_state {
            None => RunAssertion::INITIAL,
            Some(state) => RunAssertion {
                state,
                epoch: self.boot_epoch.saturating_add(self.assertions),
            },
        }
    }

    pub(crate) fn asserted(&self) -> Option<RunState> {
        self.asserted_state
    }

    pub(crate) fn verdict(&self, reported: Option<Lifecycle>) -> ControlVerdict {
        if self.asserted_state.is_none() {
            return ControlVerdict::NoOpinion;
        }
        let Some(reported) = reported else {
            return ControlVerdict::Pending;
        };
        let mine = self.assertion().epoch;
        if reported.acknowledged_epoch > mine {
            return ControlVerdict::Lost {
                holder_epoch: reported.acknowledged_epoch,
            };
        }
        let is_applied =
            reported.acknowledged_epoch == mine && self.asserted_state == Some(reported.run_state);
        if is_applied { ControlVerdict::Applied } else { ControlVerdict::Pending }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkStatus {
    pub peer: SocketAddr,
    pub peer_index: usize,
    pub peer_count: usize,
    pub session: u64,
    pub connection: ConnectionState,
    pub phase: Option<RunPhase>,
    pub reported_state: Option<RunState>,
    pub asserted_state: Option<RunState>,
    pub control: ControlVerdict,
}

impl LinkStatus {
    pub(crate) fn next_assertion(&self) -> RunState {
        match self.asserted_state.or(self.reported_state) {
            Some(RunState::Idle) => RunState::Running,
            Some(RunState::Running) | None => RunState::Idle,
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct CatalogAssembly {
    catalog_ts_us: Option<TsUs>,
    instruments: Vec<CatalogInstrument>,
    features: Vec<CatalogFeature>,
    instrument_total: Option<u16>,
}

impl CatalogAssembly {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// When the engine stamped the catalog these frames belong to. Not the control epoch: that is
    /// an assertion ordinal, and comparing the two numbers would mean nothing.
    pub(crate) fn catalog_ts_us(&self) -> Option<TsUs> {
        self.catalog_ts_us
    }

    pub(crate) fn accept_instrument(&mut self, frame: CatalogInstrument) {
        if !self.adopt(frame.catalog_ts_us) {
            return;
        }
        self.instrument_total = Some(frame.total_count);
        let existing_index = self
            .instruments
            .iter()
            .position(|held| held.instrument == frame.instrument);
        match existing_index {
            Some(index) => self.instruments[index] = frame,
            None if self.instruments.len() < usize::from(frame.total_count) => {
                self.instruments.push(frame);
            }
            None => {}
        }
    }

    pub(crate) fn accept_feature(&mut self, frame: CatalogFeature) {
        if frame.feature.0 >= frame.total_count || !self.adopt(frame.catalog_ts_us) {
            return;
        }
        match self
            .features
            .iter()
            .position(|held| held.feature == frame.feature)
        {
            Some(index) => self.features[index] = frame,
            None => self.features.push(frame),
        }
    }

    pub(crate) fn build(
        &self,
        strategy_id: &str,
        peer: SocketAddr,
        reported: Lifecycle,
    ) -> Option<UiCatalog> {
        if !self.is_complete(reported.feature_count) {
            return None;
        }
        let mut rows: Vec<&CatalogInstrument> = self.instruments.iter().collect();
        rows.sort_unstable_by_key(|row| row.instrument.0);
        let instruments = rows
            .into_iter()
            .map(|row| UiInstrument {
                instrument_id: row.instrument,
                display: row.display.as_str().into(),
                base: row.base.as_str().into(),
                quote: row.quote.as_str().into(),
                base_asset: row.base_asset,
                quote_asset: row.quote_asset,
                tick_size: row.tick_size,
                lot_size: row.lot_size,
                qty_scale: row.qty_scale,
            })
            .collect();

        let mut feature_names: Vec<Box<str>> = vec![Box::default(); self.features.len()];
        for frame in &self.features {
            let slot = feature_names.get_mut(usize::from(frame.feature.0))?;
            *slot = frame.name.as_str().into();
        }

        Some(UiCatalog {
            strategy_id: strategy_id.into(),
            window_title: format!("Polysim - {strategy_id} @ {peer}").into(),
            // From the heartbeat, never inferred: an engine this workstation cannot hear has no mode.
            execution_mode: reported.execution_mode,
            // A peer reporting a negative cadence is nonsense the model reads as "no cadence",
            // rather than wrapping it into an enormous one.
            spin_interval_us: u64::try_from(reported.spin_interval_us.micros()).unwrap_or(0),
            instruments,
            feature_names,
        })
    }

    fn is_complete(&self, feature_count: u16) -> bool {
        self.instrument_total
            .is_some_and(|total| self.instruments.len() == usize::from(total))
            && self.features.len() == usize::from(feature_count)
    }

    fn adopt(&mut self, catalog_ts_us: TsUs) -> bool {
        match self.catalog_ts_us {
            Some(held) if catalog_ts_us < held => false,
            Some(held) if catalog_ts_us == held => true,
            _ => {
                self.catalog_ts_us = Some(catalog_ts_us);
                self.instruments.clear();
                self.features.clear();
                self.instrument_total = None;
                true
            }
        }
    }
}
