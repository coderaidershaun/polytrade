//! Binding market window facts to execution. The tradeable token id rotates every five minutes,
//! so it comes from the market-data actor at window assignment, not configuration.
//! Two additional facts must be fetched: tick size from /clob-markets and the neg-risk flag from /neg-risk
//! per token. /clob-markets does not publish neg-risk; wrong contract signatures fail silently.

use std::sync::Arc;

use super::super::rotation::WindowAssignment;
use crate::ids::{InstrumentId, Price};
use crate::time::{DurationUs, TsUs};

// Bindings pending enrichment reads. Cap prevents venue refusals from growing the list unbounded.
const PENDING_BINDINGS: usize = 4;

// Late fills on retired tokens route to their instruments by matching against kept bindings (multiple windows worth).
pub const RETIRED_BINDINGS: usize = 8;

// Allowance refreshes that may be outstanding at once. One per live leg is the steady state; the
// slack absorbs a rotation overlapping the previous window's tokens.
const OUTSTANDING_REFRESHES: usize = 8;

// Tokens remembered as allowance-warm. Evicting one makes its sells read as cold and be withheld,
// so this holds several windows of legs rather than just the live pair.
const WARM_TOKENS: usize = 8;

// Windows remembered as already swept by the close-margin backstop, so it fires once each. Only the
// windows still closing matter; older markers can go.
const SWEPT_WINDOWS: usize = 8;

// Retry pace for outstanding enrichment reads. Public GETs have no rate risk. Single transients recover next tick.
pub const ENRICHMENT_RETRY: DurationUs = DurationUs::from_micros(1_000_000);

// Attempts before giving up on a binding. Beyond this, the market is unreadable and further retries burn budget.
pub const MAX_ENRICHMENT_ATTEMPTS: u32 = 5;

/// One leg of a window, waiting for the facts an order needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegBinding {
    pub instrument: InstrumentId,
    pub token_id: Arc<str>,
    pub is_neg_risk: Option<bool>,
}

/// A resolved window whose enrichment reads are outstanding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingBinding {
    pub condition_id: Arc<str>,
    pub legs: [LegBinding; 2],
    pub window_open_ts_us: TsUs,
    pub window_close_ts_us: TsUs,
    pub tick: Option<Price>,
    // Number of enrichment read re-issues so far.
    attempts: u32,
    // Earliest time to re-issue. Armed on first retry poll (not at creation) because initial reads
    // leave immediately with the assignment.
    retry_at: TsUs,
    is_retry_armed: bool,
}

impl PendingBinding {
    fn from_assignment(assignment: &WindowAssignment) -> Self {
        Self {
            condition_id: Arc::clone(&assignment.condition_id),
            legs: [
                LegBinding {
                    instrument: assignment.up.instrument,
                    token_id: Arc::from(assignment.up.token.as_str()),
                    is_neg_risk: None,
                },
                LegBinding {
                    instrument: assignment.down.instrument,
                    token_id: Arc::from(assignment.down.token.as_str()),
                    is_neg_risk: None,
                },
            ],
            window_open_ts_us: assignment.window_open_ts_us,
            window_close_ts_us: assignment.window_close_ts_us,
            tick: None,
            attempts: 0,
            retry_at: TsUs::from_micros(0),
            is_retry_armed: false,
        }
    }

    fn is_complete(&self) -> bool {
        self.tick.is_some() && self.legs.iter().all(|leg| leg.is_neg_risk.is_some())
    }
}

/// An enrichment read to (re)issue for a pending binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnrichmentRead {
    Market {
        condition_id: Arc<str>,
    },
    NegRisk {
        condition_id: Arc<str>,
        instrument: InstrumentId,
        token_id: Arc<str>,
    },
}

/// What a live instrument is trading, once every enrichment read has landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveWindow {
    pub instrument: InstrumentId,
    pub token_id: Arc<str>,
    pub window_close_ts_us: TsUs,
}

#[derive(Debug, Default)]
pub struct Bindings {
    pending: Vec<PendingBinding>,
    live: Vec<LiveWindow>,
    /// Tokens whose CLOB allowance cache has been warmed this run. A sell on a token absent from
    /// here reads as an empty wallet however funded the account is.
    allowed: Vec<Arc<str>>,
    /// Refreshes already in flight. The refresh endpoint allows 50 calls per ten seconds and a
    /// withheld sell is re-decided every spin, so asking again while one is outstanding would spend
    /// the budget on the same answer.
    requested: Vec<Arc<str>>,
    /// Windows already swept by the close-margin backstop, so it fires once each.
    swept: Vec<SweptWindow>,
    refused: u64,
}

/// One window the close-margin backstop has already acted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SweptWindow {
    instrument: InstrumentId,
    window_close_ts_us: TsUs,
}

/// One instrument and the token it is trading — the pair every rotation-driven read needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenOfInstrument {
    pub instrument: InstrumentId,
    pub token_id: Arc<str>,
}

/// What the driver must do next for a binding that just arrived or advanced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindingStep {
    /// Read tick and minimum size for the market, then the neg-risk flag for each token.
    Enrich {
        condition_id: Arc<str>,
        tokens: Vec<TokenOfInstrument>,
    },
    /// Every fact is in: bind both legs and start quoting them.
    Ready(Vec<ReadyBinding>),
    Wait,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyBinding {
    pub instrument: InstrumentId,
    pub token_id: Arc<str>,
    pub tick: Price,
    pub is_neg_risk: bool,
    pub window_open_ts_us: TsUs,
    pub window_close_ts_us: TsUs,
}

impl Bindings {
    /// Windows whose enrichment reads were abandoned because newer ones displaced them.
    pub fn refused(&self) -> u64 {
        self.refused
    }

    /// A window this engine has already bound arrives again on reconnect; the assembly restarts
    /// rather than duplicating, because the enrichment answers name the condition id.
    pub fn on_assignment(&mut self, assignment: &WindowAssignment) -> BindingStep {
        let binding = PendingBinding::from_assignment(assignment);
        let tokens = binding
            .legs
            .iter()
            .map(|leg| TokenOfInstrument {
                instrument: leg.instrument,
                token_id: Arc::clone(&leg.token_id),
            })
            .collect();
        let condition_id = Arc::clone(&binding.condition_id);
        self.pending
            .retain(|existing| existing.condition_id != condition_id);
        if self.pending.len() >= PENDING_BINDINGS {
            self.pending.remove(0);
            self.refused += 1;
        }
        self.pending.push(binding);
        BindingStep::Enrich {
            condition_id,
            tokens,
        }
    }

    pub fn on_market(&mut self, condition_id: &str, tick: Price) -> BindingStep {
        let Some(binding) = self.find_mut(condition_id) else {
            return BindingStep::Wait;
        };
        binding.tick = Some(tick);
        self.settle(condition_id)
    }

    pub fn on_neg_risk(
        &mut self,
        condition_id: &str,
        instrument: InstrumentId,
        is_neg_risk: bool,
    ) -> BindingStep {
        let Some(binding) = self.find_mut(condition_id) else {
            return BindingStep::Wait;
        };
        if let Some(leg) = binding
            .legs
            .iter_mut()
            .find(|leg| leg.instrument == instrument)
        {
            leg.is_neg_risk = Some(is_neg_risk);
        }
        self.settle(condition_id)
    }

    /// Enrichment reads to re-issue for pending bindings whose outstanding facts have not landed in
    /// time. The initial reads leave on the assignment; this recovers a transient failure, because a
    /// single failed public read would otherwise leave the instrument unbound for the whole window —
    /// every placement refused `UnboundInstrument` until the next rotation. Only the MISSING reads
    /// are re-issued, and a binding still incomplete after [`MAX_ENRICHMENT_ATTEMPTS`] is given up
    /// (counted as refused) rather than retried into the read budget.
    pub fn due_enrichment_reads(&mut self, now: TsUs) -> Vec<EnrichmentRead> {
        // Giving up and walking are separate passes on purpose. Sharing one cursor between them
        // means every branch has to remember whether it may advance, and the branch that gets it
        // wrong spins forever on the async edge rather than returning a wrong answer.
        let mut given_up = 0;
        self.pending.retain(|binding| {
            let is_spent = !binding.is_complete()
                && binding.is_retry_armed
                && now >= binding.retry_at
                && binding.attempts + 1 > MAX_ENRICHMENT_ATTEMPTS;
            given_up += u64::from(is_spent);
            !is_spent
        });
        self.refused += given_up;

        let mut reads = Vec::new();
        for binding in &mut self.pending {
            if binding.is_complete() {
                continue;
            }
            // The first poll only arms the timer: the reads it would issue already left on the
            // assignment, so re-issuing now would double them.
            if !binding.is_retry_armed {
                binding.is_retry_armed = true;
                binding.retry_at = now + ENRICHMENT_RETRY;
                continue;
            }
            if now < binding.retry_at {
                continue;
            }
            binding.attempts += 1;
            binding.retry_at = now + ENRICHMENT_RETRY;
            if binding.tick.is_none() {
                reads.push(EnrichmentRead::Market {
                    condition_id: Arc::clone(&binding.condition_id),
                });
            }
            for leg in &binding.legs {
                if leg.is_neg_risk.is_none() {
                    reads.push(EnrichmentRead::NegRisk {
                        condition_id: Arc::clone(&binding.condition_id),
                        instrument: leg.instrument,
                        token_id: Arc::clone(&leg.token_id),
                    });
                }
            }
        }
        reads
    }

    /// Whether this token's sell side may be sent yet.
    pub fn is_allowance_warm(&self, token_id: &str) -> bool {
        self.allowed.iter().any(|known| &**known == token_id)
    }

    /// Whether a refresh should be sent now. `false` means one is already outstanding or the cache
    /// is already warm.
    pub fn claim_allowance_refresh(&mut self, token_id: &str) -> bool {
        if self.is_allowance_warm(token_id)
            || self.requested.iter().any(|known| &**known == token_id)
        {
            return false;
        }
        self.requested.push(Arc::from(token_id));
        while self.requested.len() > OUTSTANDING_REFRESHES {
            self.requested.remove(0);
        }
        true
    }

    /// The claimed refresh never reached the venue. Handing the claim back is the whole reason the
    /// claim is separate from the send: an answer is the only other thing that clears it, and no
    /// answer is coming for a request that was dropped.
    pub fn release_allowance_refresh(&mut self, token_id: &str) {
        self.requested.retain(|known| &**known != token_id);
    }

    /// The refresh answered, either way. A refusal clears the in-flight mark so the next withheld
    /// sell asks again rather than waiting forever on an answer that already came back.
    pub fn on_allowance_answered(&mut self, token_id: &str, is_warm: bool) {
        self.requested.retain(|known| &**known != token_id);
        if !is_warm || self.is_allowance_warm(token_id) {
            return;
        }
        self.allowed.push(Arc::from(token_id));
        while self.allowed.len() > WARM_TOKENS {
            self.allowed.remove(0);
        }
    }

    /// Instruments whose window is close enough to its end that anything still resting should be
    /// pulled. The hot engine withdraws first; this fires once per window as the backstop.
    pub fn close_margin_reached(
        &mut self,
        now: TsUs,
        margin: DurationUs,
    ) -> Vec<TokenOfInstrument> {
        let mut reached = Vec::new();
        for window in &self.live {
            if now + margin < window.window_close_ts_us {
                continue;
            }
            let marker = SweptWindow {
                instrument: window.instrument,
                window_close_ts_us: window.window_close_ts_us,
            };
            if self.swept.contains(&marker) {
                continue;
            }
            self.swept.push(marker);
            reached.push(TokenOfInstrument {
                instrument: window.instrument,
                token_id: Arc::clone(&window.token_id),
            });
        }
        while self.swept.len() > SWEPT_WINDOWS {
            self.swept.remove(0);
        }
        reached
    }

    fn find_mut(&mut self, condition_id: &str) -> Option<&mut PendingBinding> {
        self.pending
            .iter_mut()
            .find(|binding| &*binding.condition_id == condition_id)
    }

    fn settle(&mut self, condition_id: &str) -> BindingStep {
        let Some(index) = self
            .pending
            .iter()
            .position(|binding| &*binding.condition_id == condition_id)
        else {
            return BindingStep::Wait;
        };
        if !self.pending[index].is_complete() {
            return BindingStep::Wait;
        }
        let binding = self.pending.remove(index);
        let tick = binding.tick.expect("settle runs only on complete bindings");
        let ready: Vec<ReadyBinding> = binding
            .legs
            .iter()
            .map(|leg| ReadyBinding {
                instrument: leg.instrument,
                token_id: Arc::clone(&leg.token_id),
                tick,
                is_neg_risk: leg
                    .is_neg_risk
                    .expect("settle runs only on complete bindings"),
                window_open_ts_us: binding.window_open_ts_us,
                window_close_ts_us: binding.window_close_ts_us,
            })
            .collect();
        for entry in &ready {
            self.live
                .retain(|window| window.instrument != entry.instrument);
            self.live.push(LiveWindow {
                instrument: entry.instrument,
                token_id: Arc::clone(&entry.token_id),
                window_close_ts_us: entry.window_close_ts_us,
            });
        }
        BindingStep::Ready(ready)
    }
}
