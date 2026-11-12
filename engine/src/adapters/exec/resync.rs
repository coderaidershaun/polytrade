//! Resync policy: read orders + balances before quoting (mirror + hot-side need them). Fails are
//! state transitions (retry + converge or give up connection). Pure + clock-free: caller provides
//! now + retry deadline (testable without socket).

use crate::time::TsUs;

/// Total passes a connection gets to answer quoting's question. One hiccup is expected; a
/// connection that cannot answer in four is not going to.
pub const MAX_RESYNC_ATTEMPTS: u32 = 4;

/// Driver's tick decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResyncStep {
    Wait,
    Retry,
    GiveUp,
}

/// One post-subscribe pass (orders + balances) across retries.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ResyncPass {
    seq: u64,
    outstanding: usize,
    attempts: u32,
    retry_at: Option<TsUs>,
}

impl ResyncPass {
    /// Fresh pass on new connection (earlier attempts cleared). Returns seq for pass jobs.
    pub fn begin(&mut self, reads: usize) -> u64 {
        self.attempts = 0;
        self.open(reads)
    }

    /// Same pass again (attempt count survives; gives connection up eventually).
    pub fn begin_retry(&mut self, reads: usize) -> u64 {
        self.open(reads)
    }

    /// Read landed. True when last (actor's licence). Stale reads settle nothing.
    pub fn on_read(&mut self, seq: u64) -> bool {
        if seq != self.seq || self.outstanding == 0 {
            return false;
        }
        self.outstanding -= 1;
        if self.outstanding > 0 {
            return false;
        }
        self.attempts = 0;
        true
    }

    /// Failed (indistinguishable read/queue). One PASS = one attempt (outage hits all). Scheduled instant is flag.
    pub fn on_failure(&mut self, seq: u64, retry_at: TsUs) {
        if seq != self.seq || self.retry_at.is_some() {
            return;
        }
        self.attempts += 1;
        self.retry_at = Some(retry_at);
    }

    pub fn due(&self, now: TsUs) -> ResyncStep {
        let Some(retry_at) = self.retry_at else {
            return ResyncStep::Wait;
        };
        if now < retry_at {
            return ResyncStep::Wait;
        }
        match self.attempts >= MAX_RESYNC_ATTEMPTS {
            true => ResyncStep::GiveUp,
            false => ResyncStep::Retry,
        }
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Pass still missing a read.
    pub fn is_outstanding(&self) -> bool {
        self.outstanding > 0
    }

    fn open(&mut self, reads: usize) -> u64 {
        self.seq += 1;
        self.outstanding = reads;
        self.retry_at = None;
        self.seq
    }
}
