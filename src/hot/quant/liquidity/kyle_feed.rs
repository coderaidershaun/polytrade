//! Bar stream -> Kyle observations: a bar carries its flow only once closed, so the fold holds one
//! bar back and pairs it with the mid move the following close reveals.

use crate::hot::quant::liquidity::kyles_lambda::{KyleEstimate, KylesLambda, KylesLambdaSpec};
use crate::ids::Price;

/// Bars one closing trade shut, folded to Kyle observation. flow grows as bars arrive; mid sampled pre-impact.
#[derive(Debug, Clone, Copy)]
struct PendingRun {
    flow: f64,
    mid: f64,
}

/// Kyle feed: rolling estimator + stream-to-observation state.
pub struct KyleFeed {
    estimator: KylesLambda,
    pending: Option<PendingRun>,
    prev_mid: Option<f64>,
}

impl KyleFeed {
    pub fn new(spec: KylesLambdaSpec, tick: Price) -> Self {
        Self {
            estimator: KylesLambda::new(spec, tick),
            pending: None,
            prev_mid: None,
        }
    }

    /// Folds one closed bar in, returning an estimate only where the bar completed an observation.
    /// Δm spans [previous run's close, this one's]; a missing mid drops the run and re-anchors.
    ///
    /// Keys on ARRIVALS rather than on the notional the bar carries. One trade bigger than a bar
    /// target closes several bars at once and every one of them is handed over inside that single
    /// trade's dispatch, so they all read the same mid — scoring them separately would credit the
    /// trade's whole price move to its first slice and pair the rest with a mid that cannot have
    /// moved yet. A bar no new trade arrived in therefore pours its flow into the open run.
    pub fn on_bar(
        &mut self,
        flow: f64,
        trade_arrivals: u32,
        mid_now: Option<f64>,
    ) -> Option<KyleEstimate> {
        if trade_arrivals == 0 {
            if let Some(pending) = self.pending.as_mut() {
                pending.flow += flow;
            }
            return None;
        }
        let mut has_pushed = false;
        if let Some(run) = self.pending.take() {
            if let Some(previous_mid) = self.prev_mid {
                self.estimator.push(run.flow, run.mid - previous_mid);
                has_pushed = true;
            }
            self.prev_mid = Some(run.mid);
        }
        match mid_now {
            Some(mid) => self.pending = Some(PendingRun { flow, mid }),
            None => {
                self.pending = None;
                self.prev_mid = None;
            }
        }
        if !has_pushed {
            return None;
        }
        self.estimator.fit()
    }

    pub fn reset(&mut self) {
        self.estimator.clear();
        self.pending = None;
        self.prev_mid = None;
    }
}
