//! Pairs the signed flow of closed volume bars with the move of the mid that the flow caused, as the
//! observations of Kyle's lambda. A run waits for the book commit after its closing trade.

use crate::hot::quant::liquidity::kyles_lambda::{KyleEstimate, KylesLambda, KylesLambdaSpec};

/// The flow of the bars that closed since the last commit. It becomes one observation when the next
/// committed mid shows how far the book moved.
#[derive(Debug, Clone, Copy)]
struct PendingRun {
    flow: f64,
}

/// The rolling Kyle estimator, fed from the stream of closed bars and committed mids.
pub struct KyleFeed {
    estimator: KylesLambda,
    pending: Option<PendingRun>,
    prev_mid: Option<f64>,
}

impl KyleFeed {
    pub fn new(spec: KylesLambdaSpec) -> Self {
        Self {
            estimator: KylesLambda::new(spec),
            pending: None,
            prev_mid: None,
        }
    }

    /// Folds one closed bar into the run that waits for its book commit. `mid_now` is the committed
    /// mid that the trade's dispatch reads: `None` means the book is gone, so the run and the
    /// anchor drop and no observation spans the gap.
    ///
    /// Keys on ARRIVALS rather than on the notional the bar carries. One trade bigger than a bar
    /// target closes several bars at once, and every one of them is handed over inside that single
    /// trade's dispatch, before the trade's own book update, so they all wait for the same commit.
    /// Scoring them separately would credit the trade's whole price move to its first slice and
    /// pair the rest with a mid that cannot have moved. A bar no new trade arrived in therefore
    /// pours its flow into the open run and never opens one. A trade that arrives while a run
    /// waits joins it too, because no commit has shown the move of either trade yet.
    pub fn on_bar(&mut self, flow: f64, trade_arrivals: u32, mid_now: Option<f64>) {
        if mid_now.is_none() {
            self.reset_continuity();
            return;
        }
        match self.pending.as_mut() {
            Some(run) => run.flow += flow,
            None if trade_arrivals > 0 => self.pending = Some(PendingRun { flow }),
            None => {}
        }
    }

    /// Folds one committed mid in. It closes the waiting run, and returns an estimate only where
    /// that completed an observation. Δm spans [the commit of the previous run, this one], and the
    /// first run after a start or a gap only anchors. A missing mid drops the run and the anchor.
    pub fn on_mid(&mut self, mid: Option<f64>) -> Option<KyleEstimate> {
        let Some(mid) = mid else {
            self.reset_continuity();
            return None;
        };
        let run = self.pending.take()?;
        let previous_mid = self.prev_mid.replace(mid)?;
        self.estimator.push(run.flow, mid - previous_mid);
        self.estimator.fit()
    }

    pub fn clear(&mut self) {
        self.estimator.clear();
        self.pending = None;
        self.prev_mid = None;
    }

    fn reset_continuity(&mut self) {
        self.pending = None;
        self.prev_mid = None;
    }
}
