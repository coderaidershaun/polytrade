//! REST-only jobs: lost-answer reads, balance snapshots (not deltas), cancel on socket failure.
//! A single task and a single client share one clock offset, one retry budget and one
//! rate-limit state.

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::adapters::binance::rest::{
    AccountInfo, AccountTrade, FailureVerdict, OrderRecord, RestError, SignedRestClient,
};
use crate::adapters::exec::EngineIdentity;
use crate::ids::{ClientOrderId, InstrumentId};
use crate::warn;

use super::super::{ClockOffset, SymbolTable, format_client_order_id};

/// Backpressure for a stalled venue. A full queue means something is wrong.
pub(super) const JOB_CAPACITY: usize = 64;

/// The `myTrades` page size. The cursor walks forward, refetching from its new position.
const TRADE_PAGE: u32 = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RestJob {
    SyncClock,
    /// Absolute. resync_seq=0 for delta trigger.
    Account {
        resync_seq: u64,
    },
    /// Open. resync_seq=0 for hot read.
    OpenOrders {
        instrument: InstrumentId,
        resync_seq: u64,
    },
    /// Recon. recon_seq for hot discard on stale.
    OrderStatus {
        instrument: InstrumentId,
        client_id: ClientOrderId,
        recon_seq: u64,
    },
    /// Trade walk from_id = last + 1.
    MyTrades {
        instrument: InstrumentId,
        from_id: Option<i64>,
    },
    /// Owner lookup. `myTrades` lacks client_id. `trade_id` rides along so a failed lookup can put
    /// the trade back in front of the cursor instead of losing the fill.
    OrderByVenueId {
        instrument: InstrumentId,
        venue_order_id: i64,
        trade_id: i64,
    },
    Cancel {
        instrument: InstrumentId,
        client_id: ClientOrderId,
        recon_seq: u64,
    },
}

#[derive(Debug)]
pub(super) enum RestAnswer {
    Clock(ClockOffset),
    Account(Box<AccountInfo>),
    Orders(Vec<OrderRecord>),
    Order(Box<OrderRecord>),
    Trades(Vec<AccountTrade>),
}

#[derive(Debug)]
pub(super) struct RestOutcome {
    pub job: RestJob,
    pub answer: Result<RestAnswer, RestJobError>,
}

#[derive(thiserror::Error, Debug)]
pub(super) enum RestJobError {
    #[error(transparent)]
    Rest(#[from] RestError),
    #[error(
        "no venue symbol for instrument {instrument} — the execution symbol table and the job disagree"
    )]
    UnknownInstrument { instrument: u16 },
}

pub(super) struct RestChannels {
    pub jobs: mpsc::Sender<RestJob>,
    pub outcomes: mpsc::Receiver<RestOutcome>,
    pub join: JoinHandle<()>,
}

pub(super) fn spawn_rest_worker(
    client: SignedRestClient,
    symbols: SymbolTable,
    identity: EngineIdentity,
    rt: &tokio::runtime::Handle,
) -> RestChannels {
    let (job_tx, job_rx) = mpsc::channel(JOB_CAPACITY);
    let (outcome_tx, outcome_rx) = mpsc::channel(JOB_CAPACITY);
    let worker = Worker {
        client,
        symbols,
        identity,
    };
    RestChannels {
        jobs: job_tx,
        outcomes: outcome_rx,
        join: rt.spawn(crate::log::tag_task(
            "binance-exec-rest",
            worker.run(job_rx, outcome_tx),
        )),
    }
}

struct Worker {
    client: SignedRestClient,
    symbols: SymbolTable,
    identity: EngineIdentity,
}

impl Worker {
    async fn run(mut self, mut jobs: mpsc::Receiver<RestJob>, outcomes: mpsc::Sender<RestOutcome>) {
        while let Some(job) = jobs.recv().await {
            let answer = self.execute(job).await;
            if outcomes.send(RestOutcome { job, answer }).await.is_err() {
                return;
            }
        }
    }

    async fn execute(&mut self, job: RestJob) -> Result<RestAnswer, RestJobError> {
        match job {
            RestJob::SyncClock => Ok(RestAnswer::Clock(self.client.sync_clock().await?)),
            RestJob::Account { .. } => {
                Ok(RestAnswer::Account(Box::new(self.client.account().await?)))
            }
            RestJob::OpenOrders { instrument, .. } => {
                let symbol = self.symbol(instrument)?;
                Ok(RestAnswer::Orders(self.client.open_orders(&symbol).await?))
            }
            RestJob::OrderStatus {
                instrument,
                client_id,
                ..
            } => {
                let symbol = self.symbol(instrument)?;
                let order = self
                    .client
                    .order_status(&symbol, &self.client_order_id(client_id))
                    .await?;
                Ok(RestAnswer::Order(Box::new(order)))
            }
            RestJob::MyTrades {
                instrument,
                from_id,
            } => {
                let symbol = self.symbol(instrument)?;
                let trades = self.client.my_trades(&symbol, from_id, TRADE_PAGE).await?;
                Ok(RestAnswer::Trades(trades))
            }
            RestJob::OrderByVenueId {
                instrument,
                venue_order_id,
                ..
            } => {
                let symbol = self.symbol(instrument)?;
                let order = self
                    .client
                    .order_status_by_venue_id(&symbol, venue_order_id)
                    .await?;
                Ok(RestAnswer::Order(Box::new(order)))
            }
            RestJob::Cancel {
                instrument,
                client_id,
                ..
            } => {
                let symbol = self.symbol(instrument)?;
                let order = self
                    .client
                    .cancel_order(&symbol, &self.client_order_id(client_id))
                    .await?;
                Ok(RestAnswer::Order(Box::new(order)))
            }
        }
    }

    fn symbol(&self, instrument: InstrumentId) -> Result<String, RestJobError> {
        self.symbols
            .symbol(instrument)
            .map(str::to_owned)
            .ok_or(RestJobError::UnknownInstrument {
                instrument: instrument.0,
            })
    }

    fn client_order_id(&self, client_id: ClientOrderId) -> String {
        format_client_order_id(self.identity.te_tag, client_id)
    }
}

/// Answers whether the job queued. A dropped one is a reconciliation that never happens, so every
/// caller owes the skip a repair or an explicit reason it heals itself.
#[must_use]
pub(super) fn submit(jobs: &mpsc::Sender<RestJob>, job: RestJob) -> bool {
    if jobs.try_send(job).is_ok() {
        return true;
    }
    report_full_queue(job);
    false
}

#[cold]
fn report_full_queue(job: RestJob) {
    warn!("binance execution rest queue full at {JOB_CAPACITY} — dropped {job:?}");
}

/// Map error to verdict. Routine + StatusQuery = definitive "no such order".
pub(super) fn verdict_of(error: &RestJobError) -> FailureVerdict {
    match error {
        RestJobError::Rest(error) => error.verdict(),
        RestJobError::UnknownInstrument { .. } => FailureVerdict::Fatal,
    }
}
