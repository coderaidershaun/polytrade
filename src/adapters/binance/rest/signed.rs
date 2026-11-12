//! Signed REST reads and cancels. A cancel over REST still works when the WebSocket is dead, so a
//! crash cannot leave orders resting with no way to pull them.

use std::time::Instant;

use serde::de::DeserializeOwned;

use crate::adapters::backoff::BackoffCaps;
use crate::adapters::binance::exec::{ClockOffset, RecvWindow, RequestParams, RequestSigner};
use crate::adapters::rest_quiet::SharedRestQuiet;
use crate::config::BinanceMarket;
use crate::secrets::Credentials;
use crate::time::EngineClock;
use crate::warn;

use super::{
    AccountInfo, AccountTrade, BinanceEnv, FailureVerdict, Fetched, OrderRecord, Prepared,
    RestClient, RestError, RestRequest, decode,
};

// The venue's code for a stale timestamp. Each retry resyncs the clock at most once, because more
// resyncs than that become a loop.
const STALE_TIMESTAMP_CODE: i64 = -1021;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignedRestConfig {
    pub env: BinanceEnv,
    pub recv_window: RecvWindow,
    pub backoff: BackoffCaps,
    /// Total tries, not retries: 1 means send once and surface whatever comes back.
    pub max_attempts: u32,
}

impl Default for SignedRestConfig {
    fn default() -> Self {
        Self {
            env: BinanceEnv::Production,
            recv_window: RecvWindow::DEFAULT,
            backoff: BackoffCaps::default(),
            max_attempts: 3,
        }
    }
}

/// Signed REST for one deployment. Spot only: the private futures endpoints sit under a different
/// prefix with different weights, and pretending otherwise would charge the wrong budget silently.
pub struct SignedRestClient {
    rest: RestClient,
    credentials: Credentials,
    signer: RequestSigner,
    clock: EngineClock,
    clock_offset: ClockOffset,
    config: SignedRestConfig,
    quiet: SharedRestQuiet,
}

impl SignedRestClient {
    /// `quiet` is the venue's window, not this client's: the market-data actor's reads are charged
    /// against the same per-IP allowance, so a rate limit either of them earns must hold both off.
    ///
    /// # Errors
    /// [`RestError::Transport`] if the TLS backend fails to initialise.
    pub fn new(
        credentials: Credentials,
        config: SignedRestConfig,
        quiet: SharedRestQuiet,
    ) -> Result<Self, RestError> {
        let signer = RequestSigner::new(credentials.api_secret());
        Ok(Self {
            rest: RestClient::new(BinanceMarket::Spot, config.env)?,
            credentials,
            signer,
            clock: EngineClock::start(),
            clock_offset: ClockOffset::NONE,
            config,
            quiet,
        })
    }

    /// # Errors
    /// [`RestError`] if venue unreachable.
    pub async fn sync_clock(&mut self) -> Result<ClockOffset, RestError> {
        let server_time = self.rest.server_time().await?;
        // Read the clock after the await, at receipt, so the offset errs late rather than early:
        // the venue rejects an early stamp.
        self.clock_offset = ClockOffset::learn(server_time, self.clock.now());
        Ok(self.clock_offset)
    }

    pub fn clock_offset(&self) -> ClockOffset {
        self.clock_offset
    }

    /// # Errors
    /// [`RestError`].
    pub async fn account(&mut self) -> Result<AccountInfo, RestError> {
        self.signed(RestRequest::AccountInfo).await
    }

    /// # Errors
    /// [`RestError`].
    pub async fn my_trades(
        &mut self,
        symbol: &str,
        from_id: Option<i64>,
        limit: u32,
    ) -> Result<Vec<AccountTrade>, RestError> {
        self.signed(RestRequest::MyTrades {
            symbol: symbol.to_owned(),
            from_id,
            limit,
        })
        .await
    }

    /// # Errors
    /// [`RestError`].
    pub async fn open_orders(&mut self, symbol: &str) -> Result<Vec<OrderRecord>, RestError> {
        self.signed(RestRequest::OpenOrders {
            symbol: symbol.to_owned(),
        })
        .await
    }

    /// # Errors
    /// [`RestError`]; a `Routine` verdict here means the venue has no such order.
    pub async fn order_status(
        &mut self,
        symbol: &str,
        orig_client_order_id: &str,
    ) -> Result<OrderRecord, RestError> {
        self.signed(RestRequest::OrderStatus {
            symbol: symbol.to_owned(),
            orig_client_order_id: orig_client_order_id.to_owned(),
        })
        .await
    }

    /// The same read keyed by the venue's own id, for the one caller that has no client id to use:
    /// a trade reported by `myTrades`.
    ///
    /// # Errors
    /// [`RestError`]; a `Routine` verdict means the venue has no such order.
    pub async fn order_status_by_venue_id(
        &mut self,
        symbol: &str,
        venue_order_id: i64,
    ) -> Result<OrderRecord, RestError> {
        self.signed(RestRequest::OrderStatusByVenueId {
            symbol: symbol.to_owned(),
            venue_order_id,
        })
        .await
    }

    /// Cancels one order BY CLIENT ID. There is deliberately no cancel-all: it would reach orders
    /// this engine never placed.
    ///
    /// # Errors
    /// [`RestError`]; a `Routine` verdict means the order was already gone, which a cancel racing a
    /// fill sees regularly.
    pub async fn cancel_order(
        &mut self,
        symbol: &str,
        orig_client_order_id: &str,
    ) -> Result<OrderRecord, RestError> {
        self.signed(RestRequest::CancelOrder {
            symbol: symbol.to_owned(),
            orig_client_order_id: orig_client_order_id.to_owned(),
        })
        .await
    }

    async fn signed<T: DeserializeOwned>(&mut self, request: RestRequest) -> Result<T, RestError> {
        let fetched = self.send_signed(&request).await?;
        decode(&fetched)
    }

    async fn send_signed(&mut self, request: &RestRequest) -> Result<Fetched, RestError> {
        let plan = request.plan(BinanceMarket::Spot);
        let mut attempt = 0;
        loop {
            if let Some(remaining) = self.quiet.remaining(Instant::now()) {
                tokio::time::sleep(remaining).await;
            }

            let params = plan
                .query
                .iter()
                .fold(RequestParams::new(), |params, (name, value)| {
                    params.set(name, value.clone())
                })
                .set_recv_window(self.config.recv_window);
            let signed = self
                .signer
                .sign(params, self.clock_offset.stamp(self.clock.now()))
                .map_err(|source| RestError::Sign {
                    endpoint: plan.endpoint,
                    source,
                })?;

            let outcome = self
                .rest
                .send(Prepared {
                    plan: &plan,
                    signed_query: Some(signed.query()),
                    api_key: Some(self.credentials.api_key()),
                })
                .await;

            let Err(failure) = outcome else {
                return outcome;
            };
            attempt += 1;
            if failure.verdict() != FailureVerdict::Retry || attempt >= self.config.max_attempts {
                return Err(failure);
            }
            self.wait_before_retry(&failure, plan.endpoint, attempt)
                .await;
        }
    }

    async fn wait_before_retry(&mut self, failure: &RestError, endpoint: &str, attempt: u32) {
        if is_stale_timestamp(failure) {
            warn!("binance rejected {endpoint} stamp as stale — resyncing the venue clock");
            // A failed resync leaves the old offset in place; the retry then fails the same way and
            // the attempt budget ends it, which beats turning a clock blip into a startup abort.
            self.sync_clock().await.ok();
        }
        if let RestError::RateLimited {
            retry_after_secs, ..
        } = failure
        {
            let wait = self.quiet.open(*retry_after_secs, Instant::now());
            warn!(
                "binance rate limited {endpoint} — holding off {}s before retry {attempt}",
                wait.as_secs()
            );
            tokio::time::sleep(wait).await;
            return;
        }
        tokio::time::sleep(self.config.backoff.delay(attempt.saturating_sub(1))).await;
    }
}

fn is_stale_timestamp(failure: &RestError) -> bool {
    matches!(failure, RestError::Status { code: Some(code), .. } if *code == STALE_TIMESTAMP_CODE)
}
