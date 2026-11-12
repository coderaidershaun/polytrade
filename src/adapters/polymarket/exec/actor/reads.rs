//! The balance sweep, and the answers to the reads the driver asks for rather than causes:
//! balances, fills, the two public calls a rotation binding needs, the allowance cache, and the
//! dead man's switch.
//!
//! None of these answer an order. They are what makes an order possible — which token this leg is
//! trading, whether its sell side is even reachable, and whether the venue still believes this
//! process is alive.

use crate::adapters::http::TransportError;
use crate::ids::AssetId;
use crate::msg::exec::AccountChunkKind;
use crate::{info, warn};

use super::super::binding::BindingStep;
use super::super::codec::{
    AccountStamps, account_snapshot, collateral_balance, conditional_balance, decode_balance,
    decode_clob_market, decode_heartbeat, decode_trades_page, trades_page,
};
use super::super::rest::ClobResponse;
use super::Actor;
use super::answer::read_body;
use super::rest::{Lane, RestJob, RestPurpose, Submitted};
use super::resync::{MAX_PAGES, PageWalk};

/// A balance sweep that belongs to no resync pass: a post-fill restatement answers the account
/// table, not the admission gate, so it settles nothing and schedules no retry of its own.
const RESTATEMENT: Option<u64> = None;

impl Actor {
    /// Re-reads balances after a fill. A sweep already outstanding was issued before the fill and
    /// can only answer with pre-fill balances, so the restatement waits and fires when that sweep
    /// lands.
    pub(super) fn restate_balances(&mut self) {
        if self.balances_outstanding > 0 {
            self.is_restatement_due = true;
            return;
        }
        // A restatement that could not be sent stays owed. Nothing else would ask again, and a
        // reservation is only released by a balance read carrying settlement the hot side has not
        // already seen.
        self.is_restatement_due = !self.request_balance_sweep(RESTATEMENT);
    }

    /// Reads each asset separately, since this venue has no multi-asset balance endpoint.
    ///
    /// Only reads that actually left are counted outstanding. Counting the intent instead leaves the
    /// gate waiting on an answer nobody asked for: the sweep never reaches zero, the account snapshot
    /// is never forwarded, and every later restatement defers behind it forever.
    pub(super) fn request_balance_sweep(&mut self, resync_seq: Option<u64>) -> bool {
        self.balances.clear();
        self.is_balance_sweep_readable = true;
        let mut reads: Vec<(AssetId, super::super::codec::EncodedRequest)> =
            vec![(self.quote_asset(), collateral_balance(self.signature_type))];
        for &instrument in self.session.instruments() {
            let Some(token_id) = self
                .tokens
                .live_binding(instrument)
                .map(|binding| binding.token_id.clone())
            else {
                continue;
            };
            reads.push((
                self.base_asset(instrument),
                conditional_balance(&token_id, self.signature_type),
            ));
        }
        self.balances_outstanding = 0;
        let mut is_queued = true;
        for (asset, request) in reads {
            let purpose = RestPurpose::Balance { asset, resync_seq };
            match self.submit(Lane::Control, RestJob::signed(purpose, request)) {
                Submitted::Queued => self.balances_outstanding += 1,
                // An asset missing from the sweep makes the snapshot partial, and a partial account
                // is not an account: the reads that did land are discarded with the rest.
                Submitted::LaneFull => {
                    is_queued = false;
                    self.is_balance_sweep_readable = false;
                }
            }
        }
        is_queued
    }

    pub(super) fn on_balance(
        &mut self,
        asset: AssetId,
        resync_seq: Option<u64>,
        answer: Result<ClobResponse, TransportError>,
    ) {
        self.balances_outstanding = self.balances_outstanding.saturating_sub(1);
        let is_readable = self.fold_balance(asset, &answer);
        // An incomplete sweep must never be read as a full account snapshot; retrying on
        // failure is the only way to settle the pass.
        self.is_balance_sweep_readable &= is_readable;
        if !is_readable {
            self.fail_resync(resync_seq);
        }
        if self.balances_outstanding > 0 {
            return;
        }
        if !std::mem::replace(&mut self.is_balance_sweep_readable, true) {
            self.balances.clear();
            return;
        }
        let chunks = account_snapshot(
            &self.balances,
            AccountChunkKind::Snapshot,
            AccountStamps {
                settled_through: self.settled_through,
                received_ts_us: self.control.clock.now(),
            },
        );
        self.balances.clear();
        self.session.events.send_account(chunks);
        self.settle_pass_read(resync_seq);
        // A fill may have landed during the sweep, making these reads stale, so a
        // restatement is issued if one is now due.
        if std::mem::take(&mut self.is_restatement_due) {
            self.restate_balances();
        }
    }

    /// `false` when this asset's balance did not land, whatever the reason. The venue's own status
    /// reaches the decoder here rather than a fabricated 200, which is what lets a 425 or 503 park
    /// the driver instead of reading as a zero balance.
    fn fold_balance(
        &mut self,
        asset: AssetId,
        answer: &Result<ClobResponse, TransportError>,
    ) -> bool {
        let response = match answer {
            Ok(response) => response,
            Err(error) => {
                warn!("polymarket execution balance read failed: {error}");
                return false;
            }
        };
        let decoded = decode_balance(response.answer(), asset, &self.decode_context());
        let Some(balance) = self.answered(decoded, "a balance") else {
            return false;
        };
        // A failing status decodes to a blank balance, which would starve the funds gate as
        // convincingly as a real zero.
        if !response.is_success() {
            warn!(
                "polymarket execution balance read answered http {}: {}",
                response.status,
                response.excerpt()
            );
            return false;
        }
        self.balances.push(balance);
        true
    }

    /// Reads fills only to watch for settlement and to dedup; quantity itself comes from the
    /// order surface, never from here.
    pub(super) fn on_trades_page(
        &mut self,
        resync_seq: Option<u64>,
        page: u32,
        answer: Result<ClobResponse, TransportError>,
    ) {
        let response = match &answer {
            Ok(response) if response.is_success() => response,
            Ok(response) => {
                warn!(
                    "polymarket execution trades read answered http {}: {}",
                    response.status,
                    response.excerpt()
                );
                self.fail_resync(resync_seq);
                return;
            }
            Err(error) => {
                warn!("polymarket execution trades read failed: {error}");
                self.fail_resync(resync_seq);
                return;
            }
        };
        let decoded = decode_trades_page(response.answer(), &self.decode_context());
        let Some(fills) = self.answered(decoded, "a trades page") else {
            self.fail_resync(resync_seq);
            return;
        };
        for trade in fills.trades {
            self.on_trade(trade);
        }
        // The pass settles on the page that exhausts the cursor, never on the first of several.
        if self.next_trades_page(resync_seq, page, fills.next_cursor.as_deref())
            == PageWalk::Complete
        {
            self.settle_pass_read(resync_seq);
        }
    }

    fn next_trades_page(
        &mut self,
        resync_seq: Option<u64>,
        page: u32,
        cursor: Option<&str>,
    ) -> PageWalk {
        let Some(cursor) = cursor else {
            return PageWalk::Complete;
        };
        if page + 1 >= MAX_PAGES {
            warn!(
                "polymarket execution stopped walking its fills at {MAX_PAGES} pages — the rest of the account goes unread"
            );
            return PageWalk::Complete;
        }
        let next = RestPurpose::Trades {
            resync_seq,
            page: page + 1,
        };
        if !self
            .submit(
                Lane::Control,
                RestJob::signed(next, trades_page(Some(cursor))),
            )
            .is_queued()
        {
            self.fail_resync(resync_seq);
        }
        PageWalk::Pending
    }

    pub(super) fn on_market_answer(
        &mut self,
        condition_id: &str,
        answer: Result<ClobResponse, TransportError>,
    ) {
        let step = match read_body("clob-markets", &answer) {
            Some(body) => match decode_clob_market(body) {
                Ok(market) => {
                    if !market.is_accepting_orders {
                        warn!(
                            "polymarket execution bound a market that is not accepting orders yet: {condition_id}"
                        );
                    }
                    self.bindings.on_market(condition_id, market.tick_size)
                }
                Err(error) => {
                    warn!("polymarket execution could not read market metadata: {error}");
                    BindingStep::Wait
                }
            },
            None => BindingStep::Wait,
        };
        self.apply_binding_step(step);
    }

    pub(super) fn on_allowance(
        &mut self,
        token_id: &str,
        answer: Result<ClobResponse, TransportError>,
    ) {
        let is_warm = read_body("balance-allowance/update", &answer).is_some();
        self.bindings.on_allowance_answered(token_id, is_warm);
        match is_warm {
            true => info!("polymarket execution warmed the allowance cache for token {token_id}"),
            false => warn!(
                "polymarket execution could not warm the allowance cache for token {token_id} — sells on it stay withheld"
            ),
        }
    }

    /// The venue answers with the id the next heartbeat must echo, whether this one was
    /// refused or succeeded.
    pub(super) fn on_heartbeat_answer(&mut self, answer: Result<ClobResponse, TransportError>) {
        let Ok(response) = answer else {
            warn!(
                "polymarket execution heartbeat request failed in transport — the venue cancels this book after ten seconds of silence"
            );
            return;
        };
        match decode_heartbeat(&response.body) {
            Ok(id) => self.heartbeat.id = Some(id),
            Err(error) => warn!(
                "polymarket execution heartbeat answer unreadable ({error}) — the venue cancels this book after ten seconds of silence"
            ),
        }
    }

    pub(super) fn apply_binding_step(&mut self, step: BindingStep) {
        let BindingStep::Ready(ready) = step else {
            return;
        };
        for entry in ready {
            info!(
                "polymarket execution bound instrument {} to token {} (tick {}, neg-risk {}) until {}",
                entry.instrument.0,
                entry.token_id,
                entry.tick.0,
                entry.is_neg_risk,
                entry.window_close_ts_us.micros()
            );
            self.tokens.bind(super::super::codec::TokenBinding {
                instrument: entry.instrument,
                token_id: entry.token_id.as_ref().into(),
                tick: entry.tick,
                is_neg_risk: entry.is_neg_risk,
            });
            // Both of these are per-token reads, and both are needed before trading begins:
            // any resting orders on the token, and a warm allowance cache.
            self.read_bound_token(entry.instrument, &entry.token_id);
            self.warm_allowance(&entry.token_id);
        }
    }
}
