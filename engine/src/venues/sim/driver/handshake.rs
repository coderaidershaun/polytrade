//! Simulated stream startup and readiness announcements.

use super::super::wire::decimal;
use super::{EffectStamp, SimExecDriver, asset_name, venue_millis};
use crate::msg::inbound::{InboundMessage, MessageBody};
use crate::msg::time::LocalTs;
use crate::venues::binance::exec::{DecodeContext, account_snapshot_chunks};
use crate::venues::binance::rest::Balance;
use crate::venues::exec::{Phase, open_orders_snapshot_end, stream_ready};

impl SimExecDriver {
    pub fn open(&mut self, effective_ts_us: LocalTs) {
        self.has_swept = false;
        let outgoing = self.session.connect();
        self.apply(&outgoing, EffectStamp::at(effective_ts_us));
    }

    pub fn close(&mut self) {
        self.session.core.on_disconnected();
    }

    pub fn can_arm(&self) -> bool {
        let core = &self.session.core;
        !core.has_prior_run() && core.phase() == Phase::Resyncing
    }

    /// # Panics
    /// If [`SimExecDriver::can_arm`] is false.
    pub fn announce_readiness(
        &mut self,
        at_ts_us: LocalTs,
        context: DecodeContext<'_>,
    ) -> Vec<InboundMessage> {
        assert!(
            self.session.core.on_stream_ready(),
            "the simulated venue armed a core that could not verify its stream"
        );
        let decode = DecodeContext {
            received_ts_us: at_ts_us,
            ..context
        };
        let settlement = self.venue.wallet_mut().settle(venue_millis(at_ts_us));
        let exec = |event| InboundMessage::new(at_ts_us, MessageBody::Exec(event));
        let mut messages = vec![exec(stream_ready(at_ts_us))];
        let rows: Vec<Balance> = settlement
            .balances
            .iter()
            .map(|balance| Balance {
                asset: asset_name(decode.assets, balance.asset).into(),
                free: decimal(balance.free).into(),
                locked: decimal(balance.locked).into(),
            })
            .collect();
        let chunks = account_snapshot_chunks(&rows, settlement.update_ts_ms as i64, &decode)
            .expect("the wallet's own balances decode: it minted every string in them");
        messages.extend(
            chunks
                .into_iter()
                .map(|chunk| InboundMessage::new(at_ts_us, MessageBody::Account(chunk))),
        );
        messages.push(exec(open_orders_snapshot_end(self.instrument, at_ts_us)));

        self.fold(&messages, EffectStamp::at(at_ts_us));
        messages
    }

    pub fn owes_nothing(&self) -> bool {
        self.schedule.owes_nothing()
    }
}
