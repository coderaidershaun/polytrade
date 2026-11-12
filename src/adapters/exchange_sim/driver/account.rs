//! Simulated account updates.

use super::super::core::schedule::{VenueAnswer, VenueReport, VenueVoice};
use super::super::wire::{SimBalance, stream_messages};
use super::{SimExecDriver, asset_name, venue_millis};
use crate::adapters::binance::exec::DecodeContext;
use crate::ids::Qty;
use crate::msg::inbound::InboundMessage;
use crate::time::TsUs;

impl SimExecDriver {
    pub(super) fn settlement_messages(
        &mut self,
        at_ts_us: TsUs,
        decode: DecodeContext<'_>,
    ) -> Vec<InboundMessage> {
        let settlement = self.venue.wallet_mut().settle(venue_millis(at_ts_us));
        let balances: Vec<SimBalance<'_>> = settlement
            .balances
            .iter()
            .map(|balance| SimBalance {
                asset: asset_name(decode.assets, balance.asset),
                free: Qty(balance.free),
                locked: Qty(balance.locked),
            })
            .collect();
        let payload = self
            .wire
            .account_position(&balances, at_ts_us, settlement.update_ts_ms);
        stream_messages(&[payload], decode)
    }
}

pub(super) fn has_account_update(voice: &VenueVoice) -> bool {
    match voice {
        VenueVoice::Report(
            VenueReport::Trade { .. } | VenueReport::Canceled(_) | VenueReport::Rejected(_),
        ) => true,
        VenueVoice::Response(VenueAnswer::AmendAccepted(_)) => true,
        VenueVoice::Response(VenueAnswer::Refused { .. }) => true,
        VenueVoice::Report(VenueReport::New(_))
        | VenueVoice::Response(_)
        | VenueVoice::Synthesised(_) => false,
    }
}
