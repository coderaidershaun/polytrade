//! Depth resync: buffer diffs, apply snapshot once diff spans (else resync loop).

use std::time::{Duration, Instant};

use crate::adapters::binance::depth::{ChainRule, DepthSequencer, DiffOutcome};
use crate::adapters::binance::parse::{
    DepthSnapshot, ParseContext, parse_depth_snapshot, parse_perp_depth_diff, parse_spot_depth_diff,
};
use crate::adapters::binance::rest::RestRequest;
use crate::config::BinanceMarket;
use crate::ids::InstrumentId;
use crate::msg::inbound::{InboundMessage, TappedMessage, VenueMeta};
use crate::time::TsUs;

use super::Actor;

/// Buffer diffs while REST in-flight: ample headroom over ~dozens @100ms.
const DEPTH_BUFFER_CAPACITY: usize = 2048;
/// 1000/side = weight 50 spot / 20 perp.
const DEPTH_SNAPSHOT_LIMIT: u32 = 1000;
/// Floor between snapshot fetches per instrument. Sized against the weight a resync loop spends,
/// not against latency taste: at 50 spot weight a fetch and a 6000/minute budget, one looping
/// instrument costs a quarter of the budget here, so four have to loop together before the client's
/// own half-budget warning fires. The diff stream retries on its next frame, 100ms later.
const SNAPSHOT_THROTTLE: Duration = Duration::from_secs(2);

pub(super) struct DepthState {
    sequencer: DepthSequencer,
    /// Held until diff spans (orphaned id risk).
    pending_snapshot: Option<DepthSnapshot>,
    last_snapshot_at: Option<Instant>,
    /// Highest buffered id: gate for snapshot apply.
    latest_update_id: u64,
}

impl DepthState {
    pub(super) fn new(market: BinanceMarket, instrument: InstrumentId) -> Self {
        Self {
            sequencer: DepthSequencer::new(chain_rule(market), instrument, DEPTH_BUFFER_CAPACITY),
            pending_snapshot: None,
            last_snapshot_at: None,
            latest_update_id: 0,
        }
    }
}

impl Actor {
    pub(super) async fn handle_depth(&mut self, data: &str, ctx: ParseContext) {
        let diff = match self.market {
            BinanceMarket::Spot => parse_spot_depth_diff(data, ctx),
            BinanceMarket::Perpetual => parse_perp_depth_diff(data, ctx),
        };
        let Some(diff) = self.parsed(diff) else { return };
        let final_update_id = diff.final_update_id;

        let mut out = Vec::new();
        {
            let Some(state) = self.depth_states.get_mut(&ctx.instrument) else {
                return self
                    .on_unroutable_frame(format_args!("depth on instrument {}", ctx.instrument.0));
            };
            let outcome = state.sequencer.on_diff(diff, &mut tapped_into(&mut out));
            state.latest_update_id = state.latest_update_id.max(final_update_id);
            if matches!(outcome, DiffOutcome::Resync | DiffOutcome::Overflow) {
                // Snapshot predates break -> drop, fetch fresh. The throttle stands: a break is
                // exactly the event that repeats, so clearing it here would disarm it whenever it
                // was needed.
                state.pending_snapshot = None;
            }
        }
        self.flush_tapped(out);

        self.advance_depth_sync(ctx.instrument, ctx.received_ts_us)
            .await;
    }

    /// Apply held snapshot, else fetch (throttled). received_ts_us doubles as the queue floor.
    async fn advance_depth_sync(&mut self, instrument: InstrumentId, received_ts_us: TsUs) {
        let Some(snapshot) = self.take_applicable_snapshot(instrument) else {
            return self.fetch_depth_snapshot(instrument).await;
        };
        let mut out = Vec::new();
        if let Some(state) = self.depth_states.get_mut(&instrument) {
            state.sequencer.note_emit_floor(received_ts_us);
            state
                .sequencer
                .apply_snapshot(snapshot, &mut tapped_into(&mut out));
        }
        self.flush_tapped(out);
    }

    /// Snapshot when ready: not live + spans id.
    fn take_applicable_snapshot(&mut self, instrument: InstrumentId) -> Option<DepthSnapshot> {
        let state = self.depth_states.get_mut(&instrument)?;
        if state.sequencer.is_live() {
            return None;
        }
        let ready = state
            .pending_snapshot
            .as_ref()
            .is_some_and(|snapshot| state.latest_update_id >= snapshot.last_update_id);
        if ready { state.pending_snapshot.take() } else { None }
    }

    async fn fetch_depth_snapshot(&mut self, instrument: InstrumentId) {
        if self.rest_quiet.is_active(Instant::now()) {
            return;
        }
        {
            let now = Instant::now();
            let Some(state) = self.depth_states.get_mut(&instrument) else {
                return;
            };
            let waiting = state.pending_snapshot.is_some() || state.sequencer.is_live();
            let throttled = state
                .last_snapshot_at
                .is_some_and(|p| now.duration_since(p) < SNAPSHOT_THROTTLE);
            if waiting || throttled {
                return;
            }
            state.last_snapshot_at = Some(now);
        }

        let plan = |symbol| RestRequest::DepthSnapshot {
            symbol,
            limit: DEPTH_SNAPSHOT_LIMIT,
        };
        let Some((json, ctx)) = self.read_rest(instrument, "depth snapshot", plan).await else {
            return;
        };
        let Some(snapshot) = self.parsed(parse_depth_snapshot(&json, ctx)) else { return };
        if let Some(state) = self.depth_states.get_mut(&instrument) {
            state.pending_snapshot = Some(snapshot);
        }
    }
}

fn tapped_into(out: &mut Vec<TappedMessage>) -> impl FnMut(InboundMessage, VenueMeta) {
    move |message, venue_meta| {
        out.push(TappedMessage {
            message,
            venue_meta,
        })
    }
}

fn chain_rule(market: BinanceMarket) -> ChainRule {
    match market {
        BinanceMarket::Spot => ChainRule::Spot,
        BinanceMarket::Perpetual => ChainRule::Perp,
    }
}
