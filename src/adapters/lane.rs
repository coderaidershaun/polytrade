//! One reconnect loop for both market-data feeds. Each feed supplies only its session, so the rule
//! for when the back-off starts over is written once.

use crate::adapters::IDLE_POLL;
use crate::adapters::backoff::BackoffCaps;
use crate::adapters::socket::{Socket, connect};
use crate::link::RunState;
use crate::shutdown::{FatalSignal, RunStateCell};
use crate::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Flow {
    Continue,
    Reconnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionEnd {
    Stopped,
    Parked,
    /// `is_endpoint_proven` is true once the session showed the endpoint healthy, and only then
    /// does the reconnect back-off start over.
    Reconnect {
        is_endpoint_proven: bool,
    },
}

pub(crate) struct LaneSpec {
    /// How this feed's log lines begin, for example "binance adapter spot/depth".
    pub(crate) name: String,
    pub(crate) url: String,
    pub(crate) fatal: FatalSignal,
    pub(crate) run_state: RunStateCell,
    pub(crate) backoff: BackoffCaps,
}

pub(crate) trait Lane {
    async fn session(&mut self, socket: Socket) -> SessionEnd;
}

pub(crate) async fn run_lane(spec: LaneSpec, mut lane: impl Lane) {
    let mut attempt: u32 = 0;
    while !spec.fatal.is_tripped() {
        if spec.run_state.state() == RunState::Idle {
            // A parked feed is not failing, so waiting here leaves the back-off count alone.
            tokio::time::sleep(IDLE_POLL).await;
            continue;
        }
        let socket = match connect(&spec.url).await {
            Ok(socket) => socket,
            Err(error) => {
                warn!("{} connect failed: {error}", spec.name);
                tokio::time::sleep(spec.backoff.delay(attempt)).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };
        info!("{} connected", spec.name);
        match lane.session(socket).await {
            SessionEnd::Stopped => return,
            SessionEnd::Parked => {
                info!("{} parked — socket dropped", spec.name);
                attempt = 0;
            }
            SessionEnd::Reconnect { is_endpoint_proven } => {
                if is_endpoint_proven {
                    attempt = 0;
                }
                warn!("{} disconnected — reconnecting", spec.name);
                tokio::time::sleep(spec.backoff.delay(attempt)).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}
