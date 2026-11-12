//! The four tools an agent can call, each a thin shell over the fleet: read the parameters, make
//! one fleet call, and hand back its reply or its refusal.

use std::sync::Arc;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{ContentBlock, IntoContents};
use rmcp::schemars::JsonSchema;
use rmcp::{Json, ServerHandler, tool, tool_handler, tool_router};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::config::{ExecutionMode, RunIdentity};

use super::error::FleetError;
use super::fleet::{Fleet, StopOutcome};
use super::reply::{EngineListing, ModeChange, Started, Stopped};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct EngineParams {
    strategy_id: String,
    te_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
struct ModeParams {
    strategy_id: String,
    te_id: String,
    mode: ExecutionMode,
}

/// Every clone drives the same fleet, so the session and the final stop share one registry.
#[derive(Debug, Clone)]
pub struct PolysimMcp {
    fleet: Arc<Mutex<Fleet>>,
}

impl PolysimMcp {
    pub fn new(fleet: Fleet) -> PolysimMcp {
        PolysimMcp {
            fleet: Arc::new(Mutex::new(fleet)),
        }
    }

    pub(super) async fn stop_all(&self) -> Vec<StopOutcome> {
        self.fleet.lock().await.stop_all().await
    }
}

#[tool_router]
impl PolysimMcp {
    #[tool(
        description = "List every trading engine under strategies/ on this host, with its execution mode (off, sim, live, absent or unreadable) and whether this server is running it.",
        annotations(read_only_hint = true, idempotent_hint = true)
    )]
    async fn list_strategies(&self) -> Result<Json<EngineListing>, FleetError> {
        self.fleet.lock().await.list().map(Json)
    }

    #[tool(
        description = "Start one trading engine as a child process of this server. The engine reads its config.yaml at boot, so set its mode first. Fails if it is already running, if its binary is not built, or if it exits during start-up.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false
        )
    )]
    async fn start_strategy(
        &self,
        Parameters(params): Parameters<EngineParams>,
    ) -> Result<Json<Started>, FleetError> {
        let identity = identity_of(&params.strategy_id, &params.te_id)?;
        self.fleet.lock().await.start(&identity).await.map(Json)
    }

    #[tool(
        description = "Stop a running trading engine: SIGTERM so it drains cleanly, then SIGKILL if it outlives the stop deadline.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = false
        )
    )]
    async fn stop_strategy(
        &self,
        Parameters(params): Parameters<EngineParams>,
    ) -> Result<Json<Stopped>, FleetError> {
        let identity = identity_of(&params.strategy_id, &params.te_id)?;
        self.fleet.lock().await.stop(&identity).await.map(Json)
    }

    #[tool(
        description = "Rewrite the execution mode (off, sim or live) in a stopped engine's config.yaml. live trades real money.",
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true
        )
    )]
    async fn set_execution_mode(
        &self,
        Parameters(params): Parameters<ModeParams>,
    ) -> Result<Json<ModeChange>, FleetError> {
        let identity = identity_of(&params.strategy_id, &params.te_id)?;
        self.fleet
            .lock()
            .await
            .set_mode(&identity, params.mode)
            .map(Json)
    }
}

#[tool_handler(
    name = "polysim-mcp",
    instructions = "polysim-mcp operates the trading engines checked out at this repo root. Each engine is a separate process named <strategy_id>-<te_id>; build it first with `cargo build --release --bin <strategy_id>-<te_id>`. list_strategies shows every engine with its execution mode and whether it is running. start_strategy spawns the prebuilt binary; stop_strategy sends SIGTERM and waits for the engine's own drain, forcing only past the deadline. set_execution_mode rewrites execution.mode in the engine's config.yaml and is refused while that engine runs; the mode is read at boot, so set it before starting. Mode live trades real money and this server does not guard it: confirm with the operator before setting live or starting a live engine. An engine's stdout and stderr go to its console log path, returned by every call, for diagnosis."
)]
impl ServerHandler for PolysimMcp {}

/// The refusal text carries the whole cause chain, because an agent sees only this text.
impl IntoContents for FleetError {
    fn into_contents(self) -> Vec<ContentBlock> {
        let mut text = self.to_string();
        let mut cause = std::error::Error::source(&self);
        while let Some(error) = cause {
            text.push_str(": ");
            text.push_str(&error.to_string());
            cause = error.source();
        }
        vec![ContentBlock::text(text)]
    }
}

fn identity_of(strategy_id: &str, te_id: &str) -> Result<RunIdentity, FleetError> {
    RunIdentity::new(strategy_id, te_id).map_err(|source| FleetError::InvalidIdentity { source })
}
