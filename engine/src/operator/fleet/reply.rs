//! The success replies, exactly as a client reads them: every field is always present, and a
//! value that does not apply is null rather than missing.

use rmcp::schemars::JsonSchema;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct EngineListing {
    pub engines: Vec<EngineSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct EngineSummary {
    pub strategy_id: String,
    pub te_id: String,
    pub bin: String,
    pub mode: &'static str,
    pub state: &'static str,
    pub pid: Option<u32>,
    pub started_at: Option<String>,
    pub exit_code: Option<i32>,
    pub console_log: String,
    pub link_bind: Option<String>,
    pub config_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct Started {
    pub strategy_id: String,
    pub te_id: String,
    pub pid: u32,
    pub mode: &'static str,
    pub console_log: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct Stopped {
    pub strategy_id: String,
    pub te_id: String,
    pub exit_code: Option<i32>,
    pub forced: bool,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct ModeChange {
    pub strategy_id: String,
    pub te_id: String,
    pub previous: &'static str,
    pub mode: &'static str,
    pub config_path: String,
}
