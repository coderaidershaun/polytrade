//! The `logging:` block: where the log file goes.

use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    #[serde(default = "default_logs_dir")]
    pub dir: PathBuf,
}

fn default_logs_dir() -> PathBuf {
    PathBuf::from("./logs")
}
