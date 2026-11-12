//! The config of the harness's full start, loaded and checked before anything else runs. A start
//! with execution live would load the venue keys and sign a call, so the check refuses it here.

use std::path::{Path, PathBuf};

use engine::runtime::config::{Config, ConfigError, ExecutionMode, NoParams};

#[derive(thiserror::Error, Debug)]
pub enum ProbeError {
    #[error(
        "{} sets execution {} - the harness never starts a live engine; set execution off or sim",
        path.display(),
        mode.as_str()
    )]
    LiveExecution { path: PathBuf, mode: ExecutionMode },
    #[error("cannot load the harness config {}", path.display())]
    Config {
        path: PathBuf,
        #[source]
        source: ConfigError,
    },
}

/// # Errors
/// [`ProbeError::LiveExecution`] for a config whose execution mode is live, and
/// [`ProbeError::Config`] for a file that does not load.
pub fn load_harness_config(path: &Path) -> Result<Config<NoParams>, ProbeError> {
    let config: Config<NoParams> = Config::load(path).map_err(|source| ProbeError::Config {
        path: path.to_path_buf(),
        source,
    })?;
    let mode = config.execution.as_ref().map(|execution| execution.mode);
    if mode == Some(ExecutionMode::Live) {
        return Err(ProbeError::LiveExecution {
            path: path.to_path_buf(),
            mode: ExecutionMode::Live,
        });
    }
    Ok(config)
}
