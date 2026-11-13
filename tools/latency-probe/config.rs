//! The config of the harness's full start, from its one `--config` argument, checked before
//! anything runs. It refuses execution live, which would load the venue keys and sign a call.

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
    /// The engine starts on the first config that its arguments name, so any other argument could
    /// carry a config that the harness never checked.
    #[error(
        "latency-probe stages takes exactly `--config PATH`, got {given:?} - a second config, or the flag in another form, could start an engine on a file that the harness never checked"
    )]
    StagesArguments { given: Vec<String> },
    #[error("cannot load the harness config {}", path.display())]
    Config {
        path: PathBuf,
        #[source]
        source: ConfigError,
    },
}

/// The one config of mode `stages`, from the arguments after the mode.
///
/// # Errors
/// [`ProbeError::StagesArguments`] for anything but one `--config PATH`.
pub fn stages_config_path(arguments: &[String]) -> Result<PathBuf, ProbeError> {
    match arguments {
        [flag, path] if flag == "--config" && !path.starts_with("--") => Ok(PathBuf::from(path)),
        _ => Err(ProbeError::StagesArguments {
            given: arguments.to_vec(),
        }),
    }
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
