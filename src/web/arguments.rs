//! What `polysim-web` is told on its command line: which engines to relay to, and where to serve.

use std::net::{AddrParseError, SocketAddr};
use std::num::ParseIntError;
use std::path::PathBuf;

use crate::config::{ConfigError, StrategyId};

const DEFAULT_PORT: u16 = 7879;
const DEFAULT_BUNDLE: &str = "browser/dist";

/// Deliberately not `Debug`: it carries the link token.
pub struct WebConfig {
    pub(super) port: u16,
    pub(super) bundle_dir: PathBuf,
    pub(super) mcp_url: Option<String>,
    pub(super) strategy_id: StrategyId,
    /// Engines a tab can move between. Each tab follows one at a time.
    pub(super) peers: Vec<SocketAddr>,
    pub(super) token: Option<Box<str>>,
}

#[derive(thiserror::Error, Debug)]
pub enum ArgumentError {
    #[error("{flag} needs a value")]
    MissingValue { flag: &'static str },
    #[error("--link {raw:?} is not a host:port address")]
    Link {
        raw: String,
        #[source]
        source: AddrParseError,
    },
    #[error("--strategy {raw:?} is not a valid identifier")]
    Strategy {
        raw: String,
        #[source]
        source: ConfigError,
    },
    #[error("--port {raw:?} is not a port from 0 to 65535")]
    Port {
        raw: String,
        #[source]
        source: ParseIntError,
    },
    #[error("unknown argument {argument:?}")]
    Unknown { argument: String },
    #[error("--strategy is required")]
    MissingStrategy,
    #[error("at least one --link address is required")]
    NoLink,
}

impl WebConfig {
    /// `None` when help was asked for.
    ///
    /// # Errors
    /// [`ArgumentError::MissingValue`] when a flag ends the arguments, [`ArgumentError::Link`],
    /// [`ArgumentError::Strategy`] and [`ArgumentError::Port`] for a value that does not parse,
    /// [`ArgumentError::Unknown`] for a flag nobody reads, and [`ArgumentError::MissingStrategy`]
    /// or [`ArgumentError::NoLink`] when the engine is not named.
    pub fn parse(
        mut arguments: impl Iterator<Item = String>,
    ) -> Result<Option<Self>, ArgumentError> {
        let mut peers: Vec<SocketAddr> = Vec::new();
        let mut strategy_id = None;
        let mut token = None;
        let mut port = DEFAULT_PORT;
        let mut bundle_dir = PathBuf::from(DEFAULT_BUNDLE);
        let mut mcp_url = None;
        while let Some(flag) = arguments.next() {
            match flag.as_str() {
                "--help" | "-h" => return Ok(None),
                "--link" => {
                    let raw = take_value(&mut arguments, "--link")?;
                    peers.push(
                        raw.parse()
                            .map_err(|source| ArgumentError::Link { raw, source })?,
                    );
                }
                "--strategy" => {
                    let raw = take_value(&mut arguments, "--strategy")?;
                    strategy_id = Some(
                        StrategyId::new(&raw)
                            .map_err(|source| ArgumentError::Strategy { raw, source })?,
                    );
                }
                "--token" => token = Some(take_value(&mut arguments, "--token")?.into_boxed_str()),
                "--port" => {
                    let raw = take_value(&mut arguments, "--port")?;
                    port = raw
                        .parse()
                        .map_err(|source| ArgumentError::Port { raw, source })?;
                }
                "--bundle" => bundle_dir = PathBuf::from(take_value(&mut arguments, "--bundle")?),
                "--mcp" => mcp_url = Some(take_value(&mut arguments, "--mcp")?),
                _ => {
                    return Err(ArgumentError::Unknown {
                        argument: redact_value(flag),
                    });
                }
            }
        }
        let strategy_id = strategy_id.ok_or(ArgumentError::MissingStrategy)?;
        if peers.is_empty() {
            return Err(ArgumentError::NoLink);
        }
        Ok(Some(Self {
            port,
            bundle_dir,
            mcp_url,
            strategy_id,
            peers,
            token,
        }))
    }
}

fn take_value(
    arguments: &mut impl Iterator<Item = String>,
    flag: &'static str,
) -> Result<String, ArgumentError> {
    arguments.next().ok_or(ArgumentError::MissingValue { flag })
}

/// An unknown `--name=value` is echoed without its value, since the value may be the link token,
/// which is never printed.
fn redact_value(argument: String) -> String {
    match argument.split_once('=') {
        Some((name, _)) => format!("{name}=..."),
        None => argument,
    }
}
