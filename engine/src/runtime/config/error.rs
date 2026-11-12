//! Every way a config file can be refused, in the caller's vocabulary: the field it named, the
//! value it gave, and what was expected instead.

use std::path::PathBuf;

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("config file unreadable at {}", .path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("config parse failed: {detail}")]
    Parse { detail: Box<str> },
    #[error("{kind} {raw:?} invalid: {reason}")]
    Identifier {
        kind: &'static str,
        raw: Box<str>,
        reason: &'static str,
    },
    #[error("engine.hot_core_id is required on linux — add a core id to pin the hot thread")]
    MissingHotCoreId,
    #[error(
        "strategy.tables names {tables} but there is no persistence: block to write them into — add one, or drop the tables list to run without persistence"
    )]
    TablesWithoutPersistence { tables: Box<str> },
    #[error(
        "link.bind {bind} is reachable from outside a private network — the link has no authentication, so anyone who can reach it can stop this engine and inject signals; bind a loopback/private/tailnet address, or set link.allow_public_bind: true if something else is the boundary"
    )]
    PublicLinkBind { bind: Box<str> },
    #[error("link.subscribe names {count} peers, max {max}")]
    TooManyLinkPeers { count: usize, max: usize },
    #[error(
        "link.subscribe[{address}].topics names {topic:?}, which is neither an engine topic nor one this strategy declares in link_topics()"
    )]
    UnknownLinkTopic { address: Box<str>, topic: Box<str> },
    #[error("link.subscribe[{address}] names {count} topics, max {max}")]
    TooManyPeerTopics {
        address: Box<str>,
        count: usize,
        max: usize,
    },
    #[error("{field} = {value:?}, expected {expected}")]
    Invalid {
        /// Dotted path from the document root, so the message points at one line of one block.
        field: &'static str,
        value: Box<str>,
        expected: &'static str,
    },
    #[error("strategy.instruments lists {symbol} which matches no configured instrument")]
    UnknownStrategyInstrument { symbol: Box<str> },
    #[error(
        "execution.mode is {mode} but the {venue} execution edge supports only {supported} — the run would report itself armed while placing nothing"
    )]
    ExecutionModeUnsupported {
        venue: &'static str,
        mode: &'static str,
        supported: Box<str>,
    },
    #[error(
        "execution.mode is sim but source.market is {market} — the simulated venue models binance spot only, and spot is the market whose 100ms depth granularity and aggregate trades its fill model is built on"
    )]
    SimulatedExecutionMarket { market: &'static str },
    #[error(
        "execution.mode is sim but source.subscriptions disables {missing} — the simulated venue matches against the market data it is given, so a missing stream is a venue that reports itself armed and fills nothing"
    )]
    SimulatedExecutionSubscriptions { missing: Box<str> },
}
