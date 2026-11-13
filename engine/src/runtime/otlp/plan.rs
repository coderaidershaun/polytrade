//! The check of the `otlp:` block before any thread starts: a collector on this host's loopback,
//! and room inside the engine's drain deadline for the exporter to stop.

use std::net::SocketAddr;

use crate::msg::failure::{FailureClass, Halt};
use crate::runtime::config::{Config, ExecutionMode, RunIdentity};

use super::STOP_LIMIT_MS;

/// Where the exporter sends, and the service name it sends under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpPlan {
    pub(super) collector: SocketAddr,
    /// The artifact stem of the run, so the name of a simulated run ends in `-sim`.
    pub(super) service: Box<str>,
}

impl OtlpPlan {
    /// `None` when the config has no `otlp:` block: the run exports nothing. It starts nothing and
    /// reads no network.
    ///
    /// # Errors
    /// [`OtlpError::CollectorNotLoopback`] when the collector is not a port on this host's
    /// loopback, and [`OtlpError::DrainDeadline`] when the log drain and the stop of the exporter
    /// together do not end before `engine.drain_deadline_ms`.
    pub fn check<P>(
        config: &Config<P>,
        identity: &RunIdentity,
        mode: Option<ExecutionMode>,
    ) -> Result<Option<OtlpPlan>, OtlpError> {
        let Some(otlp) = &config.otlp else {
            return Ok(None);
        };
        let collector = otlp.collector;
        if !collector.ip().is_loopback() || collector.port() == 0 {
            return Err(OtlpError::CollectorNotLoopback { collector });
        }
        let log_deadline_ms = config.logging.drain_deadline_ms;
        let engine_deadline_ms = config.engine.drain_deadline_ms;
        if log_deadline_ms.saturating_add(STOP_LIMIT_MS) >= engine_deadline_ms {
            return Err(OtlpError::DrainDeadline {
                log_deadline_ms,
                export_stop_ms: STOP_LIMIT_MS,
                engine_deadline_ms,
            });
        }
        Ok(Some(OtlpPlan {
            collector,
            service: ExecutionMode::artifact_stem(mode, identity).into_boxed_str(),
        }))
    }
}

/// An `otlp:` block that preflight refuses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OtlpError {
    #[error(
        "otlp.collector {collector} is not a port on this host's loopback — the exporter sends \
         plain http with no authentication, so it speaks only to a collector on 127.0.0.1 or \
         [::1]; name the collector's OTLP/HTTP port, 4318 by default"
    )]
    CollectorNotLoopback { collector: SocketAddr },
    #[error(
        "logging.drain_deadline_ms = {log_deadline_ms} plus the {export_stop_ms} ms that the otlp \
         exporter can take to stop is not below engine.drain_deadline_ms = {engine_deadline_ms} — \
         the drain watchdog would cut the last export or the last log lines off; set \
         logging.drain_deadline_ms below {limit}",
        limit = .engine_deadline_ms.saturating_sub(*.export_stop_ms)
    )]
    DrainDeadline {
        log_deadline_ms: u64,
        export_stop_ms: u64,
        engine_deadline_ms: u64,
    },
}

impl OtlpError {
    pub const fn failure_class(&self) -> FailureClass {
        match self {
            OtlpError::CollectorNotLoopback { .. } | OtlpError::DrainDeadline { .. } => {
                FailureClass::Halt(Halt::Start)
            }
        }
    }
}
