//! The check of the `otlp:` block before any thread starts: a collector on this host's loopback,
//! and room for the exporter to stop in the part of the drain deadline that the exit sweep leaves.

use std::net::SocketAddr;

use crate::msg::failure::{FailureClass, Halt};
use crate::runtime::assembly::SWEEP_SHARE_OF_DRAIN;
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
    /// loopback, and [`OtlpError::DrainDeadline`] when the stop of the exporter and the log drain
    /// together do not fit in the part of `engine.drain_deadline_ms` that the exit sweep leaves.
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
        // The exit sweep runs first and can take its share of the deadline. Rounding that share up
        // never states more room than the sweep leaves.
        let sweep_ms = engine_deadline_ms.div_ceil(u64::from(SWEEP_SHARE_OF_DRAIN));
        let room_ms = engine_deadline_ms.saturating_sub(sweep_ms);
        if log_deadline_ms.saturating_add(STOP_LIMIT_MS) >= room_ms {
            return Err(OtlpError::DrainDeadline {
                log_deadline_ms,
                export_stop_ms: STOP_LIMIT_MS,
                engine_deadline_ms,
                sweep_ms,
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
         plain http with no authentication, so it speaks only to a collector on a loopback \
         address (127.0.0.0/8 or [::1]); name the collector's OTLP/HTTP port, 4318 by default"
    )]
    CollectorNotLoopback { collector: SocketAddr },
    #[error(
        "logging.drain_deadline_ms = {log_deadline_ms} plus the {export_stop_ms} ms that the otlp \
         exporter can take to stop is not below the {room} ms of engine.drain_deadline_ms = \
         {engine_deadline_ms} that the exit sweep leaves ({sweep_ms} ms go to the sweep) — the \
         drain watchdog would cut the last export or the last log lines off; set \
         logging.drain_deadline_ms below {limit}, or raise engine.drain_deadline_ms",
        room = .engine_deadline_ms.saturating_sub(*.sweep_ms),
        limit = .engine_deadline_ms.saturating_sub(*.sweep_ms).saturating_sub(*.export_stop_ms)
    )]
    DrainDeadline {
        log_deadline_ms: u64,
        export_stop_ms: u64,
        engine_deadline_ms: u64,
        /// The share of `engine_deadline_ms` that the exit sweep can take before the exporter
        /// stops.
        sweep_ms: u64,
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
