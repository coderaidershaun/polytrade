//! The `otlp:` block: where the exporter sends a copy of each log line and each latency row. Only a
//! build with the export reads it, so a build without the export refuses the block.

use std::net::SocketAddr;

use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OtlpConfig {
    /// The port of an OTLP/HTTP collector on this host's loopback, for example `127.0.0.1:4318`.
    /// IPv6 needs quotes in YAML: `"[::1]:4318"`.
    pub collector: SocketAddr,
}
