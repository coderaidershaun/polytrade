//! The timing harness refuses a config that would start a live engine. A full start with execution
//! live loads the venue keys and signs a call, and the harness runs where nobody watches it, so the
//! refusal is code that runs before a runtime exists, not a rule for the person who runs it.

#[path = "../../../../tools/latency-probe/config.rs"]
mod probe_config;

use std::path::PathBuf;

use engine::runtime::config::ExecutionMode;
use probe_config::{ProbeError, load_harness_config};

/// The config that the harness ships for its full start, which runs with execution off.
const STAGES_CONFIG: &str = include_str!("../../../../tools/latency-probe/stages.yaml");

/// The fields that a live execution block cannot leave out.
const LIVE_EXECUTION: &str = "
execution:
  mode: live
  min_base_balance: 0.0
  min_quote_balance: 20.0
  max_order_notional_quote: 25.0
  max_quote_distance_bps: 50.0
  max_book_age_ms: 2000
  max_session_loss_quote: 5.0
";

fn written(name: &str, contents: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "polysim-latency-probe-{}-{name}.yaml",
        std::process::id()
    ));
    std::fs::write(&path, contents)
        .unwrap_or_else(|error| panic!("cannot write {}: {error}", path.display()));
    path
}

#[test]
fn latency_probe_refuses_live_execution() {
    let live = written("live", &format!("{STAGES_CONFIG}{LIVE_EXECUTION}"));
    let shipped = written("shipped", STAGES_CONFIG);
    let refusal = load_harness_config(&live);
    let accepted = load_harness_config(&shipped);
    std::fs::remove_file(&live).ok();
    std::fs::remove_file(&shipped).ok();

    let refusal = refusal.expect_err("a config with execution live is refused");
    assert!(
        matches!(
            &refusal,
            ProbeError::LiveExecution { path, mode: ExecutionMode::Live } if *path == live
        ),
        "the refusal names the file and the mode: {refusal:?}"
    );
    let message = refusal.to_string();
    assert!(
        message.contains(&live.display().to_string()) && message.contains("live"),
        "the message names the file and the mode: {message}"
    );
    let config = accepted.expect("the shipped harness config loads");
    assert!(
        config
            .execution
            .is_none_or(|execution| execution.mode != ExecutionMode::Live),
        "the shipped harness config never runs live"
    );
}
