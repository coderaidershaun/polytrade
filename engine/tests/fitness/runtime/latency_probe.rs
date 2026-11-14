//! The timing harness refuses a config that would start a live engine. A live start loads the keys
//! and signs, and nobody watches the harness, so the refusal is code, not a rule for a person.

#[path = "../../../../tools/latency-probe/config.rs"]
mod probe_config;

use std::path::{Path, PathBuf};

use engine::runtime::config::ExecutionMode;
use probe_config::{ProbeError, load_harness_config, stages_config_path};

/// The config that the harness ships for its full start, which runs with execution off.
const STAGES_CONFIG: &str = include_str!("../../../../tools/latency-probe/stages.yaml");

/// The fields that a live execution block cannot leave out, inside the one source of the harness.
const LIVE_EXECUTION: &str = "    execution:
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
    let armed = STAGES_CONFIG.replacen("strategy:\n", &format!("{LIVE_EXECUTION}strategy:\n"), 1);
    let live = written("live", &armed);
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
            .execution_mode()
            .is_none_or(|mode| mode != ExecutionMode::Live),
        "the shipped harness config never runs live"
    );

    // The engine starts on the first config that its arguments name, so the harness takes exactly
    // one, in one form, and checks that one.
    let arguments =
        |given: &[&str]| -> Vec<String> { given.iter().map(|arg| (*arg).to_owned()).collect() };
    assert!(
        matches!(
            stages_config_path(&arguments(&["--config", "shipped.yaml"])),
            Ok(path) if path == Path::new("shipped.yaml")
        ),
        "one --config names the config that is checked and used"
    );
    let smuggled: [&[&str]; 4] = [
        &["--config", "shipped.yaml", "--config", "live.yaml"],
        &["--config=live.yaml", "--config", "shipped.yaml"],
        &["--config", "shipped.yaml", "--config=live.yaml"],
        &[],
    ];
    for given in smuggled {
        let refusal = stages_config_path(&arguments(given));
        assert!(
            matches!(refusal, Err(ProbeError::StagesArguments { .. })),
            "{given:?} is refused before any config loads: {refusal:?}"
        );
    }
}
