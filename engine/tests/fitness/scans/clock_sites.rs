//! Message time must come from one clock. Two clocks can stamp the same moment apart, and then a
//! replay folds an order of messages that the live run never saw. So each place in the library that
//! starts an engine clock is named here, with the reason that it may.

use std::collections::BTreeMap;
use std::path::Path;

use crate::scans::rust_sources;

const CLOCK_START: &str = "EngineClock::start(";

/// Each file of the library that starts an engine clock one time, and why.
const CLOCK_STARTS: [(&str, &str); 5] = [
    (
        "src/runtime/assembly/mod.rs",
        "the one clock of the process, which every producer of message stamps reads",
    ),
    (
        "src/hot/dispatch/mod.rs",
        "the fold's own clock, which times only its latency figures, until the fold reads no clock",
    ),
    (
        "src/venues/binance/rest/signed.rs",
        "the signing clock, which learns the venue's offset and stamps signed requests, not messages",
    ),
    (
        "src/venues/polymarket/preflight.rs",
        "one read at start to find the current market window, before any message exists",
    ),
    (
        "src/operator/workstation/browser/app.rs",
        "the display clock of the browser workstation, which runs in another process",
    ),
];

#[test]
fn one_engine_clock_stamps_every_producer() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut starts: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for path in rust_sources(&root.join("src")) {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
        let relative = path
            .strip_prefix(root)
            .expect("each source sits below the package")
            .to_string_lossy()
            .into_owned();
        for (number, line) in source.lines().enumerate() {
            let code = line.split("//").next().unwrap_or(line);
            if code.contains(CLOCK_START) {
                starts.entry(relative.clone()).or_default().push(number + 1);
            }
        }
    }

    let mut problems = Vec::new();
    for (file, lines) in &starts {
        let is_listed = CLOCK_STARTS.iter().any(|(listed, _)| listed == file);
        match (is_listed, lines.as_slice()) {
            (true, [_]) => {}
            (true, _) => problems.push(format!("{file} starts {} clocks: {lines:?}", lines.len())),
            (false, _) => problems.push(format!("{file} starts a clock at lines {lines:?}")),
        }
    }
    for (listed, reason) in CLOCK_STARTS {
        if !starts.contains_key(listed) {
            problems.push(format!("{listed} no longer starts a clock ({reason})"));
        }
    }
    assert!(
        problems.is_empty(),
        "each producer of message stamps must read the one clock that the engine start builds and \
         passes on, or two time bases stamp the same moment apart. Pass that clock in, or name the \
         new site in the list with its reason:\n{}",
        problems.join("\n")
    );
}
