//! Message time must come from one clock. Two clocks can stamp the same moment apart, and then a
//! replay folds an order of messages that the live run never saw. So each place in the library that
//! starts an engine clock is named here, with the reason that it may, and so is each file that names
//! the latency tick a command carries.

use std::collections::BTreeMap;
use std::path::Path;

use crate::operator::rust_sources;

const CLOCK_START: &str = "EngineClock::start(";

/// Each file of the library that starts an engine clock one time, and why.
const CLOCK_STARTS: [(&str, &str); 4] = [
    (
        "src/runtime/assembly/mod.rs",
        "the one clock of the process, which every producer of message stamps reads",
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

const ISSUE_TICK: &str = "issue_tick";

/// Each file of the production code that names the issue tick, and what it does with it. The tick
/// is a latency stamp: a reader of it outside a sender could order or time a command by a counter
/// reading, and replay would then send other orders than the live run sent.
const ISSUE_TICK_SITES: [(&str, &str); 5] = [
    (
        "engine/src/msg/exec.rs",
        "declares the tick on the lane item that carries a command to its edge",
    ),
    (
        "engine/src/venues/edge/sender.rs",
        "copies the tick of each command it sends into that command's wire sample",
    ),
    (
        "engine/src/hot/sink/exec.rs",
        "reads the counter when a command enters the bank and hands the tick to the lane",
    ),
    (
        "engine/src/msg/latency.rs",
        "declares the tick on the wire sample that a sender writes",
    ),
    (
        "engine/src/runtime/assembly/metrics/stage.rs",
        "subtracts the ticks of a wire sample on the metrics thread",
    ),
];

#[test]
fn issue_tick_is_read_only_by_senders() {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = package.parent().expect("the package sits in the workspace");
    let roots = ["engine/src", "strategies", "tools", "lab/src"];
    let mut naming: Vec<String> = Vec::new();
    for root in roots {
        for path in rust_sources(&workspace.join(root)) {
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
            let names_it = source
                .lines()
                .map(|line| line.split("//").next().unwrap_or(line))
                .any(|code| code.contains(ISSUE_TICK));
            if names_it {
                let relative = path
                    .strip_prefix(workspace)
                    .expect("each source sits below the workspace")
                    .to_string_lossy()
                    .into_owned();
                naming.push(relative);
            }
        }
    }
    naming.sort();
    let mut listed: Vec<String> = ISSUE_TICK_SITES
        .iter()
        .map(|(file, _)| (*file).to_owned())
        .collect();
    listed.sort();
    assert_eq!(
        naming, listed,
        "the issue tick of a command is a latency stamp for the sender alone. The fold, a strategy \
         and the simulator must never read it; a new file that does needs its reason in the list"
    );
}
