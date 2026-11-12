//! Message time must come from one clock. Two clocks can stamp the same moment apart, and then a
//! replay folds an order of messages that the live run never saw. So each place in the library that
//! starts an engine clock is named here, with the reason that it may, and so is each file that names
//! the latency tick a command carries, and each file that the clock lint lets read a clock.

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

/// Each production file that holds a permitted clock read, and the banned calls that it makes. Each
/// site in it says why in the reason of its `#[expect(clippy::disallowed_methods, ...)]`.
const PERMITTED_CLOCK_SITES: [(&str, &[&str]); 47] = [
    (
        "engine/src/hot/metrics.rs",
        &["engine::msg::time::Stamper::stamp"],
    ),
    (
        "engine/src/hot/sink/exec.rs",
        &["engine::msg::time::Stamper::stamp"],
    ),
    (
        "engine/src/link/actor/mod.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/log/drain.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/log/lane.rs",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::boot_stamp_us",
        ],
    ),
    (
        "engine/src/msg/time.rs",
        &[
            "quanta::Clock::raw",
            "std::time::Instant::elapsed",
            "std::time::Instant::now",
            "std::time::SystemTime::now",
            "web_time::Instant::elapsed",
            "web_time::Instant::now",
            "web_time::SystemTime::now",
        ],
    ),
    (
        "engine/src/operator/fleet/child.rs",
        &["engine::msg::time::boot_stamp_us"],
    ),
    (
        "engine/src/operator/fleet/supervisor.rs",
        &["std::time::Instant::elapsed", "std::time::Instant::now"],
    ),
    (
        "engine/src/operator/workstation/browser/app.rs",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::start",
        ],
    ),
    (
        "engine/src/runtime/assembly/link.rs",
        &["engine::msg::time::boot_stamp_us"],
    ),
    (
        "engine/src/runtime/assembly/metrics/mod.rs",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::Stamper::stamp",
            "engine::msg::time::TickScale::nanos",
        ],
    ),
    (
        "engine/src/runtime/assembly/metrics/stage.rs",
        &[
            "engine::msg::time::TickScale::nanos",
            "engine::msg::time::TickScale::nanos_since",
        ],
    ),
    (
        "engine/src/runtime/assembly/mod.rs",
        &["engine::msg::time::EngineClock::start"],
    ),
    (
        "engine/src/runtime/assembly/timer.rs",
        &[
            "engine::msg::time::EngineClock::now",
            "tokio::time::Instant::elapsed",
            "tokio::time::Instant::now",
        ],
    ),
    (
        "engine/src/runtime/exposure/writer.rs",
        &["engine::msg::time::boot_stamp_us"],
    ),
    (
        "engine/src/tape/drain.rs",
        &[
            "engine::msg::time::boot_stamp_us",
            "std::time::Instant::elapsed",
            "std::time::Instant::now",
        ],
    ),
    (
        "engine/src/tape/retention.rs",
        &["std::time::SystemTime::now"],
    ),
    (
        "engine/src/venues/binance/actor/depth_resync.rs",
        &["std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/actor/kline_backfill.rs",
        &["std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/actor/liveness.rs",
        &["std::time::Instant::elapsed", "std::time::Instant::now"],
    ),
    (
        "engine/src/venues/binance/actor/mod.rs",
        &[
            "engine::msg::time::EngineClock::now",
            "std::time::Instant::now",
        ],
    ),
    (
        "engine/src/venues/binance/actor/tap.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/apply.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/inbound.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/mod.rs",
        &[
            "engine::msg::time::EngineClock::now",
            "tokio::time::Instant::now",
        ],
    ),
    (
        "engine/src/venues/binance/exec/actor/poll.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/reconcile.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/exec/actor/resync.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/binance/rest/signed.rs",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::start",
            "std::time::Instant::now",
        ],
    ),
    (
        "engine/src/venues/binance/rest/weight.rs",
        &["std::time::Instant::elapsed", "std::time::Instant::now"],
    ),
    (
        "engine/src/venues/edge/backoff.rs",
        &["std::time::SystemTime::now"],
    ),
    (
        "engine/src/venues/edge/mod.rs",
        &[
            "engine::msg::time::EngineClock::now",
            "tokio::time::Instant::now",
        ],
    ),
    (
        "engine/src/venues/edge/sender.rs",
        &["engine::msg::time::Stamper::stamp"],
    ),
    (
        "engine/src/venues/polymarket/actor/effects.rs",
        &[
            "engine::msg::time::EngineClock::now",
            "std::time::Instant::now",
        ],
    ),
    (
        "engine/src/venues/polymarket/actor/mod.rs",
        &[
            "engine::msg::time::EngineClock::now",
            "std::time::Instant::now",
        ],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/answer.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/apply.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/inbound.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/mod.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/poll.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/reads.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/rest.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/actor/resync.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/preflight.rs",
        &["std::time::SystemTime::now"],
    ),
    (
        "engine/src/venues/polymarket/exec/probe/mod.rs",
        &["std::time::SystemTime::now"],
    ),
    (
        "engine/src/venues/polymarket/preflight.rs",
        &[
            "engine::msg::time::EngineClock::now",
            "engine::msg::time::EngineClock::start",
        ],
    ),
    (
        "engine/src/venues/sim/actor.rs",
        &["engine::msg::time::EngineClock::now"],
    ),
];

const PRODUCTION_ROOTS: [&str; 4] = ["engine/src", "strategies", "tools", "lab/src"];
const LINT: &str = "clippy::disallowed_methods";
const ENGINE_PATH: &str = "engine::";

/// FITNESS: the clock lint bans each clock read, and the files that may read one are exactly these.
/// A clock read anywhere else would make hot state a function of how fast the process ran, and a
/// replay of the tape would part from the live run.
///
/// A banned path that no longer resolves is only a warning, and its ban is then gone. The sites that
/// call it would report their expectation as unfulfilled, so each banned engine path must keep one.
#[test]
fn permitted_clock_sites_equal_the_list() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the package sits in the workspace");
    let banned = banned_paths(workspace);
    let mut problems = Vec::new();
    let mut sites = Vec::new();
    for root in PRODUCTION_ROOTS {
        for path in rust_sources(&workspace.join(root)) {
            let code = code_of(&path);
            if !code.contains(LINT) {
                continue;
            }
            let relative = relative_to(&path, workspace);
            if lint_is_allowed(&code) {
                problems.push(format!(
                    "{relative} allows the clock lint: a permitted site expects it, so the gate \
                     reports the site once it reads no clock"
                ));
            }
            sites.push(relative);
        }
    }
    let listed: Vec<String> = PERMITTED_CLOCK_SITES
        .iter()
        .map(|(file, _)| (*file).to_owned())
        .collect();
    for site in &sites {
        if !listed.contains(site) {
            problems.push(format!("{site} reads a clock but is not a permitted site"));
        }
    }
    for (file, reads) in PERMITTED_CLOCK_SITES {
        if !sites.iter().any(|site| site == file) {
            problems.push(format!("{file} is listed but holds no permitted site"));
        }
        for read in reads {
            if !banned.iter().any(|path| path == read) {
                problems.push(format!("{file} lists {read}, which the lint does not ban"));
            }
        }
    }
    for path in banned.iter().filter(|path| path.starts_with(ENGINE_PATH)) {
        if !has_a_site_that_calls(workspace, path) {
            problems.push(format!(
                "the lint bans {path}, but no permitted site calls it, so a rename of it would \
                 drop its ban with every gate green"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "the clock reads of the production code no longer match the list of permitted sites. A new \
         site needs a reason in its expect attribute and a row in PERMITTED_CLOCK_SITES:\n{}",
        problems.join("\n")
    );
}

fn banned_paths(workspace: &Path) -> Vec<String> {
    let config = workspace.join("clippy.toml");
    std::fs::read_to_string(&config)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", config.display()))
        .lines()
        .filter_map(|line| line.split_once("path = \"")?.1.split_once('"'))
        .map(|(path, _)| path.to_owned())
        .collect()
}

/// A site of the list calls the banned path, so its expectation is what turns a rename red.
fn has_a_site_that_calls(workspace: &Path, banned: &str) -> bool {
    let method = banned
        .rsplit("::")
        .next()
        .expect("a path has a last segment");
    let call = format!("{method}(");
    PERMITTED_CLOCK_SITES
        .iter()
        .filter(|(_, reads)| reads.contains(&banned))
        .any(|(file, _)| code_of(&workspace.join(file)).contains(&call))
}

/// Each attribute that names the lint, from its opening `#[` to the lint's name.
fn lint_is_allowed(code: &str) -> bool {
    code.match_indices(LINT).any(|(at, _)| {
        let opening = code[..at].rfind("#[").unwrap_or(0);
        code[opening..at].contains("allow")
    })
}

fn code_of(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
        .lines()
        .map(|line| line.split("//").next().unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
}

fn relative_to(path: &Path, workspace: &Path) -> String {
    path.strip_prefix(workspace)
        .expect("each source sits below the workspace")
        .to_string_lossy()
        .into_owned()
}
