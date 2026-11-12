//! `log::register_thread` scopes a log lane to the calling OS THREAD, and no Rust signature can
//! stop a tokio task body from calling it. Tasks share the multi-worker pool, so two landing on one
//! worker overwrite each other's tag, and a task migrating at an `.await` pushes into whichever
//! ring that worker had last registered, so a Binance REST warning could be filed under
//! `[polymarket-exec]`. One log file carries engine and strategy records
//! alike, and the tag column is the only thing distinguishing their origin, so the attribution is
//! load-bearing rather than cosmetic.
//!
//! The rule is invisible at the call site, so the threads are named here one at a time. A new call
//! site fails until someone states which OS thread it owns, and a task author is pointed at
//! `log::tag_task` instead.

use std::path::Path;

use crate::scans::exec_no_clock::rust_sources;

/// This engine's complete census of thread lanes: the text lane of each OS thread it owns (the
/// process entry, the single hot thread, the metrics thread, and the two dedicated output
/// threads), and the hot lane of the hot thread. Everything else that runs concurrently here is a
/// tokio task. The browser workstation runs on one thread with no log lane. Each entry is (call,
/// file, tag).
const LANE_OWNERS: [(&str, &str, &str); 6] = [
    (TEXT_LANE_CALL, "src/hot/spawn.rs", "config.tag"),
    (
        TEXT_LANE_CALL,
        "src/runtime/assembly/metrics/mod.rs",
        "metrics",
    ),
    (TEXT_LANE_CALL, "src/runtime/assembly/mod.rs", "main"),
    (TEXT_LANE_CALL, "src/runtime/exposure/writer.rs", "exposure"),
    (TEXT_LANE_CALL, "src/tape/drain.rs", "persist"),
    (HOT_LANE_CALL, "src/hot/spawn.rs", "config.tag"),
];

const TEXT_LANE_CALL: &str = "log::register_thread(";
const HOT_LANE_CALL: &str = "log::register_hot_lane(";

/// Calls that put a lane on a thread with no tag and no drain. The engine never makes them: they
/// exist for a caller that runs its own drain.
const UNTAGGED_LANE_CALLS: [&str; 2] = ["log::open_hot_lane(", "log::open_text_lane("];

#[test]
fn every_thread_lane_belongs_to_an_owned_thread() {
    let calls = [TEXT_LANE_CALL, HOT_LANE_CALL]
        .into_iter()
        .chain(UNTAGGED_LANE_CALLS);
    let mut found: Vec<(String, String, String)> = calls
        .flat_map(|call| registration_sites(Path::new("src"), call))
        .collect();
    found.sort();
    let mut expected: Vec<(String, String, String)> = LANE_OWNERS
        .iter()
        .map(|(call, path, tag)| ((*call).to_owned(), (*path).to_owned(), (*tag).to_owned()))
        .collect();
    expected.sort();
    assert_eq!(
        found, expected,
        "the lane call sites under src/ no longer match the census of thread lanes. A tokio task \
         must instead be wrapped at its spawn site — `rt.spawn(log::tag_task(tag, body))` — \
         because a task registering by thread files its records under whichever tag its worker \
         last saw. If this genuinely is a new OS thread the engine owns, name it in LANE_OWNERS"
    );
}

fn registration_sites(dir: &Path, call: &str) -> Vec<(String, String, String)> {
    let mut sites = Vec::new();
    for path in rust_sources(dir) {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} is readable: {error}", path.display()));
        let file = path.to_str().expect("source paths are utf-8").to_owned();
        sites.extend(
            source
                .lines()
                .filter_map(|line| tag_argument(line, call))
                .map(|tag| (call.to_owned(), file.clone(), tag)),
        );
    }
    sites
}

fn tag_argument(line: &str, call: &str) -> Option<String> {
    let (_, rest) = line.split_once(call)?;
    let (argument, _) = rest.split_once(')')?;
    Some(argument.trim_matches('"').to_owned())
}
