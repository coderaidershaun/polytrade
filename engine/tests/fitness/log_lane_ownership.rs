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

use crate::exec_no_clock::rust_sources;

/// This engine's complete OS-thread census: the process entry, the single hot thread, and the two
/// dedicated output threads. Everything else that runs concurrently here is a tokio task. The
/// browser workstation runs on one thread with no log lane.
const THREAD_LANE_OWNERS: [(&str, &str); 4] = [
    ("src/exposure/writer.rs", "exposure"),
    ("src/hot/spawn.rs", "config.tag"),
    ("src/persist/drain.rs", "persist"),
    ("src/runtime/mod.rs", "main"),
];

const CALL_MARKER: &str = "log::register_thread(";

#[test]
fn every_lane_registered_by_thread_belongs_to_an_os_thread_this_engine_owns() {
    let mut found = registration_sites(Path::new("src"));
    found.sort();
    let expected: Vec<(String, String)> = THREAD_LANE_OWNERS
        .iter()
        .map(|(path, tag)| ((*path).to_owned(), (*tag).to_owned()))
        .collect();
    assert_eq!(
        found, expected,
        "the `log::register_thread` call sites under src/ no longer match the OS-thread census. A \
         tokio task must instead be wrapped at its spawn site — `rt.spawn(log::tag_task(tag, \
         body))` — because a task registering by thread files its records under whichever tag its \
         worker last saw. If this genuinely is a new OS thread the engine owns, name it in \
         THREAD_LANE_OWNERS"
    );
}

fn registration_sites(dir: &Path) -> Vec<(String, String)> {
    let mut sites = Vec::new();
    for path in rust_sources(dir) {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} is readable: {error}", path.display()));
        let file = path.to_str().expect("source paths are utf-8").to_owned();
        sites.extend(
            source
                .lines()
                .filter_map(tag_argument)
                .map(|tag| (file.clone(), tag)),
        );
    }
    sites
}

fn tag_argument(line: &str) -> Option<String> {
    let (_, rest) = line.split_once(CALL_MARKER)?;
    let (argument, _) = rest.split_once(')')?;
    Some(argument.trim_matches('"').to_owned())
}
