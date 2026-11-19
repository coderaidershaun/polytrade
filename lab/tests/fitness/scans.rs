//! A replay folds on one thread with no async runtime: the scheduler of a runtime would decide the
//! order of the work, and a replay must fold in the order of its tape alone.

use std::fs;
use std::path::{Path, PathBuf};

const BACKTEST: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/backtest");

/// Each name of the runtime that the engine's edges use. Clock reads are refused by the lint of
/// the workspace, so this scan reads names only.
const FORBIDDEN: &[&str] = &["tokio"];

fn sources(folder: &Path, into: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(folder)
        .unwrap_or_else(|error| panic!("cannot list {}: {error}", folder.display()));
    for entry in entries {
        let path = entry.expect("a folder entry can be read").path();
        if path.is_dir() {
            sources(&path, into);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            into.push(path);
        }
    }
}

/// FITNESS: no file of the replay names an async runtime. The test of equal folds runs on one
/// thread, so a runtime that crept in would pass it and still reorder a replay of a busy tape.
#[test]
fn backtest_names_no_async_runtime() {
    let mut files = Vec::new();
    sources(Path::new(BACKTEST), &mut files);
    assert!(
        !files.is_empty(),
        "{BACKTEST} holds no source file, so this scan reads nothing"
    );
    let mut found = Vec::new();
    for file in &files {
        let text = fs::read_to_string(file)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", file.display()));
        for (number, line) in text.lines().enumerate() {
            for name in FORBIDDEN.iter().filter(|name| line.contains(*name)) {
                found.push(format!("{}:{}: names `{name}`", file.display(), number + 1));
            }
        }
    }
    assert!(
        found.is_empty(),
        "the replay must fold on one thread:\n{}",
        found.join("\n")
    );
}
