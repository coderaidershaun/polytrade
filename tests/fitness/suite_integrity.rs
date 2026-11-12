//! This suite is meant to be immortal. Nothing in Rust makes that true: a suite stops running the
//! moment its `mod` line goes, and the tests that would have objected are the ones just switched
//! off. That is not hypothetical — 55 of 67 modules once sat undeclared for days while the gate
//! stayed green, taking the zero-allocation counting allocator and the suite's own named anchors
//! with them.
//!
//! So the declaration list is checked against the directory instead. Deleting a suite now means
//! deleting its file, which a diff shows.

use std::path::Path;

#[test]
fn every_fitness_module_on_disk_is_declared_and_therefore_runs() {
    assert_undeclared_is_empty(
        "tests/fitness",
        "tests/fitness/main.rs",
        &["main.rs", "suite_integrity.rs"],
    );
    assert_undeclared_is_empty(
        "tests/fitness/quant",
        "tests/fitness/quant/mod.rs",
        &["mod.rs"],
    );
}

/// `#[path]` includes and subdirectory modules are declared without a matching sibling file, so the
/// check runs one way only: every file is declared, never every declaration has a file.
fn assert_undeclared_is_empty(dir: &str, declaring_file: &str, exempt: &[&str]) {
    let declared = declared_modules(declaring_file);
    let mut undeclared: Vec<String> = module_files(dir, exempt)
        .into_iter()
        .filter(|module| !declared.contains(module))
        .collect();
    undeclared.sort();
    assert!(
        undeclared.is_empty(),
        "{dir} holds test files that no `mod` line reaches, so they compile nowhere and run \
         never: {undeclared:?} — declare them in {declaring_file} or delete the files"
    );
}

fn declared_modules(declaring_file: &str) -> Vec<String> {
    let source = std::fs::read_to_string(declaring_file)
        .unwrap_or_else(|error| panic!("{declaring_file} is readable: {error}"));
    source
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("mod "))
        .filter_map(|rest| rest.strip_suffix(';'))
        .map(str::to_owned)
        .collect()
}

fn module_files(dir: &str, exempt: &[&str]) -> Vec<String> {
    let entries = std::fs::read_dir(Path::new(dir))
        .unwrap_or_else(|error| panic!("{dir} is readable: {error}"));
    entries
        .map(|entry| entry.expect("directory entry is readable").file_name())
        .filter_map(|name| name.to_str().map(str::to_owned))
        .filter(|name| name.ends_with(".rs") && !exempt.contains(&name.as_str()))
        .map(|name| name.trim_end_matches(".rs").to_owned())
        .collect()
}
