//! A test that no `mod` line reaches compiles nowhere and never runs, while the gate stays green.
//! So each test folder is checked against its declarations, and a fitness test cannot be ignored.

use std::fs;
use std::path::{Path, PathBuf};

use crate::scans::rust_sources;

const TESTS_FOLDER: &str = "tests";
const FITNESS_ROOT: &str = "tests/fitness";
const REGRESSIONS_FOLDER: &str = "tests/proptest-regressions";
const TARGET_ROOT_FILE: &str = "main.rs";

/// Each fitness test that a gate step selects by a name filter. A filter that matches no test
/// passes with zero tests, so a renamed test would switch that step off without a red line.
const FILTERED_TEST_NAMES: &[&str] = &[];

#[test]
fn each_test_file_is_declared_and_none_is_ignored() {
    let features = manifest_features();
    let mut problems = Vec::new();
    for root in test_target_roots() {
        check_folder(
            &root,
            &root.join(TARGET_ROOT_FILE),
            &features,
            &mut problems,
        );
        for source in rust_sources(&root) {
            problems.extend(inner_cfg_problems(&source, &features));
        }
    }
    for source in rust_sources(Path::new(FITNESS_ROOT)) {
        problems.extend(ignore_problems(&source));
    }
    assert!(
        problems.is_empty(),
        "each line below is a problem of the test tree:\n{}",
        problems.join("\n")
    );
}

#[test]
fn each_filtered_test_name_is_a_fitness_test_function() {
    let sources: Vec<String> = rust_sources(Path::new(FITNESS_ROOT))
        .iter()
        .map(|path| read(path))
        .collect();
    let missing: Vec<&str> = FILTERED_TEST_NAMES
        .iter()
        .copied()
        .filter(|name| !sources.iter().any(|source| defines_test(source, name)))
        .collect();
    assert!(
        missing.is_empty(),
        "a gate step selects fitness tests by these names, but no test function in {FITNESS_ROOT} \
         has them, so the step runs zero tests and passes: {missing:?}. Rename the filter of the \
         gate step and this list together"
    );
}

/// proptest reads saved cases only from the file whose path mirrors the path of its test file below
/// the target root. A file at any other path replays nothing, and its test still passes.
#[test]
fn each_proptest_regression_file_belongs_to_a_test_file() {
    let roots = test_target_roots();
    let regressions = Path::new(REGRESSIONS_FOLDER);
    let orphans: Vec<String> = files_below(regressions)
        .into_iter()
        .filter(|file| {
            let relative = file
                .strip_prefix(regressions)
                .expect("the walk stays below its folder");
            let test_file = relative.with_extension("rs");
            !roots.iter().any(|root| root.join(&test_file).is_file())
        })
        .map(|file| file.display().to_string())
        .collect();
    assert!(
        orphans.is_empty(),
        "these regression files mirror no test file below a test target root, so their saved cases \
         replay never: {orphans:?}. Move each one to the path of its test file, or delete it"
    );
}

struct Declaration {
    name: String,
    line: usize,
    cfg: Option<String>,
}

/// Each folder below `tests/` that holds a `main.rs` is the root of one test target.
fn test_target_roots() -> Vec<PathBuf> {
    entries(Path::new(TESTS_FOLDER))
        .into_iter()
        .filter(|path| path.join(TARGET_ROOT_FILE).is_file())
        .collect()
}

/// `#[path]` includes declare a module with no file beside it, so the check runs one way: each file
/// and each folder of test code is declared, never each declaration has a file.
fn check_folder(
    folder: &Path,
    declaring_file: &Path,
    features: &[String],
    problems: &mut Vec<String>,
) {
    let declarations = declarations(declaring_file);
    for declaration in &declarations {
        if let Some(cfg) = &declaration.cfg
            && !is_enabled_by_a_gate_step(cfg, features)
        {
            problems.push(format!(
                "{}:{}: `mod {}` sits behind `#[cfg({cfg})]`, which no gate step enables, so it \
                 never runs. Gate it on a feature of Cargo.toml, or remove the cfg",
                declaring_file.display(),
                declaration.line,
                declaration.name
            ));
        }
    }
    for entry in entries(folder) {
        let Some(name) = module_name(&entry) else { continue };
        if entry == declaring_file {
            continue;
        }
        if !declarations
            .iter()
            .any(|declaration| declaration.name == name)
        {
            problems.push(format!(
                "{}: no `mod {name};` line in {} reaches it, so it compiles nowhere and never \
                 runs. Declare it there, or delete it",
                entry.display(),
                declaring_file.display()
            ));
            continue;
        }
        if entry.is_dir() {
            let nested = entry.join("mod.rs");
            let child_declaring =
                if nested.is_file() { nested } else { entry.with_extension("rs") };
            check_folder(&entry, &child_declaring, features, problems);
        }
    }
}

fn declarations(declaring_file: &Path) -> Vec<Declaration> {
    let source = read(declaring_file);
    let lines: Vec<&str> = source.lines().map(str::trim).collect();
    lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| {
            let name = declared_name(line)?;
            let cfg = attributes_above(&lines[..index])
                .find_map(|attribute| attribute.strip_prefix("#[cfg(")?.strip_suffix(")]"));
            Some(Declaration {
                name: name.to_owned(),
                line: index + 1,
                cfg: cfg.map(str::to_owned),
            })
        })
        .collect()
}

fn declared_name(line: &str) -> Option<&str> {
    let unscoped = line
        .strip_prefix("pub(crate) ")
        .or_else(|| line.strip_prefix("pub "))
        .unwrap_or(line);
    unscoped.strip_prefix("mod ")?.strip_suffix(';')
}

/// The attribute and doc lines directly above a line, nearest first.
fn attributes_above<'a>(lines: &'a [&'a str]) -> impl Iterator<Item = &'a str> {
    lines
        .iter()
        .rev()
        .copied()
        .take_while(|line| line.starts_with("#[") || line.starts_with("///"))
}

/// Step 4 of the gate runs the tests with no feature and with every feature, so only a cfg on a
/// feature of the manifest is on in one of them. Any other cfg, a target or a misspelt feature
/// among them, is on in no gate step.
fn is_enabled_by_a_gate_step(cfg: &str, features: &[String]) -> bool {
    cfg.strip_prefix("feature = \"")
        .and_then(|rest| rest.strip_suffix('"'))
        .is_some_and(|feature| features.iter().any(|declared| declared == feature))
}

fn inner_cfg_problems(source: &Path, features: &[String]) -> Vec<String> {
    read(source)
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let cfg = line.trim().strip_prefix("#![cfg(")?.strip_suffix(")]")?;
            (!is_enabled_by_a_gate_step(cfg, features)).then(|| {
                format!(
                    "{}:{}: `#![cfg({cfg})]` switches this module off in every gate step, so it \
                     never runs. Gate it on a feature of Cargo.toml, or remove the cfg",
                    source.display(),
                    index + 1
                )
            })
        })
        .collect()
}

fn ignore_problems(source: &Path) -> Vec<String> {
    read(source)
        .lines()
        .enumerate()
        .filter(|(_, line)| {
            let line = line.trim();
            line.starts_with("#[ignore")
                || (line.starts_with("#[cfg_attr(") && line.contains("ignore"))
        })
        .map(|(index, line)| {
            format!(
                "{}:{}: `{}` switches a fitness test off, but each fitness test runs in each gate \
                 step. Remove it, or move the test to target integration",
                source.display(),
                index + 1,
                line.trim()
            )
        })
        .collect()
}

fn defines_test(source: &str, name: &str) -> bool {
    let lines: Vec<&str> = source.lines().map(str::trim).collect();
    let signature = format!("fn {name}(");
    lines.iter().enumerate().any(|(index, line)| {
        line.starts_with(&signature)
            && attributes_above(&lines[..index]).any(|attribute| attribute == "#[test]")
    })
}

/// The manifest is read as text: a toml crate for one table would be a dependency for nothing.
fn manifest_features() -> Vec<String> {
    read(Path::new("Cargo.toml"))
        .lines()
        .skip_while(|line| line.trim() != "[features]")
        .skip(1)
        .take_while(|line| !line.trim_start().starts_with('['))
        .filter_map(|line| line.split_once('='))
        .map(|(name, _)| name.trim().to_owned())
        .collect()
}

fn module_name(entry: &Path) -> Option<String> {
    let file_name = entry.file_name()?.to_str()?;
    if entry.is_dir() {
        return (!rust_sources(entry).is_empty()).then(|| file_name.to_owned());
    }
    file_name.strip_suffix(".rs").map(str::to_owned)
}

fn entries(folder: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(folder)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", folder.display()))
        .map(|entry| entry.expect("a folder entry is readable").path())
        .collect();
    paths.sort();
    paths
}

fn files_below(folder: &Path) -> Vec<PathBuf> {
    if !folder.is_dir() {
        return Vec::new();
    }
    entries(folder)
        .into_iter()
        .flat_map(|path| if path.is_dir() { files_below(&path) } else { vec![path] })
        .collect()
}

fn read(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}
