//! Writes `contract/tables.json` from the column lists, label sets, footer keys and manifest fields
//! of this build, so a reader outside the engine holds no list of its own. With `--check` it writes
//! nothing and fails when the file in Git differs from what this build would write.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail};

fn contract_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the engine package sits in the workspace folder")
        .join("contract")
        .join("tables.json")
}

/// The first line where the two texts part, counted from 1, with both lines.
fn first_difference(on_disk: &str, fresh: &str) -> Option<(usize, String, String)> {
    let mut disk_lines = on_disk.lines();
    let mut fresh_lines = fresh.lines();
    for number in 1.. {
        match (disk_lines.next(), fresh_lines.next()) {
            (None, None) => return None,
            (disk, built) if disk != built => {
                let shown = |line: Option<&str>| line.unwrap_or("<end of file>").to_owned();
                return Some((number, shown(disk), shown(built)));
            }
            _ => {}
        }
    }
    None
}

fn check(path: &Path, fresh: &str) -> Result<()> {
    let on_disk = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    if on_disk == fresh {
        println!("{} is current", path.display());
        return Ok(());
    }
    match first_difference(&on_disk, fresh) {
        Some((line, disk, built)) => bail!(
            "{} is stale at line {line}: the file holds `{disk}`, this build writes `{built}`; \
             run `cargo run -p engine --bin contract-export` and commit the file",
            path.display()
        ),
        None => bail!(
            "{} differs from this build only in its line endings; regenerate it",
            path.display()
        ),
    }
}

fn run() -> Result<()> {
    let path = contract_path();
    let fresh = engine::tape::contract_json();
    let is_check = std::env::args()
        .skip(1)
        .any(|argument| argument == "--check");
    if is_check {
        return check(&path, &fresh);
    }
    if let Some(folder) = path.parent() {
        std::fs::create_dir_all(folder)
            .with_context(|| format!("failed to create {}", folder.display()))?;
    }
    std::fs::write(&path, &fresh).with_context(|| format!("failed to write {}", path.display()))?;
    println!("wrote {}", path.display());
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("contract-export: {error:#}");
            ExitCode::FAILURE
        }
    }
}
