//! The golden run is the one set of files whose every cell a person can check by hand, so each
//! reader outside the engine is pinned against the same numbers that the writer wrote.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

use crate::parquet_readback::{Cell, TempDir, parquet_files, read_parquet_file};

const FIRST_RUN: &str = "golden/te-golden/sim/1784000000000000";
const SECOND_RUN: &str = "golden/te-golden/sim/1784000600000000";
const ARROW_SCHEMA_KEY: &str = "ARROW:schema";

fn golden_run(out: &Path) -> String {
    let status = Command::new(env!("CARGO_BIN_EXE_golden-run"))
        .arg(out)
        .status()
        .expect("the golden-run tool starts");
    assert!(status.success(), "the golden-run tool exits 0: {status}");
    std::fs::read_to_string(out.join("expected.json")).expect("the tool writes expected.json")
}

/// The cell of a file as `expected.json` spells it: an integer as an integer, a float as the
/// shortest text that reads back to its bits, a label or text as a string, null for absent.
fn json_of(cell: &Cell) -> Value {
    match cell {
        Cell::I32(value) => Value::from(*value),
        Cell::I64(value) => Value::from(*value),
        Cell::U8(value) => Value::from(*value),
        Cell::U16(value) => Value::from(*value),
        Cell::U32(value) => Value::from(*value),
        Cell::U64(value) => Value::from(*value),
        Cell::F64Bits(bits) => Value::from(f64::from_bits(*bits)),
        Cell::Str(text) => Value::from(text.as_str()),
        Cell::Bool(value) => Value::from(*value),
        Cell::Bytes(bytes) => Value::from(bytes.clone()),
        Cell::Null => Value::Null,
    }
}

/// Each row of `table` in the run below `run_dir`, file by file in path order, and the footer of
/// each of those files.
fn table_of(run_dir: &Path, table: &str) -> (Vec<String>, Vec<Value>, Vec<Value>) {
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    let mut footers = Vec::new();
    for path in parquet_files(&run_dir.join(table)) {
        let file = read_parquet_file(&path);
        columns = file.field_names;
        rows.extend(
            file.rows
                .iter()
                .map(|row| Value::Array(row.iter().map(json_of).collect())),
        );
        // The arrow writer adds its own schema to each footer, which is no key of the contract.
        let footer: serde_json::Map<String, Value> = file
            .footer
            .into_iter()
            .filter(|(key, _)| key != ARROW_SCHEMA_KEY)
            .map(|(key, value)| (key, value.map_or(Value::Null, Value::from)))
            .collect();
        footers.push(Value::Object(footer));
    }
    (columns, rows, footers)
}

fn features_named(run_dir: &Path) -> Vec<String> {
    let mut names = Vec::new();
    for path in parquet_files(&run_dir.join("features")) {
        let file = read_parquet_file(&path);
        let dictionary: Vec<String> = file
            .footer
            .iter()
            .find_map(|(key, value)| (key == "feature_dictionary").then(|| value.clone()))
            .flatten()
            .map(|json| serde_json::from_str(&json).expect("a dictionary is a json list"))
            .expect("each file holds its own feature dictionary");
        for row in &file.rows {
            let Cell::U16(id) = row[2] else {
                panic!("feature_id is a u16, got {:?}", row[2]);
            };
            names.push(dictionary[usize::from(id)].clone());
        }
    }
    names
}

fn run_of<'a>(expected: &'a Value, run_dir: &str) -> &'a Value {
    expected["runs"]
        .as_array()
        .expect("expected.json lists its runs")
        .iter()
        .find(|run| run["run_dir"] == run_dir)
        .unwrap_or_else(|| panic!("expected.json holds the run {run_dir}"))
}

fn assert_run_decodes(out: &Path, expected: &Value, run_dir: &str) {
    let run = run_of(expected, run_dir);
    let dir: PathBuf = out.join(run_dir);
    let manifest: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.join("run.json")).expect("each run has its run.json"),
    )
    .expect("run.json is json");
    assert_eq!(manifest, run["manifest"], "{run_dir}: run.json");
    let tables = run["tables"]
        .as_object()
        .expect("expected.json holds the tables of each run");
    for (table, want) in tables {
        let (columns, rows, footers) = table_of(&dir, table);
        assert!(!rows.is_empty(), "{run_dir}: {table} holds rows");
        assert_eq!(
            Value::from(columns),
            want["columns"],
            "{run_dir}: the columns of {table}"
        );
        assert_eq!(
            Value::Array(rows),
            want["rows"],
            "{run_dir}: each cell of {table}"
        );
        for footer in footers {
            assert_eq!(footer, run["footer"], "{run_dir}: the footer of {table}");
        }
    }
}

/// CONTRACT: the golden run writes two boots through the real writer, and each file decodes by
/// its own footer: a features row of id 1 is `beta` in the first boot and `gamma` in the second.
/// Each cell of each table and each `run.json` equal `expected.json`, and a second run of the tool
/// gives the same bytes. A reader that took one footer for both boots would name the wrong feature.
#[test]
fn golden_run_of_two_boots_decodes() {
    let root = TempDir::new("golden-run");
    let out = root.path().join("golden-run");
    let first = golden_run(&out);
    let expected: Value = serde_json::from_str(&first).expect("expected.json is json");

    assert_eq!(
        features_named(&out.join(FIRST_RUN)),
        ["alpha", "beta"],
        "the first boot names feature 1 beta"
    );
    assert_eq!(
        features_named(&out.join(SECOND_RUN)),
        ["alpha", "gamma"],
        "the second boot names feature 1 gamma"
    );
    for run_dir in [FIRST_RUN, SECOND_RUN] {
        assert_run_decodes(&out, &expected, run_dir);
    }

    let second = golden_run(&out);
    assert!(
        first == second,
        "two runs of the tool give byte-equal expected.json"
    );
}
