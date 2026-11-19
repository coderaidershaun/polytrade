//! Checks the whole tape before a re-fold folds its first row: the layout of each kind in each
//! footer, the boot row first, no gap and no hole in the sequence. A refusal leaves no replay run
//! on disk to be judged.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use arrow_array::cast::AsArray;
use arrow_array::types::{UInt16Type, UInt64Type};
use engine::runtime::manifest::{CONTRACT_VERSION, RunManifest};
use engine::tape::{TAPE_KINDS, TapePayload, decode_payload};
use parquet::arrow::ProjectionMask;
use serde_json::Value;

use super::error::ReplayError;
use super::reader::open_file;

const BOOT_KIND: u16 = 256;
const GAP_KIND: u16 = 258;

/// The columns that the check reads: `seq`, `kind`, and `payload` for the span of a gap row.
const CHECKED_COLUMNS: [usize; 3] = [0, 1, 5];

/// # Errors
/// The [`ReplayError`] of the first fault in tape order.
pub(super) fn check_tape(files: &[PathBuf], manifest: &RunManifest) -> Result<(), ReplayError> {
    if manifest.stopped_tables.iter().any(|table| table == "tape") {
        return Err(ReplayError::TapeStopped);
    }
    let mut last_seq: Option<u64> = None;
    for file in files {
        let builder = open_file(file)?;
        check_footer(file, footer(&builder))?;
        let mask = ProjectionMask::leaves(builder.parquet_schema(), CHECKED_COLUMNS);
        let batches =
            builder
                .with_projection(mask)
                .build()
                .map_err(|source| ReplayError::Read {
                    file: file.clone(),
                    source,
                })?;
        for batch in batches {
            let batch = batch.map_err(|source| ReplayError::Read {
                file: file.clone(),
                source: source.into(),
            })?;
            let column_error = |detail: &str| ReplayError::Column {
                file: file.clone(),
                detail: detail.into(),
            };
            let seqs = batch
                .column(0)
                .as_primitive_opt::<UInt64Type>()
                .ok_or_else(|| column_error("column `seq` is not uint64"))?;
            let kinds = batch
                .column(1)
                .as_primitive_opt::<UInt16Type>()
                .ok_or_else(|| column_error("column `kind` is not uint16"))?;
            let payloads = batch
                .column(2)
                .as_binary_opt::<i32>()
                .ok_or_else(|| column_error("column `payload` is not binary"))?;
            for row in 0..batch.num_rows() {
                let (seq, kind) = (seqs.value(row), kinds.value(row));
                match last_seq {
                    None if (seq, kind) != (0, BOOT_KIND) => {
                        return Err(ReplayError::BootMissing { file: file.clone() });
                    }
                    Some(after_seq) if seq != after_seq + 1 => {
                        return Err(ReplayError::Hole {
                            after_seq,
                            next_seq: seq,
                        });
                    }
                    _ => {}
                }
                if kind == GAP_KIND {
                    return Err(gap(seq, payloads.value(row)));
                }
                last_seq = Some(seq);
            }
        }
    }
    Ok(())
}

fn gap(seq: u64, payload: &[u8]) -> ReplayError {
    match decode_payload(GAP_KIND, payload) {
        Ok(TapePayload::Gap(span)) => ReplayError::Gap {
            first_lost_seq: span.first_lost_seq,
            last_lost_seq: span.last_lost_seq,
        },
        Ok(_) => unreachable!("a gap kind decodes to a gap"),
        Err(source) => ReplayError::Decode {
            seq,
            kind: GAP_KIND,
            source,
        },
    }
}

fn footer<T>(
    builder: &parquet::arrow::arrow_reader::ArrowReaderBuilder<T>,
) -> HashMap<String, String> {
    builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter_map(|pair| Some((pair.key.clone(), pair.value.clone()?)))
        .collect()
}

/// The contract version that the tape was written under, and the digest of each kind it holds.
fn check_footer(file: &Path, footer: HashMap<String, String>) -> Result<(), ReplayError> {
    let missing = |key: &str| ReplayError::Footer {
        file: file.to_path_buf(),
        detail: format!("the footer has no key `{key}`").into(),
    };
    let malformed = |key: &str, value: &str| ReplayError::Footer {
        file: file.to_path_buf(),
        detail: format!("the footer key `{key}` holds {value:?}").into(),
    };
    let version = footer
        .get("contract_version")
        .ok_or_else(|| missing("contract_version"))?;
    let version: u32 = version
        .parse()
        .map_err(|_| malformed("contract_version", version))?;
    if version > CONTRACT_VERSION {
        return Err(ReplayError::ContractNewer {
            found: version,
            supported: CONTRACT_VERSION,
        });
    }
    let kinds_text = footer
        .get("tape_kinds")
        .ok_or_else(|| missing("tape_kinds"))?;
    let kinds: Vec<Value> =
        serde_json::from_str(kinds_text).map_err(|_| malformed("tape_kinds", kinds_text))?;
    for entry in &kinds {
        let (Some(kind), Some(name), Some(digest)) = (
            entry["kind"]
                .as_u64()
                .and_then(|kind| u16::try_from(kind).ok()),
            entry["name"].as_str(),
            entry["digest"].as_str().and_then(parse_hex),
        ) else {
            return Err(malformed("tape_kinds", &entry.to_string()));
        };
        let Some(built) = TAPE_KINDS.iter().find(|built| built.id == kind) else {
            return Err(ReplayError::UnknownKind {
                kind,
                file: file.to_path_buf(),
            });
        };
        if built.digest != digest {
            return Err(ReplayError::LayoutMismatch {
                kind,
                name: name.into(),
                found: digest,
                expected: built.digest,
                file: file.to_path_buf(),
            });
        }
    }
    Ok(())
}

/// `0x` and hex digits, as the footer and `run.json` write a digest.
pub(super) fn parse_hex(text: &str) -> Option<u64> {
    u64::from_str_radix(text.strip_prefix("0x")?, 16).ok()
}
