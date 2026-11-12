//! Reads the live run's `rotations` table back through the arrow reader — the venue-lineage
//! side-channel's proof it landed. Only the `instrument_id` column is decoded: the integration
//! assertion is per-leg row existence, not the full-cell roundtrip the fitness `parquet_readback`
//! already covers off recorded fixtures.

use std::path::{Path, PathBuf};

use arrow_array::{Array, UInt16Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// The instrument id of every persisted rotation row, across all files, in read order.
pub fn rotation_instruments(run_dir: &Path) -> Vec<u16> {
    let mut instruments = Vec::new();
    for path in parquet_files(&run_dir.join("rotations")) {
        read_instrument_column(&path, &mut instruments);
    }
    instruments
}

fn read_instrument_column(path: &Path, out: &mut Vec<u16>) {
    let file = std::fs::File::open(path).expect("open rotations parquet");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("read rotations footer");
    let column = builder
        .schema()
        .index_of("instrument_id")
        .expect("rotations table has an instrument_id column");
    for batch in builder.build().expect("build rotations reader") {
        let batch = batch.expect("read rotations batch");
        let ids = batch
            .column(column)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("instrument_id is a u16 column");
        out.extend((0..ids.len()).map(|row| ids.value(row)));
    }
}

fn parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect(dir, &mut files);
    files.sort();
    files
}

fn collect(dir: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            files.push(path);
        }
    }
}
