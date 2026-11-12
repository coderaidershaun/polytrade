//! Arrow schema + footer metadata: one [`TableRow`] impl per row type; mantissas as `i64`, enums as strings.
//! Split by subject: `market` (what venue did), `exec` (what engine did).

mod column;
mod exec;
mod market;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};

use crate::msg::time::TsUs;
use crate::runtime::config::ExecutionMode;

use self::column::Column;
use super::RunMeta;

pub(super) trait TableRow: Sized + 'static {
    const NAME: &'static str;
    const COLUMNS: &'static [Column<Self>];

    /// Record's local-receipt time (hour rotation + date/HH partitioning key).
    fn partition_ts(&self) -> TsUs;

    fn schema() -> SchemaRef {
        Column::schema(Self::COLUMNS)
    }

    /// # Errors
    /// [`ArrowError`] if the built columns do not match `schema` — an internal invariant
    /// breach, since both are defined here.
    fn to_batch(rows: &[Self], schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
        Column::batch(Self::COLUMNS, rows, schema)
    }
}

// SMELL: the footer names the features, instruments and assets, but not the link fields or the
// boot, so a reader cannot decode the link frames of a run or join a run that spans two boots. The
// reader also trusts only the newest footer it finds.
/// Footer metadata: fixed-point scale + id-to-name dicts for mantissa/index interpretation.
pub(super) fn footer_metadata(meta: &RunMeta) -> Vec<(String, String)> {
    vec![
        (
            "strategy_id".to_owned(),
            meta.strategy_id.as_str().to_owned(),
        ),
        ("te_id".to_owned(), meta.te_id.as_str().to_owned()),
        // `absent` covers missing configuration and legacy files.
        (
            "execution_mode".to_owned(),
            ExecutionMode::footer_value(meta.execution_mode).to_owned(),
        ),
        ("fixed_scale".to_owned(), meta.fixed_scale.to_string()),
        (
            "feature_dictionary".to_owned(),
            json_string_array(&meta.feature_names),
        ),
        (
            "instrument_dictionary".to_owned(),
            json_string_array(&meta.instrument_symbols),
        ),
        (
            "asset_dictionary".to_owned(),
            json_string_array(&meta.asset_symbols),
        ),
        ("engine_version".to_owned(), meta.engine_version.to_string()),
    ]
}

fn json_string_array(items: &[Box<str>]) -> String {
    serde_json::to_string(items).expect("a slice of strings always serialises to json")
}
