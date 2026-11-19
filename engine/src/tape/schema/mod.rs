//! Arrow schema, contract and footer of each table: one [`TableRow`] impl per row type; mantissas
//! as `i64`, labels as strings of a named set. Split by subject: `market` (what venue did), `exec`
//! (what engine did), `metrics` (how the engine ran).

mod column;
mod exec;
mod footer;
mod labels;
mod market;
mod metrics;
mod tape;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};

use crate::msg::labelled_enum::labelled_enum;
use crate::msg::time::LocalTs;

pub(super) use column::{Column, ColumnInfo};
pub(super) use footer::{FooterKey, footer_metadata};
pub(super) use labels::{LABEL_SETS, LabelSet};
pub(super) use tape::TapeRow;

use super::TapeTable;

pub(super) trait TableRow: Sized + 'static {
    const TABLE: TapeTable;
    const COLUMNS: &'static [Column<Self>];
    /// The column of [`TableRow::partition_ts`], which the contract names.
    const PARTITION_COLUMN: &'static str;

    /// When a file of the table closes besides the end of the run.
    const SEALS: Seals = Seals::BorderRowsHour;
    /// The rows of one row group, or `None` for the default of the Parquet writer.
    const ROW_GROUP_ROWS: Option<usize> = None;

    /// The stamp that picks the date folder and the hour of the file of a row.
    fn partition_ts(&self) -> LocalTs;

    fn schema() -> SchemaRef {
        Column::schema(Self::COLUMNS)
    }

    /// # Errors
    /// [`ArrowError`] if the built columns do not match `schema` — an internal invariant
    /// breach, since both are defined here.
    fn to_batch(rows: &[Self], schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
        Column::batch(Self::COLUMNS, rows, schema)
    }

    fn columns() -> Vec<ColumnInfo> {
        Self::COLUMNS.iter().map(Column::info).collect()
    }
}

labelled_enum! {
    /// When a file of a table closes, besides the end of the run.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub(in crate::tape) enum Seals {
        /// At each seal border of the fold, at the row cap, and at the cross of an hour.
        BorderRowsHour = "border_rows_hour",
        /// At each seal border of the fold only, so a file border is a point of the input.
        Border = "border",
    }
    pub(in crate::tape) fn as_str;
}
