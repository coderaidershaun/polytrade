//! One declaration per Parquet column: name, arrow type, and the value it pulls out of a row.
//! Schema and batch are built from the same list, so a column cannot appear in one and not the
//! other — the failure that writes a file whose header lies about its contents.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};

use crate::time::TsUs;

/// Column name, arrow type and accessor in one. `Ts` and `Mantissa` are both non-null `Int64` and
/// stay apart because a clock reading and a fixed-point mantissa are not the same column to a reader.
pub(in crate::persist) enum Column<R> {
    Ts(&'static str, fn(&R) -> TsUs),
    Mantissa(&'static str, fn(&R) -> i64),
    /// Nullable: absent means the venue never assigned one, and a zero would read as a real id.
    VenueId(&'static str, fn(&R) -> Option<i64>),
    QuoteLevel(&'static str, fn(&R) -> Option<u8>),
    I32(&'static str, fn(&R) -> i32),
    U16(&'static str, fn(&R) -> u16),
    U32(&'static str, fn(&R) -> u32),
    U64(&'static str, fn(&R) -> u64),
    F64(&'static str, fn(&R) -> f64),
    Bool(&'static str, fn(&R) -> bool),
    Text(&'static str, fn(&R) -> &str),
}

impl<R> Column<R> {
    pub(super) fn schema(columns: &[Self]) -> SchemaRef {
        Arc::new(Schema::new(
            columns.iter().map(Self::field).collect::<Vec<_>>(),
        ))
    }

    /// # Errors
    /// [`ArrowError`] if the built columns do not match `schema` — impossible while both come from
    /// `columns`, and an internal invariant breach if it ever happens.
    pub(super) fn batch(
        columns: &[Self],
        rows: &[R],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, ArrowError> {
        let arrays = columns.iter().map(|column| column.array(rows)).collect();
        RecordBatch::try_new(schema.clone(), arrays)
    }

    fn field(&self) -> Field {
        match self {
            Column::Ts(name, _) | Column::Mantissa(name, _) => {
                Field::new(*name, DataType::Int64, false)
            }
            Column::VenueId(name, _) => Field::new(*name, DataType::Int64, true),
            Column::QuoteLevel(name, _) => Field::new(*name, DataType::UInt8, true),
            Column::I32(name, _) => Field::new(*name, DataType::Int32, false),
            Column::U16(name, _) => Field::new(*name, DataType::UInt16, false),
            Column::U32(name, _) => Field::new(*name, DataType::UInt32, false),
            Column::U64(name, _) => Field::new(*name, DataType::UInt64, false),
            Column::F64(name, _) => Field::new(*name, DataType::Float64, false),
            Column::Bool(name, _) => Field::new(*name, DataType::Boolean, false),
            Column::Text(name, _) => Field::new(*name, DataType::Utf8, false),
        }
    }

    fn array(&self, rows: &[R]) -> ArrayRef {
        match self {
            Column::Ts(_, get) => Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| get(row).micros()),
            )),
            Column::Mantissa(_, get) => {
                Arc::new(Int64Array::from_iter_values(rows.iter().map(get)))
            }
            Column::VenueId(_, get) => Arc::new(Int64Array::from_iter(rows.iter().map(get))),
            Column::QuoteLevel(_, get) => Arc::new(UInt8Array::from_iter(rows.iter().map(get))),
            Column::I32(_, get) => Arc::new(Int32Array::from_iter_values(rows.iter().map(get))),
            Column::U16(_, get) => Arc::new(UInt16Array::from_iter_values(rows.iter().map(get))),
            Column::U32(_, get) => Arc::new(UInt32Array::from_iter_values(rows.iter().map(get))),
            Column::U64(_, get) => Arc::new(UInt64Array::from_iter_values(rows.iter().map(get))),
            Column::F64(_, get) => Arc::new(Float64Array::from_iter_values(rows.iter().map(get))),
            Column::Bool(_, get) => {
                Arc::new(BooleanArray::from(rows.iter().map(get).collect::<Vec<_>>()))
            }
            Column::Text(_, get) => Arc::new(StringArray::from_iter_values(rows.iter().map(get))),
        }
    }
}
