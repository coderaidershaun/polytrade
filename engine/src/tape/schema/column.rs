//! One declaration for each Parquet column, from which the schema, the batch and the contract are
//! all built, so no file can carry a header that does not match its columns or its contract.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};

use crate::msg::time::{LocalTs, VenueTs};

use super::labels::LabelSet;

/// Column name, contract kind and accessor in one. `Ts`, `VenueTs` and `Mantissa` are each
/// `Int64` on disk and stay apart because a clock reading and a fixed-point mantissa are not the
/// same column to a reader.
pub(in crate::tape) enum Column<R> {
    Ts(&'static str, fn(&R) -> LocalTs),
    VenueTs(&'static str, fn(&R) -> VenueTs),
    Mantissa(&'static str, fn(&R) -> i64),
    /// Absent means no value, as a mark before the first book; a zero would read as a price.
    NullableMantissa(&'static str, fn(&R) -> Option<i64>),
    /// Nullable: absent means the venue never assigned one, and a zero would read as a real id.
    VenueId(&'static str, fn(&R) -> Option<i64>),
    QuoteLevel(&'static str, fn(&R) -> Option<u8>),
    NullableU64(&'static str, fn(&R) -> Option<u64>),
    NullableVenueTs(&'static str, fn(&R) -> Option<VenueTs>),
    I32(&'static str, fn(&R) -> i32),
    U8(&'static str, fn(&R) -> u8),
    /// Absent where the row has no such value, as the queue of a row that no queue gave.
    NullableU8(&'static str, fn(&R) -> Option<u8>),
    U16(&'static str, fn(&R) -> u16),
    U32(&'static str, fn(&R) -> u32),
    U64(&'static str, fn(&R) -> u64),
    F64(&'static str, fn(&R) -> f64),
    Bool(&'static str, fn(&R) -> bool),
    /// One value of a named set, so a reader can check each cell against the contract.
    Label(&'static str, fn(&R) -> &'static str, &'static LabelSet),
    /// Free text, which no set bounds.
    Text(&'static str, fn(&R) -> &str),
    /// Free text that the row holds as a value, such as an address, and the writer prints.
    PrintedText(&'static str, fn(&R) -> String),
    /// Bytes that the contract names a layout for elsewhere, as a tape payload.
    Bytes(&'static str, fn(&R) -> &[u8]),
}

/// What the contract says of one column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::tape) struct ColumnInfo {
    pub(in crate::tape) name: &'static str,
    pub(in crate::tape) kind: &'static str,
    pub(in crate::tape) arrow: &'static str,
    pub(in crate::tape) is_nullable: bool,
    pub(in crate::tape) labels: Option<&'static str>,
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

    pub(in crate::tape) fn info(&self) -> ColumnInfo {
        let (name, kind, labels) = match self {
            Column::Ts(name, _) => (*name, "ts_local", None),
            Column::VenueTs(name, _) | Column::NullableVenueTs(name, _) => {
                (*name, "ts_venue", None)
            }
            Column::Mantissa(name, _) | Column::NullableMantissa(name, _) => {
                (*name, "mantissa", None)
            }
            Column::VenueId(name, _) => (*name, "venue_id", None),
            Column::QuoteLevel(name, _) => (*name, "quote_level", None),
            Column::NullableU64(name, _) | Column::U64(name, _) => (*name, "u64", None),
            Column::I32(name, _) => (*name, "i32", None),
            Column::U8(name, _) | Column::NullableU8(name, _) => (*name, "u8", None),
            Column::U16(name, _) => (*name, "u16", None),
            Column::U32(name, _) => (*name, "u32", None),
            Column::F64(name, _) => (*name, "f64", None),
            Column::Bool(name, _) => (*name, "bool", None),
            Column::Label(name, _, set) => (*name, "label", Some(set.name)),
            Column::Text(name, _) | Column::PrintedText(name, _) => (*name, "text", None),
            Column::Bytes(name, _) => (*name, "bytes", None),
        };
        let field = self.field();
        ColumnInfo {
            name,
            kind,
            arrow: arrow_name(field.data_type()),
            is_nullable: field.is_nullable(),
            labels,
        }
    }

    fn field(&self) -> Field {
        match self {
            Column::Ts(name, _) | Column::VenueTs(name, _) | Column::Mantissa(name, _) => {
                Field::new(*name, DataType::Int64, false)
            }
            Column::NullableMantissa(name, _)
            | Column::VenueId(name, _)
            | Column::NullableVenueTs(name, _) => Field::new(*name, DataType::Int64, true),
            Column::QuoteLevel(name, _) => Field::new(*name, DataType::UInt8, true),
            Column::NullableU64(name, _) => Field::new(*name, DataType::UInt64, true),
            Column::I32(name, _) => Field::new(*name, DataType::Int32, false),
            Column::U8(name, _) => Field::new(*name, DataType::UInt8, false),
            Column::NullableU8(name, _) => Field::new(*name, DataType::UInt8, true),
            Column::U16(name, _) => Field::new(*name, DataType::UInt16, false),
            Column::U32(name, _) => Field::new(*name, DataType::UInt32, false),
            Column::U64(name, _) => Field::new(*name, DataType::UInt64, false),
            Column::F64(name, _) => Field::new(*name, DataType::Float64, false),
            Column::Bool(name, _) => Field::new(*name, DataType::Boolean, false),
            Column::Label(name, _, _) | Column::Text(name, _) | Column::PrintedText(name, _) => {
                Field::new(*name, DataType::Utf8, false)
            }
            Column::Bytes(name, _) => Field::new(*name, DataType::Binary, false),
        }
    }

    fn array(&self, rows: &[R]) -> ArrayRef {
        match self {
            Column::Ts(_, get) => Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| get(row).micros()),
            )),
            Column::VenueTs(_, get) => Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| get(row).micros()),
            )),
            Column::Mantissa(_, get) => {
                Arc::new(Int64Array::from_iter_values(rows.iter().map(get)))
            }
            Column::NullableMantissa(_, get) | Column::VenueId(_, get) => {
                Arc::new(Int64Array::from_iter(rows.iter().map(get)))
            }
            Column::QuoteLevel(_, get) => Arc::new(UInt8Array::from_iter(rows.iter().map(get))),
            Column::NullableU64(_, get) => Arc::new(UInt64Array::from_iter(rows.iter().map(get))),
            Column::NullableVenueTs(_, get) => Arc::new(Int64Array::from_iter(
                rows.iter().map(|row| get(row).map(VenueTs::micros)),
            )),
            Column::I32(_, get) => Arc::new(Int32Array::from_iter_values(rows.iter().map(get))),
            Column::U8(_, get) => Arc::new(UInt8Array::from_iter_values(rows.iter().map(get))),
            Column::NullableU8(_, get) => Arc::new(UInt8Array::from_iter(rows.iter().map(get))),
            Column::U16(_, get) => Arc::new(UInt16Array::from_iter_values(rows.iter().map(get))),
            Column::U32(_, get) => Arc::new(UInt32Array::from_iter_values(rows.iter().map(get))),
            Column::U64(_, get) => Arc::new(UInt64Array::from_iter_values(rows.iter().map(get))),
            Column::F64(_, get) => Arc::new(Float64Array::from_iter_values(rows.iter().map(get))),
            Column::Bool(_, get) => {
                Arc::new(BooleanArray::from(rows.iter().map(get).collect::<Vec<_>>()))
            }
            Column::Label(_, get, _) => {
                Arc::new(StringArray::from_iter_values(rows.iter().map(get)))
            }
            Column::Text(_, get) => Arc::new(StringArray::from_iter_values(rows.iter().map(get))),
            Column::PrintedText(_, get) => {
                Arc::new(StringArray::from_iter_values(rows.iter().map(get)))
            }
            Column::Bytes(_, get) => Arc::new(BinaryArray::from_iter_values(rows.iter().map(get))),
        }
    }
}

fn arrow_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Int64 => "int64",
        DataType::Int32 => "int32",
        DataType::UInt8 => "uint8",
        DataType::UInt16 => "uint16",
        DataType::UInt32 => "uint32",
        DataType::UInt64 => "uint64",
        DataType::Float64 => "float64",
        DataType::Boolean => "bool",
        DataType::Utf8 => "utf8",
        DataType::Binary => "binary",
        other => unreachable!("no column of the tape is of arrow type {other:?}"),
    }
}
