//! One declaration per wire record: writer and reader are generated from the same field list, so no
//! edit can teach one half a layout the other never learned. Every number rides little-endian.
//! Rustfmt skips the bodies of brace-delimited item macros, so the macro bodies here are formatted by
//! hand.

use crate::ids::{AssetId, ClientOrderId, InstrumentId, Price, Qty};
use crate::msg::persist::FeatureId;
use crate::time::{DurationUs, TsUs};

use super::envelope::{ByteReader, ByteWriter, LinkDecodeError, LinkHash, TopicId};

/// Reading is fallible for every type so one declaration form serves plain fields and tagged ones
/// alike; the plain cases return `Ok` unconditionally.
pub(super) trait WireField: Sized {
    fn write(&self, writer: &mut ByteWriter<'_>);
    fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError>;
}

macro_rules! wire_primitive {
    ($($ty:ty),+ $(,)?) => {
        $(impl WireField for $ty {
            #[inline]
            fn write(&self, writer: &mut ByteWriter<'_>) {
                writer.write_bytes(&self.to_le_bytes());
            }

            #[inline]
            fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
                Ok(Self::from_le_bytes(reader.take()))
            }
        })+
    };
}

macro_rules! wire_micros {
    ($($ty:ident),+ $(,)?) => {
        $(impl WireField for $ty {
            #[inline]
            fn write(&self, writer: &mut ByteWriter<'_>) {
                self.micros().write(writer);
            }

            #[inline]
            fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
                Ok($ty::from_micros(i64::read(reader)?))
            }
        })+
    };
}

macro_rules! wire_newtype {
    ($($ty:ident => $inner:ty),+ $(,)?) => {
        $(impl WireField for $ty {
            #[inline]
            fn write(&self, writer: &mut ByteWriter<'_>) {
                self.0.write(writer);
            }

            #[inline]
            fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
                Ok($ty(<$inner as WireField>::read(reader)?))
            }
        })+
    };
}

/// An absent value still occupies its bytes, so the tail width never depends on the data. The reader
/// therefore takes the value before it looks at the presence byte.
macro_rules! wire_optional {
    ($($ty:ty, $label:literal);+ $(;)?) => {
        $(impl WireField for Option<$ty> {
            fn write(&self, writer: &mut ByteWriter<'_>) {
                let tag = if self.is_some() { PRESENT } else { ABSENT };
                tag.write(writer);
                self.unwrap_or_default().write(writer);
            }

            fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
                let tag = u8::read(reader)?;
                let value = <$ty as WireField>::read(reader)?;
                match tag {
                    ABSENT => Ok(None),
                    PRESENT => Ok(Some(value)),
                    _ => Err(LinkDecodeError::unknown($label, tag)),
                }
            }
        })+
    };
}

macro_rules! wire_optional_mantissa {
    ($($ty:ident),+ $(,)?) => {
        $(impl WireField for Option<$ty> {
            fn write(&self, writer: &mut ByteWriter<'_>) {
                self.map(|value| value.0).write(writer);
            }

            fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
                Ok(Option::<i64>::read(reader)?.map($ty))
            }
        })+
    };
}

/// Each value is parenthesised so the macro can splice the same tokens into a match arm and into the
/// expression that rebuilds it. Matching on the value keeps the compiler's exhaustiveness check: a
/// variant added upstream fails the build here rather than reaching the wire untagged.
macro_rules! wire_enum {
    ($ty:ty, $label:literal; $( ($($value:tt)+) = $tag:literal ),+ $(,)?) => {
        impl WireField for $ty {
            fn write(&self, writer: &mut ByteWriter<'_>) {
                let tag: u8 = match *self {
                    $( $($value)+ => $tag, )+
                };
                tag.write(writer);
            }

            fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
                let tag = u8::read(reader)?;
                match tag {
                    $( $tag => Ok($($value)+), )+
                    _ => Err(LinkDecodeError::unknown($label, tag)),
                }
            }
        }
    };
}

/// Fields ride the wire in declaration order. A field may carry a trailing `after <expr>` that runs
/// the instant it lands, which is how a length is refused before the values it sizes are trusted.
macro_rules! wire_struct {
    ($ty:ident { $($field:ident $(after $checked:expr)?),+ $(,)? }) => {
        impl WireField for $ty {
            fn write(&self, writer: &mut ByteWriter<'_>) {
                $( self.$field.write(writer); )+
            }

            fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
                $(
                    let $field = WireField::read(reader)?;
                    $( $checked; )?
                )+
                Ok(Self { $($field),+ })
            }
        }
    };
}

/// Fixed-width run of one kind. The seed is named rather than derived: a `Default` on a wire type
/// would be a public promise made for a private convenience.
macro_rules! wire_array {
    ($($ty:ty; $len:expr; $empty:expr),+ $(,)?) => {
        $(impl WireField for [$ty; $len] {
            fn write(&self, writer: &mut ByteWriter<'_>) {
                for item in self {
                    item.write(writer);
                }
            }

            fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
                let mut items = [$empty; $len];
                for item in &mut items {
                    *item = WireField::read(reader)?;
                }
                Ok(items)
            }
        })+
    };
}

pub(super) use {wire_array, wire_enum, wire_struct};

wire_primitive!(u8, u16, u32, i32, u64, i64, f64);

wire_micros!(TsUs, DurationUs);

wire_newtype! {
    TopicId => u16,
    InstrumentId => u16,
    AssetId => u16,
    FeatureId => u16,
    ClientOrderId => u64,
    Price => i64,
    Qty => i64,
    LinkHash => u64,
}

pub(super) const OPTIONAL_MANTISSA_LEN: usize = 1 + 8;
pub(super) const OPTIONAL_F64_LEN: usize = 1 + 8;
pub(super) const OPTIONAL_LEVEL_LEN: usize = OPTIONAL_MANTISSA_LEN + 8;

const ABSENT: u8 = 0;
const PRESENT: u8 = 1;

wire_optional! {
    i64, "optional mantissa";
    f64, "optional f64";
}

wire_optional_mantissa!(Price, Qty);

/// The quantity rides even when the price is absent, so every level is one width.
impl WireField for Option<(Price, Qty)> {
    fn write(&self, writer: &mut ByteWriter<'_>) {
        self.map(|(price, _)| price).write(writer);
        self.map_or(Qty(0), |(_, qty)| qty).write(writer);
    }

    fn read(reader: &mut ByteReader<'_>) -> Result<Self, LinkDecodeError> {
        let price = Option::<Price>::read(reader)?;
        let qty = Qty::read(reader)?;
        Ok(price.map(|price| (price, qty)))
    }
}
