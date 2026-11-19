//! The bytes of each tape row: one field list for each payload type gives its writer, its reader,
//! its width and the digest of its layout, so writer and reader cannot learn two layouts and a
//! changed layout changes its digest. Rustfmt skips these macro bodies, so they are formatted by
//! hand.

mod fields;
mod kinds;

pub use kinds::{GapSpan, TAPE_KINDS, TapeKindInfo, TapePayload, decode_payload};
pub(super) use kinds::{encode_boot, encode_gap, encode_message, encode_snapshot};

use crate::msg::codec::LayoutDigest;

/// A payload that the tape cannot read back. Each names what it read, so a reader can say which row
/// of which file broke.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TapeDecodeError {
    #[error("payload ends {missing} bytes short of its layout")]
    Truncated { missing: usize },
    #[error("payload holds {left} bytes past the end of its layout")]
    TrailingBytes { left: usize },
    #[error("unknown {field} tag {tag}")]
    UnknownTag { field: &'static str, tag: u8 },
    #[error("{field} {value} is out of its range")]
    OutOfRange { field: &'static str, value: u64 },
    #[error("tape kind {kind} is not a kind this build reads")]
    UnknownKind { kind: u16 },
}

/// Reading is fallible for each type, so one declaration form serves plain fields and tagged ones.
/// `WIDTH` and `DIGEST` have no default: a type cannot reach the tape without stating its layout.
pub(super) trait TapeField: Sized {
    const WIDTH: usize;
    const DIGEST: u64;
    fn write(&self, out: &mut Vec<u8>);
    fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError>;
}

/// A cursor over the bytes of one payload. A short payload is an error, never a panic: the bytes
/// come from a file on disk.
pub(super) struct TapeReader<'a> {
    bytes: &'a [u8],
}

impl<'a> TapeReader<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub(super) fn take<const N: usize>(&mut self) -> Result<[u8; N], TapeDecodeError> {
        let Some((head, rest)) = self.bytes.split_first_chunk::<N>() else {
            return Err(TapeDecodeError::Truncated {
                missing: N - self.bytes.len(),
            });
        };
        self.bytes = rest;
        Ok(*head)
    }

    /// # Errors
    /// [`TapeDecodeError::TrailingBytes`] when bytes are left past the layout.
    pub(super) fn finish(self) -> Result<(), TapeDecodeError> {
        match self.bytes.len() {
            0 => Ok(()),
            left => Err(TapeDecodeError::TrailingBytes { left }),
        }
    }
}

/// The width of a field that a macro names without its type. The function is never called: the
/// compiler infers the field type from it.
pub(super) const fn width_of<S, T: TapeField>(_field: fn(&S) -> &T) -> usize {
    T::WIDTH
}

pub(super) const fn digest_of<S, T: TapeField>(_field: fn(&S) -> &T) -> u64 {
    T::DIGEST
}

macro_rules! tape_primitive {
    ($($ty:ty),+ $(,)?) => {
        $(impl TapeField for $ty {
            const WIDTH: usize = size_of::<$ty>();
            const DIGEST: u64 = LayoutDigest::START.text(stringify!($ty)).finish();

            fn write(&self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_le_bytes());
            }

            fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError> {
                Ok(Self::from_le_bytes(input.take()?))
            }
        })+
    };
}

/// Each value is parenthesised so the macro can splice the same tokens into a match arm and into
/// the expression that rebuilds it. The match keeps the compiler's exhaustiveness check: a variant
/// added upstream fails the build here rather than reaching the tape with no tag.
macro_rules! tape_enum {
    ($ty:ty, $label:literal; $( ($($value:tt)+) = $tag:literal ),+ $(,)?) => {
        impl TapeField for $ty {
            const WIDTH: usize = 1;
            const DIGEST: u64 = $crate::msg::codec::LayoutDigest::START
                .text("enum")
                .text($label)
                $( .text(stringify!($($value)+)).number($tag) )+
                .finish();

            fn write(&self, out: &mut Vec<u8>) {
                let tag: u8 = match *self {
                    $( $($value)+ => $tag, )+
                };
                out.push(tag);
            }

            fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError> {
                let [tag] = input.take::<1>()?;
                match tag {
                    $( $tag => Ok($($value)+), )+
                    _ => Err(TapeDecodeError::UnknownTag { field: $label, tag }),
                }
            }
        }
    };
}

/// Fields are written in declaration order.
macro_rules! tape_struct {
    ($ty:ident { $($field:ident),+ $(,)? }) => {
        impl TapeField for $ty {
            const WIDTH: usize = 0 $( + super::width_of(|record: &$ty| &record.$field) )+;
            const DIGEST: u64 = $crate::msg::codec::LayoutDigest::START
                .text("struct")
                .text(stringify!($ty))
                $(
                    .text(stringify!($field))
                    .part(
                        super::width_of(|record: &$ty| &record.$field),
                        super::digest_of(|record: &$ty| &record.$field),
                    )
                )+
                .finish();

            fn write(&self, out: &mut Vec<u8>) {
                $( self.$field.write(out); )+
            }

            fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError> {
                $( let $field = TapeField::read(input)?; )+
                Ok(Self { $($field),+ })
            }
        }
    };
}

pub(super) use {tape_enum, tape_struct};

tape_primitive!(u8, u16, u32, u64, i32, i64, f64);

impl TapeField for bool {
    const WIDTH: usize = 1;
    const DIGEST: u64 = LayoutDigest::START.text("bool").finish();

    fn write(&self, out: &mut Vec<u8>) {
        out.push(u8::from(*self));
    }

    fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError> {
        match input.take::<1>()? {
            [0] => Ok(false),
            [1] => Ok(true),
            [tag] => Err(TapeDecodeError::UnknownTag { field: "bool", tag }),
        }
    }
}

const ABSENT: u8 = 0;
const PRESENT: u8 = 1;

/// An absent value still takes the bytes of its value, all zero, so each kind keeps one width. The
/// reader takes the value before it looks at the presence byte.
impl<T: TapeField> TapeField for Option<T> {
    const WIDTH: usize = 1 + T::WIDTH;
    const DIGEST: u64 = LayoutDigest::START
        .text("optional")
        .number(ABSENT as u64)
        .number(PRESENT as u64)
        .part(T::WIDTH, T::DIGEST)
        .finish();

    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Some(value) => {
                out.push(PRESENT);
                value.write(out);
            }
            None => {
                out.push(ABSENT);
                out.resize(out.len() + T::WIDTH, 0);
            }
        }
    }

    fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError> {
        let [tag] = input.take::<1>()?;
        match tag {
            ABSENT => {
                for _ in 0..T::WIDTH {
                    input.take::<1>()?;
                }
                Ok(None)
            }
            PRESENT => Ok(Some(T::read(input)?)),
            _ => Err(TapeDecodeError::UnknownTag {
                field: "optional",
                tag,
            }),
        }
    }
}

impl<T: TapeField + Copy, const N: usize> TapeField for [T; N] {
    const WIDTH: usize = T::WIDTH * N;
    const DIGEST: u64 = LayoutDigest::START
        .text("array")
        .number(N as u64)
        .part(T::WIDTH, T::DIGEST)
        .finish();

    fn write(&self, out: &mut Vec<u8>) {
        for item in self {
            item.write(out);
        }
    }

    fn read(input: &mut TapeReader<'_>) -> Result<Self, TapeDecodeError> {
        let first = T::read(input)?;
        let mut items = [first; N];
        for item in items.iter_mut().skip(1) {
            *item = T::read(input)?;
        }
        Ok(items)
    }
}
