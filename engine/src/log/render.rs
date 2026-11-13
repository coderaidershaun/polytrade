//! How a hot log value becomes words on the hot thread and text on the drain. One template parser
//! serves the compile-time check of each call site and the drain, so the two cannot disagree.

use core::fmt;

use super::record::{Callsite, WORDS_PER_RECORD};
use crate::msg::ids::InstrumentId;

/// The placeholder forms a hot template takes: `{}`, `{:?}`, `{:x}` and `{:.N}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Spec {
    Display,
    Debug,
    LowerHex,
    Precision(usize),
}

/// Writes the text of one argument from its words, under the placeholder that the template gives.
pub type RenderFn = fn(&[u64], Spec, &mut dyn fmt::Write) -> fmt::Result;

/// How the drain reads one argument back: how many words it took, and how to write them as text.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Renderer {
    words: usize,
    render: RenderFn,
}

impl Renderer {
    pub const fn of<T: LogValue>() -> Self {
        Self {
            words: T::WORDS,
            render: T::render,
        }
    }

    pub(super) const fn words(&self) -> usize {
        self.words
    }

    pub(super) fn render(
        &self,
        words: &[u64],
        spec: Spec,
        out: &mut dyn fmt::Write,
    ) -> fmt::Result {
        (self.render)(words, spec, out)
    }
}

/// A value that a hot log call can carry: it fits in whole words, and it writes its own text on the
/// drain thread, which is where all hot log text is made.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a `LogValue`, so a hot log call cannot carry it",
    label = "not a `LogValue`",
    note = "a hot log call takes numbers, `bool`, ids, fieldless enums and `Option`s of them: \
            the text is made on the drain thread, never on the hot thread"
)]
pub trait LogValue: Copy {
    const WORDS: usize;

    /// `words` holds exactly [`Self::WORDS`] words.
    fn to_words(self, words: &mut [u64]);

    /// `words` holds exactly [`Self::WORDS`] words, as `to_words` wrote them.
    fn render(words: &[u64], spec: Spec, out: &mut dyn fmt::Write) -> fmt::Result;
}

/// The argument list of one hot log call: a tuple of up to six `LogValue`s.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "each argument of a hot log call must be a `LogValue`",
    label = "an argument here is not a `LogValue`"
)]
pub trait LogArgs: Copy {
    const RENDERERS: &'static [Renderer];

    fn write_words(self, words: &mut [u64; WORDS_PER_RECORD]);
}

impl LogArgs for () {
    const RENDERERS: &'static [Renderer] = &[];

    #[inline]
    fn write_words(self, _words: &mut [u64; WORDS_PER_RECORD]) {}
}

macro_rules! log_args_for_tuple {
    ($($ty:ident $value:ident),+) => {
        impl<$($ty: LogValue),+> LogArgs for ($($ty,)+) {
            const RENDERERS: &'static [Renderer] = &[$(Renderer::of::<$ty>()),+];

            #[inline]
            #[allow(unused_assignments, reason = "the offset after the last argument is not read")]
            fn write_words(self, words: &mut [u64; WORDS_PER_RECORD]) {
                let ($($value,)+) = self;
                let mut offset = 0;
                $(
                    $value.to_words(&mut words[offset..offset + $ty::WORDS]);
                    offset += $ty::WORDS;
                )+
            }
        }
    };
}

log_args_for_tuple!(A a);
log_args_for_tuple!(A a, B b);
log_args_for_tuple!(A a, B b, C c);
log_args_for_tuple!(A a, B b, C c, D d);
log_args_for_tuple!(A a, B b, C c, D d, E e);
log_args_for_tuple!(A a, B b, C c, D d, E e, F f);

macro_rules! log_value_for_integer {
    ($($ty:ty),*) => {
        $(
            #[allow(clippy::unnecessary_cast, reason = "one body serves every width, `u64` included")]
            impl LogValue for $ty {
                const WORDS: usize = 1;

                #[inline]
                fn to_words(self, words: &mut [u64]) {
                    words[0] = self as u64;
                }

                fn render(words: &[u64], spec: Spec, out: &mut dyn fmt::Write) -> fmt::Result {
                    let value = words[0] as $ty;
                    match spec {
                        Spec::LowerHex => write!(out, "{value:x}"),
                        Spec::Display | Spec::Debug | Spec::Precision(_) => write!(out, "{value}"),
                    }
                }
            }
        )*
    };
}

log_value_for_integer!(u8, u16, u32, u64, usize, i32, i64);

impl LogValue for bool {
    const WORDS: usize = 1;

    #[inline]
    fn to_words(self, words: &mut [u64]) {
        words[0] = u64::from(self);
    }

    fn render(words: &[u64], _spec: Spec, out: &mut dyn fmt::Write) -> fmt::Result {
        write!(out, "{}", words[0] != 0)
    }
}

impl LogValue for f64 {
    const WORDS: usize = 1;

    #[inline]
    fn to_words(self, words: &mut [u64]) {
        words[0] = self.to_bits();
    }

    fn render(words: &[u64], spec: Spec, out: &mut dyn fmt::Write) -> fmt::Result {
        let value = f64::from_bits(words[0]);
        match spec {
            Spec::Debug => write!(out, "{value:?}"),
            Spec::Precision(digits) => write!(out, "{value:.digits$}"),
            Spec::Display | Spec::LowerHex => write!(out, "{value}"),
        }
    }
}

/// Renders as the bare number that its `.0` prints, because that is what a log line shows.
impl LogValue for InstrumentId {
    const WORDS: usize = 1;

    #[inline]
    fn to_words(self, words: &mut [u64]) {
        self.0.to_words(words);
    }

    fn render(words: &[u64], spec: Spec, out: &mut dyn fmt::Write) -> fmt::Result {
        u16::render(words, spec, out)
    }
}

/// Renders as `{:?}` prints an `Option`, whatever the placeholder: `Option` has no plain form.
impl<T: LogValue> LogValue for Option<T> {
    const WORDS: usize = 1 + T::WORDS;

    #[inline]
    fn to_words(self, words: &mut [u64]) {
        match self {
            Some(value) => {
                words[0] = 1;
                value.to_words(&mut words[1..]);
            }
            None => words.fill(0),
        }
    }

    fn render(words: &[u64], _spec: Spec, out: &mut dyn fmt::Write) -> fmt::Result {
        if words[0] == 0 {
            return out.write_str("None");
        }
        out.write_str("Some(")?;
        T::render(&words[1..], Spec::Debug, out)?;
        out.write_str(")")
    }
}

/// `LogValue` for an enum with no fields: the word is the variant's place in the declaration. With
/// `label = accessor`, a plain placeholder prints that label; `{:?}` always prints the variant name.
macro_rules! fieldless_log_value {
    ($ty:ident { $($variant:ident),+ $(,)? }, label = $label:ident) => {
        $crate::log::fieldless_log_value!(@impl $ty { $($variant),+ }, |variant, spec| match spec {
            $crate::log::Spec::Debug => match variant {
                $($ty::$variant => ::core::stringify!($variant)),+
            },
            _ => variant.$label(),
        });
    };
    ($ty:ident { $($variant:ident),+ $(,)? }) => {
        $crate::log::fieldless_log_value!(@impl $ty { $($variant),+ }, |variant, _spec| match variant {
            $($ty::$variant => ::core::stringify!($variant)),+
        });
    };
    (@impl $ty:ident { $($variant:ident),+ }, |$value:ident, $spec:ident| $text:expr) => {
        impl $crate::log::LogValue for $ty {
            const WORDS: usize = 1;

            #[inline]
            fn to_words(self, words: &mut [u64]) {
                words[0] = self as u64;
            }

            fn render(
                words: &[u64],
                $spec: $crate::log::Spec,
                out: &mut dyn ::core::fmt::Write,
            ) -> ::core::fmt::Result {
                const VARIANTS: &[$ty] = &[$($ty::$variant),+];
                const _: () = {
                    let mut index = 0;
                    while index < VARIANTS.len() {
                        assert!(
                            VARIANTS[index] as usize == index,
                            "the variants are listed out of their declaration order"
                        );
                        index += 1;
                    }
                };
                let found = usize::try_from(words[0])
                    .ok()
                    .and_then(|index| VARIANTS.get(index));
                let Some(&$value) = found else {
                    return out.write_str("?");
                };
                out.write_str($text)
            }
        }
    };
}

pub(crate) use fieldless_log_value;

/// One step through a template: literal text (a byte range), or one placeholder.
#[derive(Clone, Copy)]
enum Piece {
    Text { start: usize, end: usize },
    Placeholder(Spec),
}

/// # Panics
/// At compile time, when a call site's constant is evaluated: on a brace that does not pair up, or
/// a placeholder form that [`Spec`] does not have.
pub(super) const fn placeholder_count(template: &str) -> usize {
    let bytes = template.as_bytes();
    let mut count = 0;
    let mut at = 0;
    while at < bytes.len() {
        let (piece, next) = next_piece(bytes, at);
        if let Piece::Placeholder(_) = piece {
            count += 1;
        }
        at = next;
    }
    count
}

pub(super) fn render(
    callsite: &Callsite,
    words: &[u64; WORDS_PER_RECORD],
    out: &mut dyn fmt::Write,
) -> fmt::Result {
    let template = callsite.template();
    let bytes = template.as_bytes();
    let mut renderers = callsite.renderers().iter();
    let mut offset = 0;
    let mut at = 0;
    while at < bytes.len() {
        let (piece, next) = next_piece(bytes, at);
        at = next;
        match piece {
            Piece::Text { start, end } => out.write_str(template.get(start..end).unwrap_or(""))?,
            Piece::Placeholder(spec) => {
                let Some(renderer) = renderers.next() else {
                    return out.write_str("{?}");
                };
                let end = offset + renderer.words();
                let Some(argument) = words.get(offset..end) else {
                    return out.write_str("{?}");
                };
                renderer.render(argument, spec, out)?;
                offset = end;
            }
        }
    }
    Ok(())
}

const fn next_piece(bytes: &[u8], at: usize) -> (Piece, usize) {
    match bytes[at] {
        b'{' if at + 1 < bytes.len() && bytes[at + 1] == b'{' => (
            Piece::Text {
                start: at,
                end: at + 1,
            },
            at + 2,
        ),
        b'}' if at + 1 < bytes.len() && bytes[at + 1] == b'}' => (
            Piece::Text {
                start: at,
                end: at + 1,
            },
            at + 2,
        ),
        b'{' => {
            let mut close = at + 1;
            while close < bytes.len() && bytes[close] != b'}' {
                close += 1;
            }
            assert!(
                close < bytes.len(),
                "a hot log template opens a placeholder and never closes it"
            );
            (
                Piece::Placeholder(parse_spec(bytes, at + 1, close)),
                close + 1,
            )
        }
        b'}' => panic!("a hot log template closes a placeholder it never opened"),
        _ => {
            let mut end = at + 1;
            while end < bytes.len() && bytes[end] != b'{' && bytes[end] != b'}' {
                end += 1;
            }
            (Piece::Text { start: at, end }, end)
        }
    }
}

const fn parse_spec(bytes: &[u8], start: usize, end: usize) -> Spec {
    let len = end - start;
    if len == 0 {
        return Spec::Display;
    }
    assert!(
        bytes[start] == b':' && len >= 2,
        "a hot log placeholder takes no name: it is plain, `:?`, `:x` or `:.N`"
    );
    if len == 2 && bytes[start + 1] == b'?' {
        return Spec::Debug;
    }
    if len == 2 && bytes[start + 1] == b'x' {
        return Spec::LowerHex;
    }
    assert!(
        bytes[start + 1] == b'.' && len >= 3,
        "a hot log placeholder is plain, `:?`, `:x` or `:.N`"
    );
    let mut digits = 0;
    let mut index = start + 2;
    while index < end {
        let digit = bytes[index];
        assert!(
            digit.is_ascii_digit(),
            "a hot log precision is a number, as in `:.0`"
        );
        digits = digits * 10 + (digit - b'0') as usize;
        index += 1;
    }
    Spec::Precision(digits)
}
