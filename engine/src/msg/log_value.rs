//! How a value of a message becomes the words of a log record and, on the drain, its text. It sits
//! with the messages, so each message type carries its own log form without importing the log.

use core::fmt;

/// The placeholder forms a hot template takes: `{}`, `{:?}`, `{:x}` and `{:.N}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Spec {
    Display,
    Debug,
    LowerHex,
    Precision(usize),
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

/// `LogValue` for an enum with no fields: the word is the variant's place in the declaration. With
/// `label = accessor`, a plain placeholder prints that label; `{:?}` always prints the variant name.
macro_rules! fieldless_log_value {
    ($ty:ident { $($variant:ident),+ $(,)? }, label = $label:ident) => {
        $crate::msg::log_value::fieldless_log_value!(@impl $ty { $($variant),+ }, |variant, spec| match spec {
            $crate::msg::log_value::Spec::Debug => match variant {
                $($ty::$variant => ::core::stringify!($variant)),+
            },
            _ => variant.$label(),
        });
    };
    ($ty:ident { $($variant:ident),+ $(,)? }) => {
        $crate::msg::log_value::fieldless_log_value!(@impl $ty { $($variant),+ }, |variant, _spec| match variant {
            $($ty::$variant => ::core::stringify!($variant)),+
        });
    };
    (@impl $ty:ident { $($variant:ident),+ }, |$value:ident, $spec:ident| $text:expr) => {
        impl $crate::msg::log_value::LogValue for $ty {
            const WORDS: usize = 1;

            #[inline]
            fn to_words(self, words: &mut [u64]) {
                words[0] = self as u64;
            }

            fn render(
                words: &[u64],
                $spec: $crate::msg::log_value::Spec,
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
