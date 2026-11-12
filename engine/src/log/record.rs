//! The fixed-size records that cross a log lane. A hot record carries raw words that the drain turns
//! into text, so the hot thread never formats; a text record carries text its own thread made.

use core::cell::Cell;
use core::fmt;
use core::mem::{align_of, offset_of, size_of};

use super::render::{self, LogArgs, Renderer};
use crate::msg::labelled_enum::labelled_enum;
use crate::msg::time::TsUs;

labelled_enum! {
    /// Three levels only (no debug/trace).
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Level {
        Info = "INFO",
        Warn = "WARN",
        Error = "ERROR",
    }
    pub(super) fn as_str;
}

impl Level {
    pub(super) fn should_flush_immediately(self) -> bool {
        matches!(self, Level::Warn | Level::Error)
    }
}

/// The argument words one hot record carries.
pub const WORDS_PER_RECORD: usize = 6;

/// Bytes of text one text record carries. Longer text truncates at a char boundary.
pub(crate) const TEXT_CAPACITY: usize = 238;

/// One log call in the source, made once at compile time by the macro at that call.
#[derive(Debug)]
pub struct Callsite {
    level: Level,
    module: &'static str,
    file: &'static str,
    line: u32,
    template: &'static str,
    renderers: &'static [Renderer],
}

impl Callsite {
    /// # Panics
    /// At compile time, when the macro evaluates it: if the template's placeholders do not match
    /// the arguments one for one, or the arguments need more than [`WORDS_PER_RECORD`] words.
    #[doc(hidden)]
    pub const fn new(
        level: Level,
        module: &'static str,
        file: &'static str,
        line: u32,
        template: &'static str,
        renderers: &'static [Renderer],
    ) -> Self {
        assert!(
            render::placeholder_count(template) == renderers.len(),
            "a hot log template needs exactly one placeholder for each argument"
        );
        let mut words = 0;
        let mut index = 0;
        while index < renderers.len() {
            words += renderers[index].words();
            index += 1;
        }
        assert!(
            words <= WORDS_PER_RECORD,
            "the arguments of a hot log call need more than the 6 words a record carries"
        );
        Self {
            level,
            module,
            file,
            line,
            template,
            renderers,
        }
    }

    pub fn level(&self) -> Level {
        self.level
    }

    pub fn module(&self) -> &'static str {
        self.module
    }

    pub fn file(&self) -> &'static str {
        self.file
    }

    pub fn line(&self) -> u32 {
        self.line
    }

    pub fn template(&self) -> &'static str {
        self.template
    }

    pub(super) fn renderers(&self) -> &'static [Renderer] {
        self.renderers
    }
}

/// Two call sites are the same when they name the same source line. The renderers are left out:
/// one call site has one argument list, and comparing function addresses is not reliable.
impl PartialEq for Callsite {
    fn eq(&self, other: &Self) -> bool {
        self.level == other.level
            && self.module == other.module
            && self.file == other.file
            && self.line == other.line
            && self.template == other.template
    }
}

impl Eq for Callsite {}

/// One hot log event: the call site, the time of the message under fold, and the raw argument
/// words. It holds no text, so building one costs a copy of one cache line.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HotRecord {
    callsite: &'static Callsite,
    at: TsUs,
    words: [u64; WORDS_PER_RECORD],
}

const _: () = {
    assert!(size_of::<HotRecord>() == 64);
    assert!(align_of::<HotRecord>() == 64);
    assert!(offset_of!(HotRecord, callsite) == 0);
    assert!(offset_of!(HotRecord, at) == 8);
    assert!(offset_of!(HotRecord, words) == 16);
};

impl HotRecord {
    #[inline]
    pub(crate) fn new<A: LogArgs>(callsite: &'static Callsite, at: TsUs, args: A) -> Self {
        let mut words = [0; WORDS_PER_RECORD];
        args.write_words(&mut words);
        Self::from_words(callsite, at, words)
    }

    #[inline]
    pub(super) fn from_words(
        callsite: &'static Callsite,
        at: TsUs,
        words: [u64; WORDS_PER_RECORD],
    ) -> Self {
        Self {
            callsite,
            at,
            words,
        }
    }

    pub fn callsite(&self) -> &'static Callsite {
        self.callsite
    }

    /// The `received_ts_us` of the message that was under fold when the call ran.
    pub fn at(&self) -> TsUs {
        self.at
    }

    pub fn words(&self) -> &[u64; WORDS_PER_RECORD] {
        &self.words
    }

    /// Writes the text of the line: the template with each placeholder filled from the words.
    ///
    /// # Errors
    /// Only when `out` refuses a write.
    pub fn render(&self, out: &mut dyn fmt::Write) -> fmt::Result {
        render::render(self.callsite, &self.words, out)
    }
}

/// One log line that its own thread already turned into text: the line of a thread that is not
/// the hot thread, or a hot call that ran where no hot lane exists.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextRecord {
    callsite: &'static Callsite,
    at: TsUs,
    len: u16,
    text: [u8; TEXT_CAPACITY],
}

const _: () = {
    assert!(size_of::<TextRecord>() == 256);
    assert!(align_of::<TextRecord>() == 64);
    assert!(offset_of!(TextRecord, callsite) == 0);
    assert!(offset_of!(TextRecord, at) == 8);
    assert!(offset_of!(TextRecord, len) == 16);
    assert!(offset_of!(TextRecord, text) == 18);
};

impl TextRecord {
    pub(super) fn formatted(
        callsite: &'static Callsite,
        at: TsUs,
        args: fmt::Arguments<'_>,
    ) -> Self {
        Self::written(callsite, at, |writer| fmt::Write::write_fmt(writer, args))
    }

    pub(super) fn rendered(
        callsite: &'static Callsite,
        at: TsUs,
        words: &[u64; WORDS_PER_RECORD],
    ) -> Self {
        Self::written(callsite, at, |writer| {
            render::render(callsite, words, writer)
        })
    }

    fn written(
        callsite: &'static Callsite,
        at: TsUs,
        write: impl FnOnce(&mut TextWriter) -> fmt::Result,
    ) -> Self {
        let mut writer = TextWriter {
            buf: [0; TEXT_CAPACITY],
            len: 0,
            is_full: false,
        };
        // A failed write keeps the text written so far, which is the best line left to show.
        write(&mut writer).ok();
        Self {
            callsite,
            at,
            len: writer.len as u16,
            text: writer.buf,
        }
    }

    pub fn callsite(&self) -> &'static Callsite {
        self.callsite
    }

    pub fn at(&self) -> TsUs {
        self.at
    }

    pub fn text(&self) -> &str {
        let bytes = &self.text[..usize::from(self.len)];
        core::str::from_utf8(bytes).unwrap_or_default()
    }
}

struct TextWriter {
    buf: [u8; TEXT_CAPACITY],
    len: usize,
    is_full: bool,
}

impl fmt::Write for TextWriter {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        // Once full, stay full, so a truncated line is a clean prefix.
        if self.is_full {
            return Ok(());
        }
        let mut end = (TEXT_CAPACITY - self.len).min(s.len());
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        if end < s.len() {
            self.is_full = true;
        }
        self.buf[self.len..self.len + end].copy_from_slice(&s.as_bytes()[..end]);
        self.len += end;
        Ok(())
    }
}

thread_local! {
    static MESSAGE_TIME: Cell<Option<TsUs>> = const { Cell::new(None) };
}

/// Called by the fold before each message, so each hot record on this thread carries the time of
/// the message it reacted to, never a clock read.
#[inline]
pub(crate) fn set_message_time(at: TsUs) {
    MESSAGE_TIME.with(|cell| cell.set(Some(at)));
}

#[inline]
pub(super) fn message_time() -> Option<TsUs> {
    MESSAGE_TIME.with(Cell::get)
}
