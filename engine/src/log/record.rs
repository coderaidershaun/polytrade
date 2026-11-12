//! Wire record: fixed-size Copy built on stack, copied into ring. Truncating formatter (no alloc).

use core::fmt;

use crate::labelled_enum::labelled_enum;
use crate::time::{TsUs, boot_stamp_us};

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

/// Bytes of message text one record carries. Longer messages truncate at a char boundary.
pub(crate) const MSG_CAPACITY: usize = 192;

/// Fixed-size Copy POD. Producer formats into msg; drain reads. Only bytes cross ring. msg beyond msg_len = 0 always.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogRecord {
    pub(super) ts_us: TsUs,
    pub(super) level: Level,
    pub(super) module: &'static str,
    pub(super) file: &'static str,
    pub(super) line: u32,
    pub(super) msg: [u8; MSG_CAPACITY],
    pub(super) msg_len: u16,
}

impl LogRecord {
    pub(super) fn new(
        level: Level,
        module: &'static str,
        file: &'static str,
        line: u32,
        args: fmt::Arguments<'_>,
    ) -> Self {
        Self::at(boot_stamp_us(), level, module, file, line, args)
    }

    /// Stamps event_ts (not wall clock), no backtrace. Strategy log = pure fn (replay-diffable).
    pub(crate) fn strategy_at(
        event_ts: TsUs,
        level: Level,
        module: &'static str,
        file: &'static str,
        line: u32,
        args: fmt::Arguments<'_>,
    ) -> Self {
        Self::at(event_ts, level, module, file, line, args)
    }

    fn at(
        ts_us: TsUs,
        level: Level,
        module: &'static str,
        file: &'static str,
        line: u32,
        args: fmt::Arguments<'_>,
    ) -> Self {
        let mut writer = MsgWriter {
            buf: [0; MSG_CAPACITY],
            len: 0,
            is_full: false,
        };
        writer.render(args);
        Self {
            ts_us,
            level,
            module,
            file,
            line,
            msg: writer.buf,
            msg_len: writer.len as u16,
        }
    }

    pub(super) fn message(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.msg[..self.msg_len as usize])
    }
}

struct MsgWriter {
    buf: [u8; MSG_CAPACITY],
    len: usize,
    is_full: bool,
}

impl MsgWriter {
    fn render(&mut self, args: fmt::Arguments<'_>) {
        // Only fails on erroring Display; keep partial buffer.
        fmt::Write::write_fmt(self, args).ok();
    }
}

impl fmt::Write for MsgWriter {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        // Once full, stay full (truncation = clean prefix).
        if self.is_full {
            return Ok(());
        }
        let mut end = (MSG_CAPACITY - self.len).min(s.len());
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
