//! Where log lines go: one writer for each UTC day of record time, reopened when a write fails,
//! and given up for a fallback once reopening keeps failing. Engine and strategy lines share it;
//! the tag column tells them apart.

use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use super::LogConfig;
use super::lane::BacktraceMessage;
use super::record::Callsite;
use crate::msg::time::{DurationUs, LocalTs};

const FLUSH_INTERVAL: DurationUs = DurationUs::from_secs(1);
const MICROS_PER_DAY: i64 = 86_400_000_000;
/// Large enough that a busy second of lines costs one write call, not one per line.
const FILE_BUFFER_BYTES: usize = 1 << 20;

/// A writer the drain can push to disk at shutdown. A writer with no disk behind it has nothing to
/// do there.
pub trait LogWriter: Write + Send {
    /// # Errors
    /// The error of the underlying flush or sync.
    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl LogWriter for io::Stderr {}

/// Opens the writer for the UTC day of the line at the given time.
pub type OpenWriter = Box<dyn FnMut(LocalTs) -> io::Result<Box<dyn LogWriter>> + Send>;

/// Where the drain writes: what `open` gives for the day of each line, and the writer it falls back
/// to once `reopen_limit` reopens in a row have failed.
pub struct LogOutput {
    pub open: OpenWriter,
    pub fallback: Box<dyn LogWriter>,
    pub reopen_limit: u32,
}

impl LogOutput {
    /// A debug build logs to stderr. A release build logs to one file for each UTC day, below the
    /// configured folder, and falls back to stderr.
    pub(super) fn from_config(config: &LogConfig) -> Self {
        let open: OpenWriter = if cfg!(debug_assertions) {
            Box::new(|_at| Ok(Box::new(io::stderr()) as Box<dyn LogWriter>))
        } else {
            let dir = config.dir.clone();
            let stem = config.file_stem.clone();
            Box::new(move |at| open_log_file(&dir, &stem, at))
        };
        Self {
            open,
            fallback: Box::new(io::stderr()),
            reopen_limit: config.reopen_limit,
        }
    }
}

struct FileWriter(BufWriter<File>);

impl Write for FileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl LogWriter for FileWriter {
    fn sync(&mut self) -> io::Result<()> {
        self.0.flush()?;
        self.0.get_ref().sync_data()
    }
}

fn open_log_file(dir: &Path, stem: &str, at: LocalTs) -> io::Result<Box<dyn LogWriter>> {
    fs::create_dir_all(dir)?;
    let day = at.civil();
    let name = format!("{stem}-{:04}-{:02}-{:02}.log", day.year, day.month, day.day);
    let path: PathBuf = dir.join(name);
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let writer = BufWriter::with_capacity(FILE_BUFFER_BYTES, file);
    Ok(Box::new(FileWriter(writer)))
}

/// What the output did with the lines it was given.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct OutputCounts {
    pub(super) written: u64,
    pub(super) lost: u64,
    pub(super) io_errors: u64,
    pub(super) reopens: u64,
}

pub(super) struct DropNotice {
    pub(super) since_last: u64,
    pub(super) total: u64,
}

pub(super) struct Output {
    targets: LogOutput,
    writer: Option<Box<dyn LogWriter>>,
    /// The UTC day number of the writer that is open.
    day: Option<i64>,
    is_on_fallback: bool,
    /// Reopens since the last line that was written: each failed line costs one.
    reopens_in_a_row: u32,
    /// Reused for each line, so a line allocates only while the buffer still grows.
    line: String,
    last_flush: Option<LocalTs>,
    counts: OutputCounts,
}

impl Output {
    pub(super) fn new(targets: LogOutput) -> Self {
        Self {
            targets,
            writer: None,
            day: None,
            is_on_fallback: false,
            reopens_in_a_row: 0,
            line: String::new(),
            last_flush: None,
            counts: OutputCounts::default(),
        }
    }

    pub(super) fn counts(&self) -> OutputCounts {
        self.counts
    }

    /// One record's line: time, level, tag, module, file:line, text.
    pub(super) fn write_line(&mut self, tag: &str, callsite: &Callsite, at: LocalTs, text: &str) {
        self.line.clear();
        let level = callsite.level();
        push_timestamp(&mut self.line, at);
        // A String accepts every write, so these cannot fail.
        writeln!(
            self.line,
            " {:<5} [{tag}] {} ({}:{}) {text}",
            level.as_str(),
            callsite.module(),
            callsite.file(),
            callsite.line()
        )
        .ok();
        let is_written = self.emit(at, level.should_flush_immediately());
        if is_written {
            self.counts.written += 1;
        } else {
            self.counts.lost += 1;
        }
    }

    pub(super) fn write_backtrace(&mut self, message: &BacktraceMessage) {
        self.line.clear();
        push_timestamp(&mut self.line, message.ts_us);
        write!(
            self.line,
            " ERROR [{} {}:{}] error backtrace:\n{}\n",
            message.module, message.file, message.line, message.backtrace
        )
        .ok();
        self.emit(message.ts_us, true);
    }

    pub(super) fn write_drop_notice(&mut self, tag: &str, notice: DropNotice, now: LocalTs) {
        self.line.clear();
        push_timestamp(&mut self.line, now);
        writeln!(
            self.line,
            " WARN  [logging] lane {tag:?} dropped {} record(s) ({} total, ring full)",
            notice.since_last, notice.total
        )
        .ok();
        self.emit(now, true);
    }

    pub(super) fn flush_if_due(&mut self, now: LocalTs) {
        let is_due = self
            .last_flush
            .is_none_or(|last| now.diff(last) >= FLUSH_INTERVAL);
        if is_due {
            self.flush(now);
        }
    }

    pub(super) fn flush(&mut self, now: LocalTs) {
        self.last_flush = Some(now);
        let target = match self.writer.as_mut() {
            Some(writer) if !self.is_on_fallback => writer,
            _ => &mut self.targets.fallback,
        };
        if target.flush().is_err() {
            self.counts.io_errors += 1;
            if !self.is_on_fallback {
                self.writer = None;
            }
        }
    }

    /// Flushes and pushes the lines to disk, for the end of the run.
    pub(super) fn sync(&mut self, now: LocalTs) {
        self.flush(now);
        let target = match self.writer.as_mut() {
            Some(writer) if !self.is_on_fallback => writer,
            _ => &mut self.targets.fallback,
        };
        if target.sync().is_err() {
            self.counts.io_errors += 1;
        }
    }

    /// Writes the line held in `self.line`. Returns whether it reached a writer.
    fn emit(&mut self, at: LocalTs, should_flush: bool) -> bool {
        if !self.is_on_fallback {
            self.open_for_day(at);
        }
        if !self.is_on_fallback && self.writer.is_none() {
            self.reopen(at);
        }
        if self.is_on_fallback {
            return self.emit_on_fallback(should_flush);
        }
        let Some(writer) = self.writer.as_mut() else {
            return false;
        };
        let result = writer
            .write_all(self.line.as_bytes())
            .and_then(|()| if should_flush { writer.flush() } else { Ok(()) });
        if result.is_err() {
            self.counts.io_errors += 1;
            self.writer = None;
            return false;
        }
        self.reopens_in_a_row = 0;
        true
    }

    fn emit_on_fallback(&mut self, should_flush: bool) -> bool {
        let fallback = &mut self.targets.fallback;
        let result = fallback
            .write_all(self.line.as_bytes())
            .and_then(|()| if should_flush { fallback.flush() } else { Ok(()) });
        if result.is_err() {
            self.counts.io_errors += 1;
            return false;
        }
        true
    }

    /// A line of a new UTC day closes the writer of the old day and opens the next one.
    fn open_for_day(&mut self, at: LocalTs) {
        let day = at.micros().div_euclid(MICROS_PER_DAY);
        if self.day == Some(day) {
            return;
        }
        if let Some(mut old) = self.writer.take()
            && old.flush().is_err()
        {
            self.counts.io_errors += 1;
        }
        self.day = Some(day);
        match (self.targets.open)(at) {
            Ok(writer) => self.writer = Some(writer),
            Err(_) => self.counts.io_errors += 1,
        }
    }

    /// One reset of the output: a fresh writer from `open`. After `reopen_limit` reopens with no
    /// line written in between, the output gives up on `open` for good and uses the fallback.
    fn reopen(&mut self, at: LocalTs) {
        if self.reopens_in_a_row >= self.targets.reopen_limit {
            self.switch_to_fallback();
            return;
        }
        self.reopens_in_a_row += 1;
        match (self.targets.open)(at) {
            Ok(writer) => {
                self.writer = Some(writer);
                self.counts.reopens += 1;
            }
            Err(_) => {
                self.counts.io_errors += 1;
                if self.reopens_in_a_row >= self.targets.reopen_limit {
                    self.switch_to_fallback();
                }
            }
        }
    }

    fn switch_to_fallback(&mut self) {
        self.is_on_fallback = true;
        self.writer = None;
        let notice = format!(
            "polysim: the log file failed {} reopens in a row; log lines go to stderr from here\n",
            self.reopens_in_a_row
        );
        if self.targets.fallback.write_all(notice.as_bytes()).is_err() {
            self.counts.io_errors += 1;
        }
    }
}

fn push_timestamp(line: &mut String, at: LocalTs) {
    let civil = at.civil();
    write!(
        line,
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
        civil.year, civil.month, civil.day, civil.hour, civil.minute, civil.second, civil.micros
    )
    .ok();
}
