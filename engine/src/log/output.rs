//! Single output target (stderr in debug, buffered file in release). Engine + strategy records alike; the tag column is what tells them apart.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use super::LogConfig;
use super::producer::BacktraceMessage;
use super::record::LogRecord;
use crate::time::{TsUs, boot_stamp_us};

const FLUSH_INTERVAL: Duration = Duration::from_secs(1);

pub(super) struct Output {
    target: Box<dyn Write + Send>,
    last_flush: Instant,
}

pub(super) struct DropNotice {
    pub(super) since_last: u64,
    pub(super) total: u64,
}

impl Output {
    pub(super) fn open(config: &LogConfig) -> Self {
        let target: Box<dyn Write + Send> = if cfg!(debug_assertions) {
            Box::new(io::stderr())
        } else {
            open_file_target(&config.dir, &config.file_stem)
        };
        Self {
            target,
            last_flush: Instant::now(),
        }
    }

    pub(super) fn write_record(&mut self, tag: &str, record: &LogRecord) {
        let line = format!(
            "{ts} {level:<5} [{tag}] {module} ({file}:{line}) {msg}\n",
            ts = format_timestamp(record.ts_us),
            level = record.level.as_str(),
            module = record.module,
            file = record.file,
            line = record.line,
            msg = record.message(),
        );
        self.write_line(&line);
        if record.level.should_flush_immediately() {
            self.flush();
        }
    }

    pub(super) fn write_backtrace(&mut self, message: &BacktraceMessage) {
        let line = format!(
            "{ts} ERROR [{module} {file}:{line}] error backtrace:\n{backtrace}\n",
            ts = format_timestamp(message.ts_us),
            module = message.module,
            file = message.file,
            line = message.line,
            backtrace = message.backtrace,
        );
        self.write_line(&line);
        self.flush();
    }

    pub(super) fn write_drop_notice(&mut self, tag: &str, notice: DropNotice) {
        let line = format!(
            "{ts} WARN  [logging] lane {tag:?} dropped {since_last} record(s) ({total} total, ring full)\n",
            ts = format_timestamp(boot_stamp_us()),
            since_last = notice.since_last,
            total = notice.total,
        );
        self.write_line(&line);
        self.flush();
    }

    pub(super) fn flush_if_due(&mut self) {
        if self.last_flush.elapsed() >= FLUSH_INTERVAL {
            self.flush();
        }
    }

    pub(super) fn flush(&mut self) {
        if let Err(err) = self.target.flush() {
            eprintln!("polysim: log sink flush failed: {err}");
        }
        self.last_flush = Instant::now();
    }

    fn write_line(&mut self, line: &str) {
        if let Err(err) = self.target.write_all(line.as_bytes()) {
            eprintln!("polysim: log sink write failed: {err}");
        }
    }
}

fn open_file_target(dir: &Path, stem: &str) -> Box<dyn Write + Send> {
    match open_log_file(dir, stem) {
        Ok(file) => Box::new(BufWriter::new(file)),
        Err(err) => {
            eprintln!(
                "polysim: cannot open log file in {}: {err}; falling back to stderr",
                dir.display()
            );
            Box::new(io::stderr())
        }
    }
}

fn open_log_file(dir: &Path, stem: &str) -> io::Result<File> {
    fs::create_dir_all(dir)?;
    let path = dir.join(format!("{stem}-{}.log", today_date()));
    OpenOptions::new().create(true).append(true).open(path)
}

fn format_timestamp(ts: TsUs) -> String {
    let at = ts.civil();
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
        at.year, at.month, at.day, at.hour, at.minute, at.second, at.micros
    )
}

fn today_date() -> String {
    let today = boot_stamp_us().civil();
    format!("{:04}-{:02}-{:02}", today.year, today.month, today.day)
}
