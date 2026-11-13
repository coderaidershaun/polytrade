//! The log drain as a value, with a writer the test owns: a full lane drops, a stall holds up no
//! producer, shutdown drains under a deadline, a failing file falls back to stderr, each counted.

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use engine::hot::dispatch::HotEngine;
use engine::hot::strategy::{Strategy, StrategyCtx};
use engine::log::{
    DrainReport, HotRecord, LaneReader, LogDrain, LogOutput, LogWriter, open_hot_lane,
    open_text_lane,
};
use engine::msg::ids::Side;
use engine::msg::inbound::{InboundMessage, SpinTick};
use engine::msg::time::{EngineClock, LocalTs};

use crate::engine_support::{
    ONE, delta_chunk, engine_without_persistence, instrument_row, spin, strategy_log_ring,
    tracker_spec_all,
};

/// A log writer in memory that counts flushes and syncs, so a test sees what reached the output
/// and when it was pushed out.
#[derive(Clone, Default)]
struct MemoryWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
    flushes: Arc<AtomicUsize>,
    syncs: Arc<AtomicUsize>,
}

impl MemoryWriter {
    fn lines(&self) -> Vec<String> {
        let bytes = self.bytes.lock().expect("no writer panicked");
        String::from_utf8(bytes.clone())
            .expect("log lines are utf-8")
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

impl Write for MemoryWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes
            .lock()
            .expect("no writer panicked")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl LogWriter for MemoryWriter {
    fn sync(&mut self) -> io::Result<()> {
        self.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// A disk that is full: every write fails.
struct FullDisk;

impl Write for FullDisk {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("no space left on device"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl LogWriter for FullDisk {}

/// A disk that stalls: the first write waits until the test lets it go.
struct StalledDisk {
    release: Arc<Mutex<mpsc::Receiver<()>>>,
    inner: MemoryWriter,
}

impl Write for StalledDisk {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let release = self.release.lock().expect("no writer panicked");
        release.recv().ok();
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl LogWriter for StalledDisk {}

fn memory_output(writer: &MemoryWriter) -> LogOutput {
    let writer = writer.clone();
    LogOutput {
        open: Box::new(move |_at: LocalTs| Ok(Box::new(writer.clone()) as Box<dyn LogWriter>)),
        fallback: Box::new(MemoryWriter::default()),
        reopen_limit: 3,
    }
}

/// Banks one strategy line for each spin.
struct NoteEachSpin;

impl Strategy for NoteEachSpin {
    fn on_spin(&mut self, ctx: &mut StrategyCtx<'_>, tick: &SpinTick) {
        engine::note!(ctx, "spin {}", tick.seq);
    }
}

fn engine_on_this_thread() -> (HotEngine, LaneReader<HotRecord>) {
    let instruments = [instrument_row(0, tracker_spec_all(1), 64)];
    let (log_sink, strategy_records) = strategy_log_ring(64);
    let strategy_lane = LaneReader::new(strategy_records, log_sink.drops_handle());
    let engine = engine_without_persistence(&instruments, Box::new(NoteEachSpin), log_sink);
    (engine, strategy_lane)
}

/// Each delta for a book that awaits its snapshot writes one hot warning.
fn write_hot_warnings(engine: &mut HotEngine, count: i64) {
    for when in 1..=count {
        let delta = delta_chunk(0, Side::Buy, &[(100 * ONE, ONE)], when);
        engine.dispatch(&InboundMessage::Book(delta));
    }
}

fn write_strategy_notes(engine: &mut HotEngine, count: i64) {
    for seq in 1..=count {
        let message = InboundMessage::SpinTick(spin(seq as u64, 1_000 + seq));
        engine.dispatch(&message);
    }
}

/// The hot thread: it builds its hot lane, writes `count` hot warnings, and ends, so its lanes are
/// abandoned when the readers come back.
fn hot_thread_that_ended(capacity: usize, count: i64) -> LaneReader<HotRecord> {
    thread::spawn(move || {
        let hot_lane = open_hot_lane(capacity);
        let (mut engine, _strategy_lane) = engine_on_this_thread();
        write_hot_warnings(&mut engine, count);
        hot_lane
    })
    .join()
    .expect("the hot thread finished")
}

#[test]
fn full_log_lane_drops_counts_and_reports() {
    let hot_lane = hot_thread_that_ended(4, 10);
    assert_eq!(hot_lane.len(), 4, "the lane holds what fits");
    assert_eq!(hot_lane.dropped(), 6, "the rest was dropped and counted");

    let writer = MemoryWriter::default();
    let mut drain = LogDrain::new(memory_output(&writer), EngineClock::start());
    drain.add_hot_lane("hot", hot_lane);
    drain.drain_once();
    let report = drain.finish(Duration::from_secs(2));

    let lines = writer.lines();
    let warnings = lines
        .iter()
        .filter(|line| line.contains("delta chunk dropped: instrument 0 awaiting snapshot"))
        .count();
    assert_eq!(warnings, 4, "each record that fit became a line");
    assert!(
        lines
            .iter()
            .any(|line| line.ends_with("lane \"hot\" dropped 6 record(s) (6 total, ring full)")),
        "the drain reported the drops in the log: {lines:?}"
    );
    assert_eq!(report.dropped, 6, "the report carries the drops");
    assert_eq!(report.written, 4);
}

#[test]
fn stalled_drain_never_blocks_the_hot_thread() {
    const WARNINGS: i64 = 100;
    let (reader_tx, reader_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let hot = thread::spawn(move || {
        let hot_lane = open_hot_lane(4);
        let (mut engine, _strategy_lane) = engine_on_this_thread();
        reader_tx.send(hot_lane).expect("the test takes the reader");
        go_rx.recv().expect("the test says go");
        write_hot_warnings(&mut engine, WARNINGS);
        done_tx.send(()).expect("the test waits for the end");
    });

    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release = Arc::new(Mutex::new(release_rx));
    let output = LogOutput {
        open: Box::new(move |_at: LocalTs| {
            let disk = StalledDisk {
                release: Arc::clone(&release),
                inner: MemoryWriter::default(),
            };
            Ok(Box::new(disk) as Box<dyn LogWriter>)
        }),
        fallback: Box::new(MemoryWriter::default()),
        reopen_limit: 3,
    };
    let mut drain = LogDrain::new(output, EngineClock::start());
    drain.add_hot_lane(
        "hot",
        reader_rx.recv().expect("the hot thread sends its reader"),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let drain_stop = Arc::clone(&stop);
    let drain_thread = thread::spawn(move || {
        while !drain_stop.load(Ordering::Acquire) {
            drain.drain_once();
            thread::sleep(Duration::from_millis(1));
        }
        drain.finish(Duration::from_secs(2))
    });

    go_tx.send(()).expect("the hot thread waits");
    let finished = done_rx.recv_timeout(Duration::from_secs(30));
    for _ in 0..WARNINGS + 10 {
        release_tx.send(()).ok();
    }
    stop.store(true, Ordering::Release);
    assert!(
        finished.is_ok(),
        "the hot thread waited on a drain stuck in a write"
    );
    hot.join().expect("the hot thread finished");
    drop(release_tx);
    let report: DrainReport = drain_thread.join().expect("the drain thread finished");
    assert!(
        report.dropped > 0,
        "the full lane dropped records: {report:?}"
    );
    assert_eq!(
        report.written + report.dropped,
        WARNINGS as u64,
        "each record was written or counted: {report:?}"
    );
}

#[test]
fn shutdown_drains_every_lane_before_exit() {
    let (hot_lane, strategy_lane) = thread::spawn(|| {
        let hot_lane = open_hot_lane(64);
        let (mut engine, strategy_lane) = engine_on_this_thread();
        write_hot_warnings(&mut engine, 10);
        write_strategy_notes(&mut engine, 5);
        (hot_lane, strategy_lane)
    })
    .join()
    .expect("the hot thread finished");
    assert_eq!(hot_lane.len(), 10);
    assert_eq!(strategy_lane.len(), 5);
    let main_lane = open_text_lane(16);
    engine::info!("main line {}", 1);
    engine::info!("main line {}", 2);
    assert_eq!(main_lane.len(), 2);

    let writer = MemoryWriter::default();
    let mut drain = LogDrain::new(memory_output(&writer), EngineClock::start());
    drain.add_hot_lane("hot", hot_lane);
    drain.add_hot_lane("strategy", strategy_lane);
    drain.add_text_lane("main", main_lane);
    let report = drain.finish(Duration::from_secs(2));

    assert_eq!(writer.lines().len(), 17, "each record became a line");
    assert_eq!(
        report,
        DrainReport {
            written: 17,
            ..DrainReport::default()
        },
        "no line lost, none dropped, no I/O error, each lane finished"
    );
    assert!(
        writer.flushes.load(Ordering::Relaxed) >= 1,
        "the output was flushed"
    );
    assert!(
        writer.syncs.load(Ordering::Relaxed) >= 1,
        "the output was synced"
    );
}

#[test]
fn shutdown_deadline_counts_lost_lines() {
    let (reader_tx, reader_rx) = mpsc::channel();
    let (end_tx, end_rx) = mpsc::channel::<()>();
    let hot = thread::spawn(move || {
        let hot_lane = open_hot_lane(64);
        let (mut engine, _strategy_lane) = engine_on_this_thread();
        write_hot_warnings(&mut engine, 10);
        reader_tx.send(hot_lane).expect("the test takes the reader");
        end_rx.recv().ok();
    });

    let writer = MemoryWriter::default();
    let mut drain = LogDrain::new(memory_output(&writer), EngineClock::start());
    drain.add_hot_lane(
        "hot",
        reader_rx.recv().expect("the hot thread sends its reader"),
    );
    let report = drain.finish(Duration::ZERO);
    end_tx.send(()).ok();
    hot.join().expect("the hot thread finished");

    assert_eq!(
        report.lost, 10,
        "each record still waiting at the deadline is counted"
    );
    assert_eq!(report.written, 0);
    assert_eq!(
        report.unfinished,
        vec!["hot"],
        "the lane that did not finish is named"
    );
}

/// A text lane on the calling thread that holds `count` lines.
fn main_lane_with_lines(count: u64) -> LaneReader<engine::log::TextRecord> {
    let main_lane = open_text_lane(16);
    for line in 1..=count {
        engine::info!("main line {line}");
    }
    main_lane
}

#[test]
fn log_write_error_reopens_then_falls_back() {
    let main_lane = main_lane_with_lines(2);
    let reopened = MemoryWriter::default();
    let opened = Arc::new(AtomicUsize::new(0));
    let open_count = Arc::clone(&opened);
    let after_reopen = reopened.clone();
    let output = LogOutput {
        open: Box::new(move |_at: LocalTs| {
            let writer: Box<dyn LogWriter> = match open_count.fetch_add(1, Ordering::Relaxed) {
                0 => Box::new(FullDisk),
                _ => Box::new(after_reopen.clone()),
            };
            Ok(writer)
        }),
        fallback: Box::new(MemoryWriter::default()),
        reopen_limit: 3,
    };
    let mut drain = LogDrain::new(output, EngineClock::start());
    drain.add_text_lane("main", main_lane);
    let report = drain.finish(Duration::from_secs(2));

    assert_eq!(opened.load(Ordering::Relaxed), 2, "one open, one reopen");
    assert_eq!(report.reopens, 1);
    assert_eq!(report.lost, 1, "the line that failed is lost and counted");
    assert_eq!(report.io_errors, 1);
    assert_eq!(report.written, 1);
    let lines = reopened.lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0].ends_with("main line 2"),
        "the next line reached the reopened file"
    );
}

#[test]
fn log_reopen_limit_falls_back_to_stderr() {
    let main_lane = main_lane_with_lines(5);
    let opened = Arc::new(AtomicUsize::new(0));
    let open_count = Arc::clone(&opened);
    let stderr = MemoryWriter::default();
    let output = LogOutput {
        open: Box::new(move |_at: LocalTs| {
            if open_count.fetch_add(1, Ordering::Relaxed) == 0 {
                return Ok(Box::new(FullDisk) as Box<dyn LogWriter>);
            }
            Err(io::Error::other("log volume is gone"))
        }),
        fallback: Box::new(stderr.clone()),
        reopen_limit: 3,
    };
    let mut drain = LogDrain::new(output, EngineClock::start());
    drain.add_text_lane("main", main_lane);
    let report = drain.finish(Duration::from_secs(2));

    assert_eq!(
        opened.load(Ordering::Relaxed),
        4,
        "one open and three reopens, and no fourth reopen"
    );
    assert_eq!(
        report.lost, 3,
        "the lines before the fallback are lost and counted"
    );
    assert_eq!(report.written, 2);
    assert_eq!(
        report.io_errors, 4,
        "one failed write and three failed reopens"
    );
    let fallback: Vec<String> = stderr
        .lines()
        .into_iter()
        .filter(|line| line.contains("main line"))
        .collect();
    assert_eq!(fallback.len(), 2, "{fallback:?}");
    assert!(fallback[0].ends_with("main line 4"));
    assert!(fallback[1].ends_with("main line 5"));
}
