//! Records a run on one thread, as the hot thread folds it: each message through the hot driver with
//! the tee on, from one of four input queues, the writer pumped as the test asks, and the venue of
//! the test answering each command. A re-fold of its tape has a real run to equal.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use engine::hot::dispatch::StateDigest;
use engine::hot::ingress::IngressQueues;
use engine::hot::spawn::{HotDriver, QueueProducer, Step};
use engine::hot::strategy::{EngineView, Strategy, StrategyConfig};
use engine::msg::exec::{ExecCommand, ExecLaneItem};
use engine::msg::ids::{InstrumentId, QueueId, SourceId, VenueId};
use engine::msg::inbound::{InboundMessage, MessageBody};
use engine::msg::latency::EngineCounters;
use engine::msg::persist::BootExposure;
use engine::msg::time::LocalTs;
use engine::runtime::config::{Config, RunIdentity};
use engine::runtime::manifest::{
    EndState, InstrumentTerms, RunEnd, RunId, RunKind, VenueEntry, VenueTerms,
};
use engine::runtime::metrics::{DropRow, RollupRow, RollupSink, SinkKind};
use engine::runtime::registry::Registry;
use engine::runtime::shutdown::FatalSignal;
use engine::runtime::{FoldMetrics, FoldRings, FoldTape, RunFold, RunFoldSetup, build_run_fold};
use rtrb::{Producer, RingBuffer};

use crate::venue::TestVenue;

const QUEUE_CAPACITY: usize = 65_536;

/// The order that the queues are read in when two heads hold the same stamp.
const MARKET_QUEUE: usize = 0;
const TIMER_QUEUE: usize = 1;
const EXEC_QUEUE: usize = 2;
const AUDIT_STATUS_QUEUE: usize = 3;

/// The boot stamp of each recorded run. Each test writes below its own root.
pub(crate) const BOOT_US: i64 = 1_784_000_000_000_000;

/// When the writer of the recorded run drains its rings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pumping {
    /// After each step, so no ring fills.
    EachStep,
    /// After each `n`-th step, so a small research ring drops rows between pumps.
    EveryNth(usize),
    /// Not until the fold holds the bank's message, so the audit ring fills and the bank posts.
    AfterBankPost,
}

/// How the recorded run states its research drops in `metrics_drops`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DropReport {
    AsCounted,
    /// Each count one less than the fold dropped, as a writer that lost a row of its own would.
    OneShort,
}

pub(crate) struct RunPlan<'a> {
    pub(crate) config_text: &'a str,
    pub(crate) tape: Vec<InboundMessage>,
    /// `Some` for a run that trades: its nonce. The venue of the test answers each command.
    pub(crate) run_nonce: Option<u32>,
    pub(crate) exposures: Vec<BootExposure>,
    pub(crate) rings: FoldRings,
    pub(crate) pumping: Pumping,
    pub(crate) drops: DropReport,
    /// The terms that preflight would read from the venue.
    pub(crate) terms: fn(&mut InstrumentTerms),
}

/// The run on disk and what the test reads of its fold.
pub(crate) struct Recorded {
    pub(crate) run_dir: PathBuf,
    pub(crate) digest: StateDigest,
    pub(crate) places: usize,
    pub(crate) fills: usize,
    pub(crate) counters: EngineCounters,
}

pub(crate) fn record<S>(plan: RunPlan<'_>) -> Recorded
where
    S: Strategy + StrategyConfig + 'static,
{
    let config = Config::<S::Params>::from_yaml(plan.config_text).expect("the test config parses");
    let identity = identity();
    let registry = stamped_registry(&config, plan.terms);
    let engine_view = EngineView {
        spin_interval: config.engine.spin_interval(),
    };
    let strategy = Box::new(S::from_spec(&config.strategy, engine_view));
    let (status_producer, status_consumer) = RingBuffer::<InboundMessage>::new(4);
    let bank_full_to = QueueProducer::new(
        status_producer,
        FatalSignal::new(),
        queue(AUDIT_STATUS_QUEUE),
        SourceId(0),
    );
    let RunFold {
        engine,
        mut commands,
        tee,
        mut writer,
        mut rollups,
        manifest,
        run_dir,
    } = build_run_fold(RunFoldSetup {
        config: &config,
        identity: &identity,
        registry: &registry,
        strategy,
        kind: RunKind::Sim,
        run_id: RunId::from_micros(BOOT_US),
        venues: venues(&registry, plan.run_nonce),
        exposures: plan.exposures,
        source: None,
        tape: FoldTape::Recorded,
        metrics: FoldMetrics::Written,
        rings: plan.rings,
        bank_full_to: Some(bank_full_to),
    })
    .expect("the fold of the test run builds");

    let (mut producers, consumers): (Vec<Producer<InboundMessage>>, Vec<_>) = (0..3)
        .map(|_| RingBuffer::<InboundMessage>::new(QUEUE_CAPACITY))
        .unzip();
    let mut ingress_consumers = consumers;
    ingress_consumers.push(status_consumer);
    let tee = tee.expect("the run records its tape");
    let mut driver =
        HotDriver::new(IngressQueues::new(ingress_consumers), engine, None).with_tape(tee);
    let row = &registry.instruments()[0];
    let mut venue = TestVenue::new(row.base_asset, row.quote_asset);
    let start = plan.tape[0].header.received_ts_us.micros();
    if plan.run_nonce.is_some() {
        venue.open(InstrumentId(0), start);
    }

    let mut run = Running {
        steps: 0,
        places: 0,
        is_pumping: plan.pumping != Pumping::AfterBankPost,
        high_mark: plan.rings.bank.high_mark as u64,
    };
    for message in &plan.tape {
        let at = message.header.received_ts_us.micros();
        for answer in venue.due_through(at) {
            push(&mut producers[EXEC_QUEUE], answer);
        }
        push(&mut producers[queue_of(message)], *message);
        venue.on_market(message);
        run.fold(
            &mut driver,
            &mut commands,
            &mut venue,
            &mut writer,
            plan.pumping,
        );
    }
    for answer in venue.due_through(i64::MAX) {
        push(&mut producers[EXEC_QUEUE], answer);
    }
    run.fold(
        &mut driver,
        &mut commands,
        &mut venue,
        &mut writer,
        plan.pumping,
    );

    let counters = driver.engine().counters();
    let end_us = plan.tape.last().expect("a tape").header.received_ts_us;
    let rollups = rollups
        .as_mut()
        .expect("the recorded run writes its metrics rows");
    write_drops(rollups, &counters, plan.drops, end_us);
    let stopped = writer.finish().expect("the writer of the test run drains");
    manifest
        .close(RunEnd {
            state: EndState::Graceful,
            reason: None,
            stopped_tables: stopped
                .tables()
                .into_iter()
                .map(|table| table.as_str().to_owned())
                .collect(),
        })
        .expect("the manifest of the test run closes");
    Recorded {
        run_dir,
        digest: driver.engine().state_digest(),
        places: run.places,
        fills: venue.fills,
        counters,
    }
}

struct Running {
    steps: usize,
    places: usize,
    is_pumping: bool,
    high_mark: u64,
}

impl Running {
    /// Folds each message that waits, each command to the venue, and pumps as `pumping` says.
    fn fold(
        &mut self,
        driver: &mut HotDriver,
        commands: &mut [rtrb::Consumer<ExecLaneItem>],
        venue: &mut TestVenue,
        writer: &mut engine::tape::InlineWriter,
        pumping: Pumping,
    ) {
        while driver.step() == Step::Folded {
            self.steps += 1;
            for lane in commands.iter_mut() {
                while let Ok(item) = lane.pop() {
                    if let ExecLaneItem::Command(stamped) = item {
                        self.places +=
                            usize::from(matches!(stamped.command, ExecCommand::Place { .. }));
                        venue.on_command(&stamped);
                    }
                }
            }
            if !self.is_pumping && driver.engine().counters().audit_banked_peak >= self.high_mark {
                self.is_pumping = true;
            }
            let is_due = match pumping {
                Pumping::EachStep => true,
                Pumping::EveryNth(n) => self.steps.is_multiple_of(n),
                Pumping::AfterBankPost => self.is_pumping,
            };
            if is_due {
                writer.pump();
            }
        }
    }
}

fn push(producer: &mut Producer<InboundMessage>, message: InboundMessage) {
    producer
        .push(message)
        .expect("each test queue holds a whole tape");
}

fn queue(index: usize) -> QueueId {
    QueueId::new(index).expect("the test reads four queues")
}

fn queue_of(message: &InboundMessage) -> usize {
    match message.body {
        MessageBody::SpinTick(_) => TIMER_QUEUE,
        MessageBody::Exec(_) | MessageBody::Account(_) => EXEC_QUEUE,
        _ => MARKET_QUEUE,
    }
}

/// One identity for each recorded run: a run is found by its folder, never by this name.
fn identity() -> RunIdentity {
    RunIdentity::new("strat-micro-recorder", "te-refold").expect("a valid identity")
}

fn stamped_registry<P>(config: &Config<P>, stamp: fn(&mut InstrumentTerms)) -> Registry {
    let mut registry = Registry::build(config).expect("the test config builds a registry");
    let mut terms = InstrumentTerms::of(&registry);
    terms.iter_mut().for_each(stamp);
    registry
        .stamp_terms(&terms)
        .expect("the terms of the test fit its registry");
    registry
}

/// The one venue of the run, with the terms of the simulator when it trades.
fn venues(registry: &Registry, run_nonce: Option<u32>) -> Vec<VenueEntry> {
    vec![VenueEntry {
        venue_id: 0,
        name: registry.venue_name(VenueId(0)).to_owned(),
        run_nonce,
        capabilities: run_nonce.map(|_| VenueTerms {
            holds_reservations_until_settled: true,
            fee_model: "none".to_owned(),
            order_budget: Vec::new(),
            rotates_markets: false,
            base_asset_is_position: false,
        }),
    }]
}

/// The research drops of the fold, as the metrics thread writes them at the end of the run.
fn write_drops(
    rollups: &mut engine::tape::RollupRows,
    counters: &EngineCounters,
    report: DropReport,
    at: LocalTs,
) {
    let drops = counters.research_dropped;
    let less = match report {
        DropReport::AsCounted => 0,
        DropReport::OneShort => 1,
    };
    for (sink, dropped) in [
        (SinkKind::ResearchFeatures, drops.features),
        (SinkKind::ResearchTrades, drops.trades),
        (SinkKind::ResearchBookEvents, drops.book_events),
        (SinkKind::ResearchKlines, drops.klines),
        (SinkKind::ResearchLinkFrames, drops.link_frames),
        (SinkKind::ResearchMids, drops.mids),
        (SinkKind::ResearchNoQuotes, drops.no_quotes),
    ] {
        rollups.write(RollupRow::Drop(DropRow {
            interval_end_ts_us: at,
            sink,
            dropped_total: dropped.saturating_sub(less),
        }));
    }
}

/// A folder below the temporary folder for one test, removed when the test ends.
pub(crate) struct TempRoot(PathBuf);

impl TempRoot {
    pub(crate) fn new(name: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("lab-{name}-{}-{n}", std::process::id()));
        if path.exists() {
            std::fs::remove_dir_all(&path).expect("a stale test folder can be removed");
        }
        std::fs::create_dir_all(&path).expect("the test folder can be made");
        Self(path)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}
