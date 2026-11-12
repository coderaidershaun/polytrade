//! What the engine is built from: the wiring a run hands in, and the state and dense ids that
//! wiring resolves to before the first message. Separate from the loop body so a new optional
//! subsystem lands here rather than lengthening the constructor.

use crate::config::RecordedTables;
use crate::hot::book::Book;
use crate::hot::exec::{ExecEngineSetup, ExecSettings};
use crate::hot::quant::volatility::EwmaVol;
use crate::hot::strategy::{Registration, Strategy};
use crate::hot::tracker::MicroTracker;
use crate::link::{LinkHash, TopicId, schema_hash_of_fields};
use crate::msg::persist::FeatureId;
use crate::registry::InstrumentRow;
use crate::sink::{ExecSink, MetricsSink, PersistSink, StrategyLogSink, UiBookSink, UiEventSink};
use crate::time::DurationUs;

use super::{ExposureWiring, LinkWiring};

pub struct HotEngineSetup<'a> {
    pub instruments: &'a [InstrumentRow],
    pub strategy: Box<dyn Strategy>,
    /// None = no persistence (emit* discards).
    pub persistence: Option<PersistWiring>,
    pub strategy_log_sink: StrategyLogSink,
    pub metrics_sink: MetricsSink,
    pub ui_book_sink: UiBookSink,
    pub ui_event_sink: UiEventSink,
    pub link: Option<LinkWiring>,
    pub warmup: DurationUs,
    pub exec: Option<ExecWiring>,
    /// Never absent: position outlives the run, so even a flat position keeps exposure wired.
    pub exposure: ExposureWiring<'a>,
}

/// Persistence wiring: sink + tables (paired, never disagree). strategy.tables = authority.
pub struct PersistWiring {
    pub sink: PersistSink,
    pub tables: RecordedTables,
}

pub struct ExecWiring {
    pub sink: ExecSink,
    pub settings: ExecSettings,
    pub run_nonce: u32,
}

impl ExecWiring {
    /// The absent case is a real configuration, not a hole to fill with zeroes: settings that refuse
    /// every order, and a nonce nothing reads because minting a client id needs a sink to send it
    /// through.
    pub(super) fn engine_setup(
        wiring: Option<Self>,
        instruments: &[InstrumentRow],
    ) -> ExecEngineSetup<'_> {
        match wiring {
            Some(ExecWiring {
                sink,
                settings,
                run_nonce,
            }) => ExecEngineSetup {
                instruments,
                run_nonce,
                settings,
                sink: Some(sink),
            },
            None => ExecEngineSetup {
                instruments,
                run_nonce: 0,
                settings: ExecSettings::disabled(),
                sink: None,
            },
        }
    }
}

/// The dense ids the engine minted from a strategy's declarations, kept after registration handed
/// them over: names for the feature catalog, the digest every banked payload carries, and the topic
/// count `StrategyCtx::link_send` bounds against.
pub(super) struct Declarations {
    pub(super) feature_names: Vec<&'static str>,
    pub(super) link_schema_hash: LinkHash,
    pub(super) link_topics: usize,
}

impl Declarations {
    /// Ids are dense and assigned in declaration order, which registration is the strategy's one
    /// chance to learn.
    ///
    /// # Panics
    /// More than 65536 features declared, or a link field name too long for the wire.
    pub(super) fn resolve(strategy: &mut dyn Strategy, instruments: &[InstrumentRow]) -> Self {
        let feature_names: Vec<&'static str> = strategy.features().to_vec();
        let feature_ids: Vec<FeatureId> = (0..feature_names.len())
            .map(|index| {
                let raw = u16::try_from(index)
                    .expect("strategy declares more than 65536 features — feature id overflow");
                FeatureId(raw)
            })
            .collect();
        let link_schema_hash = schema_hash_of_fields(strategy.link_fields());
        let topic_ids: Vec<TopicId> = (0..strategy.link_topics().len())
            .map(TopicId::strategy)
            .collect();
        strategy.register(Registration {
            features: &feature_ids,
            feature_names: &feature_names,
            instruments,
            link_topics: &topic_ids,
        });
        Self {
            feature_names,
            link_schema_hash,
            link_topics: topic_ids.len(),
        }
    }
}

/// The three per-instrument series, built in one pass so their lengths cannot drift from each other
/// or from the registry every hot-path index is taken against.
pub(super) fn per_instrument_state(
    instruments: &[InstrumentRow],
) -> (Vec<Book>, Vec<MicroTracker>, Vec<Option<EwmaVol>>) {
    let mut books = Vec::with_capacity(instruments.len());
    let mut trackers = Vec::with_capacity(instruments.len());
    let mut ewma = Vec::with_capacity(instruments.len());
    for row in instruments {
        books.push(Book::new(row.book_capacity));
        trackers.push(MicroTracker::new(
            &row.tracker,
            &row.kline_intervals,
            row.tick_size,
        ));
        ewma.push(
            row.tracker
                .ewma_vol
                .as_ref()
                .map(|spec| EwmaVol::new(spec.halflife_events)),
        );
    }
    (books, trackers, ewma)
}
