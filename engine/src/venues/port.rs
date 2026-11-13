//! The venue reads of the start checks, behind one trait: `NetworkPort` asks the venues, and a
//! stand-in answers a test with no network, no key, and no `.env`.

use std::io;

use tokio::runtime::{Builder, Runtime};

use crate::msg::ids::InstrumentId;
use crate::runtime::config::SourceSpec;
use crate::runtime::registry::{OrderRateLimit, Registry};
use crate::venues::binance::preflight::SymbolScales;
use crate::venues::edge::rest_quiet::SharedRestQuiet;
use crate::venues::polymarket::preflight::stamp_poly_scales;
use crate::venues::polymarket::rest::GammaMarket;
use crate::venues::venue::{self, ExecutionVenue, VenueError};

/// What the scale read of each venue of a run found, before preflight stamps it on the registry.
#[derive(Debug, Default)]
pub struct VenueScales {
    pub binance: Vec<(InstrumentId, SymbolScales)>,
    /// The ORDERS buckets that the Binance instruments were read with.
    pub order_rate_limits: Vec<OrderRateLimit>,
    /// The current window of a Polymarket series, whose grid each Polymarket row takes.
    pub polymarket: Option<GammaMarket>,
}

impl VenueScales {
    pub(crate) fn stamp(self, registry: &mut Registry) {
        let has_binance = !self.binance.is_empty();
        for (instrument, scales) in self.binance {
            registry.set_scales(instrument, scales.tick_size, scales.step_size);
            registry.set_binance_limits(instrument, scales.limits);
        }
        if has_binance {
            registry.set_order_rate_limits(self.order_rate_limits);
        }
        if let Some(market) = &self.polymarket {
            stamp_poly_scales(registry, market);
        }
    }
}

/// A venue whose permission probe passed, with the credentials its edge trades with. Only a real
/// probe makes one.
pub struct ProbedVenue(pub(crate) ExecutionVenue);

pub trait VenuePort {
    /// # Errors
    /// [`VenueError`] when a venue cannot be read, or lists an instrument the engine cannot
    /// represent.
    fn read_scales(&mut self, registry: &Registry) -> Result<VenueScales, VenueError>;

    /// # Errors
    /// [`VenueError`] when the credentials of the venue that `source` names cannot trade.
    fn probe(
        &mut self,
        source: &SourceSpec,
        registry: &Registry,
        quiet: &SharedRestQuiet,
    ) -> Result<ProbedVenue, VenueError>;
}

/// The venues themselves, read on one current-thread runtime that ends with this value, so a
/// start check leaves no thread behind. Its probe reads the keys of `.env`.
pub(crate) struct NetworkPort {
    runtime: Runtime,
}

impl NetworkPort {
    pub(crate) fn new() -> io::Result<Self> {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .thread_name("polysim-preflight")
            .build()?;
        Ok(Self { runtime })
    }
}

impl VenuePort for NetworkPort {
    fn read_scales(&mut self, registry: &Registry) -> Result<VenueScales, VenueError> {
        self.runtime.block_on(venue::read_scales(registry))
    }

    fn probe(
        &mut self,
        source: &SourceSpec,
        registry: &Registry,
        quiet: &SharedRestQuiet,
    ) -> Result<ProbedVenue, VenueError> {
        self.runtime
            .block_on(ExecutionVenue::probe(source, registry, quiet.clone()))
            .map(ProbedVenue)
    }
}
