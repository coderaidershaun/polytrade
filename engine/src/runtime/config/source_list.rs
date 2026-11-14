//! How the `sources:` list maps to venues and to the orders of the run: the venue id of each
//! source, the sources that send orders, and the checks on each venue's execution block.

use super::execution::{ExecutionConfig, ExecutionMode};
use super::sources::{BinanceMarket, SourceConfig, SourceSpec};
use super::{Config, ConfigError};
use crate::msg::ids::VenueId;

impl<P: serde::de::DeserializeOwned + Default> Config<P> {
    /// The sources of one venue sit together and give it one execution block at most, and the run
    /// has one mode: the mode names the files of a run, so two sources that send orders send them
    /// in the same mode.
    pub(super) fn check_sources(&self) -> Result<(), ConfigError> {
        if self.sources.is_empty() {
            return Err(ConfigError::NoSource);
        }
        for (second, source) in self.sources.iter().enumerate() {
            let venue = source.feed.venue();
            let earlier = &self.sources[..second];
            let first = earlier.iter().position(|each| each.feed.venue() == venue);
            if let Some(first) = first
                && earlier[second - 1].feed.venue() != venue
            {
                return Err(ConfigError::VenueSourcesApart {
                    venue,
                    first,
                    second,
                });
            }
            let first_execution = earlier
                .iter()
                .position(|each| each.feed.venue() == venue && each.execution.is_some());
            if let (Some(first), Some(_)) = (first_execution, &source.execution) {
                return Err(ConfigError::VenueExecutionRepeated {
                    venue,
                    first,
                    second,
                });
            }
        }
        let mut armed = self
            .sources
            .iter()
            .enumerate()
            .filter_map(|(index, source)| Some((index, source.armed_execution()?.mode)));
        let Some((first, mode)) = armed.next() else {
            return Ok(());
        };
        match armed.find(|(_, other)| *other != mode) {
            Some((second, other)) => Err(ConfigError::SourceModesDiffer {
                first,
                mode: mode.as_str(),
                second,
                other: other.as_str(),
            }),
            None => Ok(()),
        }
    }
}

pub(super) fn check_execution_source(
    source: &SourceSpec,
    mode: ExecutionMode,
) -> Result<(), ConfigError> {
    if !mode.is_enabled() {
        return Ok(());
    }
    if !source.supports_execution(mode) {
        let supported: Vec<&str> = source
            .execution_modes()
            .iter()
            .copied()
            .map(ExecutionMode::as_str)
            .collect();
        return Err(ConfigError::ExecutionModeUnsupported {
            venue: source.venue(),
            mode: mode.as_str(),
            supported: supported.join(", ").into_boxed_str(),
        });
    }
    let SourceSpec::Binance {
        market,
        subscriptions,
        ..
    } = source
    else {
        return Ok(());
    };
    if !mode.is_simulated() {
        return Ok(());
    }
    if *market != BinanceMarket::Spot {
        return Err(ConfigError::SimulatedExecutionMarket {
            market: market.as_str(),
        });
    }
    let missing = subscriptions.disabled_streams();
    if !missing.is_empty() {
        return Err(ConfigError::SimulatedExecutionSubscriptions {
            missing: missing.join(", ").into_boxed_str(),
        });
    }
    Ok(())
}

impl<P> Config<P> {
    /// The execution mode of the run: the mode of each source that sends orders, else `off` when a
    /// source has an execution block, else `None`.
    pub fn execution_mode(&self) -> Option<ExecutionMode> {
        let armed = self.sources.iter().find_map(SourceConfig::armed_execution);
        armed
            .or_else(|| {
                self.sources
                    .iter()
                    .find_map(|source| source.execution.as_ref())
            })
            .map(|execution| execution.mode)
    }

    /// The venue of each source, in source order. The sources of one venue sit together, so a new id
    /// starts where the venue changes.
    pub(crate) fn source_venues(&self) -> Vec<VenueId> {
        let mut venues: Vec<VenueId> = Vec::with_capacity(self.sources.len());
        for (index, source) in self.sources.iter().enumerate() {
            let venue = match index.checked_sub(1) {
                None => VenueId(0),
                Some(previous) if self.sources[previous].feed.venue() == source.feed.venue() => {
                    venues[previous]
                }
                Some(previous) => VenueId(venues[previous].0 + 1),
            };
            venues.push(venue);
        }
        venues
    }

    /// Each source that sends orders, with its venue id and its execution block.
    pub(crate) fn armed_sources(
        &self,
    ) -> impl Iterator<Item = (VenueId, &SourceSpec, &ExecutionConfig)> {
        let venues = self.source_venues();
        self.sources
            .iter()
            .zip(venues)
            .filter_map(|(source, venue)| Some((venue, &source.feed, source.armed_execution()?)))
    }
}
