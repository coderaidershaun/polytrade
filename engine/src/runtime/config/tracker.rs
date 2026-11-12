//! Per-instrument tracker schema; series optional (present→built, absent→not).

use serde::Deserialize;

/// A klines volume target averages this many trailing closed one-minute candles: one day.
pub(crate) const TARGET_WINDOW_CANDLES: usize = 1_440;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrackerSpec {
    pub trades_all: Option<WindowsSpec>,
    pub trades_buy: Option<WindowsSpec>,
    pub trades_sell: Option<WindowsSpec>,
    pub microprice: Option<WindowsSpec>,
    pub imbalance: Option<ImbalanceSpec>,
    pub candles: Option<CandlesSpec>,
    pub spin_sampled: Option<SpinSampledSpec>,
    pub volume_bars: Option<VolumeBarsSpec>,
    pub ewma_vol: Option<EwmaVolSpec>,
    pub intensity: Option<IntensitySpec>,
}

impl TrackerSpec {
    pub(crate) fn uses_kline_threshold(&self) -> bool {
        self.volume_bars
            .as_ref()
            .is_some_and(|bars| bars.threshold == VolumeThreshold::Klines)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowsSpec {
    pub windows: Vec<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImbalanceSpec {
    pub top_n: usize,
    pub windows: Vec<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandlesSpec {
    pub keep: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpinSampledSpec {
    pub fields: Vec<SpinField>,
    pub window: usize,
}

/// Dollar-bar clock: bar closes when accumulated notional reaches threshold.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeBarsSpec {
    pub threshold: VolumeThreshold,
    pub keep: usize,
    pub sampled: Option<SpinSampledSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VolumeThreshold {
    Fixed(u64),
    Klines,
}

impl<'de> Deserialize<'de> for VolumeThreshold {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct VolumeThresholdVisitor;

        impl serde::de::Visitor<'_> for VolumeThresholdVisitor {
            type Value = VolumeThreshold;

            fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter.write_str("\"klines\" or a whole-dollar notional target")
            }

            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<VolumeThreshold, E> {
                Ok(VolumeThreshold::Fixed(value))
            }

            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<VolumeThreshold, E> {
                u64::try_from(value)
                    .map(VolumeThreshold::Fixed)
                    .map_err(|_| {
                        E::custom(format!(
                            "expected \"klines\" or a whole-dollar integer, got {value}"
                        ))
                    })
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<VolumeThreshold, E> {
                if value == "klines" {
                    Ok(VolumeThreshold::Klines)
                } else {
                    Err(E::custom(format!(
                        "expected \"klines\" or a whole-dollar integer, got {value:?}"
                    )))
                }
            }
        }

        deserializer.deserialize_any(VolumeThresholdVisitor)
    }
}

/// Sampleable on spin or volume clocks. `last_trade_*` survive book resets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpinField {
    Microprice,
    Spread,
    BestBid,
    BestAsk,
    Mid,
    Imbalance,
    LastTradePrice,
    LastTradeQty,
    TradedQty,
    TradedNotional,
    TradeCount,
    BuyTradeCount,
    SellTradeCount,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EwmaVolSpec {
    pub halflife_events: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IntensitySpec {
    pub max_depth_ticks: usize,
    pub half_life_secs: f64,
    pub min_events: f64,
}

impl Default for IntensitySpec {
    fn default() -> Self {
        Self {
            max_depth_ticks: 32,
            half_life_secs: 600.0,
            min_events: 5.0,
        }
    }
}
