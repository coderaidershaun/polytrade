//! The kinds that a config names and that messages and records also carry: a candle interval, a
//! recorded table, the mode of execution, and the kind of a feature.

use serde::Deserialize;

use crate::msg::labelled_enum::labelled_enum;
use crate::msg::time::DurationUs;

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
    pub enum KlineInterval {
        #[serde(rename = "1m")]
        OneMinute = "1m",
        #[serde(rename = "3m")]
        ThreeMinutes = "3m",
        #[serde(rename = "5m")]
        FiveMinutes = "5m",
        #[serde(rename = "15m")]
        FifteenMinutes = "15m",
        #[serde(rename = "30m")]
        ThirtyMinutes = "30m",
        #[serde(rename = "1h")]
        OneHour = "1h",
        #[serde(rename = "2h")]
        TwoHours = "2h",
        #[serde(rename = "4h")]
        FourHours = "4h",
        #[serde(rename = "6h")]
        SixHours = "6h",
        #[serde(rename = "8h")]
        EightHours = "8h",
        #[serde(rename = "12h")]
        TwelveHours = "12h",
        #[serde(rename = "1d")]
        OneDay = "1d",
        #[serde(rename = "3d")]
        ThreeDays = "3d",
        #[serde(rename = "1w")]
        OneWeek = "1w",
        #[serde(rename = "1M")]
        OneMonth = "1M",
    }
    /// The venue interval string (`@kline_<interval>` and REST `interval=`).
    pub fn as_str;
}

impl KlineInterval {
    /// Candle span in whole minutes, or `None` for the calendar-variable `1M` — a month has no
    /// fixed minute count, so a caller needing a span substitutes its own bound.
    pub fn fixed_minutes(self) -> Option<u64> {
        let minutes = match self {
            KlineInterval::OneMinute => 1,
            KlineInterval::ThreeMinutes => 3,
            KlineInterval::FiveMinutes => 5,
            KlineInterval::FifteenMinutes => 15,
            KlineInterval::ThirtyMinutes => 30,
            KlineInterval::OneHour => 60,
            KlineInterval::TwoHours => 120,
            KlineInterval::FourHours => 240,
            KlineInterval::SixHours => 360,
            KlineInterval::EightHours => 480,
            KlineInterval::TwelveHours => 720,
            KlineInterval::OneDay => 1_440,
            KlineInterval::ThreeDays => 4_320,
            KlineInterval::OneWeek => 10_080,
            KlineInterval::OneMonth => return None,
        };
        Some(minutes)
    }

    /// The candle's span, `None` for the calendar-variable `1M` — see [`fixed_minutes`]. The one
    /// place a span is derived from an interval; a second derivation is a divergence waiting to
    /// happen.
    ///
    /// [`fixed_minutes`]: KlineInterval::fixed_minutes
    pub fn fixed_duration(self) -> Option<DurationUs> {
        self.fixed_minutes()
            .map(|minutes| DurationUs::from_secs(minutes as i64 * 60))
    }
}

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum TableKind {
        Trades = "trades",
        BookEvents = "book_events",
        Klines = "klines",
        Features = "features",
        LinkFrames = "link_frames",
        Orders = "orders",
        Fills = "fills",
    }
    pub fn as_str;
}

labelled_enum! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
    #[cfg_attr(
        feature = "mcp",
        derive(rmcp::schemars::JsonSchema),
        schemars(crate = "rmcp::schemars")
    )]
    #[serde(rename_all = "snake_case")]
    pub enum ExecutionMode {
        Off = "off",
        Sim = "sim",
        Live = "live",
    }
    pub fn as_str;
}

impl ExecutionMode {
    #[inline]
    pub fn needs_credentials(self) -> bool {
        self == ExecutionMode::Live
    }

    #[inline]
    pub fn is_enabled(self) -> bool {
        matches!(self, ExecutionMode::Sim | ExecutionMode::Live)
    }

    #[inline]
    pub fn is_simulated(self) -> bool {
        self == ExecutionMode::Sim
    }

    /// Keeps an absent execution block distinct from `off` in recorded metadata.
    pub fn footer_value(mode: Option<Self>) -> &'static str {
        mode.map_or("absent", Self::as_str)
    }

    pub fn badge(mode: Option<Self>) -> &'static str {
        match mode {
            None | Some(ExecutionMode::Off) => "OFF",
            Some(ExecutionMode::Sim) => "SIM",
            Some(ExecutionMode::Live) => "LIVE",
        }
    }
}

labelled_enum! {
    /// What a feature depends on. A market-only feature is a function of market data alone, so a
    /// replay of the tape gives it again under any strategy; an execution-aware feature also reads
    /// the run's own orders, fills or position.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum FeatureKind {
        MarketOnly = "market_only",
        ExecutionAware = "execution_aware",
    }
    pub fn as_str;
    pub const ALL;
}
