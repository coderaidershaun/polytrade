//! Deterministic simulator latency arithmetic.

use crate::time::{DurationUs, TsUs};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LatencyBudget {
    pub order_entry: DurationUs,
    pub ack: DurationUs,
    pub max_market_data_delay: DurationUs,
}

impl LatencyBudget {
    pub fn arrival(&self, issued_ts_us: TsUs) -> TsUs {
        shifted(issued_ts_us, [self.order_entry])
    }

    pub fn market_effective(&self, received_ts_us: TsUs) -> TsUs {
        rewound(received_ts_us, self.max_market_data_delay)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StartupBudget {
    pub latency: LatencyBudget,
    pub max_heartbeat_interval: DurationUs,
    pub inflight_timeout: DurationUs,
}

impl StartupBudget {
    pub fn check(&self) -> Result<DurationUs, LatencyBudgetError> {
        let spans = [
            ("order_entry_latency", self.latency.order_entry),
            ("ack_latency", self.latency.ack),
            ("max_market_data_delay", self.latency.max_market_data_delay),
            ("producer_heartbeat", self.max_heartbeat_interval),
            ("producer_heartbeat", self.max_heartbeat_interval),
        ];
        for (name, value) in spans
            .iter()
            .copied()
            .chain([("inflight_timeout", self.inflight_timeout)])
        {
            if value < DurationUs::ZERO {
                return Err(LatencyBudgetError::Negative {
                    name,
                    micros: value.micros(),
                });
            }
        }
        let worst = spans
            .iter()
            .map(|(_, value)| i128::from(value.micros()))
            .sum::<i128>();
        let worst_us = i64::try_from(worst).map_err(|_| LatencyBudgetError::Overflow)?;
        if worst_us >= self.inflight_timeout.micros() {
            return Err(LatencyBudgetError::TimeoutTooTight {
                worst_us,
                timeout_us: self.inflight_timeout.micros(),
            });
        }
        Ok(DurationUs::from_micros(worst_us))
    }
}

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyBudgetError {
    #[error("{name} is {micros}µs — a latency span cannot run backwards")]
    Negative { name: &'static str, micros: i64 },
    #[error("the simulated latency budget does not fit a µs span")]
    Overflow,
    #[error(
        "the worst legal command round trip is {worst_us}µs against an in-flight timeout of \
         {timeout_us}µs — equality would race the timeout"
    )]
    TimeoutTooTight { worst_us: i64, timeout_us: i64 },
}

pub(crate) fn shifted<const N: usize>(base: TsUs, spans: [DurationUs; N]) -> TsUs {
    let total = spans
        .into_iter()
        .fold(i128::from(base.micros()), |sum, span| {
            sum + i128::from(span.micros())
        });
    stamp(total)
}

pub(crate) fn rewound(base: TsUs, span: DurationUs) -> TsUs {
    stamp(i128::from(base.micros()) - i128::from(span.micros()))
}

fn stamp(micros: i128) -> TsUs {
    TsUs::from_micros(
        i64::try_from(micros)
            .unwrap_or_else(|_| panic!("simulated latency stamp {micros}µs does not fit i64")),
    )
}
