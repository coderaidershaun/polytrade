//! Deterministic simulator latency arithmetic.

use crate::msg::time::{DurationUs, LocalTs};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LatencyBudget {
    pub order_entry: DurationUs,
    pub ack: DurationUs,
    pub max_market_data_delay: DurationUs,
}

impl LatencyBudget {
    pub fn arrival(&self, issued_ts_us: LocalTs) -> LocalTs {
        shifted(issued_ts_us, [self.order_entry])
    }

    pub fn market_effective(&self, received_ts_us: LocalTs) -> LocalTs {
        rewound(received_ts_us, self.max_market_data_delay)
    }
}

pub(crate) fn shifted<const N: usize>(base: LocalTs, spans: [DurationUs; N]) -> LocalTs {
    let total = spans
        .into_iter()
        .fold(i128::from(base.micros()), |sum, span| {
            sum + i128::from(span.micros())
        });
    stamp(total)
}

pub(crate) fn rewound(base: LocalTs, span: DurationUs) -> LocalTs {
    stamp(i128::from(base.micros()) - i128::from(span.micros()))
}

fn stamp(micros: i128) -> LocalTs {
    LocalTs::from_micros(
        i64::try_from(micros)
            .unwrap_or_else(|_| panic!("simulated latency stamp {micros}µs does not fit i64")),
    )
}
