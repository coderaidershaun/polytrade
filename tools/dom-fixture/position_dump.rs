//! The risk scenes as text, so they can be checked without a screen: a painted page proves nothing
//! on a locked machine.

use polysim::ids::FIXED_SCALE;
use polysim::workstation::chart::position_model::PositionBucket;

use crate::position_scenes::position_scenes;

/// One summary per scene of the series the risk chart would draw.
pub fn dump() {
    for scene in position_scenes() {
        let buckets: Vec<&PositionBucket> = scene.positions.buckets(scene.instrument).collect();
        let Some(first) = buckets.first() else {
            println!("{:<46} EMPTY series", scene.name);
            println!("{:<46}   check: {}", "", scene.check);
            continue;
        };
        let last = buckets.last().expect("a non-empty series to have a last");
        let exposure = extent(&buckets, |bucket| bucket.exposure_quote);
        let pnl = extent(&buckets, |bucket| bucket.pnl_quote);
        println!(
            // The modulo prints the scene's own spin numbers only because the base time is a whole
            // multiple of 100,000 spins. Other constants would print arbitrary numbers.
            "{:<46} {:>4} buckets  spins {}..={}  gaps {}  levels {:>3}  exposure {}  pnl {}",
            scene.name,
            buckets.len(),
            first.index % 100_000,
            last.index % 100_000,
            gap_widths(&buckets),
            distinct_levels(&buckets),
            span(exposure),
            span(pnl),
        );
        println!("{:<46}   check: {}", "", scene.check);
    }
}

/// Each gap's width in spins. A count alone cannot tell a one-bucket gap from a sixty-bucket one,
/// and those are different bugs.
fn gap_widths(buckets: &[&PositionBucket]) -> String {
    let widths: Vec<u64> = buckets
        .windows(2)
        .filter_map(|pair| (pair[1].index - pair[0].index).checked_sub(1))
        .filter(|missing| *missing > 0)
        .collect();
    match widths.is_empty() {
        true => "none".to_string(),
        false => format!("{widths:?}"),
    }
}

/// How many distinct exposure values the series holds. The extremes alone cannot see its shape: a
/// collapsed staircase has the same extremes as the right one.
fn distinct_levels(buckets: &[&PositionBucket]) -> usize {
    let mut levels: Vec<i64> = buckets.iter().map(|bucket| bucket.exposure_quote).collect();
    levels.sort_unstable();
    levels.dedup();
    levels.len()
}

fn extent(buckets: &[&PositionBucket], value: fn(&PositionBucket) -> i64) -> (i64, i64) {
    buckets
        .iter()
        .fold((i64::MAX, i64::MIN), |(low, high), bucket| {
            (low.min(value(bucket)), high.max(value(bucket)))
        })
}

/// The range in quote units, marked when it crosses zero.
fn span((low, high): (i64, i64)) -> String {
    let crosses = if low < 0 && high > 0 { " CROSSES 0" } else { "" };
    format!(
        "[{:>10.2} .. {:<10.2}]{}",
        low as f64 / FIXED_SCALE as f64,
        high as f64 / FIXED_SCALE as f64,
        crosses
    )
}
