//! How the chart pane shares its height at any size, so no band is crushed: the latency grid or its
//! one-line summary on top, then the mid chart, the series strip and the risk chart.

use crate::workstation::shell::TOUCH_HEIGHT;

/// One row per flow. The table of row labels is declared with this length, so the compiler keeps
/// the grid's height and its rows in step.
pub(crate) const LATENCY_ROW_COUNT: usize = 3;
pub(crate) const LATENCY_HEADER_HEIGHT: f32 = 24.0;
pub(crate) const LATENCY_ROW_HEIGHT: f32 = 22.0;
const LATENCY_SUMMARY_HEIGHT: f32 = 24.0;
const GRID_HEIGHT: f32 = LATENCY_HEADER_HEIGHT + LATENCY_ROW_COUNT as f32 * LATENCY_ROW_HEIGHT;

/// A rectangle cut at a fractional position can come out a hair shorter than the height asked for,
/// and must still count as that height.
const HEIGHT_SLACK: f32 = 0.01;

/// The series selector fills the strip, so the strip is as tall as a touch target.
const STRIP_HEIGHT: f32 = TOUCH_HEIGHT;

/// The mid chart is the main thing to read and the risk chart sits beside it, so the mid chart
/// takes the larger share when both fit.
const MID_SHARE: f32 = 0.7;

/// The mid chart keeps this much height before the risk chart may grow to the height its own axis
/// needs.
const MID_AXIS_HEIGHT: f32 = 130.0;

/// The risk chart's height once its plot, after the frame's padding, is tall enough for axis
/// labels: an 11 point axis needs 79.2 points of plot, and this leaves 80.
// SMELL: this assumes the theme's 11 point axis font and 10 point chart padding, but nothing ties
// it to them. A larger font or padding would quietly cost the risk chart its labels on short panes.
const RISK_AXIS_HEIGHT: f32 = 100.0;

/// The smallest height the risk chart is painted at: after the frame's padding, its plot still fits
/// one crosshair pill.
const RISK_FLOOR: f32 = 40.0;

/// What the top of the pane shows. The grid is only as tall as its rows need, and folds to a single
/// line when that would cost the charts the height their axes need.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyBand {
    Grid,
    Summary,
}

impl LatencyBand {
    /// The full grid while it takes at most `max_share` of the pane, else the one-line summary.
    pub fn for_pane(pane_height: f32, max_share: f32) -> Self {
        if pane_height * max_share >= GRID_HEIGHT { Self::Grid } else { Self::Summary }
    }

    /// What a band `height` points tall paints: the grid when all its rows fit, else the summary.
    pub fn fitting(height: f32) -> Self {
        if height + HEIGHT_SLACK >= GRID_HEIGHT { Self::Grid } else { Self::Summary }
    }

    pub fn height(self) -> f32 {
        match self {
            Self::Grid => GRID_HEIGHT,
            Self::Summary => LATENCY_SUMMARY_HEIGHT,
        }
    }
}

/// How the chart stack's height divides. The three bands always add up to the stack's height.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StackBands {
    pub mid: f32,
    pub strip: f32,
    pub risk: f32,
}

impl StackBands {
    pub fn for_height(stack_height: f32) -> Self {
        let stack = stack_height.max(0.0);
        let strip = STRIP_HEIGHT.min(stack);
        let charts = stack - strip;
        let preferred_mid = (stack * MID_SHARE).round();
        let risk_floor = RISK_AXIS_HEIGHT
            .min(charts - MID_AXIS_HEIGHT)
            .max(RISK_FLOOR)
            .min(charts);
        let mid = preferred_mid.min(charts - risk_floor).max(0.0);
        Self {
            mid,
            strip,
            risk: charts - mid,
        }
    }
}
