//! At every pane height from 200 to 1600 pt the bands add up to the pane and each chart keeps room
//! for its axis. A crushed chart or a crowded axis paints over live data with no error to catch it.

use polysim::workstation::chart::pane::{LatencyBand, StackBands};
use polysim::workstation::chart::view::{ChartBounds, value_ticks};

/// Mirrors `METRICS.latency_fraction`, the most of the pane the latency grid may take.
const SHIPPED_LATENCY_SHARE: f32 = 0.25;

/// Mirrors `METRICS.chart_pad`: the frame pads a chart this much above and below its plot.
const CHART_PAD: f32 = 10.0;

/// Mirrors `METRICS.chart_axis_font`.
const AXIS_FONT: f32 = 11.0;

/// The smallest pane that shows the whole grid: 90 pt at a quarter of the pane.
const GRID_PANE_HEIGHT: f32 = 360.0;

/// The first pane whose risk plot, 79.5 pt, is tall enough to label. The plot is 79.0 pt one step
/// below it.
const FIRST_RISK_AXIS_PANE: f32 = 289.5;

/// The plot height from which an axis draws labels, in the painter's own f32 arithmetic.
const LEGIBLE_AXIS_HEIGHT: f32 = AXIS_FONT * 1.2 * 6.0;

const MID_WINDOW: ChartBounds = ChartBounds {
    low: 174,
    high: 306,
};

fn plot_height(band: f32) -> f32 {
    band - 2.0 * CHART_PAD
}

fn pane_bands(pane: f32) -> (LatencyBand, StackBands) {
    let latency = LatencyBand::for_pane(pane, SHIPPED_LATENCY_SHARE);
    (latency, StackBands::for_height(pane - latency.height()))
}

/// FITNESS: across the whole envelope the grid is shown exactly from 360 pt, the latency band and
/// the stack's three bands sum to the pane, the strip keeps its touch height, the risk chart keeps
/// a pill's room, and the mid chart always has an axis. The risk chart gains its axis at one
/// pinned pane and keeps it at every larger one. The painters read these same functions, so
/// `split` and `paint` cannot disagree about the band.
#[test]
fn the_pane_budget_holds_across_the_whole_envelope() {
    for half_points in 400..=3_200u32 {
        let pane = half_points as f32 / 2.0;
        let (latency, bands) = pane_bands(pane);

        assert_eq!(
            latency == LatencyBand::Grid,
            pane >= GRID_PANE_HEIGHT,
            "a {pane}pt pane picked {latency:?}"
        );
        assert!(
            latency.height() == 90.0 || latency.height() == 24.0,
            "a {pane}pt pane gave the latency band {}pt",
            latency.height()
        );
        assert_eq!(
            LatencyBand::fitting(latency.height()),
            latency,
            "a {pane}pt pane's band must read as itself when painted into its own rect"
        );

        let stack = pane - latency.height();
        assert!(bands.mid >= 0.0 && bands.strip >= 0.0 && bands.risk >= 0.0);
        assert_eq!(
            bands.mid + bands.strip + bands.risk,
            stack,
            "a {pane}pt pane's stack bands must add up to the stack"
        );
        assert_eq!(bands.strip, 36.0, "{pane}pt: the strip is a touch target");
        assert!(
            bands.risk >= 40.0,
            "{pane}pt: the risk chart got {}",
            bands.risk
        );

        let mid_labels = value_ticks(MID_WINDOW, plot_height(bands.mid), AXIS_FONT).count();
        assert!(
            mid_labels >= 2,
            "{pane}pt: the mid chart drew {mid_labels} label(s)"
        );

        let risk_plot = plot_height(bands.risk);
        let risk_labels = value_ticks(MID_WINDOW, risk_plot, AXIS_FONT).count();
        assert_eq!(
            risk_labels > 0,
            pane >= FIRST_RISK_AXIS_PANE,
            "{pane}pt: the risk plot is {risk_plot}pt and drew {risk_labels} label(s)"
        );
        assert_eq!(
            risk_labels > 0,
            risk_plot >= LEGIBLE_AXIS_HEIGHT,
            "{pane}pt: the risk axis must follow the legible height"
        );
    }
}

/// FITNESS: the band choice follows the share it is handed, and the painter's rect, cut from a
/// fractional offset, still reads as the band it was cut to.
#[test]
fn the_latency_band_follows_its_share_and_survives_a_rounded_rect() {
    assert_eq!(LatencyBand::for_pane(180.0, 0.5), LatencyBand::Grid);
    assert_eq!(LatencyBand::for_pane(179.5, 0.5), LatencyBand::Summary);
    assert_eq!(LatencyBand::for_pane(359.5, 0.25), LatencyBand::Summary);
    assert_eq!(LatencyBand::for_pane(360.0, 0.25), LatencyBand::Grid);

    assert_eq!(LatencyBand::fitting(90.0), LatencyBand::Grid);
    assert_eq!(LatencyBand::fitting(300.0), LatencyBand::Grid);
    assert_eq!(LatencyBand::fitting(89.995), LatencyBand::Grid);
    assert_eq!(LatencyBand::fitting(89.0), LatencyBand::Summary);
    assert_eq!(LatencyBand::fitting(24.0), LatencyBand::Summary);
    assert_eq!(LatencyBand::fitting(0.0), LatencyBand::Summary);
}

struct PaneRow {
    pane: f32,
    latency: LatencyBand,
    stack: f32,
    mid: f32,
    strip: f32,
    risk: f32,
}

/// FITNESS: the literal budget at common pane heights and at the edges where a rule flips, so a
/// changed constant shows up as a changed number rather than as a formula that agrees with itself.
#[test]
fn the_pane_budget_at_the_screenshot_sizes() {
    let rows = [
        PaneRow {
            pane: 200.0,
            latency: LatencyBand::Summary,
            stack: 176.0,
            mid: 100.0,
            strip: 36.0,
            risk: 40.0,
        },
        PaneRow {
            pane: 289.0,
            latency: LatencyBand::Summary,
            stack: 265.0,
            mid: 130.0,
            strip: 36.0,
            risk: 99.0,
        },
        PaneRow {
            pane: 290.0,
            latency: LatencyBand::Summary,
            stack: 266.0,
            mid: 130.0,
            strip: 36.0,
            risk: 100.0,
        },
        PaneRow {
            pane: 359.0,
            latency: LatencyBand::Summary,
            stack: 335.0,
            mid: 199.0,
            strip: 36.0,
            risk: 100.0,
        },
        PaneRow {
            pane: 360.0,
            latency: LatencyBand::Grid,
            stack: 270.0,
            mid: 134.0,
            strip: 36.0,
            risk: 100.0,
        },
        PaneRow {
            pane: 600.0,
            latency: LatencyBand::Grid,
            stack: 510.0,
            mid: 357.0,
            strip: 36.0,
            risk: 117.0,
        },
        PaneRow {
            pane: 900.0,
            latency: LatencyBand::Grid,
            stack: 810.0,
            mid: 567.0,
            strip: 36.0,
            risk: 207.0,
        },
        PaneRow {
            pane: 1200.0,
            latency: LatencyBand::Grid,
            stack: 1110.0,
            mid: 777.0,
            strip: 36.0,
            risk: 297.0,
        },
        PaneRow {
            pane: 1600.0,
            latency: LatencyBand::Grid,
            stack: 1510.0,
            mid: 1057.0,
            strip: 36.0,
            risk: 417.0,
        },
    ];
    for row in rows {
        let (latency, bands) = pane_bands(row.pane);
        let pane = row.pane;
        assert_eq!(latency, row.latency, "{pane}pt: latency band");
        assert_eq!(pane - latency.height(), row.stack, "{pane}pt: stack height");
        assert_eq!(bands.mid, row.mid, "{pane}pt: mid chart");
        assert_eq!(bands.strip, row.strip, "{pane}pt: strip");
        assert_eq!(bands.risk, row.risk, "{pane}pt: risk chart");
    }
}
