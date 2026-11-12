//! The ladder scenes: fixed books, quotes and orders, the same on every run.

use polysim::ids::{ClientOrderId, FIXED_SCALE, Price, Qty};
use polysim::msg::inbound::Level;
use polysim::msg::ui::{DomQuote, UiBookSnapshot, UiBookState};
use polysim::time::DurationUs;
use polysim::time::TsUs;
use polysim::workstation::dom_view::{DomGrouping, FeedStatus};
use polysim::workstation::exec_model::{OrderCell, OrderStatus};
use polysim::workstation::format::price_decimals;

use crate::fixture::{book, levels, px, qm};

const TICK: Price = Price(FIXED_SCALE);
const QTY_SCALE: i64 = FIXED_SCALE;
const QUOTE_QTY_MILLI: i64 = 9_000;

/// The grouped scene's grid: a 0.01 tick on a 118,000 mid, so 1 bp is 1,180 ticks, or 11.80 a row.
const GROUPED_TICK: Price = Price(FIXED_SCALE / 100);
const GROUPED_MID_TICK: i64 = 11_800_000;
const GROUPED_QUOTE_QTY_MILLI: i64 = 250;

pub struct Variant {
    pub label: &'static str,
    pub snapshot: Option<UiBookSnapshot>,
    pub quote: Option<DomQuote>,
    pub bid_orders: Vec<OrderCell>,
    pub ask_orders: Vec<OrderCell>,
    pub tick: Price,
    pub grouping: DomGrouping,
    pub price_decimals: usize,
    pub qty_scale: i64,
    pub qty_decimals: usize,
    pub feed: FeedStatus,
    pub stale_age: Option<DurationUs>,
}

pub struct Scene {
    pub name: &'static str,
    pub variants: Vec<Variant>,
}

pub fn scenes() -> Vec<Scene> {
    vec![
        scene("dense valid book", vec![dense()]),
        scene("sparse — empty ticks", vec![sparse(), sparse_grouped()]),
        scene(
            "half-tick vs integer mid",
            vec![half_tick_mid(), integer_mid()],
        ),
        scene("quotes off-screen", vec![off_screen()]),
        scene(
            "real orders vs desire",
            vec![working_orders(), order_off_screen()],
        ),
        scene("stale", vec![stale()]),
        scene("awaiting book", vec![awaiting()]),
        scene("disconnected", vec![disconnected()]),
        scene("edge values", vec![one_sided(), edge_values()]),
        scene("locked book", vec![locked()]),
        scene(
            "grouped + bps",
            vec![
                grouped("ticks x1 (0.01/row)", DomGrouping::default()),
                grouped(
                    "ticks x10 (0.10/row)",
                    DomGrouping::Ticks { per_bucket: 10 },
                ),
                grouped(
                    "ticks x100 (1.00/row)",
                    DomGrouping::Ticks { per_bucket: 100 },
                ),
                grouped("bps 1 (11.80/row)", bps(1)),
                grouped("bps 0.25 (2.95/row)", bps(4)),
                grouped("bps 0.05 (0.59/row)", bps(20)),
                grouped("bps 0.01 (0.11/row)", bps(100)),
            ],
        ),
        scene(
            "narrow stress - BTC",
            vec![
                narrow_stress(),
                narrow_stale_off_screen(),
                narrow_five_decimals(),
            ],
        ),
    ]
}

const DENSE_ASKS: [(i64, i64); 12] = [
    (65992, 512),
    (65993, 1_240),
    (65994, 905),
    (65995, 2_100),
    (65996, 333),
    (65997, 1_010),
    (65998, 4_567),
    (65999, 88),
    (66000, 12_345),
    (66001, 750),
    (66002, 3_200),
    (66003, 410),
];

const DENSE_BIDS: [(i64, i64); 12] = [
    (65990, 640),
    (65989, 1_115),
    (65988, 900),
    (65987, 2_480),
    (65986, 170),
    (65985, 5_005),
    (65984, 260),
    (65983, 1_900),
    (65982, 55),
    (65981, 8_800),
    (65980, 330),
    (65979, 1_450),
];

const MID_ASKS: [(i64, i64); 4] = [(65992, 700), (65993, 1_400), (65995, 300), (65998, 2_600)];

fn dense() -> Variant {
    Variant {
        label: "bid delta 2 | ask delta 3 | qty 9",
        quote: Some(both_quotes(65989, 65994)),
        ..valid(&levels(&DENSE_BIDS), &levels(&DENSE_ASKS))
    }
}

fn sparse() -> Variant {
    let asks = levels(&[(65992, 500), (65994, 1_250), (65997, 800), (66001, 2_000)]);
    let bids = levels(&[(65990, 600), (65988, 1_100), (65985, 900), (65980, 3_300)]);
    Variant {
        label: "gaps keep grid + price",
        // The ask quote rests on an otherwise empty tick, so its row has a strategy cell and a
        // blank public cell.
        quote: Some(DomQuote::top(None, Some((px(65996), qm(QUOTE_QTY_MILLI))))),
        ..valid(&bids, &asks)
    }
}

/// The same gappy book at five ticks a row: the blank ticks between levels fold away, and the
/// quote's lone tick joins the depth around it. Both sides' nearest rows are the same bucket
/// straddling the mid, so 65990 labels a row above and below the separator; that is the case to
/// check by eye.
fn sparse_grouped() -> Variant {
    Variant {
        label: "ticks x5 - gaps merge, mid bucket shared",
        grouping: DomGrouping::Ticks { per_bucket: 5 },
        ..sparse()
    }
}

fn half_tick_mid() -> Variant {
    let asks = levels(&MID_ASKS);
    let bids = levels(&[(65991, 800), (65990, 1_200), (65988, 500), (65985, 3_100)]);
    Variant {
        label: "mid ...991.5",
        quote: Some(both_quotes(65989, 65994)),
        ..valid(&bids, &asks)
    }
}

fn integer_mid() -> Variant {
    let asks = levels(&MID_ASKS);
    let bids = levels(&[(65990, 800), (65989, 1_200), (65987, 500), (65984, 3_100)]);
    Variant {
        label: "mid 65991",
        quote: Some(both_quotes(65989, 65994)),
        ..valid(&bids, &asks)
    }
}

fn off_screen() -> Variant {
    let asks = levels(&DENSE_ASKS[..6]);
    let bids = levels(&DENSE_BIDS[..6]);
    Variant {
        label: "ask above / bid below the window",
        quote: Some(DomQuote::top(
            Some((px(65950), qm(QUOTE_QTY_MILLI))),
            Some((px(66030), qm(QUOTE_QTY_MILLI))),
        )),
        ..valid(&bids, &asks)
    }
}

fn stale() -> Variant {
    Variant {
        label: "amber, exact age",
        feed: FeedStatus::Stale,
        stale_age: Some(DurationUs::from_millis(1_234)),
        ..dense()
    }
}

fn awaiting() -> Variant {
    let asks = levels(&[(65992, 500), (65993, 400)]);
    let bids = levels(&[(65990, 600), (65989, 700)]);
    Variant {
        label: "book building — no mid",
        quote: None,
        ..variant(Some(book(
            1,
            0,
            UiBookState::AwaitingSnapshot,
            &bids,
            &asks,
        )))
    }
}

fn disconnected() -> Variant {
    Variant {
        label: "last book dimmed + warning",
        feed: FeedStatus::Disconnected,
        ..dense()
    }
}

fn one_sided() -> Variant {
    let bids = levels(&[(65990, 640), (65989, 1_115), (65988, 900)]);
    Variant {
        label: "bids only -> mid --, no rows",
        ..valid(&bids, &[])
    }
}

fn edge_values() -> Variant {
    let asks = levels(&[(65992, 0), (65994, 99_999_999), (65995, 1_000)]);
    let bids = levels(&[(65990, 12_345_678), (65989, 0), (65987, 1)]);
    Variant {
        label: "0 vs blank | max width",
        ..valid(&bids, &asks)
    }
}

fn locked() -> Variant {
    let asks = levels(&[(65991, 1_000), (65992, 512), (65993, 1_240)]);
    let bids = levels(&[(65991, 1_100), (65990, 640), (65989, 1_115)]);
    Variant {
        label: "bb == ba -> price in separator",
        ..valid(&bids, &asks)
    }
}

/// A BTC-sized book where only the grouping changes. Ungrouped, twelve rows cover 0.12 and the
/// quotes sit off screen. At x10 they come back, and at 1 bp each row carries real depth.
fn grouped(label: &'static str, grouping: DomGrouping) -> Variant {
    let asks = grouped_levels(&[
        (1, 85),
        (2, 140),
        (3, 62),
        (5, 310),
        (8, 95),
        (12, 480),
        (17, 120),
        (23, 755),
        (30, 210),
        (41, 1_640),
        (55, 305),
        (74, 890),
        (100, 2_400),
        (150, 640),
        (215, 1_180),
        (300, 3_050),
        (420, 720),
        (600, 2_260),
        (850, 1_430),
        (1_200, 4_800),
        (2_000, 2_150),
        (3_500, 6_400),
        (5_000, 3_900),
        (8_000, 11_500),
        (12_000, 7_250),
    ]);
    let bids = grouped_levels(&[
        (-1, 92),
        (-2, 118),
        (-4, 240),
        (-6, 74),
        (-9, 530),
        (-13, 165),
        (-18, 900),
        (-25, 205),
        (-33, 1_450),
        (-46, 380),
        (-62, 1_020),
        (-85, 275),
        (-110, 2_800),
        (-160, 515),
        (-230, 1_340),
        (-320, 2_700),
        (-450, 830),
        (-640, 1_960),
        (-900, 1_275),
        (-1_300, 5_200),
        (-2_100, 2_480),
        (-3_600, 5_900),
        (-5_200, 4_150),
        (-8_500, 10_200),
        (-12_500, 6_800),
    ]);
    Variant {
        label,
        tick: GROUPED_TICK,
        price_decimals: price_decimals(GROUPED_TICK),
        grouping,
        quote: Some(DomQuote::top(
            Some((grouped_px(-6), qm(GROUPED_QUOTE_QTY_MILLI))),
            Some((grouped_px(30), qm(GROUPED_QUOTE_QTY_MILLI))),
        )),
        ..valid(&bids, &asks)
    }
}

fn bps(denominator: i64) -> DomGrouping {
    DomGrouping::Bps {
        numerator: 1,
        denominator,
    }
}

fn scene(name: &'static str, variants: Vec<Variant>) -> Scene {
    Scene { name, variants }
}

/// A confirmed bid, an in-flight ask and a desired quote, so live, sent and want show side by side.
fn working_orders() -> Variant {
    let asks = levels(&[(65992, 500), (65993, 1_250), (65994, 800), (65996, 2_000)]);
    let bids = levels(&[(65990, 600), (65989, 1_100), (65988, 900), (65986, 3_300)]);
    Variant {
        label: "live bid | sent ask | want beyond",
        quote: Some(DomQuote::top(
            Some((px(65987), qm(QUOTE_QTY_MILLI))),
            Some((px(65995), qm(QUOTE_QTY_MILLI))),
        )),
        bid_orders: vec![order(1, px(65989), QUOTE_QTY_MILLI, OrderStatus::Confirmed)],
        ask_orders: vec![order(2, px(65993), QUOTE_QTY_MILLI, OrderStatus::InFlight)],
        ..valid(&bids, &asks)
    }
}

/// A lost order off screen. The chevron points at the order rather than the desired quote, because
/// the order is real exposure and the quote only an intention.
fn order_off_screen() -> Variant {
    Variant {
        label: "lost order off-screen outranks want",
        bid_orders: vec![order(3, px(65950), QUOTE_QTY_MILLI, OrderStatus::Lost)],
        ..working_orders()
    }
}

fn order(id: u64, price: Price, milli: i64, status: OrderStatus) -> OrderCell {
    OrderCell {
        client_id: ClientOrderId(id),
        status,
        price,
        qty: qm(milli),
        filled: Qty(0),
        at: TsUs::from_micros(0),
        quote_level: None,
    }
}

fn both_quotes(bid_tick: i64, ask_tick: i64) -> DomQuote {
    DomQuote::top(
        Some((px(bid_tick), qm(QUOTE_QTY_MILLI))),
        Some((px(ask_tick), qm(QUOTE_QTY_MILLI))),
    )
}

fn valid(bids: &[Level], asks: &[Level]) -> Variant {
    variant(Some(book(1, 0, UiBookState::Valid, bids, asks)))
}

fn variant(snapshot: Option<UiBookSnapshot>) -> Variant {
    Variant {
        label: "",
        snapshot,
        quote: None,
        bid_orders: Vec::new(),
        ask_orders: Vec::new(),
        tick: TICK,
        grouping: DomGrouping::default(),
        price_decimals: price_decimals(TICK),
        qty_scale: QTY_SCALE,
        qty_decimals: 3,
        feed: FeedStatus::default(),
        stale_age: None,
    }
}

/// Levels on the grouped scene's 0.01 grid, written as signed tick offsets from its mid so a row
/// reads as its distance from the touch rather than as an eight-digit tick index.
fn grouped_levels(offsets: &[(i64, i64)]) -> Vec<Level> {
    levels_from(GROUPED_MID_TICK, offsets)
}

fn grouped_px(offset_ticks: i64) -> Price {
    grid_px(GROUPED_MID_TICK + offset_ticks)
}

fn levels_from(base_tick: i64, offsets: &[(i64, i64)]) -> Vec<Level> {
    offsets
        .iter()
        .map(|&(offset, milli)| Level {
            price: grid_px(base_tick + offset),
            qty: qm(milli),
        })
        .collect()
}

fn grid_px(tick: i64) -> Price {
    Price(tick * GROUPED_TICK.0)
}

/// The best bid and ask of a BTC-sized book on the 0.01 grid: 117234.56 and 117234.57.
const NARROW_BID_TICK: i64 = 11_723_456;
const NARROW_ASK_TICK: i64 = 11_723_457;

/// Six-digit quantities at a BTC-sized price, the widest strings a 300-wide ladder has to hold.
/// Orders with six, five and one whole digits show which status tags still fit beside them.
fn narrow_stress() -> Variant {
    let asks = levels_from(
        NARROW_ASK_TICK,
        &[
            (0, 123_456_789),
            (1, 85_000),
            (2, 987_654_321),
            (3, 1_250),
            (4, 250_000_000),
            (5, 6_400),
            (7, 42_100),
            (11, 98_765_432),
        ],
    );
    let bids = levels_from(
        NARROW_BID_TICK,
        &[
            (0, 654_321_098),
            (-1, 7_800),
            (-2, 123_456_789),
            (-3, 450),
            (-5, 250_000_000),
            (-6, 3_300),
            (-8, 15_000),
            (-10, 987_654_321),
        ],
    );
    let bid_px = |offset: i64| grid_px(NARROW_BID_TICK + offset);
    let ask_px = |offset: i64| grid_px(NARROW_ASK_TICK + offset);
    Variant {
        label: "six-digit qty, every tag",
        tick: GROUPED_TICK,
        price_decimals: price_decimals(GROUPED_TICK),
        quote: Some(DomQuote::top(
            Some((bid_px(-4), qm(250_000))),
            Some((ask_px(8), qm(1_500))),
        )),
        bid_orders: vec![
            order(1, bid_px(-2), 123_456_789, OrderStatus::Confirmed),
            order(3, bid_px(-6), 98_765_432, OrderStatus::Lost),
        ],
        ask_orders: vec![
            order(2, ask_px(1), 9_000, OrderStatus::InFlight),
            order(4, ask_px(4), 250, OrderStatus::Confirmed),
        ],
        ..valid(&bids, &asks)
    }
}

/// A stale feed with the desired ask far above the window and the desired bid far below it, so the
/// badge and both chevrons paint at once.
fn narrow_stale_off_screen() -> Variant {
    Variant {
        label: "stale, quotes off both edges",
        feed: FeedStatus::Stale,
        stale_age: Some(DurationUs::from_millis(1_234)),
        quote: Some(DomQuote::top(
            Some((grid_px(NARROW_BID_TICK - 40), qm(250_000))),
            Some((grid_px(NARROW_ASK_TICK + 40), qm(1_500))),
        )),
        ..narrow_stress()
    }
}

/// The same book at five quantity decimals, where "123456.78900" is twelve characters long.
fn narrow_five_decimals() -> Variant {
    Variant {
        label: "five qty decimals",
        qty_decimals: 5,
        ..narrow_stress()
    }
}
