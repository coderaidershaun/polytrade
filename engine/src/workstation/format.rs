//! Bounded number formatting into a caller-owned `String`, so the ladder painter can reuse one
//! buffer for every cell it draws instead of allocating per cell. The axis tick generator lives
//! here because it chooses the numbers these writers then render.
//!
//! Prices read as venue prices at the instrument's tick precision, so a 0.01-tick book shows
//! `118000.005` rather than a tick count. Distances follow the ladder's unit toggle: tick counts
//! ([`write_mid`], [`write_tick_price`]) or basis points ([`write_bps_delta`]). A half-tick mid
//! never rounds: [`write_venue_mid`] widens its decimals until the value is exact.

use crate::ids::{FIXED_SCALE, Price, Qty};
use crate::time::TsUs;

/// The whole workstation's answer to "no reading". It is plain ASCII because the bundled fonts lack
/// many symbols, and only ASCII is sure to draw.
pub const MISSING: &str = "--";

/// What a writer left in the buffer. A caller colouring a cell reads this instead of comparing the
/// rendered characters against [`MISSING`]: a bound marker is a real reading the cell cannot show,
/// which is not the same as having no reading at all, and the two must not paint alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wrote {
    Value,
    Absent,
    Bound,
}

/// Put [`MISSING`] in the buffer. Every cell with nothing to show goes through here, so none of
/// them can invent its own way of saying so.
pub(crate) fn write_missing(buf: &mut String) -> Wrote {
    buf.clear();
    buf.push_str(MISSING);
    Wrote::Absent
}

pub(crate) fn render(write: impl FnOnce(&mut String)) -> String {
    let mut text = String::new();
    write(&mut text);
    text
}

const DEFAULT_QTY_DECIMALS: usize = 3;

const FEATURE_DECIMALS: usize = 4;

const MAX_MID_DECIMALS: usize = 9;

const NICE_STEP_MULTIPLES: [i64; 3] = [1, 2, 5];

const MAX_STEP_DECADE: u32 = 18;

const MIN_TICK_CEILING: usize = 5;

pub(crate) const BASIS_POINTS_PER_UNIT: i128 = 10_000;

/// Scaled value carrying two significant figures. A delta is a distance to read at a glance, not a
/// measurement to reconcile, so it rounds: `0.00085` rather than `0.0008475`. Two is the floor that
/// still separates one tick from two on any book.
const BPS_TWO_FIGURES: i128 = 10;

/// Nine characters is that cell's budget, so `0.` leaves seven places and the integer form nine
/// digits. Past either edge the bound is stated: a run of zeros and a clamped number would both
/// read as a value.
const BPS_MAX_DECIMALS: usize = 7;

const BPS_MAX_INTEGER: i128 = 1_000_000_000;

const MICROS_GROUP_DIGITS: usize = 3;

const MICROS_GROUP_SEPARATOR: char = ' ';

const SLOT_DECIMALS: usize = 1;

const BPS_UNDERFLOW: &str = "<1e-7";

const BPS_OVERFLOW: &str = ">1e9";

/// Nine characters is the latency cell's budget too, because the narrowest latency column fits
/// nine glyphs of the value font. Grouping (or the slot count's decimal place) spends two of them,
/// leaving seven integer digits, and a leading sign spends one more.
const LATENCY_MAX_INTEGER: u64 = 10_000_000;
const LATENCY_MAX_NEGATIVE_INTEGER: u64 = 1_000_000;
const LATENCY_OVERFLOW: &str = ">1e7";
const LATENCY_UNDERFLOW: &str = "<-1e6";

const NO_TICKS: AxisTicks = AxisTicks {
    cursor: 0,
    remaining: 0,
    step: 1,
};

pub(crate) fn write_tick_price(buf: &mut String, tick_index: i64) {
    buf.clear();
    push_int(buf, tick_index);
}

pub fn write_mid(buf: &mut String, mid_half_ticks: i64) {
    buf.clear();
    push_int(buf, mid_half_ticks.div_euclid(2));
    if mid_half_ticks.rem_euclid(2) != 0 {
        buf.push_str(".5");
    }
}

pub fn price_decimals(tick: Price) -> usize {
    decimals_for_increment(tick.0)
}

pub fn quote_axis_decimals(step_mantissa: i64) -> usize {
    decimals_for_increment(step_mantissa)
}

pub fn write_venue_price(buf: &mut String, price: Price, decimals: usize) {
    write_fixed_point(buf, price.0.into(), FIXED_SCALE as u128, decimals);
}

pub fn write_quote_amount(buf: &mut String, mantissa: i64, decimals: usize) {
    write_fixed_point(buf, mantissa.into(), FIXED_SCALE as u128, decimals);
}

pub fn write_venue_mid(buf: &mut String, mid_half_ticks: i64, tick: Price) {
    let twice_mantissa = i128::from(mid_half_ticks) * i128::from(tick.0);
    let decimals = exact_mid_decimals(twice_mantissa, tick);
    write_fixed_point(buf, twice_mantissa, 2 * FIXED_SCALE as u128, decimals);
}

pub fn write_opt_venue_mid(buf: &mut String, mid_half_ticks: Option<i64>, tick: Price) -> Wrote {
    write_opt(buf, mid_half_ticks, |buf, mid| {
        write_venue_mid(buf, mid, tick)
    })
}

/// Writes a present value through `write`, or [`MISSING`] for an absent one.
pub(crate) fn write_opt<T>(
    buf: &mut String,
    value: Option<T>,
    write: impl FnOnce(&mut String, T),
) -> Wrote {
    let Some(value) = value else {
        return write_missing(buf);
    };
    write(buf, value);
    Wrote::Value
}

/// The fewest places that render `twice_mantissa / (2 * FIXED_SCALE)` exactly, never below the
/// tick's own precision. Reducing modulo the divisor first keeps the search overflow-free however
/// large the mantissa.
fn exact_mid_decimals(twice_mantissa: i128, tick: Price) -> usize {
    let divisor = 2 * FIXED_SCALE as u128;
    let remainder = twice_mantissa.unsigned_abs() % divisor;
    let mut decimals = price_decimals(tick);
    while decimals < MAX_MID_DECIMALS
        && !(remainder * u128::from(pow10(decimals))).is_multiple_of(divisor)
    {
        decimals += 1;
    }
    decimals
}

pub fn write_qty(buf: &mut String, qty: Qty, scale: i64, decimals: usize) {
    debug_assert!(scale > 0, "fixed-point scale must be positive, got {scale}");
    write_fixed_point(buf, qty.0.into(), scale.unsigned_abs().into(), decimals);
}

/// Writes `value / scale` cut to `decimals` places, never rounded. An integer part too wide for `u64`
/// saturates rather than wrapping.
fn write_fixed_point(buf: &mut String, value: i128, scale: u128, decimals: usize) {
    buf.clear();
    if value < 0 {
        buf.push('-');
    }
    let magnitude = value.unsigned_abs();
    push_uint(buf, u64::try_from(magnitude / scale).unwrap_or(u64::MAX));
    if decimals == 0 {
        return;
    }
    buf.push('.');
    let fraction = (magnitude % scale) * u128::from(pow10(decimals)) / scale;
    push_padded(buf, fraction as u64, decimals);
}

pub fn write_opt_qty(buf: &mut String, qty: Option<Qty>, scale: i64, decimals: usize) -> Wrote {
    write_opt(buf, qty, |buf, qty| write_qty(buf, qty, scale, decimals))
}

pub fn qty_decimals(lot: Option<Qty>) -> usize {
    match lot {
        Some(Qty(mantissa)) if mantissa > 0 => decimals_for_increment(mantissa),
        _ => DEFAULT_QTY_DECIMALS,
    }
}

pub(crate) fn write_time_of_day(buf: &mut String, at: TsUs) {
    let at = at.civil();
    buf.clear();
    push_padded(buf, at.hour as u64, 2);
    buf.push(':');
    push_padded(buf, at.minute as u64, 2);
    buf.push(':');
    push_padded(buf, at.second as u64, 2);
    buf.push('.');
    push_padded(buf, (at.micros / 1_000) as u64, 3);
}

/// Write `delta_half_ticks / mid_half_ticks` in basis points — both share the half-tick unit, so
/// the halves cancel and no tick size is needed. Precision adapts because the magnitudes are tiny:
/// one 0.01 tick on a 118000 book is 0.00085 bp, which a fixed two-place rendering prints as
/// `0.00`. A distance is unsigned here, so a negative one renders [`MISSING`] rather than inventing
/// a sign convention the workstation does not have.
pub fn write_bps_delta(buf: &mut String, delta_half_ticks: i64, mid_half_ticks: i64) -> Wrote {
    buf.clear();
    if mid_half_ticks <= 0 || delta_half_ticks < 0 {
        return write_missing(buf);
    }
    // A quote resting at the mid reads the same bare `0` in either unit.
    if delta_half_ticks == 0 {
        buf.push('0');
        return Wrote::Value;
    }

    let denominator = i128::from(mid_half_ticks);
    let (scaled, decimals) = bps_figures(i128::from(delta_half_ticks), denominator);
    if scaled == 0 {
        buf.push_str(BPS_UNDERFLOW);
        return Wrote::Bound;
    }
    let integer = scaled / i128::from(pow10(decimals));
    if integer >= BPS_MAX_INTEGER {
        buf.push_str(BPS_OVERFLOW);
        return Wrote::Bound;
    }
    push_scaled(buf, scaled as u64, decimals);
    Wrote::Value
}

pub fn write_opt_bps_delta(
    buf: &mut String,
    delta_half_ticks: Option<i64>,
    mid_half_ticks: Option<i64>,
) -> Wrote {
    match delta_half_ticks.zip(mid_half_ticks) {
        Some((delta, mid)) => write_bps_delta(buf, delta, mid),
        None => write_missing(buf),
    }
}

/// The fewest places carrying two significant figures, and the value scaled to them. Rounding at
/// each candidate rather than once keeps the chosen place from disagreeing with what it renders.
fn bps_figures(delta_half_ticks: i128, mid_half_ticks: i128) -> (i128, usize) {
    let numerator = delta_half_ticks * BASIS_POINTS_PER_UNIT;
    for decimals in 0..BPS_MAX_DECIMALS {
        let scaled = rounded_ratio(numerator * i128::from(pow10(decimals)), mid_half_ticks);
        if scaled >= BPS_TWO_FIGURES {
            return (scaled, decimals);
        }
    }
    let scaled = rounded_ratio(
        numerator * i128::from(pow10(BPS_MAX_DECIMALS)),
        mid_half_ticks,
    );
    (scaled, BPS_MAX_DECIMALS)
}

fn rounded_ratio(numerator: i128, denominator: i128) -> i128 {
    (numerator + denominator / 2) / denominator
}

pub(crate) fn write_feature_value(buf: &mut String, value: f64) {
    buf.clear();
    if value.is_nan() {
        buf.push_str("NaN");
        return;
    }
    if value.is_infinite() {
        buf.push_str(if value < 0.0 { "-inf" } else { "inf" });
        return;
    }
    let scale = pow10(FEATURE_DECIMALS);
    let rounded = (value.abs() * scale as f64).round();
    let scaled = if rounded >= i64::MAX as f64 { i64::MAX as u64 } else { rounded as u64 };
    if value.is_sign_negative() && scaled != 0 {
        buf.push('-');
    }
    push_scaled(buf, scaled, FEATURE_DECIMALS);
}

/// A latency mean rounded to whole microseconds. Negative is a real reading, not a guard case: a
/// venue clock running ahead of the local one, or the simulated venue stamping venue time against a
/// wall-clock receive, both put an arrival before its own send.
pub fn write_latency_micros(buf: &mut String, micros: f64) {
    if !micros.is_finite() {
        write_missing(buf);
        return;
    }
    buf.clear();
    let whole = micros.abs().round() as u64;
    let is_negative = micros.is_sign_negative() && whole != 0;
    if let Some(marker) = column_bound_marker(whole, is_negative) {
        buf.push_str(marker);
        return;
    }
    if is_negative {
        buf.push('-');
    }
    push_grouped(buf, whole);
}

/// A reading wider than its column paints over the cell beside it, so past the bound the grid
/// states the bound. Reachable without a bug here: the summary crosses the link as `f64`, a venue
/// stamps what it likes, and a magnitude past `u64` saturates into this check rather than wrapping.
fn column_bound_marker(integer: u64, is_negative: bool) -> Option<&'static str> {
    if is_negative {
        return (integer >= LATENCY_MAX_NEGATIVE_INTEGER).then_some(LATENCY_UNDERFLOW);
    }
    (integer >= LATENCY_MAX_INTEGER).then_some(LATENCY_OVERFLOW)
}

pub fn write_opt_latency_micros(buf: &mut String, micros: Option<f64>) -> Wrote {
    write_opt(buf, micros, write_latency_micros)
}

pub fn write_slots(buf: &mut String, slots: f64) {
    if !slots.is_finite() {
        write_missing(buf);
        return;
    }
    buf.clear();
    let scale = pow10(SLOT_DECIMALS);
    let scaled = (slots.abs() * scale as f64).round() as u64;
    let is_negative = slots.is_sign_negative() && scaled != 0;
    if let Some(marker) = column_bound_marker(scaled / scale, is_negative) {
        buf.push_str(marker);
        return;
    }
    if is_negative {
        buf.push('-');
    }
    push_scaled(buf, scaled, SLOT_DECIMALS);
}

pub fn write_opt_slots(buf: &mut String, slots: Option<f64>) -> Wrote {
    write_opt(buf, slots, write_slots)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AxisTicks {
    cursor: i64,
    remaining: usize,
    step: i64,
}

impl AxisTicks {
    pub fn step(self) -> i64 {
        self.step
    }
}

impl Iterator for AxisTicks {
    type Item = i64;

    fn next(&mut self) -> Option<i64> {
        self.remaining = self.remaining.checked_sub(1)?;
        let value = self.cursor;
        self.cursor = value.saturating_add(self.step);
        Some(value)
    }
}

pub fn legible_tick_ceiling(height: f32, ideal_pitch: f32, minimum_pitch: f32) -> usize {
    debug_assert!(
        ideal_pitch > 0.0 && minimum_pitch > 0.0,
        "label pitches must be positive, got ideal {ideal_pitch} and minimum {minimum_pitch}"
    );
    if ((height / minimum_pitch) as usize) < MIN_TICK_CEILING {
        return 0;
    }
    (height / ideal_pitch) as usize
}

pub fn axis_ticks(low: i64, high: i64, max_ticks: usize) -> AxisTicks {
    if max_ticks == 0 {
        return NO_TICKS;
    }
    let ceiling = max_ticks.max(MIN_TICK_CEILING);
    nice_steps()
        .filter_map(|step| ticks_within(low, high, step))
        .find(|ticks| ticks.remaining <= ceiling)
        .unwrap_or(NO_TICKS)
}

fn nice_steps() -> impl Iterator<Item = i64> {
    (0..=MAX_STEP_DECADE)
        .flat_map(|decade| NICE_STEP_MULTIPLES.map(move |multiple| multiple * 10i64.pow(decade)))
}

fn ticks_within(low: i64, high: i64, step: i64) -> Option<AxisTicks> {
    let below = low.div_euclid(step).checked_mul(step)?;
    let first = if below < low { below.checked_add(step)? } else { below };
    let last = high.div_euclid(step).checked_mul(step)?;
    let span = i128::from(last) - i128::from(first);
    let count = if span < 0 { 0 } else { span / i128::from(step) + 1 };
    Some(AxisTicks {
        cursor: first,
        remaining: usize::try_from(count).ok()?,
        step,
    })
}

fn decimals_for_increment(mantissa: i64) -> usize {
    if mantissa <= 0 {
        return 0;
    }
    let mut decimals = 8i32;
    let mut value = mantissa;
    while decimals > 0 && value % 10 == 0 {
        value /= 10;
        decimals -= 1;
    }
    decimals as usize
}

fn pow10(exp: usize) -> u64 {
    10u64.pow(exp as u32)
}

fn push_int(buf: &mut String, value: i64) {
    if value < 0 {
        buf.push('-');
    }
    push_uint(buf, value.unsigned_abs());
}

fn decimal_digits(value: u64, digits: &mut [u8; 20]) -> usize {
    let mut cursor = digits.len();
    let mut n = value;
    loop {
        cursor -= 1;
        digits[cursor] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    cursor
}

fn push_uint(buf: &mut String, value: u64) {
    push_padded(buf, value, 0);
}

/// Push `scaled` as `integer.fraction`, where `scaled` already carries exactly `decimals` places.
fn push_scaled(buf: &mut String, scaled: u64, decimals: usize) {
    let unit = pow10(decimals);
    push_uint(buf, scaled / unit);
    if decimals == 0 {
        return;
    }
    buf.push('.');
    push_padded(buf, scaled % unit, decimals);
}

fn push_grouped(buf: &mut String, value: u64) {
    let mut digits = [0u8; 20];
    let cursor = decimal_digits(value, &mut digits);
    let total = digits.len() - cursor;
    for (place, &digit) in digits[cursor..].iter().enumerate() {
        if place > 0 && (total - place).is_multiple_of(MICROS_GROUP_DIGITS) {
            buf.push(MICROS_GROUP_SEPARATOR);
        }
        buf.push(digit as char);
    }
}

fn push_padded(buf: &mut String, value: u64, width: usize) {
    let mut digits = [0u8; 20];
    let cursor = decimal_digits(value, &mut digits);
    for _ in (digits.len() - cursor)..width {
        buf.push('0');
    }
    for &digit in &digits[cursor..] {
        buf.push(digit as char);
    }
}
