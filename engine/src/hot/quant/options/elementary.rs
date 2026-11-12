//! Exponential, natural logarithm and cube root from plain arithmetic, so that each target
//! computes the same bits. The methods of the standard library may round differently from one
//! platform to the next.

use std::f64::consts::{LN_2, SQRT_2};

/// The smallest degree with which the reference prices pass, plus a quarter.
const EXP_SERIES_DEGREE: u32 = 13;

/// The smallest count with which the reference prices pass, plus a quarter.
const LN_SERIES_TERMS: u32 = 9;

/// The smallest count with which the cube of each root matches its argument to 1e-14, for each
/// mantissa from 1 to 8, plus a quarter.
const CBRT_NEWTON_STEPS: u32 = 5;

/// Above this argument the exponential is larger than the largest finite number.
const OVERFLOW_ARGUMENT: f64 = f64::MAX_EXP as f64 * LN_2;

/// Below this argument the exponential rounds to zero.
const UNDERFLOW_ARGUMENT: f64 = (f64::MIN_EXP - f64::MANTISSA_DIGITS as i32 - 1) as f64 * LN_2;

/// 2, 4, 16, and so on to 2^512. Each is the square of the one before, so each is exact.
const SQUARED_POWERS_OF_TWO: [f64; 10] = squared_powers_of_two();

/// 2^64 and its exponent: a subnormal number times 2^64 is a normal number, where the exponent
/// search works.
const SUBNORMAL_LIFT: f64 = SQUARED_POWERS_OF_TWO[6];
const SUBNORMAL_LIFT_EXPONENT: i32 = 64;

const fn squared_powers_of_two() -> [f64; 10] {
    let mut powers = [2.0; 10];
    let mut index = 1;
    while index < powers.len() {
        powers[index] = powers[index - 1] * powers[index - 1];
        index += 1;
    }
    powers
}

/// e^x = 2^k × e^r, with k the integer nearest to x / ln 2, so that the remainder r is at most half
/// of ln 2 in size. The series of e^r then converges fast.
pub(crate) fn exp(x: f64) -> f64 {
    if x > OVERFLOW_ARGUMENT {
        return f64::INFINITY;
    }
    if x < UNDERFLOW_ARGUMENT {
        return 0.0;
    }
    let quotient = x / LN_2;
    let k = if quotient < 0.0 { (quotient - 0.5) as i32 } else { (quotient + 0.5) as i32 };
    let remainder = x - f64::from(k) * LN_2;
    let mut series = 1.0;
    for n in (1..=EXP_SERIES_DEGREE).rev() {
        series = 1.0 + remainder * series / f64::from(n);
    }
    let half = k / 2;
    series * power_of_two(half) * power_of_two(k - half)
}

/// ln x = e × ln 2 + 2 atanh(s), with x = m × 2^e, m from √½ to √2, and s = (m − 1) / (m + 1).
/// The series of atanh needs few terms, because s is at most 0.172 in size.
pub(crate) fn ln(x: f64) -> f64 {
    if x <= 0.0 {
        return if x < 0.0 { f64::NAN } else { f64::NEG_INFINITY };
    }
    if x >= f64::INFINITY {
        return f64::INFINITY;
    }
    let (mantissa, exponent) = split_mantissa(x);
    let s = (mantissa - 1.0) / (mantissa + 1.0);
    let s_squared = s * s;
    let mut series = 1.0 / f64::from(2 * LN_SERIES_TERMS - 1);
    for k in (0..LN_SERIES_TERMS - 1).rev() {
        series = 1.0 / f64::from(2 * k + 1) + s_squared * series;
    }
    f64::from(exponent) * LN_2 + 2.0 * s * series
}

/// cbrt x = 2^q × cbrt a, with x = a × 2^(3q) and a from 1 to 8. The Newton steps for cbrt a
/// start on the line through cbrt 1 = 1 and cbrt 8 = 2.
pub(crate) fn cbrt(x: f64) -> f64 {
    if x <= 0.0 {
        return if x < 0.0 { f64::NAN } else { 0.0 };
    }
    if x >= f64::INFINITY {
        return f64::INFINITY;
    }
    let (mantissa, exponent) = split_mantissa(x);
    let (mantissa, exponent) =
        if mantissa < 1.0 { (mantissa * 2.0, exponent - 1) } else { (mantissa, exponent) };
    let a = mantissa * power_of_two(exponent.rem_euclid(3));
    let mut root = (6.0 + a) / 7.0;
    for _ in 0..CBRT_NEWTON_STEPS {
        root -= (root * root * root - a) / (3.0 * root * root);
    }
    root * power_of_two(exponent.div_euclid(3))
}

/// 2^exponent for an exponent below 1024 in size.
fn power_of_two(exponent: i32) -> f64 {
    let magnitude = exponent.unsigned_abs();
    let mut power = 1.0;
    for (bit, square) in SQUARED_POWERS_OF_TWO.iter().enumerate() {
        if magnitude & (1 << bit) != 0 {
            power *= square;
        }
    }
    if exponent < 0 { 1.0 / power } else { power }
}

/// m and e with x = m × 2^e and m from √½ to √2, for a finite x above 0. Each step multiplies or
/// divides by a power of two, so m keeps each bit of x.
fn split_mantissa(x: f64) -> (f64, i32) {
    let (mut mantissa, mut exponent) =
        if x < f64::MIN_POSITIVE { (x * SUBNORMAL_LIFT, -SUBNORMAL_LIFT_EXPONENT) } else { (x, 0) };
    if mantissa >= 1.0 {
        for (bit, square) in SQUARED_POWERS_OF_TWO.iter().enumerate().rev() {
            if mantissa >= *square {
                mantissa /= square;
                exponent += 1 << bit;
            }
        }
    } else {
        for (bit, square) in SQUARED_POWERS_OF_TWO.iter().enumerate().rev() {
            if mantissa * square < 1.0 {
                mantissa *= square;
                exponent -= 1 << bit;
            }
        }
        mantissa *= 2.0;
        exponent -= 1;
    }
    if mantissa >= SQRT_2 {
        mantissa *= 0.5;
        exponent += 1;
    }
    (mantissa, exponent)
}
