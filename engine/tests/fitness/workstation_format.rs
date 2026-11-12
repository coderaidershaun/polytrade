//! The latency panel's number writers. A grouped reading is the only thing the operator has to
//! judge engine health by, and its failure mode is silent: a separator placed one digit off turns
//! 3.4 seconds into 345 milliseconds, which reads as a perfectly healthy engine rather than as a
//! broken renderer. The panel also threads ONE scratch buffer through every cell it paints, so each
//! test reuses a single buffer — a writer that appended instead of clearing would fail these pins
//! the same way it would corrupt the grid.

use std::path::Path;

use crate::exec_no_clock::rust_sources;

use polysim::workstation::format::{
    MISSING, write_latency_micros, write_opt_latency_micros, write_opt_slots, write_slots,
};
use proptest::prelude::*;

/// The thousands separator the grid is drawn around. An ASCII space, because the µ and → glyphs
/// this UI cannot paint proved the same point about non-ASCII: the column is monospace digits.
const SEPARATOR: char = ' ';

/// Nine 12pt monospace characters is the value column's budget at the 1200px minimum window: the
/// left panel is half of it, less the 112px label column, split seven ways = 69.7px, against Hack's
/// 7.2px advance. Grouping spends two of the nine, leaving seven integer digits; a leading sign
/// spends one more, so a negative reading runs out a decade earlier.
const WIDEST_FITTING_MICROS: u64 = 9_999_999;
const WIDEST_FITTING_NEGATIVE_MICROS: u64 = 999_999;
const OVERFLOW: &str = ">1e7";
const UNDERFLOW: &str = "<-1e6";

/// Rounding is the half of the writer the grouping property cannot reach: it feeds whole
/// magnitudes, so only literals pin what a fraction does. Negative readings are ordinary here — a
/// venue clock ahead of the local one, and the simulated venue stamping venue time against a
/// wall-clock receive, both put an arrival before its own send. Both writers share the shape (round
/// an `f64` to their displayed precision), so one table covers them.
#[test]
fn a_reading_rounds_to_its_displayed_precision() {
    struct Case {
        name: &'static str,
        writer: fn(&mut String, f64),
        input: f64,
        expected: &'static str,
    }
    let cases = [
        Case {
            name: "latency rounds down within a group",
            writer: write_latency_micros,
            input: 999.4,
            expected: "999",
        },
        Case {
            name: "latency rounding up across a group boundary gains the separator",
            writer: write_latency_micros,
            input: 999.5,
            expected: "1 000",
        },
        Case {
            name: "a latency reading that rounds away its magnitude drops its sign",
            writer: write_latency_micros,
            input: -0.4,
            expected: "0",
        },
        Case {
            name: "an empty slot queue is a number the operator wants",
            writer: write_slots,
            input: 0.0,
            expected: "0.0",
        },
        Case {
            name: "slots round down within a place",
            writer: write_slots,
            input: 0.44,
            expected: "0.4",
        },
        Case {
            name: "slots round, unlike the truncating money writers",
            writer: write_slots,
            input: 0.45,
            expected: "0.5",
        },
        Case {
            name: "slots round down at a larger magnitude",
            writer: write_slots,
            input: 12.349,
            expected: "12.3",
        },
    ];
    let mut buf = String::new();
    for case in cases {
        (case.writer)(&mut buf, case.input);
        assert_eq!(buf, case.expected, "{}", case.name);
    }
}

/// Both writers take an `f64` off a UDP link, so a NaN or an infinity is reachable without a bug
/// anywhere in this process. Either must read as absent — a rendered `inf` or a clamped maximum
/// would both look like a measurement.
#[test]
fn an_unmeasurable_value_renders_as_missing() {
    let mut buf = String::new();

    write_latency_micros(&mut buf, f64::NAN);
    assert_eq!(buf, MISSING);
    write_latency_micros(&mut buf, f64::INFINITY);
    assert_eq!(buf, MISSING);
    write_slots(&mut buf, f64::NEG_INFINITY);
    assert_eq!(buf, MISSING);

    write_opt_latency_micros(&mut buf, None);
    assert_eq!(
        buf, MISSING,
        "an unlit cell reads the same as an unusable one"
    );
    write_opt_slots(&mut buf, None);
    assert_eq!(buf, MISSING);
    write_opt_latency_micros(&mut buf, Some(1_500.0));
    assert_eq!(
        buf, "1 500",
        "and a lit one still renders after a missing one"
    );
}

/// FITNESS: a reading wider than its column does not merely look wrong, it overwrites the columns
/// beside it — the digits of one cell land on top of another cell's number, and every reading in the
/// row becomes unreadable together. A stamp the engine cannot interpret is reachable without a bug
/// here (a venue clock, a backfilled candle, a garbled link frame), so the grid states the bound it
/// can render rather than painting a number over its neighbour.
#[test]
fn a_reading_too_wide_for_its_column_states_its_bound_instead() {
    let mut buf = String::new();

    write_latency_micros(&mut buf, WIDEST_FITTING_MICROS as f64);
    assert_eq!(buf, "9 999 999", "nine characters exactly, and they fit");
    write_latency_micros(&mut buf, (WIDEST_FITTING_MICROS + 1) as f64);
    assert_eq!(
        buf, OVERFLOW,
        "the tenth character is the one that collides"
    );
    write_latency_micros(&mut buf, WIDEST_FITTING_MICROS as f64 + 0.5);
    assert_eq!(
        buf, OVERFLOW,
        "a reading that ROUNDS past the bound is past it"
    );

    // The reading that exposed this: a REST-backfilled candle's age folded into a latency mean.
    write_latency_micros(&mut buf, 5_500_000_000.0);
    assert_eq!(buf, OVERFLOW);

    write_latency_micros(&mut buf, -(WIDEST_FITTING_NEGATIVE_MICROS as f64));
    assert_eq!(buf, "-999 999", "the widest reading a sign leaves room for");
    write_latency_micros(&mut buf, -(WIDEST_FITTING_NEGATIVE_MICROS as f64 + 1.0));
    assert_eq!(buf, UNDERFLOW);

    // Same column, same budget: one decimal place costs what grouping costs.
    write_slots(&mut buf, WIDEST_FITTING_MICROS as f64);
    assert_eq!(buf, "9999999.0");
    write_slots(&mut buf, (WIDEST_FITTING_MICROS + 1) as f64);
    assert_eq!(buf, OVERFLOW);
    write_slots(&mut buf, -(WIDEST_FITTING_NEGATIVE_MICROS as f64 + 1.0));
    assert_eq!(buf, UNDERFLOW);

    // Past u64::MAX entirely: `as u64` saturates, so a value this large reads as overflow.
    write_latency_micros(&mut buf, 1e20);
    assert_eq!(buf, OVERFLOW);
    write_latency_micros(&mut buf, -1e20);
    assert_eq!(buf, UNDERFLOW);
    write_slots(&mut buf, 1e20);
    assert_eq!(buf, OVERFLOW);
}

proptest! {
    /// Grouping is only trustworthy if it never edits the number: the digits between the separators,
    /// concatenated, must be exactly the reading, and every group but the leading one must hold
    /// three. A dropped or doubled digit is invisible to a reader who has nothing to compare against.
    /// The generator stops at the positive bound: past it every magnitude is the marker's, pinned
    /// above, and a range spanning both would land practically every case on the marker instead.
    #[test]
    fn a_grouped_reading_is_its_own_digits_split_in_threes(
        magnitude in 0u64..=WIDEST_FITTING_MICROS,
        is_negative in any::<bool>(),
    ) {
        let mut buf = String::new();
        let reading = if is_negative { -(magnitude as f64) } else { magnitude as f64 };
        write_latency_micros(&mut buf, reading);

        let carries_sign = is_negative && magnitude != 0;
        if carries_sign && magnitude > WIDEST_FITTING_NEGATIVE_MICROS {
            prop_assert_eq!(buf.as_str(), UNDERFLOW, "unrenderable -{} must state its bound", magnitude);
            return Ok(());
        }
        prop_assert_eq!(buf.starts_with('-'), carries_sign, "sign on {}", buf);

        let digits = buf.trim_start_matches('-');
        let groups: Vec<&str> = digits.split(SEPARATOR).collect();
        prop_assert!(
            (1..=3).contains(&groups[0].len()),
            "leading group of {} is not 1-3 digits",
            buf
        );
        for group in &groups[1..] {
            prop_assert_eq!(group.len(), 3, "short group in {}", buf);
        }
        prop_assert_eq!(
            groups.concat().parse::<u64>().ok(),
            Some(magnitude),
            "grouping changed the value in {}",
            buf
        );
    }
}

/// Every string literal in the workstation is ASCII. The bundled default fonts carry no glyph for
/// anything else and paint a tofu box in its place, which is what a literal `—` or `Δ` would show.
/// The rule covers the whole module rather than the painters alone, because a literal one file over
/// is one copy-paste from a cell, and a log line or a window title loses nothing by obeying it.
#[test]
fn no_workstation_string_literal_carries_a_glyph_the_default_fonts_cannot_paint() {
    let mut offenders: Vec<String> = Vec::new();
    for path in rust_sources(Path::new("src/workstation")) {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{} is readable: {error}", path.display()));
        for (line, text) in non_ascii_string_literals(&source) {
            offenders.push(format!("{}:{line}: {text}", path.display()));
        }
    }
    assert!(
        offenders.is_empty(),
        "these string literals would paint as tofu boxes: {offenders:#?} - write them in ASCII, or \
         paint a shape the way the DOM chevron and the unseen badge do"
    );
}

/// Line number and content of each non-ASCII string literal. Comments are skipped: prose explaining
/// a rule is not text anyone paints, and holding comments to ASCII would cost readability for nothing.
fn non_ascii_string_literals(source: &str) -> Vec<(usize, String)> {
    let mut found = Vec::new();
    let bytes: Vec<char> = source.chars().collect();
    let mut index = 0;
    let mut line = 1;
    while index < bytes.len() {
        let character = bytes[index];
        if character == '\n' {
            line += 1;
            index += 1;
            continue;
        }
        if character == '/' && bytes.get(index + 1) == Some(&'/') {
            while index < bytes.len() && bytes[index] != '\n' {
                index += 1;
            }
            continue;
        }
        if character == '/' && bytes.get(index + 1) == Some(&'*') {
            index += 2;
            while index < bytes.len()
                && !(bytes[index] == '*' && bytes.get(index + 1) == Some(&'/'))
            {
                line += usize::from(bytes[index] == '\n');
                index += 1;
            }
            index += 2;
            continue;
        }
        if character != '"' {
            index += 1;
            continue;
        }
        let opened_at = line;
        let mut literal = String::new();
        index += 1;
        while index < bytes.len() && bytes[index] != '"' {
            if bytes[index] == '\\' {
                index += 1;
            }
            if index < bytes.len() {
                line += usize::from(bytes[index] == '\n');
                literal.push(bytes[index]);
                index += 1;
            }
        }
        index += 1;
        if !literal.is_ascii() {
            found.push((opened_at, literal));
        }
    }
    found
}
