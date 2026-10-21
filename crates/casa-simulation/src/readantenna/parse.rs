// SPDX-License-Identifier: LGPL-3.0-or-later
//! The text rules of CASA's `readantenna`, which are Python's: universal
//! newlines, `str.split()`, `str.strip()`, `float()` and `int()`.

use std::collections::BTreeMap;

use super::ReadAntennaError;

/// The header parameters and antenna rows of a configuration file, before
/// any coordinate conversion.
#[derive(Debug, Default)]
pub(super) struct ParsedConfig {
    pub(super) header: BTreeMap<String, String>,
    pub(super) x: Vec<f64>,
    pub(super) y: Vec<f64>,
    pub(super) z: Vec<f64>,
    pub(super) diameter: Vec<f64>,
    pub(super) pad_names: Vec<String>,
    pub(super) antenna_names: Vec<String>,
}

/// Parse a configuration file's text as `readantenna`'s loop does.
///
/// - A line starting with `#` is a header. Split after the `#` on `=`; when
///   that gives exactly two parts, each loses every `#` and its surrounding
///   whitespace and the pair becomes a header parameter (a later duplicate
///   wins). Any other header line is a comment.
/// - Any other line loses everything from its first `#`. With four or more
///   whitespace-separated columns it is an antenna: x, y, z, diameter, then
///   the pad (station) and the antenna name. Five columns use the pad as the
///   name too; four name both `A%02d` by antenna index.
/// - Lines with fewer than four columns are skipped (see the module docs for
///   how CASA differs).
pub(super) fn parse_config(contents: &str) -> Result<ParsedConfig, ReadAntennaError> {
    let mut parsed = ParsedConfig::default();
    for (index, line) in universal_lines(contents).enumerate() {
        if let Some(rest) = line.strip_prefix('#') {
            if let Some((key, value)) = rest.split_once('=')
                && !value.contains('=')
            {
                parsed.header.insert(clean(key), clean(value));
            }
            continue;
        }
        let data = line.split_once('#').map_or(line, |(before, _)| before);
        let columns: Vec<&str> = python_split(data).collect();
        if columns.len() < 4 {
            continue;
        }
        let number = |column: usize| {
            python_float(columns[column]).ok_or_else(|| ReadAntennaError::InvalidNumber {
                path: None,
                line: index + 1,
                value: columns[column].to_string(),
            })
        };
        let (x, y, z, diameter) = (number(0)?, number(1)?, number(2)?, number(3)?);
        let row = parsed.x.len();
        let (pad, name) = match columns.len() {
            4 => {
                let default = format!("A{row:02}");
                (default.clone(), default)
            }
            5 => (columns[4].to_string(), columns[4].to_string()),
            _ => (columns[4].to_string(), columns[5].to_string()),
        };
        parsed.x.push(x);
        parsed.y.push(y);
        parsed.z.push(z);
        parsed.diameter.push(diameter);
        parsed.pad_names.push(pad);
        parsed.antenna_names.push(name);
    }
    Ok(parsed)
}

/// `value.replace('#', '').strip()`.
fn clean(value: &str) -> String {
    python_strip(&value.replace('#', "")).to_string()
}

/// Python's text-mode line iteration: lines end at `\n`, `\r\n` or `\r`,
/// and a final terminator does not start an empty line.
fn universal_lines(text: &str) -> impl Iterator<Item = &str> {
    let mut rest = text;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        let Some(end) = rest.find(['\r', '\n']) else {
            let line = rest;
            rest = "";
            return Some(line);
        };
        let line = &rest[..end];
        let terminator = if rest[end..].starts_with("\r\n") {
            2
        } else {
            1
        };
        rest = &rest[end + terminator..];
        Some(line)
    })
}

/// Python's `str.isspace` for one character: Rust's Unicode white space
/// plus the information separators U+001C to U+001F.
fn is_python_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python's `str.strip()` with no arguments.
fn python_strip(text: &str) -> &str {
    text.trim_matches(is_python_space)
}

/// Python's `str.split()` with no arguments.
fn python_split(text: &str) -> impl Iterator<Item = &str> {
    text.split(is_python_space).filter(|part| !part.is_empty())
}

/// Remove the underscores Python allows between digits (PEP 515), or
/// `None` when an underscore is not between two ASCII digits.
fn without_digit_underscores(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    for (i, &byte) in bytes.iter().enumerate() {
        if byte == b'_' {
            let before = i.checked_sub(1).map(|j| bytes[j]);
            let after = bytes.get(i + 1).copied();
            if !(before.is_some_and(|b| b.is_ascii_digit())
                && after.is_some_and(|b| b.is_ascii_digit()))
            {
                return None;
            }
        }
    }
    Some(text.replace('_', ""))
}

/// Python's `float(text)` for ASCII text: surrounding white space is
/// ignored, underscores may separate digits, and `inf`, `infinity` and
/// `nan` (any case, optionally signed) are accepted. Unlike Python,
/// non-ASCII decimal digits are rejected.
pub(super) fn python_float(text: &str) -> Option<f64> {
    let text = without_digit_underscores(python_strip(text))?;
    text.parse().ok()
}

/// Python's `int(text)` in base 10, within `i64`: surrounding white space
/// is ignored, a sign is allowed and underscores may separate digits.
pub(super) fn python_int(text: &str) -> Option<i64> {
    let text = without_digit_underscores(python_strip(text))?;
    text.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floats_follow_python() {
        assert_eq!(python_float("12."), Some(12.0));
        assert_eq!(python_float(" -1.5e3 "), Some(-1500.0));
        assert_eq!(python_float("+.5"), Some(0.5));
        assert_eq!(python_float("1_000.000_5"), Some(1000.0005));
        assert_eq!(python_float("1e1_0"), Some(1e10));
        assert_eq!(python_float("-Infinity"), Some(f64::NEG_INFINITY));
        assert!(python_float("NaN").is_some_and(f64::is_nan));
        for bad in [
            "", "1__0", "_1", "1_", "1_.5", "1._5", "0x10", "1,5", "e5", ".", "1e",
        ] {
            assert_eq!(python_float(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn ints_follow_python() {
        assert_eq!(python_int("19"), Some(19));
        assert_eq!(python_int(" +07 "), Some(7));
        assert_eq!(python_int("-3"), Some(-3));
        assert_eq!(python_int("1_9"), Some(19));
        for bad in ["", "19.0", "1e1", "_19", "19_", "N"] {
            assert_eq!(python_int(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn lines_split_on_every_python_newline() {
        let lines: Vec<&str> = universal_lines("a\r\nb\rc\n\nd\n").collect();
        assert_eq!(lines, ["a", "b", "c", "", "d"]);
        assert_eq!(universal_lines("").count(), 0);
        assert_eq!(universal_lines("x").collect::<Vec<_>>(), ["x"]);
    }

    #[test]
    fn split_and_strip_use_python_white_space() {
        let parts: Vec<&str> = python_split(" a\tb\u{1f}c\u{a0}d \u{0b}").collect();
        assert_eq!(parts, ["a", "b", "c", "d"]);
        assert_eq!(python_strip("\u{1c} XYZ \n"), "XYZ");
    }

    #[test]
    fn header_pairs_lose_octothorpes_and_white_space() {
        let parsed = parse_config(
            "## coordsys = XYZ #\n# a=b=c\n# note: zone=19\n#observatory=VLA\n#observatory=ALMA\n# comment\n",
        )
        .expect("parses");
        let header: Vec<(&str, &str)> = parsed
            .header
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        // Two '=' make a comment; the key is everything before the '='.
        assert_eq!(
            header,
            [
                ("coordsys", "XYZ"),
                ("note: zone", "19"),
                ("observatory", "ALMA")
            ]
        );
        assert!(parsed.x.is_empty());
    }

    #[test]
    fn rows_take_names_by_column_count() {
        let parsed = parse_config(
            "1 2 3 4 P0 N0 extra # six or more\n\
             \n\
             5 6 7 8 P1\t# five\n\
             only three columns\n\
             9 10 11 12 # four\n\
             \u{20}  # indented comment\n",
        )
        .expect("parses");
        assert_eq!(parsed.x, [1.0, 5.0, 9.0]);
        assert_eq!(parsed.diameter, [4.0, 8.0, 12.0]);
        assert_eq!(parsed.pad_names, ["P0", "P1", "A02"]);
        assert_eq!(parsed.antenna_names, ["N0", "P1", "A02"]);
    }

    #[test]
    fn a_bad_number_names_its_line() {
        let error = parse_config("# coordsys=XYZ\n1 2 3 4\n1 2 three 4\n").expect_err("bad row");
        assert!(matches!(
            error,
            ReadAntennaError::InvalidNumber { path: None, line: 3, ref value } if value == "three"
        ));
    }
}
