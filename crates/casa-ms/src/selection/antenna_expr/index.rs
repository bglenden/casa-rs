// SPDX-License-Identifier: LGPL-3.0-or-later
//! Name, station and pattern lookups over ANTENNA rows: a port of
//! `ms/MSSel/MSAntennaIndex.cc` and the numeric-name rules of the
//! `antidrange` actions (`MSAntennaGram.yy:336-380`).

use super::AntennaRow;
use super::regex::FullMatch;

/// The ANTENNA rows an expression resolves against; row `i` is antenna ID `i`.
pub(super) struct AntennaIndex<'r, 'a> {
    rows: &'r [AntennaRow<'a>],
}

impl<'r, 'a> AntennaIndex<'r, 'a> {
    pub(super) fn new(rows: &'r [AntennaRow<'a>]) -> Self {
        Self { rows }
    }

    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(super) fn rows(&self) -> &'r [AntennaRow<'a>] {
        self.rows
    }

    /// `matchAntennaName(const String&)` (`MSAntennaIndex.cc:126-158`): the
    /// rows whose NAME equals `name`; only when none does, the rows whose
    /// STATION equals it.
    pub(super) fn match_antenna_name(&self, name: &str) -> Vec<usize> {
        let by_name = self.rows_where(|row| row.name == name);
        if by_name.is_empty() {
            self.match_station_name(name)
        } else {
            by_name
        }
    }

    /// The first row [`match_antenna_name`](Self::match_antenna_name) finds,
    /// without collecting the rest: what the `antidrange` actions keep
    /// (`tmp[0]`, `MSAntennaGram.yy:354,375`).
    pub(super) fn first_antenna_name(&self, name: &str) -> Option<usize> {
        self.rows
            .iter()
            .position(|row| row.name == name)
            .or_else(|| self.rows.iter().position(|row| row.station == name))
    }

    /// `matchStationName(const String&)` (`MSAntennaIndex.cc:231-263`).
    pub(super) fn match_station_name(&self, station: &str) -> Vec<usize> {
        self.rows_where(|row| row.station == station)
    }

    /// `matchAntennaRegexOrPattern` (`MSAntennaIndex.cc:82-122`): a leading
    /// `^` negates; an antenna matches when its NAME matches, or else its
    /// STATION does. Unlike [`match_antenna_name`](Self::match_antenna_name)
    /// the station fallback is per row.
    pub(super) fn match_antenna_regex_or_pattern(
        &self,
        negated: bool,
        text: &str,
        is_regex: bool,
    ) -> Result<Vec<usize>, String> {
        let matcher = compile(text, is_regex)?;
        Ok(self.rows_where(|row| {
            (matcher.matches(row.name) || matcher.matches(row.station)) != negated
        }))
    }

    /// `matchStationRegexOrPattern` (`MSAntennaIndex.cc:187-227`): as the
    /// antenna form over STATION alone.
    pub(super) fn match_station_regex_or_pattern(
        &self,
        negated: bool,
        text: &str,
        is_regex: bool,
    ) -> Result<Vec<usize>, String> {
        let matcher = compile(text, is_regex)?;
        Ok(self.rows_where(|row| matcher.matches(row.station) != negated))
    }

    /// The antennas of the integer range `start~end`, as the
    /// `INT DASH INT` action builds them (`MSAntennaGram.yy:358-380`): each
    /// integer `i` names the first antenna whose NAME (else STATION) is
    /// `i` written in decimal, and otherwise is antenna ID `i`. Only IDs of
    /// existing antennas are returned, the ones `matchId` would keep.
    ///
    /// The C++ enumerates the whole range; this visits the existing IDs and
    /// the decimal names instead, so the work and memory do not grow with
    /// the range width (`0~2147483646` costs what `0~1` does).
    pub(super) fn integer_range(&self, start: i32, end: i32) -> Vec<usize> {
        let mut ids = Vec::new();
        let existing = usize::try_from(start.max(0)).unwrap_or(0)
            ..usize::try_from(end.saturating_add(1).max(0))
                .unwrap_or(0)
                .min(self.rows.len());
        for id in existing.clone() {
            let mut digits = DecimalBuffer::default();
            let decimal = digits.format(i64::try_from(id).unwrap_or(i64::MAX));
            ids.push(self.first_antenna_name(decimal).unwrap_or(id));
        }
        // Integers outside the existing IDs reach an antenna only by name.
        for row in self.rows {
            for text in [row.name, row.station] {
                let Some(value) = canonical_integer(text) else {
                    continue;
                };
                let in_range = (i64::from(start)..=i64::from(end)).contains(&value);
                let existing_id = usize::try_from(value).is_ok_and(|id| existing.contains(&id));
                if in_range && !existing_id {
                    ids.extend(self.first_antenna_name(text));
                }
            }
        }
        ids
    }

    fn rows_where(&self, predicate: impl Fn(&AntennaRow<'a>) -> bool) -> Vec<usize> {
        self.rows
            .iter()
            .enumerate()
            .filter(|(_, row)| predicate(row))
            .map(|(id, _)| id)
            .collect()
    }
}

fn compile(text: &str, is_regex: bool) -> Result<FullMatch, String> {
    if is_regex {
        FullMatch::compile(text)
    } else {
        FullMatch::compile_pattern(text)
    }
}

/// The integer `text` writes as `ostringstream << Int` would (no sign
/// other than `-`, no leading zeros), if it is one.
fn canonical_integer(text: &str) -> Option<i64> {
    let value = text.parse::<i64>().ok()?;
    let mut digits = DecimalBuffer::default();
    (digits.format(value) == text).then_some(value)
}

/// A stack buffer for writing an integer in decimal without allocating.
#[derive(Default)]
struct DecimalBuffer {
    bytes: [u8; 20],
}

impl DecimalBuffer {
    fn format(&mut self, value: i64) -> &str {
        let mut magnitude = value.unsigned_abs();
        let mut start = self.bytes.len();
        loop {
            start -= 1;
            self.bytes[start] = b'0' + (magnitude % 10) as u8;
            magnitude /= 10;
            if magnitude == 0 {
                break;
            }
        }
        if value < 0 {
            start -= 1;
            self.bytes[start] = b'-';
        }
        std::str::from_utf8(&self.bytes[start..]).unwrap_or_default()
    }
}

/// glibc's `atoi` on a run of decimal digits: `(int) strtol(text)`, where
/// `strtol` saturates at `LONG_MAX` (64-bit) and the conversion to `int`
/// keeps the low 32 bits. `4294967296` is therefore `0`, as in CASA.
pub(super) fn c_atoi(digits: &str) -> i32 {
    let value = digits.parse::<i64>().unwrap_or(i64::MAX);
    value as i32
}

/// glibc's `atof` on a `{FLOAT}` lexeme: `strtod` reads the longest valid
/// prefix, and a Fortran-style `D` exponent is not part of one, so `1d3`
/// is `1`.
pub(super) fn c_atof(lexeme: &str) -> f64 {
    let mantissa = lexeme
        .find(['D', 'd'])
        .map_or(lexeme, |exponent| &lexeme[..exponent]);
    mantissa.parse::<f64>().unwrap_or(0.0)
}
