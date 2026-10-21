// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's quantity string reader.
//!
//! [`Quantity::read`] is the Rust counterpart of casacore's
//! `Quantity::read(Quantity&, const String&)` (`readQuantity` in
//! `Quantum2.cc`), the reader behind `QuantumHolder::fromString` and so behind
//! every quantity string a CASA tool or task accepts. It tries, in order:
//!
//! 1. an angle in `MVAngle::read` form: `12h30m00.0`, `45d00m00`, `-30.00.00.5`
//!    (dotted degrees), `12:30:00` (a time-angle, 15 degrees per hour) or
//!    `10d` (degrees), giving degrees;
//! 2. a date-time in `MVTime::read` form: `2020/01/01/12:00:00`,
//!    `2020-01-01T12:00:00+02:00`, `01Jan2020/12:00`, `15-Jan-99`, `2020/01/15.5`,
//!    `58849/12:00` (an MJD day and a time), `today` or `now`, giving days
//!    (MJD for calendar dates);
//! 3. a value and a unit: `10s`, `1.4GHz`, `50 MHz`, `.5`, `Jy` (zero Jy).
//!
//! Each step keeps casacore's quirks, which scripts rely on: `12h` is
//! 12 hours but `12h30m` is 187.5 degrees, `10d` is 10 degrees but `10.d` is
//! 10 days, and dates use casacore's calendar formula (Julian before
//! 1582-10-15, no range checks, so 2020-02-30 is 1 March).
//!
//! The unit after a value is checked with this crate's unit parser and named
//! as casacore's `Unit` names it (`" MHz"` becomes `MHz`, `km s-1` becomes
//! `km.s-1`). casacore's unit grammar is wider than the parser's in a few
//! corners (`h:`, a leading digit or repeated signs before a unit); those
//! strings are rejected here.
//!
//! Other readers in the workspace parse subsets of these forms for their own
//! inputs: `parse_sexagesimal` in casa-imaging-application's phase-centre
//! compiler and `parse_datetime_to_mjd` in casa-tables' TaQL functions.

use std::fmt;

use crate::quanta::quantity::Quantity;
use crate::quanta::unit::Unit;

/// MJD of the Unix epoch, 1970-01-01.
const UNIX_EPOCH_MJD: f64 = 40_587.0;

/// The month names `MVTime::read` matches by unique prefix.
const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

/// A string that casacore's quantity reader does not accept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuantityReadError {
    input: String,
}

impl QuantityReadError {
    /// The string that could not be read.
    pub fn input(&self) -> &str {
        &self.input
    }
}

impl fmt::Display for QuantityReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "illegal input units or format in quantity string {:?}",
            self.input
        )
    }
}

impl std::error::Error for QuantityReadError {}

impl Quantity {
    /// Reads a quantity string as casacore's `Quantity::read` does (see the
    /// [module documentation](self) for the accepted forms). `today` and
    /// `now` refer to the system clock.
    ///
    /// # Errors
    ///
    /// The string is not an angle, a date-time or a value with a known unit.
    ///
    /// # Examples
    ///
    /// ```
    /// use casa_types::quanta::Quantity;
    ///
    /// let angle = Quantity::read("12h30m").unwrap();
    /// assert_eq!((angle.value(), angle.unit().name()), (187.5, "deg"));
    /// let date = Quantity::read("2020/01/01/12:00:00").unwrap();
    /// assert_eq!((date.value(), date.unit().name()), (58849.5, "d"));
    /// let frequency = Quantity::read("1.4 GHz").unwrap();
    /// assert_eq!((frequency.value(), frequency.unit().name()), (1.4, "GHz"));
    /// ```
    pub fn read(text: &str) -> Result<Quantity, QuantityReadError> {
        Self::read_at(text, system_mjd())
    }

    /// [`read`](Self::read) with `now_mjd` (UTC MJD days) standing for the
    /// clock that `today` and `now` refer to.
    ///
    /// # Errors
    ///
    /// As [`read`](Self::read).
    pub fn read_at(text: &str, now_mjd: f64) -> Result<Quantity, QuantityReadError> {
        let error = || QuantityReadError {
            input: text.to_string(),
        };
        // casacore reads strings with no digit after their leading blanks and
        // punctuation (and no `today` or `now`) as "0.0" followed by them, so
        // "Jy" is zero Jy.
        let lower = text.to_ascii_lowercase();
        let prefixed;
        let source = if !text.is_empty()
            && !lower.contains("today")
            && !lower.contains("now")
            && !starts_with_number(text)
        {
            prefixed = format!("0.0{text}");
            prefixed.as_str()
        } else {
            text
        };
        let mut cursor = Cursor::new(source);
        if cursor.eos() {
            return Ok(Quantity::dimensionless(0.0));
        }
        if let Some(degrees) = read_angle(&mut cursor, true) {
            return Quantity::new(degrees, "deg").map_err(|_| error());
        }
        if let Some(days) = read_time(&mut cursor, true, now_mjd) {
            return Quantity::new(days, "d").map_err(|_| error());
        }
        let value = cursor.get_double();
        let unit_text = cursor.rest();
        if !unit_text.bytes().all(is_unit_byte) {
            return Err(error());
        }
        let unit = Unit::new(&casacore_unit_name(unit_text)).map_err(|_| error())?;
        Ok(Quantity::with_unit(value, unit))
    }
}

/// casacore's `MUString::minimaxNC`: the index of the name `input` matches,
/// case-insensitively, either exactly or as the only name sharing its first
/// `min(input, name)` characters. casacore matches measure reference codes
/// (`MEpoch::getType` and the others) and month names this way, so `j20`
/// is `J2000` and `U` is ambiguous among `UT1`, `UT2`, `UTC` and `UT`.
pub fn unique_prefix_index(input: &str, names: &[&str]) -> Option<usize> {
    let wanted = input.to_ascii_uppercase().into_bytes();
    let upper = |name: &str| name.to_ascii_uppercase().into_bytes();
    if let Some(exact) = names.iter().position(|name| upper(name) == wanted) {
        return Some(exact);
    }
    let shares_prefix = |name: &str| {
        let name = upper(name);
        let length = wanted.len().min(name.len());
        wanted[..length] == name[..length]
    };
    let first = names.iter().position(|name| shares_prefix(name))?;
    if names[first + 1..].iter().any(|name| shares_prefix(name)) {
        None
    } else {
        Some(first)
    }
}

/// The current UTC time as MJD days.
fn system_mjd() -> f64 {
    let since_unix_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |elapsed| elapsed.as_secs_f64());
    UNIX_EPOCH_MJD + since_unix_s / 86_400.0
}

/// Whether `text` matches casacore's `^[[:space:][:punct:]]*[[:digit:]]`.
fn starts_with_number(text: &str) -> bool {
    text.bytes()
        .find(|&byte| !(byte.is_ascii_punctuation() || is_c_space(byte)))
        .is_some_and(|byte| byte.is_ascii_digit())
}

/// C's `isspace` in the C locale.
fn is_c_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0B | 0x0C | b'\r')
}

/// Bytes casacore's unit grammar (`UnitVal::create`) can consume: names,
/// digits, signs, separators, powers and parentheses.
fn is_unit_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"_\"'$:% *./()^+-".contains(&byte)
}

/// casacore's `Unit::check` renaming: `**` and `^` dropped, `*` as `.`,
/// blanks as `.` (or `/` when a run of blanks holds one), `//` as `.`, runs of
/// dots collapsed (to `/` when they hold one) and leading and trailing dots
/// removed.
fn casacore_unit_name(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut first = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'^' => index += 1,
            b'*' if bytes.get(index + 1) == Some(&b'*') => index += 2,
            b'*' => {
                first.push(b'.');
                index += 1;
            }
            b' ' => {
                let mut separator = b'.';
                while matches!(bytes.get(index), Some(b' ' | b'/')) {
                    if bytes[index] == b'/' {
                        separator = b'/';
                    }
                    index += 1;
                }
                first.push(separator);
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                first.push(b'.');
                index += 2;
            }
            other => {
                first.push(other);
                index += 1;
            }
        }
    }
    let start = first.iter().position(|&byte| byte != b'.');
    let Some(start) = start else {
        return String::new();
    };
    let end = first
        .iter()
        .rposition(|&byte| byte != b'.')
        .unwrap_or(start)
        + 1;
    let mut name = Vec::with_capacity(end - start);
    let mut index = start;
    while index < end {
        if first[index] == b'.' {
            let mut separator = b'.';
            while index < end && matches!(first[index], b'.' | b'/') {
                if first[index] == b'/' {
                    separator = b'/';
                }
                index += 1;
            }
            name.push(separator);
        } else {
            name.push(first[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&name).into_owned()
}

/// casacore's `MVTime(yy, mm, dd)`: the MJD of a calendar date, Julian before
/// 1582-10-15 and Gregorian from then on, with no range checks.
fn calendar_mjd(year: i32, month: i32, day: f64) -> f64 {
    let (year, month) = if month < 3 {
        (year - 1, month + 12)
    } else {
        (year, month)
    };
    let mut gregorian = 0;
    if year > 1582 || (year == 1582 && (month > 10 || (month == 10 && day >= 15.0))) {
        let century = (f64::from(year) / 100.0).floor() as i32;
        gregorian = 2 - century + century / 4;
    }
    (365.25 * f64::from(year)).floor() + (30.6001 * f64::from(month + 1)).floor() + day - 679_006.0
        + f64::from(gregorian)
}

/// casacore's `MVAngle::read(Quantity&, MUString&, chk)`: an angle in
/// degrees, or `None` with the cursor where it was.
fn read_angle(cursor: &mut Cursor<'_>, check_end: bool) -> Option<f64> {
    cursor.skip_blank();
    cursor.push();
    let sign = cursor.get_sign();
    let mut value = f64::from(cursor.get_uint());
    // 1: degrees, 2: hours, 3: colon time, 4: dotted degrees.
    let mut form = 0;
    if cursor.test_char(b'.') {
        cursor.skip_one();
        let minutes = f64::from(cursor.get_uint());
        if cursor.test_char(b'.') {
            cursor.skip_one();
            value += minutes / 60.0 + cursor.get_double() / 3600.0;
            value *= sign;
            form = 4;
        }
    } else if cursor.t_skip_one_char_nc(b'd') {
        form = 1;
    } else if cursor.t_skip_one_char_nc(b'h') {
        form = 2;
    } else if cursor.t_skip_one_char(b':') {
        form = 3;
    }
    if matches!(form, 1..=3) {
        if cursor.test_char_nc(b'm') || cursor.test_char_nc(b':') {
            form = 0;
        } else {
            let separator = if form == 3 { b':' } else { b'm' };
            cursor.push();
            let minutes = f64::from(cursor.get_uint());
            if cursor.t_skip_one_char_nc(separator) {
                value += minutes / 60.0 + cursor.get_double() / 3600.0;
                if form != 3 {
                    cursor.t_skip_one_char_nc(b's');
                }
            } else if form == 3
                && (cursor.test_char(b'Z')
                    || (!cursor.test_char(b'.') && !cursor.test_char(b'/') && !cursor.test_alpha()))
            {
                value += minutes / 60.0;
            } else if !(form == 1
                && minutes == 0.0
                && !cursor.test_char(b'.')
                && !cursor.test_char(b'/'))
            {
                // `10d` alone is 10 degrees; anything else after the `d` or
                // `h` is not an angle.
                form = 0;
            }
            cursor.unpush();
            value *= sign;
        }
    }
    if check_end {
        cursor.skip_blank();
        if !cursor.eos() {
            form = 0;
        }
    }
    let degrees = match form {
        1 | 4 => value,
        // Hours (or a colon time) at 15 degrees per hour, computed as
        // casacore does: `Quantity(r/240., "h").getBaseValue()` degrees.
        2 | 3 => value / 240.0 * 3600.0,
        _ => {
            cursor.pop();
            return None;
        }
    };
    cursor.unpush();
    Some(degrees)
}

/// The time of day after a date: an angle as a fraction of a day.
fn read_day_fraction(cursor: &mut Cursor<'_>, check_end: bool) -> Option<f64> {
    read_angle(cursor, check_end).map(|degrees| degrees / 360.0)
}

/// casacore's `MVTime::read(Quantity&, MUString&, chk)`: a date-time in
/// days, or `None` with the cursor where it was.
fn read_time(cursor: &mut Cursor<'_>, check_end: bool, now_mjd: f64) -> Option<f64> {
    cursor.skip_blank();
    cursor.push();
    let sign = cursor.get_sign();
    let days = if cursor.t_skip_string_nc("today")
        || cursor.t_skip_string_nc("now")
        || cursor.test_char(b'/')
    {
        if cursor.t_skip_all(b'/') || cursor.t_skip_all(b'-') || cursor.t_skip_all(b' ') {
            let Some(fraction) = read_day_fraction(cursor, check_end) else {
                cursor.pop();
                return None;
            };
            fraction + f64::from(now_mjd as i32)
        } else {
            now_mjd
        }
    } else {
        let Some(days) = read_calendar(cursor, check_end) else {
            cursor.pop();
            return None;
        };
        days
    };
    let days = days * sign;
    if check_end {
        cursor.skip_blank();
        if !cursor.eos() {
            cursor.pop();
            return None;
        }
    }
    cursor.unpush();
    Some(days)
}

/// The date (and optional time) part of `MVTime::read`, after the sign.
fn read_calendar(cursor: &mut Cursor<'_>, check_end: bool) -> Option<f64> {
    let mut year = cursor.get_uint() as i32;
    let mut month = 0;
    let mut day = 0.0;
    // Whether the leading number is an MJD day rather than a year.
    let mut mjd_day = false;
    if cursor.test_char(b'-') || cursor.test_alpha() {
        if cursor.test_char(b'-') {
            cursor.skip_one();
        }
        if cursor.test_alpha() {
            let name = cursor.get_alpha();
            if name.len() < 3 {
                return None;
            }
            month = unique_prefix_index(&name, &MONTHS)? as i32 + 1;
        } else {
            month = if cursor.test_char(b'-') {
                1
            } else {
                cursor.get_uint() as i32
            };
            if !cursor.test_char(b'-') {
                return None;
            }
        }
        cursor.skip_all(b'-');
        let mut last = cursor.get_uint() as i32;
        if year > 1000 {
            // FITS order: year-month-day.
            day = f64::from(last);
        } else {
            // Day-month-year, with two-digit years in 1950-2049.
            if last < 50 {
                last += 2000;
            } else if last < 100 {
                last += 1900;
            }
            day = f64::from(year);
            year = last;
        }
    } else if cursor.test_char(b'/') {
        if cursor.freq_char(b'/') > 1 {
            cursor.skip_one();
            if cursor.test_char(b'/') {
                cursor.skip_one();
                month = 1;
            } else {
                month = cursor.get_uint() as i32;
                if !cursor.t_skip_all(b'/') {
                    return None;
                }
            }
            day = cursor.get_double();
        } else {
            mjd_day = true;
        }
    } else {
        return None;
    }
    let mut fraction = 0.0;
    if cursor.t_skip_all(b'/') || cursor.t_skip_all(b'-') || cursor.t_skip_all(b' ') {
        fraction = read_day_fraction(cursor, check_end)?;
    } else if cursor.t_skip_all(b'T') {
        fraction = read_day_fraction(cursor, false)?;
        // An ISO 8601 zone offset, or a UTC `Z`.
        if cursor.test_char(b'+') || cursor.test_char(b'-') {
            let zone_sign = cursor.get_sign();
            let mut hours = f64::from(cursor.get_uint());
            if cursor.t_skip_all(b':') {
                hours += f64::from(cursor.get_uint()) / 60.0;
            }
            fraction -= zone_sign * hours / 24.0;
        } else {
            cursor.t_skip_all(b'Z');
        }
    }
    Some(if mjd_day {
        fraction + f64::from(year)
    } else {
        fraction + calendar_mjd(year, month, day)
    })
}

/// A port of casacore's `MUString`: a byte cursor over a string with a stack
/// of saved positions.
struct Cursor<'a> {
    text: &'a [u8],
    position: usize,
    saved: Vec<usize>,
}

impl<'a> Cursor<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            text: text.as_bytes(),
            position: 0,
            saved: Vec::new(),
        }
    }

    fn push(&mut self) {
        self.saved.push(self.position);
    }

    fn pop(&mut self) {
        self.position = self.saved.pop().unwrap_or(0);
    }

    fn unpush(&mut self) {
        self.saved.pop();
    }

    fn eos(&self) -> bool {
        self.position >= self.text.len()
    }

    fn peek(&self) -> Option<u8> {
        self.text.get(self.position).copied()
    }

    fn rest(&self) -> &'a str {
        std::str::from_utf8(&self.text[self.position.min(self.text.len())..]).unwrap_or("")
    }

    fn test_char(&self, wanted: u8) -> bool {
        self.peek() == Some(wanted)
    }

    fn test_char_nc(&self, wanted: u8) -> bool {
        self.peek()
            .is_some_and(|byte| byte.eq_ignore_ascii_case(&wanted))
    }

    fn test_alpha(&self) -> bool {
        self.peek()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
    }

    fn test_num(&self) -> bool {
        self.peek().is_some_and(|byte| byte.is_ascii_digit())
    }

    /// `skipChar()`: one byte.
    fn skip_one(&mut self) {
        self.position = (self.position + 1).min(self.text.len());
    }

    /// `skipChar(ch)`: every repeat of `wanted`.
    fn skip_all(&mut self, wanted: u8) {
        while self.test_char(wanted) {
            self.position += 1;
        }
    }

    /// `tSkipChar(ch)`.
    fn t_skip_all(&mut self, wanted: u8) -> bool {
        let found = self.test_char(wanted);
        self.skip_all(wanted);
        found
    }

    fn t_skip_one_char(&mut self, wanted: u8) -> bool {
        let found = self.test_char(wanted);
        if found {
            self.position += 1;
        }
        found
    }

    fn t_skip_one_char_nc(&mut self, wanted: u8) -> bool {
        let found = self.test_char_nc(wanted);
        if found {
            self.position += 1;
        }
        found
    }

    /// `tSkipStringNC`: a case-insensitive match of all of `wanted`.
    fn t_skip_string_nc(&mut self, wanted: &str) -> bool {
        let end = self.position + wanted.len();
        let found = self
            .text
            .get(self.position..end)
            .is_some_and(|found| found.eq_ignore_ascii_case(wanted.as_bytes()));
        if found {
            self.position = end;
        }
        found
    }

    fn skip_blank(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t')) {
            self.position += 1;
        }
    }

    /// `getSign`: the product of every leading sign.
    fn get_sign(&mut self) -> f64 {
        let mut sign = 1.0;
        while let Some(byte @ (b'+' | b'-')) = self.peek() {
            if byte == b'-' {
                sign = -sign;
            }
            self.position += 1;
        }
        sign
    }

    /// `getuInt`: leading digits, 0 if there are none.
    fn get_uint(&mut self) -> u32 {
        let mut value: u32 = 0;
        while self.test_num() {
            value = value
                .wrapping_mul(10)
                .wrapping_add(u32::from(self.text[self.position] - b'0'));
            self.position += 1;
        }
        value
    }

    /// `getDouble`: the longest
    /// `[-+]?(([0-9]+\.[0-9]*)|([0-9]+)|(\.[0-9]+))([eE][+-]?[0-9]+)?` at the
    /// cursor, 0 (consuming nothing) if there is none.
    fn get_double(&mut self) -> f64 {
        let digits_from = |index: usize| {
            self.text[index.min(self.text.len())..]
                .iter()
                .take_while(|byte| byte.is_ascii_digit())
                .count()
        };
        let start = self.position;
        let mut end = start;
        if matches!(self.text.get(end), Some(b'+' | b'-')) {
            end += 1;
        }
        let integer = digits_from(end);
        end += integer;
        if self.text.get(end) == Some(&b'.') {
            let fraction = digits_from(end + 1);
            if integer > 0 || fraction > 0 {
                end += 1 + fraction;
            }
        }
        if end == start || !self.text[start..end].iter().any(u8::is_ascii_digit) {
            return 0.0;
        }
        if matches!(self.text.get(end), Some(b'e' | b'E')) {
            let mut exponent = end + 1;
            if matches!(self.text.get(exponent), Some(b'+' | b'-')) {
                exponent += 1;
            }
            let digits = digits_from(exponent);
            if digits > 0 {
                end = exponent + digits;
            }
        }
        self.position = end;
        std::str::from_utf8(&self.text[start..end])
            .ok()
            .and_then(|number| number.parse().ok())
            .unwrap_or(0.0)
    }

    /// `getAlpha`: leading letters and underscores.
    fn get_alpha(&mut self) -> String {
        let start = self.position;
        while self.test_alpha() {
            self.position += 1;
        }
        String::from_utf8_lossy(&self.text[start..self.position]).into_owned()
    }

    /// `freqChar`: occurrences of `wanted` from the cursor on.
    fn freq_char(&self, wanted: u8) -> usize {
        self.text[self.position..]
            .iter()
            .filter(|&&byte| byte == wanted)
            .count()
    }
}

#[cfg(test)]
#[path = "read_tests.rs"]
mod tests;
