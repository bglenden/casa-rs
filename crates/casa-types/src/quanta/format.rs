// SPDX-License-Identifier: LGPL-3.0-or-later
//! casacore's angle and time formatting: `MVAngle::print`, `MVTime::print`
//! and their format names (`MVAngle::giveMe`, `MVTime::giveMe`), and
//! `MVTime`'s calendar fields.
//!
//! A format is a set of casacore's `formatTypes` flags, such as
//! [`TIME`]` | `[`YMD`] or [`ANGLE`]` | `[`DIG2`]. The precision counts
//! digits as casacore counts them: 0 is the stream default, 6, so an angle
//! prints as `+ddd.mm.ss` and a time as `hh:mm:ss`; 2 stops at degrees or
//! hours, 4 at minutes, and each digit above 6 adds a decimal to the
//! seconds. Values round to the last digit shown.
//!
//! This is the full counterpart of the convenience formatters on
//! [`MvAngle`](super::MvAngle) and [`MvTime`](super::MvTime), which keep
//! their own precision conventions.
//!
//! Divergence: `LOCAL` formats in UTC (a time-zone offset of zero), where
//! casacore uses the time zone of its `aipsrc` or of the system.
//!
//! ```
//! use casa_types::quanta::format::{ANGLE, TIME, YMD, format_angle, format_time, time_format};
//!
//! assert_eq!(format_angle(45.5_f64.to_radians(), ANGLE, 0), "+045.30.00");
//! assert_eq!(format_time(58849.5, TIME | YMD, 0), "2020/01/01/12:00:00");
//! assert_eq!(time_format("fits"), 2049);
//! ```

use super::read::unique_prefix_index;

/// Degrees and minutes separated by `.` (or hours and minutes by `:`).
pub const ANGLE: u32 = 0;
/// A time of day, `hh:mm:ss`.
pub const TIME: u32 = 1;
/// No `+` sign and no separators that end the string.
pub const CLEAN: u32 = 4;
/// No degrees (or hours).
pub const NO_D: u32 = 8;
/// No degrees and minutes (or hours and minutes).
pub const NO_DM: u32 = NO_D + 16;
/// A date `yyyy/mm/dd` before the time.
pub const YMD: u32 = TIME + 32;
/// A date `dd-Mon-yyyy` before the time.
pub const DMY: u32 = TIME + 64;
/// The day's name before the date or time.
pub const DAY: u32 = 128;
/// The date alone.
pub const NO_TIME: u32 = 256;
/// The MJD's day number before the time.
pub const MJD: u32 = TIME + 512;
/// Two digits of degrees, and a time of day in ±12 h.
pub const DIG2: u32 = 1024;
/// A FITS date-time, `yyyy-mm-ddThh:mm:ss`.
pub const FITS: u32 = TIME + 2048;
/// The local time zone (UTC here).
pub const LOCAL: u32 = 4096;
/// A blank between the date and the time.
pub const USE_SPACE: u32 = 8192;
/// Letters as separators: `12h30m00`, `+45d30m00`.
pub const ALPHA: u32 = 16384;
/// A trailing `Z`.
pub const USE_Z: u32 = 32768;
/// ISO 8601, `yyyy-mm-dd hh:mm:ssZ`.
pub const ISO: u32 = FITS + USE_Z + USE_SPACE + CLEAN;
/// Boost's format, `dd-Mon-yyyy hh:mm:ss`.
pub const BOOST: u32 = DMY + USE_SPACE;

/// The modifier flags of an angle format (`MVAngle::MOD_MASK`).
const ANGLE_MODIFIERS: u32 = CLEAN + NO_DM + DIG2 + LOCAL + USE_SPACE + ALPHA;
/// The modifier flags of a time format (`MVTime::MOD_MASK`).
const TIME_MODIFIERS: u32 =
    CLEAN + NO_DM + DAY + NO_TIME + DIG2 + LOCAL + USE_SPACE + USE_Z + ALPHA;

/// casacore's `C::circle`.
const CIRCLE: f64 = 2.0 * std::f64::consts::PI;
/// casacore's `C::degree`.
const DEGREE: f64 = std::f64::consts::PI / 180.0;
/// The precision a C++ stream uses by default.
const STREAM_PRECISION: u32 = 6;

const ANGLE_NAMES: [(&str, u32); 6] = [
    ("ANGLE", ANGLE),
    ("TIME", TIME),
    ("CLEAN", CLEAN),
    ("NO_D", NO_D),
    ("NO_DM", NO_DM),
    ("DIG2", DIG2),
];

const TIME_NAMES: [(&str, u32); 32] = [
    ("ANGLE", ANGLE),
    ("TIME", TIME),
    ("CLEAN", CLEAN),
    ("NO_D", NO_D),
    ("NO_DM", NO_DM),
    ("YMD", YMD),
    ("DMY", DMY),
    ("MJD", MJD),
    ("DAY", DAY),
    ("NO_TIME", NO_TIME),
    ("DIG2", DIG2),
    ("FITS", FITS),
    ("LOCAL", LOCAL),
    ("USE_SPACE", USE_SPACE),
    ("ALPHA", ALPHA),
    ("USE_Z", USE_Z),
    ("ISO", ISO),
    ("BOOST", BOOST),
    ("NO_H", NO_D),
    ("NO_HM", NO_DM),
    ("ANGLE_CLEAN", ANGLE + CLEAN),
    ("ANGLE_NO_D", ANGLE + NO_D),
    ("ANGLE_NO_DM", ANGLE + NO_DM),
    ("ANGLE_CLEAN_NO_D", ANGLE + CLEAN + NO_D),
    ("ANGLE_CLEAN_NO_DM", ANGLE + CLEAN + NO_DM),
    ("TIME_CLEAN", TIME + CLEAN),
    ("TIME_NO_H", TIME + NO_D),
    ("TIME_NO_HM", TIME + NO_DM),
    ("TIME_CLEAN_NO_H", TIME + CLEAN + NO_D),
    ("TIME_CLEAN_NO_HM", TIME + CLEAN + NO_DM),
    ("YMD_ONLY", YMD + NO_TIME),
    ("MOD_MASK", TIME_MODIFIERS),
];

fn named(name: &str, table: &[(&str, u32)]) -> u32 {
    let names = table.iter().map(|(name, _)| *name).collect::<Vec<_>>();
    unique_prefix_index(name, &names).map_or(0, |index| table[index].1)
}

/// The angle format a name gives (`MVAngle::giveMe`): one of `ANGLE`,
/// `TIME`, `CLEAN`, `NO_D`, `NO_DM` and `DIG2`, by unique case-insensitive
/// prefix; 0 (`ANGLE`) for any other name.
pub fn angle_format(name: &str) -> u32 {
    named(name, &ANGLE_NAMES)
}

/// The time format a name gives (`MVTime::giveMe`): one of casacore's 32
/// names (`TIME`, `YMD`, `DMY`, `FITS`, `ISO`, `NO_TIME`, ...), by unique
/// case-insensitive prefix; 0 (`ANGLE`) for any other name.
pub fn time_format(name: &str) -> u32 {
    named(name, &TIME_NAMES)
}

/// casacore's `ifloor`, which truncates `value - 1` for negative values, so
/// a negative whole number gives the one below it (`ifloor(-2.0)` is −3).
fn ifloor(value: f64) -> i64 {
    if value >= 0.0 {
        value as i64
    } else {
        (value - 1.0) as i64
    }
}

/// An angle in radians as `MVAngle::print` writes it (`MVAngle::string`).
pub fn format_angle(radians: f64, format: u32, precision: u32) -> String {
    let mut out = String::new();
    write_angle(&mut out, radians, format, precision);
    out
}

/// `MVAngle::print` (not in local time).
fn write_angle(out: &mut String, radians: f64, format: u32, precision: u32) {
    let flag = |bits: u32| format & bits == bits;
    let kind = format & !ANGLE_MODIFIERS;
    let (mut t, first_separator, second_separator) = if kind == ANGLE {
        let degrees = radians / DEGREE;
        if flag(ALPHA) {
            (degrees, 'd', 'm')
        } else {
            (degrees, '.', '.')
        }
    } else {
        let turns = radians / CIRCLE;
        let mut hours = (turns - turns.floor()) * 24.0;
        if flag(DIG2) && hours > 12.0 {
            hours -= 24.0;
        }
        if flag(ALPHA) {
            (hours, 'h', 'm')
        } else {
            (hours, ':', ':')
        }
    };
    let precision = if precision == 0 {
        STREAM_PRECISION
    } else {
        precision
    };
    let clean = flag(CLEAN);
    let mut unit = 1.0;
    if precision > 2 {
        unit /= 60.0;
    }
    if precision > 4 {
        unit /= 60.0;
    }
    if precision > 6 {
        unit /= 10.0_f64.powf(f64::from(precision - 6));
    }
    if kind == ANGLE || flag(DIG2) {
        if t < 0.0 {
            out.push('-');
        } else if !clean || flag(DIG2) {
            out.push('+');
        } else {
            out.push(' ');
        }
    }
    // casacore adds 0.1 of the last unit against rounding errors.
    t = (((t.abs() / unit + 0.5).floor() + 0.1) * unit).abs();
    let whole = ifloor(t);
    if !flag(NO_D) {
        if kind == ANGLE && !flag(DIG2) {
            if whole > 999 {
                out.push_str("***");
            } else if clean {
                out.push_str(&format!("{whole:>3}"));
            } else {
                out.push_str(&format!("{whole:03}"));
            }
        } else if whole > 99 {
            out.push_str("**");
        } else if kind == ANGLE && clean {
            out.push_str(&format!("{whole:>2}"));
        } else {
            out.push_str(&format!("{whole:02}"));
        }
        if precision > 2 || !clean {
            out.push(first_separator);
        }
    } else if !clean {
        out.push(first_separator);
    }
    if precision > 2 {
        t = (t % 1.0) * 60.0;
        if !flag(NO_DM) {
            out.push_str(&format!("{:02}", ifloor(t)));
            if precision > 4 || !clean {
                out.push(second_separator);
            }
        } else if !clean {
            out.push(second_separator);
        }
    } else if !clean {
        out.push(second_separator);
    }
    if precision > 4 && precision < 7 {
        t = (t % 1.0) * 60.0;
        out.push_str(&format!("{:02}", ifloor(t)));
    }
    if precision > 6 {
        t = (((t % 1.0) - 6.0 * unit) * 60.0).abs();
        if t <= 0.0 {
            t = 0.0;
        }
        let decimals = (precision - 6) as usize;
        let width = (precision - 3) as usize;
        out.push_str(&format!("{t:0width$.decimals$}"));
    }
    if flag(FITS) && flag(LOCAL) {
        // The time zone, `+hh:mm`: zero here.
        write_angle(out, 0.0, TIME + CLEAN + DIG2, 4);
    }
}

/// A time in MJD days as `MVTime::print` writes it (`MVTime::string`).
pub fn format_time(mjd_days: f64, format: u32, precision: u32) -> String {
    let flag = |bits: u32| format & bits == bits;
    let kind = format & !TIME_MODIFIERS;
    let day = ifloor(mjd_days) as f64;
    let mut angle = (mjd_days - day) * CIRCLE;
    // `MVAngle(angle)(0.0)`: the time of day in [0, 2π).
    let turns = angle / CIRCLE;
    if !(0.0..1.0).contains(&turns) {
        angle -= turns.floor() * CIRCLE;
    }
    let date = CalendarDate::of(day);
    let mut out = String::new();
    if flag(DAY) {
        out.push_str(DAY_NAMES[(date.weekday - 1) as usize]);
        if kind == YMD || kind == DMY || kind == MJD || !flag(NO_TIME) {
            out.push(if flag(USE_SPACE) { ' ' } else { '-' });
        }
    }
    if kind == YMD || kind == DMY || kind == FITS {
        let (year, month, day) = (date.year, date.month, date.monthday);
        out.push_str(&match kind {
            YMD => format!("{year:04}/{month:02}/{day:02}"),
            DMY => format!("{day:02}-{}-{year:04}", MONTH_NAMES[(month - 1) as usize]),
            _ => format!("{year:04}-{month:02}-{day:02}"),
        });
        if !flag(NO_TIME) {
            out.push(if flag(USE_SPACE) {
                ' '
            } else if kind == FITS {
                'T'
            } else {
                '/'
            });
        }
    }
    if kind == MJD {
        out.push_str(&format!("{}", ifloor(day)));
        if !flag(NO_TIME) {
            out.push('/');
        }
    }
    if !flag(NO_TIME) {
        write_angle(&mut out, angle, format, precision);
    }
    if flag(USE_Z) {
        out.push('Z');
    }
    out
}

const DAY_NAMES: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const MONTH_NAMES: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// The calendar fields of an MJD as `MVTime` gives them (Gregorian from
/// 1582-10-15, Julian before).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalendarDate {
    /// The year (`MVTime::year`).
    pub year: i64,
    /// The month, 1 to 12 (`MVTime::month`).
    pub month: i64,
    /// The day of the month (`MVTime::monthday`).
    pub monthday: i64,
    /// The day of the year, 1 to 366 (`MVTime::yearday`).
    pub yearday: i64,
    /// The day of the week, 1 (Monday) to 7 (`MVTime::weekday`).
    pub weekday: i64,
    /// The ISO-style week of the year (`MVTime::yearweek`).
    pub week: i64,
}

impl CalendarDate {
    /// The calendar fields of MJD `mjd_days`.
    pub fn of(mjd_days: f64) -> Self {
        let (year, month, monthday) = ymd(mjd_days);
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let correction = if leap {
            (month + 9) / 12
        } else {
            2 * ((month + 9) / 12)
        };
        let yearday = (275 * month) / 9 - correction + monthday - 30;
        let weekday = (ifloor(mjd_days + 2.0) % 7 + 7) % 7 + 1;
        // `MVTime::yearweek`, in C++'s truncating integer arithmetic.
        let mut offset = yearday - 4;
        let mut week = (offset + 7) / 7;
        offset %= 7;
        if (offset >= 0 && offset >= weekday) || (offset < 0 && offset + 7 >= weekday) {
            week += 1;
        }
        Self {
            year,
            month,
            monthday,
            yearday,
            weekday,
            week,
        }
    }
}

/// `MVTime::ymd`.
fn ymd(mjd_days: f64) -> (i64, i64, i64) {
    let z = ifloor(mjd_days + 2_400_001.0);
    let mut dd = z;
    if z >= 2_299_161 {
        let al = ifloor((z as f64 - 1_867_216.25) / 36_524.25);
        dd = z + 1 + al - al / 4;
    }
    dd += 1524;
    let mut year = ifloor((dd as f64 - 122.1) / 365.25);
    let d = ifloor(365.25 * year as f64);
    let mut month = ifloor((dd - d) as f64 / 30.6001);
    let day = dd - (d + ifloor(30.6001 * month as f64));
    if month < 14 {
        month -= 1;
    } else {
        month -= 13;
    }
    year -= 4715;
    if month > 2 {
        year -= 1;
    }
    (year, month, day)
}

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;
