// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calendar/time formatting helpers for Modified Julian Date values.
//!
//! This module provides [`MvTime`], a lightweight Rust counterpart to C++
//! `casacore::MVTime`. Like `MVTime`, it stores its internal value as an MJD
//! day count and offers calendar extraction plus simple human-readable
//! formatting. The initial Rust implementation focuses on the subset currently
//! needed by MeasurementSet summary/reporting code.

/// A calendar/time value stored as Modified Julian Date days.
///
/// This is the Rust counterpart to C++ `casacore::MVTime`. It intentionally
/// stores a single `f64` MJD day count, which is appropriate for formatting and
/// low-precision calendar conversion. For high-precision epoch arithmetic use
/// [`crate::measures::MjdHighPrec`] or [`crate::measures::MEpoch`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MvTime {
    mjd_days: f64,
}

impl MvTime {
    /// Creates an `MvTime` from Modified Julian Date days.
    pub const fn from_mjd_days(mjd_days: f64) -> Self {
        Self { mjd_days }
    }

    /// Creates an `MvTime` from Modified Julian Date seconds.
    pub fn from_mjd_seconds(mjd_seconds: f64) -> Self {
        Self {
            mjd_days: mjd_seconds / 86_400.0,
        }
    }

    /// Returns the stored Modified Julian Date in days.
    pub const fn as_mjd_days(self) -> f64 {
        self.mjd_days
    }

    /// Returns the stored Modified Julian Date in seconds.
    pub fn as_mjd_seconds(self) -> f64 {
        self.mjd_days * 86_400.0
    }

    /// Creates an `MvTime` from a UTC calendar date/time.
    ///
    /// This mirrors the role of C++ `casacore::MVTime(Int yy, Int mm, Double dd, Double d)`
    /// for the common Rust use case of converting parsed calendar fields into
    /// MJD-based formatting/selection values.
    pub fn from_ymd_hms_utc(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: f64,
    ) -> Option<Self> {
        if !(1..=12).contains(&month) {
            return None;
        }
        let max_day = days_in_month(year, month);
        if day == 0 || day > max_day || hour > 23 || minute > 59 || !(0.0..60.0).contains(&second) {
            return None;
        }

        const MJD_UNIX_EPOCH_DAYS: i64 = 40_587;
        let unix_days = days_from_civil(year, month, day);
        let mjd_days = unix_days + MJD_UNIX_EPOCH_DAYS;
        let fractional_day = (hour as f64 * 3_600.0 + minute as f64 * 60.0 + second) / 86_400.0;
        Some(Self::from_mjd_days(mjd_days as f64 + fractional_day))
    }

    /// Returns the calendar year.
    pub fn year(self) -> i32 {
        self.date_parts().year
    }

    /// Returns the month number in the range `1..=12`.
    pub fn month(self) -> u32 {
        self.date_parts().month
    }

    /// Returns the day-of-month in the range `1..=31`.
    pub fn month_day(self) -> u32 {
        self.date_parts().day
    }

    /// Formats the value as `dd-Mon-yyyy`.
    pub fn format_dmy_date(self) -> String {
        let parts = self.date_parts();
        format!(
            "{:02}-{}-{:04}",
            parts.day,
            month_abbrev(parts.month as usize),
            parts.year
        )
    }

    /// Formats the time portion as `hh:mm:ss[.f...]`.
    pub fn format_time(self, second_decimals: usize) -> String {
        let parts = self.time_parts(second_decimals);
        format_time_parts(parts, second_decimals)
    }

    /// Formats the value as `dd-Mon-yyyy/hh:mm:ss[.f...]`.
    pub fn format_dmy(self, second_decimals: usize) -> String {
        let date = self.date_parts();
        let time = format_time_parts(self.time_parts(second_decimals), second_decimals);
        format!(
            "{:02}-{}-{:04}/{}",
            date.day,
            month_abbrev(date.month as usize),
            date.year,
            time
        )
    }

    fn date_parts(self) -> DateParts {
        const MJD_UNIX_EPOCH_DAYS: i64 = 40_587;
        let mjd_days = self.mjd_days.floor() as i64;
        let unix_days = mjd_days - MJD_UNIX_EPOCH_DAYS;
        let (year, month, day) = civil_from_days(unix_days);

        DateParts { year, month, day }
    }

    fn time_parts(self, second_decimals: usize) -> TimeParts {
        let scale = 10_i64.pow(second_decimals as u32);
        let units_per_day = 86_400_i64 * scale;
        let day_floor = self.mjd_days.floor();
        let day_fraction = self.mjd_days - day_floor;
        let rem_units = (day_fraction * units_per_day as f64).round() as i64;

        let hour = (rem_units / (3_600 * scale)) as u32;
        let minute = ((rem_units / (60 * scale)) % 60) as u32;
        let second = ((rem_units / scale) % 60) as u32;
        let fractional_second = (rem_units % scale) as u32;

        TimeParts {
            hour,
            minute,
            second,
            fractional_second,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DateParts {
    year: i32,
    month: u32,
    day: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeParts {
    hour: u32,
    minute: u32,
    second: u32,
    fractional_second: u32,
}

fn format_time_parts(parts: TimeParts, second_decimals: usize) -> String {
    if second_decimals == 0 {
        return format!("{:02}:{:02}:{:02}", parts.hour, parts.minute, parts.second);
    }
    format!(
        "{:02}:{:02}:{:02}.{:0second_decimals$}",
        parts.hour, parts.minute, parts.second, parts.fractional_second,
    )
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i32 + era as i32 * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year, month as u32, day as u32)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year - if month <= 2 { 1 } else { 0 };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) as i64
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn month_abbrev(month: usize) -> &'static str {
    match month {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => "???",
    }
}

#[cfg(test)]
mod tests {
    use super::MvTime;

    #[test]
    fn formats_j2000_noon() {
        let time = MvTime::from_mjd_days(51_544.5);
        assert_eq!(time.year(), 2000);
        assert_eq!(time.month(), 1);
        assert_eq!(time.month_day(), 1);
        assert_eq!(time.format_dmy_date(), "01-Jan-2000");
        assert_eq!(time.format_time(1), "12:00:00.0");
        assert_eq!(time.format_dmy(1), "01-Jan-2000/12:00:00.0");
    }

    #[test]
    fn rounds_across_day_boundary() {
        let time = MvTime::from_mjd_days(51_544.0 + (86_399.96 / 86_400.0));
        assert_eq!(time.format_dmy(1), "01-Jan-2000/24:00:00.0");
        assert_eq!(time.format_dmy_date(), "01-Jan-2000");
    }

    #[test]
    fn formats_mjd_zero() {
        let time = MvTime::from_mjd_days(0.0);
        assert_eq!(time.format_dmy(1), "17-Nov-1858/00:00:00.0");
    }

    #[test]
    fn converts_calendar_to_mjd() {
        let time = MvTime::from_ymd_hms_utc(2000, 1, 1, 12, 0, 0.0).expect("valid calendar");
        assert!((time.as_mjd_days() - 51_544.5).abs() < 1e-10);
    }

    #[test]
    fn rejects_invalid_calendar_components() {
        assert!(MvTime::from_ymd_hms_utc(2000, 2, 30, 0, 0, 0.0).is_none());
        assert!(MvTime::from_ymd_hms_utc(2000, 13, 1, 0, 0, 0.0).is_none());
        assert!(MvTime::from_ymd_hms_utc(2000, 1, 1, 24, 0, 0.0).is_none());
    }
}
