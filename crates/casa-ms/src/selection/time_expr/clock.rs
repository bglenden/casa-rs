// SPDX-License-Identifier: LGPL-3.0-or-later
//! The calendar arithmetic `MSTimeParse` does: casacore's `Time` class
//! (`casa/OS/Time.cc`), `MVEpoch`'s whole-and-fractional day sum
//! (`casa/Quanta/MVEpoch.cc`) and `MVTime`'s days and seconds
//! (`casa/Quanta/MVTime.cc`).
//!
//! Every operation is the C++ one in IEEE double precision, unfused, in the
//! same order, so a default or a bound comes out bit for bit as CASA's x86-64
//! build computes it. Where the C++ converts a double that is out of range for
//! an unsigned integer (undefined behaviour, which differs between CASA's
//! platforms) the port saturates, as Rust's casts and CASA's arm64 build do.

use casa_types::quanta::format::CalendarDate;

use super::grammar::{TimeFields, UNSET};

/// Seconds in a day, casacore's `C::day`.
const DAY_S: f64 = 86_400.0;

/// casacore `Time`: an MJD day number held in a C `uInt` and a day fraction.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct CasacoreTime {
    day: u32,
    fraction: f64,
}

impl CasacoreTime {
    /// `Time::Time(double jdn)` (Time.cc:61-72), or `None` where its `(int)`
    /// cast of `jdn` would be undefined.
    pub(super) fn from_julian_day(jdn: f64) -> Option<Self> {
        if !(jdn > -2_147_483_649.0 && jdn < 2_147_483_648.0) {
            return None;
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "C's (int) cast truncates; the range is checked above"
        )]
        let whole = jdn as i32;
        let fraction = jdn - f64::from(whole);
        // `mJulianDay` is a uInt; an int below the offset wraps.
        Some(if fraction >= 0.5 {
            Self {
                day: whole.wrapping_sub(2_400_000).cast_unsigned(),
                fraction: fraction - 0.5,
            }
        } else {
            Self {
                day: whole.wrapping_sub(2_400_001).cast_unsigned(),
                fraction: fraction + 0.5,
            }
        })
    }

    /// `Time::Time(uInt year, uInt month, uInt day, uInt hour, uInt min,
    /// double sec)`, which runs `Time::setDate` (Time.cc:344-381). The `Int`
    /// fields of the caller pass through `uInt` parameters unchanged in their
    /// bits. `None` where the C `int` day-number arithmetic would overflow
    /// (undefined behaviour), which only a year beyond about 1.4 million does.
    ///
    /// The hour, minute and second are not range-checked, so 25:00 or 60 s
    /// roll into the next day or minute, as they do in CASA.
    pub(super) fn from_date(
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        minute: i32,
        sec: f64,
    ) -> Option<Self> {
        let (y, m, d) = (year, month, day);
        // md = (1461*(y+4800+(m-14)/12))/4+(367*(m-2-12*((m-14)/12)))/12
        //      -(3*((y+4900+(m-14)/12)/100))/4+d-32075, in C int arithmetic.
        let a = m.checked_sub(14)? / 12;
        let years = 1461_i32.checked_mul(y.checked_add(4800)?.checked_add(a)?)? / 4;
        let months =
            367_i32.checked_mul(m.checked_sub(2)?.checked_sub(12_i32.checked_mul(a)?)?)? / 12;
        let centuries = 3_i32.checked_mul(y.checked_add(4900)?.checked_add(a)? / 100)? / 4;
        let md = years
            .checked_add(months)?
            .checked_sub(centuries)?
            .checked_add(d)?
            .checked_sub(32_075)?;
        let fraction = f64::from(hour.cast_unsigned()) / 24.0
            + f64::from(minute.cast_unsigned()) / 1440.0
            + sec / DAY_S;
        Some(Self {
            // `uInt md` and `mJulianDay = md - 2400001` in uInt arithmetic.
            day: md.cast_unsigned().wrapping_sub(2_400_001),
            fraction,
        })
    }

    /// `Time::modifiedJulianDay` (Time.cc:91-95).
    pub(super) fn modified_julian_day(self) -> f64 {
        f64::from(self.day) + self.fraction
    }

    /// `Time::hours` (Time.cc:419-423).
    fn hours(self) -> u32 {
        uint(self.fraction * 24.0 + 2e-5 / 3600.0)
    }

    /// `Time::minutes` (Time.cc:411-417).
    fn minutes(self) -> u32 {
        let hour = self.fraction * 24.0;
        let minutes = (hour - f64::from(self.hours())) * 60.0;
        uint(minutes + 2e-5 / 60.0)
    }

    /// `Time::seconds` (Time.cc:398-402) of `Time::dseconds` (Time.cc:404-409).
    fn seconds(self) -> u32 {
        let hour = self.fraction * 24.0;
        let seconds = (hour - f64::from(self.hours())) * 3600.0 - f64::from(self.minutes()) * 60.0;
        uint(seconds + 2e-5)
    }

    /// `Time::operator-` (Time.cc:104-119): `self - begin` in seconds.
    fn seconds_since(self, begin: Self) -> f64 {
        let mut fraction = self.fraction - begin.fraction;
        let days = if fraction < 0.0 {
            fraction += 1.0;
            f64::from(self.day) - f64::from(begin.day) - 1.0
        } else {
            f64::from(self.day) - f64::from(begin.day)
        };
        (days + fraction) * DAY_S
    }
}

/// C++'s `uInt(x)` for the values `Time`'s accessors convert. A negative or
/// too large `x` is undefined behaviour there; Rust's cast saturates.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "C's uInt cast truncates toward zero"
)]
fn uint(value: f64) -> u32 {
    value as u32
}

/// `MVEpoch(inday, infrac).get()` (MVEpoch.cc:54-59, 156-172, 249-253): the
/// whole days and the fractions of both arguments summed apart, then added.
pub(super) fn epoch_days(day: f64, fraction: f64) -> f64 {
    let mut whole = 0.0;
    let mut part = 0.0;
    for value in [day, fraction] {
        let floor = value.floor();
        whole += floor;
        part += value - floor;
    }
    // `adjust()` loops, but for the finite days here `part` is in [0, 2), so
    // its loops run at most once.
    if part < 0.0 {
        part += 1.0;
        whole -= 1.0;
    }
    if part >= 1.0 {
        part -= 1.0;
        whole += 1.0;
    }
    whole + part
}

/// `MSTimeParse::toTAIInSec` (MSTimeParse.cc:339-344): `MVTime(epoch) *
/// 86400`. Its conversion to TAI is commented out, so the seconds stay in the
/// epoch's own scale, which is that of the `TIME` column they are compared to.
pub(super) fn epoch_seconds(days: f64) -> f64 {
    days * DAY_S
}

/// The fields and exposure `MSTimeParse::getDefaults` (MSTimeParse.cc:114-184)
/// derives from one MAIN row's `TIME` and `EXPOSURE`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct RowDefaults {
    pub(super) fields: TimeFields,
    pub(super) exposure_s: f64,
}

/// `getDefaults` for a row with `time_s` (MJD seconds) and `exposure_s`, or
/// `None` where the `TIME` is outside the days casacore's `Time` converts.
pub(super) fn row_defaults(time_s: f64, exposure_s: f64) -> Option<RowDefaults> {
    // `MVTime(timeQuant()(row))`: the base value in seconds over `C::day`.
    let days = time_s / DAY_S;
    // `Time t0(firstRowTime.getTime())`, `MVTime::getTime` (MVTime.cc:130-132).
    let first = CasacoreTime::from_julian_day(days + 2_400_000.5)?;
    // `MVTime::year`, `month` and `monthday` (MVTime.cc:160-176, 209-232).
    let date = CalendarDate::of(days);
    let year = i32::try_from(date.year).ok()?;
    let month = i32::try_from(date.month).ok()?;
    let day = i32::try_from(date.monthday).ok()?;
    let hour = first.hours().cast_signed();
    let minute = first.minutes().cast_signed();
    let sec = first.seconds().cast_signed();
    let whole = CasacoreTime::from_date(year, month, day, hour, minute, f64::from(sec))?;
    // `defaultFractionalSec = (Int)((t0-t1)*1E3)`.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "C's (Int) cast truncates toward zero"
    )]
    let millisec = (first.seconds_since(whole) * 1e3) as i32;
    Some(RowDefaults {
        fields: TimeFields {
            year,
            month,
            day,
            hour,
            minute,
            sec,
            millisec,
        },
        exposure_s,
    })
}

/// `MSTimeParse::setDefaults(tf, True)` (MSTimeParse.cc:357-367).
pub(super) fn fill_from_row(fields: TimeFields, row: &RowDefaults) -> TimeFields {
    fill(fields, row.fields)
}

/// `MSTimeParse::setDefaults(tf, False)` (MSTimeParse.cc:368-382): the MJD
/// reference date (`Time(2400000.5)`, 1858/11/17) and zero time of day.
pub(super) fn fill_from_mjd_reference(fields: TimeFields) -> TimeFields {
    fill(
        fields,
        TimeFields {
            year: 1858,
            month: 11,
            day: 17,
            hour: 0,
            minute: 0,
            sec: 0,
            millisec: 0,
        },
    )
}

/// `MSTimeParse::copyDefaults(target, source)` (MSTimeParse.cc:415-424).
pub(super) fn fill(target: TimeFields, source: TimeFields) -> TimeFields {
    let pick = |value: i32, default: i32| if value == UNSET { default } else { value };
    TimeFields {
        year: pick(target.year, source.year),
        month: pick(target.month, source.month),
        day: pick(target.day, source.day),
        hour: pick(target.hour, source.hour),
        minute: pick(target.minute, source.minute),
        sec: pick(target.sec, source.sec),
        millisec: pick(target.millisec, source.millisec),
    }
}

/// The `Time` of complete fields: `Double s = Double(tf.sec) +
/// Double(tf.fsec)/1000.0; Time t(...)` (MSTimeParse.cc:322-323).
pub(super) fn time_of(fields: TimeFields) -> Option<CasacoreTime> {
    let sec = f64::from(fields.sec) + f64::from(fields.millisec) / 1000.0;
    CasacoreTime::from_date(
        fields.year,
        fields.month,
        fields.day,
        fields.hour,
        fields.minute,
        sec,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY_57813_S: f64 = 57_813.0 * DAY_S;

    #[test]
    fn set_date_counts_days_from_the_mjd_epoch() {
        // 1858/11/17 is MJD 0; 2017/03/01 is MJD 57813.
        let epoch = CasacoreTime::from_date(1858, 11, 17, 0, 0, 0.0).unwrap();
        assert_eq!(epoch.modified_julian_day(), 0.0);
        let day = CasacoreTime::from_date(2017, 3, 1, 9, 0, 0.0).unwrap();
        assert_eq!(day.modified_julian_day(), 57_813.375);
        // Fields past their range roll over: Feb 29 2017 is Mar 1, 24 h the next day.
        let rolled = CasacoreTime::from_date(2017, 2, 29, 24, 0, 0.0).unwrap();
        assert_eq!(rolled.modified_julian_day(), 57_814.0);
        // A date before MJD 0 wraps the uInt day number, as in C++.
        let early = CasacoreTime::from_date(1858, 11, 16, 0, 0, 0.0).unwrap();
        assert_eq!(early.modified_julian_day(), f64::from(u32::MAX));
        assert!(CasacoreTime::from_date(2_000_000, 1, 1, 0, 0, 0.0).is_none());
    }

    #[test]
    fn epoch_days_sums_whole_days_and_fractions_apart() {
        assert_eq!(epoch_days(57_813.375, 0.0), 57_813.375);
        assert_eq!(epoch_days(57_813.75, 0.5), 57_814.25);
        assert_eq!(epoch_days(57_813.375, 2.0), 57_815.375);
    }

    #[test]
    fn defaults_split_the_first_row_time_into_casas_fields() {
        // 2017/03/01/09:00:00.5, EXPOSURE 2 s.
        let row = row_defaults(DAY_57813_S + 32_400.5, 2.0).unwrap();
        assert_eq!(
            row.fields,
            TimeFields {
                year: 2017,
                month: 3,
                day: 1,
                hour: 9,
                minute: 0,
                sec: 0,
                millisec: 500,
            }
        );
        assert_eq!(row.exposure_s, 2.0);
        // The milliseconds truncate: 0.1234 s keeps 123 ms.
        let row = row_defaults(DAY_57813_S + 32_400.123_4, 2.0).unwrap();
        assert_eq!((row.fields.sec, row.fields.millisec), (0, 123));
        // 23:59:59.75 on 2017/02/28.
        let row = row_defaults(DAY_57813_S - 0.25, 1.0).unwrap();
        assert_eq!(
            (row.fields.year, row.fields.month, row.fields.day),
            (2017, 2, 28)
        );
        assert_eq!(
            (row.fields.hour, row.fields.minute, row.fields.sec),
            (23, 59, 59)
        );
        assert!(row_defaults(f64::NAN, 1.0).is_none());
        assert!(row_defaults(1e20, 1.0).is_none());
    }

    #[test]
    fn reference_defaults_are_mjd_zero() {
        let fields = fill_from_mjd_reference(TimeFields {
            year: UNSET,
            month: UNSET,
            day: UNSET,
            hour: UNSET,
            minute: 30,
            sec: UNSET,
            millisec: UNSET,
        });
        let time = time_of(fields).unwrap();
        assert_eq!(time.modified_julian_day(), 30.0 / 1440.0);
    }
}
