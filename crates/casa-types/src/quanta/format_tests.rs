// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tests of casacore's angle and time formatting, with expectations from
//! CASA 6.7.6's `qa.angle`, `qa.time` and `qa.splitdate` (probed
//! 2026-10-21).

use super::*;
use crate::quanta::Quantity;

/// An angle as `qa.angle` reads it: the quantity's base value.
fn radians(text: &str) -> f64 {
    let angle = Quantity::read(text).unwrap();
    let base = angle.get_si_value();
    if angle.unit().val().dim == crate::quanta::UnitDim::basis(crate::quanta::Dimension::Time) {
        base * (CIRCLE / 86_400.0)
    } else {
        base
    }
}

/// A time as `qa.time` reads it (`MVTime(Quantity)`): days, or turns of an
/// angle.
fn days(text: &str) -> f64 {
    let time = Quantity::read(text).unwrap();
    let base = time.get_si_value();
    if time.unit().val().dim == crate::quanta::UnitDim::basis(crate::quanta::Dimension::Angle) {
        base / CIRCLE
    } else {
        base / 86_400.0
    }
}

/// `makeFormA` and `makeFormT`: the named flags added to the default.
fn angle_form(names: &[&str]) -> u32 {
    names.iter().fold(angle_format("angle"), |form, name| {
        form | angle_format(name)
    })
}

fn time_form(names: &[&str]) -> u32 {
    names
        .iter()
        .fold(time_format("time"), |form, name| form | time_format(name))
}

#[test]
fn angles_format_as_casa_formats_them() {
    for (text, form, precision, expected) in [
        ("45.5deg", &[][..], 0, "+045.30.00"),
        ("45.5deg", &[], 8, "+045.30.00.00"),
        ("190deg", &["dig2"], 0, "+**.00.00"),
        ("190deg", &["time"], 0, "12:40:00"),
        ("190deg", &["clean"], 0, " 190.00.00"),
        ("45.5deg", &["no_d"], 0, "+.30.00"),
        ("-45.5deg", &[], 0, "-045.30.00"),
        ("190deg", &[], 0, "+190.00.00"),
        ("1rad", &[], 7, "+057.17.44.8"),
        ("12h", &[], 0, "+180.00.00"),
        ("45.5deg", &[], 2, "+046.."),
        ("45.5deg", &[], 4, "+045.30."),
        ("45.5deg", &["clean"], 6, "  45.30.00"),
        ("45.5deg", &["clean", "no_d"], 6, " 30.00"),
        ("-0.0001deg", &[], 6, "-000.00.00"),
        ("359.99999deg", &[], 6, "+360.00.00"),
        ("1000deg", &[], 6, "+***.00.00"),
        ("45.5deg", &["time"], 9, "03:02:00.000"),
        ("45.5deg", &["dig2", "clean"], 6, "+45.30.00"),
        ("45.5deg", &["no_dm"], 8, "+..00.00"),
    ] {
        assert_eq!(
            format_angle(radians(text), angle_form(form), precision),
            expected,
            "{text} {form:?} {precision}"
        );
    }
}

#[test]
fn times_format_as_casa_formats_them() {
    let date = "2020/01/01/12:30:15.5";
    for (text, form, precision, expected) in [
        ("12h30m15.5s", &[][..], 0, "12:30:16"),
        ("12h30m15.5s", &[], 6, "12:30:16"),
        ("12h30m15.123456789s", &[], 9, "12:30:15.123"),
        (date, &["ymd"], 0, "2020/01/01/12:30:15"),
        (date, &["dmy"], 0, "01-Jan-2020/12:30:15"),
        (date, &["fits"], 7, "2020-01-01T12:30:15.5"),
        (date, &["ymd", "no_time"], 0, "2020/01/01"),
        (date, &["day"], 0, "Wed-12:30:15"),
        (date, &["mjd"], 0, "58849/12:30:15"),
        (date, &["iso"], 0, "2020-01-01 12:30:15Z"),
        (date, &["boost"], 0, "01-Jan-2020 12:30:15"),
        (date, &["ymd", "clean"], 0, "2020/01/01/12:30:15"),
        (date, &["ymd", "day"], 0, "Wed-2020/01/01/12:30:15"),
        (date, &["ymd", "use_space"], 0, "2020/01/01 12:30:15"),
        ("58849.5d", &["ymd"], 0, "2020/01/01/12:00:00"),
        ("12h30m", &[], 0, "12:30:00"),
        ("45deg", &[], 0, "03:00:00"),
        ("-1h", &[], 0, "23:00:00"),
        ("25h", &[], 0, "01:00:00"),
        ("12h30m15.5s", &["alpha"], 0, "12h30m16"),
        ("45.5deg", &["alpha"], 0, "03h02m00"),
        ("12h30m15.5s", &["time", "dig2"], 0, "-11:29:44"),
        ("-1h", &["dig2"], 0, "-01:00:00"),
        ("12h30m15.5s", &[], 2, "13::"),
        ("12h30m15.5s", &[], 4, "12:30:"),
        ("12h30m15.5s", &[], 3, "12:30:"),
        ("12h30m15.5s", &["clean"], 2, "13"),
        ("12h30m15.5s", &["no_dm"], 6, "::16"),
        ("12h30m15.5s", &["time", "no_d"], 6, ":30:16"),
        ("12h30m15.5s", &["time", "clean"], 6, "12:30:16"),
    ] {
        assert_eq!(
            format_time(days(text), time_form(form), precision),
            expected,
            "{text} {form:?} {precision}"
        );
    }
}

#[test]
fn local_times_are_utc() {
    let date = days("2020/01/01/12:30:15.5");
    assert_eq!(
        format_time(date, time_form(&["fits", "local"]), 0),
        "2020-01-01T12:30:15+00:00"
    );
}

#[test]
fn format_names_resolve_by_unique_prefix() {
    assert_eq!(time_format("fits"), FITS);
    assert_eq!(time_format("ymd"), YMD);
    assert_eq!(time_format("YMD_ONLY"), YMD + NO_TIME);
    // casacore also counts a name that is a prefix of the input: YMD.
    assert_eq!(time_format("YMD_O"), 0);
    assert_eq!(time_format("no"), 0, "NO_D, NO_DM, NO_TIME, NO_H and NO_HM");
    assert_eq!(time_format("bogus"), 0);
    assert_eq!(angle_format("dig"), DIG2);
    assert_eq!(angle_format("no_dm"), NO_DM);
    assert_eq!(angle_format("ymd"), 0, "not an angle format");
}

#[test]
fn calendar_fields_follow_mvtime() {
    for (text, year, month, monthday, yearday, week, weekday) in [
        ("2020/02/29", 2020, 2, 29, 60, 9, 6),
        ("2020/01/01", 2020, 1, 1, 1, 1, 3),
        ("2020/12/31", 2020, 12, 31, 366, 53, 4),
        ("2021/01/01", 2021, 1, 1, 1, 0, 5),
        ("2021/01/03", 2021, 1, 3, 3, 0, 7),
        ("2021/01/04", 2021, 1, 4, 4, 1, 1),
        ("2019-12-30", 2019, 12, 30, 364, 53, 1),
        ("2016/01/01", 2016, 1, 1, 1, 0, 5),
        ("2000/03/01", 2000, 3, 1, 61, 9, 3),
        ("1582/10/15", 1582, 10, 15, 288, 42, 4),
        ("1582/10/04", 1582, 10, 4, 277, 40, 3),
        ("1900/02/28", 1900, 2, 28, 59, 9, 3),
    ] {
        let date = CalendarDate::of(days(text));
        assert_eq!(
            date,
            CalendarDate {
                year,
                month,
                monthday,
                yearday,
                weekday,
                week,
            },
            "{text}"
        );
    }
}
