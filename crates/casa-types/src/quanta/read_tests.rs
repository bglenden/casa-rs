// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tests of the quantity reader against CASA 6.7.6.
//!
//! The expected values are what CASA 6.7.6's `qa.quantity(text)` returned
//! (its quantity conversion is `QuantumHolder::fromString`, which is
//! `Quantity::read`), recorded 2026-10-21.

use super::*;

/// `(text, value, unit)` as CASA reads them.
const CASA_READS: &[(&str, f64, &str)] = &[
    // Value and unit.
    ("10s", 10.0, "s"),
    ("1.4GHz", 1.4, "GHz"),
    ("-30.5deg", -30.5, "deg"),
    ("50 MHz", 50.0, "MHz"),
    ("1e5Hz", 100000.0, "Hz"),
    ("1.5e-3 Jy", 0.0015, "Jy"),
    ("10", 10.0, ""),
    ("", 0.0, ""),
    ("Jy", 0.0, "Jy"),
    ("deg", 0.0, "deg"),
    ("10.d", 10.0, "d"),
    ("+5m", 5.0, "m"),
    (".5s", 0.5, "s"),
    ("5.", 5.0, ""),
    (" 3s", 3.0, "s"),
    ("1.4 GHz ", 1.4, "GHz"),
    ("0.5 km/s", 0.5, "km/s"),
    ("1e-6", 1e-06, ""),
    ("12h", 12.0, "h"),
    ("+12h", 12.0, "h"),
    ("10d30", 10.0, "d30"),
    ("J2000", 0.0, "J2000"),
    ("5 s", 5.0, "s"),
    ("1/s", 1.0, "/s"),
    ("10m/s", 10.0, "m/s"),
    ("1E3m", 1000.0, "m"),
    (".5", 0.5, ""),
    ("-.5deg", -0.5, "deg"),
    ("1d2", 1.0, "d2"),
    // Angles.
    ("10d", 10.0, "deg"),
    ("12h00m00.0", 180.0, "deg"),
    ("12h30m", 187.5, "deg"),
    ("45d00m00", 45.0, "deg"),
    ("45d30m", 45.5, "deg"),
    ("-30.00.00.5", -30.000138888888888, "deg"),
    ("30.15.20", 30.255555555555556, "deg"),
    ("12:30:00", 187.5, "deg"),
    ("12:30", 187.5, "deg"),
    ("-12:30:00.5", -187.50208333333333, "deg"),
    ("45d", 45.0, "deg"),
    ("10d30m15s", 10.504166666666666, "deg"),
    ("1h2m3s", 15.512500000000001, "deg"),
    ("12h30m15.5s", 187.56458333333333, "deg"),
    ("12:30:15.5", 187.56458333333333, "deg"),
    ("-0d30m00", -0.5, "deg"),
    ("12H30M", 187.5, "deg"),
    ("45D30M", 45.5, "deg"),
    ("12h00m00.0 ", 180.0, "deg"),
    ("1..5", 1.0013888888888889, "deg"),
    ("12:", 180.0, "deg"),
    // Date-times.
    ("2020/01/01/00:00:00", 58849.0, "d"),
    ("2020-01-01T00:00:00", 58849.0, "d"),
    ("2020/01/01", 58849.0, "d"),
    ("01Jan2020/12:00:00", 58849.5, "d"),
    ("2020-01-01T12:00:00+02:00", 58849.416666666664, "d"),
    ("2020-01-01T12:00:00-05:30", 58849.729166666664, "d"),
    ("2020-01-01T12:00:00Z", 58849.5, "d"),
    ("2020/01/01-12:00", 58849.5, "d"),
    ("2020-01-01", 58849.0, "d"),
    ("1-Jan-2020", 58849.0, "d"),
    ("01-jan-20", 58849.0, "d"),
    ("15-Jan-99", 51193.0, "d"),
    ("2020/01/01/12h30m", 58849.520833333336, "d"),
    ("2020/01/15.5", 58863.5, "d"),
    ("2020//15", 58863.0, "d"),
    ("2020-Jan-01/12:00", 58849.5, "d"),
    ("2020Jan01", 58849.0, "d"),
    ("1582/10/15", -100840.0, "d"),
    ("1582/10/04", -100841.0, "d"),
    ("1000/01/01", -313693.0, "d"),
    ("58849/12:00:00", 58849.5, "d"),
    ("2020/03/01/06:00:00.5", 58909.250005787035, "d"),
    ("1858/11/17", 0.0, "d"),
    ("2020/1/1/0:0:0", 58849.0, "d"),
    ("2020-02-30", 58909.0, "d"),
    ("01Mayday2020", 58970.0, "d"),
    ("2020/01/01T12:00", 58849.5, "d"),
    ("2020/13/01", 59215.0, "d"),
    ("2020/01/01/25:00:00", 58850.041666666664, "d"),
];

/// Strings CASA rejects.
const CASA_REJECTS: &[&str] = &[
    "12:30:00Z",
    "abc",
    "10 furlong",
    "12h00m00.0x",
    "1.4GHzz",
    "10xyz",
    "5\ts",
    "1e",
];

/// Strings CASA reads whose unit this crate's unit parser does not accept
/// (casacore's unit grammar allows repeated signs and a leading power before a
/// name, and `h:`): CASA reads `--5s` as 0 `--5s`, `1.4.5GHz` as
/// 1.4 `5GHz` and `12h:` as 12 `h:`.
const UNIT_PARSER_GAPS: &[&str] = &["--5s", "-+-5s", "1.4.5GHz", "12h:"];

fn close(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-12 * left.abs().max(right.abs()).max(1.0)
}

#[test]
fn strings_read_as_casa_reads_them() {
    for &(text, value, unit) in CASA_READS {
        let quantity = Quantity::read_at(text, 60_000.25)
            .unwrap_or_else(|error| panic!("{text:?} should read: {error}"));
        assert!(
            close(quantity.value(), value) && quantity.unit().name() == unit,
            "{text:?}: read {} {:?}, CASA {value} {unit:?}",
            quantity.value(),
            quantity.unit().name()
        );
    }
}

#[test]
fn strings_casa_rejects_are_rejected() {
    for &text in CASA_REJECTS {
        let error = Quantity::read_at(text, 60_000.25).expect_err(text);
        assert_eq!(error.input(), text);
    }
}

#[test]
fn unit_parser_gaps_are_rejected() {
    for &text in UNIT_PARSER_GAPS {
        assert!(Quantity::read(text).is_err(), "{text:?}");
    }
    // A long exponent saturates rather than overflowing.
    assert!(Quantity::read("1 J99999999999").is_ok());
}

#[test]
fn units_resolve_to_their_dimensions() {
    let speed = Quantity::read("0.5 km/s").unwrap();
    assert!(close(speed.get_si_value(), 500.0));
    let spaced = Quantity::read("2 km s-1").unwrap();
    assert_eq!(spaced.unit().name(), "km.s-1");
    assert!(close(spaced.get_si_value(), 2000.0));
    let angle = Quantity::read("12h30m").unwrap();
    assert!(close(angle.get_si_value(), 187.5_f64.to_radians()));
}

#[test]
fn today_and_now_use_the_given_clock() {
    let now = 60_000.75;
    let read = |text: &str| Quantity::read_at(text, now).unwrap();
    assert_eq!(
        (read("today").value(), read("today").unit().name()),
        (now, "d")
    );
    assert_eq!(read("NOW").value(), now);
    assert!(close(read("today/06:00:00").value(), 60_000.25));
    assert!(close(read("now 18:00").value(), 60_000.75));
    assert!(close(read("/12:00").value(), 60_000.5));
    assert!(Quantity::read_at("today/garbage", now).is_err());
}

#[test]
fn casacore_calendar_switches_at_the_gregorian_reform() {
    assert_eq!(calendar_mjd(1858, 11, 17.0), 0.0);
    assert_eq!(calendar_mjd(2020, 1, 1.0), 58849.0);
    // Julian 1582-10-04 is the day before Gregorian 1582-10-15.
    assert_eq!(
        calendar_mjd(1582, 10, 15.0) - calendar_mjd(1582, 10, 4.0),
        1.0
    );
    assert_eq!(calendar_mjd(2020, 2, 30.0), calendar_mjd(2020, 3, 1.0));
}

#[test]
fn unique_prefixes_follow_minimax() {
    let epochs = [
        "LAST", "LMST", "GMST1", "GAST", "UT1", "UT2", "UTC", "TAI", "TDT", "TCG", "TDB", "TCB",
        "IAT", "GMST", "TT", "ET", "UT",
    ];
    assert_eq!(unique_prefix_index("utc", &epochs), Some(6));
    assert_eq!(unique_prefix_index("UT", &epochs), Some(16));
    assert_eq!(unique_prefix_index("U", &epochs), None);
    assert_eq!(unique_prefix_index("LA", &epochs), Some(0));
    assert_eq!(unique_prefix_index("BOGUS", &epochs), None);
    assert_eq!(unique_prefix_index("Jan", &MONTHS), Some(0));
    assert_eq!(unique_prefix_index("Mayday", &MONTHS), Some(4));
    assert_eq!(unique_prefix_index("Ju", &MONTHS), None);
    assert_eq!(unique_prefix_index("", &["ONLY"]), Some(0));
}

#[test]
fn unit_names_follow_casacore() {
    assert_eq!(casacore_unit_name(" MHz"), "MHz");
    assert_eq!(casacore_unit_name("km s-1"), "km.s-1");
    assert_eq!(casacore_unit_name("m**2"), "m2");
    assert_eq!(casacore_unit_name("m^2"), "m2");
    assert_eq!(casacore_unit_name("kg*m"), "kg.m");
    assert_eq!(casacore_unit_name("m / s"), "m/s");
    assert_eq!(casacore_unit_name("m//s"), "m.s");
    assert_eq!(casacore_unit_name(".5GHz"), "5GHz");
    assert_eq!(casacore_unit_name("Jy..beam"), "Jy.beam");
    assert_eq!(casacore_unit_name("   "), "");
}
