// SPDX-License-Identifier: LGPL-3.0-or-later
//! The expected strings are CASA 6.7.6's (`tools/simulation/simutil_oracle.py`;
//! casa-simulation's `simutil_casa.json` holds the whole grid).

use super::{add, convert, div, formxxx, mul, mvangle_normalized, stream_double, sub, tos};
use crate::quanta::{Quantity, Unit};

fn read(text: &str) -> Quantity {
    Quantity::read(text).expect("a quantity")
}

#[test]
fn formxxx_writes_casas_strings() {
    for (value, format, precision, written) in [
        ("18h00m00.031s", "hms", 5, "18:00:00.03100"),
        ("-22d59m59.6s", "dms", 5, "-022.59.59.600000"),
        ("-0.5deg", "hms", 5, "23:58:00.00000"),
        ("-0.5deg", "+deg", 2, "359.5"),
        ("359.9999999deg", "dms", 5, "-000.00.00.000360"),
        ("-180deg", "dms", 2, "-180.00.00.000"),
        ("180deg", "dms", 2, "-180.00.00.000"),
        ("24h", "hms", 5, "00:00:00.00000"),
        ("-1h", "hms", 2, "23:00:00.00"),
        ("1e-9rad", "dms", 9, "+000.00.00.0002062648"),
        ("3.14159265358979rad", "rad", 2, "3.14159"),
        ("45d30m00.0000005", "dms", 9, "+045.30.00.0000005000"),
        ("86399.9999999s", "hms", 5, "24:00:00.00000"),
        ("-89.99999999deg", "dms", 5, "-089.59.59.999964"),
        ("90deg", "dms", 0, "+090.00.00.0"),
        ("720.25deg", "deg", 2, "0.25"),
        ("720.25deg", "DMS", 2, "+000.15.00.000"),
    ] {
        assert_eq!(
            formxxx(&read(value), format, precision).unwrap().as_deref(),
            Some(written),
            "{value} {format} {precision}"
        );
    }
    assert_eq!(formxxx(&read("0.5deg"), "xyz", 2).unwrap(), None);
    assert!(formxxx(&read("1Jy"), "hms", 2).is_err());
}

#[test]
fn tos_writes_fixed_decimals_and_the_unit() {
    for (value, precision, written) in [
        ("100GHz", 9, "100.000000000GHz"),
        ("100GHz", 15, "100.000000000000000GHz"),
        ("229.98GHz", 0, "230GHz"),
        ("1.25e9Hz", 2, "1250000000.00Hz"),
        ("-0.5arcsec", 9, "-0.500000000arcsec"),
        ("7200s", 0, "7200s"),
        ("0.1", 2, "0.10"),
        ("1e-12Jy", 9, "0.000000000Jy"),
        ("1e-12Jy", 15, "0.000000000001000Jy"),
        ("12345678.9Hz", 0, "12345679Hz"),
    ] {
        assert_eq!(tos(&read(value), precision), written, "{value} {precision}");
    }
}

#[test]
fn sums_convert_the_second_value_into_the_first_unit() {
    let sum = add(&read("1km"), &read("1m")).unwrap();
    assert_eq!((sum.value(), sum.unit().name()), (1.001, "km"));
    let difference = sub(&read("10deg"), &read("1800arcsec")).unwrap();
    assert_eq!(difference.unit().name(), "deg");
    // casacore divides by the factor ratio, which rounds unlike a product.
    let arcsec = read("0.1arcsec");
    let deg = Unit::new("deg").unwrap();
    let ratio = deg.val().factor / arcsec.unit().val().factor;
    assert_eq!(difference.value(), 10.0 - 1800.0 / ratio);
    assert!(add(&read("1km"), &read("1s")).is_err());
}

#[test]
fn products_and_quotients_name_their_units_as_casacore_does() {
    let product = mul(&Quantity::dimensionless(0.866025404), &read("10arcsec")).unwrap();
    assert_eq!(product.unit().name(), "arcsec");
    let quotient = div(&read("60arcsec"), &product).unwrap();
    assert_eq!(quotient.unit().name(), "arcsec/(arcsec)");
    let inverse = div(&Quantity::dimensionless(2.0), &read("4s")).unwrap();
    assert_eq!((inverse.value(), inverse.unit().name()), (0.5, "(s)-1"));
    let compound = mul(&read("2km"), &read("3s")).unwrap();
    assert_eq!(compound.unit().name(), "km.s");
    let dimensionless = convert(&quotient, Some(&Unit::new("").unwrap())).unwrap();
    assert_eq!(dimensionless.value(), 60.0 / product.value());
}

#[test]
fn conversions_go_between_angles_and_times_at_a_turn_a_day() {
    let hours = convert(&read("15deg"), Some(&Unit::new("h").unwrap())).unwrap();
    assert!((hours.value() - 1.0).abs() < 1e-15);
    let si = convert(&read("1.5km"), None).unwrap();
    assert_eq!((si.value(), si.unit().name()), (1500.0, "m"));
}

#[test]
fn normalising_moves_only_angles_outside_the_range() {
    let inside = 1.0_f64;
    assert_eq!(mvangle_normalized(inside, -0.5), inside);
    let outside = 1.0 + 4.0 * std::f64::consts::PI;
    assert_eq!(mvangle_normalized(outside, -0.5), outside - 2.0 * (2.0 * std::f64::consts::PI));
    assert_eq!(mvangle_normalized(-1.0, 0.0), -1.0 + 2.0 * std::f64::consts::PI);
}

#[test]
fn doubles_stream_as_cpp_writes_them() {
    for (value, written) in [
        (std::f64::consts::PI, "3.14159"),
        (0.785_398_163_397_448_2, "0.785398"),
        (6_373_576.280_915_651, "6.37358e+06"),
        (58_849.0, "58849"),
        (58_849.25, "58849.2"),
        (1.4e9, "1.4e+09"),
        (-1.878_294_258_139_436_2, "-1.87829"),
        (1e-6, "1e-06"),
        (0.000_123_4, "0.0001234"),
        (999_999.5, "1e+06"),
        (30.0, "30"),
    ] {
        assert_eq!(stream_double(value), written, "{value}");
    }
}
