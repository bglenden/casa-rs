// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tests of CASA's quantity argument conventions, with expectations from
//! CASA 6.7.6 probes (2026-10-21).

use super::*;

fn text(value: &str) -> Variant {
    Variant::Text(value.to_string())
}

fn record(fields: &[(&str, Variant)]) -> Variant {
    Variant::Record(
        fields
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect(),
    )
}

fn close(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-9 * left.abs().max(right.abs()).max(1.0)
}

#[test]
fn empty_and_unhandled_values_are_zero() {
    for value in [
        Variant::Empty,
        Variant::Numbers(vec![10.0]),
        Variant::Other("object()".to_string()),
    ] {
        let quantity = quantity(&value).unwrap();
        assert_eq!((quantity.value(), quantity.unit().name()), (0.0, ""));
    }
    assert_eq!(quantity_value(&Variant::Empty, "s").unwrap(), 0.0);
}

#[test]
fn bare_numbers_are_in_the_si_base_unit() {
    // CASA: integrationtime=10 is 10 s, freq=1.4e9 is Hz, elevationlimit=10
    // is 10 rad, simplenoise=0.1 is 1e25 Jy.
    assert_eq!(quantity_value(&Variant::Number(10.0), "s").unwrap(), 10.0);
    assert_eq!(
        quantity_value(&Variant::Number(1.4e9), "Hz").unwrap(),
        1.4e9
    );
    assert_eq!(quantity_value(&Variant::Number(10.0), "rad").unwrap(), 10.0);
    assert!(close(
        quantity_value(&Variant::Number(0.1), "Jy").unwrap(),
        1e25
    ));
    assert!(close(quantity_value(&text("10"), "s").unwrap(), 10.0));
}

#[test]
fn strings_and_records_convert_to_the_target_unit() {
    assert!(close(quantity_value(&text("1.4GHz"), "Hz").unwrap(), 1.4e9));
    assert!(close(quantity_value(&text("0.5min"), "s").unwrap(), 30.0));
    assert!(close(quantity_value(&text("1h"), "s").unwrap(), 3600.0));
    let seconds = record(&[("value", Variant::Number(2.0)), ("unit", text("min"))]);
    assert!(close(quantity_value(&seconds, "s").unwrap(), 120.0));
    assert!(close(
        quantity_value(&text("8deg"), "rad").unwrap(),
        8_f64.to_radians()
    ));
}

#[test]
fn angles_times_and_wavelengths_convert_as_casacore_does() {
    // CASA: integrationtime='1deg' is 240 s; freq='21cm' is c/0.21 Hz.
    assert!(close(quantity_value(&text("1deg"), "s").unwrap(), 240.0));
    assert!(close(quantity_value(&text("240s"), "deg").unwrap(), 1.0));
    assert!(close(
        quantity_value(&text("21cm"), "Hz").unwrap(),
        1_427_583_133.333_333_3
    ));
    assert!(close(
        quantity_value(&text("1.420405752GHz"), "m").unwrap(),
        0.211_061_140_5
    ));
}

#[test]
fn non_conformant_units_are_refused() {
    // CASA silently takes '10Jy' as 1e-25 s and '10m' (metres) as 10 s.
    for (value, unit, name) in [("10Jy", "s", "Jy"), ("10m", "s", "m"), ("1s", "Hz", "s")] {
        let error = quantity_value(&text(value), unit).unwrap_err();
        assert_eq!(
            error,
            ConvertError::Runtime(format!(
                "From/to units not consistent. Cannot convert {name} to {unit}"
            ))
        );
    }
}

#[test]
fn unreadable_strings_raise_casas_message() {
    let error = quantity(&text("10 furlong")).unwrap_err();
    assert_eq!(
        error,
        ConvertError::Runtime(
            "Error in QuantumHolder::fromString with input string \"10 furlong\": \
             Illegal input units or format\n in converting quantity"
                .to_string()
        )
    );
    // A list of strings is not read element by element.
    assert!(quantity(&Variant::Texts(vec!["10s".to_string()])).is_err());
    let bad_unit = record(&[("value", Variant::Number(1.0)), ("unit", text("furlong"))]);
    assert!(quantity(&bad_unit).is_err());
    let not_a_quantity = record(&[("v", Variant::Number(1.0))]);
    assert!(quantity(&not_a_quantity).is_err());
}

#[test]
fn variants_describe_themselves_like_casa() {
    assert_eq!(Variant::Empty.describe(), "[]");
    assert_eq!(text("NOWHERE").describe(), "NOWHERE");
    assert_eq!(Variant::Numbers(vec![1.0, 2.5]).describe(), "[1, 2.5]");
}
