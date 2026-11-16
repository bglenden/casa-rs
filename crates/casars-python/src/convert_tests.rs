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
            .collect::<BTreeMap<_, _>>()
            .into(),
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
fn empty_complex_vector_retains_measure_defaults() {
    let value = Variant::ComplexArray {
        shape: vec![0],
        values: vec![],
    };
    assert_eq!(
        measures::epoch(&value).unwrap().unwrap().value().as_mjd(),
        0.0
    );
    assert_eq!(measures::frequency(&value).unwrap().0.unwrap().hz(), 0.0);
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
    assert_eq!(
        Variant::Complexes(vec![Complex64::new(1.0, 0.5)]).describe(),
        "[(1,0.5)]"
    );
}

#[test]
fn numbers_read_as_complex_vectors_and_a_complex_is_no_quantity() {
    // `setflux` reads its values with casatools' `toComplexVec`, and
    // `casaQuantity` of a complex number raises (CASA 6.7.6, 2026-10-23).
    let complex = Complex64::new;
    assert_eq!(
        complex_values(&Variant::Number(2.0)),
        Some(vec![complex(2.0, 0.0)])
    );
    assert_eq!(
        complex_values(&Variant::Numbers(vec![1.0, 0.5])),
        Some(vec![complex(1.0, 0.0), complex(0.5, 0.0)])
    );
    assert_eq!(
        complex_values(&Variant::Complex(complex(2.0, 3.0))),
        Some(vec![complex(2.0, 3.0)])
    );
    assert_eq!(complex_values(&text("1")), None);
    assert_eq!(
        quantity(&Variant::Complex(complex(0.0, 1.0)))
            .unwrap_err()
            .to_string(),
        "Wrong QuantumHolder to convert asQuantumDouble"
    );
    let vector = quantity(&Variant::Complexes(vec![complex(0.0, 1.0)])).unwrap();
    assert_eq!((vector.value(), vector.unit().name()), (0.0, ""));
}

#[test]
fn record_scalar_metadata_changes_only_quantity_interpretation() {
    let fields = BTreeMap::from([
        ("value".to_string(), Variant::Number(i64::MAX as f64)),
        ("unit".to_string(), Variant::Text("m".to_string())),
    ]);
    let ordinary = Variant::Record(fields.clone().into());
    let typed = Variant::Record(VariantRecord {
        fields,
        scalar_kind: Some(RecordScalarKind::NativeInteger(i64::MAX)),
    });
    assert_eq!(
        value::CasaValue::from_variant(&ordinary),
        value::CasaValue::from_variant(&typed)
    );
    assert_eq!(quantity(&typed).unwrap().value(), -1.0);
}

#[test]
fn zero_dimensional_float_arrays_survive_in_ordinary_records() {
    Python::initialize();
    Python::attach(|py| {
        let array = py
            .eval(
                c"__import__('numpy').array(1.25, dtype='float32')",
                None,
                None,
            )
            .unwrap();
        let input = pyo3::types::PyDict::new(py);
        input.set_item("value", array).unwrap();
        input.set_item("unit", "m").unwrap();
        let record = variant_from_python(input.as_any()).unwrap();
        let value::CasaValue::Record(fields) = value::CasaValue::from_variant(&record) else {
            panic!("expected an ordinary record");
        };
        assert_eq!(
            fields[1].1,
            value::CasaValue::Floats {
                shape: vec![],
                values: vec![1.25],
            }
        );
    });
}

#[test]
fn complex_numpy_arrays_keep_values_and_shapes_in_ordinary_records() {
    Python::initialize();
    Python::attach(|py| {
        let array = py
            .eval(
                c"__import__('numpy').array([[1+2j, 3+4j], [5+6j, 7+8j]], dtype='complex64')",
                None,
                None,
            )
            .unwrap();
        let input = pyo3::types::PyDict::new(py);
        input.set_item("payload", array).unwrap();
        let record = variant_from_python(input.as_any()).unwrap();
        let value::CasaValue::Record(fields) = value::CasaValue::from_variant(&record) else {
            panic!("expected an ordinary record");
        };
        assert_eq!(
            fields[0].1,
            value::CasaValue::Complexes {
                shape: vec![2, 2],
                values: vec![
                    Complex64::new(1.0, 2.0),
                    Complex64::new(5.0, 6.0),
                    Complex64::new(3.0, 4.0),
                    Complex64::new(7.0, 8.0),
                ],
            }
        );
    });
}

#[test]
fn bare_numpy_complex64_keeps_both_parts() {
    Python::initialize();
    Python::attach(|py| {
        let scalar = py
            .eval(c"__import__('numpy').complex64(1+2j)", None, None)
            .unwrap();
        assert_eq!(
            variant_from_python(&scalar).unwrap(),
            Variant::Complex(Complex64::new(1.0, 2.0))
        );
        let input = pyo3::types::PyDict::new(py);
        input.set_item("value", scalar).unwrap();
        let record = variant_from_python(input.as_any()).unwrap();
        let value::CasaValue::Record(fields) = value::CasaValue::from_variant(&record) else {
            panic!("expected an ordinary record");
        };
        assert_eq!(
            fields[0].1,
            value::CasaValue::Complex(Complex64::new(1.0, 2.0))
        );
    });
}
