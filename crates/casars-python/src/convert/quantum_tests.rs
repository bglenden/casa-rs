// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tests of casacore's quantum arithmetic, with expectations from CASA
//! 6.7.6's `qa` tool (probed 2026-10-21).

use super::*;

fn read(text: &str) -> Quantity {
    Quantity::read(text).unwrap()
}

fn shown(quantity: &Quantity) -> (f64, String) {
    (quantity.value(), quantity.unit().name().to_string())
}

fn converted(text: &str, target: &str) -> (f64, String) {
    let target = (!target.is_empty()).then(|| casacore_unit(target).unwrap());
    let quantum = Quantum::scalar(&read(text))
        .convert(target.as_ref())
        .unwrap();
    (quantum.values[0], quantum.unit.name().to_string())
}

#[test]
fn conversions_follow_quantum_convert() {
    assert_eq!(converted("1.5km", "m"), (1500.0, "m".to_string()));
    assert_eq!(converted("1.5km", ""), (1500.0, "m".to_string()));
    assert_eq!(converted("1h", "deg"), (15.0, "deg".to_string()));
    // Non-conformant units keep the remaining dimensions.
    assert_eq!(converted("1.5km", "s"), (1500.0, "s.m.s-1".to_string()));
    assert_eq!(converted("1GHz", "m"), (1e9, "m.m-1.s-1".to_string()));
    assert_eq!(
        converted("1.5km/h", ""),
        (0.416_666_666_666_666_7, "m.s-1".to_string())
    );
    assert_eq!(converted("1Jy", ""), (1e-26, "kg.s-2".to_string()));
    assert_eq!(
        converted("1deg", ""),
        (0.017_453_292_519_943_295, "rad".to_string())
    );
    let vector = Quantum::vector(vec![1.0, 2.0], casacore_unit("km").unwrap())
        .convert(Some(&casacore_unit("m").unwrap()))
        .unwrap();
    assert_eq!(vector.values, [1000.0, 2000.0]);
}

#[test]
fn arithmetic_follows_quantum_operators() {
    let (km, m, s) = (read("1km"), read("1m"), read("3s"));
    assert_eq!(
        shown(&add(&km, &m, false).unwrap()),
        (1.001, "km".to_string())
    );
    assert_eq!(
        shown(&add(&m, &km, false).unwrap()),
        (1001.0, "m".to_string())
    );
    assert_eq!(
        shown(&add(&km, &m, true).unwrap()),
        (0.999, "km".to_string())
    );
    assert_eq!(
        add(&m, &s, false).unwrap_err().to_string(),
        "Quantum::operator+ unequal units 'm, 's'"
    );
    assert_eq!(
        add(&m, &Quantity::dimensionless(2.0), false)
            .unwrap_err()
            .to_string(),
        "Quantum::operator+ unequal units 'm, ''"
    );
    let two_km = read("2km");
    assert_eq!(
        shown(&multiply(&two_km, &s, false).unwrap()),
        (6.0, "km.s".to_string())
    );
    assert_eq!(
        shown(&multiply(&two_km, &Quantity::dimensionless(3.0), false).unwrap()),
        (6.0, "km".to_string())
    );
    assert_eq!(
        shown(&multiply(&two_km, &read("4s"), true).unwrap()),
        (0.5, "km/(s)".to_string())
    );
    assert_eq!(
        shown(&multiply(&Quantity::dimensionless(1.0), &read("4s"), true).unwrap()),
        (0.25, "(s)-1".to_string())
    );
    assert!(
        multiply(&two_km, &read("0s"), true)
            .unwrap()
            .value()
            .is_infinite()
    );
}

#[test]
fn holders_read_strings_and_records() {
    assert_eq!(quantum_holder(&Variant::Number(1.0)).unwrap(), None);
    let scalar = quantum_holder(&Variant::Text("1.5km".to_string()))
        .unwrap()
        .unwrap();
    assert!(!scalar.is_array());
    let array = Variant::Record(BTreeMap::from([
        ("value".to_string(), Variant::Numbers(vec![2.0, 3.0])),
        ("unit".to_string(), Variant::Text("s".to_string())),
    ]));
    let array = quantum_holder(&array).unwrap().unwrap();
    assert_eq!(
        (array.values.clone(), array.shape.clone()),
        (vec![2.0, 3.0], Some(vec![2]))
    );
    let bad = Variant::Record(BTreeMap::from([(
        "value".to_string(),
        Variant::Number(1.0),
    )]));
    assert_eq!(
        quantum_holder(&bad).unwrap_err().to_string(),
        "Error Illegal Quantum record in QuantumHolder::fromRecord\n in converting quantity"
    );
}
