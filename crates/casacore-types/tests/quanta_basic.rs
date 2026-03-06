// SPDX-License-Identifier: LGPL-3.0-or-later
//! End-to-end integration tests for the quanta module.

use casacore_types::quanta::{Dimension, Quantity, Unit, UnitDim, UnitVal, constants};

// ── Unit parsing ──

#[test]
fn parse_base_si_units() {
    for name in &["m", "kg", "s", "A", "K", "cd", "mol", "rad", "sr"] {
        let u = Unit::new(name).expect(name);
        assert_eq!(u.val().factor, 1.0, "factor for {name}");
    }
}

#[test]
fn parse_si_derived() {
    let n = Unit::new("N").unwrap();
    // Newton = kg.m.s-2
    assert_eq!(n.val().dim.get(Dimension::Mass), 1);
    assert_eq!(n.val().dim.get(Dimension::Length), 1);
    assert_eq!(n.val().dim.get(Dimension::Time), -2);

    let j = Unit::new("J").unwrap();
    // Joule = kg.m2.s-2
    assert_eq!(j.val().dim.get(Dimension::Length), 2);
}

#[test]
fn parse_prefixed_units() {
    let km = Unit::new("km").unwrap();
    assert!((km.val().factor - 1e3).abs() < 1e-10);

    let mhz = Unit::new("MHz").unwrap();
    assert!((mhz.val().factor - 1e6).abs() < 1.0);
    assert_eq!(mhz.val().dim.get(Dimension::Time), -1);

    let mjy = Unit::new("mJy").unwrap();
    assert!((mjy.val().factor - 1e-29).abs() < 1e-42);
}

#[test]
fn parse_compound_expressions() {
    let vel = Unit::new("km/s").unwrap();
    assert_eq!(vel.val().dim.get(Dimension::Length), 1);
    assert_eq!(vel.val().dim.get(Dimension::Time), -1);
    assert!((vel.val().factor - 1e3).abs() < 1e-10);

    let force = Unit::new("kg.m.s-2").unwrap();
    assert_eq!(force.val().dim, Unit::new("N").unwrap().val().dim);

    let flux = Unit::new("W/m2/Hz").unwrap();
    assert_eq!(flux.val().dim.get(Dimension::Mass), 1);
    assert_eq!(flux.val().dim.get(Dimension::Time), -2);
}

#[test]
fn parse_parenthesised_expression() {
    let u = Unit::new("(m/s)2").unwrap();
    assert_eq!(u.val().dim.get(Dimension::Length), 2);
    assert_eq!(u.val().dim.get(Dimension::Time), -2);
}

// ── Quantity arithmetic ──

#[test]
fn quantity_conversion_km_to_m() {
    let km = Quantity::new(2.5, "km").unwrap();
    let m_unit = Unit::new("m").unwrap();
    let m_val = km.get_value_in(&m_unit).unwrap();
    assert!((m_val - 2500.0).abs() < 1e-10);
}

#[test]
fn quantity_add_conformant() {
    let a = Quantity::new(1.0, "km").unwrap();
    let b = Quantity::new(500.0, "m").unwrap();
    let c = &a + &b;
    assert!((c.value() - 1.5).abs() < 1e-12);
    assert_eq!(c.unit().name(), "km");
}

#[test]
fn quantity_non_conformant_fails() {
    let km = Quantity::new(1.0, "km").unwrap();
    let s = Unit::new("s").unwrap();
    assert!(km.convert(&s).is_err());
}

#[test]
fn quantity_mul_div() {
    let d = Quantity::new(100.0, "km").unwrap();
    let t = Quantity::new(2.0, "h").unwrap();
    let v = &d / &t;
    assert!((v.value() - 50.0).abs() < 1e-10);

    // Verify the velocity can be converted to m/s.
    let ms = Unit::new("m/s").unwrap();
    let v_ms = v.get_value_in(&ms).unwrap();
    // 100 km / 2 h = 50 km/h = 50000/3600 m/s ≈ 13.889 m/s
    assert!((v_ms - 50000.0 / 3600.0).abs() < 1e-6);
}

#[test]
fn quantity_from_str_roundtrip() {
    let q: Quantity = "42.5 MHz".parse().unwrap();
    assert!((q.value() - 42.5).abs() < 1e-12);
    assert_eq!(q.unit().name(), "MHz");
}

// ── Dimensions ──

#[test]
fn dimension_arithmetic() {
    let m2 = UnitDim::basis(Dimension::Length).pow(2);
    let m = m2.sqrt().unwrap();
    assert_eq!(m.get(Dimension::Length), 1);

    let vel = UnitDim::basis(Dimension::Length) / UnitDim::basis(Dimension::Time);
    assert_eq!(vel.get(Dimension::Length), 1);
    assert_eq!(vel.get(Dimension::Time), -1);
}

#[test]
fn unit_val_conformance() {
    let km = UnitVal::new(1e3, UnitDim::basis(Dimension::Length));
    let mile = UnitVal::new(1609.344, UnitDim::basis(Dimension::Length));
    assert!(km.conformant(&mile));
    assert!(!km.conformant(&UnitVal::TIME));
}

// ── Physical constants ──

#[test]
fn speed_of_light() {
    let c = constants::c();
    assert!((c.value() - 2.997_924_58e8).abs() < 1.0);
    assert_eq!(c.unit().name(), "m/s");
}

#[test]
fn planck_constant() {
    let h = constants::h();
    assert!((h.value() - 6.626_075_5e-34).abs() < 1e-40);
}

#[test]
fn boltzmann_derived() {
    let k = constants::boltzmann();
    let expected = 8.314_510 / 6.022_136_7e23;
    let rel_err = ((k.value() - expected) / expected).abs();
    assert!(rel_err < 1e-6);
}

#[test]
fn full_turn_to_radians() {
    let ft = constants::full_turn();
    let rad = Unit::new("rad").unwrap();
    let val = ft.get_value_in(&rad).unwrap();
    assert!((val - 2.0 * std::f64::consts::PI).abs() < 1e-12);
}

// ── Astronomical units ──

#[test]
fn parsec_to_metres() {
    let pc = Unit::new("pc").unwrap();
    let m = Unit::new("m").unwrap();
    // 1 pc ≈ 3.086e16 m
    let q = Quantity::with_unit(1.0, pc);
    let val = q.get_value_in(&m).unwrap();
    assert!((val - 3.085_677_6e16).abs() / 3.085_677_6e16 < 1e-4);
}

#[test]
fn au_to_metres() {
    let au = Unit::new("AU").unwrap();
    // 1 AU ≈ 1.496e11 m
    let q = Quantity::with_unit(1.0, au);
    let m = Unit::new("m").unwrap();
    let val = q.get_value_in(&m).unwrap();
    assert!((val - 1.496e11).abs() / 1.496e11 < 1e-3);
}

#[test]
fn jansky_dimensions() {
    let jy = Unit::new("Jy").unwrap();
    // Jy = 1e-26 W/m2/Hz = 1e-26 kg.s-2
    assert_eq!(jy.val().dim.get(Dimension::Mass), 1);
    assert_eq!(jy.val().dim.get(Dimension::Time), -2);
    assert_eq!(jy.val().dim.get(Dimension::Length), 0);
}

// ── Edge cases ──

#[test]
fn empty_string_is_dimensionless() {
    let u = Unit::new("").unwrap();
    assert!(u.val().dim.is_dimensionless());
    assert_eq!(u.val().factor, 1.0);
}

#[test]
fn unknown_unit_returns_error() {
    assert!(Unit::new("xyzzy").is_err());
}

#[test]
fn underscore_is_undimensioned() {
    let u = Unit::new("_").unwrap();
    assert_eq!(u.val().dim.get(Dimension::Undefined), 1);
}

#[test]
fn degree_to_radian_conversion() {
    let deg = Quantity::new(180.0, "deg").unwrap();
    let rad = Unit::new("rad").unwrap();
    let val = deg.get_value_in(&rad).unwrap();
    assert!((val - std::f64::consts::PI).abs() < 1e-12);
}
