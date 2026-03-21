// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cross-validation tests: compare Rust unit parsing against C++ casacore.

#![cfg(has_casacore_cpp)]

use casacore_test_support::quanta_interop::{
    cpp_conformant, cpp_mvangle_format_angle, cpp_mvangle_format_angle_dig2,
    cpp_mvangle_format_time, cpp_mvtime_format_dmy, cpp_mvtime_format_dmy_date,
    cpp_mvtime_format_time, cpp_parse_factor, cpp_parse_full, cpp_qc_c, cpp_qc_constant, cpp_qc_h,
};
use casacore_types::quanta::{MvAngle, MvTime, Unit, constants};

fn assert_close(a: f64, b: f64, tol: f64, label: &str) {
    let scale = a.abs().max(b.abs()).max(1e-30);
    let rel = (a - b).abs() / scale;
    assert!(
        rel < tol,
        "{label}: rust={a:.15e} vs cpp={b:.15e} (rel diff {rel:.2e})"
    );
}

/// Compare Rust factor against C++ factor for ~120 unit strings.
#[test]
fn cross_validate_unit_factors() {
    // DEFINING units
    let defining = ["m", "kg", "s", "A", "K", "cd", "mol", "rad", "sr", "_"];

    // SI_DERIVED units (skip "$" which may not round-trip via CString)
    let si_derived = [
        "%", "%%", "g", "Bq", "Hz", "C", "lm", "N", "J", "W", "V", "F", "Gy", "lx", "Ohm", "Pa",
        "S", "Sv", "Wb", "H", "T",
    ];

    // SI_EXTRA units
    let si_extra = [
        "deg", "arcmin", "arcsec", "as", "L", "l", "d", "h", "min", "a", "t", "Jy", "AU", "UA",
        "AE", "S0", "M0", "pc", "cy",
    ];

    // CUSTOMARY units (selection -- skip symbolic units like ', '', " which need care)
    let customary = [
        "sq_deg",
        "sq_arcmin",
        "sq_arcsec",
        "deg_2",
        "arcmin_2",
        "arcsec_2",
        "FU",
        "fu",
        "WU",
        "abA",
        "abC",
        "abF",
        "abH",
        "abOhm",
        "abV",
        "statA",
        "statC",
        "statF",
        "statH",
        "statOhm",
        "statV",
        "debye",
        "ac",
        "Ah",
        "Angstrom",
        "atm",
        "ata",
        "u",
        "bar",
        "Btu",
        "cal",
        "Cal",
        "CM",
        "mHg",
        "dyn",
        "eV",
        "erg",
        "fl_oz",
        "USfl_oz",
        "ft",
        "fur",
        "Gal",
        "gal",
        "USgal",
        "G",
        "Gb",
        "ha",
        "hp",
        "cwt",
        "in",
        "kn",
        "ly",
        "Mx",
        "mile",
        "n_mile",
        "Oe",
        "oz",
        "lb",
        "R",
        "sb",
        "St",
        "Torr",
        "yd",
        "yr",
        "adu",
        "beam",
        "count",
        "pixel",
        "lambda",
    ];

    // Compound unit expressions
    let compound = [
        "km", "MHz", "mJy", "kHz", "m/s", "km/s", "kg.m.s-2", "W/m2/Hz",
    ];

    let all_units: Vec<&str> = defining
        .iter()
        .chain(si_derived.iter())
        .chain(si_extra.iter())
        .chain(customary.iter())
        .chain(compound.iter())
        .copied()
        .collect();

    eprintln!("cross-validating {} unit factors", all_units.len());

    for unit in &all_units {
        let rust = Unit::new(unit).unwrap_or_else(|e| panic!("Rust parse failed for {unit}: {e}"));
        let cpp_factor =
            cpp_parse_factor(unit).unwrap_or_else(|| panic!("C++ parse failed for {unit}"));

        assert_close(
            rust.val().factor,
            cpp_factor,
            1e-10,
            &format!("factor({unit})"),
        );
    }
}

/// Cross-validate dimensional conformance.
#[test]
fn cross_validate_conformance() {
    let conformant_pairs = [
        ("km", "m"),
        ("MHz", "Hz"),
        ("Jy", "W/m2/Hz"),
        ("deg", "rad"),
    ];
    for (a, b) in &conformant_pairs {
        let rust_conf = Unit::new(a).unwrap().conformant(&Unit::new(b).unwrap());
        let cpp_conf = cpp_conformant(a, b).unwrap();
        assert_eq!(rust_conf, cpp_conf, "conformance({a}, {b})");
    }

    let non_conformant_pairs = [("m", "s"), ("kg", "Hz"), ("Jy", "m")];
    for (a, b) in &non_conformant_pairs {
        let rust_conf = Unit::new(a).unwrap().conformant(&Unit::new(b).unwrap());
        let cpp_conf = cpp_conformant(a, b).unwrap();
        assert_eq!(rust_conf, cpp_conf, "non-conformance({a}, {b})");
    }
}

/// Cross-validate dimension exponents for all unit categories.
#[test]
fn cross_validate_unit_dimensions() {
    let units = [
        // Base units with known single-dimension exponents
        "m", "kg", "s", "A", "K", "cd", "mol", "rad", "sr", "_",
        // Derived units with multi-dimension exponents
        "N", "J", "W", "V", "F", "Pa", "Ohm", "Hz", "C", "H", "T", "Wb", "S",
        // Extra units
        "deg", "arcmin", "arcsec", "Jy", "AU", "pc", // Customary
        "atm", "bar", "eV", "erg", "ft", "lb", "ly", "Torr", // Compound expressions
        "m/s", "km/s", "kg.m.s-2", "W/m2/Hz", "J/K/mol",
    ];

    for unit in &units {
        let rust = Unit::new(unit).unwrap_or_else(|e| panic!("Rust parse failed for {unit}: {e}"));
        let (_, cpp_dims) =
            cpp_parse_full(unit).unwrap_or_else(|| panic!("C++ parse_full failed for {unit}"));

        let rust_dims = rust.val().dim.dims;
        for i in 0..10 {
            assert_eq!(
                rust_dims[i] as i32, cpp_dims[i],
                "dim[{i}] mismatch for unit '{unit}': rust={:?} cpp={:?}",
                rust_dims, cpp_dims
            );
        }
    }
}

/// Cross-validate QC::c (speed of light).
#[test]
fn cross_validate_qc_speed_of_light() {
    assert_close(constants::c().value(), cpp_qc_c(), 1e-15, "QC::c");
}

/// Cross-validate QC::h (Planck constant).
#[test]
fn cross_validate_qc_planck() {
    assert_close(constants::h().value(), cpp_qc_h(), 1e-12, "QC::h");
}

/// Cross-validate all 21 QC physical constants against C++.
#[test]
fn cross_validate_all_qc_constants() {
    // (C++ name, Rust accessor, notes)
    let constants_table: Vec<(&str, &dyn Fn() -> &'static casacore_types::quanta::Quantity)> = vec![
        ("c", &constants::c),
        ("G", &constants::gravitational),
        ("h", &constants::h),
        ("HI", &constants::hi_line),
        ("R", &constants::gas_constant),
        ("NA", &constants::avogadro),
        ("e", &constants::electron_charge),
        ("mp", &constants::proton_mass),
        ("mp_me", &constants::proton_electron_ratio),
        ("mu0", &constants::vacuum_permeability),
        ("epsilon0", &constants::vacuum_permittivity),
        ("k", &constants::boltzmann),
        ("F", &constants::faraday),
        ("me", &constants::electron_mass),
        ("re", &constants::electron_radius),
        ("a0", &constants::bohr_radius),
        ("R0", &constants::solar_radius),
        ("k2", &constants::iau_k2),
        ("qTurn", &constants::quarter_turn),
        ("hTurn", &constants::half_turn),
        ("fTurn", &constants::full_turn),
    ];

    for (cpp_name, rust_fn) in &constants_table {
        let rust_q = rust_fn();
        let (cpp_value, cpp_unit_str, cpp_dims) = cpp_qc_constant(cpp_name)
            .unwrap_or_else(|| panic!("C++ QC constant '{cpp_name}' not found"));

        // Compare SI-normalized values: value * factor
        let rust_si = rust_q.value() * rust_q.unit().val().factor;
        let cpp_unit = Unit::new(&cpp_unit_str).unwrap_or_else(|e| {
            panic!("Failed to parse C++ unit '{cpp_unit_str}' for {cpp_name}: {e}")
        });
        let cpp_si = cpp_value * cpp_unit.val().factor;

        // Some constants (k, F, me, epsilon0) are derived differently in C++
        // (e.g. k = R/NA) vs Rust (direct CODATA value), so allow 1e-6 tolerance.
        assert_close(
            rust_si,
            cpp_si,
            1e-6,
            &format!("QC::{cpp_name} SI-normalized value"),
        );

        // Compare dimension exponents
        let rust_dims = rust_q.unit().val().dim.dims;
        for i in 0..10 {
            assert_eq!(
                rust_dims[i] as i32, cpp_dims[i],
                "QC::{cpp_name} dim[{i}] mismatch: rust={:?} cpp={:?}",
                rust_dims, cpp_dims
            );
        }
    }
}

#[test]
fn cross_validate_mvangle_formatting_subset() {
    let cases = [
        (
            MvAngle::from_radians(-0.25 * std::f64::consts::TAU)
                .normalized(0.0)
                .format_time(6),
            cpp_mvangle_format_time(-0.25 * std::f64::consts::TAU, 0.0, 6).unwrap(),
            "ra_time_wrap",
        ),
        (
            MvAngle::from_radians((-12.5_f64).to_radians()).format_angle_dig2(5),
            cpp_mvangle_format_angle_dig2((-12.5_f64).to_radians(), 5).unwrap(),
            "declination_dig2",
        ),
        (
            MvAngle::from_radians(123.5_f64.to_radians()).format_angle(1),
            cpp_mvangle_format_angle(123.5_f64.to_radians(), 1).unwrap(),
            "longitude_angle",
        ),
    ];

    for (rust, cpp, label) in cases {
        assert_eq!(rust, cpp, "{label}: rust={rust:?} cpp={cpp:?}");
    }
}

#[test]
fn cross_validate_mvtime_formatting_subset() {
    let j2000 = 51_544.5;
    let rounded_boundary = 51_544.0 + (86_399.96 / 86_400.0);
    let cases = [
        (
            MvTime::from_mjd_days(j2000).format_dmy(1),
            cpp_mvtime_format_dmy(j2000, 1).unwrap(),
            "j2000_dmy",
        ),
        (
            MvTime::from_mjd_days(j2000).format_time(1),
            cpp_mvtime_format_time(j2000, 1).unwrap(),
            "j2000_time",
        ),
        (
            MvTime::from_mjd_days(j2000).format_dmy_date(),
            cpp_mvtime_format_dmy_date(j2000).unwrap(),
            "j2000_date",
        ),
        (
            MvTime::from_mjd_days(rounded_boundary).format_dmy(1),
            cpp_mvtime_format_dmy(rounded_boundary, 1).unwrap(),
            "round_day_boundary",
        ),
    ];

    for (rust, cpp, label) in cases {
        assert_eq!(rust, cpp, "{label}: rust={rust:?} cpp={cpp:?}");
    }
}
