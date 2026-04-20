// SPDX-License-Identifier: LGPL-3.0-or-later
//! Interop tests for TaQL measure UDFs: Rust meas.* vs C++ casacore conversions.
//!
//! Compares numeric outputs from:
//! - RR: Rust TaQL `meas.*` functions
//! - CC: C++ direct measure conversion APIs (baseline)
//!
//! These tests verify that the Rust TaQL meas UDF layer produces results
//! consistent with C++ casacore's measure conversion engine.
#![cfg(all(feature = "cpp-interop-tests", has_casacore_cpp))]

use casa_tables::taql::ast::IndexStyle;
use casa_tables::taql::eval::{EvalContext, ExprValue};
use casa_tables::taql::functions::call_function;
use casa_test_support::measures_interop::{
    cpp_direction_convert, cpp_doppler_convert, cpp_earthmag_convert_angles, cpp_eop_query,
    cpp_epoch_convert, cpp_epoch_convert_with_frame, cpp_frequency_convert,
    cpp_frequency_convert_with_rv, cpp_frequency_rest_with_doppler,
    cpp_frequency_shift_with_doppler, cpp_igrf_value, cpp_named_direction_convert,
    cpp_position_convert, cpp_position_to_record, cpp_position_to_wgs_xyz, cpp_radvel_convert,
    cpp_riseset,
};
use casa_types::RecordValue;
use casa_types::quanta::Quantity;

const J2000_MJD: f64 = 51544.5;
const SECONDS_PER_DAY: f64 = 86_400.0;

// VLA in WGS84
const VLA_LON: f64 = -1.878_283_2;
const VLA_LAT: f64 = 0.595_370_3;
const VLA_H: f64 = 2124.0;

// M31 in J2000
const M31_LON: f64 = 0.185_948_8;
const M31_LAT: f64 = 0.722_777_4;

fn s(val: &str) -> ExprValue {
    ExprValue::String(val.to_string())
}
fn fl(val: f64) -> ExprValue {
    ExprValue::Float(val)
}
fn q(val: f64, unit: &str) -> ExprValue {
    ExprValue::Quantity(Quantity::new(val, unit).unwrap())
}

fn eval_meas(name: &str, args: &[ExprValue]) -> ExprValue {
    let dummy_row = RecordValue::new(vec![]);
    let ctx = EvalContext {
        row: &dummy_row,
        row_index: 0,
        style: IndexStyle::default(),
    };
    call_function(name, args, &ctx).unwrap()
}

fn extract_float(val: &ExprValue) -> f64 {
    match val {
        ExprValue::Float(v) => *v,
        other => panic!("expected Float, got {other:?}"),
    }
}

fn extract_dir(val: &ExprValue) -> (f64, f64) {
    match val {
        ExprValue::Array(arr) => {
            assert_eq!(arr.shape, vec![2]);
            (extract_float(&arr.data[0]), extract_float(&arr.data[1]))
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

fn extract_pos(val: &ExprValue) -> [f64; 3] {
    match val {
        ExprValue::Array(arr) => {
            assert_eq!(arr.shape, vec![3]);
            [
                extract_float(&arr.data[0]),
                extract_float(&arr.data[1]),
                extract_float(&arr.data[2]),
            ]
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

fn extract_array3(val: &ExprValue) -> [f64; 3] {
    match val {
        ExprValue::Array(arr) => {
            assert_eq!(arr.shape, vec![3]);
            [
                extract_float(&arr.data[0]),
                extract_float(&arr.data[1]),
                extract_float(&arr.data[2]),
            ]
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

fn extract_datetime_pair(val: &ExprValue) -> (f64, f64) {
    match val {
        ExprValue::Array(arr) => {
            assert_eq!(arr.shape, vec![2]);
            match (&arr.data[0], &arr.data[1]) {
                (ExprValue::DateTime(a), ExprValue::DateTime(b)) => (*a, *b),
                other => panic!("expected DateTime pair, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

fn close(a: f64, b: f64, tol: f64) -> bool {
    (a - b).abs() < tol
}

/// Compare two angles, accounting for wrapping at ±π / 2π boundaries.
fn close_angle(a: f64, b: f64, tol: f64) -> bool {
    let diff = (a - b).rem_euclid(std::f64::consts::TAU);
    let diff = if diff > std::f64::consts::PI {
        std::f64::consts::TAU - diff
    } else {
        diff
    };
    diff < tol
}

// ── Epoch: Rust meas.epoch vs C++ epoch_convert ──

#[test]
fn epoch_utc_to_tai() {
    let rust_result = eval_meas("meas.epoch", &[s("TAI"), fl(J2000_MJD)]);
    let rust_mjd = extract_float(&rust_result);

    let cpp_mjd = cpp_epoch_convert(J2000_MJD, "UTC", "TAI").unwrap();

    let diff_s = (rust_mjd - cpp_mjd).abs() * SECONDS_PER_DAY;
    assert!(
        diff_s < 1e-6,
        "epoch UTC→TAI: Rust={rust_mjd}, C++={cpp_mjd}, diff={diff_s}s"
    );
}

#[test]
fn epoch_tai_to_tt() {
    let tai_mjd = J2000_MJD + 32.0 / SECONDS_PER_DAY;
    let rust_result = eval_meas("meas.epoch", &[s("TT"), fl(tai_mjd), s("TAI")]);
    let rust_mjd = extract_float(&rust_result);

    let cpp_mjd = cpp_epoch_convert(tai_mjd, "TAI", "TT").unwrap();

    let diff_s = (rust_mjd - cpp_mjd).abs() * SECONDS_PER_DAY;
    assert!(
        diff_s < 1e-6,
        "epoch TAI→TT: Rust={rust_mjd}, C++={cpp_mjd}, diff={diff_s}s"
    );
}

#[test]
fn epoch_utc_to_tdb() {
    let rust_result = eval_meas("meas.epoch", &[s("TDB"), fl(J2000_MJD)]);
    let rust_mjd = extract_float(&rust_result);

    let cpp_mjd = cpp_epoch_convert(J2000_MJD, "UTC", "TDB").unwrap();

    let diff_s = (rust_mjd - cpp_mjd).abs() * SECONDS_PER_DAY;
    assert!(
        diff_s < 0.01,
        "epoch UTC→TDB: Rust={rust_mjd}, C++={cpp_mjd}, diff={diff_s}s"
    );
}

#[test]
fn epoch_last_with_position() {
    let vla_itrf = extract_pos(&eval_meas(
        "meas.pos",
        &[s("ITRF"), fl(VLA_LON), fl(VLA_LAT), fl(VLA_H), s("WGS84")],
    ));

    let rust_result = eval_meas(
        "meas.last",
        &[
            fl(J2000_MJD),
            fl(vla_itrf[0]),
            fl(vla_itrf[1]),
            fl(vla_itrf[2]),
            s("ITRF"),
        ],
    );
    let rust_mjd = extract_float(&rust_result);

    let (dut1, _, _) = cpp_eop_query(J2000_MJD).unwrap();
    let cpp_last =
        cpp_epoch_convert_with_frame(J2000_MJD, "UTC", "LAST", VLA_LON, VLA_LAT, VLA_H, dut1)
            .unwrap();
    let cpp_seconds = cpp_last.fract() * SECONDS_PER_DAY;

    let diff_s = (rust_mjd - cpp_seconds).abs();
    assert!(
        diff_s < 1e-4,
        "epoch UTC→LAST: Rust={rust_mjd}, C++={cpp_seconds}, diff={diff_s}s"
    );
}

// ── Direction: Rust meas.dir vs C++ direction_convert ──

#[test]
fn dir_j2000_to_galactic() {
    let rust_result = eval_meas(
        "meas.dir",
        &[s("GALACTIC"), fl(M31_LON), fl(M31_LAT), s("J2000")],
    );
    let (rust_lon, rust_lat) = extract_dir(&rust_result);

    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(M31_LON, M31_LAT, "J2000", "GALACTIC", 0.0, 0.0, 0.0, 0.0).unwrap();

    // Use angle-aware comparison (accounts for 2π wrapping) and ~1e-5 tolerance
    // for known SOFA vs casacore algorithm differences in galactic conversion.
    assert!(
        close_angle(rust_lon, cpp_lon, 1e-4) && close(rust_lat, cpp_lat, 1e-4),
        "dir J2000→GAL: Rust=({rust_lon},{rust_lat}), C++=({cpp_lon},{cpp_lat})"
    );
}

#[test]
fn dir_galactic_to_j2000() {
    // Galactic center
    let rust_result = eval_meas("meas.j2000", &[fl(0.0), fl(0.0), s("GALACTIC")]);
    let (rust_lon, rust_lat) = extract_dir(&rust_result);

    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(0.0, 0.0, "GALACTIC", "J2000", 0.0, 0.0, 0.0, 0.0).unwrap();

    assert!(
        close_angle(rust_lon, cpp_lon, 1e-4) && close(rust_lat, cpp_lat, 1e-4),
        "dir GAL→J2000: Rust=({rust_lon},{rust_lat}), C++=({cpp_lon},{cpp_lat})"
    );
}

#[test]
fn dir_j2000_to_b1950() {
    let rust_result = eval_meas("meas.b1950", &[fl(M31_LON), fl(M31_LAT)]);
    let (rust_lon, rust_lat) = extract_dir(&rust_result);

    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(M31_LON, M31_LAT, "J2000", "B1950", 0.0, 0.0, 0.0, 0.0).unwrap();

    assert!(
        close_angle(rust_lon, cpp_lon, 1e-4) && close(rust_lat, cpp_lat, 1e-4),
        "dir J2000→B1950: Rust=({rust_lon},{rust_lat}), C++=({cpp_lon},{cpp_lat})"
    );
}

#[test]
fn dircos_app_matches_cpp_angles() {
    let vla_itrf = extract_pos(&eval_meas(
        "meas.pos",
        &[s("ITRF"), fl(VLA_LON), fl(VLA_LAT), fl(VLA_H), s("WGS84")],
    ));

    let rust_result = eval_meas(
        "meas.dircos",
        &[
            s("APP"),
            fl(M31_LON),
            fl(M31_LAT),
            s("J2000"),
            fl(J2000_MJD),
            fl(vla_itrf[0]),
            fl(vla_itrf[1]),
            fl(vla_itrf[2]),
        ],
    );
    let rust_cos = extract_array3(&rust_result);

    let (cpp_lon, cpp_lat) = cpp_direction_convert(
        M31_LON, M31_LAT, "J2000", "APP", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let cpp_cos = [
        cpp_lon.cos() * cpp_lat.cos(),
        cpp_lon.sin() * cpp_lat.cos(),
        cpp_lat.sin(),
    ];

    for i in 0..3 {
        assert!(
            close(rust_cos[i], cpp_cos[i], 1e-6),
            "dircos APP[{i}]: Rust={}, C++={}",
            rust_cos[i],
            cpp_cos[i]
        );
    }
}

#[test]
fn azel_shortcut_matches_cpp() {
    let vla_itrf = extract_pos(&eval_meas(
        "meas.pos",
        &[s("ITRF"), fl(VLA_LON), fl(VLA_LAT), fl(VLA_H), s("WGS84")],
    ));

    let rust_result = eval_meas(
        "meas.azel",
        &[
            fl(M31_LON),
            fl(M31_LAT),
            s("J2000"),
            fl(J2000_MJD),
            fl(vla_itrf[0]),
            fl(vla_itrf[1]),
            fl(vla_itrf[2]),
        ],
    );
    let (rust_lon, rust_lat) = extract_dir(&rust_result);

    let (cpp_lon, cpp_lat) = cpp_direction_convert(
        M31_LON, M31_LAT, "J2000", "AZEL", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();

    assert!(
        close_angle(rust_lon, cpp_lon, 1e-5) && close(rust_lat, cpp_lat, 1e-5),
        "dir J2000→AZEL: Rust=({rust_lon},{rust_lat}), C++=({cpp_lon},{cpp_lat})"
    );
}

#[test]
fn itrfd_shortcut_matches_cpp() {
    let vla_itrf = extract_pos(&eval_meas(
        "meas.pos",
        &[s("ITRF"), fl(VLA_LON), fl(VLA_LAT), fl(VLA_H), s("WGS84")],
    ));

    let rust_result = eval_meas(
        "meas.itrfd",
        &[
            fl(M31_LON),
            fl(M31_LAT),
            s("J2000"),
            fl(J2000_MJD),
            fl(vla_itrf[0]),
            fl(vla_itrf[1]),
            fl(vla_itrf[2]),
        ],
    );
    let (rust_lon, rust_lat) = extract_dir(&rust_result);

    let (cpp_lon, cpp_lat) = cpp_direction_convert(
        M31_LON, M31_LAT, "J2000", "ITRF", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();

    assert!(
        close_angle(rust_lon, cpp_lon, 1e-6) && close(rust_lat, cpp_lat, 1e-6),
        "dir J2000→ITRF: Rust=({rust_lon},{rust_lat}), C++=({cpp_lon},{cpp_lat})"
    );
}

#[test]
fn named_source_fixed_direction_matches_cpp() {
    let rust_result = eval_meas("meas.j2000", &[s("CasA")]);
    let (rust_lon, rust_lat) = extract_dir(&rust_result);

    let (cpp_lon, cpp_lat) =
        cpp_named_direction_convert("CasA", "J2000", 0.0, 0.0, 0.0, 0.0).unwrap();

    assert!(
        close_angle(rust_lon, cpp_lon, 1e-12) && close(rust_lat, cpp_lat, 1e-12),
        "dir CasA→J2000: Rust=({rust_lon},{rust_lat}), C++=({cpp_lon},{cpp_lat})"
    );
}

#[test]
fn named_source_catalog_direction_matches_cpp() {
    let rust_result = eval_meas("meas.j2000", &[s("0002-478")]);
    let (rust_lon, rust_lat) = extract_dir(&rust_result);

    let (cpp_lon, cpp_lat) =
        cpp_named_direction_convert("0002-478", "J2000", 0.0, 0.0, 0.0, 0.0).unwrap();

    assert!(
        close_angle(rust_lon, cpp_lon, 1e-11) && close(rust_lat, cpp_lat, 1e-11),
        "dir 0002-478→J2000: Rust=({rust_lon},{rust_lat}), C++=({cpp_lon},{cpp_lat})"
    );
}

#[test]
fn named_source_sun_direction_matches_cpp() {
    let rust_result = eval_meas("meas.dir", &[s("ITRF"), s("SUN"), fl(J2000_MJD), s("VLA")]);
    let (rust_lon, rust_lat) = extract_dir(&rust_result);

    let (cpp_lon, cpp_lat) =
        cpp_named_direction_convert("SUN", "ITRF", J2000_MJD, VLA_LON, VLA_LAT, VLA_H).unwrap();

    assert!(
        close_angle(rust_lon, cpp_lon, 5e-4) && close(rust_lat, cpp_lat, 5e-4),
        "dir SUN→ITRF: Rust=({rust_lon},{rust_lat}), C++=({cpp_lon},{cpp_lat})"
    );
}

#[test]
fn riseset_fixed_source_matches_cpp() {
    let rust_result = eval_meas("meas.riseset", &[s("CasA"), fl(J2000_MJD), s("VLA")]);
    let (rust_rise, rust_set) = extract_datetime_pair(&rust_result);

    let (cpp_rise, cpp_set) = cpp_riseset("CasA", J2000_MJD, VLA_LON, VLA_LAT, VLA_H).unwrap();

    let rise_diff_s = (rust_rise - cpp_rise).abs() * SECONDS_PER_DAY;
    let set_diff_s = (rust_set - cpp_set).abs() * SECONDS_PER_DAY;
    assert!(
        rise_diff_s < 0.5 && set_diff_s < 0.5,
        "riseset CasA: Rust=({rust_rise},{rust_set}), C++=({cpp_rise},{cpp_set}), diff=({rise_diff_s}s,{set_diff_s}s)"
    );
}

#[test]
fn riseset_sun_matches_cpp_with_reasonable_tolerance() {
    let rust_result = eval_meas("meas.riseset", &[s("SUN"), fl(J2000_MJD), s("VLA")]);
    let (rust_rise, rust_set) = extract_datetime_pair(&rust_result);

    let (cpp_rise, cpp_set) = cpp_riseset("SUN", J2000_MJD, VLA_LON, VLA_LAT, VLA_H).unwrap();

    let rise_diff_s = (rust_rise - cpp_rise).abs() * SECONDS_PER_DAY;
    let set_diff_s = (rust_set - cpp_set).abs() * SECONDS_PER_DAY;
    assert!(
        rise_diff_s < 300.0 && set_diff_s < 300.0,
        "riseset SUN: Rust=({rust_rise},{rust_set}), C++=({cpp_rise},{cpp_set}), diff=({rise_diff_s}s,{set_diff_s}s)"
    );
}

// ── EarthMagnetic: Rust meas.em* / meas.igrf* vs C++ EarthMagnetic ──

#[test]
fn igrfxyz_matches_cpp() {
    let rust_result = eval_meas(
        "meas.igrfxyz",
        &[
            fl(0.0),
            fl(0.0),
            fl(std::f64::consts::FRAC_PI_2),
            s("AZEL"),
            fl(J2000_MJD),
            s("VLA"),
        ],
    );
    let rust_xyz = extract_array3(&rust_result);

    let cpp_xyz = cpp_igrf_value(
        "xyz",
        Some("ITRF"),
        0.0,
        0.0,
        std::f64::consts::FRAC_PI_2,
        "AZEL",
        J2000_MJD,
        VLA_LON,
        VLA_LAT,
        VLA_H,
    )
    .unwrap();

    for i in 0..3 {
        assert!(
            close(rust_xyz[i], cpp_xyz[i], 75.0),
            "igrfxyz[{i}]: Rust={}, C++={}",
            rust_xyz[i],
            cpp_xyz[i]
        );
    }
}

#[test]
fn igrflos_matches_cpp() {
    let rust_result = eval_meas(
        "meas.igrflos",
        &[
            fl(0.0),
            fl(0.0),
            fl(std::f64::consts::FRAC_PI_2),
            s("AZEL"),
            fl(J2000_MJD),
            s("VLA"),
        ],
    );
    let rust_value = extract_float(&rust_result);

    let cpp_value = cpp_igrf_value(
        "los",
        None,
        0.0,
        0.0,
        std::f64::consts::FRAC_PI_2,
        "AZEL",
        J2000_MJD,
        VLA_LON,
        VLA_LAT,
        VLA_H,
    )
    .unwrap()[0];

    assert!(
        close(rust_value, cpp_value, 50.0),
        "igrflos: Rust={rust_value}, C++={cpp_value}"
    );
}

#[test]
fn igrflong_matches_cpp() {
    let rust_result = eval_meas(
        "meas.igrflong",
        &[
            fl(0.0),
            fl(0.0),
            fl(std::f64::consts::FRAC_PI_2),
            s("AZEL"),
            fl(J2000_MJD),
            s("VLA"),
        ],
    );
    let rust_value = extract_float(&rust_result);

    let cpp_value = cpp_igrf_value(
        "long",
        None,
        0.0,
        0.0,
        std::f64::consts::FRAC_PI_2,
        "AZEL",
        J2000_MJD,
        VLA_LON,
        VLA_LAT,
        VLA_H,
    )
    .unwrap()[0];

    assert!(
        close_angle(rust_value, cpp_value, 2e-5),
        "igrflong: Rust={rust_value}, C++={cpp_value}"
    );
}

#[test]
fn emang_quantity_scalars_match_cpp() {
    let rust_result = eval_meas(
        "meas.emang",
        &[
            s("J2000"),
            q(0.35, "rad"),
            q(-0.1, "rad"),
            q(48_000.0, "nT"),
            s("ITRF"),
            fl(J2000_MJD),
            s("VLA"),
        ],
    );
    let (rust_lon, rust_lat) = extract_dir(&rust_result);

    let (cpp_lon, cpp_lat) = cpp_earthmag_convert_angles(
        0.35, -0.1, 48_000.0, "ITRF", "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();

    assert!(
        close_angle(rust_lon, cpp_lon, 1e-7) && close(rust_lat, cpp_lat, 1e-7),
        "emang quantity scalars: Rust=({rust_lon},{rust_lat}), C++=({cpp_lon},{cpp_lat})"
    );
}

// ── Position: Rust meas.pos vs C++ position_convert ──

#[test]
fn pos_wgs84_to_itrf() {
    let rust_result = eval_meas(
        "meas.pos",
        &[s("ITRF"), fl(VLA_LON), fl(VLA_LAT), fl(VLA_H), s("WGS84")],
    );
    let rust_vals = extract_pos(&rust_result);

    let cpp_vals = cpp_position_convert(VLA_LON, VLA_LAT, VLA_H, "WGS84", "ITRF").unwrap();
    let cpp_arr = [cpp_vals.0, cpp_vals.1, cpp_vals.2];

    for i in 0..3 {
        assert!(
            close(rust_vals[i], cpp_arr[i], 1.0),
            "pos WGS84→ITRF[{i}]: Rust={}, C++={}",
            rust_vals[i],
            cpp_arr[i]
        );
    }
}

#[test]
fn pos_itrf_to_wgs84() {
    // VLA approximate ITRF
    let x = -1601185.0_f64;
    let y = -5041977.0_f64;
    let z = 3554876.0_f64;

    let rust_result = eval_meas("meas.pos", &[s("WGS84"), fl(x), fl(y), fl(z), s("ITRF")]);
    let rust_vals = extract_pos(&rust_result);

    let cpp_vals = cpp_position_convert(x, y, z, "ITRF", "WGS84").unwrap();
    let cpp_arr = [cpp_vals.0, cpp_vals.1, cpp_vals.2];

    for i in 0..3 {
        let tol = if i < 2 { 1e-8 } else { 1.0 }; // tighter for angles, looser for height
        assert!(
            close(rust_vals[i], cpp_arr[i], tol),
            "pos ITRF→WGS84[{i}]: Rust={}, C++={}",
            rust_vals[i],
            cpp_arr[i]
        );
    }
}

#[test]
fn pos_itrfllh_matches_cpp_record() {
    let rust_result = eval_meas(
        "meas.itrfllh",
        &[fl(VLA_LON), fl(VLA_LAT), fl(VLA_H), s("WGS84")],
    );
    let rust_vals = extract_pos(&rust_result);

    let cpp_itrf = cpp_position_convert(VLA_LON, VLA_LAT, VLA_H, "WGS84", "ITRF").unwrap();
    let cpp_vals = cpp_position_to_record(cpp_itrf.0, cpp_itrf.1, cpp_itrf.2).unwrap();
    let cpp_arr = [cpp_vals.0, cpp_vals.1, cpp_vals.2];

    for i in 0..3 {
        let tol = if i < 2 { 1e-8 } else { 1e-3 };
        assert!(
            close(rust_vals[i], cpp_arr[i], tol),
            "pos ITRFLLH[{i}]: Rust={}, C++={}",
            rust_vals[i],
            cpp_arr[i]
        );
    }
}

#[test]
fn pos_wgsllh_matches_cpp() {
    let x = -1601185.0_f64;
    let y = -5041977.0_f64;
    let z = 3554876.0_f64;

    let rust_result = eval_meas("meas.wgsllh", &[fl(x), fl(y), fl(z), s("ITRF")]);
    let rust_vals = extract_pos(&rust_result);

    let cpp_vals = cpp_position_convert(x, y, z, "ITRF", "WGS84").unwrap();
    let cpp_arr = [cpp_vals.0, cpp_vals.1, cpp_vals.2];

    for i in 0..3 {
        let tol = if i < 2 { 1e-8 } else { 1.0 };
        assert!(
            close(rust_vals[i], cpp_arr[i], tol),
            "pos WGSLLH[{i}]: Rust={}, C++={}",
            rust_vals[i],
            cpp_arr[i]
        );
    }
}

#[test]
fn pos_wgsxyz_matches_cpp_raw_value() {
    let rust_vals = extract_pos(&eval_meas(
        "meas.wgsxyz",
        &[fl(VLA_LON), fl(VLA_LAT), fl(VLA_H), s("WGS84")],
    ));
    let cpp_vals = cpp_position_to_wgs_xyz(VLA_LON, VLA_LAT, VLA_H, "WGS84").unwrap();
    let cpp_arr = [cpp_vals.0, cpp_vals.1, cpp_vals.2];

    for i in 0..3 {
        assert!(
            close(rust_vals[i], cpp_arr[i], 1e-8),
            "pos WGSXYZ[{i}]: Rust={}, C++={}",
            rust_vals[i],
            cpp_arr[i]
        );
    }
}

// ── Doppler: Rust meas.doppler vs C++ doppler_convert ──

#[test]
fn doppler_radio_to_z() {
    let rust_result = eval_meas("meas.doppler", &[s("Z"), fl(0.5), s("RADIO")]);
    let rust_val = extract_float(&rust_result);

    let cpp_val = cpp_doppler_convert(0.5, "RADIO", "Z").unwrap();

    assert!(
        close(rust_val, cpp_val, 1e-10),
        "doppler RADIO→Z: Rust={rust_val}, C++={cpp_val}"
    );
}

#[test]
fn doppler_z_to_beta() {
    let rust_result = eval_meas("meas.doppler", &[s("BETA"), fl(1.0), s("Z")]);
    let rust_val = extract_float(&rust_result);

    let cpp_val = cpp_doppler_convert(1.0, "Z", "BETA").unwrap();

    assert!(
        close(rust_val, cpp_val, 1e-10),
        "doppler Z→BETA: Rust={rust_val}, C++={cpp_val}"
    );
}

// ── Frequency: Rust meas.freq vs C++ frequency_convert ──

#[test]
fn freq_lsrk_to_bary() {
    // The Rust meas UDF takes ITRF position args; C++ wrapper takes WGS84.
    // First convert VLA from WGS84 to ITRF for the Rust side.
    let vla_itrf = extract_pos(&eval_meas(
        "meas.pos",
        &[s("ITRF"), fl(VLA_LON), fl(VLA_LAT), fl(VLA_H), s("WGS84")],
    ));

    let rust_result = eval_meas(
        "meas.freq",
        &[
            s("BARY"),
            fl(1.4e9),
            s("LSRK"),
            fl(M31_LON),
            fl(M31_LAT),
            fl(J2000_MJD),
            fl(vla_itrf[0]),
            fl(vla_itrf[1]),
            fl(vla_itrf[2]),
        ],
    );
    let rust_hz = extract_float(&rust_result);

    let cpp_hz = cpp_frequency_convert(
        1.4e9, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();

    let rel_diff = (rust_hz - cpp_hz).abs() / cpp_hz;
    assert!(
        rel_diff < 1e-6,
        "freq LSRK→BARY: Rust={rust_hz}, C++={cpp_hz}, rel_diff={rel_diff}"
    );
}

#[test]
fn freq_lsrk_to_rest_with_radvel() {
    let rust_result = eval_meas(
        "meas.freq",
        &[
            s("REST"),
            fl(1.4e9),
            s("LSRK"),
            fl(50_000.0),
            s("LSRK"),
            fl(M31_LON),
            fl(M31_LAT),
            fl(J2000_MJD),
        ],
    );
    let rust_hz = extract_float(&rust_result);

    let cpp_hz = cpp_frequency_convert_with_rv(
        1.4e9, "LSRK", "REST", M31_LON, M31_LAT, "J2000", J2000_MJD, 0.0, 0.0, 0.0, 50_000.0,
        "LSRK",
    )
    .unwrap();

    let rel_diff = (rust_hz - cpp_hz).abs() / cpp_hz;
    assert!(
        rel_diff < 1e-6,
        "freq LSRK→REST: Rust={rust_hz}, C++={cpp_hz}, rel_diff={rel_diff}"
    );
}

#[test]
fn rest_with_doppler_matches_cpp() {
    let rust_result = eval_meas("meas.rest", &[fl(1.0e9), s("LSRK"), fl(0.5), s("RADIO")]);
    let rust_hz = extract_float(&rust_result);

    let cpp_hz = cpp_frequency_rest_with_doppler(1.0e9, "LSRK", 0.5, "RADIO").unwrap();
    assert!(close(rust_hz, cpp_hz, 1e-6));
}

#[test]
fn shift_with_doppler_matches_cpp() {
    let rust_result = eval_meas("meas.shift", &[fl(1.0e9), s("LSRK"), fl(0.5), s("RADIO")]);
    let rust_hz = extract_float(&rust_result);

    let cpp_hz = cpp_frequency_shift_with_doppler(1.0e9, "LSRK", 0.5, "RADIO").unwrap();
    assert!(close(rust_hz, cpp_hz, 1e-6));
}

// ── Radial velocity: Rust meas.radvel vs C++ radvel_convert ──

#[test]
fn radvel_lsrk_to_bary() {
    let vla_itrf = extract_pos(&eval_meas(
        "meas.pos",
        &[s("ITRF"), fl(VLA_LON), fl(VLA_LAT), fl(VLA_H), s("WGS84")],
    ));

    let rust_result = eval_meas(
        "meas.radvel",
        &[
            s("BARY"),
            fl(1000.0),
            s("LSRK"),
            fl(M31_LON),
            fl(M31_LAT),
            fl(J2000_MJD),
            fl(vla_itrf[0]),
            fl(vla_itrf[1]),
            fl(vla_itrf[2]),
        ],
    );
    let rust_ms = extract_float(&rust_result);

    let cpp_ms = cpp_radvel_convert(
        1000.0, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();

    let diff = (rust_ms - cpp_ms).abs();
    assert!(
        diff < 1.0,
        "radvel LSRK→BARY: Rust={rust_ms}, C++={cpp_ms}, diff={diff} m/s"
    );
}
