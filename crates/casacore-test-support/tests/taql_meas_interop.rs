// SPDX-License-Identifier: LGPL-3.0-or-later
//! Interop tests for TaQL measure UDFs: Rust meas.* vs C++ casacore conversions.
//!
//! Compares numeric outputs from:
//! - RR: Rust TaQL `meas.*` functions
//! - CC: C++ direct measure conversion APIs (baseline)
//!
//! These tests verify that the Rust TaQL meas UDF layer produces results
//! consistent with C++ casacore's measure conversion engine.
#![cfg(has_casacore_cpp)]

use casacore_tables::taql::ast::IndexStyle;
use casacore_tables::taql::eval::{EvalContext, ExprValue};
use casacore_tables::taql::functions::call_function;
use casacore_test_support::measures_interop::{
    cpp_direction_convert, cpp_doppler_convert, cpp_epoch_convert, cpp_frequency_convert,
    cpp_position_convert, cpp_radvel_convert,
};
use casacore_types::RecordValue;

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
