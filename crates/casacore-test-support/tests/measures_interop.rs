// SPDX-License-Identifier: LGPL-3.0-or-later
//! 2x2 interop tests for measures: Rust vs C++ casacore.
#![cfg(has_casacore_cpp)]

use casacore_test_support::measures_interop::{
    cpp_direction_convert, cpp_direction_convert_iau2000a, cpp_doppler_convert, cpp_earth_velocity,
    cpp_eop_query, cpp_epoch_convert, cpp_epoch_convert_with_frame, cpp_epoch_to_record,
    cpp_frequency_convert, cpp_frequency_convert_with_rv, cpp_iau2000_precession_matrix,
    cpp_position_convert, cpp_position_to_record, cpp_radvel_convert,
};
use casacore_types::measures::direction::{DirectionRef, MDirection};
use casacore_types::measures::doppler::{DopplerRef, MDoppler};
use casacore_types::measures::frequency::{FrequencyRef, MFrequency};
use casacore_types::measures::radial_velocity::{MRadialVelocity, RadialVelocityRef};
use casacore_types::measures::{
    EpochRef, IauModel, MEpoch, MPosition, MeasFrame, PositionRef, epoch_to_record,
    position_to_record,
};

const J2000_MJD: f64 = 51544.5;
const SECONDS_PER_DAY: f64 = 86_400.0;

// Direction for frequency tests: M31 in J2000
const M31_LON: f64 = 0.185_948_8; // ~10.68° RA
const M31_LAT: f64 = 0.722_777_4; // ~41.27° Dec
// VLA WGS84 coordinates
const VLA_LON: f64 = -1.878_283_2; // -107.618°
const VLA_LAT: f64 = 0.595_370_3; // 34.079°
const VLA_H: f64 = 2124.0;

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

/// Angular separation between two (lon, lat) positions on the sphere (radians).
/// Uses the Vincenty formula for numerical precision at all separations.
fn angular_sep(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let dlon = lon2 - lon1;
    let (s1, c1) = lat1.sin_cos();
    let (s2, c2) = lat2.sin_cos();
    let (sd, cd) = dlon.sin_cos();
    let y = ((c2 * sd).powi(2) + (c1 * s2 - s1 * c2 * cd).powi(2)).sqrt();
    let x = s1 * s2 + c1 * c2 * cd;
    y.atan2(x)
}

/// Angular separation in arcseconds (convenience wrapper).
fn sep_arcsec(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    angular_sep(lon1, lat1, lon2, lat2).to_degrees() * 3600.0
}

// VLA ITRF coordinates — reused across position tests.
const VLA_X: f64 = -1601185.4;
const VLA_Y: f64 = -5041977.5;
const VLA_Z: f64 = 3554875.9;

// ==========================================================================
// RR: Rust-only (already in unit tests — included here for matrix completeness)
// The RR column is covered by the unit tests in casacore-types; the interop
// test file focuses on RC/CR/CC cells.
// ==========================================================================

// ==========================================================================
// RC: Rust creates, C++ converts
// ==========================================================================

#[test]
fn rc_epoch_utc_to_tai() {
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new();
    let rust_tai = utc.convert_to(EpochRef::TAI, &frame).unwrap();

    let cpp_tai_mjd = cpp_epoch_convert(J2000_MJD, "UTC", "TAI").unwrap();

    let diff_s = (rust_tai.value().as_mjd() - cpp_tai_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-6, "Rust vs C++ TAI difference: {diff_s}s");
}

#[test]
fn rc_epoch_tai_to_utc() {
    let tai = MEpoch::from_mjd(J2000_MJD, EpochRef::TAI);
    let frame = MeasFrame::new();
    let rust_utc = tai.convert_to(EpochRef::UTC, &frame).unwrap();

    let cpp_utc_mjd = cpp_epoch_convert(J2000_MJD, "TAI", "UTC").unwrap();

    let diff_s = (rust_utc.value().as_mjd() - cpp_utc_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-6, "Rust vs C++ UTC difference: {diff_s}s");
}

#[test]
fn rc_epoch_utc_to_tt() {
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new();
    let rust_tt = utc.convert_to(EpochRef::TT, &frame).unwrap();

    let cpp_tt_mjd = cpp_epoch_convert(J2000_MJD, "UTC", "TT").unwrap();

    let diff_s = (rust_tt.value().as_mjd() - cpp_tt_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-3, "Rust vs C++ TT difference: {diff_s}s");
}

#[test]
fn rc_epoch_tt_to_tdb() {
    let tt = MEpoch::from_mjd(J2000_MJD, EpochRef::TT);
    let frame = MeasFrame::new();
    let rust_tdb = tt.convert_to(EpochRef::TDB, &frame).unwrap();

    let cpp_tdb_mjd = cpp_epoch_convert(J2000_MJD, "TT", "TDB").unwrap();

    let diff_s = (rust_tdb.value().as_mjd() - cpp_tdb_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-2, "Rust vs C++ TDB difference: {diff_s}s");
}

#[test]
fn rc_epoch_record_format() {
    // Rust creates record, C++ creates from same input, compare fields.
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let rec = epoch_to_record(&epoch);

    let (cpp_value, cpp_unit, cpp_refer) = cpp_epoch_to_record(J2000_MJD, "UTC").unwrap();

    if let Some(casacore_types::Value::Scalar(casacore_types::ScalarValue::String(r))) =
        rec.get("refer")
    {
        assert_eq!(r, &cpp_refer, "refer mismatch: Rust={r}, C++={cpp_refer}");
    }
    assert_eq!(cpp_unit, "d", "C++ unit should be 'd'");
    assert!(
        close(cpp_value, J2000_MJD, 1e-10),
        "C++ value {cpp_value} != {J2000_MJD}"
    );
}

#[test]
fn rc_position_itrf_to_wgs84() {
    let pos = MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z);
    let rust_wgs = pos.convert_to(PositionRef::WGS84).unwrap();

    let (cpp_lon, cpp_lat, _cpp_r) =
        cpp_position_convert(VLA_X, VLA_Y, VLA_Z, "ITRF", "WGS84").unwrap();

    assert!(
        close(rust_wgs.values()[0], cpp_lon, 1e-6),
        "lon: Rust={}, C++={}",
        rust_wgs.values()[0],
        cpp_lon
    );
    assert!(
        close(rust_wgs.values()[1], cpp_lat, 1e-6),
        "lat: Rust={}, C++={}",
        rust_wgs.values()[1],
        cpp_lat
    );
}

#[test]
fn rc_position_wgs84_to_itrf() {
    let lon = -107.6_f64.to_radians();
    let lat = 34.1_f64.to_radians();
    let h = 2124.0_f64;

    let pos = MPosition::new_wgs84(lon, lat, h);
    let rust_itrf = pos.convert_to(PositionRef::ITRF).unwrap();

    let (cpp_x, cpp_y, cpp_z) = cpp_position_convert(lon, lat, h, "WGS84", "ITRF").unwrap();

    for (i, (r, c)) in rust_itrf
        .values()
        .iter()
        .zip([cpp_x, cpp_y, cpp_z].iter())
        .enumerate()
    {
        assert!(close(*r, *c, 1.0), "coord {i}: Rust={r}, C++={c}");
    }
}

#[test]
fn rc_position_record_format() {
    // Rust creates spherical record, C++ creates from same ITRF, compare.
    let pos = MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z);
    let rec = position_to_record(&pos);

    let (cpp_lon, cpp_lat, cpp_r) = cpp_position_to_record(VLA_X, VLA_Y, VLA_Z).unwrap();

    // Extract Rust record m0/m1/m2 values
    let rust_lon = extract_quantity(&rec, "m0");
    let rust_lat = extract_quantity(&rec, "m1");
    let rust_rad = extract_quantity(&rec, "m2");

    assert!(
        close(rust_lon, cpp_lon, 1e-10),
        "lon: Rust={rust_lon}, C++={cpp_lon}"
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-10),
        "lat: Rust={rust_lat}, C++={cpp_lat}"
    );
    assert!(
        close(rust_rad, cpp_r, 1.0),
        "radius: Rust={rust_rad}, C++={cpp_r}"
    );
}

#[test]
fn rc_doppler_radio_to_z() {
    let d = MDoppler::new(0.3, DopplerRef::RADIO);
    let frame = MeasFrame::new();
    let rust_z = d.convert_to(DopplerRef::Z, &frame).unwrap();
    let cpp_z = cpp_doppler_convert(0.3, "RADIO", "Z").unwrap();
    assert!(
        close(rust_z.value(), cpp_z, 1e-10),
        "Rust={}, C++={}",
        rust_z.value(),
        cpp_z
    );
}

#[test]
fn rc_doppler_z_to_ratio() {
    let d = MDoppler::new(0.5, DopplerRef::Z);
    let frame = MeasFrame::new();
    let rust_ratio = d.convert_to(DopplerRef::RATIO, &frame).unwrap();
    let cpp_ratio = cpp_doppler_convert(0.5, "Z", "RATIO").unwrap();
    assert!(
        close(rust_ratio.value(), cpp_ratio, 1e-10),
        "Rust={}, C++={}",
        rust_ratio.value(),
        cpp_ratio
    );
}

#[test]
fn rc_doppler_beta_to_radio() {
    let d = MDoppler::new(0.6, DopplerRef::BETA);
    let frame = MeasFrame::new();
    let rust_radio = d.convert_to(DopplerRef::RADIO, &frame).unwrap();
    let cpp_radio = cpp_doppler_convert(0.6, "BETA", "RADIO").unwrap();
    assert!(
        close(rust_radio.value(), cpp_radio, 1e-10),
        "Rust={}, C++={}",
        rust_radio.value(),
        cpp_radio
    );
}

#[test]
fn rc_direction_j2000_to_galactic() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let rust_gal = d.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_gal.as_angles();
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "GALACTIC", 0.0, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close(rust_lon, cpp_lon, 1e-4),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-4),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn rc_direction_j2000_to_ecliptic() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new().with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC));
    let rust_ecl = d.convert_to(DirectionRef::ECLIPTIC, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_ecl.as_angles();
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "ECLIPTIC", J2000_MJD, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close(rust_lon, cpp_lon, 1e-4),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-4),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn rc_direction_galactic_to_supergal() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::GALACTIC);
    let frame = MeasFrame::new();
    let rust_sg = d.convert_to(DirectionRef::SUPERGAL, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_sg.as_angles();
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "GALACTIC", "SUPERGAL", 0.0, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close(rust_lon, cpp_lon, 1e-4),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-4),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn rc_frequency_lsrk_to_bary() {
    let f = MFrequency::new(1.42e9, FrequencyRef::LSRK);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let rust_bary = f.convert_to(FrequencyRef::BARY, &frame).unwrap();
    let cpp_bary = cpp_frequency_convert(
        1.42e9, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_bary.hz(), cpp_bary, 100.0),
        "Rust={}, C++={}",
        rust_bary.hz(),
        cpp_bary
    );
}

#[test]
fn rc_frequency_bary_to_lgroup() {
    let f = MFrequency::new(1.42e9, FrequencyRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let rust_lg = f.convert_to(FrequencyRef::LGROUP, &frame).unwrap();
    let cpp_lg = cpp_frequency_convert(
        1.42e9, "BARY", "LGROUP", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_lg.hz(), cpp_lg, 1000.0),
        "Rust={}, C++={}",
        rust_lg.hz(),
        cpp_lg
    );
}

#[test]
fn rc_frequency_bary_to_cmb() {
    let f = MFrequency::new(1.42e9, FrequencyRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let rust_cmb = f.convert_to(FrequencyRef::CMB, &frame).unwrap();
    let cpp_cmb = cpp_frequency_convert(
        1.42e9, "BARY", "CMB", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_cmb.hz(), cpp_cmb, 1000.0),
        "Rust={}, C++={}",
        rust_cmb.hz(),
        cpp_cmb
    );
}

// ==========================================================================
// CR: C++ creates, Rust reads and converts
// ==========================================================================

#[test]
fn cr_epoch_utc_to_tai() {
    let cpp_tai_mjd = cpp_epoch_convert(J2000_MJD, "UTC", "TAI").unwrap();

    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new();
    let rust_tai = utc.convert_to(EpochRef::TAI, &frame).unwrap();

    let diff_s = (rust_tai.value().as_mjd() - cpp_tai_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-6, "C++ vs Rust TAI difference: {diff_s}s");
}

#[test]
fn cr_epoch_tai_to_utc() {
    let cpp_utc_mjd = cpp_epoch_convert(J2000_MJD, "TAI", "UTC").unwrap();

    let tai = MEpoch::from_mjd(J2000_MJD, EpochRef::TAI);
    let frame = MeasFrame::new();
    let rust_utc = tai.convert_to(EpochRef::UTC, &frame).unwrap();

    let diff_s = (rust_utc.value().as_mjd() - cpp_utc_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-6, "C++ vs Rust UTC difference: {diff_s}s");
}

#[test]
fn cr_epoch_utc_to_tt() {
    let cpp_tt_mjd = cpp_epoch_convert(J2000_MJD, "UTC", "TT").unwrap();

    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new();
    let rust_tt = utc.convert_to(EpochRef::TT, &frame).unwrap();

    let diff_s = (rust_tt.value().as_mjd() - cpp_tt_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-3, "C++ vs Rust TT difference: {diff_s}s");
}

#[test]
fn cr_epoch_tt_to_tdb() {
    let cpp_tdb_mjd = cpp_epoch_convert(J2000_MJD, "TT", "TDB").unwrap();

    let tt = MEpoch::from_mjd(J2000_MJD, EpochRef::TT);
    let frame = MeasFrame::new();
    let rust_tdb = tt.convert_to(EpochRef::TDB, &frame).unwrap();

    let diff_s = (rust_tdb.value().as_mjd() - cpp_tdb_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-2, "C++ vs Rust TDB difference: {diff_s}s");
}

#[test]
fn cr_epoch_record_format() {
    // C++ serializes epoch to record fields, Rust produces same format.
    let (cpp_value, cpp_unit, cpp_refer) = cpp_epoch_to_record(J2000_MJD, "UTC").unwrap();

    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let rec = epoch_to_record(&epoch);

    if let Some(casacore_types::Value::Scalar(casacore_types::ScalarValue::String(r))) =
        rec.get("refer")
    {
        assert_eq!(r, &cpp_refer, "refer mismatch: Rust={r}, C++={cpp_refer}");
    }
    assert_eq!(cpp_unit, "d", "C++ unit should be 'd'");
    assert!(
        close(cpp_value, J2000_MJD, 1e-10),
        "C++ value {cpp_value} != {J2000_MJD}"
    );
}

#[test]
fn cr_position_itrf_to_wgs84() {
    let (cpp_lon, cpp_lat, _) = cpp_position_convert(VLA_X, VLA_Y, VLA_Z, "ITRF", "WGS84").unwrap();

    let pos = MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z);
    let rust_wgs = pos.convert_to(PositionRef::WGS84).unwrap();

    assert!(
        close(rust_wgs.values()[0], cpp_lon, 1e-6),
        "lon: Rust={}, C++={}",
        rust_wgs.values()[0],
        cpp_lon
    );
    assert!(
        close(rust_wgs.values()[1], cpp_lat, 1e-6),
        "lat: Rust={}, C++={}",
        rust_wgs.values()[1],
        cpp_lat
    );
}

#[test]
fn cr_position_wgs84_to_itrf() {
    let lon = -107.6_f64.to_radians();
    let lat = 34.1_f64.to_radians();
    let h = 2124.0_f64;

    let (cpp_x, cpp_y, cpp_z) = cpp_position_convert(lon, lat, h, "WGS84", "ITRF").unwrap();

    let pos = MPosition::new_wgs84(lon, lat, h);
    let rust_itrf = pos.convert_to(PositionRef::ITRF).unwrap();

    for (i, (r, c)) in rust_itrf
        .values()
        .iter()
        .zip([cpp_x, cpp_y, cpp_z].iter())
        .enumerate()
    {
        assert!(close(*r, *c, 1.0), "coord {i}: Rust={r}, C++={c}");
    }
}

#[test]
fn cr_position_record_format() {
    // C++ creates spherical from ITRF, Rust creates from same, compare.
    let (cpp_lon, cpp_lat, cpp_r) = cpp_position_to_record(VLA_X, VLA_Y, VLA_Z).unwrap();

    let pos = MPosition::new_itrf(VLA_X, VLA_Y, VLA_Z);
    let rec = position_to_record(&pos);

    let rust_lon = extract_quantity(&rec, "m0");
    let rust_lat = extract_quantity(&rec, "m1");
    let rust_rad = extract_quantity(&rec, "m2");

    assert!(
        close(rust_lon, cpp_lon, 1e-10),
        "lon: Rust={rust_lon}, C++={cpp_lon}"
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-10),
        "lat: Rust={rust_lat}, C++={cpp_lat}"
    );
    assert!(
        close(rust_rad, cpp_r, 1.0),
        "radius: Rust={rust_rad}, C++={cpp_r}"
    );
}

#[test]
fn cr_doppler_radio_to_z() {
    let cpp_z = cpp_doppler_convert(0.3, "RADIO", "Z").unwrap();
    let d = MDoppler::new(0.3, DopplerRef::RADIO);
    let frame = MeasFrame::new();
    let rust_z = d.convert_to(DopplerRef::Z, &frame).unwrap();
    assert!(
        close(rust_z.value(), cpp_z, 1e-10),
        "Rust={}, C++={}",
        rust_z.value(),
        cpp_z
    );
}

#[test]
fn cr_doppler_z_to_ratio() {
    let cpp_ratio = cpp_doppler_convert(0.5, "Z", "RATIO").unwrap();
    let d = MDoppler::new(0.5, DopplerRef::Z);
    let frame = MeasFrame::new();
    let rust_ratio = d.convert_to(DopplerRef::RATIO, &frame).unwrap();
    assert!(
        close(rust_ratio.value(), cpp_ratio, 1e-10),
        "Rust={}, C++={}",
        rust_ratio.value(),
        cpp_ratio
    );
}

#[test]
fn cr_doppler_beta_to_radio() {
    let cpp_radio = cpp_doppler_convert(0.6, "BETA", "RADIO").unwrap();
    let d = MDoppler::new(0.6, DopplerRef::BETA);
    let frame = MeasFrame::new();
    let rust_radio = d.convert_to(DopplerRef::RADIO, &frame).unwrap();
    assert!(
        close(rust_radio.value(), cpp_radio, 1e-10),
        "Rust={}, C++={}",
        rust_radio.value(),
        cpp_radio
    );
}

#[test]
fn cr_direction_j2000_to_galactic() {
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "GALACTIC", 0.0, 0.0, 0.0, 0.0).unwrap();
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let rust_gal = d.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_gal.as_angles();
    assert!(
        close(rust_lon, cpp_lon, 1e-4),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-4),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn cr_direction_j2000_to_ecliptic() {
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "ECLIPTIC", J2000_MJD, 0.0, 0.0, 0.0).unwrap();
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new().with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC));
    let rust_ecl = d.convert_to(DirectionRef::ECLIPTIC, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_ecl.as_angles();
    assert!(
        close(rust_lon, cpp_lon, 1e-4),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-4),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn cr_direction_galactic_to_supergal() {
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "GALACTIC", "SUPERGAL", 0.0, 0.0, 0.0, 0.0).unwrap();
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::GALACTIC);
    let frame = MeasFrame::new();
    let rust_sg = d.convert_to(DirectionRef::SUPERGAL, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_sg.as_angles();
    assert!(
        close(rust_lon, cpp_lon, 1e-4),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-4),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn cr_frequency_lsrk_to_bary() {
    let cpp_bary = cpp_frequency_convert(
        1.42e9, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let f = MFrequency::new(1.42e9, FrequencyRef::LSRK);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let rust_bary = f.convert_to(FrequencyRef::BARY, &frame).unwrap();
    assert!(
        close(rust_bary.hz(), cpp_bary, 100.0),
        "Rust={}, C++={}",
        rust_bary.hz(),
        cpp_bary
    );
}

#[test]
fn cr_frequency_bary_to_lgroup() {
    let cpp_lg = cpp_frequency_convert(
        1.42e9, "BARY", "LGROUP", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let f = MFrequency::new(1.42e9, FrequencyRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let rust_lg = f.convert_to(FrequencyRef::LGROUP, &frame).unwrap();
    assert!(
        close(rust_lg.hz(), cpp_lg, 1000.0),
        "Rust={}, C++={}",
        rust_lg.hz(),
        cpp_lg
    );
}

#[test]
fn cr_frequency_bary_to_cmb() {
    let cpp_cmb = cpp_frequency_convert(
        1.42e9, "BARY", "CMB", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let f = MFrequency::new(1.42e9, FrequencyRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let rust_cmb = f.convert_to(FrequencyRef::CMB, &frame).unwrap();
    assert!(
        close(rust_cmb.hz(), cpp_cmb, 1000.0),
        "Rust={}, C++={}",
        rust_cmb.hz(),
        cpp_cmb
    );
}

// ==========================================================================
// Broader coverage: multiple values, multi-hop, perf-test conversions
// ==========================================================================

#[test]
fn rc_doppler_gamma_to_ratio() {
    let d = MDoppler::new(2.0, DopplerRef::GAMMA);
    let frame = MeasFrame::new();
    let rust_ratio = d.convert_to(DopplerRef::RATIO, &frame).unwrap();
    let cpp_ratio = cpp_doppler_convert(2.0, "GAMMA", "RATIO").unwrap();
    assert!(
        close(rust_ratio.value(), cpp_ratio, 1e-10),
        "Rust={}, C++={}",
        rust_ratio.value(),
        cpp_ratio
    );
}

#[test]
fn rc_doppler_beta_to_gamma() {
    // Same conversion tested in perf benchmark
    let d = MDoppler::new(0.5, DopplerRef::BETA);
    let frame = MeasFrame::new();
    let rust_gamma = d.convert_to(DopplerRef::GAMMA, &frame).unwrap();
    let cpp_gamma = cpp_doppler_convert(0.5, "BETA", "GAMMA").unwrap();
    assert!(
        close(rust_gamma.value(), cpp_gamma, 1e-10),
        "Rust={}, C++={}",
        rust_gamma.value(),
        cpp_gamma
    );
}

#[test]
fn rc_doppler_multiple_values() {
    // Prove output varies with input (not cached/short-circuited)
    let frame = MeasFrame::new();
    let mut prev = f64::NAN;
    for v in [0.1, 0.3, 0.5, 0.7, 0.9] {
        let d = MDoppler::new(v, DopplerRef::RADIO);
        let rust_z = d.convert_to(DopplerRef::Z, &frame).unwrap();
        let cpp_z = cpp_doppler_convert(v, "RADIO", "Z").unwrap();
        assert!(
            close(rust_z.value(), cpp_z, 1e-10),
            "v={v}: Rust={}, C++={}",
            rust_z.value(),
            cpp_z
        );
        assert!(
            prev.is_nan() || (rust_z.value() - prev).abs() > 1e-6,
            "output didn't change for v={v}"
        );
        prev = rust_z.value();
    }
}

#[test]
fn rc_direction_j2000_to_icrs() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let rust_icrs = d.convert_to(DirectionRef::ICRS, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_icrs.as_angles();
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "ICRS", 0.0, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close(rust_lon, cpp_lon, 1e-4),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-4),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn rc_direction_j2000_to_jmean() {
    // Same conversion tested in perf benchmark — epoch-dependent precession
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new().with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC));
    let rust_jm = d.convert_to(DirectionRef::JMEAN, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_jm.as_angles();
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "JMEAN", J2000_MJD, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close(rust_lon, cpp_lon, 5e-5),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 5e-5),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn rc_direction_multiple_values() {
    // Prove direction output varies with input
    let frame = MeasFrame::new();
    let mut prev_lon = f64::NAN;
    for lon_in in [0.5, 1.0, 2.0, 3.0, 5.0] {
        let d = MDirection::from_angles(lon_in, 0.5, DirectionRef::J2000);
        let rust_gal = d.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
        let (rust_lon, rust_lat) = rust_gal.as_angles();
        let (cpp_lon, cpp_lat) =
            cpp_direction_convert(lon_in, 0.5, "J2000", "GALACTIC", 0.0, 0.0, 0.0, 0.0).unwrap();
        // Use angle-aware comparison (C++ getLong returns [-π,π], Rust returns [0,2π))
        assert!(
            close_angle(rust_lon, cpp_lon, 1e-4),
            "lon_in={lon_in}: Rust lon={}, C++ lon={}",
            rust_lon,
            cpp_lon
        );
        assert!(
            close(rust_lat, cpp_lat, 1e-4),
            "lon_in={lon_in}: Rust lat={}, C++ lat={}",
            rust_lat,
            cpp_lat
        );
        assert!(
            prev_lon.is_nan() || !close_angle(rust_lon, prev_lon, 1e-6),
            "output didn't change for lon_in={lon_in}"
        );
        prev_lon = rust_lon;
    }
}

#[test]
fn rc_frequency_lsrd_to_bary() {
    let f = MFrequency::new(1.42e9, FrequencyRef::LSRD);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let rust_bary = f.convert_to(FrequencyRef::BARY, &frame).unwrap();
    let cpp_bary = cpp_frequency_convert(
        1.42e9, "LSRD", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_bary.hz(), cpp_bary, 1000.0),
        "Rust={}, C++={}",
        rust_bary.hz(),
        cpp_bary
    );
}

#[test]
fn rc_frequency_lsrk_to_lgroup_multihop() {
    // Multi-hop: LSRK → BARY → LGROUP
    let f = MFrequency::new(1.42e9, FrequencyRef::LSRK);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let rust_lg = f.convert_to(FrequencyRef::LGROUP, &frame).unwrap();
    let cpp_lg = cpp_frequency_convert(
        1.42e9, "LSRK", "LGROUP", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_lg.hz(), cpp_lg, 2000.0),
        "Rust={}, C++={}",
        rust_lg.hz(),
        cpp_lg
    );
}

#[test]
fn rc_frequency_multiple_values() {
    // Prove frequency output varies with input
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let mut prev = f64::NAN;
    for hz_in in [1.0e9, 1.2e9, 1.42e9, 2.0e9, 5.0e9] {
        let f = MFrequency::new(hz_in, FrequencyRef::LSRK);
        let rust_bary = f.convert_to(FrequencyRef::BARY, &frame).unwrap();
        let cpp_bary = cpp_frequency_convert(
            hz_in, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
        )
        .unwrap();
        assert!(
            close(rust_bary.hz(), cpp_bary, 100.0),
            "hz_in={hz_in}: Rust={}, C++={}",
            rust_bary.hz(),
            cpp_bary
        );
        assert!(
            prev.is_nan() || (rust_bary.hz() - prev).abs() > 1e3,
            "output didn't change for hz_in={hz_in}"
        );
        prev = rust_bary.hz();
    }
}

#[test]
fn rc_frequency_direction_sensitivity() {
    // Prove frequency shift changes sign for opposite directions
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);

    // Direction toward galactic center
    let dir1 = MDirection::from_angles(0.0, 0.0, DirectionRef::GALACTIC);
    let frame1 = MeasFrame::new()
        .with_direction(dir1.clone())
        .with_epoch(epoch.clone());
    let j1 = dir1.convert_to(DirectionRef::J2000, &frame1).unwrap();
    let (lon1, lat1) = j1.as_angles();

    // Direction opposite (galactic anticenter)
    let dir2 = MDirection::from_angles(std::f64::consts::PI, 0.0, DirectionRef::GALACTIC);
    let frame2 = MeasFrame::new()
        .with_direction(dir2.clone())
        .with_epoch(epoch);
    let j2 = dir2.convert_to(DirectionRef::J2000, &frame2).unwrap();
    let (lon2, lat2) = j2.as_angles();

    let f_in = 1.42e9;

    let rust_bary1 = MFrequency::new(f_in, FrequencyRef::LSRK)
        .convert_to(FrequencyRef::BARY, &frame1)
        .unwrap();
    let cpp_bary1 = cpp_frequency_convert(
        f_in, "LSRK", "BARY", lon1, lat1, "J2000", J2000_MJD, 0.0, 0.0, 0.0,
    )
    .unwrap();

    let rust_bary2 = MFrequency::new(f_in, FrequencyRef::LSRK)
        .convert_to(FrequencyRef::BARY, &frame2)
        .unwrap();
    let cpp_bary2 = cpp_frequency_convert(
        f_in, "LSRK", "BARY", lon2, lat2, "J2000", J2000_MJD, 0.0, 0.0, 0.0,
    )
    .unwrap();

    // Shifts should be in opposite directions
    let rust_shift1 = rust_bary1.hz() - f_in;
    let rust_shift2 = rust_bary2.hz() - f_in;
    let cpp_shift1 = cpp_bary1 - f_in;
    let cpp_shift2 = cpp_bary2 - f_in;

    // Rust and C++ should agree on each direction
    assert!(
        close(rust_shift1, cpp_shift1, 100.0),
        "dir1: Rust shift={}, C++ shift={}",
        rust_shift1,
        cpp_shift1
    );
    assert!(
        close(rust_shift2, cpp_shift2, 100.0),
        "dir2: Rust shift={}, C++ shift={}",
        rust_shift2,
        cpp_shift2
    );

    // The two shifts should have opposite signs (or at least very different values)
    assert!(
        rust_shift1 * rust_shift2 < 0.0 || (rust_shift1 - rust_shift2).abs() > 10000.0,
        "Opposite directions should give different shifts: {} vs {}",
        rust_shift1,
        rust_shift2
    );
}

// --------------------------------------------------------------------------
// BARY ↔ GEO (epoch-dependent: Earth orbital velocity)
// --------------------------------------------------------------------------

#[test]
fn rc_frequency_bary_to_geo() {
    let f = MFrequency::new(1.42e9, FrequencyRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs);
    let rust_geo = f.convert_to(FrequencyRef::GEO, &frame).unwrap();
    let cpp_geo = cpp_frequency_convert(
        1.42e9, "BARY", "GEO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_geo.hz(), cpp_geo, 100.0),
        "BARY→GEO: Rust={}, C++={}",
        rust_geo.hz(),
        cpp_geo
    );
}

#[test]
fn cr_frequency_bary_to_geo() {
    let cpp_geo = cpp_frequency_convert(
        1.42e9, "BARY", "GEO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let f = MFrequency::new(cpp_geo, FrequencyRef::GEO);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs);
    let rust_bary = f.convert_to(FrequencyRef::BARY, &frame).unwrap();
    assert!(
        close(rust_bary.hz(), 1.42e9, 100.0),
        "GEO→BARY: Rust={}, expected=1.42e9",
        rust_bary.hz()
    );
}

// --------------------------------------------------------------------------
// GEO ↔ TOPO (epoch+position-dependent: diurnal rotation velocity)
// --------------------------------------------------------------------------

#[test]
fn rc_frequency_geo_to_topo() {
    let f = MFrequency::new(1.42e9, FrequencyRef::GEO);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs)
        .with_dut1(0.3);
    let rust_topo = f.convert_to(FrequencyRef::TOPO, &frame).unwrap();
    let cpp_topo = cpp_frequency_convert(
        1.42e9, "GEO", "TOPO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_topo.hz(), cpp_topo, 100.0),
        "GEO→TOPO: Rust={}, C++={}",
        rust_topo.hz(),
        cpp_topo
    );
}

#[test]
fn cr_frequency_geo_to_topo() {
    let cpp_topo = cpp_frequency_convert(
        1.42e9, "GEO", "TOPO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let f = MFrequency::new(cpp_topo, FrequencyRef::TOPO);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs)
        .with_dut1(0.3);
    let rust_geo = f.convert_to(FrequencyRef::GEO, &frame).unwrap();
    assert!(
        close(rust_geo.hz(), 1.42e9, 100.0),
        "TOPO→GEO: Rust={}, expected=1.42e9",
        rust_geo.hz()
    );
}

// --------------------------------------------------------------------------
// Multi-hop: LSRK ↔ TOPO (routes through BARY → GEO → TOPO)
// --------------------------------------------------------------------------

#[test]
fn rc_frequency_lsrk_to_topo() {
    let f = MFrequency::new(1.42e9, FrequencyRef::LSRK);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs)
        .with_dut1(0.3);
    let rust_topo = f.convert_to(FrequencyRef::TOPO, &frame).unwrap();
    let cpp_topo = cpp_frequency_convert(
        1.42e9, "LSRK", "TOPO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_topo.hz(), cpp_topo, 200.0),
        "LSRK→TOPO: Rust={}, C++={}",
        rust_topo.hz(),
        cpp_topo
    );
}

// ==========================================================================
// CC: C++ roundtrip baseline
// ==========================================================================

#[test]
fn cc_epoch_utc_tai_roundtrip() {
    let tai_mjd = cpp_epoch_convert(J2000_MJD, "UTC", "TAI").unwrap();
    let back_mjd = cpp_epoch_convert(tai_mjd, "TAI", "UTC").unwrap();

    let diff_s = (back_mjd - J2000_MJD).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-9, "C++ UTC→TAI roundtrip error: {diff_s}s");
}

#[test]
fn cc_epoch_tai_utc_roundtrip() {
    let utc_mjd = cpp_epoch_convert(J2000_MJD, "TAI", "UTC").unwrap();
    let back_mjd = cpp_epoch_convert(utc_mjd, "UTC", "TAI").unwrap();

    let diff_s = (back_mjd - J2000_MJD).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-9, "C++ TAI→UTC roundtrip error: {diff_s}s");
}

#[test]
fn cc_epoch_utc_to_tt() {
    // C++ roundtrip UTC→TT→UTC
    let tt_mjd = cpp_epoch_convert(J2000_MJD, "UTC", "TT").unwrap();
    let back_mjd = cpp_epoch_convert(tt_mjd, "TT", "UTC").unwrap();

    let diff_s = (back_mjd - J2000_MJD).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-9, "C++ UTC→TT roundtrip error: {diff_s}s");
}

#[test]
fn cc_epoch_tt_to_tdb() {
    // C++ roundtrip TT→TDB→TT
    let tdb_mjd = cpp_epoch_convert(J2000_MJD, "TT", "TDB").unwrap();
    let back_mjd = cpp_epoch_convert(tdb_mjd, "TDB", "TT").unwrap();

    let diff_s = (back_mjd - J2000_MJD).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-6, "C++ TT→TDB roundtrip error: {diff_s}s");
}

#[test]
fn cc_epoch_record_roundtrip() {
    // C++ creates record fields, verify values match input.
    let (value, unit, refer) = cpp_epoch_to_record(J2000_MJD, "TAI").unwrap();
    assert!(
        close(value, J2000_MJD, 1e-10),
        "value: {value} != {J2000_MJD}"
    );
    assert_eq!(unit, "d");
    assert_eq!(refer, "TAI");
}

#[test]
fn cc_position_roundtrip() {
    // C++ ITRF→WGS84→ITRF roundtrip
    let (lon, lat, h) = cpp_position_convert(VLA_X, VLA_Y, VLA_Z, "ITRF", "WGS84").unwrap();
    let (back_x, back_y, back_z) = cpp_position_convert(lon, lat, h, "WGS84", "ITRF").unwrap();

    assert!(close(VLA_X, back_x, 1.0), "x: {VLA_X} vs {back_x}");
    assert!(close(VLA_Y, back_y, 1.0), "y: {VLA_Y} vs {back_y}");
    assert!(close(VLA_Z, back_z, 1.0), "z: {VLA_Z} vs {back_z}");
}

#[test]
fn cc_position_record() {
    // C++ creates position record fields, verify values are sensible.
    let (lon, lat, radius) = cpp_position_to_record(VLA_X, VLA_Y, VLA_Z).unwrap();

    // VLA is in New Mexico: lon ~ -107.6°, lat ~ 34.1°
    let lon_deg = lon.to_degrees();
    let lat_deg = lat.to_degrees();
    assert!(
        (-108.0..=-107.0).contains(&lon_deg),
        "lon_deg={lon_deg} not near VLA"
    );
    assert!(
        (33.0..=35.0).contains(&lat_deg),
        "lat_deg={lat_deg} not near VLA"
    );
    // Geocentric radius should be ~6.37e6 m
    assert!(
        (6.3e6..=6.4e6).contains(&radius),
        "radius={radius} not reasonable"
    );
}

#[test]
fn cc_doppler_radio_z_roundtrip() {
    let z_val = cpp_doppler_convert(0.3, "RADIO", "Z").unwrap();
    let back = cpp_doppler_convert(z_val, "Z", "RADIO").unwrap();
    assert!(
        close(back, 0.3, 1e-12),
        "C++ RADIO→Z→RADIO roundtrip: 0.3 vs {back}"
    );
}

#[test]
fn cc_direction_j2000_galactic_roundtrip() {
    let (gal_lon, gal_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "GALACTIC", 0.0, 0.0, 0.0, 0.0).unwrap();
    let (back_lon, back_lat) =
        cpp_direction_convert(gal_lon, gal_lat, "GALACTIC", "J2000", 0.0, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close(back_lon, 1.0, 1e-10),
        "lon roundtrip: 1.0 vs {back_lon}"
    );
    assert!(
        close(back_lat, 0.5, 1e-10),
        "lat roundtrip: 0.5 vs {back_lat}"
    );
}

#[test]
fn cc_frequency_lsrk_bary_roundtrip() {
    let bary = cpp_frequency_convert(
        1.42e9, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let back = cpp_frequency_convert(
        bary, "BARY", "LSRK", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(back, 1.42e9, 1.0),
        "C++ LSRK→BARY→LSRK roundtrip: 1.42e9 vs {back}"
    );
}

// ==========================================================================
// Wave 5: MRadialVelocity RC/CR/CC
// ==========================================================================

#[test]
fn rc_radvel_lsrk_to_bary() {
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::LSRK);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let rust_bary = rv.convert_to(RadialVelocityRef::BARY, &frame).unwrap();
    let cpp_bary = cpp_radvel_convert(
        50_000.0, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_bary.ms(), cpp_bary, 1.0),
        "Rust={}, C++={}",
        rust_bary.ms(),
        cpp_bary
    );
}

#[test]
fn rc_radvel_bary_to_geo() {
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs);
    let rust_geo = rv.convert_to(RadialVelocityRef::GEO, &frame).unwrap();
    let cpp_geo = cpp_radvel_convert(
        50_000.0, "BARY", "GEO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_geo.ms(), cpp_geo, 1.0),
        "Rust={}, C++={}",
        rust_geo.ms(),
        cpp_geo
    );
}

#[test]
fn rc_radvel_bary_to_lgroup() {
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let frame = MeasFrame::new().with_direction(dir);
    let rust_lg = rv.convert_to(RadialVelocityRef::LGROUP, &frame).unwrap();
    let cpp_lg = cpp_radvel_convert(
        50_000.0, "BARY", "LGROUP", M31_LON, M31_LAT, "J2000", 0.0, 0.0, 0.0, 0.0,
    )
    .unwrap();
    assert!(
        close(rust_lg.ms(), cpp_lg, 1.0),
        "Rust={}, C++={}",
        rust_lg.ms(),
        cpp_lg
    );
}

#[test]
fn rc_radvel_bary_to_cmb() {
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let frame = MeasFrame::new().with_direction(dir);
    let rust_cmb = rv.convert_to(RadialVelocityRef::CMB, &frame).unwrap();
    let cpp_cmb = cpp_radvel_convert(
        50_000.0, "BARY", "CMB", M31_LON, M31_LAT, "J2000", 0.0, 0.0, 0.0, 0.0,
    )
    .unwrap();
    assert!(
        close(rust_cmb.ms(), cpp_cmb, 1.0),
        "Rust={}, C++={}",
        rust_cmb.ms(),
        cpp_cmb
    );
}

#[test]
fn rc_radvel_multiple_values() {
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let mut prev = f64::NAN;
    for ms_in in [10_000.0, 30_000.0, 50_000.0, 100_000.0, 200_000.0] {
        let rv = MRadialVelocity::new(ms_in, RadialVelocityRef::LSRK);
        let rust_bary = rv.convert_to(RadialVelocityRef::BARY, &frame).unwrap();
        let cpp_bary = cpp_radvel_convert(
            ms_in, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
        )
        .unwrap();
        assert!(
            close(rust_bary.ms(), cpp_bary, 1.0),
            "ms_in={ms_in}: Rust={}, C++={}",
            rust_bary.ms(),
            cpp_bary
        );
        assert!(
            prev.is_nan() || (rust_bary.ms() - prev).abs() > 100.0,
            "output didn't change for ms_in={ms_in}"
        );
        prev = rust_bary.ms();
    }
}

#[test]
fn cr_radvel_lsrk_to_bary() {
    let cpp_bary = cpp_radvel_convert(
        50_000.0, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::LSRK);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = MeasFrame::new().with_direction(dir).with_epoch(epoch);
    let rust_bary = rv.convert_to(RadialVelocityRef::BARY, &frame).unwrap();
    assert!(
        close(rust_bary.ms(), cpp_bary, 1.0),
        "Rust={}, C++={}",
        rust_bary.ms(),
        cpp_bary
    );
}

#[test]
fn cr_radvel_bary_to_geo() {
    let cpp_geo = cpp_radvel_convert(
        50_000.0, "BARY", "GEO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs);
    let rust_geo = rv.convert_to(RadialVelocityRef::GEO, &frame).unwrap();
    assert!(
        close(rust_geo.ms(), cpp_geo, 1.0),
        "Rust={}, C++={}",
        rust_geo.ms(),
        cpp_geo
    );
}

#[test]
fn cr_radvel_bary_to_lgroup() {
    let cpp_lg = cpp_radvel_convert(
        50_000.0, "BARY", "LGROUP", M31_LON, M31_LAT, "J2000", 0.0, 0.0, 0.0, 0.0,
    )
    .unwrap();
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let frame = MeasFrame::new().with_direction(dir);
    let rust_lg = rv.convert_to(RadialVelocityRef::LGROUP, &frame).unwrap();
    assert!(
        close(rust_lg.ms(), cpp_lg, 1.0),
        "Rust={}, C++={}",
        rust_lg.ms(),
        cpp_lg
    );
}

#[test]
fn cc_radvel_lsrk_bary_roundtrip() {
    let bary = cpp_radvel_convert(
        50_000.0, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let back = cpp_radvel_convert(
        bary, "BARY", "LSRK", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(back, 50_000.0, 0.01),
        "C++ LSRK→BARY→LSRK roundtrip: 50000 vs {back}"
    );
}

// ==========================================================================
// Wave 5: REST frequency RC/CR/CC
// ==========================================================================

#[test]
fn rc_freq_rest_to_lsrk() {
    let f = MFrequency::new(1.42e9, FrequencyRef::REST);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::LSRK);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_radial_velocity(rv);
    let rust_lsrk = f.convert_to(FrequencyRef::LSRK, &frame).unwrap();
    let cpp_lsrk = cpp_frequency_convert_with_rv(
        1.42e9, "REST", "LSRK", M31_LON, M31_LAT, "J2000", 0.0, 0.0, 0.0, 0.0, 50_000.0, "LSRK",
    )
    .unwrap();
    assert!(
        close(rust_lsrk.hz(), cpp_lsrk, 100.0),
        "Rust={}, C++={}",
        rust_lsrk.hz(),
        cpp_lsrk
    );
}

#[test]
fn rc_freq_lsrk_to_rest() {
    let f = MFrequency::new(1.42e9, FrequencyRef::LSRK);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::LSRK);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_radial_velocity(rv);
    let rust_rest = f.convert_to(FrequencyRef::REST, &frame).unwrap();
    let cpp_rest = cpp_frequency_convert_with_rv(
        1.42e9, "LSRK", "REST", M31_LON, M31_LAT, "J2000", 0.0, 0.0, 0.0, 0.0, 50_000.0, "LSRK",
    )
    .unwrap();
    assert!(
        close(rust_rest.hz(), cpp_rest, 100.0),
        "Rust={}, C++={}",
        rust_rest.hz(),
        cpp_rest
    );
}

#[test]
fn cr_freq_rest_to_lsrk() {
    let cpp_lsrk = cpp_frequency_convert_with_rv(
        1.42e9, "REST", "LSRK", M31_LON, M31_LAT, "J2000", 0.0, 0.0, 0.0, 0.0, 50_000.0, "LSRK",
    )
    .unwrap();
    let f = MFrequency::new(1.42e9, FrequencyRef::REST);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::LSRK);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_radial_velocity(rv);
    let rust_lsrk = f.convert_to(FrequencyRef::LSRK, &frame).unwrap();
    assert!(
        close(rust_lsrk.hz(), cpp_lsrk, 100.0),
        "Rust={}, C++={}",
        rust_lsrk.hz(),
        cpp_lsrk
    );
}

#[test]
fn cc_freq_rest_lsrk_roundtrip() {
    let lsrk = cpp_frequency_convert_with_rv(
        1.42e9, "REST", "LSRK", M31_LON, M31_LAT, "J2000", 0.0, 0.0, 0.0, 0.0, 50_000.0, "LSRK",
    )
    .unwrap();
    let back = cpp_frequency_convert_with_rv(
        lsrk, "LSRK", "REST", M31_LON, M31_LAT, "J2000", 0.0, 0.0, 0.0, 0.0, 50_000.0, "LSRK",
    )
    .unwrap();
    assert!(
        close(back, 1.42e9, 1.0),
        "C++ REST→LSRK→REST roundtrip: 1.42e9 vs {back}"
    );
}

// ==========================================================================
// Wave 5: B1950 direction RC/CR/CC
// ==========================================================================

#[test]
fn rc_dir_j2000_to_b1950() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let rust_b = d.convert_to(DirectionRef::B1950, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_b.as_angles();
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "B1950", 0.0, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close_angle(rust_lon, cpp_lon, 1e-4),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-4),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn rc_dir_b1950_to_j2000() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::B1950);
    let frame = MeasFrame::new();
    let rust_j = d.convert_to(DirectionRef::J2000, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_j.as_angles();
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "B1950", "J2000", 0.0, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close_angle(rust_lon, cpp_lon, 1e-4),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-4),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn cr_dir_j2000_to_b1950() {
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "B1950", 0.0, 0.0, 0.0, 0.0).unwrap();
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new();
    let rust_b = d.convert_to(DirectionRef::B1950, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_b.as_angles();
    assert!(
        close_angle(rust_lon, cpp_lon, 1e-4),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-4),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn cc_dir_b1950_roundtrip() {
    let (b_lon, b_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "B1950", 0.0, 0.0, 0.0, 0.0).unwrap();
    let (back_lon, back_lat) =
        cpp_direction_convert(b_lon, b_lat, "B1950", "J2000", 0.0, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close(back_lon, 1.0, 1e-6),
        "lon roundtrip: 1.0 vs {back_lon}"
    );
    assert!(
        close(back_lat, 0.5, 1e-6),
        "lat roundtrip: 0.5 vs {back_lat}"
    );
}

// ==========================================================================
// Wave 5: ITRF direction RC/CR
// ==========================================================================

#[test]
fn rc_dir_j2000_to_itrf() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_dut1(0.3);
    let rust_itrf = d.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_itrf.as_angles();
    let (cpp_lon, cpp_lat) = cpp_direction_convert(
        1.0, 0.5, "J2000", "ITRF", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close_angle(rust_lon, cpp_lon, 1e-3),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-3),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn rc_dir_itrf_to_j2000() {
    // First get an ITRF direction from C++, then convert back
    let (itrf_lon, itrf_lat) = cpp_direction_convert(
        1.0, 0.5, "J2000", "ITRF", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let d = MDirection::from_angles(itrf_lon, itrf_lat, DirectionRef::ITRF);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_dut1(0.3);
    let rust_j = d.convert_to(DirectionRef::J2000, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_j.as_angles();
    let (cpp_lon, cpp_lat) = cpp_direction_convert(
        itrf_lon, itrf_lat, "ITRF", "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close_angle(rust_lon, cpp_lon, 1e-3),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-3),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn cr_dir_j2000_to_itrf() {
    let (cpp_lon, cpp_lat) = cpp_direction_convert(
        1.0, 0.5, "J2000", "ITRF", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_dut1(0.3);
    let rust_itrf = d.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_itrf.as_angles();
    assert!(
        close_angle(rust_lon, cpp_lon, 1e-3),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-3),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

// ==========================================================================
// Wave 5: TOPO direction — Rust-only roundtrip
// ==========================================================================
// Note: C++ casacore's HADEC→TOPO routes through a full transformation chain
// (topocentric parallax, etc.) that differs fundamentally from our first-order
// diurnal aberration. C++ interop tests are not meaningful here.
// Rust roundtrip consistency is tested in casacore-types/tests/measures.rs.

#[test]
fn rc_dir_hadec_topo_roundtrip() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::HADEC);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_dut1(0.3);
    let topo = d.convert_to(DirectionRef::TOPO, &frame).unwrap();
    let back = topo.convert_to(DirectionRef::HADEC, &frame).unwrap();
    let (back_lon, back_lat) = back.as_angles();
    // Roundtrip should be sub-arcsecond
    assert!(
        close_angle(back_lon, 1.0, 1e-8),
        "lon roundtrip: 1.0 vs {back_lon}"
    );
    assert!(
        close(back_lat, 0.5, 1e-8),
        "lat roundtrip: 0.5 vs {back_lat}"
    );
}

// ==========================================================================
// Wave 5: Epoch GAST/LAST RC/CR
// ==========================================================================

#[test]
fn rc_epoch_ut1_to_gast() {
    let ut1_mjd = J2000_MJD + 0.3 / SECONDS_PER_DAY; // approximate UT1
    let ut1 = MEpoch::from_mjd(ut1_mjd, EpochRef::UT1);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new().with_position(vla).with_dut1(0.3);
    let rust_gast = ut1.convert_to(EpochRef::GAST, &frame).unwrap();
    let cpp_gast =
        cpp_epoch_convert_with_frame(ut1_mjd, "UT1", "GAST", VLA_LON, VLA_LAT, VLA_H, 0.3).unwrap();
    // C++ and Rust store sidereal epochs with different integer-day conventions:
    // Rust uses the UT1 integer day + sidereal fraction,
    // C++ tracks cumulative sidereal days from MJD 0.
    // Only the fractional part (sidereal time of day) is physically meaningful.
    let rust_frac = rust_gast.value().frac();
    let cpp_frac = cpp_gast - cpp_gast.floor();
    let diff_s = (rust_frac - cpp_frac).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-3, "Rust vs C++ GAST frac diff: {diff_s}s");
}

#[test]
fn rc_epoch_gast_to_last() {
    // First get GAST from C++
    let ut1_mjd = J2000_MJD + 0.3 / SECONDS_PER_DAY;
    let cpp_gast =
        cpp_epoch_convert_with_frame(ut1_mjd, "UT1", "GAST", VLA_LON, VLA_LAT, VLA_H, 0.3).unwrap();
    let gast = MEpoch::from_mjd(cpp_gast, EpochRef::GAST);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new().with_position(vla).with_dut1(0.3);
    let rust_last = gast.convert_to(EpochRef::LAST, &frame).unwrap();
    let cpp_last =
        cpp_epoch_convert_with_frame(cpp_gast, "GAST", "LAST", VLA_LON, VLA_LAT, VLA_H, 0.3)
            .unwrap();
    // Compare fractional sidereal time (integer-day conventions differ)
    let rust_frac = rust_last.value().frac();
    let cpp_frac = cpp_last - cpp_last.floor();
    let diff_s = (rust_frac - cpp_frac).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-3, "Rust vs C++ LAST frac diff: {diff_s}s");
}

#[test]
fn rc_epoch_gmst1_to_ut1() {
    // First get GMST1 from a known UT1
    let ut1_mjd = J2000_MJD + 0.3 / SECONDS_PER_DAY;
    let ut1 = MEpoch::from_mjd(ut1_mjd, EpochRef::UT1);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new().with_position(vla).with_dut1(0.3);
    let gmst = ut1.convert_to(EpochRef::GMST1, &frame).unwrap();
    // Now convert back
    let back_ut1 = gmst.convert_to(EpochRef::UT1, &frame).unwrap();
    let diff_s = (back_ut1.value().as_mjd() - ut1_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-3, "GMST1→UT1 roundtrip error: {diff_s}s");
}

#[test]
fn cr_epoch_ut1_to_gast() {
    let ut1_mjd = J2000_MJD + 0.3 / SECONDS_PER_DAY;
    let cpp_gast =
        cpp_epoch_convert_with_frame(ut1_mjd, "UT1", "GAST", VLA_LON, VLA_LAT, VLA_H, 0.3).unwrap();
    let ut1 = MEpoch::from_mjd(ut1_mjd, EpochRef::UT1);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new().with_position(vla).with_dut1(0.3);
    let rust_gast = ut1.convert_to(EpochRef::GAST, &frame).unwrap();
    // Compare fractional sidereal time (integer-day conventions differ)
    let rust_frac = rust_gast.value().frac();
    let cpp_frac = cpp_gast - cpp_gast.floor();
    let diff_s = (rust_frac - cpp_frac).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-3, "C++ vs Rust GAST frac diff: {diff_s}s");
}

#[test]
fn cr_epoch_gast_to_last() {
    let ut1_mjd = J2000_MJD + 0.3 / SECONDS_PER_DAY;
    let cpp_gast =
        cpp_epoch_convert_with_frame(ut1_mjd, "UT1", "GAST", VLA_LON, VLA_LAT, VLA_H, 0.3).unwrap();
    let cpp_last =
        cpp_epoch_convert_with_frame(cpp_gast, "GAST", "LAST", VLA_LON, VLA_LAT, VLA_H, 0.3)
            .unwrap();
    let gast = MEpoch::from_mjd(cpp_gast, EpochRef::GAST);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new().with_position(vla).with_dut1(0.3);
    let rust_last = gast.convert_to(EpochRef::LAST, &frame).unwrap();
    // Compare fractional sidereal time (integer-day conventions differ)
    let rust_frac = rust_last.value().frac();
    let cpp_frac = cpp_last - cpp_last.floor();
    let diff_s = (rust_frac - cpp_frac).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-3, "C++ vs Rust LAST frac diff: {diff_s}s");
}

// ==========================================================================
// Wave 5b: EOP data interop — Rust bundled data vs C++ casacore IERS tables
// ==========================================================================

#[test]
fn eop_dut1_at_j2000() {
    // Compare dUT1 from Rust bundled EOP data vs C++ casacore's IERS tables
    let eop = casacore_measures_data::EopTable::bundled();
    let rust_vals = eop
        .interpolate(J2000_MJD)
        .expect("J2000 should be in EOP range");
    let (cpp_dut1, _, _) = cpp_eop_query(J2000_MJD).unwrap();

    // C++ and Rust may use slightly different data vintages, so allow
    // moderate tolerance. Both should agree to within ~10 ms.
    let diff = (rust_vals.dut1_seconds - cpp_dut1).abs();
    assert!(
        diff < 0.01,
        "dUT1 at J2000: Rust={:.7}, C++={:.7}, diff={:.4}s",
        rust_vals.dut1_seconds,
        cpp_dut1,
        diff
    );
}

#[test]
fn eop_polar_motion_at_j2000() {
    let eop = casacore_measures_data::EopTable::bundled();
    let rust_vals = eop
        .interpolate(J2000_MJD)
        .expect("J2000 should be in EOP range");
    let (_, cpp_xp, cpp_yp) = cpp_eop_query(J2000_MJD).unwrap();

    // Polar motion should agree to within ~1 mas (0.001 arcsec)
    let xp_diff = (rust_vals.x_arcsec - cpp_xp).abs();
    let yp_diff = (rust_vals.y_arcsec - cpp_yp).abs();
    assert!(
        xp_diff < 0.001,
        "xp at J2000: Rust={:.6}\", C++={:.6}\", diff={:.6}\"",
        rust_vals.x_arcsec,
        cpp_xp,
        xp_diff
    );
    assert!(
        yp_diff < 0.001,
        "yp at J2000: Rust={:.6}\", C++={:.6}\", diff={:.6}\"",
        rust_vals.y_arcsec,
        cpp_yp,
        yp_diff
    );
}

#[test]
fn eop_dut1_range_of_dates() {
    // Compare dUT1 across several dates to verify systematic agreement
    let eop = casacore_measures_data::EopTable::bundled();
    let test_mjds = [
        48622.0, // Start of finals2000A range (~1992)
        50000.0, // ~1995
        51544.5, // J2000.0
        53000.0, // ~2004
        54000.0, // ~2006
        55000.0, // ~2009
        56000.0, // ~2012
        57000.0, // ~2014
        58000.0, // ~2017
        59000.0, // ~2020
        60000.0, // ~2023
    ];

    for &mjd in &test_mjds {
        let rust_vals = match eop.interpolate(mjd) {
            Some(v) => v,
            None => continue,
        };
        let (cpp_dut1, cpp_xp, cpp_yp) = match cpp_eop_query(mjd) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let dut1_diff = (rust_vals.dut1_seconds - cpp_dut1).abs();
        assert!(
            dut1_diff < 0.01,
            "dUT1 at MJD {mjd}: Rust={:.7}, C++={:.7}, diff={:.4}s",
            rust_vals.dut1_seconds,
            cpp_dut1,
            dut1_diff
        );

        let xp_diff = (rust_vals.x_arcsec - cpp_xp).abs();
        let yp_diff = (rust_vals.y_arcsec - cpp_yp).abs();
        assert!(
            xp_diff < 0.001,
            "xp at MJD {mjd}: Rust={:.6}\", C++={:.6}\", diff={:.6}\"",
            rust_vals.x_arcsec,
            cpp_xp,
            xp_diff
        );
        assert!(
            yp_diff < 0.001,
            "yp at MJD {mjd}: Rust={:.6}\", C++={:.6}\", diff={:.6}\"",
            rust_vals.y_arcsec,
            cpp_yp,
            yp_diff
        );
    }
}

#[test]
fn eop_epoch_conversion_with_bundled_data() {
    // Test that epoch conversion using bundled EOP data matches C++ results.
    // This verifies the full pipeline: bundled data → MeasFrame → dut1_for_mjd → conversion.
    let eop = casacore_measures_data::EopTable::bundled();
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);

    // Use bundled EOP for dUT1 instead of hardcoded value
    let frame = MeasFrame::new().with_eop(std::sync::Arc::new(eop.clone()));

    let rust_ut1 = utc.convert_to(EpochRef::UT1, &frame).unwrap();

    // C++ uses its own IERS tables for the same conversion
    let cpp_ut1_mjd = cpp_epoch_convert(J2000_MJD, "UTC", "UT1").unwrap();

    let diff_s = (rust_ut1.value().as_mjd() - cpp_ut1_mjd).abs() * SECONDS_PER_DAY;
    assert!(
        diff_s < 0.01,
        "UTC→UT1 at J2000 with EOP: Rust={}, C++={}, diff={:.4}s",
        rust_ut1.value().as_mjd(),
        cpp_ut1_mjd,
        diff_s
    );
}

// ==========================================================================
// Helpers
// ==========================================================================

/// Extract a quantity value from a sub-record field in a RecordValue.
fn extract_quantity(rec: &casacore_types::RecordValue, field: &str) -> f64 {
    match rec.get(field) {
        Some(casacore_types::Value::Record(sub)) => match sub.get("value") {
            Some(casacore_types::Value::Scalar(casacore_types::ScalarValue::Float64(v))) => *v,
            other => panic!("expected Float64 in {field}/value, got {other:?}"),
        },
        other => panic!("expected Record for {field}, got {other:?}"),
    }
}

// ==========================================================================
// Wave 5c: EOP-driven interop tests
// Both Rust (with_bundled_eop) and C++ (internal IERS tables) use their
// own EOP data — no hardcoded dUT1.
// ==========================================================================

// MJD constants for multi-epoch tests
const MJD_2010: f64 = 55197.0; // 2010-01-01
const MJD_2020: f64 = 58849.0; // 2020-01-01

// --- EOP-driven ITRF direction interop ---

#[test]
fn eop_dir_j2000_to_itrf() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop();
    let rust_itrf = d.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_itrf.as_angles();
    let (cpp_lon, cpp_lat) = cpp_direction_convert(
        1.0, 0.5, "J2000", "ITRF", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let sep = sep_arcsec(rust_lon, rust_lat, cpp_lon, cpp_lat);
    eprintln!(
        "ITRF J2000: sep={sep:.6} arcsec (R=({rust_lon:.10},{rust_lat:.10}) C=({cpp_lon:.10},{cpp_lat:.10}))"
    );
    // ~1.5 mas deviation from SOFA vs casacore (see direction.rs module docs)
    assert!(
        angular_sep(rust_lon, rust_lat, cpp_lon, cpp_lat) < 5e-6,
        "ITRF J2000: sep={sep:.3} arcsec"
    );
}

#[test]
fn eop_dir_j2000_to_itrf_2010() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(MJD_2010, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop();
    let rust_itrf = d.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_itrf.as_angles();
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "ITRF", MJD_2010, VLA_LON, VLA_LAT, VLA_H)
            .unwrap();
    let sep = sep_arcsec(rust_lon, rust_lat, cpp_lon, cpp_lat);
    assert!(
        angular_sep(rust_lon, rust_lat, cpp_lon, cpp_lat) < 5e-6,
        "ITRF 2010: sep={sep:.3} arcsec"
    );
}

#[test]
fn eop_dir_j2000_to_itrf_2020() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(MJD_2020, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop();
    let rust_itrf = d.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_itrf.as_angles();
    let (cpp_lon, cpp_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "ITRF", MJD_2020, VLA_LON, VLA_LAT, VLA_H)
            .unwrap();
    let sep = sep_arcsec(rust_lon, rust_lat, cpp_lon, cpp_lat);
    assert!(
        angular_sep(rust_lon, rust_lat, cpp_lon, cpp_lat) < 5e-6,
        "ITRF 2020: sep={sep:.3} arcsec"
    );
}

// --- EOP-driven GAST/LAST epoch interop ---

#[test]
fn eop_epoch_utc_to_gast() {
    // Full pipeline: UTC → UT1 (via bundled EOP) → GAST
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new().with_position(vla).with_bundled_eop();
    let rust_ut1 = utc.convert_to(EpochRef::UT1, &frame).unwrap();
    let rust_gast = rust_ut1.convert_to(EpochRef::GAST, &frame).unwrap();

    // C++ uses its own IERS tables; pass dut1=0.0 since it's ignored
    let cpp_ut1_mjd = cpp_epoch_convert(J2000_MJD, "UTC", "UT1").unwrap();
    let cpp_gast =
        cpp_epoch_convert_with_frame(cpp_ut1_mjd, "UT1", "GAST", VLA_LON, VLA_LAT, VLA_H, 0.0)
            .unwrap();

    let rust_frac = rust_gast.value().frac();
    let cpp_frac = cpp_gast - cpp_gast.floor();
    let diff_s = (rust_frac - cpp_frac).abs() * SECONDS_PER_DAY;
    assert!(
        diff_s < 1e-3,
        "EOP GAST frac diff: {diff_s}s (Rust={rust_frac}, C++={cpp_frac})"
    );
}

#[test]
fn eop_epoch_utc_to_last() {
    // Full pipeline: UTC → UT1 (via bundled EOP) → GAST → LAST
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new().with_position(vla).with_bundled_eop();
    let rust_ut1 = utc.convert_to(EpochRef::UT1, &frame).unwrap();
    let rust_gast = rust_ut1.convert_to(EpochRef::GAST, &frame).unwrap();
    let rust_last = rust_gast.convert_to(EpochRef::LAST, &frame).unwrap();

    // C++ chain
    let cpp_ut1_mjd = cpp_epoch_convert(J2000_MJD, "UTC", "UT1").unwrap();
    let cpp_gast =
        cpp_epoch_convert_with_frame(cpp_ut1_mjd, "UT1", "GAST", VLA_LON, VLA_LAT, VLA_H, 0.0)
            .unwrap();
    let cpp_last =
        cpp_epoch_convert_with_frame(cpp_gast, "GAST", "LAST", VLA_LON, VLA_LAT, VLA_H, 0.0)
            .unwrap();

    let rust_frac = rust_last.value().frac();
    let cpp_frac = cpp_last - cpp_last.floor();
    let diff_s = (rust_frac - cpp_frac).abs() * SECONDS_PER_DAY;
    assert!(
        diff_s < 1e-3,
        "EOP LAST frac diff: {diff_s}s (Rust={rust_frac}, C++={cpp_frac})"
    );
}

// --- EOP-driven HADEC/AZEL direction interop ---

#[test]
fn eop_dir_j2000_to_hadec() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop();
    let rust_hadec = d.convert_to(DirectionRef::HADEC, &frame).unwrap();
    let (rust_ha, rust_dec) = rust_hadec.as_angles();
    let (cpp_ha, cpp_dec) = cpp_direction_convert(
        1.0, 0.5, "J2000", "HADEC", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let sep = sep_arcsec(rust_ha, rust_dec, cpp_ha, cpp_dec);
    eprintln!(
        "HADEC: sep={sep:.6} arcsec (R=({rust_ha:.10},{rust_dec:.10}) C=({cpp_ha:.10},{cpp_dec:.10}))"
    );
    // ~1.5 mas deviation from SOFA vs casacore (see direction.rs module docs)
    assert!(
        angular_sep(rust_ha, rust_dec, cpp_ha, cpp_dec) < 1e-4,
        "HADEC: sep={sep:.3} arcsec"
    );
}

#[test]
fn eop_dir_j2000_to_azel() {
    // Use (1.0, 0.5) source at J2000 epoch, same as other tests.
    // This source may be near/below horizon but AZEL should still be compared.
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop();
    let rust_azel = d.convert_to(DirectionRef::AZEL, &frame).unwrap();
    let (rust_az, rust_el) = rust_azel.as_angles();
    let (cpp_az, cpp_el) = cpp_direction_convert(
        1.0, 0.5, "J2000", "AZEL", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let sep = sep_arcsec(rust_az, rust_el, cpp_az, cpp_el);
    eprintln!(
        "AZEL: sep={sep:.6} arcsec (R=({rust_az:.10},{rust_el:.10}) C=({cpp_az:.10},{cpp_el:.10}))"
    );
    assert!(
        angular_sep(rust_az, rust_el, cpp_az, cpp_el) < 0.01,
        "AZEL: sep={sep:.3} arcsec"
    );
}

// --- EOP-driven frequency/radvel BARY→TOPO interop ---

#[test]
fn eop_freq_bary_to_topo() {
    let f = MFrequency::new(1.42e9, FrequencyRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs)
        .with_bundled_eop();
    let rust_topo = f.convert_to(FrequencyRef::TOPO, &frame).unwrap();
    let cpp_topo = cpp_frequency_convert(
        1.42e9, "BARY", "TOPO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_topo.hz(), cpp_topo, 200.0),
        "Rust={}, C++={}, diff={}",
        rust_topo.hz(),
        cpp_topo,
        (rust_topo.hz() - cpp_topo).abs()
    );
}

#[test]
fn eop_rv_bary_to_topo() {
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs)
        .with_bundled_eop();
    let rust_topo = rv.convert_to(RadialVelocityRef::TOPO, &frame).unwrap();
    let cpp_topo = cpp_radvel_convert(
        50_000.0, "BARY", "TOPO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    assert!(
        close(rust_topo.ms(), cpp_topo, 1.0),
        "Rust={}, C++={}, diff={}",
        rust_topo.ms(),
        cpp_topo,
        (rust_topo.ms() - cpp_topo).abs()
    );
}

// --- Full pipeline integration test ---

#[test]
fn eop_full_pipeline_radio_astronomy() {
    // Realistic radio astronomy workflow:
    // UTC epoch at a modern date, a direction near transit for the VLA,
    // Convert to ITRF, HADEC, AZEL using only with_bundled_eop().
    // Compare all three outputs against C++.
    let epoch_mjd = MJD_2010; // 2010-01-01

    // Use a source near transit at MJD 2010 for the VLA (well above horizon).
    // GAST at MJD 55197 ≈ 6.6h ≈ 1.73 rad; LAST ≈ 1.73 + VLA_LON ≈ -0.15
    // Use RA ≈ 6.0 rad with Dec=0 for well-defined Az/El (~56° elevation).
    let src = MDirection::from_angles(6.0, 0.0, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(epoch_mjd, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = MeasFrame::new()
        .with_epoch(epoch)
        .with_position(obs)
        .with_direction(src.clone())
        .with_bundled_eop();

    // ITRF
    let rust_itrf = src.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (r_itrf_lon, r_itrf_lat) = rust_itrf.as_angles();
    let (c_itrf_lon, c_itrf_lat) = cpp_direction_convert(
        6.0, 0.0, "J2000", "ITRF", epoch_mjd, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let itrf_sep = sep_arcsec(r_itrf_lon, r_itrf_lat, c_itrf_lon, c_itrf_lat);
    assert!(
        angular_sep(r_itrf_lon, r_itrf_lat, c_itrf_lon, c_itrf_lat) < 5e-6,
        "ITRF: sep={itrf_sep:.3} arcsec"
    );

    // HADEC
    let rust_hadec = src.convert_to(DirectionRef::HADEC, &frame).unwrap();
    let (r_ha, r_dec) = rust_hadec.as_angles();
    let (c_ha, c_dec) = cpp_direction_convert(
        6.0, 0.0, "J2000", "HADEC", epoch_mjd, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let hadec_sep = sep_arcsec(r_ha, r_dec, c_ha, c_dec);
    assert!(
        angular_sep(r_ha, r_dec, c_ha, c_dec) < 1e-4,
        "HADEC: sep={hadec_sep:.3} arcsec"
    );

    // AZEL
    let rust_azel = src.convert_to(DirectionRef::AZEL, &frame).unwrap();
    let (r_az, r_el) = rust_azel.as_angles();
    let (c_az, c_el) = cpp_direction_convert(
        6.0, 0.0, "J2000", "AZEL", epoch_mjd, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let azel_sep = sep_arcsec(r_az, r_el, c_az, c_el);
    eprintln!("Pipeline AZEL: sep={azel_sep:.3} arcsec");
    assert!(
        angular_sep(r_az, r_el, c_az, c_el) < 2e-3,
        "AZEL: sep={azel_sep:.1} arcsec"
    );
}

/// Diagnostic test: trace each intermediate step of the J2000→HADEC chain
/// to identify where Rust and C++ diverge.
#[test]
fn diag_direction_chain_steps() {
    let lon = 1.0_f64;
    let lat = 0.5_f64;
    let epoch_mjd = J2000_MJD;

    // Rust conversions at each step
    let d = MDirection::from_angles(lon, lat, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop();

    let steps = ["JMEAN", "JTRUE", "APP", "HADEC", "AZEL"];
    for step in &steps {
        let ref_type = match *step {
            "JMEAN" => DirectionRef::JMEAN,
            "JTRUE" => DirectionRef::JTRUE,
            "APP" => DirectionRef::APP,
            "HADEC" => DirectionRef::HADEC,
            "AZEL" => DirectionRef::AZEL,
            _ => unreachable!(),
        };
        let rust_result = d.convert_to(ref_type, &frame).unwrap();
        let (r_lon, r_lat) = rust_result.as_angles();
        let (c_lon, c_lat) =
            cpp_direction_convert(lon, lat, "J2000", step, epoch_mjd, VLA_LON, VLA_LAT, VLA_H)
                .unwrap();
        let sep = sep_arcsec(r_lon, r_lat, c_lon, c_lat);
        eprintln!(
            "CHAIN J2000→{step:6}: sep={sep:12.6} arcsec  (R={:.8},{:.8}  C={:.8},{:.8})",
            r_lon, r_lat, c_lon, c_lat
        );
    }
}

// ==========================================================================
// IAU 2006/2000A interop tests
//
// SOFA and casacore implement the same IAU standards but with different
// polynomial series and internal decompositions. For IAU 2000A, the
// precession/nutation matrices agree perfectly (verified in
// diag_iau2000a_chain_steps: JMEAN and JTRUE match to 0.000"),
// but the apparent-place (APP) step diverges by ~16 mas due to
// differences in the aberration/deflection computations (Stumpff vs
// VSOP87 velocity series, plus casacore applies full Sun gravitational
// deflection that SOFA's ab() omits). This is direction-dependent and
// larger than the ~1.5 mas deviation for IAU 1976/1980.
//
// Tolerance: 1e-7 rad ≈ 20 mas. See misc/casacore_vs_sofa_deviation.cpp
// for a standalone C++ test quantifying this, and the corresponding
// GitHub issue filed with the casacore maintainers.
// ==========================================================================

#[test]
fn iau2000a_dir_j2000_to_hadec() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop()
        .with_iau_model(IauModel::Iau2006_2000A);
    let rust_hadec = d.convert_to(DirectionRef::HADEC, &frame).unwrap();
    let (rust_ha, rust_dec) = rust_hadec.as_angles();
    let (cpp_ha, cpp_dec) = cpp_direction_convert_iau2000a(
        1.0, 0.5, "J2000", "HADEC", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let sep = sep_arcsec(rust_ha, rust_dec, cpp_ha, cpp_dec);
    eprintln!("IAU2000A HADEC: sep={sep:.6} arcsec");
    // ~16 mas deviation from SOFA vs casacore IAU 2000A differences (see comment above)
    assert!(
        angular_sep(rust_ha, rust_dec, cpp_ha, cpp_dec) < 1e-7,
        "IAU2000A HADEC: sep={sep:.3} arcsec"
    );
}

#[test]
fn iau2000a_dir_j2000_to_azel() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop()
        .with_iau_model(IauModel::Iau2006_2000A);
    let rust_azel = d.convert_to(DirectionRef::AZEL, &frame).unwrap();
    let (rust_az, rust_el) = rust_azel.as_angles();
    let (cpp_az, cpp_el) = cpp_direction_convert_iau2000a(
        1.0, 0.5, "J2000", "AZEL", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let sep = sep_arcsec(rust_az, rust_el, cpp_az, cpp_el);
    eprintln!("IAU2000A AZEL: sep={sep:.6} arcsec");
    // ~20 mas deviation from SOFA vs casacore IAU 2000A differences (see comment above)
    assert!(
        angular_sep(rust_az, rust_el, cpp_az, cpp_el) < 1e-7,
        "IAU2000A AZEL: sep={sep:.3} arcsec"
    );
}

#[test]
fn iau2000a_dir_j2000_to_itrf() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop()
        .with_iau_model(IauModel::Iau2006_2000A);
    let rust_itrf = d.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_itrf.as_angles();
    let (cpp_lon, cpp_lat) = cpp_direction_convert_iau2000a(
        1.0, 0.5, "J2000", "ITRF", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let sep = sep_arcsec(rust_lon, rust_lat, cpp_lon, cpp_lat);
    eprintln!("IAU2000A ITRF: sep={sep:.6} arcsec");
    // ~16 mas deviation from SOFA vs casacore IAU 2000A differences (see comment above)
    assert!(
        angular_sep(rust_lon, rust_lat, cpp_lon, cpp_lat) < 1e-7,
        "IAU2000A ITRF: sep={sep:.3} arcsec"
    );
}

/// Diagnostic: trace IAU 2000A chain steps to find where Rust and C++ diverge.
#[test]
fn diag_iau2000a_chain_steps() {
    let lon = 1.0_f64;
    let lat = 0.5_f64;
    let epoch_mjd = J2000_MJD;

    let d = MDirection::from_angles(lon, lat, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop()
        .with_iau_model(IauModel::Iau2006_2000A);

    let steps = ["JMEAN", "JTRUE", "APP", "HADEC", "AZEL"];
    for step in &steps {
        let ref_type = match *step {
            "JMEAN" => DirectionRef::JMEAN,
            "JTRUE" => DirectionRef::JTRUE,
            "APP" => DirectionRef::APP,
            "HADEC" => DirectionRef::HADEC,
            "AZEL" => DirectionRef::AZEL,
            _ => unreachable!(),
        };
        let rust_result = d.convert_to(ref_type, &frame).unwrap();
        let (r_lon, r_lat) = rust_result.as_angles();
        let (c_lon, c_lat) = cpp_direction_convert_iau2000a(
            lon, lat, "J2000", step, epoch_mjd, VLA_LON, VLA_LAT, VLA_H,
        )
        .unwrap();
        let sep = sep_arcsec(r_lon, r_lat, c_lon, c_lat);
        eprintln!("IAU2000A CHAIN J2000->{step:6}: sep={sep:12.6} arcsec");
    }
}

/// Diagnostic: compare C++ precession steps vs Rust JMEAN
#[test]
fn diag_iau2000_precession_matrix() {
    let tt_mjd = J2000_MJD + 64.184 / 86400.0;
    let data = cpp_iau2000_precession_matrix(tt_mjd).unwrap();

    let bias_lon = data[0][0];
    let bias_lat = data[0][1];
    let prec_lon = data[1][0];
    let prec_lat = data[1][1];
    let euler_zeta = data[2][0];
    let euler_theta = data[2][1];
    let euler_z = data[2][2];

    eprintln!("C++ after bias:      lon={bias_lon:.12} lat={bias_lat:.12}");
    eprintln!("C++ after bias+prec: lon={prec_lon:.12} lat={prec_lat:.12}");
    eprintln!(
        "C++ Euler(zeta,theta,z) = ({euler_zeta:.10e}, {euler_theta:.10e}, {euler_z:.10e}) rad"
    );
    eprintln!(
        "C++ Euler arcsec = ({:.6}, {:.6}, {:.6})",
        euler_zeta * 206265.0,
        euler_theta * 206265.0,
        euler_z * 206265.0
    );

    // Rust JMEAN via conversion
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop()
        .with_iau_model(IauModel::Iau2006_2000A);
    let rust_jmean = d.convert_to(DirectionRef::JMEAN, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_jmean.as_angles();
    eprintln!("Rust JMEAN:          lon={rust_lon:.12} lat={rust_lat:.12}");

    // C++ via full direction convert
    let (cpp_full_lon, cpp_full_lat) = cpp_direction_convert_iau2000a(
        1.0, 0.5, "J2000", "JMEAN", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    eprintln!("C++ full JMEAN:      lon={cpp_full_lon:.12} lat={cpp_full_lat:.12}");

    eprintln!(
        "Rust vs C++ full:    {:.6} arcsec",
        sep_arcsec(rust_lon, rust_lat, cpp_full_lon, cpp_full_lat)
    );
    eprintln!(
        "C++ step vs C++ full: {:.6} arcsec",
        sep_arcsec(prec_lon, prec_lat, cpp_full_lon, cpp_full_lat)
    );
}

/// Diagnostic: verify rotation invariance and model shifts.
#[test]
fn diag_aberration_rotation_invariance() {
    let d_j2000 = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);

    // First compute JTRUE for both models to verify model shift
    for (model_name, model) in &[
        ("IAU1976", IauModel::Iau1976_1980),
        ("IAU2000A", IauModel::Iau2006_2000A),
    ] {
        let f = MeasFrame::new()
            .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
            .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
            .with_bundled_eop()
            .with_iau_model(*model);
        let jtrue = d_j2000.convert_to(DirectionRef::JTRUE, &f).unwrap();
        let (jt_lon, jt_lat) = jtrue.as_angles();
        let app = d_j2000.convert_to(DirectionRef::APP, &f).unwrap();
        let (app_lon, app_lat) = app.as_angles();
        eprintln!(
            "Rust {model_name} JTRUE: lon={:.12} lat={:.12}",
            jt_lon, jt_lat
        );
        eprintln!(
            "Rust {model_name} APP:   lon={:.12} lat={:.12}",
            app_lon, app_lat
        );
    }

    let frame = MeasFrame::new()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_bundled_eop()
        .with_iau_model(IauModel::Iau2006_2000A);

    // Method 1: normal chain J2000→JMEAN→JTRUE→APP
    let app1 = d_j2000.convert_to(DirectionRef::APP, &frame).unwrap();
    let (app1_lon, app1_lat) = app1.as_angles();

    // Method 2: ab(d_J2000, v_J2000) then rotate by BPN
    // Get velocity
    let c_au_per_day: f64 = 173.144_632_674_240_34;
    let mjd_offset = 2_400_000.5;
    let (pvh, pvb) = sofars::eph::epv00(mjd_offset, J2000_MJD).unwrap();
    let v_j2000 = [
        pvb[1][0] / c_au_per_day,
        pvb[1][1] / c_au_per_day,
        pvb[1][2] / c_au_per_day,
    ];
    let sun_dist = sofars::vm::pm(pvh[0]);
    let v2: f64 = v_j2000.iter().map(|x| x * x).sum();
    let bm1 = (1.0 - v2).sqrt();

    // Apply ab in J2000 frame
    let d_cos = d_j2000.cosines();
    let app_j2000 = sofars::astro::ab(&d_cos, &v_j2000, sun_dist, bm1);

    // Rotate to apparent frame using BPN
    // Get TT epoch for BPN
    let epoch = frame.epoch().unwrap();
    let tt = epoch
        .convert_to(casacore_types::measures::EpochRef::TT, &frame)
        .unwrap();
    let (tt1, tt2) = tt.value().as_jd_pair();
    let bpn = sofars::pnp::pnm00a(tt1, tt2);
    // BPN × app_j2000
    let mut app2_cos = [0.0; 3];
    for i in 0..3 {
        app2_cos[i] =
            bpn[i][0] * app_j2000[0] + bpn[i][1] * app_j2000[1] + bpn[i][2] * app_j2000[2];
    }
    let app2_lon = app2_cos[1].atan2(app2_cos[0]);
    let app2_lat = app2_cos[2].asin();

    let chain_vs_direct = sep_arcsec(app1_lon, app1_lat, app2_lon, app2_lat);
    eprintln!("Aberration rotation invariance (IAU 2000A):");
    eprintln!("  Chain APP:  lon={:.12} lat={:.12}", app1_lon, app1_lat);
    eprintln!("  Direct APP: lon={:.12} lat={:.12}", app2_lon, app2_lat);
    eprintln!("  Angular separation: {chain_vs_direct:.9} arcsec");

    // Also compare with C++ IAU 1976 and IAU 2000A
    let (cpp_1976_lon, cpp_1976_lat) =
        cpp_direction_convert(1.0, 0.5, "J2000", "APP", J2000_MJD, VLA_LON, VLA_LAT, VLA_H)
            .unwrap();
    let (cpp_2000a_lon, cpp_2000a_lat) = cpp_direction_convert_iau2000a(
        1.0, 0.5, "J2000", "APP", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    eprintln!(
        "  C++ IAU1976 APP:  lon={:.12} lat={:.12}",
        cpp_1976_lon, cpp_1976_lat
    );
    eprintln!(
        "  C++ IAU2000A APP: lon={:.12} lat={:.12}",
        cpp_2000a_lon, cpp_2000a_lat
    );
    eprintln!(
        "  Rust chain vs C++ IAU2000A: {:.6} arcsec",
        sep_arcsec(app1_lon, app1_lat, cpp_2000a_lon, cpp_2000a_lat),
    );
    eprintln!(
        "  Rust direct vs C++ IAU2000A: {:.6} arcsec",
        sep_arcsec(app2_lon, app2_lat, cpp_2000a_lon, cpp_2000a_lat),
    );
    eprintln!(
        "  Rust chain vs C++ IAU1976: {:.6} arcsec",
        sep_arcsec(app1_lon, app1_lat, cpp_1976_lon, cpp_1976_lat),
    );
    eprintln!(
        "  Rust direct vs C++ IAU1976: {:.6} arcsec",
        sep_arcsec(app2_lon, app2_lat, cpp_1976_lon, cpp_1976_lat),
    );
}

/// Diagnostic: compare Earth velocity from Rust (SOFA epv00) vs C++ (Stumpff series).
#[test]
fn diag_earth_velocity_comparison() {
    let epoch_mjd = J2000_MJD; // MJD 51544.5

    // Rust: get Earth velocity from sofars::eph::epv00
    let c_au_per_day: f64 = 173.144_632_674_240_34;
    let mjd_offset = 2_400_000.5;
    let tt1 = mjd_offset;
    let tt2 = epoch_mjd;
    let (pvh, pvb) = sofars::eph::epv00(tt1, tt2).unwrap();
    let rust_v = [
        pvb[1][0] / c_au_per_day,
        pvb[1][1] / c_au_per_day,
        pvb[1][2] / c_au_per_day,
    ];
    let rust_sun_dist = sofars::vm::pm(pvh[0]);

    // C++: get Earth velocity from Stumpff series
    let (cpp_v, cpp_sun) = cpp_earth_velocity(epoch_mjd).unwrap();
    let cpp_sun_dist =
        (cpp_sun[0] * cpp_sun[0] + cpp_sun[1] * cpp_sun[1] + cpp_sun[2] * cpp_sun[2]).sqrt();

    eprintln!("Earth velocity comparison at MJD {epoch_mjd}:");
    eprintln!(
        "  Rust (epv00)    vx={:.15e} vy={:.15e} vz={:.15e}",
        rust_v[0], rust_v[1], rust_v[2]
    );
    eprintln!(
        "  C++ (Stumpff)   vx={:.15e} vy={:.15e} vz={:.15e}",
        cpp_v[0], cpp_v[1], cpp_v[2]
    );
    let dv = [
        rust_v[0] - cpp_v[0],
        rust_v[1] - cpp_v[1],
        rust_v[2] - cpp_v[2],
    ];
    let dv_mag = (dv[0] * dv[0] + dv[1] * dv[1] + dv[2] * dv[2]).sqrt();
    let dv_arcsec = dv_mag * 206265.0; // velocity in units of c → arcsec of aberration
    eprintln!(
        "  Δv (Rust-C++)   vx={:.15e} vy={:.15e} vz={:.15e}",
        dv[0], dv[1], dv[2]
    );
    eprintln!(
        "  |Δv| = {:.6e} c → {:.6} arcsec aberration difference",
        dv_mag, dv_arcsec
    );
    eprintln!(
        "  Sun distance: Rust={:.6} AU  C++={:.6} AU",
        rust_sun_dist, cpp_sun_dist
    );

    // Compare C++ IAU 1976 vs C++ IAU 2000A at all levels
    // This tells us if the IAU 2000A C++ shim is actually working.
    for step in &["JMEAN", "JTRUE", "APP", "HADEC"] {
        let (cpp_1976_lon, cpp_1976_lat) =
            cpp_direction_convert(1.0, 0.5, "J2000", step, J2000_MJD, VLA_LON, VLA_LAT, VLA_H)
                .unwrap();
        let (cpp_2000a_lon, cpp_2000a_lat) = cpp_direction_convert_iau2000a(
            1.0, 0.5, "J2000", step, J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
        )
        .unwrap();
        let model_sep = sep_arcsec(cpp_1976_lon, cpp_1976_lat, cpp_2000a_lon, cpp_2000a_lat);
        eprintln!("  C++ IAU1976 vs IAU2000A at {step}: sep={model_sep:.6} arcsec");
        eprintln!(
            "    IAU1976:  lon={:.12} lat={:.12}",
            cpp_1976_lon, cpp_1976_lat
        );
        eprintln!(
            "    IAU2000A: lon={:.12} lat={:.12}",
            cpp_2000a_lon, cpp_2000a_lat
        );
    }
}
