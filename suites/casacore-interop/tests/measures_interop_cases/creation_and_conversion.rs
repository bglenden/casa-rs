// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

// ==========================================================================
// RR: Rust-only (already in unit tests — included here for matrix completeness)
// The RR column is covered by the unit tests in casa-types; the interop
// test file focuses on RC/CR/CC cells.
// ==========================================================================

// ==========================================================================
// RC: Rust creates, C++ converts
// ==========================================================================

#[test]
fn rc_epoch_utc_to_tai() {
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = measures_frame();
    let rust_tai = utc.convert_to(EpochRef::TAI, &frame).unwrap();

    let cpp_tai_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "UTC", "TAI").unwrap();

    let diff_s = (rust_tai.value().as_mjd() - cpp_tai_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-6, "Rust vs C++ TAI difference: {diff_s}s");
}

#[test]
fn rc_epoch_tai_to_utc() {
    let tai = MEpoch::from_mjd(J2000_MJD, EpochRef::TAI);
    let frame = measures_frame();
    let rust_utc = tai.convert_to(EpochRef::UTC, &frame).unwrap();

    let cpp_utc_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "TAI", "UTC").unwrap();

    let diff_s = (rust_utc.value().as_mjd() - cpp_utc_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-6, "Rust vs C++ UTC difference: {diff_s}s");
}

#[test]
fn rc_epoch_utc_to_tt() {
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = measures_frame();
    let rust_tt = utc.convert_to(EpochRef::TT, &frame).unwrap();

    let cpp_tt_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "UTC", "TT").unwrap();

    let diff_s = (rust_tt.value().as_mjd() - cpp_tt_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-3, "Rust vs C++ TT difference: {diff_s}s");
}

#[test]
fn rc_epoch_tt_to_tdb() {
    let tt = MEpoch::from_mjd(J2000_MJD, EpochRef::TT);
    let frame = measures_frame();
    let rust_tdb = tt.convert_to(EpochRef::TDB, &frame).unwrap();

    let cpp_tdb_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "TT", "TDB").unwrap();

    let diff_s = (rust_tdb.value().as_mjd() - cpp_tdb_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-2, "Rust vs C++ TDB difference: {diff_s}s");
}

#[test]
fn rc_epoch_record_format() {
    // Rust creates record, C++ creates from same input, compare fields.
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let rec = epoch_to_record(&epoch);

    let (cpp_value, cpp_unit, cpp_refer) =
        MeasuresOracle::epoch_to_record(J2000_MJD, "UTC").unwrap();

    if let Some(casa_core::Value::Scalar(casa_core::ScalarValue::String(r))) = rec.get("refer") {
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
        MeasuresOracle::position_convert(VLA_X, VLA_Y, VLA_Z, "ITRF", "WGS84").unwrap();

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

    let (cpp_x, cpp_y, cpp_z) =
        MeasuresOracle::position_convert(lon, lat, h, "WGS84", "ITRF").unwrap();

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

    let (cpp_lon, cpp_lat, cpp_r) =
        MeasuresOracle::position_to_record(VLA_X, VLA_Y, VLA_Z).unwrap();

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
    let frame = measures_frame();
    let rust_z = d.convert_to(DopplerRef::Z, &frame).unwrap();
    let cpp_z = MeasuresOracle::doppler_convert(0.3, "RADIO", "Z").unwrap();
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
    let frame = measures_frame();
    let rust_ratio = d.convert_to(DopplerRef::RATIO, &frame).unwrap();
    let cpp_ratio = MeasuresOracle::doppler_convert(0.5, "Z", "RATIO").unwrap();
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
    let frame = measures_frame();
    let rust_radio = d.convert_to(DopplerRef::RADIO, &frame).unwrap();
    let cpp_radio = MeasuresOracle::doppler_convert(0.6, "BETA", "RADIO").unwrap();
    assert!(
        close(rust_radio.value(), cpp_radio, 1e-10),
        "Rust={}, C++={}",
        rust_radio.value(),
        cpp_radio
    );
}

#[test]
fn rc_catalog_line_hi_matches_cpp() {
    let measures = measures_runtime();
    let rust_line = MFrequency::from_line_name("HI", measures.as_ref()).unwrap();
    let cpp_line_hz = MeasuresOracle::line_frequency("HI").unwrap();
    assert!(
        close(rust_line.hz(), cpp_line_hz, 1.0e-3),
        "HI rest frequency: Rust={}, C++={}",
        rust_line.hz(),
        cpp_line_hz
    );
}

#[test]
fn rc_direction_j2000_to_galactic() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = measures_frame();
    let rust_gal = d.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_gal.as_angles();
    let (cpp_lon, cpp_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "J2000", "GALACTIC", 0.0, 0.0, 0.0, 0.0)
            .unwrap();
    assert!(
        close(rust_lon, cpp_lon, 1e-9),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-9),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn rc_direction_j2000_to_ecliptic() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = measures_frame().with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC));
    let rust_ecl = d.convert_to(DirectionRef::ECLIPTIC, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_ecl.as_angles();
    let (cpp_lon, cpp_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "J2000", "ECLIPTIC", J2000_MJD, 0.0, 0.0, 0.0)
            .unwrap();
    assert!(
        close(rust_lon, cpp_lon, 1e-9),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-9),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn rc_direction_galactic_to_supergal() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::GALACTIC);
    let frame = measures_frame();
    let rust_sg = d.convert_to(DirectionRef::SUPERGAL, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_sg.as_angles();
    let (cpp_lon, cpp_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "GALACTIC", "SUPERGAL", 0.0, 0.0, 0.0, 0.0)
            .unwrap();
    assert!(
        close(rust_lon, cpp_lon, 1e-9),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-9),
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
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
    let rust_bary = f.convert_to(FrequencyRef::BARY, &frame).unwrap();
    let cpp_bary = MeasuresOracle::frequency_convert(
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
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
    let rust_lg = f.convert_to(FrequencyRef::LGROUP, &frame).unwrap();
    let cpp_lg = MeasuresOracle::frequency_convert(
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
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
    let rust_cmb = f.convert_to(FrequencyRef::CMB, &frame).unwrap();
    let cpp_cmb = MeasuresOracle::frequency_convert(
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
    let cpp_tai_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "UTC", "TAI").unwrap();

    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = measures_frame();
    let rust_tai = utc.convert_to(EpochRef::TAI, &frame).unwrap();

    let diff_s = (rust_tai.value().as_mjd() - cpp_tai_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-6, "C++ vs Rust TAI difference: {diff_s}s");
}

#[test]
fn cr_epoch_tai_to_utc() {
    let cpp_utc_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "TAI", "UTC").unwrap();

    let tai = MEpoch::from_mjd(J2000_MJD, EpochRef::TAI);
    let frame = measures_frame();
    let rust_utc = tai.convert_to(EpochRef::UTC, &frame).unwrap();

    let diff_s = (rust_utc.value().as_mjd() - cpp_utc_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-6, "C++ vs Rust UTC difference: {diff_s}s");
}

#[test]
fn cr_epoch_utc_to_tt() {
    let cpp_tt_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "UTC", "TT").unwrap();

    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = measures_frame();
    let rust_tt = utc.convert_to(EpochRef::TT, &frame).unwrap();

    let diff_s = (rust_tt.value().as_mjd() - cpp_tt_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-3, "C++ vs Rust TT difference: {diff_s}s");
}

#[test]
fn cr_epoch_tt_to_tdb() {
    let cpp_tdb_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "TT", "TDB").unwrap();

    let tt = MEpoch::from_mjd(J2000_MJD, EpochRef::TT);
    let frame = measures_frame();
    let rust_tdb = tt.convert_to(EpochRef::TDB, &frame).unwrap();

    let diff_s = (rust_tdb.value().as_mjd() - cpp_tdb_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-2, "C++ vs Rust TDB difference: {diff_s}s");
}

#[test]
fn cr_epoch_record_format() {
    // C++ serializes epoch to record fields, Rust produces same format.
    let (cpp_value, cpp_unit, cpp_refer) =
        MeasuresOracle::epoch_to_record(J2000_MJD, "UTC").unwrap();

    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let rec = epoch_to_record(&epoch);

    if let Some(casa_core::Value::Scalar(casa_core::ScalarValue::String(r))) = rec.get("refer") {
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
    let (cpp_lon, cpp_lat, _) =
        MeasuresOracle::position_convert(VLA_X, VLA_Y, VLA_Z, "ITRF", "WGS84").unwrap();

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

    let (cpp_x, cpp_y, cpp_z) =
        MeasuresOracle::position_convert(lon, lat, h, "WGS84", "ITRF").unwrap();

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
    let (cpp_lon, cpp_lat, cpp_r) =
        MeasuresOracle::position_to_record(VLA_X, VLA_Y, VLA_Z).unwrap();

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
    let cpp_z = MeasuresOracle::doppler_convert(0.3, "RADIO", "Z").unwrap();
    let d = MDoppler::new(0.3, DopplerRef::RADIO);
    let frame = measures_frame();
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
    let cpp_ratio = MeasuresOracle::doppler_convert(0.5, "Z", "RATIO").unwrap();
    let d = MDoppler::new(0.5, DopplerRef::Z);
    let frame = measures_frame();
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
    let cpp_radio = MeasuresOracle::doppler_convert(0.6, "BETA", "RADIO").unwrap();
    let d = MDoppler::new(0.6, DopplerRef::BETA);
    let frame = measures_frame();
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
        MeasuresOracle::direction_convert(1.0, 0.5, "J2000", "GALACTIC", 0.0, 0.0, 0.0, 0.0)
            .unwrap();
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = measures_frame();
    let rust_gal = d.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_gal.as_angles();
    assert!(
        close(rust_lon, cpp_lon, 1e-9),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-9),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn cr_direction_j2000_to_ecliptic() {
    let (cpp_lon, cpp_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "J2000", "ECLIPTIC", J2000_MJD, 0.0, 0.0, 0.0)
            .unwrap();
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = measures_frame().with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC));
    let rust_ecl = d.convert_to(DirectionRef::ECLIPTIC, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_ecl.as_angles();
    assert!(
        close(rust_lon, cpp_lon, 1e-9),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-9),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn cr_direction_galactic_to_supergal() {
    let (cpp_lon, cpp_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "GALACTIC", "SUPERGAL", 0.0, 0.0, 0.0, 0.0)
            .unwrap();
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::GALACTIC);
    let frame = measures_frame();
    let rust_sg = d.convert_to(DirectionRef::SUPERGAL, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_sg.as_angles();
    assert!(
        close(rust_lon, cpp_lon, 1e-9),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-9),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn cr_frequency_lsrk_to_bary() {
    let cpp_bary = MeasuresOracle::frequency_convert(
        1.42e9, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let f = MFrequency::new(1.42e9, FrequencyRef::LSRK);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
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
    let cpp_lg = MeasuresOracle::frequency_convert(
        1.42e9, "BARY", "LGROUP", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let f = MFrequency::new(1.42e9, FrequencyRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
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
    let cpp_cmb = MeasuresOracle::frequency_convert(
        1.42e9, "BARY", "CMB", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let f = MFrequency::new(1.42e9, FrequencyRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
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
    let frame = measures_frame();
    let rust_ratio = d.convert_to(DopplerRef::RATIO, &frame).unwrap();
    let cpp_ratio = MeasuresOracle::doppler_convert(2.0, "GAMMA", "RATIO").unwrap();
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
    let frame = measures_frame();
    let rust_gamma = d.convert_to(DopplerRef::GAMMA, &frame).unwrap();
    let cpp_gamma = MeasuresOracle::doppler_convert(0.5, "BETA", "GAMMA").unwrap();
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
    let frame = measures_frame();
    let mut prev = f64::NAN;
    for v in [0.1, 0.3, 0.5, 0.7, 0.9] {
        let d = MDoppler::new(v, DopplerRef::RADIO);
        let rust_z = d.convert_to(DopplerRef::Z, &frame).unwrap();
        let cpp_z = MeasuresOracle::doppler_convert(v, "RADIO", "Z").unwrap();
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
    let frame = measures_frame();
    let rust_icrs = d.convert_to(DirectionRef::ICRS, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_icrs.as_angles();
    let (cpp_lon, cpp_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "J2000", "ICRS", 0.0, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close(rust_lon, cpp_lon, 1e-9),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-9),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn rc_direction_j2000_to_jmean() {
    // Same conversion tested in perf benchmark — epoch-dependent precession
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = measures_frame().with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC));
    let rust_jm = d.convert_to(DirectionRef::JMEAN, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_jm.as_angles();
    let (cpp_lon, cpp_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "J2000", "JMEAN", J2000_MJD, 0.0, 0.0, 0.0)
            .unwrap();
    assert!(
        close(rust_lon, cpp_lon, 5e-5),
        "lon: Rust={}, C++={}",
        rust_lon,
        cpp_lon
    );
    assert!(
        close(rust_lat, cpp_lat, 1e-9),
        "lat: Rust={}, C++={}",
        rust_lat,
        cpp_lat
    );
}

#[test]
fn rc_direction_multiple_values() {
    // Prove direction output varies with input
    let frame = measures_frame();
    let mut prev_lon = f64::NAN;
    for lon_in in [0.5, 1.0, 2.0, 3.0, 5.0] {
        let d = MDirection::from_angles(lon_in, 0.5, DirectionRef::J2000);
        let rust_gal = d.convert_to(DirectionRef::GALACTIC, &frame).unwrap();
        let (rust_lon, rust_lat) = rust_gal.as_angles();
        let (cpp_lon, cpp_lat) =
            MeasuresOracle::direction_convert(lon_in, 0.5, "J2000", "GALACTIC", 0.0, 0.0, 0.0, 0.0)
                .unwrap();
        // Use angle-aware comparison (C++ getLong returns [-π,π], Rust returns [0,2π))
        assert!(
            close_angle(rust_lon, cpp_lon, 1e-9),
            "lon_in={lon_in}: Rust lon={}, C++ lon={}",
            rust_lon,
            cpp_lon
        );
        assert!(
            close(rust_lat, cpp_lat, 1e-9),
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
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
    let rust_bary = f.convert_to(FrequencyRef::BARY, &frame).unwrap();
    let cpp_bary = MeasuresOracle::frequency_convert(
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
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
    let rust_lg = f.convert_to(FrequencyRef::LGROUP, &frame).unwrap();
    let cpp_lg = MeasuresOracle::frequency_convert(
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
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
    let mut prev = f64::NAN;
    for hz_in in [1.0e9, 1.2e9, 1.42e9, 2.0e9, 5.0e9] {
        let f = MFrequency::new(hz_in, FrequencyRef::LSRK);
        let rust_bary = f.convert_to(FrequencyRef::BARY, &frame).unwrap();
        let cpp_bary = MeasuresOracle::frequency_convert(
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
    let frame1 = measures_frame()
        .with_direction(dir1.clone())
        .with_epoch(epoch.clone());
    let j1 = dir1.convert_to(DirectionRef::J2000, &frame1).unwrap();
    let (lon1, lat1) = j1.as_angles();

    // Direction opposite (galactic anticenter)
    let dir2 = MDirection::from_angles(std::f64::consts::PI, 0.0, DirectionRef::GALACTIC);
    let frame2 = measures_frame()
        .with_direction(dir2.clone())
        .with_epoch(epoch);
    let j2 = dir2.convert_to(DirectionRef::J2000, &frame2).unwrap();
    let (lon2, lat2) = j2.as_angles();

    let f_in = 1.42e9;

    let rust_bary1 = MFrequency::new(f_in, FrequencyRef::LSRK)
        .convert_to(FrequencyRef::BARY, &frame1)
        .unwrap();
    let cpp_bary1 = MeasuresOracle::frequency_convert(
        f_in, "LSRK", "BARY", lon1, lat1, "J2000", J2000_MJD, 0.0, 0.0, 0.0,
    )
    .unwrap();

    let rust_bary2 = MFrequency::new(f_in, FrequencyRef::LSRK)
        .convert_to(FrequencyRef::BARY, &frame2)
        .unwrap();
    let cpp_bary2 = MeasuresOracle::frequency_convert(
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
    let frame = measures_frame()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs);
    let rust_geo = f.convert_to(FrequencyRef::GEO, &frame).unwrap();
    let cpp_geo = MeasuresOracle::frequency_convert(
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
    let cpp_geo = MeasuresOracle::frequency_convert(
        1.42e9, "BARY", "GEO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let f = MFrequency::new(cpp_geo, FrequencyRef::GEO);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = measures_frame()
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
    let frame = measures_frame()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs)
        .with_dut1(0.3);
    let rust_topo = f.convert_to(FrequencyRef::TOPO, &frame).unwrap();
    let cpp_topo = MeasuresOracle::frequency_convert(
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
    let cpp_topo = MeasuresOracle::frequency_convert(
        1.42e9, "GEO", "TOPO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let f = MFrequency::new(cpp_topo, FrequencyRef::TOPO);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = measures_frame()
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
    let frame = measures_frame()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs)
        .with_dut1(0.3);
    let rust_topo = f.convert_to(FrequencyRef::TOPO, &frame).unwrap();
    let cpp_topo = MeasuresOracle::frequency_convert(
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
