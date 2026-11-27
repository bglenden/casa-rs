// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

// ==========================================================================
// CC: C++ roundtrip baseline
// ==========================================================================

#[test]
fn cc_epoch_utc_tai_roundtrip() {
    let tai_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "UTC", "TAI").unwrap();
    let back_mjd = MeasuresOracle::epoch_convert(tai_mjd, "TAI", "UTC").unwrap();

    let diff_s = (back_mjd - J2000_MJD).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-9, "C++ UTC→TAI roundtrip error: {diff_s}s");
}

#[test]
fn cc_epoch_tai_utc_roundtrip() {
    let utc_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "TAI", "UTC").unwrap();
    let back_mjd = MeasuresOracle::epoch_convert(utc_mjd, "UTC", "TAI").unwrap();

    let diff_s = (back_mjd - J2000_MJD).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-9, "C++ TAI→UTC roundtrip error: {diff_s}s");
}

#[test]
fn cc_epoch_utc_to_tt() {
    // C++ roundtrip UTC→TT→UTC
    let tt_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "UTC", "TT").unwrap();
    let back_mjd = MeasuresOracle::epoch_convert(tt_mjd, "TT", "UTC").unwrap();

    let diff_s = (back_mjd - J2000_MJD).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-9, "C++ UTC→TT roundtrip error: {diff_s}s");
}

#[test]
fn cc_epoch_tt_to_tdb() {
    // C++ roundtrip TT→TDB→TT
    let tdb_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "TT", "TDB").unwrap();
    let back_mjd = MeasuresOracle::epoch_convert(tdb_mjd, "TDB", "TT").unwrap();

    let diff_s = (back_mjd - J2000_MJD).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-6, "C++ TT→TDB roundtrip error: {diff_s}s");
}

#[test]
fn cc_epoch_record_roundtrip() {
    // C++ creates record fields, verify values match input.
    let (value, unit, refer) = MeasuresOracle::epoch_to_record(J2000_MJD, "TAI").unwrap();
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
    let (lon, lat, h) =
        MeasuresOracle::position_convert(VLA_X, VLA_Y, VLA_Z, "ITRF", "WGS84").unwrap();
    let (back_x, back_y, back_z) =
        MeasuresOracle::position_convert(lon, lat, h, "WGS84", "ITRF").unwrap();

    assert!(close(VLA_X, back_x, 1.0), "x: {VLA_X} vs {back_x}");
    assert!(close(VLA_Y, back_y, 1.0), "y: {VLA_Y} vs {back_y}");
    assert!(close(VLA_Z, back_z, 1.0), "z: {VLA_Z} vs {back_z}");
}

#[test]
fn cc_position_record() {
    // C++ creates position record fields, verify values are sensible.
    let (lon, lat, radius) = MeasuresOracle::position_to_record(VLA_X, VLA_Y, VLA_Z).unwrap();

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
    let z_val = MeasuresOracle::doppler_convert(0.3, "RADIO", "Z").unwrap();
    let back = MeasuresOracle::doppler_convert(z_val, "Z", "RADIO").unwrap();
    assert!(
        close(back, 0.3, 1e-12),
        "C++ RADIO→Z→RADIO roundtrip: 0.3 vs {back}"
    );
}

#[test]
fn cc_direction_j2000_galactic_roundtrip() {
    let (gal_lon, gal_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "J2000", "GALACTIC", 0.0, 0.0, 0.0, 0.0)
            .unwrap();
    let (back_lon, back_lat) = MeasuresOracle::direction_convert(
        gal_lon, gal_lat, "GALACTIC", "J2000", 0.0, 0.0, 0.0, 0.0,
    )
    .unwrap();
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
    let bary = MeasuresOracle::frequency_convert(
        1.42e9, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let back = MeasuresOracle::frequency_convert(
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
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
    let rust_bary = rv.convert_to(RadialVelocityRef::BARY, &frame).unwrap();
    let cpp_bary = MeasuresOracle::radvel_convert(
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
    let frame = measures_frame()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs);
    let rust_geo = rv.convert_to(RadialVelocityRef::GEO, &frame).unwrap();
    let cpp_geo = MeasuresOracle::radvel_convert(
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
    let frame = measures_frame().with_direction(dir);
    let rust_lg = rv.convert_to(RadialVelocityRef::LGROUP, &frame).unwrap();
    let cpp_lg = MeasuresOracle::radvel_convert(
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
    let frame = measures_frame().with_direction(dir);
    let rust_cmb = rv.convert_to(RadialVelocityRef::CMB, &frame).unwrap();
    let cpp_cmb = MeasuresOracle::radvel_convert(
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
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
    let mut prev = f64::NAN;
    for ms_in in [10_000.0, 30_000.0, 50_000.0, 100_000.0, 200_000.0] {
        let rv = MRadialVelocity::new(ms_in, RadialVelocityRef::LSRK);
        let rust_bary = rv.convert_to(RadialVelocityRef::BARY, &frame).unwrap();
        let cpp_bary = MeasuresOracle::radvel_convert(
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
    let cpp_bary = MeasuresOracle::radvel_convert(
        50_000.0, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::LSRK);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let frame = measures_frame().with_direction(dir).with_epoch(epoch);
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
    let cpp_geo = MeasuresOracle::radvel_convert(
        50_000.0, "BARY", "GEO", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = measures_frame()
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
    let cpp_lg = MeasuresOracle::radvel_convert(
        50_000.0, "BARY", "LGROUP", M31_LON, M31_LAT, "J2000", 0.0, 0.0, 0.0, 0.0,
    )
    .unwrap();
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::BARY);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let frame = measures_frame().with_direction(dir);
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
    let bary = MeasuresOracle::radvel_convert(
        50_000.0, "LSRK", "BARY", M31_LON, M31_LAT, "J2000", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let back = MeasuresOracle::radvel_convert(
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
    let frame = measures_frame()
        .with_direction(dir)
        .with_radial_velocity(rv);
    let rust_lsrk = f.convert_to(FrequencyRef::LSRK, &frame).unwrap();
    let cpp_lsrk = MeasuresOracle::frequency_convert_with_rv(
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
    let frame = measures_frame()
        .with_direction(dir)
        .with_radial_velocity(rv);
    let rust_rest = f.convert_to(FrequencyRef::REST, &frame).unwrap();
    let cpp_rest = MeasuresOracle::frequency_convert_with_rv(
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
    let cpp_lsrk = MeasuresOracle::frequency_convert_with_rv(
        1.42e9, "REST", "LSRK", M31_LON, M31_LAT, "J2000", 0.0, 0.0, 0.0, 0.0, 50_000.0, "LSRK",
    )
    .unwrap();
    let f = MFrequency::new(1.42e9, FrequencyRef::REST);
    let dir = MDirection::from_angles(M31_LON, M31_LAT, DirectionRef::J2000);
    let rv = MRadialVelocity::new(50_000.0, RadialVelocityRef::LSRK);
    let frame = measures_frame()
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
    let lsrk = MeasuresOracle::frequency_convert_with_rv(
        1.42e9, "REST", "LSRK", M31_LON, M31_LAT, "J2000", 0.0, 0.0, 0.0, 0.0, 50_000.0, "LSRK",
    )
    .unwrap();
    let back = MeasuresOracle::frequency_convert_with_rv(
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
    let frame = measures_frame();
    let rust_b = d.convert_to(DirectionRef::B1950, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_b.as_angles();
    let (cpp_lon, cpp_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "J2000", "B1950", 0.0, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close_angle(rust_lon, cpp_lon, 1e-9),
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
fn rc_dir_b1950_to_j2000() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::B1950);
    let frame = measures_frame();
    let rust_j = d.convert_to(DirectionRef::J2000, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_j.as_angles();
    let (cpp_lon, cpp_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "B1950", "J2000", 0.0, 0.0, 0.0, 0.0).unwrap();
    assert!(
        close_angle(rust_lon, cpp_lon, 1e-9),
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
fn cr_dir_j2000_to_b1950() {
    let (cpp_lon, cpp_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "J2000", "B1950", 0.0, 0.0, 0.0, 0.0).unwrap();
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = measures_frame();
    let rust_b = d.convert_to(DirectionRef::B1950, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_b.as_angles();
    assert!(
        close_angle(rust_lon, cpp_lon, 1e-9),
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
fn cc_dir_b1950_roundtrip() {
    let (b_lon, b_lat) =
        MeasuresOracle::direction_convert(1.0, 0.5, "J2000", "B1950", 0.0, 0.0, 0.0, 0.0).unwrap();
    let (back_lon, back_lat) =
        MeasuresOracle::direction_convert(b_lon, b_lat, "B1950", "J2000", 0.0, 0.0, 0.0, 0.0)
            .unwrap();
    // FK4/FK5 roundtrips are not exact to machine precision in casacore C++;
    // keep this as a stability check without making it platform-fragile.
    let sep = sep_arcsec(1.0, 0.5, back_lon, back_lat);
    assert!(
        sep < 5.0,
        "B1950/J2000 C++ roundtrip separation: {sep} arcsec"
    );
}

// ==========================================================================
// Wave 5: ITRF direction RC/CR
// ==========================================================================

#[test]
fn rc_dir_j2000_to_itrf() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_dut1(0.3);
    let rust_itrf = d.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_itrf.as_angles();
    let (cpp_lon, cpp_lat) = MeasuresOracle::direction_convert(
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
    let (itrf_lon, itrf_lat) = MeasuresOracle::direction_convert(
        1.0, 0.5, "J2000", "ITRF", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let d = MDirection::from_angles(itrf_lon, itrf_lat, DirectionRef::ITRF);
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_dut1(0.3);
    let rust_j = d.convert_to(DirectionRef::J2000, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_j.as_angles();
    let (cpp_lon, cpp_lat) = MeasuresOracle::direction_convert(
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
    let (cpp_lon, cpp_lat) = MeasuresOracle::direction_convert(
        1.0, 0.5, "J2000", "ITRF", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = measures_frame()
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
// Rust roundtrip consistency is tested in casa-types/tests/measures.rs.

#[test]
fn rc_dir_hadec_topo_roundtrip() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::HADEC);
    let frame = measures_frame()
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
    let frame = measures_frame().with_position(vla).with_dut1(0.3);
    let rust_gast = ut1.convert_to(EpochRef::GAST, &frame).unwrap();
    let cpp_gast = MeasuresOracle::epoch_convert_with_frame(
        ut1_mjd, "UT1", "GAST", VLA_LON, VLA_LAT, VLA_H, 0.3,
    )
    .unwrap();
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
    let cpp_gast = MeasuresOracle::epoch_convert_with_frame(
        ut1_mjd, "UT1", "GAST", VLA_LON, VLA_LAT, VLA_H, 0.3,
    )
    .unwrap();
    let gast = MEpoch::from_mjd(cpp_gast, EpochRef::GAST);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = measures_frame().with_position(vla).with_dut1(0.3);
    let rust_last = gast.convert_to(EpochRef::LAST, &frame).unwrap();
    let cpp_last = MeasuresOracle::epoch_convert_with_frame(
        cpp_gast, "GAST", "LAST", VLA_LON, VLA_LAT, VLA_H, 0.3,
    )
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
    let frame = measures_frame().with_position(vla).with_dut1(0.3);
    let gmst = ut1.convert_to(EpochRef::GMST1, &frame).unwrap();
    // Now convert back
    let back_ut1 = gmst.convert_to(EpochRef::UT1, &frame).unwrap();
    let diff_s = (back_ut1.value().as_mjd() - ut1_mjd).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-3, "GMST1→UT1 roundtrip error: {diff_s}s");
}

#[test]
fn cr_epoch_ut1_to_gast() {
    let ut1_mjd = J2000_MJD + 0.3 / SECONDS_PER_DAY;
    let cpp_gast = MeasuresOracle::epoch_convert_with_frame(
        ut1_mjd, "UT1", "GAST", VLA_LON, VLA_LAT, VLA_H, 0.3,
    )
    .unwrap();
    let ut1 = MEpoch::from_mjd(ut1_mjd, EpochRef::UT1);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = measures_frame().with_position(vla).with_dut1(0.3);
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
    let cpp_gast = MeasuresOracle::epoch_convert_with_frame(
        ut1_mjd, "UT1", "GAST", VLA_LON, VLA_LAT, VLA_H, 0.3,
    )
    .unwrap();
    let cpp_last = MeasuresOracle::epoch_convert_with_frame(
        cpp_gast, "GAST", "LAST", VLA_LON, VLA_LAT, VLA_H, 0.3,
    )
    .unwrap();
    let gast = MEpoch::from_mjd(cpp_gast, EpochRef::GAST);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = measures_frame().with_position(vla).with_dut1(0.3);
    let rust_last = gast.convert_to(EpochRef::LAST, &frame).unwrap();
    // Compare fractional sidereal time (integer-day conventions differ)
    let rust_frac = rust_last.value().frac();
    let cpp_frac = cpp_last - cpp_last.floor();
    let diff_s = (rust_frac - cpp_frac).abs() * SECONDS_PER_DAY;
    assert!(diff_s < 1e-3, "C++ vs Rust LAST frac diff: {diff_s}s");
}
