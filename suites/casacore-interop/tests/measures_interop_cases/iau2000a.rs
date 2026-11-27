// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

// ==========================================================================
// IAU 2006/2000A interop tests
//
// SOFA and casacore implement the same IAU standards but with different
// polynomial series and internal decompositions. For IAU 2000A, the
// precession/nutation matrices agree perfectly (verified in
// diag_iau2000a_chain_steps: JMEAN and JTRUE match to 0.000"),
// but the apparent-place (APP) step diverges by ~16 mas. Upstream
// casacore PR #1464 records the same missing-frame-bias hypothesis for
// casacore's shared IAU2000 JNAT<->APP helper, but that PR is not
// independent confirmation because it was derived from the same line of
// investigation. Until that lands upstream, keep the current residual
// explicitly bounded here. The smaller ~1.5 mas residual still remains
// from the expected aberration/deflection and velocity-series differences.
//
// Tolerance: 1e-7 rad ≈ 20 mas. See misc/casacore_vs_sofa_deviation.cpp
// for a standalone C++ test quantifying this, and the corresponding
// GitHub issue filed with the casacore maintainers.
// ==========================================================================

#[test]
fn iau2000a_dir_j2000_to_hadec() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime())
        .with_iau_model(IauModel::Iau2006_2000A);
    let rust_hadec = d.convert_to(DirectionRef::HADEC, &frame).unwrap();
    let (rust_ha, rust_dec) = rust_hadec.as_angles();
    let (cpp_ha, cpp_dec) = MeasuresOracle::direction_convert_iau2000a(
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
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime())
        .with_iau_model(IauModel::Iau2006_2000A);
    let rust_azel = d.convert_to(DirectionRef::AZEL, &frame).unwrap();
    let (rust_az, rust_el) = rust_azel.as_angles();
    let (cpp_az, cpp_el) = MeasuresOracle::direction_convert_iau2000a(
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
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime())
        .with_iau_model(IauModel::Iau2006_2000A);
    let rust_itrf = d.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_itrf.as_angles();
    let (cpp_lon, cpp_lat) = MeasuresOracle::direction_convert_iau2000a(
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
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime())
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
        let (c_lon, c_lat) = MeasuresOracle::direction_convert_iau2000a(
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
    let data = MeasuresOracle::iau2000_precession_matrix(tt_mjd).unwrap();

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
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime())
        .with_iau_model(IauModel::Iau2006_2000A);
    let rust_jmean = d.convert_to(DirectionRef::JMEAN, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_jmean.as_angles();
    eprintln!("Rust JMEAN:          lon={rust_lon:.12} lat={rust_lat:.12}");

    // C++ via full direction convert
    let (cpp_full_lon, cpp_full_lat) = MeasuresOracle::direction_convert_iau2000a(
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
        let f = measures_frame()
            .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
            .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
            .with_measures(measures_runtime())
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

    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime())
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
        .convert_to(casa_core::measures::EpochRef::TT, &frame)
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
    let (cpp_1976_lon, cpp_1976_lat) = MeasuresOracle::direction_convert(
        1.0, 0.5, "J2000", "APP", J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
    )
    .unwrap();
    let (cpp_2000a_lon, cpp_2000a_lat) = MeasuresOracle::direction_convert_iau2000a(
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
    let (cpp_v, cpp_sun) = MeasuresOracle::earth_velocity(epoch_mjd).unwrap();
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
        let (cpp_1976_lon, cpp_1976_lat) = MeasuresOracle::direction_convert(
            1.0, 0.5, "J2000", step, J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
        )
        .unwrap();
        let (cpp_2000a_lon, cpp_2000a_lat) = MeasuresOracle::direction_convert_iau2000a(
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
