// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

// ==========================================================================
// Wave 5b: EOP data interop — Rust explicit runtime data vs C++ casacore IERS tables
// ==========================================================================

#[test]
fn eop_dut1_at_j2000() {
    // Compare dUT1 from Rust explicit runtime EOP data vs C++ casacore's IERS tables
    let eop = measures_runtime();
    let rust_vals = eop
        .eop_values(J2000_MJD)
        .expect("read EOP")
        .expect("J2000 should be in EOP range");
    let (cpp_dut1, _, _) = MeasuresOracle::eop_query(J2000_MJD).unwrap();

    // The oracle reads the Rust runtime's tables (#845): agreement to rounding.
    let diff = (rust_vals.dut1_seconds - cpp_dut1).abs();
    assert!(
        diff < 1e-9,
        "dUT1 at J2000: Rust={:.7}, C++={:.7}, diff={:.4}s",
        rust_vals.dut1_seconds,
        cpp_dut1,
        diff
    );
}

#[test]
fn eop_polar_motion_at_j2000() {
    let eop = measures_runtime();
    let rust_vals = eop
        .eop_values(J2000_MJD)
        .expect("read EOP")
        .expect("J2000 should be in EOP range");
    let (_, cpp_xp, cpp_yp) = MeasuresOracle::eop_query(J2000_MJD).unwrap();

    // One table set on both sides (#845): agreement to rounding.
    let xp_diff = (rust_vals.x_arcsec - cpp_xp).abs();
    let yp_diff = (rust_vals.y_arcsec - cpp_yp).abs();
    assert!(
        xp_diff < 1e-9,
        "xp at J2000: Rust={:.6}\", C++={:.6}\", diff={:.6}\"",
        rust_vals.x_arcsec,
        cpp_xp,
        xp_diff
    );
    assert!(
        yp_diff < 1e-9,
        "yp at J2000: Rust={:.6}\", C++={:.6}\", diff={:.6}\"",
        rust_vals.y_arcsec,
        cpp_yp,
        yp_diff
    );
}

#[test]
fn eop_dut1_range_of_dates() {
    // Compare dUT1 across several dates to verify systematic agreement
    let eop = measures_runtime();
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
        let rust_vals = match eop.eop_values(mjd).expect("read EOP") {
            Some(v) => v,
            None => continue,
        };
        let (cpp_dut1, cpp_xp, cpp_yp) = match MeasuresOracle::eop_query(mjd) {
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
fn eop_epoch_conversion_with_runtime_data() {
    // Test that epoch conversion using explicit runtime EOP data matches C++ results.
    // This verifies the full pipeline: explicit runtime data → MeasFrame → dut1_for_mjd → conversion.
    let eop = measures_runtime();
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);

    // Use explicit runtime EOP for dUT1 instead of hardcoded value
    let frame = measures_frame().with_measures(eop);

    let rust_ut1 = utc.convert_to(EpochRef::UT1, &frame).unwrap();

    // C++ reads the same runtime's IERS tables through the oracle (#845)
    let cpp_ut1_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "UTC", "UT1").unwrap();

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

// ==========================================================================
// Wave 5c: EOP-driven interop tests
// Both Rust (measures provider) and C++ (internal IERS tables) use their
// own EOP data — no hardcoded dUT1.
// ==========================================================================

// MJD constants for multi-epoch tests
const MJD_2010: f64 = 55197.0; // 2010-01-01
const MJD_2020: f64 = 58849.0; // 2020-01-01

// --- EOP-driven ITRF direction interop ---

#[test]
fn eop_dir_j2000_to_itrf() {
    let d = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime());
    let rust_itrf = d.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_itrf.as_angles();
    let (cpp_lon, cpp_lat) = MeasuresOracle::direction_convert(
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
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(MJD_2010, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime());
    let rust_itrf = d.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_itrf.as_angles();
    let (cpp_lon, cpp_lat) = MeasuresOracle::direction_convert(
        1.0, 0.5, "J2000", "ITRF", MJD_2010, VLA_LON, VLA_LAT, VLA_H,
    )
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
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(MJD_2020, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime());
    let rust_itrf = d.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (rust_lon, rust_lat) = rust_itrf.as_angles();
    let (cpp_lon, cpp_lat) = MeasuresOracle::direction_convert(
        1.0, 0.5, "J2000", "ITRF", MJD_2020, VLA_LON, VLA_LAT, VLA_H,
    )
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
    // Full pipeline: UTC → UT1 (via explicit runtime EOP) → GAST
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = measures_frame()
        .with_position(vla)
        .with_measures(measures_runtime());
    let rust_ut1 = utc.convert_to(EpochRef::UT1, &frame).unwrap();
    let rust_gast = rust_ut1.convert_to(EpochRef::GAST, &frame).unwrap();

    // C++ reads dUT1 from the shared IERS tables; the dut1 argument is ignored
    let cpp_ut1_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "UTC", "UT1").unwrap();
    let cpp_gast = MeasuresOracle::epoch_convert_with_frame(
        cpp_ut1_mjd,
        "UT1",
        "GAST",
        VLA_LON,
        VLA_LAT,
        VLA_H,
        0.0,
    )
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
    // Full pipeline: UTC → UT1 (via explicit runtime EOP) → GAST → LAST
    let utc = MEpoch::from_mjd(J2000_MJD, EpochRef::UTC);
    let vla = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = measures_frame()
        .with_position(vla)
        .with_measures(measures_runtime());
    let rust_ut1 = utc.convert_to(EpochRef::UT1, &frame).unwrap();
    let rust_gast = rust_ut1.convert_to(EpochRef::GAST, &frame).unwrap();
    let rust_last = rust_gast.convert_to(EpochRef::LAST, &frame).unwrap();

    // C++ chain
    let cpp_ut1_mjd = MeasuresOracle::epoch_convert(J2000_MJD, "UTC", "UT1").unwrap();
    let cpp_gast = MeasuresOracle::epoch_convert_with_frame(
        cpp_ut1_mjd,
        "UT1",
        "GAST",
        VLA_LON,
        VLA_LAT,
        VLA_H,
        0.0,
    )
    .unwrap();
    let cpp_last = MeasuresOracle::epoch_convert_with_frame(
        cpp_gast, "GAST", "LAST", VLA_LON, VLA_LAT, VLA_H, 0.0,
    )
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
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime());
    let rust_hadec = d.convert_to(DirectionRef::HADEC, &frame).unwrap();
    let (rust_ha, rust_dec) = rust_hadec.as_angles();
    let (cpp_ha, cpp_dec) = MeasuresOracle::direction_convert(
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
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime());
    let rust_azel = d.convert_to(DirectionRef::AZEL, &frame).unwrap();
    let (rust_az, rust_el) = rust_azel.as_angles();
    let (cpp_az, cpp_el) = MeasuresOracle::direction_convert(
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
    let frame = measures_frame()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs)
        .with_measures(measures_runtime());
    let rust_topo = f.convert_to(FrequencyRef::TOPO, &frame).unwrap();
    let cpp_topo = MeasuresOracle::frequency_convert(
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
    let frame = measures_frame()
        .with_direction(dir)
        .with_epoch(epoch)
        .with_position(obs)
        .with_measures(measures_runtime());
    let rust_topo = rv.convert_to(RadialVelocityRef::TOPO, &frame).unwrap();
    let cpp_topo = MeasuresOracle::radvel_convert(
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

#[test]
fn accepted_direction_divergence_routes_are_bounded() {
    fn target_ref(name: &str) -> DirectionRef {
        match name {
            "APP" => DirectionRef::APP,
            "HADEC" => DirectionRef::HADEC,
            "AZEL" => DirectionRef::AZEL,
            "ITRF" => DirectionRef::ITRF,
            _ => unreachable!("unexpected route"),
        }
    }

    let source = MDirection::from_angles(1.0, 0.5, DirectionRef::J2000);
    // IAU 1976/1980 agrees to 1 µas once the oracle shares tables and model (#845).
    let cases = [
        ("IAU1976/1980", IauModel::Iau1976_1980, 0.000_1_f64),
        ("IAU2006/2000A", IauModel::Iau2006_2000A, 0.03_f64),
    ];
    let routes = ["APP", "HADEC", "AZEL", "ITRF"];

    for (model_name, model, max_sep_arcsec) in cases {
        let frame = measures_frame()
            .with_epoch(MEpoch::from_mjd(J2000_MJD, EpochRef::UTC))
            .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
            .with_measures(measures_runtime())
            .with_iau_model(model);

        for route in routes {
            let rust_value = source.convert_to(target_ref(route), &frame).unwrap();
            let (rust_lon, rust_lat) = rust_value.as_angles();
            let (cpp_lon, cpp_lat) = match model {
                IauModel::Iau1976_1980 => MeasuresOracle::direction_convert(
                    1.0, 0.5, "J2000", route, J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
                ),
                IauModel::Iau2006_2000A => MeasuresOracle::direction_convert_iau2000a(
                    1.0, 0.5, "J2000", route, J2000_MJD, VLA_LON, VLA_LAT, VLA_H,
                ),
            }
            .unwrap();

            let sep = sep_arcsec(rust_lon, rust_lat, cpp_lon, cpp_lat);
            eprintln!("{model_name} J2000->{route}: sep={sep:.6} arcsec");
            assert!(
                sep <= max_sep_arcsec,
                "{model_name} J2000->{route}: sep={sep:.6} arcsec exceeds {max_sep_arcsec:.6}"
            );
        }
    }
}

// --- Full pipeline integration test ---

#[test]
fn eop_full_pipeline_radio_astronomy() {
    // Realistic radio astronomy workflow:
    // UTC epoch at a modern date, a direction near transit for the VLA,
    // Convert to ITRF, HADEC, AZEL using only measures provider().
    // Compare all three outputs against C++.
    let epoch_mjd = MJD_2010; // 2010-01-01

    // Use a source near transit at MJD 2010 for the VLA (well above horizon).
    // GAST at MJD 55197 ≈ 6.6h ≈ 1.73 rad; LAST ≈ 1.73 + VLA_LON ≈ -0.15
    // Use RA ≈ 6.0 rad with Dec=0 for well-defined Az/El (~56° elevation).
    let src = MDirection::from_angles(6.0, 0.0, DirectionRef::J2000);
    let epoch = MEpoch::from_mjd(epoch_mjd, EpochRef::UTC);
    let obs = MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H);
    let frame = measures_frame()
        .with_epoch(epoch)
        .with_position(obs)
        .with_direction(src.clone())
        .with_measures(measures_runtime());

    // ITRF
    let rust_itrf = src.convert_to(DirectionRef::ITRF, &frame).unwrap();
    let (r_itrf_lon, r_itrf_lat) = rust_itrf.as_angles();
    let (c_itrf_lon, c_itrf_lat) = MeasuresOracle::direction_convert(
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
    let (c_ha, c_dec) = MeasuresOracle::direction_convert(
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
    let (c_az, c_el) = MeasuresOracle::direction_convert(
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
    let frame = measures_frame()
        .with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC))
        .with_position(MPosition::new_wgs84(VLA_LON, VLA_LAT, VLA_H))
        .with_measures(measures_runtime());

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
        let (c_lon, c_lat) = MeasuresOracle::direction_convert(
            lon, lat, "J2000", step, epoch_mjd, VLA_LON, VLA_LAT, VLA_H,
        )
        .unwrap();
        let sep = sep_arcsec(r_lon, r_lat, c_lon, c_lat);
        eprintln!(
            "CHAIN J2000→{step:6}: sep={sep:12.6} arcsec  (R={:.8},{:.8}  C={:.8},{:.8})",
            r_lon, r_lat, c_lon, c_lat
        );
    }
}
