// SPDX-License-Identifier: LGPL-3.0-or-later

//! The analytic beams against CASA's: its band selection, its Float
//! rounding at radial bin edges and the values its `.pb` holds.

#[test]
fn evla_common_beams_cover_the_l_s_and_c_bands_only() {
    // casa-numerics holds CASA's fits and their nearest-frequency rule;
    // imaging fails closed outside L, S and C.
    for (frequency_hz, supported) in [
        (850.0e6, false),
        (1.4e9, true),
        (2.0e9, true),
        (3.0e9, true),
        (6.0e9, true),
        (8.4e9, false),
        (0.3e9, false),
    ] {
        assert_eq!(
            super::evla_common_pattern(frequency_hz).is_some(),
            supported,
            "{frequency_hz} Hz"
        );
    }
}

#[test]
fn vla_pb_channel_455_edge_matches_casa_support() {
    let table = super::vla_band_voltage_table(44.91e9).expect("VLA Q-band table");
    let mut plane = vec![0.0; 3 * 43];
    super::add_annular_aperture_power(
        &mut plane,
        [3, 43],
        [512.0, 512.0],
        [-2.908_882_086_657_215_7e-7, 2.908_882_086_657_215_7e-7],
        44.91e9,
        &table,
    );
    let edge = plane[2 * 43 + 42];
    assert!((edge - 0.200_095_06).abs() < 1e-6, "edge PB={edge}");
    assert!(edge > 0.2, "CASA keeps this channel-455 PB pixel");
}

#[test]
fn beam_coverage_includes_vla_nvss_and_keeps_evla_band_limits() {
    use crate::AnalyticPrimaryBeamModel::{CasaEvlaCommon, CasaVlaBand};
    for (model, frequency_hz, supported) in [
        (CasaVlaBand, 1.0e9, true),
        (CasaVlaBand, 1.4e9, true),
        (CasaVlaBand, 2.0e9, true),
        (CasaVlaBand, 8.0e9, true),
        (CasaVlaBand, 35.0e9, true),
        (CasaVlaBand, 44.0e9, true),
        (CasaVlaBand, 45.022e9, true),
        (CasaVlaBand, 54.880e9, true),
        (CasaVlaBand, 55.0e9, true),
        (CasaVlaBand, 55.008e9, true),
        (CasaEvlaCommon, 0.9e9, false),
        (CasaEvlaCommon, 1.4e9, true),
        (CasaEvlaCommon, 3.0e9, true),
        (CasaEvlaCommon, 6.0e9, true),
        (CasaEvlaCommon, 8.001e9, false),
        (CasaEvlaCommon, 44.0e9, false),
    ] {
        assert_eq!(
            super::primary_beam_frequency_supported(model, frequency_hz),
            supported,
            "{model:?} at {frequency_hz}"
        );
    }
}

#[test]
fn evla_common_s_band_uses_casa_sampled_power_lookup() {
    let frequency_hz = 2.091_980_123e9_f64;
    let pattern =
        super::evla_common_pattern(frequency_hz).expect("T44 frequency is in EVLA S band");
    assert_eq!(
        pattern.coefficients(),
        &[1.000, -1.429e-3, 7.52e-7, -1.47e-10]
    );
    let sample_index = 2_210.0_f64;
    let inverse_increment = 9_999.0_f64 / 58.0;
    let sampled_radius_arcmin_ghz = sample_index / inverse_increment;
    let radius_arcmin_ghz = sampled_radius_arcmin_ghz + 0.75 / inverse_increment;
    let x2 = sampled_radius_arcmin_ghz * sampled_radius_arcmin_ghz;
    let expected_power = 1.0 - 1.429e-3 * x2 + 7.52e-7 * x2.powi(2) - 1.47e-10 * x2.powi(3);
    let actual = pattern.power(radius_arcmin_ghz);
    assert!((actual - expected_power as f32).abs() < 1.0e-7);
    assert!(super::evla_common_pattern(850.0e6).is_none());
}

#[test]
fn evla_radial_bin_edges_use_casa_float_rounding_and_voltage_squared() {
    let coefficients = super::evla_common_pattern(6.0e9).unwrap();
    let cell = (0.05_f64 / 3600.0).to_radians();
    // Measured CASA PBMath1D pixels (8,693), (8,890), (29,59) on the
    // 4096-square 6-GHz pilot. Double-radius lookup picks adjacent bins.
    for (offset, expected) in [
        ([2040.0, 1355.0], 0.800_096_45_f32),
        ([2040.0, 1158.0], 0.815_362_93_f32),
        ([2019.0, 1989.0], 0.739_581_9_f32),
    ] {
        let mut actual = [0.0];
        super::add_evla_common_power(
            &mut actual,
            [1, 1],
            offset,
            [-cell, cell],
            6.0e9,
            coefficients,
        );
        assert_eq!(actual, [expected]);
    }
}

#[test]
fn vla_named_bands_share_the_casa_annular_airy_lookup() {
    let frequency_hz = 45_469_370_205.156_37;
    let table = super::vla_band_voltage_table(frequency_hz)
        .expect("issue #607 representative frequency is in VLA Q band");
    assert_eq!(table.maximum_radius(), 0.8564 * 60.0);

    let voltage = table.evaluate(29.919_033_706_45);
    assert_eq!((voltage * voltage).to_bits(), 0x3e6d_1a6e);

    for ghz in [0.05, 0.3, 1.5, 5.0, 9.0, 15.0, 25.0, 45.0] {
        let band = super::vla_band_voltage_table(ghz * 1e9).expect("named VLA band");
        assert_eq!(band.maximum_radius(), table.maximum_radius());
        for radius in [0.0, 10.0, 29.919_033_706_45, 50.0, 52.0] {
            assert_eq!(band.evaluate(radius), table.evaluate(radius));
        }
    }
}

/// ALMA's 12 m beam is CASA's `PBMath1DAiry(10.7 m, 0.75 m, 1.784°,
/// 1 GHz)`: its `.pb` along a row of a 345 GHz MFS image of
/// 2.0886e-7 rad cells, centred between pixels at (960.5, 960.5), as
/// CASA 6.7.6.14's tclean wrote it for simanalyze's call on a casa-rs
/// simobserve MeasurementSet (2026-11-03). The radius of ACA's 3.568°,
/// which casa-rs used for both, samples the table half as finely and
/// moves the beam by up to 2e-4, flipping the `pblimit` mask's edge
/// pixels.
#[test]
fn the_alma_12m_beam_is_the_one_casa_writes() {
    let increment_deg = 2.088_624_122_740_107_8e-7_f64.to_degrees();
    let frequency_factor = 60.0 * 345.0e9 / 1.0e9;
    let table = super::CasaAlmaAiry::ALMA_12M.table();
    // Pixel x of row 960, CASA's value there.
    let casa = [
        (1010, 0.9713292_f32),
        (1060, 0.85777074),
        (1110, 0.69328696),
        (1160, 0.5072244),
        (1210, 0.3295922),
        (1240, 0.23725419),
    ];
    for (x, expected) in casa {
        let [dx, dy] = [f64::from(x) - 960.5, 960.0 - 960.5];
        let r2 = ((dx * increment_deg).powi(2) + (dy * increment_deg).powi(2)) as f32;
        let radius = (f64::from(r2.sqrt()) * frequency_factor) as f32 as f64;
        let power = table.evaluate(radius).powi(2);
        assert!(
            (power - expected).abs() <= 2.0e-6,
            "pixel {x}: {power} against CASA's {expected}"
        );
    }
}

#[test]
fn vla_unassigned_intervals_and_exact_band_edges_select_nvss() {
    let nvss = super::AnnularApertureVoltageTable::new(24.5, 0.0, 0.8564 * 60.0);
    let named = super::vla_band_voltage_table(1.5e9).unwrap();
    assert_ne!(nvss.evaluate(30.0), named.evaluate(30.0));
    for ghz in [
        0.1,
        0.15,
        0.2,
        0.4,
        0.999_981_059_496_862_4,
        1.0,
        2.0,
        3.0,
        4.0,
        7.0,
        11.0,
        19.0,
        35.0,
        55.0,
        55.008,
        100.0,
    ] {
        let table = super::vla_band_voltage_table(ghz * 1e9).expect("VLA NVSS");
        assert_eq!(table.maximum_radius(), nvss.maximum_radius());
        for radius in [0.0, 10.0, 30.0, 50.0, 52.0] {
            assert_eq!(table.evaluate(radius), nvss.evaluate(radius), "{ghz} GHz");
        }
    }
}

#[test]
fn vla_invalid_frequencies_fail_closed() {
    for unsupported_hz in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -1.0] {
        assert!(!super::primary_beam_frequency_supported(
            crate::AnalyticPrimaryBeamModel::CasaVlaBand,
            unsupported_hz,
        ));
        assert!(super::vla_band_voltage_table(unsupported_hz).is_err());
    }
}
