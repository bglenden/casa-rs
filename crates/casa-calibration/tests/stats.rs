// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_types::Complex32;
#[cfg(feature = "slow-tests")]
use serde_json::Value as JsonValue;
use tempfile::TempDir;

use casa_calibration::{CalibrationStatsAxis, CalibrationStatsRequest, calibration_stats};

#[test]
fn calibration_stats_reports_global_and_grouped_amplitude_values() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = common::create_apply_gain_caltable(
        &dir.path().join("stats.gcal"),
        &["FIELD0", "FIELD1"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 10.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![Complex32::new(1.0, 0.0), Complex32::new(2.0, 0.0)],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 20.0,
                field_id: 1,
                spectral_window_id: 1,
                antenna_id: 1,
                gains: vec![Complex32::new(3.0, 0.0), Complex32::new(4.0, 0.0)],
                flags: vec![true, false],
            },
        ],
    );

    let report = calibration_stats(
        &table_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("CPARAM".to_string()),
            use_flags: false,
        },
    )
    .expect("compute stats");

    assert_eq!(report.global.npts, 4);
    assert_eq!(report.global.flagged_npts, 1);
    assert_eq!(report.global.total_npts, 4);
    assert!((report.global.sum - 10.0).abs() <= 1.0e-9);
    assert!((report.global.mean - 2.5).abs() <= 1.0e-9);
    assert!((report.global.median - 2.5).abs() <= 1.0e-9);
    assert!((report.global.min - 1.0).abs() <= 1.0e-9);
    assert!((report.global.max - 4.0).abs() <= 1.0e-9);
    assert!((report.global.q1 - 1.5).abs() <= 1.0e-9);
    assert!((report.global.q3 - 3.5).abs() <= 1.0e-9);
    assert!((report.global.quartile - 2.0).abs() <= 1.0e-9);

    assert_eq!(report.by_field_id.len(), 2);
    assert_eq!(report.by_spectral_window_id.len(), 2);
    assert_eq!(report.by_antenna1_id.len(), 2);
    assert_eq!(report.by_observation_id.len(), 1);
    assert_eq!(report.by_field_id[0].key, 0);
    assert!((report.by_field_id[0].stats.sum - 3.0).abs() <= 1.0e-9);
    assert_eq!(report.by_field_id[1].key, 1);
    assert!((report.by_field_id[1].stats.sum - 7.0).abs() <= 1.0e-9);
}

#[test]
fn calibration_stats_can_exclude_flagged_values() {
    let dir = TempDir::new().expect("tempdir");
    let table_path = common::create_apply_gain_caltable(
        &dir.path().join("stats-flags.gcal"),
        &["FIELD0"],
        &[common::SyntheticGainSolutionRow {
            time_seconds: 10.0,
            field_id: 0,
            spectral_window_id: 0,
            antenna_id: 0,
            gains: vec![Complex32::new(1.0, 0.0), Complex32::new(10.0, 0.0)],
            flags: vec![false, true],
        }],
    );

    let report = calibration_stats(
        &table_path,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("CPARAM".to_string()),
            use_flags: true,
        },
    )
    .expect("compute stats");

    assert_eq!(report.global.total_npts, 2);
    assert_eq!(report.global.flagged_npts, 1);
    assert_eq!(report.global.npts, 1);
    assert!((report.global.sum - 1.0).abs() <= 1.0e-9);
}

#[cfg(feature = "slow-tests")]
#[test]
fn calibration_stats_matches_casa_calstat_on_phase_gain_amplitudes() {
    let dir = TempDir::new().expect("tempdir");
    let phase_table = match common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15") {
        Ok(path) => path,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

    let rust = calibration_stats(
        &phase_table,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("CPARAM".to_string()),
            use_flags: false,
        },
    )
    .expect("compute Rust stats");
    let casa = match common::run_casa_calstat(&phase_table, "amp", "CPARAM") {
        Ok(value) => value,
        Err(reason) => {
            eprintln!("{reason}");
            return;
        }
    };

    let casa_stats = casa
        .get("CPARAM")
        .expect("CPARAM key in CASA calstat output");
    assert_casa_stat_close(casa_stats, "npts", rust.global.npts as f64, 1.0e-9);
    assert_casa_stat_close(casa_stats, "sum", rust.global.sum, 1.0e-5);
    assert_casa_stat_close(casa_stats, "sumsq", rust.global.sumsq, 1.0e-4);
    assert_casa_stat_close(casa_stats, "mean", rust.global.mean, 1.0e-6);
    assert_casa_stat_close(casa_stats, "median", rust.global.median, 1.0e-6);
    assert_casa_stat_close(casa_stats, "medabsdevmed", rust.global.medabsdevmed, 1.0e-6);
    assert_casa_stat_close(casa_stats, "quartile", rust.global.quartile, 1.0e-6);
    assert_casa_stat_close(casa_stats, "min", rust.global.min, 1.0e-6);
    assert_casa_stat_close(casa_stats, "max", rust.global.max, 1.0e-6);
    assert_casa_stat_close(casa_stats, "var", rust.global.var, 1.0e-6);
    assert_casa_stat_close(casa_stats, "stddev", rust.global.stddev, 1.0e-6);
    assert_casa_stat_close(casa_stats, "rms", rust.global.rms, 1.0e-6);
}

#[cfg(feature = "slow-tests")]
fn assert_casa_stat_close(stats: &JsonValue, key: &str, rust_value: f64, tolerance: f64) {
    let casa_value = stats
        .get(key)
        .and_then(JsonValue::as_f64)
        .unwrap_or_else(|| panic!("missing CASA stat {key}"));
    let diff = (casa_value - rust_value).abs();
    assert!(
        diff <= tolerance,
        "CASA stat mismatch for {key}: casa={casa_value:.9} rust={rust_value:.9} tolerance={tolerance}"
    );
}
