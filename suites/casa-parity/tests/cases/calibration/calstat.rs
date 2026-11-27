// SPDX-License-Identifier: LGPL-3.0-or-later
use casa_calibration::table::{CalibrationStatsAxis, CalibrationStatsRequest, calibration_stats};
use serde_json::Value as JsonValue;
use tempfile::TempDir;

use super::common;

#[test]
fn calibration_stats_matches_casa_calstat_on_phase_gain_amplitudes() {
    let dir = TempDir::new().expect("tempdir");
    let phase_table = common::generate_casa_phase_gain(dir.path(), "0", "0", "VA15")
        .unwrap_or_else(|reason| panic!("{reason}"));

    let rust = calibration_stats(
        &phase_table,
        &CalibrationStatsRequest {
            axis: CalibrationStatsAxis::Amplitude,
            datacolumn: Some("CPARAM".to_string()),
            use_flags: false,
        },
    )
    .expect("compute Rust stats");
    let casa = common::run_casa_calstat(&phase_table, "amp", "CPARAM")
        .unwrap_or_else(|reason| panic!("{reason}"));

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
