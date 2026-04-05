// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_calibration::{FluxScaleRequest, fluxscale};
use casacore_tables::{Table, TableOptions};
use casacore_types::{ArrayValue, Complex32, ScalarValue, Value};
use tempfile::TempDir;

#[test]
fn fluxscale_scales_transfer_gains_and_reports_flux_density() {
    let dir = TempDir::new().expect("tempdir");
    let input_table = common::create_apply_gain_caltable(
        &dir.path().join("input.gcal"),
        &["REF0", "TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 10.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 20.0,
                field_id: 1,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![Complex32::new(2.0, 0.0), Complex32::new(2.0, 0.0)],
                flags: vec![false, false],
            },
        ],
    );
    let output_table = dir.path().join("scaled.gcal");

    let report = fluxscale(&FluxScaleRequest {
        input_table: input_table.clone(),
        output_table: output_table.clone(),
        reference_fields: vec!["REF*".to_string()],
        transfer_fields: Vec::new(),
        refspwmap: Vec::new(),
        gainthreshold: None,
        incremental: false,
    })
    .expect("run fluxscale");

    let field = report.fields.get(&1).expect("transfer field");
    assert_eq!(field.field_name, "TARGET0");
    assert!((field.fit_fluxd - 4.0).abs() <= 1.0e-9);
    assert!((field.fit_ref_frequency_hz - 1.0005e9).abs() <= 1.0);
    assert_eq!(field.spw_results.get(&0).expect("spw 0").num_sol[0], 2.0);

    let transfer_rows = read_field_cparams(&output_table, 1);
    assert_eq!(transfer_rows.len(), 1);
    for gain in &transfer_rows[0] {
        assert!(
            (gain.re - 1.0).abs() <= 1.0e-6,
            "unexpected real part: {}",
            gain.re
        );
        assert!(
            gain.im.abs() <= 1.0e-6,
            "unexpected imaginary part: {}",
            gain.im
        );
    }
}

#[test]
fn fluxscale_incremental_writes_correction_factors_and_honors_gainthreshold() {
    let dir = TempDir::new().expect("tempdir");
    let input_table = common::create_apply_gain_caltable(
        &dir.path().join("input-threshold.gcal"),
        &["REF0", "TARGET0"],
        &[
            common::SyntheticGainSolutionRow {
                time_seconds: 10.0,
                field_id: 0,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![Complex32::new(1.0, 0.0), Complex32::new(1.0, 0.0)],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 20.0,
                field_id: 1,
                spectral_window_id: 0,
                antenna_id: 0,
                gains: vec![Complex32::new(2.0, 0.0), Complex32::new(2.0, 0.0)],
                flags: vec![false, false],
            },
            common::SyntheticGainSolutionRow {
                time_seconds: 30.0,
                field_id: 1,
                spectral_window_id: 0,
                antenna_id: 1,
                gains: vec![Complex32::new(2.0, 0.0), Complex32::new(10.0, 0.0)],
                flags: vec![false, false],
            },
        ],
    );
    let output_table = dir.path().join("incremental.gcal");

    let report = fluxscale(&FluxScaleRequest {
        input_table: input_table.clone(),
        output_table: output_table.clone(),
        reference_fields: vec!["REF0".to_string()],
        transfer_fields: vec!["TARGET*".to_string()],
        refspwmap: Vec::new(),
        gainthreshold: Some(0.2),
        incremental: true,
    })
    .expect("run incremental fluxscale");

    let field = report.fields.get(&1).expect("transfer field");
    assert!((field.fit_fluxd - 4.0).abs() <= 1.0e-9);

    let transfer_rows = read_field_cparams(&output_table, 1);
    assert_eq!(transfer_rows.len(), 2);
    for gains in transfer_rows {
        for gain in gains {
            assert!(
                (gain.re - 0.5).abs() <= 1.0e-6,
                "unexpected real part: {}",
                gain.re
            );
            assert!(
                gain.im.abs() <= 1.0e-6,
                "unexpected imaginary part: {}",
                gain.im
            );
        }
    }
}

fn read_field_cparams(table_path: &std::path::Path, field_id: i32) -> Vec<Vec<Complex32>> {
    let table = Table::open(TableOptions::new(table_path)).expect("open table");
    let mut rows = Vec::new();
    for row in 0..table.row_count() {
        let row_field_id = match table.cell(row, "FIELD_ID").expect("FIELD_ID cell") {
            Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
            other => panic!("unexpected FIELD_ID value: {other:?}"),
        };
        if row_field_id != field_id {
            continue;
        }
        let gains = match table.get_array_cell(row, "CPARAM").expect("CPARAM cell") {
            ArrayValue::Complex32(values) => values.iter().copied().collect::<Vec<_>>(),
            other => panic!("unexpected CPARAM value: {other:?}"),
        };
        rows.push(gains);
    }
    rows
}
