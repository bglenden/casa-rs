// SPDX-License-Identifier: LGPL-3.0-or-later
//! Replays CASA's captured gains and selector results without a CASA install.

use crate::common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_calibration::fluxscale::{FluxScaleOptions, FluxScaleRequest, fluxscale};
use casa_core::{Complex32, ScalarValue, Value};
use casa_tables::{Table, TableOptions};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    rows: Vec<GainRow>,
}
#[derive(Deserialize)]
struct GainRow {
    time: f64,
    field_id: i32,
    spw_id: i32,
    antenna_id: i32,
    antenna2_id: i32,
    gains: Vec<[f32; 2]>,
    flags: Vec<bool>,
}
#[derive(Deserialize)]
struct Cases {
    cases: Vec<Case>,
}
#[derive(Deserialize)]
struct Case {
    name: String,
    arguments: serde_json::Value,
    flux_density_jy: f64,
    flux_density_error_jy: f64,
    num_sol: f64,
}

fn fixture(path: &Path) -> PathBuf {
    let input: Fixture =
        serde_json::from_str(include_str!("fixtures/fluxscale-original.json")).unwrap();
    let gains = input
        .rows
        .iter()
        .map(|row| common::synthetic_tables::SyntheticGainSolutionRow {
            time_seconds: row.time,
            field_id: row.field_id,
            spectral_window_id: row.spw_id,
            antenna_id: row.antenna_id,
            gains: row
                .gains
                .iter()
                .map(|[re, im]| Complex32::new(*re, *im))
                .collect(),
            flags: row.flags.clone(),
        })
        .collect::<Vec<_>>();
    let gain =
        common::synthetic_tables::create_apply_gain_caltable(path, &["FLUX", "PHASE"], &gains);
    let mut times = BTreeMap::<i32, Vec<f64>>::new();
    for row in &input.rows {
        let times = times.entry(row.field_id).or_default();
        if !times.contains(&row.time) {
            times.push(row.time);
        }
    }
    let mut table = Table::open(TableOptions::new(&gain)).unwrap();
    for (index, row) in input.rows.iter().enumerate() {
        let scan = 1 + times[&row.field_id]
            .iter()
            .position(|time| *time == row.time)
            .unwrap() as i32
            % 3;
        for (column, value) in [("ANTENNA2", row.antenna2_id), ("SCAN_NUMBER", scan)] {
            table
                .cell_accessor_mut(index, column)
                .unwrap()
                .set(Value::Scalar(ScalarValue::Int32(value)))
                .unwrap();
        }
        table
            .cell_accessor_mut(index, "INTERVAL")
            .unwrap()
            .set(Value::Scalar(ScalarValue::Float64(0.)))
            .unwrap();
    }
    table.save(TableOptions::new(&gain)).unwrap();
    let ant = gain.join("ANTENNA");
    let mut table = Table::open(TableOptions::new(&ant)).unwrap();
    for index in 0..table.row_count() {
        table
            .cell_accessor_mut(index, "NAME")
            .unwrap()
            .set(Value::Scalar(ScalarValue::String(format!("A{index}"))))
            .unwrap();
    }
    table.save(TableOptions::new(ant)).unwrap();
    gain
}

fn request(gain: &Path, output: &Path) -> FluxScaleRequest {
    FluxScaleRequest {
        options: FluxScaleOptions::default(),
        input_table: gain.into(),
        output_table: output.into(),
        reference_fields: vec!["FLUX".into()],
        transfer_fields: vec!["PHASE".into()],
        refspwmap: Vec::new(),
        gainthreshold: None,
        incremental: false,
        append: false,
    }
}

#[test]
fn fluxscale_casa_selections_match_flux_error_count_and_keep_unselected_output_rows() {
    let dir = tempfile::tempdir().unwrap();
    let gain = fixture(&dir.path().join("input.gcal"));
    let cases: Cases =
        serde_json::from_str(include_str!("fixtures/fluxscale-selections.json")).unwrap();
    for case in cases.cases {
        let mut request = request(&gain, &dir.path().join(&case.name));
        for (name, value) in case.arguments.as_object().unwrap() {
            match name.as_str() {
                "antenna" => request.options.antenna = value.as_str().unwrap().into(),
                "timerange" => request.options.timerange = value.as_str().unwrap().into(),
                "scan" => request.options.scan = value.as_str().unwrap().into(),
                "gainthreshold" => request.gainthreshold = value.as_f64(),
                "reference" => {
                    request.reference_fields = serde_json::from_value(value.clone()).unwrap()
                }
                _ => panic!("unknown fixture argument {name}"),
            }
        }
        let report = fluxscale(&request).unwrap_or_else(|error| panic!("{}: {error}", case.name));
        let result = &report.fields[&1].spw_results[&0];
        assert!(
            (result.fluxd[0] - case.flux_density_jy).abs() < 1e-6,
            "{}: {:?} != {}",
            case.name,
            result,
            case.flux_density_jy
        );
        assert!(
            (result.fluxd_err[0] - case.flux_density_error_jy).abs() < 1e-6,
            "{}",
            case.name
        );
        assert_eq!(result.num_sol[0], case.num_sol, "{}", case.name);
        assert_eq!(
            Table::open(TableOptions::new(request.output_table))
                .unwrap()
                .row_count(),
            60
        );
    }
}

#[test]
fn fluxscale_incremental_emits_one_correction_per_antenna_field_window_at_casa_reference_time() {
    let dir = tempfile::tempdir().unwrap();
    let gain = fixture(&dir.path().join("input.gcal"));
    let mut request = request(&gain, &dir.path().join("incremental.gcal"));
    request.incremental = true;
    fluxscale(&request).unwrap();
    let table = Table::open(TableOptions::new(request.output_table)).unwrap();
    assert_eq!(table.row_count(), 10);
    for row in 0..table.row_count() {
        assert_eq!(
            table.cell_accessor(row, "TIME").unwrap().value().unwrap(),
            Some(&Value::Scalar(ScalarValue::Float64(5_084_545_995.296_027)))
        );
        assert_eq!(
            table
                .cell_accessor(row, "ANTENNA2")
                .unwrap()
                .value()
                .unwrap(),
            Some(&Value::Scalar(ScalarValue::Int32(-1)))
        );
        assert_eq!(
            table
                .cell_accessor(row, "SCAN_NUMBER")
                .unwrap()
                .value()
                .unwrap(),
            Some(&Value::Scalar(ScalarValue::Int32(-1)))
        );
    }
}

#[test]
fn partial_antenna_samples_do_not_move_the_casa_incremental_reference_time() {
    let dir = tempfile::tempdir().unwrap();
    let gain = fixture(&dir.path().join("sparse.gcal"));
    let mut table = Table::open(TableOptions::new(&gain)).unwrap();
    let mut removed = Vec::new();
    for index in 0..table.row_count() {
        let integer = |name| match table
            .cell_accessor(index, name)
            .and_then(|cell| cell.value())
            .unwrap()
        {
            Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
            other => panic!("unexpected {name}: {other:?}"),
        };
        let field = integer("FIELD_ID");
        let antenna = integer("ANTENNA1");
        let Some(Value::Scalar(ScalarValue::Float64(time))) = table
            .cell_accessor(index, "TIME")
            .and_then(|cell| cell.value())
            .unwrap()
        else {
            panic!("TIME type")
        };
        let time = *time;
        if field == 1 && time == 5_084_545_995.296_027 && antenna != 0 {
            removed.push(index);
        }
        table
            .cell_accessor_mut(index, "ANTENNA2")
            .unwrap()
            .set(Value::Scalar(ScalarValue::Int32(-1)))
            .unwrap();
        table
            .cell_accessor_mut(index, "FLAG")
            .unwrap()
            .set(Value::Array(casa_core::ArrayValue::from_bool_vec(
                vec![false; 2],
            )))
            .unwrap();
    }
    table.remove_rows(&removed).unwrap();
    table.save(TableOptions::new(&gain)).unwrap();
    let mut request = request(&gain, &dir.path().join("sparse.fluxcal"));
    request.incremental = true;
    request.options.antenna = "!0".into();
    request.options.scan = "2".into();
    let report = fluxscale(&request).unwrap();
    // Captured independently by partial-sparse-time in fluxscale_oracle.py.
    assert!((report.fields[&1].spw_results[&0].fluxd[0] - 0.961_900_323_327_529_4).abs() < 1e-6);
    let output = Table::open(TableOptions::new(request.output_table)).unwrap();
    for index in 0..output.row_count() {
        assert_eq!(
            output
                .cell_accessor(index, "TIME")
                .unwrap()
                .value()
                .unwrap(),
            Some(&Value::Scalar(ScalarValue::Float64(5_084_545_995.296_027)))
        );
    }
}

#[test]
fn unmatched_transfer_field_does_not_discard_another_fields_valid_flux() {
    let dir = tempfile::tempdir().unwrap();
    let rows = [(0, 0, 2.), (1, 0, 4.), (2, 1, 3.)].map(|(field, antenna, amplitude)| {
        common::synthetic_tables::SyntheticGainSolutionRow {
            time_seconds: 10. * field as f64,
            field_id: field,
            spectral_window_id: 0,
            antenna_id: antenna,
            gains: vec![Complex32::new(amplitude, 0.); 2],
            flags: vec![false; 2],
        }
    });
    let gain = common::synthetic_tables::create_apply_gain_caltable(
        &dir.path().join("disjoint.gcal"),
        &["FLUX", "PHASE", "OTHER"],
        &rows,
    );
    let mut request = request(&gain, &dir.path().join("disjoint.fluxcal"));
    request.transfer_fields = vec!["PHASE".into(), "OTHER".into()];
    let report = fluxscale(&request).unwrap();
    assert_eq!(report.fields.len(), 1);
    assert_eq!(report.fields[&1].spw_results[&0].fluxd[0], 4.);
    assert_eq!(report.fields[&1].spw_results[&0].num_sol[0], 2.);
    let output = Table::open(TableOptions::new(request.output_table)).unwrap();
    assert_eq!(output.row_count(), 3);
    let untouched = output.cell_accessor(2, "CPARAM").unwrap().array().unwrap();
    assert_eq!(
        untouched,
        &casa_core::ArrayValue::from_complex32_vec(vec![Complex32::new(3., 0.); 2])
    );
}

#[test]
fn listing_cannot_truncate_an_input_scientific_table_file() {
    let dir = tempfile::tempdir().unwrap();
    let gain = fixture(&dir.path().join("protected.gcal"));
    let path = gain.join("table.dat");
    let before = std::fs::read(&path).unwrap();
    let mut request = request(&gain, &dir.path().join("protected.fluxcal"));
    request.options.listfile = Some(path.clone());
    assert!(fluxscale(&request).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(!request.output_table.exists());
}
