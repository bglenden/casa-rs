// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_calibration::{FluxScaleRequest, fluxscale};
use casa_core::{ArrayValue, Complex32, ScalarValue, Value};
use casa_tables::{Table, TableOptions};
use tempfile::TempDir;

#[test]
fn fluxscale_scales_transfer_gains_and_reports_flux_density() {
    check_scaled_product(None, false);
}

#[cfg(unix)]
#[test]
fn fluxscale_copies_readonly_standard_tables_in_both_byte_orders() {
    for endian in [
        casa_tables::EndianFormat::BigEndian,
        casa_tables::EndianFormat::LittleEndian,
    ] {
        check_scaled_product(Some(endian), false);
    }
}

#[test]
fn fluxscale_retains_the_incremental_storage_gain_reader_fallback() {
    check_scaled_product(None, true);
}

fn use_incremental_gain_storage(input_table: &std::path::Path) {
    use casa_core::PrimitiveType;
    use casa_tables::{ColumnBinding, ColumnOptions, ColumnSchema, DataManagerKind, TableSchema};
    let mut table = Table::open(TableOptions::new(input_table)).unwrap();
    let mut columns = table.schema().unwrap().columns().to_vec();
    for column in &mut columns {
        let primitive = match column.name() {
            "CPARAM" => PrimitiveType::Complex32,
            "FLAG" => PrimitiveType::Bool,
            _ => continue,
        };
        *column = ColumnSchema::array_fixed(column.name(), primitive, vec![2, 1])
            .with_options(ColumnOptions {
                direct: true,
                undefined: false,
            })
            .unwrap();
    }
    let mut variable = table.schema().unwrap().columns().to_vec();
    for column in &mut variable {
        if matches!(column.name(), "CPARAM" | "FLAG") {
            *column =
                ColumnSchema::array_variable(column.name(), column.data_type().unwrap(), None);
        }
    }
    table
        .set_schema(TableSchema::new(variable).unwrap())
        .unwrap();
    for row in 0..table.row_count() {
        let gains = match table
            .column_accessor("CPARAM")
            .unwrap()
            .get(row)
            .unwrap()
            .unwrap()
        {
            Value::Array(ArrayValue::Complex32(values)) => values.iter().copied().collect(),
            _ => unreachable!(),
        };
        let flags = match table
            .column_accessor("FLAG")
            .unwrap()
            .get(row)
            .unwrap()
            .unwrap()
        {
            Value::Array(ArrayValue::Bool(values)) => values.iter().copied().collect(),
            _ => unreachable!(),
        };
        table
            .column_accessor_mut("CPARAM")
            .unwrap()
            .set(
                row,
                Value::Array(ArrayValue::Complex32(
                    ndarray::ArrayD::from_shape_vec(vec![2, 1], gains).unwrap(),
                )),
            )
            .unwrap();
        table
            .column_accessor_mut("FLAG")
            .unwrap()
            .set(
                row,
                Value::Array(ArrayValue::Bool(
                    ndarray::ArrayD::from_shape_vec(vec![2, 1], flags).unwrap(),
                )),
            )
            .unwrap();
    }
    table
        .set_schema(TableSchema::new(columns).unwrap())
        .unwrap();
    let bindings = ["CPARAM", "FLAG"]
        .into_iter()
        .map(|name| {
            (
                name.to_string(),
                ColumnBinding {
                    data_manager: DataManagerKind::IncrementalStMan,
                    tile_shape: None,
                },
            )
        })
        .collect();
    table
        .save_with_bindings(TableOptions::new(input_table), &bindings)
        .unwrap();
}

fn check_scaled_product(endian: Option<casa_tables::EndianFormat>, ism: bool) {
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
    if ism {
        use_incremental_gain_storage(&input_table);
    }
    if let Some(endian) = endian {
        Table::open(TableOptions::new(&input_table))
            .unwrap()
            .save(
                TableOptions::new(&input_table)
                    .with_data_manager(casa_tables::DataManagerKind::StandardStMan)
                    .with_endian_format(endian),
            )
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for file in ["table.f0", "table.f0i"] {
                std::fs::set_permissions(
                    input_table.join(file),
                    std::fs::Permissions::from_mode(0o444),
                )
                .unwrap();
            }
        }
    }
    let main_files = std::fs::read_dir(&input_table)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("table.f"))
        .map(|entry| (entry.file_name(), std::fs::read(entry.path()).unwrap()))
        .collect::<Vec<_>>();
    let output_table = dir.path().join("scaled.gcal");

    let report = fluxscale(&FluxScaleRequest {
        options: Default::default(),
        append: false,
        input_table: input_table.clone(),
        output_table: output_table.clone(),
        reference_fields: vec!["REF*".to_string()],
        transfer_fields: Vec::new(),
        refspwmap: Vec::new(),
        gainthreshold: None,
        incremental: false,
    })
    .expect("run fluxscale");
    for (name, expected) in main_files {
        assert_eq!(
            std::fs::read(input_table.join(&name)).unwrap(),
            expected,
            "source {name:?} changed"
        );
    }

    let field = report.fields.get(&1).expect("transfer field");
    assert_eq!(field.field_name, "TARGET0");
    assert!((field.fit_fluxd - 4.0).abs() <= 1.0e-9);
    // The window's middle channel, CASA's solFreq.
    assert!((field.fit_ref_frequency_hz - 1.001e9).abs() <= 1.0);
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
        options: Default::default(),
        append: false,
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
        let row_field_id = match table
            .cell_accessor(row, "FIELD_ID")
            .and_then(|cell| cell.value())
            .expect("FIELD_ID cell")
        {
            Some(Value::Scalar(ScalarValue::Int32(value))) => *value,
            other => panic!("unexpected FIELD_ID value: {other:?}"),
        };
        if row_field_id != field_id {
            continue;
        }
        let gains = match table
            .cell_accessor(row, "CPARAM")
            .and_then(|cell| cell.array())
            .expect("CPARAM cell")
        {
            ArrayValue::Complex32(values) => values.iter().copied().collect::<Vec<_>>(),
            other => panic!("unexpected CPARAM value: {other:?}"),
        };
        rows.push(gains);
    }
    rows
}
