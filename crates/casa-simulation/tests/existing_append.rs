// SPDX-License-Identifier: LGPL-3.0-or-later
//! Actual CASA input MSs retain old cells while observe appends new rows.
mod predict_support;
use casa_simulation::{ApplySpec, ObserveManySpec, Pointing, Simulator, SimulatorOptions};
use casa_tables::{Table, TableOptions};
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, ScalarValue};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

fn copy(source: &Path, output: &Path) {
    std::fs::create_dir_all(output).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() == "table.lock" {
            continue;
        }
        if entry.path().is_dir() {
            copy(&entry.path(), &output.join(entry.file_name()));
        } else {
            std::fs::copy(entry.path(), output.join(entry.file_name())).unwrap();
        }
    }
}

fn compare(path: &Path, case: &Value) {
    let table = Table::open(TableOptions::new(path)).unwrap();
    let expected = case["after"]["rows"].as_array().unwrap();
    assert_eq!(table.row_count(), expected.len());
    for (row, expected) in expected.iter().enumerate() {
        for (column, key) in [
            ("FIELD_ID", "field"),
            ("TIME", "time"),
            ("INTERVAL", "interval"),
            ("EXPOSURE", "exposure"),
            ("SCAN_NUMBER", "scan_number"),
            ("OBSERVATION_ID", "observation_id"),
            ("STATE_ID", "state_id"),
            ("ARRAY_ID", "array_id"),
            ("DATA_DESC_ID", "data_desc_id"),
            ("FEED1", "feed1"),
            ("FEED2", "feed2"),
        ] {
            let value = table.cell_accessor(row, column).unwrap().scalar().unwrap();
            match value {
                ScalarValue::Int32(value) => assert_eq!(
                    i64::from(*value),
                    expected[key].as_i64().unwrap(),
                    "{column}"
                ),
                ScalarValue::Float64(value) => {
                    assert_eq!(*value, expected[key].as_f64().unwrap(), "{column}")
                }
                _ => panic!("unexpected scalar type"),
            }
        }
        for column in [
            "DATA",
            "MODEL_DATA",
            "CORRECTED_DATA",
            "FLAG",
            "WEIGHT",
            "SIGMA",
            "UVW",
        ] {
            let values = table.cell_accessor(row, column).unwrap().array().unwrap();
            let key = if column == "FLAG" {
                "flags".to_string()
            } else {
                column.to_lowercase()
            };
            let reference = expected[key].as_array().unwrap();
            match values {
                ArrayValue::Complex32(values) => {
                    for (index, reference) in reference.iter().enumerate() {
                        let value = values[[index % 4, index / 4]];
                        assert!(
                            (f64::from(value.re) - reference[0].as_f64().unwrap()).abs() < 1e-6,
                            "{column} row{row}"
                        );
                        assert!(
                            (f64::from(value.im) - reference[1].as_f64().unwrap()).abs() < 1e-6,
                            "{column} row{row}"
                        );
                    }
                }
                ArrayValue::Bool(values) => {
                    for (index, reference) in reference.iter().enumerate() {
                        assert_eq!(values[[index % 4, index / 4]], reference.as_bool().unwrap());
                    }
                }
                ArrayValue::Float32(values) => {
                    for (value, reference) in values.iter().zip(reference) {
                        assert_eq!(
                            f64::from(*value),
                            reference.as_f64().unwrap(),
                            "{column} row{row}"
                        );
                    }
                }
                ArrayValue::Float64(values) => {
                    for (value, reference) in values.iter().zip(reference) {
                        assert!(
                            (value - reference.as_f64().unwrap()).abs() < 1e-3,
                            "{column} row{row}"
                        );
                    }
                }
                _ => panic!("unexpected array type"),
            }
        }
        let ScalarValue::Bool(flag) = table
            .cell_accessor(row, "FLAG_ROW")
            .unwrap()
            .scalar()
            .unwrap()
        else {
            panic!("FLAG_ROW")
        };
        assert_eq!(*flag, expected["flag_row"].as_bool().unwrap());
    }
    for name in ["OBSERVATION", "STATE", "POINTING"] {
        let table = Table::open(TableOptions::new(path.join(name))).unwrap();
        assert_eq!(
            table.row_count(),
            case["after"]["metadata"][name]["rows"].as_u64().unwrap() as usize,
            "{name}"
        );
        compare_metadata(&table, &case["after"]["metadata"][name]["columns"]);
    }
}

fn compare_metadata(table: &Table, columns: &Value) {
    for (name, expected) in columns.as_object().unwrap() {
        for row in 0..table.row_count() {
            let cell = table.cell_accessor(row, name).unwrap();
            if matches!(
                table.schema().unwrap().column(name).unwrap().column_type(),
                casa_tables::ColumnType::Array(_)
            ) {
                let ArrayValue::Float64(values) = cell.array().unwrap() else {
                    panic!("unexpected array {name}")
                };
                use ndarray::Dimension;
                for (index, value) in values.indexed_iter() {
                    let reference = index
                        .slice()
                        .iter()
                        .fold(expected, |reference, index| &reference[*index]);
                    let tolerance = if name == "DIRECTION" || name == "TARGET" {
                        1e-9
                    } else {
                        1e-6
                    };
                    assert!(
                        (*value - reference[row].as_f64().unwrap()).abs() < tolerance,
                        "{name} row {row}: {value} != {}",
                        reference[row]
                    );
                }
            } else {
                let actual = match cell.scalar().unwrap() {
                    ScalarValue::Bool(value) => Value::from(*value),
                    ScalarValue::Int32(value) => Value::from(*value),
                    ScalarValue::Float64(value) => Value::from(*value),
                    ScalarValue::String(value) => Value::from(value.clone()),
                    other => panic!("unexpected {name}: {other:?}"),
                };
                assert_eq!(actual, expected[row], "{name} row {row}");
            }
        }
    }
}

#[test]
fn reopened_observe_and_observemany_match_actual_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_existing_append_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_existing_append");
    for (workers, case) in [1, 3].into_iter().flat_map(|workers| {
        fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .map(move |case| (workers, case))
    }) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("appended.ms");
        copy(&inputs.join("base.ms"), &path);
        let name = case["name"].as_str().unwrap();
        if name == "array_id" {
            let mut table = Table::open(TableOptions::new(&path)).unwrap();
            for row in 0..table.row_count() {
                table
                    .cell_accessor_mut(row, "ARRAY_ID")
                    .unwrap()
                    .set(casa_types::Value::Scalar(ScalarValue::Int32(7)))
                    .unwrap();
            }
            table.save(TableOptions::new(&path)).unwrap();
        }
        let mut sm = Simulator::open_from_ms(
            &path,
            SimulatorOptions {
                measures: casa_test_support::deterministic_measures_provider(),
                resources: predict_support::resources(workers, 2 << 30),
                now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
            },
        )
        .unwrap();
        sm.set_times(
            10.,
            false,
            MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
        )
        .unwrap();
        if name == "predict_then_append" {
            sm.predict(&casa_simulation::PredictSpec {
                images: Vec::new(),
                component_lists: vec![inputs.join("point.cl")],
                incremental: false,
            })
            .unwrap();
        }
        if name == "selected_corrupt_then_append" {
            sm.set_data(&[], &[1], "").unwrap();
            sm.set_apply(&ApplySpec {
                table: inputs.join("gain.cal").display().to_string(),
                kind: String::new(),
                interval_s: 0.,
                interp: "nearest".into(),
                calwt: false,
                spwmap: vec![-1],
                opacity: 0.,
            })
            .unwrap();
            sm.corrupt().unwrap();
        }
        let mut spec = predict_support::scan(
            if name == "missing_source" {
                "missing"
            } else {
                "target"
            },
            40.,
            60.,
        );
        spec.add_observation = matches!(name, "observation" | "many" | "array_id");
        spec.project = "continued".into();
        spec.observer = "CASA simulator".into();
        let result = if name == "many" {
            let mut many = ObserveManySpec::from(&spec);
            many.state.obs_mode = "OBSERVE_TARGET#ON_SOURCE".into();
            many.pointings.push(Pointing {
                source: "offset".into(),
                start_s: 60.,
                stop_s: 80.,
                direction: None,
            });
            sm.observe_many(&many)
        } else {
            sm.observe(&spec)
        };
        assert_eq!(
            result.is_ok(),
            case["returned"].as_bool().unwrap(),
            "{name}"
        );
        if name.starts_with("append_then_selected") {
            sm.set_data(&[], &[0], "").unwrap();
            sm.predict(&casa_simulation::PredictSpec {
                images: Vec::new(),
                component_lists: vec![inputs.join("point.cl")],
                incremental: name.ends_with("increment"),
            })
            .unwrap();
            if name.ends_with("corrupt") {
                sm.set_apply(&ApplySpec {
                    table: inputs.join("gain.cal").display().to_string(),
                    kind: String::new(),
                    interval_s: 0.,
                    interp: "nearest".into(),
                    calwt: false,
                    spwmap: vec![-1],
                    opacity: 0.,
                })
                .unwrap();
                sm.corrupt().unwrap();
            }
        }
        sm.close().unwrap();
        compare(&path, case);
        if let Some(output) = std::env::var_os("CASA_RS_SIMULATION_APPEND_EXPORT") {
            copy(
                &path,
                &Path::new(&output).join(format!("w{workers}-{name}.ms")),
            );
        }
        println!("append {name}, W{workers}: DATA, quality, scratch and metadata match CASA");
    }
}

#[test]
fn append_noise_uses_physical_rows_and_close_failure_preserves_the_input() {
    use casa_simulation::NoiseSpec;
    use predict_support::{bits, data, read_main, scan, session};
    let directory = tempfile::tempdir().unwrap();
    let mut reference = None;
    for (name, reopened, workers) in [
        ("one", false, 1),
        ("reopened", true, 1),
        ("workers", true, 3),
    ] {
        let path = directory.path().join(format!("{name}.ms"));
        let resources = predict_support::resources(workers, 2 << 30);
        let mut sm = session(&path, "ALMA", "XX XY YX YY", resources);
        sm.observe(&scan("target", 0., 40.)).unwrap();
        if reopened {
            sm.close().unwrap();
            sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources,
                    now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            sm.set_times(
                10.,
                false,
                MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
            )
            .unwrap();
        }
        sm.observe(&scan("offset", 40., 60.)).unwrap();
        sm.set_noise(&NoiseSpec::simple(0.1)).unwrap();
        sm.corrupt().unwrap();
        sm.close().unwrap();
        let actual = bits(&data(&read_main(&path).0));
        if let Some(reference) = &reference {
            assert_eq!(
                &actual, reference,
                "{name}: appended noise restarts its physical row counter"
            );
        } else {
            reference = Some(actual);
        }
    }
    let path = directory.path().join("reopened.ms");
    let before = casa_test_support::ms_digest::measurement_set_manifest(&path);
    let mut sm = Simulator::open_from_ms(
        &path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: predict_support::resources(
                1,
                casa_imaging_runtime::process_cache_bytes() + (1 << 20),
            ),
            now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap();
    sm.observe(&scan("target", 60., 80.)).unwrap();
    assert!(sm.close().unwrap_err().admission_refused().is_some());
    assert_eq!(
        before,
        casa_test_support::ms_digest::measurement_set_manifest(&path)
    );
}
