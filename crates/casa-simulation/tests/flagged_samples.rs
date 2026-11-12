// SPDX-License-Identifier: LGPL-3.0-or-later
//! Existing-MS sample flags against independent CASA 6.7.6.14 (#1053).

mod pass_summary;
mod predict_support;

use std::io::Read;
use std::path::Path;

use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_ms::{
    MeasurementSet, MeasurementSetColumnStorage, MeasurementSetColumnWriteMode,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession,
};
use casa_simulation::{
    BackendRequest, NoiseSpec, PredictSpec, PredictionOptions, SessionResources, Simulator,
    SimulatorOptions,
};
use casa_tables::ColumnSchema;
use casa_types::measures::{EpochRef, MEpoch};
use casa_types::{ArrayValue, Complex32, PrimitiveType, ScalarValue};
use flate2::read::GzDecoder;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use predict_support::{CENTRE_RAD, N, REFERENCE_MJD, Row, read_main, scan, session, write_model};
use serde_json::Value;

fn fixture() -> Value {
    let mut text = String::new();
    GzDecoder::new(&include_bytes!("data/sm_flagged_samples_casa.json.gz")[..])
        .read_to_string(&mut text)
        .unwrap();
    let fixture: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 17);
    fixture
}

fn array(values: Vec<Complex32>) -> ArrayValue {
    ArrayValue::Complex32(ArrayD::from_shape_vec(IxDyn(&[2, 4]).f(), values).unwrap())
}

fn flags(row: usize) -> Vec<bool> {
    let mut flags = vec![false; 8];
    match row % 8 {
        2 => {
            flags[0] = true;
            flags[5] = true;
        }
        3 => flags.fill(true),
        5 => flags[2] = true,
        _ => {}
    }
    flags
}

fn column(name: &str, mode: MeasurementSetColumnWriteMode) -> MeasurementSetWriteColumnPlan {
    MeasurementSetWriteColumnPlan {
        name: name.to_string(),
        bytes_per_row: 512,
        mode,
        storage_manager: if mode == MeasurementSetColumnWriteMode::Create {
            MeasurementSetColumnStorage::TiledShape
        } else {
            MeasurementSetColumnStorage::Persisted
        },
        tile_shape: None,
        create_source_column: (mode == MeasurementSetColumnWriteMode::Create)
            .then(|| "DATA".to_string()),
    }
}

fn plan(rows: Vec<usize>, columns: Vec<MeasurementSetWriteColumnPlan>) -> MeasurementSetWritePlan {
    MeasurementSetWritePlan::selected_row_mutation(
        rows,
        columns,
        MeasurementSetWriteResources {
            available_bytes: 1 << 20,
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: 0,
        },
    )
    .unwrap()
}

/// Independent input recipe, including contradictory row/sample flags and
/// sentinel scratch columns. All writes use the bounded MS mutation path.
fn prepare(path: &Path) {
    let mut ms = MeasurementSet::open(path).unwrap();
    let rows = ms.row_count();
    for name in ["MODEL_DATA", "CORRECTED_DATA"] {
        ms.main_table_mut()
            .add_column(
                ColumnSchema::array_variable(name, PrimitiveType::Complex32, Some(2)),
                None,
            )
            .unwrap();
    }
    let create = plan(
        vec![0],
        ["MODEL_DATA", "CORRECTED_DATA"]
            .map(|name| column(name, MeasurementSetColumnWriteMode::Create))
            .to_vec(),
    );
    drop(MeasurementSetWriteSession::start_selected_row_mutation(&mut ms, create).unwrap());
    let names = [
        "DATA",
        "MODEL_DATA",
        "CORRECTED_DATA",
        "FLAG",
        "FLAG_ROW",
        "WEIGHT",
        "SIGMA",
    ];
    let plan = plan(
        (0..rows).collect(),
        names
            .map(|name| column(name, MeasurementSetColumnWriteMode::Replace))
            .to_vec(),
    );
    let mut writer =
        MeasurementSetWriteSession::start_selected_row_mutation(&mut ms, plan).unwrap();
    let row_indices: Vec<usize> = (0..rows).collect();
    let columns = names
        .into_iter()
        .map(|name| {
            let values = if name == "FLAG_ROW" {
                MeasurementSetMutationColumnValues::Scalars(
                    (0..rows)
                        .map(|row| ScalarValue::Bool(matches!(row % 8, 4 | 5)))
                        .collect(),
                )
            } else {
                MeasurementSetMutationColumnValues::Arrays(
                    (0..rows)
                        .map(|row| match name {
                            "FLAG" => ArrayValue::Bool(
                                ArrayD::from_shape_vec(IxDyn(&[2, 4]).f(), flags(row)).unwrap(),
                            ),
                            "WEIGHT" | "SIGMA" => ArrayValue::Float32(
                                ArrayD::from_shape_vec(
                                    IxDyn(&[2]),
                                    if name == "WEIGHT" {
                                        vec![2.0, 3.0]
                                    } else {
                                        vec![0.5, 0.75]
                                    },
                                )
                                .unwrap(),
                            ),
                            _ => array(vec![
                                match name {
                                    "DATA" => Complex32::new(3.0, 4.0),
                                    "MODEL_DATA" => Complex32::new(7.0, -2.0),
                                    _ => Complex32::new(-5.0, 6.0),
                                };
                                8
                            ]),
                        })
                        .collect(),
                )
            };
            MeasurementSetMutationColumnBatch {
                name: name.to_string(),
                values,
            }
        })
        .collect();
    writer
        .write_mutation_batch(
            &mut ms,
            MeasurementSetMutationBatch {
                row_indices,
                columns,
            },
        )
        .unwrap();
    writer.finish_mutation().unwrap();
}

fn options(workers: usize, backend: BackendRequest) -> SimulatorOptions {
    SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources: SessionResources::pinned(
            HostResources {
                threads: 8,
                performance_cores: 8,
                available_memory: 8 << 30,
                metal: backend == BackendRequest::Metal,
            },
            ResourcePolicy::Explicit {
                workers,
                memory: 2 << 30,
            },
            backend,
        )
        .unwrap(),
        now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    }
}

fn run_case(case: &Value, workers: usize, backend: BackendRequest) -> Vec<Row> {
    let directory = tempfile::tempdir().unwrap();
    let a = write_model(
        directory.path(),
        "a.image",
        &[vec![(N / 2, N / 2, 1.25), (N / 2 + 9, N / 2 - 4, 0.75)]],
    );
    let b = predict_support::write_model_of(
        directory.path(),
        "b.image",
        96,
        &(0..4)
            .map(|c| vec![(48, 48, 0.5 + 0.25 * c as f32), (60, 49, -0.1)])
            .collect::<Vec<_>>(),
    );
    let component = directory.path().join("point.cl");
    predict_support::write_point_list(&component, CENTRE_RAD, 0.25);
    let path = directory.path().join("flagged.ms");
    let mut sm = session(&path, "ALMA", "XX YY", options(workers, backend).resources);
    sm.observe(&scan("target", 0.0, 20.0)).unwrap();
    sm.observe(&scan("offset", 20.0, 40.0)).unwrap();
    sm.close().unwrap();
    prepare(&path);
    let mut sm = Simulator::open_from_ms(&path, options(workers, backend)).unwrap();
    let machine = case["machine"].as_str().unwrap_or("ft");
    sm.set_options(&PredictionOptions {
        ftmachine: machine.to_string(),
        w_planes: 16,
        grid_function: if machine == "sd" { "BOX" } else { "SF" }.to_string(),
        ..PredictionOptions::default()
    })
    .unwrap();
    let kind = case["kind"].as_str().unwrap_or("image");
    if kind != "noise" {
        sm.predict(&PredictSpec {
            images: match kind {
                "components" => vec![],
                "multiple" => vec![a, b],
                _ => vec![a],
            },
            component_lists: if matches!(kind, "components" | "mixed") {
                vec![component]
            } else {
                vec![]
            },
            incremental: case["incremental"].as_bool().unwrap_or(false),
        })
        .unwrap();
    }
    if kind == "noise" || case["noise"].as_bool().unwrap_or(false) {
        sm.set_seed(617);
        sm.set_noise(&NoiseSpec::simple(0.1)).unwrap();
        sm.corrupt().unwrap();
    }
    let ((), summary) = pass_summary::capture(|| sm.close().unwrap());
    if backend == BackendRequest::Metal
        && machine != "sd"
        && kind != "components"
        && kind != "noise"
    {
        assert_eq!(summary.unwrap().backend, "Some(Metal)");
    }
    check_columns(&path, case);
    read_main(&path).0
}

fn complex(value: &Value) -> Complex32 {
    Complex32::new(
        value[0].as_f64().unwrap() as f32,
        value[1].as_f64().unwrap() as f32,
    )
}

fn check_columns(path: &Path, case: &Value) {
    let ms = MeasurementSet::open(path).unwrap();
    let table = ms.main_table();
    let cell = |row, name| table.cell_accessor(row, name).unwrap().array().unwrap();
    let noise_only = case["kind"] == "noise";
    for (row, expected) in case["rows"].as_array().unwrap().iter().enumerate() {
        assert_eq!(
            table
                .cell_accessor(row, "FLAG_ROW")
                .unwrap()
                .scalar()
                .unwrap(),
            &ScalarValue::Bool(matches!(row % 8, 4 | 5))
        );
        let flag = cell(row, "FLAG");
        let ArrayValue::Bool(flag) = flag else {
            panic!("FLAG type")
        };
        let actual = (0..4)
            .flat_map(|c| (0..2).map(move |p| flag[[p, c]]))
            .collect::<Vec<_>>();
        assert_eq!(actual, flags(row));
        assert_eq!(
            actual,
            expected["flags"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_bool().unwrap())
                .collect::<Vec<_>>()
        );
        for (column, values) in [("WEIGHT", vec![2.0, 3.0]), ("SIGMA", vec![0.5, 0.75])] {
            let ArrayValue::Float32(actual) = cell(row, column) else {
                panic!("{column} type")
            };
            assert_eq!(actual.iter().copied().collect::<Vec<_>>(), values);
        }
        let data = cell(row, "DATA");
        assert_eq!(cell(row, "CORRECTED_DATA"), data);
        let model = cell(row, "MODEL_DATA");
        assert_eq!(
            model,
            &array(vec![
                if noise_only {
                    Complex32::new(7.0, -2.0)
                } else {
                    Complex32::new(1.0, 0.0)
                };
                8
            ])
        );
    }
}

fn expected(case: &Value, cases: &[Value], row: usize, sample: usize) -> Complex32 {
    let find = |name: &str| cases.iter().find(|c| c["name"] == name).unwrap();
    let incremental = case["incremental"].as_bool().unwrap_or(false);
    if case["kind"] == "mixed" {
        // CASA doubles the retained component at flagged samples. Compare
        // the intended sum to independent image-only/component-only runs.
        return complex(&find("ft_replace")["rows"][row]["data"][sample])
            + complex(&find("components_replace")["rows"][row]["data"][sample])
            + if incremental {
                Complex32::new(3.0, 4.0)
            } else {
                Complex32::default()
            };
    }
    let reference = &case["rows"][row];
    if case["machine"] == "sd" && reference["antennas"][0] != reference["antennas"][1] {
        // Standing SDGrid correction: no cross-correlation total power.
        return if incremental {
            Complex32::new(3.0, 4.0)
        } else {
            Complex32::default()
        };
    }
    complex(&reference["data"][sample])
}

fn assert_casa(case: &Value, cases: &[Value], actual: &[Row]) {
    let reference = case["rows"].as_array().unwrap();
    assert_eq!(actual.len(), reference.len());
    let (mut error, mut power) = (0.0, 0.0);
    for (index, (row, reference)) in actual.iter().zip(reference).enumerate() {
        assert_eq!(row.field as u64, reference["field"].as_u64().unwrap());
        assert_eq!(row.flag_row, reference["flag_row"].as_bool().unwrap());
        for (antenna, expected) in row
            .antennas
            .iter()
            .zip(reference["antennas"].as_array().unwrap())
        {
            assert_eq!(u64::from(*antenna), expected.as_u64().unwrap());
        }
        for (sample, &actual) in row.data.iter().enumerate() {
            let expected = expected(case, cases, index, sample);
            assert!(actual.re.is_finite() && actual.im.is_finite());
            if expected == Complex32::default() {
                assert_eq!(
                    actual, expected,
                    "{} row {index} sample {sample}",
                    case["name"]
                );
            }
            error += f64::from((actual - expected).norm_sqr());
            power += f64::from(expected.norm_sqr());
        }
    }
    assert!(power > 0.0);
    let nrms = (error / power).sqrt();
    println!(
        "{}: flagged and unflagged DATA NRMS {nrms:.9e}",
        case["name"]
    );
    assert!(nrms <= 1e-3, "{}: {nrms}", case["name"]);
}

#[test]
fn existing_ms_flags_predict_against_casa() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    for case in cases
        .iter()
        .filter(|case| case["kind"] != "noise" && case["noise"] != true)
    {
        let one = run_case(case, 1, BackendRequest::Cpu);
        assert_casa(case, cases, &one);
        let many = run_case(case, 3, BackendRequest::Cpu);
        assert_eq!(
            predict_support::bits(&one.iter().map(|r| r.data.clone()).collect::<Vec<_>>()),
            predict_support::bits(&many.iter().map(|r| r.data.clone()).collect::<Vec<_>>())
        );
        if cfg!(target_os = "macos")
            && !matches!(case["machine"].as_str(), Some("sd" | "wproject"))
            && case["kind"] != "components"
        {
            let metal = run_case(case, 3, BackendRequest::Metal);
            assert_casa(case, cases, &metal);
            let peak = one
                .iter()
                .flat_map(|r| &r.data)
                .map(|v| v.norm())
                .fold(0.0_f32, f32::max);
            let error = metal
                .iter()
                .zip(&one)
                .flat_map(|(m, c)| m.data.iter().zip(&c.data))
                .map(|(m, c)| (m - c).norm())
                .fold(0.0_f32, f32::max);
            println!(
                "{}: Metal/CPU maximum relative error {:.9e}",
                case["name"],
                error / peak
            );
            assert!(
                error / peak <= 1e-4,
                "{}: Metal/CPU {}",
                case["name"],
                error / peak
            );
        }
    }
}

fn assert_noise(case: &Value, cases: &[Value], actual: &[Row], casa: bool) {
    let mut residuals = [Vec::new(), Vec::new()];
    let name = if case["kind"] == "noise" {
        None
    } else if case["incremental"] == true {
        Some("ft_increment")
    } else {
        Some("ft_replace")
    };
    let baseline = name.map(|name| cases.iter().find(|c| c["name"] == name).unwrap());
    for (index, row) in actual.iter().enumerate() {
        for (sample, &value) in row.data.iter().enumerate() {
            let value = if casa {
                complex(&case["rows"][index]["data"][sample])
            } else {
                value
            };
            let original = baseline.map_or(Complex32::new(3.0, 4.0), |c| {
                complex(&c["rows"][index]["data"][sample])
            });
            let residual = value - original;
            let flagged = usize::from(row.flag_row || flags(index)[sample]);
            if row.antennas[0] == row.antennas[1] {
                assert!(
                    (residual.im).abs() < 1e-6,
                    "autocorrelation imaginary noise"
                );
            } else {
                residuals[flagged].extend([f64::from(residual.re), f64::from(residual.im)]);
            }
        }
    }
    for (flagged, values) in residuals.iter().enumerate() {
        let count = values.len() as f64;
        assert!(count > 300.0);
        let mean = values.iter().sum::<f64>() / count;
        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / count;
        println!(
            "{} CASA={casa} flagged={flagged} n={count} mean={mean:.6} variance={variance:.6}",
            case["name"]
        );
        assert!(mean.abs() < 5.0 * 0.1 / count.sqrt());
        assert!((0.006..0.014).contains(&variance));
    }
}

#[test]
fn corrupts_flagged_samples_after_prediction_and_from_disk() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    for case in cases
        .iter()
        .filter(|c| c["kind"] == "noise" || c["noise"] == true)
    {
        let one = run_case(case, 1, BackendRequest::Cpu);
        assert_noise(case, cases, &one, false);
        assert_noise(case, cases, &one, true);
        let many = run_case(case, 3, BackendRequest::Cpu);
        assert_eq!(
            predict_support::bits(&one.iter().map(|r| r.data.clone()).collect::<Vec<_>>()),
            predict_support::bits(&many.iter().map(|r| r.data.clone()).collect::<Vec<_>>())
        );
        if cfg!(target_os = "macos") && case["kind"] != "noise" {
            let metal = run_case(case, 3, BackendRequest::Metal);
            assert_noise(case, cases, &metal, false);
            let peak = one
                .iter()
                .flat_map(|r| &r.data)
                .map(|v| v.norm())
                .fold(0.0_f32, f32::max);
            let error = metal
                .iter()
                .zip(&one)
                .flat_map(|(m, c)| m.data.iter().zip(&c.data))
                .map(|(m, c)| (m - c).norm())
                .fold(0.0_f32, f32::max);
            println!(
                "{}: Metal/CPU maximum relative error {:.9e}",
                case["name"],
                error / peak
            );
            assert!(error / peak <= 1e-4);
        }
    }
}

#[test]
fn independent_casa_controls_expose_flagged_component_duplication() {
    let fixture = fixture();
    let cases = fixture["cases"].as_array().unwrap();
    let component = cases
        .iter()
        .find(|c| c["name"] == "components_replace")
        .unwrap();
    for case in cases.iter().filter(|c| c["kind"] == "mixed") {
        let mut exposed = 0;
        for (row, reference) in case["rows"].as_array().unwrap().iter().enumerate() {
            for (sample, raw) in reference["data"].as_array().unwrap().iter().enumerate() {
                let intended = expected(case, cases, row, sample);
                let excess = complex(raw) - intended;
                if reference["flag_row"] == true || reference["flags"][sample] == true {
                    let retained = complex(&component["rows"][row]["data"][sample]);
                    assert!((excess - retained).norm() < 1e-6);
                    exposed += 1;
                } else {
                    assert!(excess.norm() < 1e-6);
                }
            }
        }
        assert!(exposed > 100);
    }
}
