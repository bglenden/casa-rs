// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::lookup::interpolate_gain_amplitude_phase;
use std::fs;
use std::sync::{Mutex, OnceLock};

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::position::MPosition;
use casa_core::{
    ArrayValue, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use casa_ms::{MeasurementSet, MeasurementSetBuilder, OptionalMainColumn};
use casa_tables::{ArrayShapeContract, ColumnSchema, ColumnType, Table};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use tempfile::TempDir;

use crate::model::{
    CalibrationColumnSummary, CalibrationKeywordSummary, CalibrationSubtableSummary,
    CalibrationTableSummary,
};

fn perf_env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn row(fields: Vec<RecordField>) -> RecordValue {
    RecordValue::new(fields)
}

fn scalar_table(fields: Vec<RecordField>) -> Table {
    Table::from_rows_memory(vec![row(fields)])
}

fn assert_complex_close(actual: Complex32, expected: Complex32, tolerance: f32) {
    assert!(
        (actual - expected).norm() <= tolerance,
        "actual={actual:?} expected={expected:?}"
    );
}

fn empty_cal_summary(path: PathBuf) -> CalibrationTableSummary {
    CalibrationTableSummary {
        path,
        table_type: "Calibration".to_string(),
        table_subtype: "G Jones".to_string(),
        row_count: 0,
        columns: Vec::new(),
        keywords: CalibrationKeywordSummary {
            par_type: None,
            vis_cal: None,
            ms_name: None,
            pol_basis: None,
            casa_version: None,
        },
        subtables: Vec::<CalibrationSubtableSummary>::new(),
        parameter_family: CalibrationParameterFamily::Complex,
        parameter_column: CalibrationColumnSummary {
            parameter_column: None,
            parameter_primitive_type: None,
            first_cell_shape: None,
        },
        field_ids: Vec::new(),
        spectral_window_ids: Vec::new(),
        antenna1_ids: Vec::new(),
        antenna2_ids: Vec::new(),
        observation_ids: Vec::new(),
        time_coverage: None,
        issues: Vec::new(),
    }
}

#[test]
fn calibration_perf_tracer_from_env_writes_jsonl_and_summary_log() {
    let _guard = perf_env_lock().lock().expect("perf env lock");
    let tempdir = TempDir::new().expect("tempdir");
    unsafe {
        std::env::set_var(PERF_ENV, "1");
        std::env::set_var(PERF_DIR_ENV, tempdir.path());
    }

    let mut tracer = CalibrationPerfTracer::from_env();
    assert!(tracer.is_enabled());
    tracer.write_event(&CalibrationPerfEvent {
        kind: CalibrationPerfEventKind::ApplyCompleted,
        monotonic_ns: 42,
        ms_path: "/tmp/test.ms".to_string(),
        apply_mode: "CalFlag".to_string(),
        selected_row_count: 8,
        calibration_table_count: 1,
        parang: false,
        created_corrected_data_column: true,
        updated_row_count: 8,
        flagged_row_count: 2,
        flagged_sample_count: 5,
        planning_ns: 11,
        planning_selection_ns: 12,
        planning_selected_rows_ns: 13,
        planning_measurement_set_spectral_windows_ns: 14,
        planning_calibration_table_plans_ns: 15,
        open_measurement_set_ns: 16,
        row_field_index_lookup_ns: 17,
        ensure_corrected_data_ns: 18,
        correlation_lookup_ns: 19,
        calibration_load_ns: 20,
        row_loop_ns: 21,
        row_read_total_ns: 22,
        row_fetch_ns: 23,
        row_compute_ns: 24,
        row_read_overhead_ns: 25,
        row_writeback_ns: 26,
        save_ns: 27,
        execute_apply_plan_ns: 28,
        execute_apply_plan_unattributed_ns: 29,
        drop_ns: 30,
        total_ns: 31,
    });
    drop(tracer);

    unsafe {
        std::env::remove_var(PERF_ENV);
        std::env::remove_var(PERF_DIR_ENV);
    }

    let mut json_paths = fs::read_dir(tempdir.path())
        .expect("read perf dir")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .collect::<Vec<_>>();
    json_paths.sort();
    let mut log_paths = fs::read_dir(tempdir.path())
        .expect("read perf dir")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
        .collect::<Vec<_>>();
    log_paths.sort();
    assert_eq!(json_paths.len(), 1);
    assert_eq!(log_paths.len(), 1);

    let json = fs::read_to_string(json_paths[0].clone()).expect("json trace");
    assert!(json.contains("\"kind\":\"apply_completed\""));
    assert!(json.contains("\"row_read_overhead_ns\":25"));

    let log = fs::read_to_string(log_paths[0].clone()).expect("summary log");
    assert!(log.contains("kind=ApplyCompleted"));
    assert!(log.contains("row_read_overhead_ms=0.00"));
}

#[test]
fn calibration_perf_tracer_emits_plan_and_completion_metadata() {
    let _guard = perf_env_lock().lock().expect("perf env lock");
    let tempdir = TempDir::new().expect("tempdir");
    unsafe {
        std::env::set_var(PERF_ENV, "1");
        std::env::set_var(PERF_DIR_ENV, tempdir.path());
    }

    let cal_path = tempdir.path().join("gain.cal");
    let spw_plan = crate::plan::SpectralWindowPlan {
        spw_id: 2,
        num_chan: 3,
        ref_frequency_hz: 1.4e9,
        channel_frequencies_hz: vec![1.399e9, 1.4e9, 1.401e9],
    };
    let plan = ApplyPlan {
        measurement_set_path: Some(tempdir.path().join("input.ms")),
        apply_mode: ApplyMode::CalFlag,
        requires_corrected_data_column: true,
        channel_selections: Default::default(),
        selected_rows: vec![ApplyRowPlan {
            row_index: 4,
            field_id: 5,
            observation_id: 6,
            calibration_observation_id: 6,
            scan_number: 1,
            data_desc_id: 7,
            data_spw_id: 2,
            polarization_id: 8,
            antenna1: 9,
            antenna2: 10,
            feed1: 0,
            feed2: 0,
            time_seconds: 123.0,
            interval_seconds: 2.0,
        }],
        selected_row_count: 1,
        parang: true,
        selected_field_ids: vec![5],
        selected_data_desc_ids: vec![7],
        selected_data_spw_ids: vec![2],
        measurement_set_spectral_windows: vec![spw_plan.clone()],
        calibration_tables: vec![ApplyCalibrationTablePlan {
            applicable_row_indices: None,
            unavailable_row_indices: Vec::new(),
            spec: crate::plan::ApplyCalibrationTableSpec {
                callibrary: None,
                path: cal_path.clone(),
                apply_to: Default::default(),
                gainfield: None,
                spwmap: vec![2],
                interp: ApplyInterpolationMode::Linear,
                calwt: true,
            },
            applicable_selected_row_count: 1,
            summary: empty_cal_summary(cal_path.clone()),
            resolved_gainfields: Vec::new(),
            resolved_nearest_gainfields: Vec::new(),
            spw_mapping: vec![crate::plan::ApplySpwMapping {
                data_spw_id: 2,
                calibration_spw_id: 2,
            }],
            calibration_spectral_windows: vec![spw_plan],
            interp: ApplyInterpolationMode::Linear,
            calwt: true,
        }],
    };
    let summary = ExecuteApplyPlanTraceSummary {
        selected_row_count: 1,
        calibration_table_count: 1,
        parang: true,
        created_corrected_data_column: true,
        updated_row_count: 1,
        flagged_row_count: 0,
        flagged_sample_count: 2,
        row_field_index_lookup_ns: 11,
        ensure_corrected_data_ns: 12,
        correlation_lookup_ns: 13,
        calibration_load_ns: 14,
        row_loop_ns: 15,
        row_read_total_ns: 16,
        row_fetch_ns: 17,
        row_compute_ns: 18,
        row_read_overhead_ns: 19,
        row_writeback_ns: 20,
        save_ns: 21,
        execute_apply_plan_ns: 22,
        execute_apply_plan_unattributed_ns: 23,
    };
    let report = ApplyExecutionReport {
        plan,
        created_corrected_data_column: true,
        wrote_measurement_set: true,
        updated_row_count: 1,
        flagged_row_count: 0,
        flagged_sample_count: 2,
        flag_backup_version: None,
        timings: ApplyExecutionTimings {
            planning_ns: 31,
            planning_selection_ns: 32,
            planning_selected_rows_ns: 33,
            planning_measurement_set_spectral_windows_ns: 34,
            planning_calibration_table_plans_ns: 35,
            open_measurement_set_ns: 36,
            row_field_index_lookup_ns: 37,
            ensure_corrected_data_ns: 38,
            correlation_lookup_ns: 39,
            calibration_load_ns: 40,
            row_compute_ns: 41,
            row_writeback_ns: 42,
            save_ns: 43,
            total_ns: 44,
        },
    };

    let mut tracer = CalibrationPerfTracer::from_env();
    tracer.emit_apply_plan_summary("/tmp/input.ms", &report.plan, summary);
    tracer.emit_apply_completed("/tmp/input.ms", &report, 55, summary);
    drop(tracer);

    unsafe {
        std::env::remove_var(PERF_ENV);
        std::env::remove_var(PERF_DIR_ENV);
    }

    let json_path = fs::read_dir(tempdir.path())
        .expect("read perf dir")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .expect("json trace");
    let json = fs::read_to_string(json_path).expect("json trace");
    assert!(json.contains("\"kind\":\"apply_plan_summary\""));
    assert!(json.contains("\"kind\":\"apply_completed\""));
    assert!(json.contains("\"apply_mode\":\"CalFlag\""));
    assert!(json.contains("\"parang\":true"));
    assert!(json.contains("\"planning_calibration_table_plans_ns\":35"));
    assert!(json.contains("\"drop_ns\":55"));
    assert!(json.contains("\"execute_apply_plan_unattributed_ns\":23"));
}

fn default_main_value(column: &ColumnSchema) -> Value {
    match column.column_type() {
        ColumnType::Scalar => match column.data_type().unwrap_or(PrimitiveType::Int32) {
            PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            PrimitiveType::String => Value::Scalar(ScalarValue::String(String::new())),
            _ => Value::Scalar(ScalarValue::Float64(0.0)),
        },
        ColumnType::Record => Value::Record(RecordValue::new(vec![])),
        ColumnType::Array(ArrayShapeContract::Fixed { shape }) => {
            let total: usize = shape.iter().product();
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; total]).unwrap(),
            ))
        }
        ColumnType::Array(ArrayShapeContract::Variable { ndim }) => {
            let shape: Vec<usize> = vec![1; ndim.unwrap_or(1)];
            let total: usize = shape.iter().product();
            match column.data_type().unwrap_or(PrimitiveType::Float64) {
                PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                )),
                PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape, vec![1.0; total]).unwrap(),
                )),
                _ => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
            }
        }
    }
}

#[test]
fn circular_parang_gain_matches_expected_rr_rl_lr_ll_phases() {
    let ant1 = 0.3_f64;
    let ant2 = -0.2_f64;
    let all = vec![5, 6, 7, 8];

    let rr = parallactic_angle_gain(5, 0, &all, ant1, ant2).expect("RR correction");
    let rl = parallactic_angle_gain(6, 0, &all, ant1, ant2).expect("RL correction");
    let lr = parallactic_angle_gain(7, 0, &all, ant1, ant2).expect("LR correction");
    let ll = parallactic_angle_gain(8, 0, &all, ant1, ant2).expect("LL correction");

    assert_eq!(rr, circular_parang_gain(-ant1 + ant2));
    assert_eq!(rl, circular_parang_gain(-ant1 - ant2));
    assert_eq!(lr, circular_parang_gain(ant1 + ant2));
    assert_eq!(ll, circular_parang_gain(ant1 - ant2));
}

#[test]
fn linear_feed_parang_basis_is_rejected() {
    let all = vec![9, 10, 11, 12];
    let err = parallactic_angle_gain(9, 7, &all, 0.1, 0.2).expect_err("linear basis error");
    match err {
        ApplyExecutionError::UnsupportedParallacticAngleBasis {
            data_desc_id,
            correlation_types,
        } => {
            assert_eq!(data_desc_id, 7);
            assert_eq!(correlation_types, vec!["XX", "XY", "YX", "YY"]);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn feed_angle_lookup_prefers_exact_spw_then_wildcard_and_nearest_time() {
    let state = ParallacticAngleState {
        engine: MsCalEngine::from_parts(
            vec![MPosition::new_itrf(0.0, 0.0, 0.0)],
            vec![MDirection::from_angles(1.0, 0.5, DirectionRef::J2000)],
            MPosition::new_itrf(0.0, 0.0, 0.0),
            casa_test_support::deterministic_measures_provider(),
        ),
        feed_rows: HashMap::from([(
            (0, 0),
            vec![
                FeedAngleRow {
                    spectral_window_id: -1,
                    time_seconds: 10.0,
                    interval_seconds: 0.0,
                    receptor0_angle_rad: 0.25,
                },
                FeedAngleRow {
                    spectral_window_id: 3,
                    time_seconds: 20.0,
                    interval_seconds: 5.0,
                    receptor0_angle_rad: 0.5,
                },
            ],
        )]),
    };

    assert_eq!(state.lookup_receptor0_angle(0, 0, 3, 20.0), Some(0.5));
    assert_eq!(state.lookup_receptor0_angle(0, 0, 2, 20.0), Some(0.25));
}

#[test]
fn feed_parallactic_angle_adds_receptor0_angle() {
    let time_seconds = 59000.5 * 86400.0;
    let state = ParallacticAngleState {
        engine: MsCalEngine::from_parts(
            vec![
                MPosition::new_itrf(-1_601_185.4, -5_041_977.5, 3_554_875.9),
                MPosition::new_itrf(-1_601_085.4, -5_041_977.5, 3_554_875.9),
            ],
            vec![MDirection::from_angles(
                0.0,
                std::f64::consts::FRAC_PI_4,
                DirectionRef::J2000,
            )],
            MPosition::new_itrf(-1_601_185.4, -5_041_977.5, 3_554_875.9),
            casa_test_support::deterministic_measures_provider(),
        ),
        feed_rows: HashMap::from([(
            (0, 0),
            vec![FeedAngleRow {
                spectral_window_id: -1,
                time_seconds,
                interval_seconds: 0.0,
                receptor0_angle_rad: 0.25,
            }],
        )]),
    };

    let base = state
        .engine
        .feed_parallactic_angle(time_seconds, 0, 0)
        .unwrap();
    let feed = state
        .feed_parallactic_angle(time_seconds, 0, 0, 0, 1)
        .unwrap();

    assert!((feed - (base + 0.25)).abs() < 1.0e-12);
}

#[test]
fn row_geometry_cache_reuses_row_dependent_grids_and_fast_terms() {
    let mut cache = RowGeometryCache::default();
    let offset_a = [1_u64, 2, 3];
    let offset_b = [4_u64, 5, 6];
    cache.ensure_fast_term_context(12.5, 3);

    assert_eq!(cache.cached_fast_antpos_delay(0, 2, offset_a), None);
    cache.remember_fast_antpos_delay(0, 2, offset_a, 4.25);
    assert_eq!(cache.cached_fast_antpos_delay(0, 2, offset_a), Some(4.25));
    assert_eq!(cache.cached_fast_antpos_delay(0, 2, offset_b), None);

    cache.ensure_fast_term_context(12.5, 4);
    assert_eq!(cache.cached_fast_antpos_delay(0, 2, offset_a), None);

    let antpos = Arc::new(CalibrationGrid::Antpos(AntposGrid {
        offsets_m: [1.0, 2.0, 3.0],
        flagged: false,
    }));
    let gain_curve = Arc::new(CalibrationGrid::GainCurve(GainCurveGrid {
        power: false,
        receptor_count: 1,
        coefficients: ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![1.0_f32, 0.0, 0.0, 0.0])
            .unwrap(),
        flags: ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false; 4]).unwrap(),
    }));
    let opacity = Arc::new(CalibrationGrid::Opacity(OpacityGrid {
        tau: 0.1,
        flagged: true,
    }));
    let complex = Arc::new(CalibrationGrid::Complex(GainGrid {
        receptor_count: 1,
        channel_count: 1,
        values: ArrayD::from_shape_vec(IxDyn(&[1, 1]).f(), vec![Complex32::new(1.0, 0.0)]).unwrap(),
        flags: ArrayD::from_shape_vec(IxDyn(&[1, 1]).f(), vec![false]).unwrap(),
    }));

    let antpos_key = materialized_grid_key(&antpos, 3, 2, 12.5, 7).expect("antpos key");
    let gain_curve_key = materialized_grid_key(&gain_curve, 3, 2, 12.5, 7).expect("gain curve key");
    let opacity_key = materialized_grid_key(&opacity, 3, 2, 12.5, 7).expect("opacity key");
    assert_eq!(antpos_key.kind, 1);
    assert_eq!(gain_curve_key.kind, 2);
    assert_eq!(opacity_key.kind, 3);
    assert!(materialized_grid_key(&complex, 3, 2, 12.5, 7).is_none());

    assert!(cache.materialized_grid(antpos_key, false).is_none());
    cache.insert_materialized_grid(antpos_key, antpos.clone(), false);
    assert!(Arc::ptr_eq(
        &cache
            .materialized_grid(antpos_key, false)
            .expect("materialized grid"),
        &antpos
    ));

    let lookup_key =
        calibration_lookup_key(5, 3, 7, 2, 12.5, ApplyInterpolationMode::NearestLinear);
    assert_eq!(lookup_key.interp, 3);
    assert_eq!(
        calibration_lookup_key(5, 3, 7, 2, 12.5, ApplyInterpolationMode::Nearest).interp,
        1
    );
    assert_eq!(
        calibration_lookup_key(5, 3, 7, 2, 12.5, ApplyInterpolationMode::Linear).interp,
        2
    );

    let mut load_count = 0;
    let first = cache
        .calibration_lookup(lookup_key, None, || {
            load_count += 1;
            Ok(Some(fields::GridAtTime {
                grid: gain_curve.clone(),
                interpolated: false,
            }))
        })
        .expect("first lookup")
        .expect("grid");
    let second = cache
        .calibration_lookup(lookup_key, None, || {
            load_count += 1;
            Ok(Some(fields::GridAtTime {
                grid: opacity.clone(),
                interpolated: false,
            }))
        })
        .expect("cached lookup")
        .expect("grid");
    assert_eq!(load_count, 1);
    assert!(Arc::ptr_eq(&first.grid, &second.grid));

    let missing_key = calibration_lookup_key(6, 3, 7, 2, 12.5, ApplyInterpolationMode::Nearest);
    assert!(
        cache
            .calibration_lookup(missing_key, None, || Ok(None))
            .expect("missing lookup")
            .is_none()
    );
    assert!(
        cache
            .calibration_lookup(missing_key, None, || panic!(
                "cached None should not reload"
            ))
            .expect("cached missing lookup")
            .is_none()
    );
}

#[test]
fn interpolate_time_linear_covers_complex_delay_and_error_cases() {
    let path = Path::new("/tmp/interp.cal");
    let complex_pair = [
        CalibrationSolution {
            time_seconds: 30.0,
            grid: Arc::new(CalibrationGrid::Complex(GainGrid {
                receptor_count: 1,
                channel_count: 2,
                values: ArrayD::from_shape_vec(
                    IxDyn(&[1, 2]).f(),
                    vec![Complex32::new(5.0, 4.0), Complex32::new(7.0, 6.0)],
                )
                .unwrap(),
                flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![true, false]).unwrap(),
            })),
        },
        CalibrationSolution {
            time_seconds: 10.0,
            grid: Arc::new(CalibrationGrid::Complex(GainGrid {
                receptor_count: 1,
                channel_count: 2,
                values: ArrayD::from_shape_vec(
                    IxDyn(&[1, 2]).f(),
                    vec![Complex32::new(1.0, 0.0), Complex32::new(3.0, 2.0)],
                )
                .unwrap(),
                flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false]).unwrap(),
            })),
        },
    ];

    match interpolate_time_linear(path, &complex_pair, 20.0)
        .unwrap()
        .as_ref()
    {
        CalibrationGrid::Complex(grid) => {
            assert_complex_close(
                grid.values[[0, 0]],
                interpolate_gain_amplitude_phase(
                    Complex32::new(1.0, 0.0),
                    Complex32::new(5.0, 4.0),
                    0.5,
                ),
                1.0e-6,
            );
            assert_complex_close(
                grid.values[[0, 1]],
                interpolate_gain_amplitude_phase(
                    Complex32::new(3.0, 2.0),
                    Complex32::new(7.0, 6.0),
                    0.5,
                ),
                1.0e-6,
            );
            assert!(grid.flags[[0, 0]]);
            assert!(!grid.flags[[0, 1]]);
        }
        _ => panic!("expected complex interpolation"),
    }

    let wrapped = interpolate_gain_amplitude_phase(
        Complex32::new((-3.0_f32).cos(), (-3.0_f32).sin()),
        Complex32::new(3.0_f32.cos(), 3.0_f32.sin()),
        0.5,
    );
    assert!(wrapped.arg().abs() > 3.0);

    let delay_pair = [
        CalibrationSolution {
            time_seconds: 1.0,
            grid: Arc::new(CalibrationGrid::Delay(DelayGrid {
                receptor_count: 1,
                channel_count: 2,
                values_ns: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![1.0_f32, 3.0]).unwrap(),
                flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, true]).unwrap(),
            })),
        },
        CalibrationSolution {
            time_seconds: 3.0,
            grid: Arc::new(CalibrationGrid::Delay(DelayGrid {
                receptor_count: 1,
                channel_count: 2,
                values_ns: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![5.0_f32, 7.0]).unwrap(),
                flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![true, false]).unwrap(),
            })),
        },
    ];
    match interpolate_time_linear(path, &delay_pair, 2.0)
        .unwrap()
        .as_ref()
    {
        CalibrationGrid::Delay(grid) => {
            assert_eq!(grid.values_ns[[0, 0]], 3.0);
            assert_eq!(grid.values_ns[[0, 1]], 5.0);
            assert!(grid.flags[[0, 0]]);
            assert!(grid.flags[[0, 1]]);
        }
        _ => panic!("expected delay interpolation"),
    }

    match interpolate_time_linear(
        path,
        &[CalibrationSolution {
            time_seconds: 10.0,
            grid: Arc::new(CalibrationGrid::Complex(GainGrid {
                receptor_count: 1,
                channel_count: 2,
                values: ArrayD::from_shape_vec(
                    IxDyn(&[1, 2]).f(),
                    vec![Complex32::new(1.0, 0.0), Complex32::new(3.0, 2.0)],
                )
                .unwrap(),
                flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false]).unwrap(),
            })),
        }],
        0.0,
    )
    .unwrap()
    .as_ref()
    {
        CalibrationGrid::Complex(grid) => {
            assert_eq!(grid.values[[0, 0]], Complex32::new(1.0, 0.0))
        }
        _ => panic!("expected complex interpolation"),
    }
    match interpolate_time_linear(path, &[], 0.0) {
        Ok(_) => panic!("expected empty interpolation to fail"),
        Err(ApplyExecutionError::UnsupportedInterpolation { .. }) => {}
        Err(other) => panic!("unexpected error: {other:?}"),
    }
    match interpolate_time_linear(
        path,
        &[
            CalibrationSolution {
                time_seconds: 10.0,
                grid: Arc::new(CalibrationGrid::Complex(GainGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values: ArrayD::from_shape_vec(
                        IxDyn(&[1, 2]).f(),
                        vec![Complex32::new(1.0, 0.0), Complex32::new(3.0, 2.0)],
                    )
                    .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false]).unwrap(),
                })),
            },
            CalibrationSolution {
                time_seconds: 20.0,
                grid: Arc::new(CalibrationGrid::Delay(DelayGrid {
                    receptor_count: 1,
                    channel_count: 2,
                    values_ns: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![1.0_f32, 2.0])
                        .unwrap(),
                    flags: ArrayD::from_shape_vec(IxDyn(&[1, 2]).f(), vec![false, false]).unwrap(),
                })),
            },
        ],
        15.0,
    ) {
        Ok(_) => panic!("expected mixed-family interpolation to fail"),
        Err(ApplyExecutionError::UnsupportedInterpolation { .. }) => {}
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn fast_gain_pair_application_covers_layouts_flags_and_frequency_errors() {
    let scalar_ant1 = Arc::new(CalibrationGrid::Complex(GainGrid {
        receptor_count: 2,
        channel_count: 1,
        values: ArrayD::from_shape_vec(
            IxDyn(&[2, 1]).f(),
            vec![Complex32::new(2.0, 0.0), Complex32::new(3.0, 0.0)],
        )
        .unwrap(),
        flags: ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![false, false]).unwrap(),
    }));
    let scalar_ant2 = Arc::new(CalibrationGrid::Complex(GainGrid {
        receptor_count: 2,
        channel_count: 1,
        values: ArrayD::from_shape_vec(
            IxDyn(&[2, 1]).f(),
            vec![Complex32::new(4.0, 0.0), Complex32::new(5.0, 0.0)],
        )
        .unwrap(),
        flags: ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![false, false]).unwrap(),
    }));
    let channel_ant1 = Arc::new(CalibrationGrid::Complex(GainGrid {
        receptor_count: 2,
        channel_count: 2,
        values: ArrayD::from_shape_vec(
            IxDyn(&[2, 2]).f(),
            vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(2.0, 0.0),
                Complex32::new(3.0, 0.0),
                Complex32::new(4.0, 0.0),
            ],
        )
        .unwrap(),
        flags: ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false, false, false, true]).unwrap(),
    }));
    let channel_ant2 = Arc::new(CalibrationGrid::Complex(GainGrid {
        receptor_count: 2,
        channel_count: 2,
        values: ArrayD::from_shape_vec(
            IxDyn(&[2, 2]).f(),
            vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
                Complex32::new(1.0, 0.0),
            ],
        )
        .unwrap(),
        flags: ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false; 4]).unwrap(),
    }));

    let mut corrected =
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![Complex32::new(80.0, 0.0); 4]).unwrap();
    let mut flags =
        ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![false, false, false, false]).unwrap();
    let mut newly_flagged_samples = 0;
    apply_complex_gain_pairs_fast(
        &[
            FastGainPair::Complex(scalar_ant1.clone(), scalar_ant2.clone()),
            FastGainPair::Complex(channel_ant1.clone(), channel_ant2.clone()),
        ],
        &mut ApplyBuffer {
            data_desc_id: 0,
            correlation_types: &[5, 8],
            data_channels: DataChannelGrid::all(&[1.0e9, 1.1e9]),
            corrected: &mut corrected,
            flags: &mut flags,
            propagate_flags: true,
            newly_flagged_samples: &mut newly_flagged_samples,
        },
    )
    .unwrap();
    assert_complex_close(corrected[[0, 0]], Complex32::new(10.0, 0.0), 1.0e-6);
    assert_complex_close(corrected[[1, 0]], Complex32::new(80.0 / 30.0, 0.0), 1.0e-6);
    assert_complex_close(corrected[[0, 1]], Complex32::new(80.0 / 24.0, 0.0), 1.0e-6);
    assert!(flags[[1, 1]]);
    assert_eq!(newly_flagged_samples, 1);

    let mut fortran_corrected =
        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![Complex32::new(90.0, 0.0); 4]).unwrap();
    let mut fortran_flags =
        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false, false, true, false]).unwrap();
    let mut fortran_new_flags = 0;
    apply_complex_gain_pairs_fast(
        &[
            FastGainPair::GainCurve {
                ant1: FastReceptorGains {
                    receptor_count: 2,
                    values: [2.0, 3.0, 0.0, 0.0],
                    flags: [false, false, false, false],
                },
                ant2: FastReceptorGains {
                    receptor_count: 2,
                    values: [5.0, 7.0, 0.0, 0.0],
                    flags: [false, false, false, false],
                },
            },
            FastGainPair::Opacity {
                denom: Complex32::new(0.5, 0.0),
                flagged: false,
            },
        ],
        &mut ApplyBuffer {
            data_desc_id: 0,
            correlation_types: &[5, 8],
            data_channels: DataChannelGrid::all(&[1.0e9, 1.1e9]),
            corrected: &mut fortran_corrected,
            flags: &mut fortran_flags,
            propagate_flags: true,
            newly_flagged_samples: &mut fortran_new_flags,
        },
    )
    .unwrap();
    assert_complex_close(fortran_corrected[[0, 0]], Complex32::new(18.0, 0.0), 1.0e-6);
    assert_complex_close(
        fortran_corrected[[1, 0]],
        Complex32::new(90.0 / 21.0 / 0.5, 0.0),
        1.0e-6,
    );
    assert_eq!(fortran_new_flags, 0);

    let mut antpos_corrected =
        ArrayD::from_shape_vec(IxDyn(&[1, 2]), vec![Complex32::new(1.0, 0.0); 2]).unwrap();
    let mut antpos_flags = ArrayD::from_shape_vec(IxDyn(&[1, 2]), vec![false; 2]).unwrap();
    let mut antpos_new_flags = 0;
    assert!(matches!(
        apply_complex_gain_pairs_fast(
            &[FastGainPair::Antpos {
                delay_delta_ns: 1.0,
                flagged: false,
            }],
            &mut ApplyBuffer {
                data_desc_id: 0,
                correlation_types: &[5],
                data_channels: DataChannelGrid::all(&[1.0e9]),
                corrected: &mut antpos_corrected,
                flags: &mut antpos_flags,
                propagate_flags: false,
                newly_flagged_samples: &mut antpos_new_flags,
            },
        )
        .unwrap_err(),
        ApplyExecutionError::UnsupportedInterpolation { .. }
    ));

    assert!(matches!(
        fast_pair_receptors(9, &[42], 1).unwrap_err(),
        ApplyExecutionError::UnsupportedCorrelationLayout { .. }
    ));
}

#[test]
fn bpoly_helpers_and_typed_accessors_cover_branchy_paths() {
    let path = Path::new("/tmp/bpoly.cal");
    assert_eq!(
        infer_bpoly_receptor_count(4, 2, 2, path, 0, "AMP").unwrap(),
        2
    );
    assert_eq!(
        infer_bpoly_receptor_count(0, 0, 0, path, 0, "AMP").unwrap(),
        1
    );
    assert!(infer_bpoly_receptor_count(5, 2, 3, path, 0, "AMP").is_err());

    assert_eq!(
        split_bpoly_coefficients(vec![1.0, 2.0, 3.0, 4.0], 2, 2, path, 1, "AMP").unwrap(),
        vec![vec![1.0, 2.0], vec![3.0, 4.0]]
    );
    assert!(split_bpoly_coefficients(vec![1.0, 2.0], 2, 2, path, 1, "AMP").is_err());

    assert_eq!(legacy_bpoly_chebyshev_value(&[], 0.0, 1.0, 0.5), 0.0);
    assert_eq!(legacy_bpoly_chebyshev_value(&[4.0], 0.0, 1.0, 0.5), 2.0);
    assert!((legacy_bpoly_chebyshev_value(&[2.0, 1.0], 0.0, 10.0, 10.0) - 2.0).abs() < 1.0e-12);

    let table = scalar_table(vec![
        RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(7))),
        RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(12.5))),
        RecordField::new(
            "SCALE_FACTOR",
            Value::Scalar(ScalarValue::Complex64(Complex64::new(1.0, -2.0))),
        ),
        RecordField::new(
            "PHASE_UNITS",
            Value::Scalar(ScalarValue::String("DEG".to_string())),
        ),
        RecordField::new(
            "F32S",
            Value::Array(ArrayValue::Float32(
                ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![5.0_f32, 6.0]).unwrap(),
            )),
        ),
        RecordField::new(
            "I32S",
            Value::Array(ArrayValue::Int32(
                ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![3_i32, 4]).unwrap(),
            )),
        ),
    ]);
    assert_eq!(get_i32(&table, 0, "FIELD_ID").unwrap(), 7);
    assert_eq!(get_f64(&table, 0, "TIME").unwrap(), 12.5);
    assert_eq!(
        get_complex32(&table, 0, "SCALE_FACTOR").unwrap(),
        Complex32::new(1.0, -2.0)
    );
    assert_eq!(get_string(&table, 0, "PHASE_UNITS").unwrap(), "DEG");
    assert_eq!(
        get_numeric_array(&table, 0, "I32S", path).unwrap(),
        vec![3.0, 4.0]
    );
    assert_eq!(
        get_f64_array(&table, 0, "F32S", path).unwrap(),
        vec![5.0, 6.0]
    );
    assert!(matches!(
        get_i32(&table, 0, "PHASE_UNITS").unwrap_err(),
        ApplyExecutionError::UnsupportedParameterShape { .. }
    ));

    let cal_desc = Table::from_rows_memory(vec![
        row(vec![
            RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![9_i32]).unwrap(),
                )),
            ),
            RecordField::new("NUM_RECEPTORS", Value::Scalar(ScalarValue::Int32(2))),
        ]),
        row(vec![
            RecordField::new(
                "SPECTRAL_WINDOW_ID",
                Value::Array(ArrayValue::Int32(
                    ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![3_i32]).unwrap(),
                )),
            ),
            RecordField::new("NUM_RECEPTORS", Value::Scalar(ScalarValue::Int32(1))),
        ]),
    ]);
    let entries = load_bpoly_cal_desc_map(path, &cal_desc).unwrap();
    assert_eq!(entries.get(&0).unwrap().spw_id, 9);
    assert_eq!(entries.get(&1).unwrap().receptor_count, 1);
    let invalid_cal_desc = Table::from_rows_memory(vec![row(vec![
        RecordField::new(
            "SPECTRAL_WINDOW_ID",
            Value::Array(ArrayValue::Int32(
                ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![3_i32]).unwrap(),
            )),
        ),
        RecordField::new("NUM_RECEPTORS", Value::Scalar(ScalarValue::Int32(-1))),
    ])]);
    assert!(load_bpoly_cal_desc_map(path, &invalid_cal_desc).is_err());

    assert_eq!(weights::lower_median(vec![3.0, 1.0, 2.0]), 2.0);
    assert_eq!(weights::lower_median(vec![4.0, 1.0, 3.0, 2.0]), 2.0);
    assert_eq!(
        expand_weight_to_spectrum(
            &ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![1.5_f32, 2.5]).unwrap(),
            2,
        )
        .iter()
        .copied()
        .collect::<Vec<_>>(),
        vec![1.5, 1.5, 2.5, 2.5]
    );
    assert_eq!(stokes_name(5), "RR");
    assert_eq!(stokes_name(99), "??");
    assert_eq!(correlation_receptors(10), Some((0, 1)));
    assert_eq!(correlation_receptors(42), None);
}

#[test]
fn sample_bpoly_row_and_corrected_data_column_cover_remaining_helpers() {
    let bpoly = scalar_table(vec![
        RecordField::new(
            COL_SCALE_FACTOR,
            Value::Scalar(ScalarValue::Complex32(Complex32::new(2.0, 0.0))),
        ),
        RecordField::new(
            COL_VALID_DOMAIN,
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![1.0_f64, 3.0]).unwrap(),
            )),
        ),
        RecordField::new(COL_N_POLY_AMP, Value::Scalar(ScalarValue::Int32(1))),
        RecordField::new(COL_N_POLY_PHASE, Value::Scalar(ScalarValue::Int32(1))),
        RecordField::new(
            COL_POLY_COEFF_AMP,
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0_f64, 0.0]).unwrap(),
            )),
        ),
        RecordField::new(
            COL_POLY_COEFF_PHASE,
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0_f64, 90.0]).unwrap(),
            )),
        ),
        RecordField::new(
            COL_PHASE_UNITS,
            Value::Scalar(ScalarValue::String("DEG".to_string())),
        ),
    ]);
    let plan = crate::plan::SpectralWindowPlan {
        spw_id: 5,
        num_chan: 3,
        ref_frequency_hz: 1.2e9,
        channel_frequencies_hz: vec![0.5, 1.5, 2.5],
    };
    assert_eq!(cal_spw_reference_frequency_hz(&plan), 1.5);
    match sample_bpoly_row(
        &bpoly,
        0,
        Path::new("/tmp/bpoly"),
        &plan,
        LegacyCalDescEntry {
            spw_id: 5,
            receptor_count: 2,
        },
        true,
    )
    .unwrap()
    {
        CalibrationGrid::Complex(grid) => {
            assert_eq!(grid.receptor_count, 2);
            assert_eq!(grid.channel_count, 3);
            assert_eq!(grid.values[[0, 0]], Complex32::new(1.0, 0.0));
            assert!((grid.values[[0, 1]].norm() - 2.0).abs() < 1.0e-5);
            assert_ne!(grid.values[[0, 1]], Complex32::new(1.0, 0.0));
        }
        _ => panic!("expected complex BPOLY output"),
    }

    let bad_phase_units = scalar_table(vec![
        RecordField::new(
            COL_SCALE_FACTOR,
            Value::Scalar(ScalarValue::Complex32(Complex32::new(2.0, 0.0))),
        ),
        RecordField::new(
            COL_VALID_DOMAIN,
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![1.0_f64, 3.0]).unwrap(),
            )),
        ),
        RecordField::new(COL_N_POLY_AMP, Value::Scalar(ScalarValue::Int32(1))),
        RecordField::new(COL_N_POLY_PHASE, Value::Scalar(ScalarValue::Int32(1))),
        RecordField::new(
            COL_POLY_COEFF_AMP,
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0_f64, 0.0]).unwrap(),
            )),
        ),
        RecordField::new(
            COL_POLY_COEFF_PHASE,
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[2]).f(), vec![0.0_f64, 90.0]).unwrap(),
            )),
        ),
        RecordField::new(
            COL_PHASE_UNITS,
            Value::Scalar(ScalarValue::String("TURNS".to_string())),
        ),
    ]);
    assert!(
        sample_bpoly_row(
            &bad_phase_units,
            0,
            Path::new("/tmp/bpoly"),
            &plan,
            LegacyCalDescEntry {
                spw_id: 5,
                receptor_count: 2,
            },
            true,
        )
        .is_err()
    );

    let mut ms = MeasurementSet::create_memory(
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .unwrap();
    let schema = ms.main_table().schema().unwrap().clone();
    let fields = schema
        .columns()
        .iter()
        .map(|column| {
            if column.name() == VisibilityDataColumn::Data.name() {
                RecordField::new(
                    column.name(),
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[2, 1]).f(),
                            vec![Complex32::new(1.0, 0.0), Complex32::new(0.0, 1.0)],
                        )
                        .unwrap(),
                    )),
                )
            } else {
                RecordField::new(column.name(), default_main_value(column))
            }
        })
        .collect::<Vec<_>>();
    ms.main_table_mut()
        .add_row(RecordValue::new(fields))
        .unwrap();

    assert_eq!(
        ensure_corrected_data_column(&mut ms, "DATA").unwrap(),
        CorrectedDataCreation { created: true }
    );
    assert!(
        ms.main_table()
            .schema()
            .unwrap()
            .contains_column(VisibilityDataColumn::CorrectedData.name())
    );
    assert!(
        ms.main_table()
            .row_accessor()
            .row(0)
            .unwrap()
            .get(VisibilityDataColumn::CorrectedData.name())
            .is_some()
    );
    assert_eq!(
        ensure_corrected_data_column(&mut ms, "DATA").unwrap(),
        CorrectedDataCreation { created: false }
    );
    assert_eq!(display_ms_path(&ms), "<in-memory>");
}
