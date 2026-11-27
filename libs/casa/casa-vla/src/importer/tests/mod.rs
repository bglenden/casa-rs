// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use std::path::PathBuf;

use crate::VlaDiskReader;
use tempfile::tempdir;

fn test_measures() -> Arc<dyn MeasuresProvider> {
    Arc::new(
        casa_measures::MeasuresRuntime::open_discovered(Default::default())
            .expect("test measures runtime"),
    )
}

#[test]
fn import_perf_tracer_from_env_writes_jsonl_and_summary_log() {
    let tempdir = tempdir().expect("create perf tempdir");
    // SAFETY: test configures process-local env before constructing the tracer and
    // clears it before returning; tests in this crate do not read these keys concurrently.
    unsafe {
        std::env::set_var(PERF_ENV, "1");
        std::env::set_var(PERF_DIR_ENV, tempdir.path());
    }

    let mut tracer = ImportPerfTracer::from_env();
    let report = ImportReport {
        vis: PathBuf::from("/tmp/example.ms"),
        logical_records_seen: 10,
        logical_records_imported: 9,
        logical_records_skipped: 1,
        main_rows_written: 42,
    };
    tracer.emit_import_completed(
        &report.vis,
        &report,
        ImportPerfSummary {
            create_measurement_set_ns: 1,
            normalize_record_ns: 2,
            push_record_ns: 3,
            reorder_baseline_ns: 4,
            flag_row_ns: 5,
            append_main_row_ns: 7,
            finish_metadata_ns: 8,
            save_ns: 9,
            write_producer_ns: 10,
            write_queue_wait_ns: 11,
            write_assemble_ns: 12,
            write_physical_ns: 13,
            write_finalize_ns: 14,
            write_rows: 42,
            write_bytes: 16,
            write_peak_planned_resident_bytes: 17,
        },
        40,
    );

    // SAFETY: paired cleanup for the test-local env configuration above.
    unsafe {
        std::env::remove_var(PERF_ENV);
        std::env::remove_var(PERF_DIR_ENV);
    }

    let mut json_paths = Vec::new();
    let mut log_paths = Vec::new();
    for entry in std::fs::read_dir(tempdir.path()).expect("read perf dir") {
        let path = entry.expect("read dir entry").path();
        if path
            .extension()
            .is_some_and(|extension| extension == "jsonl")
        {
            json_paths.push(path);
        } else if path.extension().is_some_and(|extension| extension == "log") {
            log_paths.push(path);
        }
    }
    assert_eq!(json_paths.len(), 1, "expected one jsonl perf trace");
    assert_eq!(log_paths.len(), 1, "expected one log perf trace");

    let json = std::fs::read_to_string(&json_paths[0]).expect("read perf jsonl");
    assert!(json.contains("\"kind\":\"import_completed\""));
    assert!(json.contains("\"main_rows_written\":42"));
    assert!(json.contains("\"write_rows\":42"));
    assert!(json.contains("\"write_peak_planned_resident_bytes\":17"));

    let log = std::fs::read_to_string(&log_paths[0]).expect("read perf log");
    assert!(log.contains("rows=42"));
    assert!(log.contains("save_ms="));
    assert!(log.contains("write_physical_ms="));
    assert!(log.contains("write_peak_planned_resident_bytes=17"));
}

#[test]
fn reorders_cross_hand_correlations_when_antennas_swap() {
    let data = ArrayValue::Complex32(
        ArrayD::from_shape_vec(
            IxDyn(&[4, 1]).f(),
            vec![
                Complex32::new(1.0, 1.0),
                Complex32::new(2.0, 2.0),
                Complex32::new(3.0, 3.0),
                Complex32::new(4.0, 4.0),
            ],
        )
        .unwrap(),
    );
    let flag =
        ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false; 4]).unwrap());
    let weight = ArrayValue::Float32(
        ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
    );
    let sigma = ArrayValue::Float32(
        ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![5.0, 6.0, 7.0, 8.0]).unwrap(),
    );

    let (
        a1,
        a2,
        reordered,
        _,
        _,
        reordered_flag,
        reordered_flag_category,
        reordered_weight,
        reordered_sigma,
        corr_types,
    ) = reorder_baseline_for_ms(
        3,
        1,
        data.clone(),
        data.clone(),
        data,
        flag,
        ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1, 1]).f(), vec![false; 4]).unwrap()),
        weight,
        sigma,
        &[5, 6, 7, 8],
    )
    .unwrap();

    assert_eq!((a1, a2), (1, 3));
    assert_eq!(corr_types, vec![5, 7, 6, 8]);
    let ArrayValue::Complex32(array) = reordered else {
        panic!("expected Complex32");
    };
    assert_eq!(array[[0, 0]], Complex32::new(1.0, -1.0));
    assert_eq!(array[[1, 0]], Complex32::new(3.0, -3.0));
    assert_eq!(array[[2, 0]], Complex32::new(2.0, -2.0));
    assert_eq!(array[[3, 0]], Complex32::new(4.0, -4.0));
    let ArrayValue::Float32(weight_array) = reordered_weight else {
        panic!("expected Float32");
    };
    let ArrayValue::Float32(sigma_array) = reordered_sigma else {
        panic!("expected Float32");
    };
    let ArrayValue::Bool(flag_array) = reordered_flag else {
        panic!("expected Bool");
    };
    let ArrayValue::Bool(flag_category_array) = reordered_flag_category else {
        panic!("expected Bool");
    };
    assert_eq!(
        weight_array.iter().copied().collect::<Vec<_>>(),
        vec![1.0, 3.0, 2.0, 4.0]
    );
    assert_eq!(
        sigma_array.iter().copied().collect::<Vec<_>>(),
        vec![5.0, 7.0, 6.0, 8.0]
    );
    assert_eq!(
        flag_array.iter().copied().collect::<Vec<_>>(),
        vec![false, false, false, false]
    );
    assert_eq!(
        flag_category_array.iter().copied().collect::<Vec<_>>(),
        vec![false, false, false, false]
    );
}

#[test]
fn swapped_baselines_keep_flag_categories_and_merge_online_shadow_flags() {
    let flag = ArrayValue::Bool(
        ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false, false, true, true]).unwrap(),
    );
    let flag_category = ArrayValue::Bool(
        ArrayD::from_shape_vec(
            IxDyn(&[4, 1, DEFAULT_FLAG_CATEGORIES]).f(),
            vec![
                false, false, false, false, false, false, //
                false, false, false, false, false, false, //
                false, false, true, false, false, false, //
                false, false, true, false, false, false, //
            ],
        )
        .unwrap(),
    );
    let expected_flag_category = flag_category.clone();

    let (_, _, _, _, _, reordered_flag, reordered_flag_category, _, _, _) =
        reorder_baseline_for_ms(
            3,
            1,
            ArrayValue::Complex32(
                ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![Complex32::new(0.0, 0.0); 4])
                    .unwrap(),
            ),
            ArrayValue::Complex32(
                ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![Complex32::new(0.0, 0.0); 4])
                    .unwrap(),
            ),
            ArrayValue::Complex32(
                ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![Complex32::new(0.0, 0.0); 4])
                    .unwrap(),
            ),
            flag,
            flag_category.clone(),
            ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap()),
            ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap()),
            &[5, 6, 7, 8],
        )
        .unwrap();

    let ArrayValue::Bool(flag_array) = reordered_flag else {
        panic!("expected Bool");
    };
    assert_eq!(
        flag_array.iter().copied().collect::<Vec<_>>(),
        vec![false, true, true, true]
    );
    assert_eq!(reordered_flag_category, expected_flag_category);
}

#[test]
fn writes_normalized_record_into_measurement_set() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("synthetic.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData)
        .with_optional_subtable(SubtableId::Source)
        .with_optional_subtable(SubtableId::Doppler);
    let ms = MeasurementSet::create(&ms_path, builder).unwrap();
    let mut writer = VlaImportState::new(
        ImportVlaOptions {
            archivefiles: vec![PathBuf::from("synthetic.xp1")],
            vis: Some(ms_path.clone()),
            ..ImportVlaOptions::default()
        },
        ms,
        1,
        1,
    )
    .unwrap();

    let data = ArrayValue::Complex32(
        ArrayD::from_shape_vec(
            IxDyn(&[4, 1]).f(),
            vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(2.0, 0.0),
                Complex32::new(3.0, 0.0),
                Complex32::new(4.0, 0.0),
            ],
        )
        .unwrap(),
    );
    let model = ArrayValue::Complex32(
        ArrayD::from_shape_vec(
            IxDyn(&[4, 1]).f(),
            vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(1.0, 0.0),
            ],
        )
        .unwrap(),
    );
    let flag =
        ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false; 4]).unwrap());
    let flag_category =
        ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1, 1]).f(), vec![false; 4]).unwrap());
    let weight =
        ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());
    let sigma = ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());

    writer
        .push_record(NormalizedRecord {
            project: "AB123".to_string(),
            source_name: "3C286".to_string(),
            calibration_code: "C".to_string(),
            source_num_lines: 1,
            source_direction: [1.0, 0.5],
            array_id: 0,
            time_seconds: 5_097_600_000.0,
            integration_seconds: 10.0,
            direction_epoch: DirectionEpoch::J2000,
            antennas: vec![
                NormalizedAntenna {
                    antenna_id: 1,
                    name: "VA01".to_string(),
                    station: "VA01".to_string(),
                    position_itrf_m: [-1601185.4, -5041977.5, 3554875.9],
                },
                NormalizedAntenna {
                    antenna_id: 2,
                    name: "VA02".to_string(),
                    station: "VA02".to_string(),
                    position_itrf_m: [-1601085.4, -5041977.5, 3554875.9],
                },
            ],
            groups: vec![NormalizedGroup {
                cda_ids: vec![CdaId::Cda0],
                descriptor: SpectralDescriptor {
                    edge_frequency_hz: 1.4e9 - 0.5 * 5.0e7,
                    observed_frequency_hz: 1.4e9,
                    channel_width_hz: 5.0e7,
                    num_chan: 1,
                    if_conv_chain: 0,
                    rest_frequency_hz: 0.0,
                    rest_frame: FrequencyFrame::Topocentric,
                    doppler_definition: DopplerDefinition::Radio,
                    doppler_velocity_mps: 0.0,
                    doppler_tracking: false,
                },
                baselines: vec![NormalizedBaselineRow {
                    antenna1_archive: 0,
                    antenna2_archive: 1,
                    corr_types: vec![5, 6, 7, 8],
                    uvw_m: [10.0, 20.0, 30.0],
                    data: data.clone(),
                    corrected_data: data,
                    model_data: model,
                    flag,
                    flag_category,
                    weight,
                    sigma,
                }],
            }],
        })
        .unwrap();
    writer.finish().unwrap();

    let reopened = MeasurementSet::open(&ms_path).unwrap();
    assert_eq!(reopened.row_count(), 1);
    assert_eq!(reopened.antenna().unwrap().row_count(), 2);
    assert_eq!(reopened.field().unwrap().row_count(), 1);
    assert_eq!(reopened.observation().unwrap().row_count(), 1);
    assert_eq!(reopened.source().unwrap().row_count(), 1);
    assert_eq!(reopened.spectral_window().unwrap().row_count(), 1);
    assert_eq!(reopened.polarization().unwrap().row_count(), 1);
    assert_eq!(reopened.data_description().unwrap().row_count(), 1);

    let data_accessor = reopened
        .data_column(casa_ms::VisibilityDataColumn::Data)
        .unwrap();
    let data_column = data_accessor.get(0).unwrap();
    let ArrayValue::Complex32(array) = data_column else {
        panic!("expected Complex32 DATA column");
    };
    assert_eq!(array.shape(), &[4, 1]);
    assert_eq!(array[[0, 0]], Complex32::new(1.0, 0.0));
    assert_eq!(array[[3, 0]], Complex32::new(4.0, 0.0));
}

fn test_descriptor(observed_frequency_hz: f64) -> SpectralDescriptor {
    SpectralDescriptor {
        edge_frequency_hz: observed_frequency_hz - 0.5 * 5.0e7,
        observed_frequency_hz,
        channel_width_hz: 5.0e7,
        num_chan: 1,
        if_conv_chain: 0,
        rest_frequency_hz: 0.0,
        rest_frame: FrequencyFrame::Topocentric,
        doppler_definition: DopplerDefinition::Radio,
        doppler_velocity_mps: 0.0,
        doppler_tracking: false,
    }
}

fn test_antennas() -> Vec<NormalizedAntenna> {
    vec![
        NormalizedAntenna {
            antenna_id: 1,
            name: "VA01".to_string(),
            station: "VA01".to_string(),
            position_itrf_m: [-1601185.4, -5041977.5, 3554875.9],
        },
        NormalizedAntenna {
            antenna_id: 2,
            name: "VA02".to_string(),
            station: "VA02".to_string(),
            position_itrf_m: [-1601085.4, -5041977.5, 3554875.9],
        },
    ]
}

fn test_baseline() -> NormalizedBaselineRow {
    let data = ArrayValue::Complex32(
        ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![Complex32::new(1.0, 0.0); 4]).unwrap(),
    );
    let model = ArrayValue::Complex32(
        ArrayD::from_shape_vec(
            IxDyn(&[4, 1]).f(),
            vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(1.0, 0.0),
            ],
        )
        .unwrap(),
    );
    let flag =
        ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1]).f(), vec![false; 4]).unwrap());
    let flag_category =
        ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[4, 1, 1]).f(), vec![false; 4]).unwrap());
    let weight =
        ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());
    let sigma = ArrayValue::Float32(ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0; 4]).unwrap());

    NormalizedBaselineRow {
        antenna1_archive: 0,
        antenna2_archive: 1,
        corr_types: vec![5, 6, 7, 8],
        uvw_m: [10.0, 20.0, 30.0],
        data: data.clone(),
        corrected_data: data,
        model_data: model,
        flag,
        flag_category,
        weight,
        sigma,
    }
}

fn test_record(
    source_name: &str,
    time_seconds: f64,
    descriptor: SpectralDescriptor,
) -> NormalizedRecord {
    NormalizedRecord {
        project: "AG189".to_string(),
        source_name: source_name.to_string(),
        calibration_code: "".to_string(),
        source_num_lines: 1,
        source_direction: [1.0, 0.5],
        array_id: 0,
        time_seconds,
        integration_seconds: 10.0,
        direction_epoch: DirectionEpoch::J2000,
        antennas: test_antennas(),
        groups: vec![NormalizedGroup {
            cda_ids: vec![CdaId::Cda0],
            descriptor,
            baselines: vec![test_baseline()],
        }],
    }
}

fn main_scan_number(ms: &MeasurementSet, row: usize) -> i32 {
    let table_row = ms.main_table().row_accessor().row(row).unwrap();
    let Value::Scalar(ScalarValue::Int32(scan_number)) = table_row.get("SCAN_NUMBER").unwrap()
    else {
        panic!("expected SCAN_NUMBER scalar");
    };
    *scan_number
}

fn measure_ref_keyword(table: &Table, column: &str) -> String {
    let Some(keywords) = table.column_keywords(column) else {
        panic!("expected keywords for {column}");
    };
    let Some(Value::Record(measinfo)) = keywords.get("MEASINFO") else {
        panic!("expected MEASINFO for {column}");
    };
    let Some(Value::Scalar(ScalarValue::String(reference))) = measinfo.get("Ref") else {
        panic!("expected Ref string for {column}");
    };
    reference.clone()
}

fn measure_type_keyword(table: &Table, column: &str) -> String {
    let Some(keywords) = table.column_keywords(column) else {
        panic!("expected keywords for {column}");
    };
    let Some(Value::Record(measinfo)) = keywords.get("MEASINFO") else {
        panic!("expected MEASINFO for {column}");
    };
    let Some(Value::Scalar(ScalarValue::String(measure_type))) = measinfo.get("type") else {
        panic!("expected type string for {column}");
    };
    measure_type.clone()
}

fn quantum_units_keyword(table: &Table, column: &str) -> Vec<String> {
    let Some(keywords) = table.column_keywords(column) else {
        panic!("expected keywords for {column}");
    };
    let Some(Value::Array(ArrayValue::String(units))) = keywords.get("QuantumUnits") else {
        panic!("expected QuantumUnits for {column}");
    };
    units.iter().cloned().collect()
}

fn optional_i32_scalar(row: &RecordValue, column: &str) -> Option<i32> {
    match row.get(column) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Some(*value),
        None => None,
        Some(other) => panic!("{column} had unexpected value {other:?}"),
    }
}

#[test]
fn spectral_window_lookup_tries_the_cda_then_the_newest_match() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("overlapping-spw.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData)
        .with_optional_subtable(SubtableId::Source)
        .with_optional_subtable(SubtableId::Doppler);
    let ms = MeasurementSet::create(&path, builder).unwrap();
    let options = ImportVlaOptions {
        frequencytol_hz: 3.2e9,
        ..Default::default()
    };
    let mut writer = VlaImportState::new(options, ms, 5, 1).unwrap();
    for (index, (frequency, cda)) in [
        (8.5149e9, CdaId::Cda0),
        (4.8851e9, CdaId::Cda1),
        (7.8149e9, CdaId::Cda0),
        (8.5149e9, CdaId::Cda1),
        (7.8149e9, CdaId::Cda1),
    ]
    .into_iter()
    .enumerate()
    {
        let mut record = test_record(
            "AT166",
            4_281_838_800.0 + index as f64,
            test_descriptor(frequency),
        );
        record.groups[0].cda_ids[0] = cda;
        writer.push_record(record).unwrap();
    }
    writer.finish().unwrap();
    let reopened = MeasurementSet::open(&path).unwrap();
    assert_eq!(reopened.spectral_window().unwrap().row_count(), 2);
    for (row, expected) in [0, 1, 1, 0, 0].into_iter().enumerate() {
        let row = reopened.main_table().row_accessor().row(row).unwrap();
        assert_eq!(optional_i32_scalar(row, "DATA_DESC_ID"), Some(expected));
    }
}

#[test]
fn spectral_window_bandwidth_depends_on_the_channel_grid() {
    let mut narrow = test_descriptor(1.4e9);
    narrow.num_chan = 16;
    narrow.channel_width_hz = 50_000.0;
    let mut wide = narrow.clone();
    wide.channel_width_hz *= 2.0;
    assert!(!same_spectral_descriptor(&narrow, &wide, 1.0e9));
    assert!(!same_spectral_descriptor(&wide, &narrow, 1.0e9));
}

#[test]
fn original_version_keeps_correlation_and_channel_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flags.ms");
    let mut ms = MeasurementSet::create(&path, MeasurementSetBuilder::new()).unwrap();
    let flags = ArrayValue::Bool(
        ArrayD::from_shape_vec(
            IxDyn(&[2, 3]).f(),
            vec![true, false, false, true, false, false],
        )
        .unwrap(),
    );
    ms.main_table_mut()
        .add_row(
            make_row_from_columns(
                casa_ms::schema::main_table::REQUIRED_COLUMNS,
                &[
                    ("FLAG", Value::Array(flags.clone())),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(true))),
                ],
            )
            .unwrap(),
        )
        .unwrap();
    ms.save().unwrap();
    let source = MeasurementSet::open(&path).unwrap();
    casa_flagging::save_initial_flag_version(
        &source,
        vec![MeasurementSetArrayShapePlan::visibility(2, 3, 1, "VLA")],
    )
    .unwrap();
    let snapshot = Table::open(casa_tables::TableOptions::new(
        dir.path().join("flags.ms.flagversions/flags.Original"),
    ))
    .unwrap();
    let row = snapshot.row_accessor().row(0).unwrap();
    assert_eq!(row.get("FLAG"), Some(&Value::Array(flags)));
    assert_eq!(
        row.get("FLAG_ROW"),
        Some(&Value::Scalar(ScalarValue::Bool(true)))
    );
}

#[test]
fn skipped_records_force_new_scan_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("skip-scan.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData)
        .with_optional_subtable(SubtableId::Source)
        .with_optional_subtable(SubtableId::Doppler);
    let ms = MeasurementSet::create(&ms_path, builder).unwrap();
    let mut writer = VlaImportState::new(ImportVlaOptions::default(), ms, 3, 1).unwrap();
    set_flag_category_names(writer.ms.main_table_mut()).unwrap();

    let descriptor = test_descriptor(1.4e9);
    writer
        .push_record(test_record("0836+710", 5_097_600_000.0, descriptor.clone()))
        .unwrap();
    writer
        .push_record(test_record("0836+710", 5_097_600_010.0, descriptor.clone()))
        .unwrap();
    writer.note_skipped_record();
    writer
        .push_record(test_record("0836+710", 5_097_600_020.0, descriptor))
        .unwrap();
    writer.finish().unwrap();

    let reopened = MeasurementSet::open(&ms_path).unwrap();
    assert_eq!(main_scan_number(&reopened, 0), 1);
    assert_eq!(main_scan_number(&reopened, 1), 1);
    assert_eq!(main_scan_number(&reopened, 2), 2);
}

#[test]
fn data_description_changes_force_new_scan_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("dd-scan.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData)
        .with_optional_subtable(SubtableId::Source)
        .with_optional_subtable(SubtableId::Doppler);
    let ms = MeasurementSet::create(&ms_path, builder).unwrap();
    let mut writer = VlaImportState::new(ImportVlaOptions::default(), ms, 3, 1).unwrap();
    set_flag_category_names(writer.ms.main_table_mut()).unwrap();

    writer
        .push_record(test_record(
            "0836+710",
            5_097_600_000.0,
            test_descriptor(1.4e9),
        ))
        .unwrap();
    writer
        .push_record(test_record(
            "0836+710",
            5_097_600_010.0,
            test_descriptor(1.4e9),
        ))
        .unwrap();
    writer
        .push_record(test_record(
            "0836+710",
            5_097_600_020.0,
            test_descriptor(1.5e9),
        ))
        .unwrap();
    writer.finish().unwrap();

    let reopened = MeasurementSet::open(&ms_path).unwrap();
    assert_eq!(main_scan_number(&reopened, 0), 1);
    assert_eq!(main_scan_number(&reopened, 1), 1);
    assert_eq!(main_scan_number(&reopened, 2), 2);
}

#[test]
fn spectral_windows_match_across_frequency_reference_frames() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("frame-match.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData)
        .with_optional_subtable(SubtableId::Source)
        .with_optional_subtable(SubtableId::Doppler);
    let ms = MeasurementSet::create(&ms_path, builder).unwrap();
    let mut writer = VlaImportState::new(ImportVlaOptions::default(), ms, 2, 1).unwrap();
    set_flag_category_names(writer.ms.main_table_mut()).unwrap();

    let topo_descriptor = SpectralDescriptor {
        edge_frequency_hz: 23_691_048_000.0 - 127.0 / 2.0 * 24_414.062_5,
        observed_frequency_hz: 23_691_048_000.0,
        channel_width_hz: 24_414.062_5,
        num_chan: 127,
        if_conv_chain: 0,
        rest_frequency_hz: 0.0,
        rest_frame: FrequencyFrame::Topocentric,
        doppler_definition: DopplerDefinition::Radio,
        doppler_velocity_mps: 0.0,
        doppler_tracking: false,
    };
    let source_direction = [1.0, 0.5];
    let time_seconds = 4_558_040_657.5;
    let frame = spectral_conversion_frame(
        time_seconds,
        source_direction,
        DirectionEpoch::J2000,
        test_measures(),
    )
    .unwrap();
    let lsrk_frequency_hz = convert_topocentric_frequency(
        topo_descriptor.observed_frequency_hz,
        FrequencyRef::LSRK,
        &frame,
    )
    .unwrap();
    let lsrk_descriptor = SpectralDescriptor {
        edge_frequency_hz: topo_descriptor.edge_frequency_hz,
        observed_frequency_hz: topo_descriptor.observed_frequency_hz,
        channel_width_hz: topo_descriptor.channel_width_hz,
        num_chan: topo_descriptor.num_chan,
        if_conv_chain: topo_descriptor.if_conv_chain,
        rest_frequency_hz: lsrk_frequency_hz,
        rest_frame: FrequencyFrame::Lsrk,
        doppler_definition: DopplerDefinition::Radio,
        doppler_velocity_mps: 0.0,
        doppler_tracking: true,
    };

    let mut topo_record = test_record("0530+135", time_seconds, topo_descriptor);
    topo_record.direction_epoch = DirectionEpoch::J2000;
    topo_record.source_direction = source_direction;
    let mut lsrk_record = test_record("05309+13319", time_seconds + 475.0, lsrk_descriptor);
    lsrk_record.direction_epoch = DirectionEpoch::J2000;
    lsrk_record.source_direction = source_direction;

    writer.push_record(topo_record).unwrap();
    writer.push_record(lsrk_record).unwrap();
    writer.finish().unwrap();

    let reopened = MeasurementSet::open(&ms_path).unwrap();
    assert_eq!(reopened.spectral_window().unwrap().row_count(), 1);
    assert_eq!(reopened.data_description().unwrap().row_count(), 1);
    let dd0 = reopened
        .main_table()
        .row_accessor()
        .row(0)
        .unwrap()
        .get("DATA_DESC_ID")
        .unwrap()
        .clone();
    let dd1 = reopened
        .main_table()
        .row_accessor()
        .row(1)
        .unwrap()
        .get("DATA_DESC_ID")
        .unwrap()
        .clone();
    assert_eq!(dd0, dd1);
}

#[test]
fn spectral_conversion_frame_uses_tai_like_casa_vlafiller() {
    let time_seconds = 4_558_025_017.500007;
    let source_direction = [1.4439993710957795, 0.23617770712489528];
    let frame = spectral_conversion_frame(
        time_seconds,
        source_direction,
        DirectionEpoch::J2000,
        test_measures(),
    )
    .unwrap();
    let converted =
        convert_topocentric_frequency(23_689_651_393.610_19, FrequencyRef::LSRK, &frame).unwrap();
    assert!(
        (converted - 23_692_506_802.643_28).abs() < 1.0e-2,
        "converted={converted}"
    );
}

#[test]
fn importer_marks_vla_epoch_columns_as_tai() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("tai-metadata.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData)
        .with_optional_subtable(SubtableId::Source)
        .with_optional_subtable(SubtableId::Doppler);
    let ms = MeasurementSet::create(&ms_path, builder).unwrap();
    let mut writer = VlaImportState::new(ImportVlaOptions::default(), ms, 1, 1).unwrap();
    set_flag_category_names(writer.ms.main_table_mut()).unwrap();
    writer
        .push_record(test_record(
            "0836+710",
            5_097_600_000.0,
            test_descriptor(1.4e9),
        ))
        .unwrap();
    writer.finish().unwrap();

    let reopened = MeasurementSet::open(&ms_path).unwrap();
    assert_eq!(measure_ref_keyword(reopened.main_table(), "TIME"), "TAI");
    assert_eq!(
        measure_ref_keyword(reopened.main_table(), "TIME_CENTROID"),
        "TAI"
    );
    assert_eq!(measure_ref_keyword(reopened.main_table(), "UVW"), "J2000");
    assert_eq!(
        measure_ref_keyword(reopened.field().unwrap().table(), "TIME"),
        "TAI"
    );
    assert_eq!(
        measure_ref_keyword(reopened.feed().unwrap().table(), "TIME"),
        "TAI"
    );
    assert_eq!(
        measure_ref_keyword(reopened.source().unwrap().table(), "TIME"),
        "TAI"
    );
    assert_eq!(
        measure_ref_keyword(reopened.observation().unwrap().table(), "TIME_RANGE"),
        "TAI"
    );
    assert_eq!(
        measure_ref_keyword(reopened.observation().unwrap().table(), "RELEASE_DATE"),
        "TAI"
    );
}

#[test]
fn importer_persists_measure_types_refs_and_units_for_vla_columns() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("measure-keywords.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData)
        .with_optional_subtable(SubtableId::Source)
        .with_optional_subtable(SubtableId::Doppler);
    let ms = MeasurementSet::create(&ms_path, builder).unwrap();
    let mut writer = VlaImportState::new(ImportVlaOptions::default(), ms, 1, 1).unwrap();
    set_flag_category_names(writer.ms.main_table_mut()).unwrap();
    writer
        .push_record(test_record(
            "0836+710",
            5_097_600_000.0,
            test_descriptor(1.4e9),
        ))
        .unwrap();
    writer.finish().unwrap();

    let reopened = MeasurementSet::open(&ms_path).unwrap();
    assert_eq!(measure_type_keyword(reopened.main_table(), "TIME"), "epoch");
    assert_eq!(measure_type_keyword(reopened.main_table(), "UVW"), "uvw");
    assert_eq!(measure_ref_keyword(reopened.main_table(), "UVW"), "J2000");
    assert_eq!(
        quantum_units_keyword(reopened.main_table(), "UVW"),
        vec!["m".to_string(), "m".to_string(), "m".to_string()]
    );

    let field = reopened.field().unwrap();
    for column in ["DELAY_DIR", "PHASE_DIR", "REFERENCE_DIR"] {
        assert_eq!(measure_type_keyword(field.table(), column), "direction");
        assert_eq!(measure_ref_keyword(field.table(), column), "J2000");
        assert_eq!(
            quantum_units_keyword(field.table(), column),
            vec!["rad".to_string(), "rad".to_string()]
        );
    }

    let feed = reopened.feed().unwrap();
    assert_eq!(
        measure_type_keyword(feed.table(), "BEAM_OFFSET"),
        "direction"
    );
    assert_eq!(measure_ref_keyword(feed.table(), "BEAM_OFFSET"), "J2000");
    assert_eq!(
        quantum_units_keyword(feed.table(), "BEAM_OFFSET"),
        vec!["rad".to_string(), "rad".to_string()]
    );

    let antenna = reopened.antenna().unwrap();
    assert_eq!(
        measure_type_keyword(antenna.table(), "POSITION"),
        "position"
    );
    assert_eq!(measure_ref_keyword(antenna.table(), "POSITION"), "ITRF");
    assert_eq!(
        quantum_units_keyword(antenna.table(), "POSITION"),
        vec!["m".to_string(), "m".to_string(), "m".to_string()]
    );

    let observation = reopened.observation().unwrap();
    assert_eq!(
        measure_type_keyword(observation.table(), "TIME_RANGE"),
        "epoch"
    );
    assert_eq!(
        quantum_units_keyword(observation.table(), "TIME_RANGE"),
        vec!["s".to_string()]
    );

    let source = reopened.source().unwrap();
    assert_eq!(
        measure_type_keyword(source.table(), "DIRECTION"),
        "direction"
    );
    assert_eq!(measure_ref_keyword(source.table(), "DIRECTION"), "J2000");
    assert_eq!(
        quantum_units_keyword(source.table(), "DIRECTION"),
        vec!["rad".to_string(), "rad".to_string()]
    );

    let spw = reopened.spectral_window().unwrap();
    for column in ["REF_FREQUENCY", "CHAN_FREQ"] {
        assert_eq!(measure_type_keyword(spw.table(), column), "frequency");
    }
    for column in [
        "CHAN_WIDTH",
        "EFFECTIVE_BW",
        "RESOLUTION",
        "TOTAL_BANDWIDTH",
        "REF_FREQUENCY",
        "CHAN_FREQ",
    ] {
        assert_eq!(
            quantum_units_keyword(spw.table(), column),
            vec!["Hz".to_string()]
        );
    }
}

#[test]
fn importer_persists_spw_doppler_id_for_doppler_tracked_rows() {
    let dir = tempfile::tempdir().unwrap();
    let ms_path = dir.path().join("spw-doppler-id.ms");
    let builder = MeasurementSetBuilder::new()
        .with_main_column(OptionalMainColumn::Data)
        .with_main_column(OptionalMainColumn::CorrectedData)
        .with_main_column(OptionalMainColumn::ModelData)
        .with_optional_subtable(SubtableId::Source)
        .with_optional_subtable(SubtableId::Doppler);
    let ms = MeasurementSet::create(&ms_path, builder).unwrap();
    let mut writer = VlaImportState::new(ImportVlaOptions::default(), ms, 1, 1).unwrap();
    set_flag_category_names(writer.ms.main_table_mut()).unwrap();
    writer
        .push_record(test_record(
            "0530+135",
            4_558_025_017.500007,
            SpectralDescriptor {
                edge_frequency_hz: 23_689_639_186.578_94,
                observed_frequency_hz: 23_691_189_479.547_69,
                channel_width_hz: 24_414.062_5,
                num_chan: 127,
                if_conv_chain: 0,
                rest_frequency_hz: 23_694_044_992.921_69,
                rest_frame: FrequencyFrame::Lsrk,
                doppler_definition: DopplerDefinition::Radio,
                doppler_velocity_mps: 5_700.0,
                doppler_tracking: true,
            },
        ))
        .unwrap();
    writer.finish().unwrap();

    let reopened = MeasurementSet::open(&ms_path).unwrap();
    let spw = reopened.spectral_window().unwrap();
    let spw_table = spw.table();
    assert!(
        spw_table
            .schema()
            .is_some_and(|schema| schema.contains_column("DOPPLER_ID"))
    );
    let spw_row = spw_table.row_accessor().row(0).unwrap();
    assert_eq!(optional_i32_scalar(spw_row, "DOPPLER_ID"), Some(0));
}

#[test]
fn rejects_mixed_direction_epochs_until_epoch_conversion_is_implemented() {
    let input = [1.0, 0.5];
    let error = align_direction_for_epoch(input, DirectionEpoch::B1950Vla, DirectionEpoch::J2000)
        .unwrap_err();
    assert!(
        error.to_string().contains("mixed direction epochs"),
        "{error}"
    );
}

#[test]
fn wraps_source_longitudes_into_signed_range() {
    let wrapped = normalize_longitude_radians(2.0 * PI - 0.25);
    assert!((wrapped + 0.25).abs() < 1.0e-12, "{wrapped}");
}

#[test]
fn formats_spectral_window_names_like_casa() {
    let descriptor = SpectralDescriptor {
        edge_frequency_hz: 7.8149e9 - 0.5 * 50.0e6,
        observed_frequency_hz: 7.8149e9,
        channel_width_hz: 50.0e6,
        num_chan: 1,
        if_conv_chain: 0,
        rest_frequency_hz: 0.0,
        rest_frame: FrequencyFrame::Topocentric,
        doppler_definition: DopplerDefinition::Radio,
        doppler_velocity_mps: 0.0,
        doppler_tracking: false,
    };
    assert_eq!(
        spectral_window_name(
            &descriptor,
            descriptor.observed_frequency_hz,
            FrequencyRef::TOPO
        ),
        "1*50 MHz channels @ 7.81 GHz (TOPO)"
    );
}

#[test]
fn helper_paths_cover_import_options_arrays_and_frequency_metadata() {
    let mut options = ImportVlaOptions {
        archivefiles: vec![PathBuf::from("input.xp1")],
        ..ImportVlaOptions::default()
    };
    assert!(ArchiveSelection::new(&options).is_ok());
    options.bandname = Some(crate::BandName::L);
    options.starttime = Some("01-Jan-1985/00:00:00".to_string());
    options.stoptime = Some("01-Jan-1985/01:00:00".to_string());
    options.evlabands = true;
    assert!(ArchiveSelection::new(&options).is_ok());
    options.stoptime = Some("31-Dec-1984/00:00:00".to_string());
    assert!(matches!(
        ArchiveSelection::new(&options),
        Err(VlaError::InvalidArgument {
            argument: "stoptime",
            ..
        })
    ));

    assert_eq!(
        [
            frequency_reference_for_frame(FrequencyFrame::Topocentric),
            frequency_reference_for_frame(FrequencyFrame::Geocentric),
            frequency_reference_for_frame(FrequencyFrame::Barycentric),
            frequency_reference_for_frame(FrequencyFrame::Lsrk),
        ],
        [
            FrequencyRef::TOPO,
            FrequencyRef::GEO,
            FrequencyRef::BARY,
            FrequencyRef::LSRK,
        ]
    );
    assert_eq!(
        doppler_reference_for_definition(DopplerDefinition::Optical),
        DopplerRef::Z
    );
    assert_eq!(
        doppler_reference_for_definition(DopplerDefinition::Unknown),
        DopplerRef::RADIO
    );
    assert_eq!(
        direction_reference_for_epoch(DirectionEpoch::Unknown(17)),
        DirectionRef::J2000
    );
    assert_eq!(format_sigfigs(0.0, 3), "0");
    assert_eq!(format_sigfigs(12_345.0, 3), "12345");
    assert_eq!(format_sigfigs(12.340, 4), "12.34");
    assert_eq!(
        spectral_window_name(&test_descriptor(327.5e6), 327.5e6, FrequencyRef::TOPO),
        "1*50 MHz channels @ 328 MHz (TOPO)"
    );

    assert_eq!(
        [
            stokes_code(StokesProduct::Rr),
            stokes_code(StokesProduct::Rl)
        ],
        [5, 6]
    );
    assert_eq!(
        [
            stokes_code(StokesProduct::Lr),
            stokes_code(StokesProduct::Ll)
        ],
        [7, 8]
    );
    assert_eq!(
        build_model_data(&[5, 6, 7, 8], 2),
        vec![
            Complex32::new(1.0, 0.0),
            Complex32::new(0.0, 0.0),
            Complex32::new(0.0, 0.0),
            Complex32::new(1.0, 0.0),
            Complex32::new(1.0, 0.0),
            Complex32::new(0.0, 0.0),
            Complex32::new(0.0, 0.0),
            Complex32::new(1.0, 0.0),
        ]
    );
    assert_eq!(flag_category_index(1, 2, 3, 4, 5), 69);

    let (weight, sigma) = visibility_weight_sigma(1.0e6, 10.0, 4.0, true, false);
    assert!((weight - 30.0).abs() < 1.0e-6);
    assert!((sigma - (1.0_f32 / 120.0_f32.sqrt() * 2.0)).abs() < 1.0e-6);
    assert_eq!(
        visibility_weight_sigma(0.0, 10.0, 1.0, true, false),
        (0.0, 0.0)
    );

    let all_flagged =
        ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![true, true]).unwrap());
    let partly_flagged =
        ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), vec![true, false]).unwrap());
    assert!(flag_row_value(&all_flagged).unwrap());
    assert!(!flag_row_value(&partly_flagged).unwrap());
    assert!(
        flag_row_value(&ArrayValue::Float32(
            ArrayD::from_shape_vec(IxDyn(&[1]).f(), vec![1.0_f32]).unwrap()
        ))
        .is_err()
    );

    let weights = ArrayValue::Float32(
        ArrayD::from_shape_vec(IxDyn(&[4]).f(), vec![1.0, 2.0, 3.0, 4.0]).unwrap(),
    );
    let reordered = reorder_vector_array(&weights, &[5, 6, 7, 8], &[5, 7, 6, 8]).unwrap();
    let ArrayValue::Float32(reordered) = reordered else {
        panic!("expected float vector");
    };
    assert_eq!(
        reordered.iter().copied().collect::<Vec<_>>(),
        vec![1.0, 3.0, 2.0, 4.0]
    );
    assert!(reorder_vector_array(&all_flagged, &[5], &[5]).is_err());
    assert!(reorder_index_map(&[5, 6], &[5, 7], "test").is_err());

    let flags = ArrayValue::Bool(
        ArrayD::from_shape_vec(IxDyn(&[2, 2]).f(), vec![false, true, true, false]).unwrap(),
    );
    let reordered_flags = reorder_bool_array_generic(&flags, &[5, 8], &[8, 5], "flag").unwrap();
    let ArrayValue::Bool(reordered_flags) = reordered_flags else {
        panic!("expected bool flags");
    };
    assert_eq!(
        reordered_flags.iter().copied().collect::<Vec<_>>(),
        vec![true, false, false, true]
    );
    assert!(reorder_bool_array_generic(&weights, &[5], &[5], "flag").is_err());

    let corr_product = corr_product_array(&[5, 6, 7, 8]).unwrap();
    let ArrayValue::Int32(corr_product) = corr_product else {
        panic!("expected int corr product");
    };
    assert_eq!(
        corr_product.iter().copied().collect::<Vec<_>>(),
        vec![0, 0, 1, 1, 0, 1, 0, 1]
    );
    assert!(corr_product_array(&[42]).is_err());
}

#[test]
fn inspect_real_archive_target_row_when_configured() {
    let Some(path) = std::env::var_os("CASA_RS_IMPORTVLA_ARCHIVE").map(PathBuf::from) else {
        eprintln!("skipping: CASA_RS_IMPORTVLA_ARCHIVE not set");
        return;
    };
    if !path.exists() {
        eprintln!("skipping: {} does not exist", path.display());
        return;
    }

    let target_row = std::env::var("CASA_RS_IMPORTVLA_TARGET_ROW")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(25_726);

    let options = ImportVlaOptions {
        archivefiles: vec![path.clone()],
        ..ImportVlaOptions::default()
    };
    let selection = ArchiveSelection::new(&options).expect("archive selection");
    let mut reader = VlaDiskReader::open(&path).expect("open archive");
    let mut row_base = 0usize;
    let mut logical_record_index = 0usize;
    let measures = test_measures();
    let mut frequency_tolerance = options.frequencytol_hz;

    while let Some(record) = reader.next_record().expect("read logical record") {
        let Some(normalized) = normalize_record(
            &record,
            &options,
            &selection,
            measures.as_ref(),
            &mut frequency_tolerance,
        )
        .expect("normalize record") else {
            logical_record_index += 1;
            continue;
        };

        let grouped_cdas = grouped_cdas_for_test(&record, options.frequencytol_hz)
            .expect("group CDAs for diagnostic");
        assert_eq!(grouped_cdas.len(), normalized.groups.len());

        for (group_index, (group, (_, cda_ids))) in normalized
            .groups
            .iter()
            .zip(grouped_cdas.iter())
            .enumerate()
        {
            let row_end = row_base + group.baselines.len();
            if !(row_base..row_end).contains(&target_row) {
                row_base = row_end;
                continue;
            }

            let baseline_index = target_row - row_base;
            let baseline = &group.baselines[baseline_index];
            let ant1 = baseline.antenna1_archive;
            let ant2 = baseline.antenna2_archive;

            eprintln!(
                "target_row={target_row} logical_record_index={logical_record_index} group_index={group_index} baseline_index={baseline_index}"
            );
            eprintln!(
                "archive_baseline=({ant1},{ant2}) corr_types={:?} descriptor={:?}",
                baseline.corr_types, group.descriptor
            );
            eprintln!(
                "archive_antenna_names=({}, {})",
                normalized.antennas[ant1].name, normalized.antennas[ant2].name
            );
            eprintln!(
                "global_flag={:?}",
                bool_matrix(&baseline.flag).expect("flag matrix")
            );
            eprintln!(
                "global_flag_category={:?}",
                bool_cube(&baseline.flag_category).expect("flag cube")
            );

            for &cda_id in cda_ids {
                let global_stokes = record
                    .stokes_products(cda_id, 0, 0)
                    .expect("global stokes products");
                let pair_stokes = record
                    .stokes_products(cda_id, ant1, ant2)
                    .expect("pair stokes products");
                let if_usage = record
                    .sda()
                    .expect("SDA")
                    .if_usage(cda_id)
                    .expect("IF usage");
                eprintln!(
                    "cda={cda_id:?} if_usage={if_usage:?} global_stokes={global_stokes:?} pair_stokes={pair_stokes:?}"
                );
            }

            let ada1 = record.ada(ant1).expect("decode ant1 ADA");
            let ada2 = record.ada(ant2).expect("decode ant2 ADA");
            eprintln!(
                "ant1_pols A={:?} B={:?} C={:?} D={:?}",
                ada1.if_polarization(IfId::A).unwrap(),
                ada1.if_polarization(IfId::B).unwrap(),
                ada1.if_polarization(IfId::C).unwrap(),
                ada1.if_polarization(IfId::D).unwrap()
            );
            eprintln!(
                "ant2_pols A={:?} B={:?} C={:?} D={:?}",
                ada2.if_polarization(IfId::A).unwrap(),
                ada2.if_polarization(IfId::B).unwrap(),
                ada2.if_polarization(IfId::C).unwrap(),
                ada2.if_polarization(IfId::D).unwrap()
            );
            eprintln!(
                "ant1_status A={} B={} C={} D={}",
                ada1.if_status(IfId::A).unwrap(),
                ada1.if_status(IfId::B).unwrap(),
                ada1.if_status(IfId::C).unwrap(),
                ada1.if_status(IfId::D).unwrap()
            );
            eprintln!(
                "ant2_status A={} B={} C={} D={}",
                ada2.if_status(IfId::A).unwrap(),
                ada2.if_status(IfId::B).unwrap(),
                ada2.if_status(IfId::C).unwrap(),
                ada2.if_status(IfId::D).unwrap()
            );
            return;
        }

        logical_record_index += 1;
    }

    panic!("target row {target_row} not found in archive");
}

fn grouped_cdas_for_test(
    record: &LogicalRecord,
    tolerance_hz: f64,
) -> Result<Vec<(SpectralDescriptor, Vec<CdaId>)>, VlaError> {
    let rca = record.rca();
    let sda = record
        .sda()
        .map_err(|error| VlaError::import(format!("decode SDA: {error}")))?;
    let mut cda_groups: Vec<(SpectralDescriptor, Vec<CdaId>)> = Vec::new();

    for cda_id in [CdaId::Cda0, CdaId::Cda1, CdaId::Cda2, CdaId::Cda3] {
        if rca
            .cda_offset_bytes(cda_id.index())
            .map_err(|error| VlaError::import(format!("decode CDA offset {:?}: {error}", cda_id)))?
            == 0
        {
            continue;
        }
        let cda = record
            .cda(cda_id)
            .map_err(|error| VlaError::import(format!("decode CDA {:?}: {error}", cda_id)))?;
        if !cda.is_valid() {
            continue;
        }
        if sda
            .n_polarizations(cda_id)
            .map_err(|error| VlaError::import(format!("decode CDA polarization count: {error}")))?
            == 0
        {
            continue;
        }

        let descriptor = SpectralDescriptor {
            edge_frequency_hz: sda.edge_frequency_hz(cda_id).map_err(|error| {
                VlaError::import(format!("decode edge frequency for {:?}: {error}", cda_id))
            })?,
            observed_frequency_hz: sda.observed_frequency_hz(cda_id).map_err(|error| {
                VlaError::import(format!(
                    "decode observed frequency for {:?}: {error}",
                    cda_id
                ))
            })?,
            channel_width_hz: sda.channel_width_hz(cda_id).map_err(|error| {
                VlaError::import(format!("decode channel width for {:?}: {error}", cda_id))
            })?,
            num_chan: usize::try_from(sda.n_channels(cda_id).map_err(|error| {
                VlaError::import(format!("decode channel count for {:?}: {error}", cda_id))
            })?)
            .map_err(|_| VlaError::import("channel count does not fit in usize"))?,
            if_conv_chain: i32::try_from(sda.electronic_path(cda_id).map_err(|error| {
                VlaError::import(format!("decode IF chain for {:?}: {error}", cda_id))
            })?)
            .map_err(|_| VlaError::import("IF conversion chain does not fit in i32"))?,
            rest_frequency_hz: sda.rest_frequency_hz(cda_id).map_err(|error| {
                VlaError::import(format!("decode rest frequency for {:?}: {error}", cda_id))
            })?,
            rest_frame: sda.rest_frame(cda_id).map_err(|error| {
                VlaError::import(format!("decode rest frame for {:?}: {error}", cda_id))
            })?,
            doppler_definition: sda.doppler_definition(cda_id).map_err(|error| {
                VlaError::import(format!(
                    "decode doppler definition for {:?}: {error}",
                    cda_id
                ))
            })?,
            doppler_velocity_mps: sda.radial_velocity_mps(cda_id).map_err(|error| {
                VlaError::import(format!("decode radial velocity for {:?}: {error}", cda_id))
            })?,
            doppler_tracking: sda.doppler_tracking(cda_id).map_err(|error| {
                VlaError::import(format!("decode doppler tracking for {:?}: {error}", cda_id))
            })?,
        };

        if let Some((_, cdas)) = cda_groups
            .iter_mut()
            .find(|(existing, _)| same_spectral_descriptor(existing, &descriptor, tolerance_hz))
        {
            cdas.push(cda_id);
        } else {
            cda_groups.push((descriptor, vec![cda_id]));
        }
    }

    Ok(cda_groups)
}

fn bool_matrix(value: &ArrayValue) -> Result<Vec<Vec<bool>>, VlaError> {
    let ArrayValue::Bool(array) = value else {
        return Err(VlaError::import("expected Bool matrix"));
    };
    let shape = array.shape();
    let n_corr = shape[0];
    let n_chan = shape[1];
    Ok((0..n_corr)
        .map(|corr| (0..n_chan).map(|chan| array[[corr, chan]]).collect())
        .collect())
}

fn bool_cube(value: &ArrayValue) -> Result<Vec<Vec<Vec<bool>>>, VlaError> {
    let ArrayValue::Bool(array) = value else {
        return Err(VlaError::import("expected Bool cube"));
    };
    let shape = array.shape();
    let n_corr = shape[0];
    let n_chan = shape[1];
    let n_cat = shape[2];
    Ok((0..n_corr)
        .map(|corr| {
            (0..n_chan)
                .map(|chan| (0..n_cat).map(|cat| array[[corr, chan, cat]]).collect())
                .collect()
        })
        .collect())
}

mod spectral_windows;
