// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_ms::{
    MeasurementSet, MeasurementSetColumnStorage, MeasurementSetColumnWriteMode,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession, MsTransformRequest, SubTable,
    TransformDataColumn, mstransform, schema::main_table::VisibilityDataColumn,
    selection::MsSelection,
};
use casa_types::{ArrayValue, ScalarValue};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

#[test]
fn mstransform_selects_channels_updates_metadata_and_weight_spectrum() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let output_ms = dir.path().join("transformed.ms");

    let report = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "0:2~5".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: true,
    })
    .expect("mstransform");

    assert_eq!(report.row_count, 4);
    assert_eq!(report.source_column, "DATA");
    assert_eq!(report.output_column, "DATA");
    assert_eq!(report.spectral_window_ids, vec![0]);
    assert_eq!(report.output_channels_by_spw[&0], 4);

    let output = MeasurementSet::open(&output_ms).expect("open transformed MS");
    assert_eq!(output.row_count(), 4);
    let data = output
        .data_column(VisibilityDataColumn::Data)
        .expect("DATA column");
    let row = data.get(1).expect("row 1 DATA");
    let ArrayValue::Complex32(row) = row else {
        panic!("expected Complex32 DATA");
    };
    assert_eq!(row.shape(), &[common::NUM_CORR, 4]);
    for corr in 0..common::NUM_CORR {
        for (out_chan, source_chan) in (2..=5).enumerate() {
            let expected_index =
                common::NUM_CORR * common::NUM_CHAN + source_chan * common::NUM_CORR + corr;
            assert_eq!(
                row[[corr, out_chan]],
                casa_types::Complex32::new(expected_index as f32, -(expected_index as f32) * 0.5)
            );
        }
    }

    let flags = output
        .main_table()
        .cell_accessor(0, "FLAG")
        .and_then(|cell| cell.array())
        .expect("FLAG row");
    assert_eq!(flags.shape(), &[common::NUM_CORR, 4]);
    let weights = output
        .main_table()
        .cell_accessor(1, "WEIGHT_SPECTRUM")
        .and_then(|cell| cell.array())
        .expect("WEIGHT_SPECTRUM row");
    let ArrayValue::Float32(weights) = weights else {
        panic!("expected Float32 WEIGHT_SPECTRUM");
    };
    assert_eq!(weights.shape(), &[common::NUM_CORR, 4]);
    assert_eq!(weights[[0, 0]], 2008.0);
    assert_eq!(weights[[3, 3]], 2107.0);

    let spw = output.spectral_window().expect("SPECTRAL_WINDOW");
    assert_eq!(spw.num_chan(0).expect("NUM_CHAN"), 4);
    assert_eq!(
        spw.chan_freq(0).expect("CHAN_FREQ"),
        vec![1.002e9, 1.003e9, 1.004e9, 1.005e9]
    );
    let spw_table = spw.table();
    assert_eq!(
        spw_table
            .cell_accessor(0, "REF_FREQUENCY")
            .and_then(|cell| cell.scalar())
            .expect("REF_FREQUENCY"),
        &ScalarValue::Float64(1.002e9)
    );
    assert_eq!(
        spw_table
            .cell_accessor(0, "TOTAL_BANDWIDTH")
            .and_then(|cell| cell.scalar())
            .expect("TOTAL_BANDWIDTH"),
        &ScalarValue::Float64(4.0e6)
    );
}

#[test]
fn mstransform_filters_rows_by_selected_spw_and_preserves_time_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_averaging_fixture_ms(dir.path());
    let output_ms = dir.path().join("spw1.ms");

    let report = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "1:0~5".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new().field(&[0]),
        keep_flags: true,
    })
    .expect("mstransform spw 1");

    assert_eq!(report.row_count, 3);
    assert_eq!(report.spectral_window_ids, vec![1]);
    assert_eq!(report.output_channels_by_spw[&1], 6);

    let output = MeasurementSet::open(&output_ms).expect("open transformed MS");
    let data = output
        .data_column(VisibilityDataColumn::Data)
        .expect("DATA column");
    for row_index in 0..output.row_count() {
        assert_eq!(data.shape(row_index).expect("DATA shape"), vec![4, 6]);
        assert_eq!(
            output
                .main_table()
                .cell_accessor(row_index, "DATA_DESC_ID")
                .and_then(|cell| cell.scalar())
                .expect("DATA_DESC_ID"),
            &ScalarValue::Int32(0)
        );
    }
    let spw = output.spectral_window().expect("SPECTRAL_WINDOW");
    assert_eq!(spw.row_count(), 1);
    assert_eq!(spw.num_chan(0).expect("NUM_CHAN"), 6);
    let data_description = output.data_description().expect("DATA_DESCRIPTION");
    assert_eq!(data_description.row_count(), 1);
    assert_eq!(
        data_description
            .spectral_window_id(0)
            .expect("compact DATA_DESCRIPTION SPW"),
        0
    );
    assert_eq!(
        output
            .main_table()
            .cell_accessor(0, "TIME")
            .and_then(|cell| cell.scalar())
            .expect("first TIME"),
        &ScalarValue::Float64(common::TIME_BASE_SECONDS)
    );
    assert_eq!(
        output
            .main_table()
            .cell_accessor(1, "TIME")
            .and_then(|cell| cell.scalar())
            .expect("second TIME"),
        &ScalarValue::Float64(common::TIME_BASE_SECONDS)
    );
    assert_eq!(
        output
            .main_table()
            .cell_accessor(2, "TIME")
            .and_then(|cell| cell.scalar())
            .expect("third TIME"),
        &ScalarValue::Float64(common::TIME_BASE_SECONDS + 30.0)
    );
}

#[test]
fn mstransform_compacts_selected_field_table_and_remaps_main_field_ids() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_averaging_fixture_ms(dir.path());
    let output_ms = dir.path().join("field1.ms");

    let report = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "0".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new().field(&[1]),
        keep_flags: true,
    })
    .expect("mstransform field 1");

    assert_eq!(report.row_count, 1);
    let output = MeasurementSet::open(&output_ms).expect("open transformed MS");
    assert_eq!(output.row_count(), 1);
    let field = output.field().expect("FIELD");
    assert_eq!(field.row_count(), 1);
    assert_eq!(field.name(0).expect("field name"), "FIELD1");
    assert_eq!(
        output
            .main_table()
            .cell_accessor(0, "FIELD_ID")
            .and_then(|cell| cell.scalar())
            .expect("FIELD_ID"),
        &ScalarValue::Int32(0)
    );
}

#[test]
fn mstransform_width_averages_selected_channels_and_metadata() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let output_ms = dir.path().join("averaged.ms");

    let report = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "0:2~5".to_string(),
        width: 2,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: true,
    })
    .expect("mstransform width");

    assert_eq!(report.output_channels_by_spw[&0], 2);
    assert_eq!(report.width, 2);

    let output = MeasurementSet::open(&output_ms).expect("open averaged MS");
    let data = output
        .data_column(VisibilityDataColumn::Data)
        .expect("DATA column");
    let row = data.get(1).expect("row 1 DATA");
    let ArrayValue::Complex32(row) = row else {
        panic!("expected Complex32 DATA");
    };
    assert_eq!(row.shape(), &[common::NUM_CORR, 2]);
    for corr in 0..common::NUM_CORR {
        let source_index_2 = common::NUM_CORR * common::NUM_CHAN + 2 * common::NUM_CORR + corr;
        let source_index_3 = common::NUM_CORR * common::NUM_CHAN + 3 * common::NUM_CORR + corr;
        let expected = ((source_index_2 + source_index_3) as f32) / 2.0;
        assert_eq!(
            row[[corr, 0]],
            casa_types::Complex32::new(expected, -expected * 0.5)
        );
    }

    let spw = output.spectral_window().expect("SPECTRAL_WINDOW");
    assert_eq!(spw.num_chan(0).expect("NUM_CHAN"), 2);
    assert_eq!(
        spw.chan_freq(0).expect("CHAN_FREQ"),
        vec![1.0025e9, 1.0045e9]
    );
    let spw_table = spw.table();
    let widths = spw_table
        .cell_accessor(0, "CHAN_WIDTH")
        .and_then(|cell| cell.array())
        .expect("CHAN_WIDTH");
    let ArrayValue::Float64(widths) = widths else {
        panic!("expected Float64 CHAN_WIDTH");
    };
    assert_eq!(
        widths.iter().copied().collect::<Vec<_>>(),
        vec![2.0e6, 2.0e6]
    );
    assert_eq!(
        spw_table
            .cell_accessor(0, "TOTAL_BANDWIDTH")
            .and_then(|cell| cell.scalar())
            .expect("TOTAL_BANDWIDTH"),
        &ScalarValue::Float64(4.0e6)
    );
}

#[test]
fn mstransform_no_keepflags_drops_flag_row_samples() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[1, 3]);
    let output_ms = dir.path().join("unflagged.ms");

    let report = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "0:0~3".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: false,
    })
    .expect("mstransform keepflags false");

    assert_eq!(report.row_count, 2);
    let output = MeasurementSet::open(&output_ms).expect("open transformed MS");
    assert_eq!(output.row_count(), 2);
    assert_eq!(
        output
            .main_table()
            .cell_accessor(0, "SCAN_NUMBER")
            .and_then(|cell| cell.scalar())
            .expect("first scan"),
        &ScalarValue::Int32(1)
    );
    assert_eq!(
        output
            .main_table()
            .cell_accessor(1, "SCAN_NUMBER")
            .and_then(|cell| cell.scalar())
            .expect("second scan"),
        &ScalarValue::Int32(3)
    );
}

#[test]
fn mstransform_reports_contract_errors_before_mutating_outputs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_fixture_ms(dir.path());
    let output_ms = dir.path().join("bad.ms");

    let same_path = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: input_ms.clone(),
        spw: "0".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: true,
    })
    .unwrap_err();
    assert!(same_path.to_string().contains("must differ"));

    let invalid_spw = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "9".to_string(),
        width: 1,
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
        keep_flags: true,
    })
    .unwrap_err();
    assert!(invalid_spw.to_string().contains("outside SPECTRAL_WINDOW"));
    assert!(!output_ms.exists());

    let missing_column = mstransform(&MsTransformRequest {
        input_ms,
        output_ms,
        spw: "0".to_string(),
        width: 1,
        data_column: TransformDataColumn::CorrectedData,
        selection: MsSelection::new(),
        keep_flags: true,
    })
    .unwrap_err();
    assert!(missing_column.to_string().contains("CORRECTED_DATA"));
}

#[test]
fn selected_row_write_session_persists_bounded_typed_batches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let mut measurement_set = MeasurementSet::open(&ms_path).expect("open fixture MS");
    let samples = common::NUM_CORR * common::NUM_CHAN;
    let selected_rows = vec![0, 2];
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        selected_rows.clone(),
        vec![
            MeasurementSetWriteColumnPlan {
                name: "FLAG".to_string(),
                bytes_per_row: samples,
                mode: MeasurementSetColumnWriteMode::Replace,
                storage_manager: MeasurementSetColumnStorage::Persisted,
                tile_shape: None,
                create_source_column: None,
            },
            MeasurementSetWriteColumnPlan {
                name: "FLAG_ROW".to_string(),
                bytes_per_row: 1,
                mode: MeasurementSetColumnWriteMode::Replace,
                storage_manager: MeasurementSetColumnStorage::Persisted,
                tile_shape: None,
                create_source_column: None,
            },
        ],
        MeasurementSetWriteResources {
            available_bytes: 2 * (samples + 1),
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("mutation plan");
    assert_eq!(plan.batch_rows(), 1);

    let mut session =
        MeasurementSetWriteSession::start_selected_row_mutation(&mut measurement_set, plan)
            .expect("start mutation");
    assert!(ms_path.join(".casa-rs-write-incomplete").exists());
    while !session
        .next_mutation_rows()
        .expect("next mutation rows")
        .is_empty()
    {
        let rows = session
            .next_mutation_rows()
            .expect("next mutation rows")
            .to_vec();
        session
            .write_mutation_batch(
                &mut measurement_set,
                MeasurementSetMutationBatch {
                    row_indices: rows,
                    columns: vec![
                        MeasurementSetMutationColumnBatch {
                            name: "FLAG".to_string(),
                            values: MeasurementSetMutationColumnValues::Arrays(vec![
                                ArrayValue::Bool(
                                    ArrayD::from_shape_vec(
                                        IxDyn(&[common::NUM_CORR, common::NUM_CHAN]).f(),
                                        vec![true; samples],
                                    )
                                    .expect("FLAG cell"),
                                ),
                            ]),
                        },
                        MeasurementSetMutationColumnBatch {
                            name: "FLAG_ROW".to_string(),
                            values: MeasurementSetMutationColumnValues::Scalars(vec![
                                ScalarValue::Bool(true),
                            ]),
                        },
                    ],
                },
            )
            .expect("write mutation batch");
    }
    let telemetry = session.finish_mutation().expect("finish mutation");
    assert_eq!(telemetry.bytes_written, selected_rows.len() * (samples + 1));
    assert_eq!(telemetry.rows_written, selected_rows.len());
    assert_eq!(telemetry.maximum_resident_bytes, 2 * (samples + 1));
    assert_eq!(telemetry.queue_wait_seconds, 0.0);
    assert!(telemetry.producer_seconds >= telemetry.write_seconds);
    assert!(telemetry.finalize_seconds >= 0.0);
    assert!(!ms_path.join(".casa-rs-write-incomplete").exists());
    drop(measurement_set);

    let reopened = MeasurementSet::open(&ms_path).expect("reopen mutated MS");
    for row in selected_rows {
        let flags = reopened
            .main_table()
            .cell_accessor(row, "FLAG")
            .and_then(|cell| cell.array())
            .expect("mutated FLAG");
        let ArrayValue::Bool(flags) = flags else {
            panic!("expected Bool FLAG");
        };
        assert!(flags.iter().all(|flag| *flag));
        assert_eq!(
            reopened
                .main_table()
                .cell_accessor(row, "FLAG_ROW")
                .and_then(|cell| cell.scalar())
                .expect("mutated FLAG_ROW"),
            &ScalarValue::Bool(true)
        );
    }
    assert_eq!(
        reopened
            .main_table()
            .cell_accessor(1, "FLAG_ROW")
            .and_then(|cell| cell.scalar())
            .expect("untouched FLAG_ROW"),
        &ScalarValue::Bool(false)
    );
}

#[test]
fn interrupted_selected_row_write_remains_detectable_without_snapshot_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let mut measurement_set = MeasurementSet::open(&ms_path).expect("open fixture MS");
    let plan = MeasurementSetWritePlan::selected_row_mutation(
        vec![0],
        vec![MeasurementSetWriteColumnPlan {
            name: "FLAG_ROW".to_string(),
            bytes_per_row: 1,
            mode: MeasurementSetColumnWriteMode::Replace,
            storage_manager: MeasurementSetColumnStorage::Persisted,
            tile_shape: None,
            create_source_column: None,
        }],
        MeasurementSetWriteResources {
            available_bytes: 1,
            maximum_live_batches: 1,
            tiled_column_buffer_bytes: 0,
        },
    )
    .expect("mutation plan");
    let mut session =
        MeasurementSetWriteSession::start_selected_row_mutation(&mut measurement_set, plan)
            .expect("start mutation");
    session
        .write_mutation_batch(
            &mut measurement_set,
            MeasurementSetMutationBatch {
                row_indices: vec![0],
                columns: vec![MeasurementSetMutationColumnBatch {
                    name: "FLAG_ROW".to_string(),
                    values: MeasurementSetMutationColumnValues::Scalars(vec![ScalarValue::Bool(
                        true,
                    )]),
                }],
            },
        )
        .expect("persist one row before interruption");
    drop(session);
    drop(measurement_set);

    let error = match MeasurementSet::open(&ms_path) {
        Ok(_) => panic!("marker must reject open"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("incomplete write marker"));
    std::fs::remove_file(ms_path.join(".casa-rs-write-incomplete")).expect("remove test marker");
    let reopened = MeasurementSet::open(&ms_path).expect("reopen after explicit marker removal");
    assert_eq!(
        reopened
            .main_table()
            .cell_accessor(0, "FLAG_ROW")
            .and_then(|cell| cell.scalar())
            .expect("persisted interrupted value"),
        &ScalarValue::Bool(true)
    );
}
