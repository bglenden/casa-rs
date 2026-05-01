// SPDX-License-Identifier: LGPL-3.0-or-later

mod common;

use casa_ms::{
    MeasurementSet, MsTransformRequest, SubTable, TransformDataColumn, mstransform,
    schema::main_table::VisibilityDataColumn, selection::MsSelection,
};
use casa_types::{ArrayValue, ScalarValue};

#[test]
fn mstransform_selects_channels_updates_metadata_and_weight_spectrum() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_spectrum_fixture_ms(dir.path(), true, &[]);
    let output_ms = dir.path().join("transformed.ms");

    let report = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "0:2~5".to_string(),
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
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
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new().field(&[0]),
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
            &ScalarValue::Int32(1)
        );
    }
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
fn mstransform_reports_contract_errors_before_mutating_outputs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_ms = common::create_msexplore_fixture_ms(dir.path());
    let output_ms = dir.path().join("bad.ms");

    let same_path = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: input_ms.clone(),
        spw: "0".to_string(),
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
    })
    .unwrap_err();
    assert!(same_path.to_string().contains("must differ"));

    let invalid_spw = mstransform(&MsTransformRequest {
        input_ms: input_ms.clone(),
        output_ms: output_ms.clone(),
        spw: "9".to_string(),
        data_column: TransformDataColumn::Data,
        selection: MsSelection::new(),
    })
    .unwrap_err();
    assert!(invalid_spw.to_string().contains("outside SPECTRAL_WINDOW"));
    assert!(!output_ms.exists());

    let missing_column = mstransform(&MsTransformRequest {
        input_ms,
        output_ms,
        spw: "0".to_string(),
        data_column: TransformDataColumn::CorrectedData,
        selection: MsSelection::new(),
    })
    .unwrap_err();
    assert!(missing_column.to_string().contains("CORRECTED_DATA"));
}
