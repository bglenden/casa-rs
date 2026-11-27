// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::{RecordField, RecordValue, Value};
use casa_tables::Table;
use ndarray::ArrayD;

use super::*;
use crate::builder::MeasurementSetBuilder;
use crate::schema::main_table::OptionalMainColumn;
use crate::test_helpers::default_value;

#[test]
fn visibility_channel_read_range_tracks_contiguous_and_covering_ranges() {
    assert_eq!(
        VisibilityChannelReadRange::from_contiguous_indices(&[2, 3, 4]),
        Some(VisibilityChannelReadRange::new(2, 3))
    );
    assert_eq!(
        VisibilityChannelReadRange::from_contiguous_indices(&[2, 4]),
        None
    );
    assert_eq!(
        VisibilityChannelReadRange::covering_indices([4, 2, 7]),
        Some(VisibilityChannelReadRange::new(2, 6))
    );
    assert_eq!(VisibilityChannelReadRange::full(5).end_exclusive(), 5);
}

#[test]
fn visibility_read_block_plan_builds_buffer_request() {
    let source_partition = SourcePartition::new(SourcePartitionId(3), 1, 7, 8, 9, 16, 4);
    let plan = VisibilityReadBlockPlan::new(
        source_partition.clone(),
        vec![11, 10],
        VisibilityChannelReadRange::new(4, 6),
    );

    let request = plan.to_buffer_request(VisibilityDataColumn::CorrectedData, false);

    assert_eq!(request.source_partition, Some(source_partition));
    assert_eq!(request.data_column, VisibilityDataColumn::CorrectedData);
    assert_eq!(request.row_indices, vec![11, 10]);
    assert_eq!(request.channel_start, 4);
    assert_eq!(request.channel_count, 6);
    assert!(!request.include_data);
    assert!(request.include_flags);
    assert!(request.include_weights);
    assert!(request.include_weight_spectrum);
    assert!(request.include_uvw);
}

#[test]
fn fill_visibility_buffer_reads_selected_channels_columnar() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = dir.path().join("visibility-buffer.ms");
    let mut ms = MeasurementSet::create(
        &ms_path,
        MeasurementSetBuilder::new()
            .with_main_column(OptionalMainColumn::Data)
            .with_main_column(OptionalMainColumn::WeightSpectrum),
    )
    .unwrap();
    add_visibility_test_row(ms.main_table_mut(), 0);
    add_visibility_test_row(ms.main_table_mut(), 1);
    ms.save().expect("save visibility-buffer test MS");
    let ms = MeasurementSet::open(&ms_path).expect("reopen visibility-buffer test MS");

    let mut request =
        VisibilityBufferRequest::imaging(VisibilityDataColumn::Data, vec![1, 0], 1, 2)
            .with_source_partition(SourcePartition::new(SourcePartitionId(0), 0, 0, 0, 0, 4, 2));
    request.include_time = true;
    request.include_interval = true;
    request.include_exposure = true;
    request.include_array_ids = true;
    request.include_observation_ids = true;
    request.include_scan_numbers = true;
    request.include_state_ids = true;
    request.include_weight_spectrum = false;
    let mut buffer = VisibilityBuffer::default();
    let report = ms.fill_visibility_buffer(&request, &mut buffer).unwrap();

    assert_eq!(buffer.row_indices, vec![1, 0]);
    assert_eq!(buffer.channel_start, 1);
    assert_eq!(buffer.channel_range(), 1..3);
    assert_eq!(buffer.channel_count, 2);
    assert_eq!(buffer.corr_count, 2);
    assert_eq!(
        buffer
            .source_partition
            .as_ref()
            .map(|partition| partition.id),
        Some(SourcePartitionId(0))
    );
    assert_eq!(report.row_count, 2);
    assert_eq!(report.channel_count, 2);
    assert!(report.logical_output_bytes > 0);
    assert!(report.modeled_physical_read_bytes >= report.logical_output_bytes);
    assert_eq!(report.explicit_adaptation_copy_bytes, 64);

    let Some(VisibilityComplexSamples::Complex32(data)) = &buffer.data else {
        panic!("expected Complex32 data");
    };
    assert_eq!(data.len(), 8);
    assert_eq!(
        data[sample_index(0, 0, 0, 2, 2)],
        Complex32::new(110.0, -110.0)
    );
    assert_eq!(
        data[sample_index(0, 1, 0, 2, 2)],
        Complex32::new(10.0, -10.0)
    );
    assert_eq!(
        data[sample_index(1, 0, 1, 2, 2)],
        Complex32::new(121.0, -121.0)
    );
    assert_eq!(
        data[sample_index(1, 1, 1, 2, 2)],
        Complex32::new(21.0, -21.0)
    );

    let flags = buffer.flags.as_ref().expect("flags");
    assert!(flags[sample_index(0, 0, 0, 2, 2)]);
    assert!(!flags[sample_index(0, 1, 0, 2, 2)]);

    let Some(VisibilityFloatSamples::Float32(weights)) = &buffer.weights else {
        panic!("expected Float32 weights");
    };
    assert_eq!(weights, &[11.0, 12.0, 1.0, 2.0]);

    let uvw = buffer.uvw.as_ref().expect("uvw");
    assert_eq!(&uvw[0..3], &[101.0, 102.0, 103.0]);
    assert_eq!(&uvw[3..6], &[1.0, 2.0, 3.0]);
    assert_eq!(buffer.antenna1.as_deref(), Some(&[11, 1][..]));
    assert_eq!(buffer.antenna2.as_deref(), Some(&[12, 2][..]));
    assert_eq!(buffer.data_desc_ids.as_deref(), Some(&[13, 3][..]));
    assert_eq!(buffer.field_ids.as_deref(), Some(&[14, 4][..]));
    assert_eq!(buffer.flag_row.as_deref(), Some(&[false, true][..]));
    assert_eq!(buffer.time.as_deref(), Some(&[1001.0, 1.0][..]));
    assert_eq!(buffer.interval.as_deref(), Some(&[1010.0, 10.0][..]));
    assert_eq!(buffer.exposure.as_deref(), Some(&[1020.0, 20.0][..]));
    assert_eq!(buffer.array_ids.as_deref(), Some(&[15, 5][..]));
    assert_eq!(buffer.observation_ids.as_deref(), Some(&[16, 6][..]));
    assert_eq!(buffer.scan_numbers.as_deref(), Some(&[17, 7][..]));
    assert_eq!(buffer.state_ids.as_deref(), Some(&[18, 8][..]));

    let data_pointer = buffer.data.as_ref().map(|samples| match samples {
        VisibilityComplexSamples::Complex32(values) => values.as_ptr() as usize,
        VisibilityComplexSamples::Complex64(values) => values.as_ptr() as usize,
    });
    let flag_pointer = buffer.flags.as_ref().map(|values| values.as_ptr() as usize);
    let second_report = ms.fill_visibility_buffer(&request, &mut buffer).unwrap();
    assert!(second_report.allocation.reused_buffers > 0);
    assert!(second_report.explicit_adaptation_copy_bytes > report.explicit_adaptation_copy_bytes);
    assert_eq!(
        buffer.data.as_ref().map(|samples| match samples {
            VisibilityComplexSamples::Complex32(values) => values.as_ptr() as usize,
            VisibilityComplexSamples::Complex64(values) => values.as_ptr() as usize,
        }),
        data_pointer
    );
    assert_eq!(
        buffer.flags.as_ref().map(|values| values.as_ptr() as usize),
        flag_pointer
    );
}

#[test]
fn fill_visibility_buffer_rejects_source_partition_shape_mismatch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = dir.path().join("visibility-buffer-shape-mismatch.ms");
    let mut ms = MeasurementSet::create(
        &ms_path,
        MeasurementSetBuilder::new().with_main_column(OptionalMainColumn::Data),
    )
    .unwrap();
    add_visibility_test_row(ms.main_table_mut(), 0);
    ms.save().expect("save visibility-buffer mismatch test MS");
    let ms = MeasurementSet::open(&ms_path).expect("reopen visibility-buffer mismatch test MS");

    let request = VisibilityBufferRequest::imaging(VisibilityDataColumn::Data, vec![0], 1, 2)
        .with_source_partition(SourcePartition::new(SourcePartitionId(0), 0, 0, 0, 0, 2, 2));
    let mut buffer = VisibilityBuffer::default();
    let error = ms
        .fill_visibility_buffer(&request, &mut buffer)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exceeds source partition channel count"),
        "{error}"
    );
}

fn add_visibility_test_row(table: &mut Table, row_id: i32) {
    let fields = table
        .schema()
        .unwrap()
        .columns()
        .iter()
        .map(|column| {
            let value = match column.name() {
                "DATA" => Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(vec![2, 4], complex_row(row_id)).unwrap(),
                )),
                "FLAG" => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(vec![2, 4], flag_row(row_id)).unwrap(),
                )),
                "WEIGHT" => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(
                        vec![2],
                        vec![row_id as f32 * 10.0 + 1.0, row_id as f32 * 10.0 + 2.0],
                    )
                    .unwrap(),
                )),
                "WEIGHT_SPECTRUM" => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(vec![2, 4], weight_spectrum_row(row_id)).unwrap(),
                )),
                "UVW" => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(
                        vec![3],
                        vec![
                            row_id as f64 * 100.0 + 1.0,
                            row_id as f64 * 100.0 + 2.0,
                            row_id as f64 * 100.0 + 3.0,
                        ],
                    )
                    .unwrap(),
                )),
                "ANTENNA1" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 1)),
                "ANTENNA2" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 2)),
                "DATA_DESC_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 3)),
                "FIELD_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 4)),
                "FLAG_ROW" => Value::Scalar(ScalarValue::Bool(row_id == 0)),
                "TIME" => Value::Scalar(ScalarValue::Float64(row_id as f64 * 1000.0 + 1.0)),
                "INTERVAL" => Value::Scalar(ScalarValue::Float64(row_id as f64 * 1000.0 + 10.0)),
                "EXPOSURE" => Value::Scalar(ScalarValue::Float64(row_id as f64 * 1000.0 + 20.0)),
                "ARRAY_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 5)),
                "OBSERVATION_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 6)),
                "SCAN_NUMBER" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 7)),
                "STATE_ID" => Value::Scalar(ScalarValue::Int32(row_id * 10 + 8)),
                _ => default_value(column.name()),
            };
            RecordField::new(column.name(), value)
        })
        .collect::<Vec<_>>();
    table.add_row(RecordValue::new(fields)).unwrap();
}

fn complex_row(row_id: i32) -> Vec<Complex32> {
    let mut values = Vec::new();
    for corr in 0..2 {
        for chan in 0..4 {
            let value = row_id as f32 * 100.0 + chan as f32 * 10.0 + corr as f32;
            values.push(Complex32::new(value, -value));
        }
    }
    values
}

fn flag_row(row_id: i32) -> Vec<bool> {
    let mut values = Vec::new();
    for corr in 0..2 {
        for chan in 0..4 {
            values.push((row_id + chan + corr) % 2 == 0);
        }
    }
    values
}

fn weight_spectrum_row(row_id: i32) -> Vec<f32> {
    let mut values = Vec::new();
    for corr in 0..2 {
        for chan in 0..4 {
            values.push(row_id as f32 * 100.0 + chan as f32 * 10.0 + corr as f32 + 0.5);
        }
    }
    values
}

fn sample_index(
    channel_slot: usize,
    row_slot: usize,
    corr_slot: usize,
    row_count: usize,
    corr_count: usize,
) -> usize {
    (channel_slot * row_count + row_slot) * corr_count + corr_slot
}
