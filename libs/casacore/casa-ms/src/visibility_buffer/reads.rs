// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) type TimedRead<T> = Option<(T, Duration)>;
pub(super) type DataReadResult = (
    VisibilityComplexSamples,
    usize,
    VisibilityBufferColumnReport,
);
pub(super) type BoolChannelReadResult = (Vec<bool>, usize, VisibilityBufferColumnReport);
pub(super) type FloatReadResult = (
    VisibilityFloatSamples,
    usize,
    VisibilityBufferColumnReport,
    u64,
);
pub(super) type UvwReadResult = (Vec<f64>, VisibilityBufferColumnReport);

pub(super) struct ParallelVisibilityReads {
    pub(super) data: TimedRead<DataReadResult>,
    pub(super) flags: TimedRead<BoolChannelReadResult>,
    pub(super) weights: TimedRead<FloatReadResult>,
    pub(super) weight_spectrum: TimedRead<Option<FloatReadResult>>,
    pub(super) uvw: TimedRead<UvwReadResult>,
    pub(super) scalars: TimedRead<ScalarColumnReadResult>,
}

pub(super) struct ScalarColumnExistingBuffers {
    pub(super) antenna1: Option<Vec<i32>>,
    pub(super) antenna2: Option<Vec<i32>>,
    pub(super) data_desc_ids: Option<Vec<i32>>,
    pub(super) field_ids: Option<Vec<i32>>,
    pub(super) flag_row: Option<Vec<bool>>,
    pub(super) time: Option<Vec<f64>>,
    pub(super) interval: Option<Vec<f64>>,
    pub(super) exposure: Option<Vec<f64>>,
    pub(super) array_ids: Option<Vec<i32>>,
    pub(super) observation_ids: Option<Vec<i32>>,
    pub(super) scan_numbers: Option<Vec<i32>>,
    pub(super) state_ids: Option<Vec<i32>>,
}

pub(super) struct ScalarColumnReadResult {
    pub(super) antenna1: Option<Vec<i32>>,
    pub(super) antenna2: Option<Vec<i32>>,
    pub(super) data_desc_ids: Option<Vec<i32>>,
    pub(super) field_ids: Option<Vec<i32>>,
    pub(super) flag_row: Option<Vec<bool>>,
    pub(super) time: Option<Vec<f64>>,
    pub(super) interval: Option<Vec<f64>>,
    pub(super) exposure: Option<Vec<f64>>,
    pub(super) array_ids: Option<Vec<i32>>,
    pub(super) observation_ids: Option<Vec<i32>>,
    pub(super) scan_numbers: Option<Vec<i32>>,
    pub(super) state_ids: Option<Vec<i32>>,
    pub(super) reports: Vec<VisibilityBufferColumnReport>,
}

pub(super) fn join_visibility_buffer_worker<T>(
    handle: Option<thread::ScopedJoinHandle<'_, MsResult<(T, Duration)>>>,
) -> MsResult<Option<(T, Duration)>> {
    handle
        .map(|handle| {
            handle
                .join()
                .map_err(|_| invalid_input("visibility buffer read worker panicked".to_string()))?
        })
        .transpose()
}

pub(super) fn read_scalar_columns(
    ms: &MeasurementSet,
    request: &VisibilityBufferRequest,
    mut existing: ScalarColumnExistingBuffers,
) -> MsResult<ScalarColumnReadResult> {
    let mut result = ScalarColumnReadResult {
        antenna1: None,
        antenna2: None,
        data_desc_ids: None,
        field_ids: None,
        flag_row: None,
        time: None,
        interval: None,
        exposure: None,
        array_ids: None,
        observation_ids: None,
        scan_numbers: None,
        state_ids: None,
        reports: Vec::new(),
    };
    let mut column_names = Vec::with_capacity(12);
    if request.include_antenna_ids {
        column_names.extend(["ANTENNA1", "ANTENNA2"]);
    }
    if request.include_data_desc_ids {
        column_names.push("DATA_DESC_ID");
    }
    if request.include_field_ids {
        column_names.push("FIELD_ID");
    }
    if request.include_flag_row {
        column_names.push("FLAG_ROW");
    }
    if request.include_time {
        column_names.push("TIME");
    }
    if request.include_interval {
        column_names.push("INTERVAL");
    }
    if request.include_exposure {
        column_names.push("EXPOSURE");
    }
    if request.include_array_ids {
        column_names.push("ARRAY_ID");
    }
    if request.include_observation_ids {
        column_names.push("OBSERVATION_ID");
    }
    if request.include_scan_numbers {
        column_names.push("SCAN_NUMBER");
    }
    if request.include_state_ids {
        column_names.push("STATE_ID");
    }
    let mut columns = ms
        .main_table()
        .required_scalar_columns_owned_for_rows(&column_names, &request.row_indices)?;

    if request.include_antenna_ids {
        let (antenna1, report) = take_i32_scalar_column(
            ms,
            "ANTENNA1",
            &mut columns,
            request.row_indices.len(),
            existing.antenna1.take(),
        )?;
        let (antenna2, report2) = take_i32_scalar_column(
            ms,
            "ANTENNA2",
            &mut columns,
            request.row_indices.len(),
            existing.antenna2.take(),
        )?;
        result.antenna1 = Some(antenna1);
        result.antenna2 = Some(antenna2);
        result.reports.push(report);
        result.reports.push(report2);
    }
    if request.include_data_desc_ids {
        let (data_desc_ids, report) = take_i32_scalar_column(
            ms,
            "DATA_DESC_ID",
            &mut columns,
            request.row_indices.len(),
            existing.data_desc_ids.take(),
        )?;
        result.data_desc_ids = Some(data_desc_ids);
        result.reports.push(report);
    }
    if request.include_field_ids {
        let (field_ids, report) = take_i32_scalar_column(
            ms,
            "FIELD_ID",
            &mut columns,
            request.row_indices.len(),
            existing.field_ids.take(),
        )?;
        result.field_ids = Some(field_ids);
        result.reports.push(report);
    }
    if request.include_flag_row {
        let (flag_row, report) = take_bool_scalar_column(
            ms,
            "FLAG_ROW",
            &mut columns,
            request.row_indices.len(),
            existing.flag_row.take(),
        )?;
        result.flag_row = Some(flag_row);
        result.reports.push(report);
    }
    if request.include_time {
        let (time, report) = take_f64_scalar_column(
            ms,
            "TIME",
            &mut columns,
            request.row_indices.len(),
            existing.time.take(),
        )?;
        result.time = Some(time);
        result.reports.push(report);
    }
    if request.include_interval {
        let (interval, report) = take_f64_scalar_column(
            ms,
            "INTERVAL",
            &mut columns,
            request.row_indices.len(),
            existing.interval.take(),
        )?;
        result.interval = Some(interval);
        result.reports.push(report);
    }
    if request.include_exposure {
        let (exposure, report) = take_f64_scalar_column(
            ms,
            "EXPOSURE",
            &mut columns,
            request.row_indices.len(),
            existing.exposure.take(),
        )?;
        result.exposure = Some(exposure);
        result.reports.push(report);
    }
    if request.include_array_ids {
        let (array_ids, report) = take_i32_scalar_column(
            ms,
            "ARRAY_ID",
            &mut columns,
            request.row_indices.len(),
            existing.array_ids.take(),
        )?;
        result.array_ids = Some(array_ids);
        result.reports.push(report);
    }
    if request.include_observation_ids {
        let (observation_ids, report) = take_i32_scalar_column(
            ms,
            "OBSERVATION_ID",
            &mut columns,
            request.row_indices.len(),
            existing.observation_ids.take(),
        )?;
        result.observation_ids = Some(observation_ids);
        result.reports.push(report);
    }
    if request.include_scan_numbers {
        let (scan_numbers, report) = take_i32_scalar_column(
            ms,
            "SCAN_NUMBER",
            &mut columns,
            request.row_indices.len(),
            existing.scan_numbers.take(),
        )?;
        result.scan_numbers = Some(scan_numbers);
        result.reports.push(report);
    }
    if request.include_state_ids {
        let (state_ids, report) = take_i32_scalar_column(
            ms,
            "STATE_ID",
            &mut columns,
            request.row_indices.len(),
            existing.state_ids.take(),
        )?;
        result.state_ids = Some(state_ids);
        result.reports.push(report);
    }
    Ok(result)
}

pub(super) fn validate_source_partition(
    request: &VisibilityBufferRequest,
    corr_count: usize,
) -> MsResult<()> {
    let Some(source_partition) = &request.source_partition else {
        return Ok(());
    };
    let requested_end = request
        .channel_start
        .checked_add(request.channel_count)
        .ok_or_else(|| invalid_input("visibility buffer channel range overflow".to_string()))?;
    if requested_end > source_partition.shape.channel_count {
        return Err(invalid_input(format!(
            "requested channel range {}..{} exceeds source partition channel count {}",
            request.channel_start, requested_end, source_partition.shape.channel_count
        )));
    }
    if corr_count != 0 && corr_count != source_partition.shape.corr_count {
        return Err(invalid_input(format!(
            "visibility buffer correlation count {corr_count} does not match source partition correlation count {}",
            source_partition.shape.corr_count
        )));
    }
    Ok(())
}

pub(super) fn ensure_typed_channel_block_shape(
    column_name: &str,
    actual_rows: usize,
    axis0_count: usize,
    actual_channels: usize,
    expected_rows: usize,
    expected_channels: usize,
) -> MsResult<usize> {
    if actual_rows != expected_rows {
        return Err(invalid_input(format!(
            "{column_name} typed selected-row block has {actual_rows} rows, expected {expected_rows}"
        )));
    }
    if actual_channels != expected_channels {
        return Err(invalid_input(format!(
            "{column_name} typed selected-row block has {actual_channels} channels, expected {expected_channels}"
        )));
    }
    if axis0_count == 0 {
        return Err(invalid_input(format!(
            "{column_name} typed selected-row block has empty axis 0"
        )));
    }
    Ok(axis0_count)
}

pub(super) fn channel_block_report(
    ms: &MeasurementSet,
    column_name: &str,
    primitive: PrimitiveType,
    channel_start: usize,
    channel_count: usize,
    corr_count: usize,
    row_count: usize,
) -> MsResult<VisibilityBufferColumnReport> {
    column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name,
        primitive,
        channelized: true,
        channel_start,
        requested_channels: channel_count,
        elements_per_channel_or_row: corr_count,
        row_count,
    })
}

pub(super) fn read_complex_channel_column(
    ms: &MeasurementSet,
    column_name: &str,
    row_indices: &[usize],
    channel_start: usize,
    channel_count: usize,
    existing: Option<VisibilityComplexSamples>,
) -> MsResult<(
    VisibilityComplexSamples,
    usize,
    VisibilityBufferColumnReport,
)> {
    let primitive = main_column_primitive_type(ms.main_table(), column_name)?;
    let accessor = ms.main_table().column_accessor(column_name)?;
    let (samples, shape) = match primitive {
        PrimitiveType::Complex32 => {
            let mut values = match existing {
                Some(VisibilityComplexSamples::Complex32(values)) => values,
                _ => Vec::new(),
            };
            let shape = accessor
                .fill_array_cells_2d_channel_range_typed_uncached(
                    row_indices,
                    channel_start,
                    channel_count,
                    SelectedArray2DCellsMut::Complex32(&mut values),
                )?
                .ok_or_else(|| {
                    invalid_input(format!(
                        "required visibility column {column_name} is undefined for the selected rows"
                    ))
                })?;
            (VisibilityComplexSamples::Complex32(values), shape)
        }
        PrimitiveType::Complex64 => {
            let mut values = match existing {
                Some(VisibilityComplexSamples::Complex64(values)) => values,
                _ => Vec::new(),
            };
            let shape = accessor
                .fill_array_cells_2d_channel_range_typed_uncached(
                    row_indices,
                    channel_start,
                    channel_count,
                    SelectedArray2DCellsMut::Complex64(&mut values),
                )?
                .ok_or_else(|| {
                    invalid_input(format!(
                        "required visibility column {column_name} is undefined for the selected rows"
                    ))
                })?;
            (VisibilityComplexSamples::Complex64(values), shape)
        }
        other => {
            return Err(column_type_error(
                column_name,
                "Complex32 or Complex64 array",
                other,
            ));
        }
    };
    let corr_count = ensure_typed_channel_block_shape(
        column_name,
        shape.row_count,
        shape.axis0_count,
        shape.channel_count,
        row_indices.len(),
        channel_count,
    )?;
    let report = channel_block_report(
        ms,
        column_name,
        primitive,
        channel_start,
        channel_count,
        corr_count,
        row_indices.len(),
    )?;
    Ok((samples, corr_count, report))
}

pub(super) fn read_bool_channel_column(
    ms: &MeasurementSet,
    column_name: &str,
    row_indices: &[usize],
    channel_start: usize,
    channel_count: usize,
    existing: Option<Vec<bool>>,
) -> MsResult<(Vec<bool>, usize, VisibilityBufferColumnReport)> {
    let primitive = main_column_primitive_type(ms.main_table(), column_name)?;
    if primitive != PrimitiveType::Bool {
        return Err(column_type_error(column_name, "Bool array", primitive));
    }
    let mut out = existing.unwrap_or_default();
    let shape = ms
        .main_table()
        .column_accessor(column_name)?
        .fill_array_cells_2d_channel_range_typed_uncached(
            row_indices,
            channel_start,
            channel_count,
            SelectedArray2DCellsMut::Bool(&mut out),
        )?
        .ok_or_else(|| {
            invalid_input(format!(
                "required visibility column {column_name} is undefined for the selected rows"
            ))
        })?;
    let corr_count = ensure_typed_channel_block_shape(
        column_name,
        shape.row_count,
        shape.axis0_count,
        shape.channel_count,
        row_indices.len(),
        channel_count,
    )?;
    let report = channel_block_report(
        ms,
        column_name,
        primitive,
        channel_start,
        channel_count,
        corr_count,
        row_indices.len(),
    )?;
    Ok((out, corr_count, report))
}

pub(super) fn read_float_channel_column(
    ms: &MeasurementSet,
    column_name: &str,
    row_indices: &[usize],
    channel_start: usize,
    channel_count: usize,
    existing: Option<VisibilityFloatSamples>,
) -> MsResult<Option<FloatReadResult>> {
    let primitive = main_column_primitive_type(ms.main_table(), column_name)?;
    let accessor = ms.main_table().column_accessor(column_name)?;
    let (samples, shape) = match primitive {
        PrimitiveType::Float32 => {
            let mut values = match existing {
                Some(VisibilityFloatSamples::Float32(values)) => values,
                _ => Vec::new(),
            };
            let Some(shape) = accessor.fill_array_cells_2d_channel_range_typed_uncached(
                row_indices,
                channel_start,
                channel_count,
                SelectedArray2DCellsMut::Float32(&mut values),
            )?
            else {
                return Ok(None);
            };
            (VisibilityFloatSamples::Float32(values), shape)
        }
        PrimitiveType::Float64 => {
            let mut values = match existing {
                Some(VisibilityFloatSamples::Float64(values)) => values,
                _ => Vec::new(),
            };
            let Some(shape) = accessor.fill_array_cells_2d_channel_range_typed_uncached(
                row_indices,
                channel_start,
                channel_count,
                SelectedArray2DCellsMut::Float64(&mut values),
            )?
            else {
                return Ok(None);
            };
            (VisibilityFloatSamples::Float64(values), shape)
        }
        other => {
            return Err(column_type_error(
                column_name,
                "Float32 or Float64 array",
                other,
            ));
        }
    };
    let corr_count = ensure_typed_channel_block_shape(
        column_name,
        shape.row_count,
        shape.axis0_count,
        shape.channel_count,
        row_indices.len(),
        channel_count,
    )?;
    let report = channel_block_report(
        ms,
        column_name,
        primitive,
        channel_start,
        channel_count,
        corr_count,
        row_indices.len(),
    )?;
    Ok(Some((samples, corr_count, report, 0)))
}

pub(super) fn read_float_row_column(
    ms: &MeasurementSet,
    column_name: &str,
    row_indices: &[usize],
    existing: Option<VisibilityFloatSamples>,
) -> MsResult<FloatReadResult> {
    if let Ok(cells) = ms
        .main_table()
        .column_accessor(column_name)?
        .array_cells_1d_typed_uncached(row_indices)
    {
        let primitive = cells.primitive_type();
        let corr_count = cells.axis0_count();
        let (samples, copied_bytes) = match cells {
            SelectedArray1DCells::Float32(values) => {
                let (values, copied_bytes) = reuse_or_replace_f32(existing, values.into_values());
                (VisibilityFloatSamples::Float32(values), copied_bytes)
            }
            SelectedArray1DCells::Float64(values) => {
                let (values, copied_bytes) = reuse_or_replace_f64(existing, values.into_values());
                (VisibilityFloatSamples::Float64(values), copied_bytes)
            }
            other => {
                return Err(column_type_error(
                    column_name,
                    "Float32 or Float64 array",
                    other.primitive_type(),
                ));
            }
        };
        let report = column_report(ColumnReportInput {
            table: ms.main_table(),
            column_name,
            primitive,
            channelized: false,
            channel_start: 0,
            requested_channels: 1,
            elements_per_channel_or_row: corr_count,
            row_count: row_indices.len(),
        })?;
        return Ok((samples, corr_count, report, copied_bytes));
    }

    let values = ms
        .main_table()
        .column_accessor(column_name)?
        .array_cells_owned_uncached(row_indices)?;
    let primitive = main_column_primitive_type(ms.main_table(), column_name)?;
    let corr_count = first_1d_count(&values, column_name)?;
    let sample_count = row_indices.len().saturating_mul(corr_count);
    let samples = match primitive {
        PrimitiveType::Float32 => {
            let mut out = match existing {
                Some(VisibilityFloatSamples::Float32(mut values)) => {
                    values.clear();
                    values.reserve(sample_count.saturating_sub(values.capacity()));
                    values
                }
                _ => Vec::with_capacity(sample_count),
            };
            out.resize(sample_count, 0.0);
            for (row_slot, value) in values.into_iter().enumerate() {
                let row = require_array(value, column_name, row_slot)?;
                let ArrayValue::Float32(array) = row else {
                    return Err(column_type_error(
                        column_name,
                        "Float32 array",
                        row.primitive_type(),
                    ));
                };
                let array = array.into_dimensionality::<Ix1>().map_err(|error| {
                    invalid_input(format!("{column_name} row {row_slot} rank: {error}"))
                })?;
                ensure_row_shape(column_name, row_slot, array.shape(), corr_count)?;
                for corr in 0..corr_count {
                    out[row_slot * corr_count + corr] = array[corr];
                }
            }
            VisibilityFloatSamples::Float32(out)
        }
        PrimitiveType::Float64 => {
            let mut out = match existing {
                Some(VisibilityFloatSamples::Float64(mut values)) => {
                    values.clear();
                    values.reserve(sample_count.saturating_sub(values.capacity()));
                    values
                }
                _ => Vec::with_capacity(sample_count),
            };
            out.resize(sample_count, 0.0);
            for (row_slot, value) in values.into_iter().enumerate() {
                let row = require_array(value, column_name, row_slot)?;
                let ArrayValue::Float64(array) = row else {
                    return Err(column_type_error(
                        column_name,
                        "Float64 array",
                        row.primitive_type(),
                    ));
                };
                let array = array.into_dimensionality::<Ix1>().map_err(|error| {
                    invalid_input(format!("{column_name} row {row_slot} rank: {error}"))
                })?;
                ensure_row_shape(column_name, row_slot, array.shape(), corr_count)?;
                for corr in 0..corr_count {
                    out[row_slot * corr_count + corr] = array[corr];
                }
            }
            VisibilityFloatSamples::Float64(out)
        }
        other => {
            return Err(column_type_error(
                column_name,
                "Float32 or Float64 array",
                other,
            ));
        }
    };
    let report = column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name,
        primitive,
        channelized: false,
        channel_start: 0,
        requested_channels: 1,
        elements_per_channel_or_row: corr_count,
        row_count: row_indices.len(),
    })?;
    let copied_bytes =
        u64::try_from(sample_count.saturating_mul(primitive.fixed_width_bytes().unwrap_or(0)))
            .unwrap_or(u64::MAX);
    Ok((samples, corr_count, report, copied_bytes))
}

pub(super) fn reuse_or_replace_f32(
    existing: Option<VisibilityFloatSamples>,
    replacement: Vec<f32>,
) -> (Vec<f32>, u64) {
    if let Some(VisibilityFloatSamples::Float32(mut values)) = existing {
        let copied_bytes = (replacement.len() as u64).saturating_mul(size_of::<f32>() as u64);
        values.clear();
        values.extend_from_slice(&replacement);
        (values, copied_bytes)
    } else {
        (replacement, 0)
    }
}

pub(super) fn reuse_or_replace_f64(
    existing: Option<VisibilityFloatSamples>,
    replacement: Vec<f64>,
) -> (Vec<f64>, u64) {
    if let Some(VisibilityFloatSamples::Float64(mut values)) = existing {
        let copied_bytes = (replacement.len() as u64).saturating_mul(size_of::<f64>() as u64);
        values.clear();
        values.extend_from_slice(&replacement);
        (values, copied_bytes)
    } else {
        (replacement, 0)
    }
}

pub(super) fn read_uvw_column(
    ms: &MeasurementSet,
    row_indices: &[usize],
    existing: Option<Vec<f64>>,
) -> MsResult<(Vec<f64>, VisibilityBufferColumnReport)> {
    if let Ok(cells) = ms
        .main_table()
        .column_accessor("UVW")?
        .array_cells_1d_typed_uncached(row_indices)
    {
        let primitive = cells.primitive_type();
        if primitive != PrimitiveType::Float64 {
            return Err(column_type_error("UVW", "Float64 array", primitive));
        }
        let SelectedArray1DCells::Float64(values) = cells else {
            return Err(column_type_error("UVW", "Float64 array", primitive));
        };
        if values.axis0_count() != 3 {
            return Err(invalid_input(format!(
                "UVW typed selected 1-D rows have axis length {}, expected 3",
                values.axis0_count()
            )));
        }
        let replacement = values.into_values();
        let mut out = existing.unwrap_or_else(|| Vec::with_capacity(replacement.len()));
        out.clear();
        out.extend_from_slice(&replacement);
        let report = column_report(ColumnReportInput {
            table: ms.main_table(),
            column_name: "UVW",
            primitive,
            channelized: false,
            channel_start: 0,
            requested_channels: 1,
            elements_per_channel_or_row: 3,
            row_count: row_indices.len(),
        })?;
        return Ok((out, report));
    }

    let values = ms
        .main_table()
        .column_accessor("UVW")?
        .array_cells_owned_uncached(row_indices)?;
    let primitive = main_column_primitive_type(ms.main_table(), "UVW")?;
    if primitive != PrimitiveType::Float64 {
        return Err(column_type_error("UVW", "Float64 array", primitive));
    }
    let sample_count = row_indices.len().saturating_mul(3);
    let mut out = existing.unwrap_or_else(|| Vec::with_capacity(sample_count));
    out.clear();
    out.reserve(sample_count.saturating_sub(out.capacity()));
    out.resize(sample_count, 0.0);
    for (row_slot, value) in values.into_iter().enumerate() {
        let row = require_array(value, "UVW", row_slot)?;
        let ArrayValue::Float64(array) = row else {
            return Err(column_type_error(
                "UVW",
                "Float64 array",
                row.primitive_type(),
            ));
        };
        let array = array
            .into_dimensionality::<Ix1>()
            .map_err(|error| invalid_input(format!("UVW row {row_slot} rank: {error}")))?;
        ensure_row_shape("UVW", row_slot, array.shape(), 3)?;
        for axis in 0..3 {
            out[row_slot * 3 + axis] = array[axis];
        }
    }
    let report = column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name: "UVW",
        primitive,
        channelized: false,
        channel_start: 0,
        requested_channels: 1,
        elements_per_channel_or_row: 3,
        row_count: row_indices.len(),
    })?;
    Ok((out, report))
}

pub(super) fn take_i32_scalar_column(
    ms: &MeasurementSet,
    column_name: &str,
    columns: &mut HashMap<String, RequiredScalarColumnValues>,
    row_count: usize,
    existing: Option<Vec<i32>>,
) -> MsResult<(Vec<i32>, VisibilityBufferColumnReport)> {
    let primitive = main_column_primitive_type(ms.main_table(), column_name)?;
    if primitive != PrimitiveType::Int32 {
        return Err(column_type_error(column_name, "Int32 scalar", primitive));
    }
    let Some(RequiredScalarColumnValues::Int32(values)) = columns.remove(column_name) else {
        return Err(invalid_input(format!(
            "required Int32 scalar column {column_name} was not loaded"
        )));
    };
    let out = reuse_or_replace_vec(existing, values);
    let report = column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name,
        primitive,
        channelized: false,
        channel_start: 0,
        requested_channels: 1,
        elements_per_channel_or_row: 1,
        row_count,
    })?;
    Ok((out, report))
}

pub(super) fn take_f64_scalar_column(
    ms: &MeasurementSet,
    column_name: &str,
    columns: &mut HashMap<String, RequiredScalarColumnValues>,
    row_count: usize,
    existing: Option<Vec<f64>>,
) -> MsResult<(Vec<f64>, VisibilityBufferColumnReport)> {
    let primitive = main_column_primitive_type(ms.main_table(), column_name)?;
    if primitive != PrimitiveType::Float64 {
        return Err(column_type_error(column_name, "Float64 scalar", primitive));
    }
    let Some(RequiredScalarColumnValues::Float64(values)) = columns.remove(column_name) else {
        return Err(invalid_input(format!(
            "required Float64 scalar column {column_name} was not loaded"
        )));
    };
    let out = reuse_or_replace_vec(existing, values);
    let report = column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name,
        primitive,
        channelized: false,
        channel_start: 0,
        requested_channels: 1,
        elements_per_channel_or_row: 1,
        row_count,
    })?;
    Ok((out, report))
}

pub(super) fn take_bool_scalar_column(
    ms: &MeasurementSet,
    column_name: &str,
    columns: &mut HashMap<String, RequiredScalarColumnValues>,
    row_count: usize,
    existing: Option<Vec<bool>>,
) -> MsResult<(Vec<bool>, VisibilityBufferColumnReport)> {
    let primitive = main_column_primitive_type(ms.main_table(), column_name)?;
    if primitive != PrimitiveType::Bool {
        return Err(column_type_error(column_name, "Bool scalar", primitive));
    }
    let Some(RequiredScalarColumnValues::Bool(values)) = columns.remove(column_name) else {
        return Err(invalid_input(format!(
            "required Bool scalar column {column_name} was not loaded"
        )));
    };
    let out = reuse_or_replace_vec(existing, values);
    let report = column_report(ColumnReportInput {
        table: ms.main_table(),
        column_name,
        primitive,
        channelized: false,
        channel_start: 0,
        requested_channels: 1,
        elements_per_channel_or_row: 1,
        row_count,
    })?;
    Ok((out, report))
}

pub(super) fn reuse_or_replace_vec<T: Clone>(
    existing: Option<Vec<T>>,
    replacement: Vec<T>,
) -> Vec<T> {
    let Some(mut existing) = existing.filter(|values| values.capacity() >= replacement.len())
    else {
        return replacement;
    };
    existing.clear();
    existing.extend_from_slice(&replacement);
    existing
}
