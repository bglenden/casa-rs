// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(crate) fn read_ism_file_columns(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[(usize, &ColumnDescContents)],
    total_column_count: usize,
    nrrow: usize,
) -> Result<Vec<(String, IsmColumnResult)>, StorageError> {
    let mut file = File::open(file_path)?;
    let header = parse_ism_header(&mut file)?;
    let _dm_name = parse_ism_dm_blob(dm_blob)?;
    let index = parse_ism_index(&mut file, &header)?;
    let be = header.big_endian;

    // Pre-compute column info
    let col_info: Vec<(CasacoreDataType, usize, bool, bool)> = col_descs
        .iter()
        .map(|(_, c)| {
            let scalar_dt =
                CasacoreDataType::from_primitive_type(c.require_primitive_type()?, false);
            let nrelem = if c.is_array && !c.shape.is_empty() {
                c.shape.iter().map(|&s| s as usize).product()
            } else {
                1
            };
            let indirect = is_ism_indirect_array(c);
            Ok((
                scalar_dt,
                nrelem,
                c.is_array && !indirect && !c.shape.is_empty(),
                indirect,
            ))
        })
        .collect::<Result<_, StorageError>>()?;

    let mut array_reader = if col_descs
        .iter()
        .any(|(_, col_desc)| is_ism_indirect_array(col_desc))
    {
        let array_path = PathBuf::from(format!("{}i", file_path.display()));
        if !array_path.is_file() {
            return Err(StorageError::MissingDataFile(array_path));
        }
        Some(StManArrayFileReader::open(&array_path, be)?)
    } else {
        None
    };

    // Initialize column-major result vectors
    let ncol = col_descs.len();
    let mut flat_columns: Vec<Vec<casa_core::Value>> =
        (0..ncol).map(|_| Vec::with_capacity(nrrow)).collect();
    let mut indirect_columns: Vec<Option<Vec<Option<casa_core::Value>>>> = col_descs
        .iter()
        .map(|(_, col_desc)| is_ism_indirect_array(col_desc).then(|| Vec::with_capacity(nrrow)))
        .collect();

    // Iterate over bucket intervals from the ISMIndex
    let n_intervals = index.bucket_nrs.len();
    let mut last_bucket_nr: Option<u32> = None;
    let mut cached_bucket: Option<IsmBucket> = None;

    for interval in 0..n_intervals {
        let bucket_start = index.rows[interval] as usize;
        let bucket_end = index.rows[interval + 1] as usize;
        let rows_in_bucket = bucket_end - bucket_start;
        let bucket_nr = index.bucket_nrs[interval];

        // Load bucket (cache for repeated access)
        if last_bucket_nr != Some(bucket_nr) {
            let raw = read_ism_bucket(&mut file, &header, bucket_nr)?;
            cached_bucket = Some(parse_ism_bucket(&raw, total_column_count, be)?);
            last_bucket_nr = Some(bucket_nr);
        }
        let bucket = cached_bucket.as_ref().unwrap();

        // For each column, expand the interval values
        for (out_col_idx, &(source_col_idx, col_desc)) in col_descs.iter().enumerate() {
            let (dt, nrelem, is_direct_fixed_array, is_indirect_array) = col_info[out_col_idx];

            if source_col_idx >= bucket.col_indices.len() {
                // Column not in this bucket — fill with defaults
                for _ in 0..rows_in_bucket {
                    if is_indirect_array {
                        indirect_columns[out_col_idx]
                            .as_mut()
                            .expect("indirect ISM column storage")
                            .push(None);
                    } else {
                        flat_columns[out_col_idx].push(default_value(dt, false, col_desc));
                    }
                }
                continue;
            }

            let col_index = &bucket.col_indices[source_col_idx];

            // Expand each row in this bucket's interval
            for rel_row in 0..rows_in_bucket {
                let k = get_interval(col_index, rel_row as u32);
                let data_offset = col_index.offsets[k] as usize;

                if is_indirect_array {
                    let offset_value =
                        read_scalar_at(&bucket.data, data_offset, CasacoreDataType::TpInt64, be)?;
                    let offset = match offset_value {
                        casa_core::Value::Scalar(casa_core::ScalarValue::Int64(value)) => value,
                        other => {
                            return Err(StorageError::FormatMismatch(format!(
                                "ISM indirect column '{}' expected Int64 offset, got {other:?}",
                                col_desc.col_name
                            )));
                        }
                    };
                    let value = array_reader
                        .as_mut()
                        .expect("indirect ISM array reader")
                        .read_array_at(offset, dt)?;
                    indirect_columns[out_col_idx]
                        .as_mut()
                        .expect("indirect ISM column storage")
                        .push(value);
                } else {
                    let value = if is_direct_fixed_array {
                        decode_array_cell(
                            &bucket.data[data_offset..],
                            dt,
                            nrelem,
                            &col_desc.shape,
                            be,
                        )?
                    } else {
                        read_scalar_at(&bucket.data, data_offset, dt, be)?
                    };
                    flat_columns[out_col_idx].push(value);
                }
            }
        }
    }

    // Convert per-column results into the shared storage-layer form.
    let mut result = Vec::with_capacity(ncol);
    for (col_idx, &(_, col_desc)) in col_descs.iter().enumerate() {
        let (dt, nrelem, is_direct_fixed_array, is_indirect_array) = col_info[col_idx];
        if is_indirect_array {
            result.push((
                col_desc.col_name.clone(),
                IsmColumnResult::Indirect(
                    indirect_columns[col_idx]
                        .take()
                        .expect("indirect ISM column storage"),
                ),
            ));
        } else {
            let values = &flat_columns[col_idx];
            let raw = if is_direct_fixed_array {
                flatten_array_cells(values, dt, nrelem)?
            } else {
                values_to_column_raw_scalar(values, dt)?
            };
            result.push((col_desc.col_name.clone(), IsmColumnResult::Flat(raw)));
        }
    }

    Ok(result)
}

pub(crate) fn read_ism_scalar_column_rows(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    target_col_idx: usize,
    selected_rows: &[usize],
    preserve_defaults: bool,
) -> Result<Option<Vec<Option<ScalarValue>>>, StorageError> {
    if selected_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }

    if target_col_idx >= col_descs.len() {
        return Err(StorageError::FormatMismatch(format!(
            "ISM scalar column index {target_col_idx} is out of range"
        )));
    }
    let target_col_desc = col_descs[target_col_idx];
    if target_col_desc.is_array || target_col_desc.is_record() {
        return Ok(None);
    }

    let cache_key =
        selected_scalars_cache::key(file_path, col_descs, selected_rows, preserve_defaults)?;
    if let Some(values) =
        selected_scalars_cache::get(&cache_key, target_col_idx, selected_rows.len())
    {
        return Ok(Some(values));
    }

    let values_by_column = read_ism_scalar_columns_rows_uncached(
        file_path,
        dm_blob,
        col_descs,
        selected_rows,
        preserve_defaults,
    )?;
    let values = values_by_column
        .get(target_col_idx)
        .cloned()
        .ok_or_else(|| {
            StorageError::FormatMismatch(format!(
                "ISM scalar column '{}' missing from selected-row cache",
                target_col_desc.col_name
            ))
        })?;
    selected_scalars_cache::insert(cache_key, values_by_column);
    Ok(Some(values))
}

/// Fill selected direct fixed-shape f64 cells from their original ISM buckets.
/// The caller owns the packed output; only the current bucket is retained.
pub(crate) fn fill_ism_f64_array_rows(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    target_col_idx: usize,
    selected_rows: &[usize],
    values: &mut Vec<f64>,
) -> Result<usize, StorageError> {
    let column = col_descs.get(target_col_idx).ok_or_else(|| {
        StorageError::FormatMismatch("ISM array column binding is missing".into())
    })?;
    if !column.is_array
        || column.option & 1 == 0
        || column.nrdim != 1
        || column.shape.len() != 1
        || column.shape[0] <= 0
        || column.require_primitive_type()? != casa_core::PrimitiveType::Float64
    {
        return Err(StorageError::FormatMismatch(format!(
            "typed selected ISM reads require a direct fixed-shape f64 vector, got '{}'",
            column.col_name
        )));
    }
    let axis0_count = column.shape[0] as usize;
    let output_count = selected_rows
        .len()
        .checked_mul(axis0_count)
        .ok_or_else(|| StorageError::FormatMismatch("ISM selected array size overflow".into()))?;
    let cell_bytes = axis0_count
        .checked_mul(8)
        .ok_or_else(|| StorageError::FormatMismatch("ISM array cell size overflow".into()))?;
    values.resize(output_count, 0.0);
    if selected_rows.is_empty() {
        return Ok(axis0_count);
    }
    let _dm_name = parse_ism_dm_blob(dm_blob)?;
    let mut file = File::open(file_path)?;
    let header = parse_ism_header(&mut file)?;
    let index = parse_ism_index(&mut file, &header)?;
    let mut requests: Vec<_> = selected_rows.iter().copied().enumerate().collect();
    requests.sort_unstable_by_key(|&(_, row)| row);
    let mut cached_interval = None;
    let mut cached_bucket = None;
    for (output_row, row) in requests {
        let interval = index.rows.partition_point(|&start| start <= row as u64);
        if interval == 0 || interval >= index.rows.len() {
            return Err(StorageError::FormatMismatch(format!(
                "ISM index has no bucket for row {row}"
            )));
        }
        let interval = interval - 1;
        if cached_interval != Some(interval) {
            let bucket_nr = *index.bucket_nrs.get(interval).ok_or_else(|| {
                StorageError::FormatMismatch("ISM selected array bucket is missing".into())
            })?;
            let raw = read_ism_bucket(&mut file, &header, bucket_nr)?;
            cached_bucket = Some(parse_ism_bucket(&raw, col_descs.len(), header.big_endian)?);
            cached_interval = Some(interval);
        }
        let bucket = cached_bucket.as_ref().expect("selected ISM bucket loaded");
        let column_index = bucket.col_indices.get(target_col_idx).ok_or_else(|| {
            StorageError::FormatMismatch("ISM selected array value index is missing".into())
        })?;
        let relative_row = u32::try_from(row as u64 - index.rows[interval]).map_err(|_| {
            StorageError::FormatMismatch("ISM selected array relative row overflow".into())
        })?;
        let value_interval = get_interval(column_index, relative_row);
        let offset = *column_index.offsets.get(value_interval).ok_or_else(|| {
            StorageError::FormatMismatch("ISM selected array value offset is missing".into())
        })? as usize;
        let end = offset.checked_add(cell_bytes).ok_or_else(|| {
            StorageError::FormatMismatch("ISM selected array offset overflow".into())
        })?;
        let raw = bucket.data.get(offset..end).ok_or_else(|| {
            StorageError::FormatMismatch("ISM selected array exceeds its bucket".into())
        })?;
        for (output, bytes) in values[output_row * axis0_count..(output_row + 1) * axis0_count]
            .iter_mut()
            .zip(raw.as_chunks::<8>().0)
        {
            *output = if header.big_endian {
                read_f64_be(bytes)
            } else {
                read_f64_le(bytes)
            };
        }
    }
    Ok(axis0_count)
}

/// Read selected required scalar columns without expanding every physical
/// column or every row in an incremental bucket.
pub(crate) fn read_ism_required_scalar_columns_rows(
    file_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[&ColumnDescContents],
    requested_col_descs: &[(usize, &ColumnDescContents)],
    selected_rows: &[usize],
) -> Result<HashMap<String, RequiredScalarColumnData>, StorageError> {
    let mut outputs = HashMap::with_capacity(requested_col_descs.len());
    read_ism_required_scalar_columns_rows_reusing(
        file_path,
        dm_blob,
        all_col_descs,
        requested_col_descs,
        selected_rows,
        &mut outputs,
    )?;
    Ok(outputs)
}

pub(crate) fn read_ism_required_scalar_columns_rows_reusing(
    file_path: &Path,
    dm_blob: &[u8],
    all_col_descs: &[&ColumnDescContents],
    requested_col_descs: &[(usize, &ColumnDescContents)],
    selected_rows: &[usize],
    outputs: &mut HashMap<String, RequiredScalarColumnData>,
) -> Result<(), StorageError> {
    let _dm_name = parse_ism_dm_blob(dm_blob)?;
    for (_, col_desc) in requested_col_descs {
        let values =
            outputs
                .entry(col_desc.col_name.clone())
                .or_insert_with(|| match col_desc.data_type {
                    CasacoreDataType::TpBool => RequiredScalarColumnData::Bool(Vec::new()),
                    CasacoreDataType::TpInt => RequiredScalarColumnData::Int32(Vec::new()),
                    CasacoreDataType::TpFloat => RequiredScalarColumnData::Float32(Vec::new()),
                    CasacoreDataType::TpDouble => RequiredScalarColumnData::Float64(Vec::new()),
                    _ => RequiredScalarColumnData::Bool(Vec::new()),
                });
        match (col_desc.data_type, values) {
            (CasacoreDataType::TpBool, RequiredScalarColumnData::Bool(values)) => {
                values.resize(selected_rows.len(), false);
            }
            (CasacoreDataType::TpInt, RequiredScalarColumnData::Int32(values)) => {
                values.resize(selected_rows.len(), 0);
            }
            (CasacoreDataType::TpFloat, RequiredScalarColumnData::Float32(values)) => {
                values.resize(selected_rows.len(), 0.0);
            }
            (CasacoreDataType::TpDouble, RequiredScalarColumnData::Float64(values)) => {
                values.resize(selected_rows.len(), 0.0);
            }
            other => {
                return Err(StorageError::FormatMismatch(format!(
                    "required selected-row ISM column '{}' has incompatible type {other:?}",
                    col_desc.col_name
                )));
            }
        }
    }
    if selected_rows.is_empty() || outputs.is_empty() {
        return Ok(());
    }

    let mut file = File::open(file_path)?;
    let header = parse_ism_header(&mut file)?;
    let index = parse_ism_index(&mut file, &header)?;
    let contiguous_rows = selected_rows
        .windows(2)
        .all(|rows| rows[1] == rows[0].saturating_add(1));
    if contiguous_rows {
        let selected_end = selected_rows[0]
            .checked_add(selected_rows.len())
            .ok_or_else(|| StorageError::FormatMismatch("ISM row range overflow".to_string()))?;
        let mut row_index = selected_rows[0];
        let mut output_start = 0usize;
        while row_index < selected_end {
            let interval = index
                .rows
                .partition_point(|&start| start <= row_index as u64);
            if interval == 0 || interval >= index.rows.len() {
                return Err(StorageError::FormatMismatch(format!(
                    "ISM index has no bucket for row {row_index}"
                )));
            }
            let interval_idx = interval - 1;
            let bucket_start = index.rows[interval_idx] as usize;
            let bucket_end = (index.rows[interval_idx + 1] as usize).min(selected_end);
            let bucket_nr = *index.bucket_nrs.get(interval_idx).ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "ISM bucket number missing for row interval {interval_idx}"
                ))
            })?;
            let raw_bucket = read_ism_bucket(&mut file, &header, bucket_nr)?;
            let bucket = parse_ism_bucket(&raw_bucket, all_col_descs.len(), header.big_endian)?;
            let relative_start = row_index - bucket_start;
            let row_count = bucket_end - row_index;
            for (col_idx, col_desc) in requested_col_descs {
                let col_index = bucket.col_indices.get(*col_idx).ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "required ISM column '{}' missing bucket index {col_idx}",
                        col_desc.col_name
                    ))
                })?;
                let output = outputs
                    .get_mut(&col_desc.col_name)
                    .expect("required ISM output initialized before range decode");
                fill_ism_required_scalar_range(
                    output,
                    &bucket.data,
                    col_index,
                    relative_start,
                    output_start,
                    row_count,
                    header.big_endian,
                )?;
            }
            row_index = bucket_end;
            output_start += row_count;
        }
        return Ok(());
    }
    let mut requests: Vec<(usize, usize)> = selected_rows
        .iter()
        .copied()
        .enumerate()
        .map(|(out_idx, row_index)| (row_index, out_idx))
        .collect();
    requests.sort_unstable_by_key(|&(row_index, _)| row_index);

    let mut cached_interval = None;
    let mut cached_bucket_start = 0usize;
    let mut cached_bucket = None;
    for (row_index, out_idx) in requests {
        let interval = index
            .rows
            .partition_point(|&start| start <= row_index as u64);
        if interval == 0 || interval >= index.rows.len() {
            return Err(StorageError::FormatMismatch(format!(
                "ISM index has no bucket for row {row_index}"
            )));
        }
        let interval_idx = interval - 1;
        if cached_interval != Some(interval_idx) {
            let bucket_nr = *index.bucket_nrs.get(interval_idx).ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "ISM bucket number missing for row interval {interval_idx}"
                ))
            })?;
            cached_interval = Some(interval_idx);
            cached_bucket_start = index.rows[interval_idx] as usize;
            let raw_bucket = read_ism_bucket(&mut file, &header, bucket_nr)?;
            cached_bucket = Some(parse_ism_bucket(
                &raw_bucket,
                all_col_descs.len(),
                header.big_endian,
            )?);
        }
        let bucket = cached_bucket
            .as_ref()
            .expect("ISM bucket loaded before selected scalar decode");
        let relative_row = row_index.saturating_sub(cached_bucket_start) as u32;
        for (col_idx, col_desc) in requested_col_descs {
            let col_index = bucket.col_indices.get(*col_idx).ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "required ISM column '{}' missing bucket index {col_idx}",
                    col_desc.col_name
                ))
            })?;
            let value_interval = get_interval(col_index, relative_row);
            let offset = *col_index.offsets.get(value_interval).ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "required ISM column '{}' missing value offset",
                    col_desc.col_name
                ))
            })? as usize;
            let output = outputs
                .get_mut(&col_desc.col_name)
                .expect("required ISM output initialized before decode");
            match output {
                RequiredScalarColumnData::Bool(values) => {
                    values[out_idx] = bucket.data[offset] != 0;
                }
                RequiredScalarColumnData::Int32(values) => {
                    values[out_idx] = if header.big_endian {
                        read_i32_be(&bucket.data[offset..])
                    } else {
                        read_i32_le(&bucket.data[offset..])
                    };
                }
                RequiredScalarColumnData::Float32(values) => {
                    values[out_idx] = if header.big_endian {
                        read_f32_be(&bucket.data[offset..])
                    } else {
                        read_f32_le(&bucket.data[offset..])
                    };
                }
                RequiredScalarColumnData::Float64(values) => {
                    values[out_idx] = if header.big_endian {
                        read_f64_be(&bucket.data[offset..])
                    } else {
                        read_f64_le(&bucket.data[offset..])
                    };
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fill_ism_required_scalar_range(
    output: &mut RequiredScalarColumnData,
    bucket_data: &[u8],
    col_index: &IsmBucketColIndex,
    source_row_start: usize,
    output_row_start: usize,
    row_count: usize,
    big_endian: bool,
) -> Result<(), StorageError> {
    let source_end = source_row_start + row_count;
    let mut source_row = source_row_start;
    let mut output_row = output_row_start;
    while source_row < source_end {
        let interval = get_interval(col_index, source_row as u32);
        let run_end = col_index
            .row_nrs
            .get(interval + 1)
            .map_or(source_end, |row| *row as usize)
            .min(source_end);
        let run_rows = run_end - source_row;
        let offset = *col_index.offsets.get(interval).ok_or_else(|| {
            StorageError::FormatMismatch("ISM required scalar run has no offset".to_string())
        })? as usize;
        match output {
            RequiredScalarColumnData::Bool(values) => {
                values[output_row..output_row + run_rows].fill(bucket_data[offset] != 0);
            }
            RequiredScalarColumnData::Int32(values) => {
                let value = if big_endian {
                    read_i32_be(&bucket_data[offset..])
                } else {
                    read_i32_le(&bucket_data[offset..])
                };
                values[output_row..output_row + run_rows].fill(value);
            }
            RequiredScalarColumnData::Float32(values) => {
                let value = if big_endian {
                    read_f32_be(&bucket_data[offset..])
                } else {
                    read_f32_le(&bucket_data[offset..])
                };
                values[output_row..output_row + run_rows].fill(value);
            }
            RequiredScalarColumnData::Float64(values) => {
                let value = if big_endian {
                    read_f64_be(&bucket_data[offset..])
                } else {
                    read_f64_le(&bucket_data[offset..])
                };
                values[output_row..output_row + run_rows].fill(value);
            }
        }
        source_row = run_end;
        output_row += run_rows;
    }
    Ok(())
}

pub(crate) fn read_ism_scalar_column(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    target_col_idx: usize,
    nrrow: usize,
) -> Result<Option<Vec<Option<ScalarValue>>>, StorageError> {
    if target_col_idx >= col_descs.len() {
        return Err(StorageError::FormatMismatch(format!(
            "ISM scalar column index {target_col_idx} is out of range"
        )));
    }
    let target_col_desc = col_descs[target_col_idx];
    if target_col_desc.is_array || target_col_desc.is_record() {
        return Ok(None);
    }

    let _dm_name = parse_ism_dm_blob(dm_blob)?;
    let mut file = File::open(file_path)?;
    let header = parse_ism_header(&mut file)?;
    let index = parse_ism_index(&mut file, &header)?;
    let big_endian = header.big_endian;
    let scalar_dt =
        CasacoreDataType::from_primitive_type(target_col_desc.require_primitive_type()?, false);

    let mut values = Vec::with_capacity(nrrow);
    let mut last_bucket_nr = None;
    let mut cached_bucket = None;
    for interval in 0..index.bucket_nrs.len() {
        let bucket_start = index.rows[interval] as usize;
        let bucket_end = index.rows[interval + 1] as usize;
        let rows_in_bucket = bucket_end.saturating_sub(bucket_start);
        let bucket_nr = index.bucket_nrs[interval];
        if last_bucket_nr != Some(bucket_nr) {
            let raw_bucket = read_ism_bucket(&mut file, &header, bucket_nr)?;
            cached_bucket = Some(parse_ism_bucket(&raw_bucket, col_descs.len(), big_endian)?);
            last_bucket_nr = Some(bucket_nr);
        }
        let bucket = cached_bucket
            .as_ref()
            .expect("ISM bucket loaded before scalar column extraction");
        values.extend(load_scalar_bucket_column_values(
            bucket,
            target_col_desc,
            target_col_idx,
            scalar_dt,
            rows_in_bucket,
            big_endian,
            false,
        )?);
    }

    values.truncate(nrrow);
    if values.len() < nrrow {
        values.resize(nrrow, None);
    }
    Ok(Some(values))
}

pub(super) fn read_ism_scalar_columns_rows_uncached(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    selected_rows: &[usize],
    preserve_defaults: bool,
) -> Result<Vec<Vec<Option<ScalarValue>>>, StorageError> {
    let _dm_name = parse_ism_dm_blob(dm_blob)?;
    let mut file = File::open(file_path)?;
    let header = parse_ism_header(&mut file)?;
    let index = parse_ism_index(&mut file, &header)?;
    let big_endian = header.big_endian;

    let owned_col_descs: Vec<ColumnDescContents> =
        col_descs.iter().map(|col| (*col).clone()).collect();
    let col_info: Vec<Option<CasacoreDataType>> = owned_col_descs
        .iter()
        .map(|col_desc| {
            if col_desc.is_array || col_desc.is_record() {
                return Ok(None);
            }
            Ok(Some(CasacoreDataType::from_primitive_type(
                col_desc.require_primitive_type()?,
                false,
            )))
        })
        .collect::<Result<_, StorageError>>()?;

    let mut requests: Vec<(usize, usize)> = selected_rows
        .iter()
        .copied()
        .enumerate()
        .map(|(out_idx, row_index)| (row_index, out_idx))
        .collect();
    requests.sort_unstable_by_key(|&(row_index, _)| row_index);

    let mut cached_interval = None;
    let mut cached_bucket_start = 0usize;
    let mut cached_bucket_values: Vec<Vec<Option<ScalarValue>>> = Vec::new();
    let mut values_by_column = (0..col_descs.len())
        .map(|_| vec![None; selected_rows.len()])
        .collect::<Vec<_>>();

    for (row_index, out_idx) in requests {
        let interval = index
            .rows
            .partition_point(|&start| start <= row_index as u64);
        if interval == 0 || interval >= index.rows.len() {
            return Err(StorageError::FormatMismatch(format!(
                "ISM index has no bucket for row {row_index}"
            )));
        }
        let interval_idx = interval - 1;
        if interval_idx >= index.bucket_nrs.len() {
            return Err(StorageError::FormatMismatch(format!(
                "ISM bucket number missing for row interval {interval_idx}"
            )));
        }

        if cached_interval != Some(interval_idx) {
            cached_interval = Some(interval_idx);
            cached_bucket_start = index.rows[interval_idx] as usize;
            let bucket_end = index.rows[interval_idx + 1] as usize;
            let raw_bucket = read_ism_bucket(&mut file, &header, index.bucket_nrs[interval_idx])?;
            let bucket = parse_ism_bucket(&raw_bucket, owned_col_descs.len(), big_endian)?;
            let rows_in_bucket = bucket_end.saturating_sub(cached_bucket_start);
            cached_bucket_values = owned_col_descs
                .iter()
                .zip(col_info.iter())
                .enumerate()
                .map(|(col_idx, (col_desc, scalar_dt))| match scalar_dt {
                    Some(scalar_dt) => load_scalar_bucket_column_values(
                        &bucket,
                        col_desc,
                        col_idx,
                        *scalar_dt,
                        rows_in_bucket,
                        big_endian,
                        preserve_defaults,
                    ),
                    None => Ok(vec![None; rows_in_bucket]),
                })
                .collect::<Result<Vec<_>, StorageError>>()?;
        }

        let bucket_row = row_index.saturating_sub(cached_bucket_start);
        for (column_values, bucket_column_values) in
            values_by_column.iter_mut().zip(cached_bucket_values.iter())
        {
            column_values[out_idx] = bucket_column_values
                .get(bucket_row)
                .cloned()
                .unwrap_or(None);
        }
    }

    Ok(values_by_column)
}

pub(super) fn default_value(
    dt: CasacoreDataType,
    _is_array: bool,
    _col_desc: &ColumnDescContents,
) -> casa_core::Value {
    use casa_core::{ScalarValue, Value};
    match dt {
        CasacoreDataType::TpBool => Value::Scalar(ScalarValue::Bool(false)),
        CasacoreDataType::TpUChar => Value::Scalar(ScalarValue::UInt8(0)),
        CasacoreDataType::TpShort => Value::Scalar(ScalarValue::Int16(0)),
        CasacoreDataType::TpUShort => Value::Scalar(ScalarValue::UInt16(0)),
        CasacoreDataType::TpInt => Value::Scalar(ScalarValue::Int32(0)),
        CasacoreDataType::TpUInt => Value::Scalar(ScalarValue::UInt32(0)),
        CasacoreDataType::TpFloat => Value::Scalar(ScalarValue::Float32(0.0)),
        CasacoreDataType::TpDouble => Value::Scalar(ScalarValue::Float64(0.0)),
        CasacoreDataType::TpInt64 => Value::Scalar(ScalarValue::Int64(0)),
        CasacoreDataType::TpComplex => {
            Value::Scalar(ScalarValue::Complex32(casa_core::Complex32::new(0.0, 0.0)))
        }
        CasacoreDataType::TpDComplex => {
            Value::Scalar(ScalarValue::Complex64(casa_core::Complex64::new(0.0, 0.0)))
        }
        CasacoreDataType::TpString => Value::Scalar(ScalarValue::String(String::new())),
        _ => Value::Scalar(ScalarValue::Int32(0)),
    }
}

/// Convert scalar values into column-major ColumnRawData.
pub(super) fn values_to_column_raw_scalar(
    values: &[casa_core::Value],
    dt: CasacoreDataType,
) -> Result<ColumnRawData, StorageError> {
    use casa_core::{ScalarValue, Value};

    match dt {
        CasacoreDataType::TpBool => {
            let v: Vec<bool> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Bool(b)) => *b,
                    _ => false,
                })
                .collect();
            Ok(ColumnRawData::Bool(v))
        }
        CasacoreDataType::TpUChar => {
            let v: Vec<u8> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::UInt8(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::UInt8(v))
        }
        CasacoreDataType::TpShort => {
            let v: Vec<i16> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Int16(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::Int16(v))
        }
        CasacoreDataType::TpUShort => {
            let v: Vec<u16> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::UInt16(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::UInt16(v))
        }
        CasacoreDataType::TpInt => {
            let v: Vec<i32> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Int32(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::Int32(v))
        }
        CasacoreDataType::TpUInt => {
            let v: Vec<u32> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::UInt32(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::UInt32(v))
        }
        CasacoreDataType::TpFloat => {
            let v: Vec<f32> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Float32(b)) => *b,
                    _ => 0.0,
                })
                .collect();
            Ok(ColumnRawData::Float32(v))
        }
        CasacoreDataType::TpDouble => {
            let v: Vec<f64> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Float64(b)) => *b,
                    _ => 0.0,
                })
                .collect();
            Ok(ColumnRawData::Float64(v))
        }
        CasacoreDataType::TpInt64 => {
            let v: Vec<i64> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Int64(b)) => *b,
                    _ => 0,
                })
                .collect();
            Ok(ColumnRawData::Int64(v))
        }
        CasacoreDataType::TpComplex => {
            let v: Vec<casa_core::Complex32> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Complex32(c)) => *c,
                    _ => casa_core::Complex32::new(0.0, 0.0),
                })
                .collect();
            Ok(ColumnRawData::Complex32(v))
        }
        CasacoreDataType::TpDComplex => {
            let v: Vec<casa_core::Complex64> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::Complex64(c)) => *c,
                    _ => casa_core::Complex64::new(0.0, 0.0),
                })
                .collect();
            Ok(ColumnRawData::Complex64(v))
        }
        CasacoreDataType::TpString => {
            let v: Vec<String> = values
                .iter()
                .map(|v| match v {
                    Value::Scalar(ScalarValue::String(s)) => s.clone(),
                    _ => String::new(),
                })
                .collect();
            Ok(ColumnRawData::String(v))
        }
        _ => Err(StorageError::FormatMismatch(format!(
            "unsupported ISM scalar type: {dt:?}"
        ))),
    }
}
