// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(crate) fn read_ssm_file_columns(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[(usize, &ColumnDescContents)],
    nrrow: usize,
) -> Result<Vec<(String, SsmColumnResult)>, StorageError> {
    let mut file = DataFile::open(file_path)?;
    let header = parse_ssm_header(&mut file)?;
    let dm_info = parse_ssm_dm_blob(dm_blob)?;
    let indices = parse_ssm_indices(&mut file, &header)?;

    // Check if any array columns are stored indirectly via the shared array file.
    let has_indirect = col_descs.iter().map(|(_, desc)| *desc).any(is_ssm_indirect);

    // Lazily open the shared array file for indirect columns.
    let mut array_reader: Option<StManArrayFileReader> = if has_indirect {
        let mut arr_path = file_path.as_os_str().to_os_string();
        arr_path.push("i");
        let arr_pathbuf = std::path::PathBuf::from(arr_path);
        if DataFile::is_file(&arr_pathbuf) {
            Some(StManArrayFileReader::open(&arr_pathbuf, header.big_endian)?)
        } else {
            None
        }
    } else {
        None
    };

    let mut result = Vec::with_capacity(col_descs.len());

    for &(col_idx, col_desc) in col_descs {
        if col_idx >= dm_info.column_offsets.len() {
            return Err(StorageError::FormatMismatch(format!(
                "SSM column index {col_idx} out of range for columnOffsets"
            )));
        }

        let column_offset = dm_info.column_offsets[col_idx] as usize;
        let index_nr = dm_info.col_index_map[col_idx] as usize;

        if index_nr >= indices.len() {
            return Err(StorageError::FormatMismatch(format!(
                "SSM column {} references index {index_nr} but only {} indices exist",
                col_desc.col_name,
                indices.len()
            )));
        }
        let index = &indices[index_nr];

        let is_indirect = is_ssm_indirect(col_desc);
        let is_string_array = col_desc.is_array && is_ssm_variable_string(col_desc);

        if is_string_array {
            let refs = read_column_from_buckets(
                &mut file,
                &header,
                index,
                column_offset,
                CasacoreDataType::TpInt,
                0,
                3,
                0,
                nrrow,
            )?;

            let ref_values = match refs {
                ColumnRawData::Int32(v) => v,
                _ => {
                    return Err(StorageError::FormatMismatch(
                        "SSM string array: expected Int32 bucket/offset/length references"
                            .to_string(),
                    ));
                }
            };

            let mut per_row = Vec::with_capacity(nrrow);
            let mut payload_buckets = LastBucket::default();
            for row_idx in 0..nrrow {
                let base = row_idx * 3;
                let bucket_nr = ref_values[base];
                let str_offset = ref_values[base + 1];
                let str_length = ref_values[base + 2];
                let value = read_ssm_string_array_value(
                    &mut file,
                    &header,
                    &mut payload_buckets,
                    bucket_nr,
                    str_offset,
                    str_length,
                    if col_desc.shape.is_empty() {
                        None
                    } else {
                        Some(&col_desc.shape)
                    },
                )
                .map_err(|err| {
                    StorageError::FormatMismatch(format!(
                        "SSM string array column '{}' row {} locator ({}, {}, {}): {}",
                        col_desc.col_name, row_idx, bucket_nr, str_offset, str_length, err
                    ))
                })?;
                per_row.push(value);
            }

            result.push((
                col_desc.col_name.clone(),
                SsmColumnResult::Indirect(per_row),
            ));
        } else if is_indirect {
            // Read i64 offsets from bucket data (8 bytes per row).
            let offsets = read_column_from_buckets(
                &mut file,
                &header,
                index,
                column_offset,
                CasacoreDataType::TpInt64,
                0,
                1,
                0,
                nrrow,
            )?;

            let offset_values = match offsets {
                ColumnRawData::Int64(v) => v,
                _ => {
                    return Err(StorageError::FormatMismatch(
                        "SSM indirect: expected Int64 offsets".to_string(),
                    ));
                }
            };

            let dt = if col_desc.is_record() {
                CasacoreDataType::TpUChar
            } else {
                CasacoreDataType::from_primitive_type(col_desc.require_primitive_type()?, false)
            };
            let mut per_row = Vec::with_capacity(nrrow);
            if let Some(reader) = array_reader.as_mut() {
                for (row_idx, &off) in offset_values.iter().enumerate() {
                    let value = reader.read_array_at(off, dt).map_err(|err| {
                        StorageError::FormatMismatch(format!(
                            "SSM indirect column '{}' row {} offset {} type {:?}: {}",
                            col_desc.col_name, row_idx, off, dt, err
                        ))
                    })?;
                    let value = if col_desc.is_record() {
                        match value {
                            Some(Value::Array(ArrayValue::UInt8(bytes))) => {
                                if bytes.ndim() != 1 {
                                    return Err(StorageError::FormatMismatch(format!(
                                        "SSM record column '{}' row {} expected 1-D u8 payload, got shape {:?}",
                                        col_desc.col_name,
                                        row_idx,
                                        bytes.shape()
                                    )));
                                }
                                let raw_bytes: Vec<u8> = bytes.iter().copied().collect();
                                Some(Value::Record(
                                    super::super::table_control::deserialize_record_from_uchar(
                                        &raw_bytes,
                                    )?,
                                ))
                            }
                            Some(other) => {
                                return Err(StorageError::FormatMismatch(format!(
                                    "SSM record column '{}' row {} expected u8 payload, got {other:?}",
                                    col_desc.col_name, row_idx
                                )));
                            }
                            None => None,
                        }
                    } else {
                        value
                    };
                    per_row.push(value);
                }
            } else if nrrow == 0 || offset_values.iter().all(|&off| off == 0) {
                per_row.resize(nrrow, None);
            } else {
                return Err(StorageError::FormatMismatch(
                    "SSM indirect column but no array file found".to_string(),
                ));
            }

            result.push((
                col_desc.col_name.clone(),
                SsmColumnResult::Indirect(per_row),
            ));
        } else {
            let nrelem = if col_desc.is_array && !col_desc.shape.is_empty() {
                col_desc.shape.iter().map(|&s| s as usize).product()
            } else {
                1usize
            };

            let raw = read_column_from_buckets(
                &mut file,
                &header,
                index,
                column_offset,
                col_desc.data_type,
                col_desc.max_length,
                nrelem,
                0,
                nrrow,
            )?;

            result.push((col_desc.col_name.clone(), SsmColumnResult::Flat(raw)));
        }
    }

    Ok(result)
}

pub(crate) fn read_ssm_scalar_column_rows(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    target_col_idx: usize,
    selected_rows: &[usize],
) -> Result<Option<Vec<Option<ScalarValue>>>, StorageError> {
    if selected_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut file = DataFile::open(file_path)?;
    let header = parse_ssm_header(&mut file)?;
    let dm_info = parse_ssm_dm_blob(dm_blob)?;
    let indices = parse_ssm_indices(&mut file, &header)?;

    if target_col_idx >= col_descs.len() {
        return Err(StorageError::FormatMismatch(format!(
            "SSM scalar column index {target_col_idx} is out of range"
        )));
    }
    if target_col_idx >= dm_info.column_offsets.len()
        || target_col_idx >= dm_info.col_index_map.len()
    {
        return Err(StorageError::FormatMismatch(format!(
            "SSM scalar column index {target_col_idx} missing dm blob metadata"
        )));
    }

    let col_desc = col_descs[target_col_idx];
    if col_desc.is_array || col_desc.is_record() {
        return Ok(None);
    }

    let column_offset = dm_info.column_offsets[target_col_idx] as usize;
    let index_nr = dm_info.col_index_map[target_col_idx] as usize;
    if index_nr >= indices.len() {
        return Err(StorageError::FormatMismatch(format!(
            "SSM column '{}' references index {index_nr} but only {} indices exist",
            col_desc.col_name,
            indices.len()
        )));
    }
    let index = &indices[index_nr];
    let be = header.big_endian;

    let mut requests: Vec<(usize, usize)> = selected_rows
        .iter()
        .copied()
        .enumerate()
        .map(|(out_idx, row_index)| (row_index, out_idx))
        .collect();
    requests.sort_unstable_by_key(|&(row_index, _)| row_index);

    let mut data_buckets = LastBucket::default();
    let mut payload_buckets = LastBucket::default();
    let mut values = vec![None; selected_rows.len()];
    let (elem_bytes, _) = canonical_element_size(col_desc.data_type);

    for (row_index, out_idx) in requests {
        let (bucket_nr, start_row, _end_row) =
            index.find_bucket(row_index as u64).ok_or_else(|| {
                StorageError::FormatMismatch(format!("SSM index has no bucket for row {row_index}"))
            })?;
        let cached_bucket = data_buckets.read(&mut file, &header, bucket_nr)?;
        let row_in_bucket = (row_index as u64 - start_row) as usize;
        let value = match col_desc.data_type {
            CasacoreDataType::TpBool => {
                let byte_offset = column_offset + row_in_bucket / 8;
                let bit_offset = row_in_bucket % 8;
                Value::Scalar(ScalarValue::Bool(
                    ((cached_bucket[byte_offset] >> bit_offset) & 1) != 0,
                ))
            }
            CasacoreDataType::TpString => {
                let string = if col_desc.max_length > 0 {
                    let width = col_desc.max_length as usize;
                    let data_offset = column_offset + row_in_bucket * width;
                    read_fixed_ssm_string(&cached_bucket[data_offset..data_offset + width])?
                } else {
                    let ref_offset = column_offset + row_in_bucket * 12;
                    let str_bucket = read_i32_canonical(&cached_bucket[ref_offset..], be);
                    let str_offset = read_i32_canonical(&cached_bucket[ref_offset + 4..], be);
                    let str_length = read_i32_canonical(&cached_bucket[ref_offset + 8..], be);
                    if str_length <= 8 {
                        String::from_utf8(
                            cached_bucket[ref_offset..ref_offset + str_length as usize].to_vec(),
                        )
                        .map_err(|err| {
                            StorageError::FormatMismatch(format!("invalid UTF-8: {err}"))
                        })?
                    } else {
                        read_ssm_string(
                            &mut file,
                            &header,
                            &mut payload_buckets,
                            str_bucket,
                            str_offset,
                            str_length,
                        )?
                    }
                };
                Value::Scalar(ScalarValue::String(string))
            }
            _ => {
                let data_start = column_offset + row_in_bucket * elem_bytes;
                let data_end = data_start + elem_bytes;
                extract_scalar_from_ssm_bytes(
                    &cached_bucket[data_start..data_end],
                    col_desc.data_type,
                    be,
                )?
            }
        };
        values[out_idx] = Some(match value {
            Value::Scalar(scalar) => scalar,
            other => {
                return Err(StorageError::FormatMismatch(format!(
                    "SSM column '{}' expected scalar value, found {:?}",
                    col_desc.col_name,
                    other.kind()
                )));
            }
        });
    }

    Ok(Some(values))
}

/// Read several required scalar columns while visiting each selected bucket once.
///
/// The data-manager column indices are retained because an SSM group can bind
/// array columns between the requested scalar columns. Columns with different
/// SSM indices remain supported; each index keeps only its current bucket.
pub(crate) fn read_ssm_required_scalar_columns_rows(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[(usize, &ColumnDescContents)],
    selected_rows: &[usize],
) -> Result<HashMap<String, RequiredScalarColumnData>, StorageError> {
    let mut outputs = HashMap::with_capacity(col_descs.len());
    read_ssm_required_scalar_columns_rows_reusing(
        file_path,
        dm_blob,
        col_descs,
        selected_rows,
        &mut outputs,
    )?;
    Ok(outputs)
}

pub(crate) fn read_ssm_required_scalar_columns_rows_reusing(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[(usize, &ColumnDescContents)],
    selected_rows: &[usize],
    outputs: &mut HashMap<String, RequiredScalarColumnData>,
) -> Result<(), StorageError> {
    for (_, col_desc) in col_descs {
        if col_desc.is_array || col_desc.is_record() {
            continue;
        }
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
                    "required selected-row SSM column '{}' has incompatible type {other:?}",
                    col_desc.col_name
                )));
            }
        }
    }
    if selected_rows.is_empty() || outputs.is_empty() {
        return Ok(());
    }

    let mut file = DataFile::open(file_path)?;
    let header = parse_ssm_header(&mut file)?;
    let dm_info = parse_ssm_dm_blob(dm_blob)?;
    let indices = parse_ssm_indices(&mut file, &header)?;
    let contiguous_rows = selected_rows
        .windows(2)
        .all(|rows| rows[1] == rows[0].saturating_add(1));
    let index_numbers = col_descs
        .iter()
        .map(|(dm_col_idx, _)| dm_info.col_index_map.get(*dm_col_idx).copied())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            StorageError::FormatMismatch(
                "SSM required scalar columns are missing an index mapping".to_string(),
            )
        })?;
    let shared_index_nr = index_numbers
        .first()
        .copied()
        .filter(|first| index_numbers.iter().all(|value| value == first));
    if contiguous_rows && let Some(index_nr) = shared_index_nr {
        let mut buckets = ReadBuckets::new(file)?;
        let index = indices.get(index_nr as usize).ok_or_else(|| {
            StorageError::FormatMismatch(format!(
                "SSM required scalar columns reference missing index {index_nr}"
            ))
        })?;
        let mut row_index = selected_rows[0];
        let mut out_start = 0usize;
        while out_start < selected_rows.len() {
            let (bucket_nr, bucket_start, bucket_end) =
                index.find_bucket(row_index as u64).ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "SSM index has no bucket for row {row_index}"
                    ))
                })?;
            let bucket = buckets.read(&header, bucket_nr)?;
            let row_in_bucket = row_index - bucket_start as usize;
            let row_count =
                (bucket_end as usize - row_index + 1).min(selected_rows.len() - out_start);
            for (dm_col_idx, col_desc) in col_descs {
                let column_offset = *dm_info.column_offsets.get(*dm_col_idx).ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "SSM scalar column '{}' missing column offset",
                        col_desc.col_name
                    ))
                })? as usize;
                let output = outputs.get_mut(&col_desc.col_name).ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "SSM scalar column '{}' missing typed output",
                        col_desc.col_name
                    ))
                })?;
                copy_ssm_required_scalar_range(
                    output,
                    bucket,
                    column_offset,
                    row_in_bucket,
                    out_start,
                    row_count,
                    header.big_endian,
                );
            }
            row_index += row_count;
            out_start += row_count;
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

    // index_nr -> (bucket_nr, bucket start row, bytes)
    let mut cached_buckets: HashMap<usize, (u32, u64, Vec<u8>)> = HashMap::new();
    for (row_index, out_idx) in requests {
        for (dm_col_idx, col_desc) in col_descs {
            let Some(output) = outputs.get_mut(&col_desc.col_name) else {
                continue;
            };
            let column_offset = *dm_info.column_offsets.get(*dm_col_idx).ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "SSM scalar column '{}' missing column offset",
                    col_desc.col_name
                ))
            })? as usize;
            let index_nr = *dm_info.col_index_map.get(*dm_col_idx).ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "SSM scalar column '{}' missing index mapping",
                    col_desc.col_name
                ))
            })? as usize;
            let index = indices.get(index_nr).ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "SSM scalar column '{}' references missing index {index_nr}",
                    col_desc.col_name
                ))
            })?;
            let (bucket_nr, start_row, _end_row) =
                index.find_bucket(row_index as u64).ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "SSM index has no bucket for row {row_index}"
                    ))
                })?;
            if cached_buckets
                .get(&index_nr)
                .is_none_or(|(cached_nr, _, _)| *cached_nr != bucket_nr)
            {
                let bucket = read_bucket(&mut file, &header, bucket_nr)?;
                cached_buckets.insert(index_nr, (bucket_nr, start_row, bucket));
            }
            let (_, cached_start_row, bucket) = cached_buckets
                .get(&index_nr)
                .expect("SSM bucket inserted before selected scalar decode");
            let row_in_bucket = row_index as u64 - *cached_start_row;
            match output {
                RequiredScalarColumnData::Bool(values) => {
                    let byte_offset = column_offset + row_in_bucket as usize / 8;
                    let bit_offset = row_in_bucket as usize % 8;
                    values[out_idx] = ((bucket[byte_offset] >> bit_offset) & 1) != 0;
                }
                RequiredScalarColumnData::Int32(values) => {
                    let offset = column_offset + row_in_bucket as usize * 4;
                    values[out_idx] =
                        read_i32_canonical(&bucket[offset..offset + 4], header.big_endian);
                }
                RequiredScalarColumnData::Float32(values) => {
                    let offset = column_offset + row_in_bucket as usize * 4;
                    values[out_idx] = if header.big_endian {
                        read_f32_be(&bucket[offset..offset + 4])
                    } else {
                        read_f32_le(&bucket[offset..offset + 4])
                    };
                }
                RequiredScalarColumnData::Float64(values) => {
                    let offset = column_offset + row_in_bucket as usize * 8;
                    values[out_idx] = if header.big_endian {
                        read_f64_be(&bucket[offset..offset + 8])
                    } else {
                        read_f64_le(&bucket[offset..offset + 8])
                    };
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn copy_ssm_required_scalar_range(
    output: &mut RequiredScalarColumnData,
    bucket: &[u8],
    column_offset: usize,
    source_row_start: usize,
    output_row_start: usize,
    row_count: usize,
    big_endian: bool,
) {
    match output {
        RequiredScalarColumnData::Bool(values) => {
            for row_offset in 0..row_count {
                let source_row = source_row_start + row_offset;
                values[output_row_start + row_offset] =
                    ((bucket[column_offset + source_row / 8] >> (source_row % 8)) & 1) != 0;
            }
        }
        RequiredScalarColumnData::Int32(values) => {
            let source_start = column_offset + source_row_start * 4;
            let source = &bucket[source_start..source_start + row_count * 4];
            let output = &mut values[output_row_start..output_row_start + row_count];
            if big_endian == cfg!(target_endian = "big") {
                scalar_slice_as_bytes_mut(output).copy_from_slice(source);
            } else {
                for (value, bytes) in output.iter_mut().zip(source.as_chunks::<4>().0) {
                    *value = read_i32_canonical(bytes, big_endian);
                }
            }
        }
        RequiredScalarColumnData::Float32(values) => {
            let source_start = column_offset + source_row_start * 4;
            let source = &bucket[source_start..source_start + row_count * 4];
            let output = &mut values[output_row_start..output_row_start + row_count];
            if big_endian == cfg!(target_endian = "big") {
                scalar_slice_as_bytes_mut(output).copy_from_slice(source);
            } else {
                for (value, bytes) in output.iter_mut().zip(source.as_chunks::<4>().0) {
                    *value = if big_endian {
                        read_f32_be(bytes)
                    } else {
                        read_f32_le(bytes)
                    };
                }
            }
        }
        RequiredScalarColumnData::Float64(values) => {
            let source_start = column_offset + source_row_start * 8;
            let source = &bucket[source_start..source_start + row_count * 8];
            let output = &mut values[output_row_start..output_row_start + row_count];
            if big_endian == cfg!(target_endian = "big") {
                scalar_slice_as_bytes_mut(output).copy_from_slice(source);
            } else {
                for (value, bytes) in output.iter_mut().zip(source.as_chunks::<8>().0) {
                    *value = if big_endian {
                        read_f64_be(bytes)
                    } else {
                        read_f64_le(bytes)
                    };
                }
            }
        }
    }
}

/// One cell of an indirect SSM array column, read at `offset` of its array
/// file. A table without an array file has only undefined cells.
pub(super) fn read_ssm_indirect_cell(
    reader: Option<&mut StManArrayFileReader>,
    offset: i64,
    data_type: CasacoreDataType,
    col_desc: &ColumnDescContents,
    row: usize,
) -> Result<Option<ArrayValue>, StorageError> {
    let Some(reader) = reader else {
        return if offset == 0 {
            Ok(None)
        } else {
            Err(StorageError::FormatMismatch(
                "SSM indirect array column but no array file found".to_string(),
            ))
        };
    };
    let value = reader.read_array_at(offset, data_type).map_err(|err| {
        StorageError::FormatMismatch(format!(
            "SSM indirect array column '{}' row {row} offset {offset} type {:?}: {err}",
            col_desc.col_name, data_type
        ))
    })?;
    match value {
        Some(Value::Array(array)) => Ok(Some(array)),
        Some(other) => Err(StorageError::FormatMismatch(format!(
            "SSM indirect array column '{}' row {row} expected array value, found {:?}",
            col_desc.col_name,
            other.kind()
        ))),
        None => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
pub(super) fn read_column_from_buckets(
    file: &mut impl DataRead,
    header: &SsmHeader,
    index: &SsmIndex,
    column_offset: usize,
    data_type: CasacoreDataType,
    max_length: u32,
    nrelem: usize,
    first_row: usize,
    nrrow: usize,
) -> Result<ColumnRawData, StorageError> {
    let (elem_bytes, _) = canonical_element_size(data_type);
    let be = header.big_endian;

    match data_type {
        CasacoreDataType::TpBool => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            let mut row = first_row;
            let end_exclusive = first_row + nrrow;
            while row < end_exclusive {
                let (bucket_nr, start_row, end_row) =
                    index.find_bucket(row as u64).ok_or_else(|| {
                        StorageError::FormatMismatch(format!(
                            "SSM index has no bucket for row {row}"
                        ))
                    })?;
                let bucket = read_bucket(file, header, bucket_nr)?;
                let row_in_bucket = (row as u64 - start_row) as usize;
                let rows_in_chunk =
                    ((end_row - start_row + 1) as usize - row_in_bucket).min(end_exclusive - row);
                let bit_offset = row_in_bucket * nrelem;
                let byte_offset = column_offset + bit_offset / 8;
                let sub_bit = bit_offset % 8;
                let bools = read_bool_bits(&bucket[byte_offset..], sub_bit, rows_in_chunk * nrelem);
                values.extend_from_slice(&bools);
                row += rows_in_chunk;
            }
            Ok(ColumnRawData::Bool(values))
        }
        CasacoreDataType::TpUChar => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                first_row,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend_from_slice(&data[..count]);
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::UInt8(values))
        }
        CasacoreDataType::TpShort => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                first_row,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_i16_slice_be(data, count)
                    } else {
                        read_i16_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Int16(values))
        }
        CasacoreDataType::TpUShort => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                first_row,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_u16_slice_be(data, count)
                    } else {
                        read_u16_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::UInt16(values))
        }
        CasacoreDataType::TpInt => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                first_row,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_i32_slice_be(data, count)
                    } else {
                        read_i32_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Int32(values))
        }
        CasacoreDataType::TpUInt => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                first_row,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_u32_slice_be(data, count)
                    } else {
                        read_u32_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::UInt32(values))
        }
        CasacoreDataType::TpFloat => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                first_row,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_f32_slice_be(data, count)
                    } else {
                        read_f32_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Float32(values))
        }
        CasacoreDataType::TpDouble => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                first_row,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_f64_slice_be(data, count)
                    } else {
                        read_f64_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Float64(values))
        }
        CasacoreDataType::TpInt64 => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                first_row,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    values.extend(if be {
                        read_i64_slice_be(data, count)
                    } else {
                        read_i64_slice_le(data, count)
                    });
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Int64(values))
        }
        CasacoreDataType::TpComplex => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                first_row,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    for i in 0..count {
                        let (re, im) = if be {
                            (read_f32_be(&data[i * 8..]), read_f32_be(&data[i * 8 + 4..]))
                        } else {
                            (read_f32_le(&data[i * 8..]), read_f32_le(&data[i * 8 + 4..]))
                        };
                        values.push(casa_core::Complex32::new(re, im));
                    }
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Complex32(values))
        }
        CasacoreDataType::TpDComplex => {
            let mut values = Vec::with_capacity(nrrow * nrelem);
            read_typed_column(
                file,
                header,
                index,
                column_offset,
                first_row,
                nrrow,
                nrelem,
                elem_bytes,
                |data, count| {
                    for i in 0..count {
                        let (re, im) = if be {
                            (
                                read_f64_be(&data[i * 16..]),
                                read_f64_be(&data[i * 16 + 8..]),
                            )
                        } else {
                            (
                                read_f64_le(&data[i * 16..]),
                                read_f64_le(&data[i * 16 + 8..]),
                            )
                        };
                        values.push(casa_core::Complex64::new(re, im));
                    }
                    Ok(())
                },
            )?;
            Ok(ColumnRawData::Complex64(values))
        }
        CasacoreDataType::TpString => {
            // C++ SSM stores ONE 12-byte reference per row for strings.
            // Scalars (nrelem=1): short strings (<=8 bytes) inline, long in bucket.
            // Arrays (nrelem>1): all elements serialized as [BE_i32_len][data]
            // pairs in a string bucket, one reference per row.
            let mut values = Vec::with_capacity(nrrow * nrelem);
            let fixed_width = max_length as usize;
            let mut data_buckets = LastBucket::default();
            let mut payload_buckets = LastBucket::default();
            for row in first_row..first_row + nrrow {
                let (bucket_nr, start_row, _end_row) =
                    index.find_bucket(row as u64).ok_or_else(|| {
                        StorageError::FormatMismatch(format!(
                            "SSM index has no bucket for row {row}"
                        ))
                    })?;
                let bucket = data_buckets.read(file, header, bucket_nr)?;
                let row_in_bucket = (row as u64 - start_row) as usize;
                if fixed_width > 0 {
                    let row_width = fixed_width * nrelem;
                    let row_offset = column_offset + row_in_bucket * row_width;
                    for elem in 0..nrelem {
                        let elem_offset = row_offset + elem * fixed_width;
                        values.push(read_fixed_ssm_string(
                            &bucket[elem_offset..elem_offset + fixed_width],
                        )?);
                    }
                    continue;
                }
                let ref_offset = column_offset + row_in_bucket * 12;
                let str_bucket = read_i32_canonical(&bucket[ref_offset..], be);
                let str_offset = read_i32_canonical(&bucket[ref_offset + 4..], be);
                let str_length = read_i32_canonical(&bucket[ref_offset + 8..], be);

                if nrelem == 1 {
                    let s = if str_length <= 8 {
                        String::from_utf8(
                            bucket[ref_offset..ref_offset + str_length as usize].to_vec(),
                        )
                        .map_err(|e| StorageError::FormatMismatch(format!("invalid UTF-8: {e}")))?
                    } else {
                        read_ssm_string(
                            file,
                            header,
                            &mut payload_buckets,
                            str_bucket,
                            str_offset,
                            str_length,
                        )?
                    };
                    values.push(s);
                } else if str_length == 0 {
                    for _ in 0..nrelem {
                        values.push(String::new());
                    }
                } else {
                    let block = read_ssm_string_bytes(
                        file,
                        header,
                        &mut payload_buckets,
                        str_bucket,
                        str_offset,
                        str_length,
                    )?;
                    let mut pos = 0usize;
                    for _ in 0..nrelem {
                        if pos + 4 > block.len() {
                            return Err(StorageError::FormatMismatch(
                                "SSM string array: block too short for length prefix".to_string(),
                            ));
                        }
                        let slen = u32::from_be_bytes([
                            block[pos],
                            block[pos + 1],
                            block[pos + 2],
                            block[pos + 3],
                        ]) as usize;
                        pos += 4;
                        if pos + slen > block.len() {
                            return Err(StorageError::FormatMismatch(
                                "SSM string array: block too short for string data".to_string(),
                            ));
                        }
                        let s =
                            String::from_utf8(block[pos..pos + slen].to_vec()).map_err(|e| {
                                StorageError::FormatMismatch(format!("invalid UTF-8: {e}"))
                            })?;
                        pos += slen;
                        values.push(s);
                    }
                }
            }
            Ok(ColumnRawData::String(values))
        }
        _ => Err(StorageError::FormatMismatch(format!(
            "unsupported SSM data type: {data_type:?}"
        ))),
    }
}

/// Iterate over data buckets and process column data for non-Bool, non-String types.
#[allow(clippy::too_many_arguments)]
pub(super) fn read_typed_column<F>(
    file: &mut impl DataRead,
    header: &SsmHeader,
    index: &SsmIndex,
    column_offset: usize,
    first_row: usize,
    nrrow: usize,
    nrelem: usize,
    elem_bytes: usize,
    mut process: F,
) -> Result<(), StorageError>
where
    F: FnMut(&[u8], usize) -> Result<(), StorageError>,
{
    let bytes_per_row = elem_bytes * nrelem;
    let mut row = first_row;
    let end_exclusive = first_row + nrrow;

    while row < end_exclusive {
        let (bucket_nr, start_row, end_row) = index.find_bucket(row as u64).ok_or_else(|| {
            StorageError::FormatMismatch(format!("SSM index has no bucket for row {row}"))
        })?;
        let bucket = read_bucket(file, header, bucket_nr)?;
        let row_in_bucket = (row as u64 - start_row) as usize;
        let rows_in_chunk =
            ((end_row - start_row + 1) as usize - row_in_bucket).min(end_exclusive - row);
        let data_start = column_offset + row_in_bucket * bytes_per_row;
        process(&bucket[data_start..], rows_in_chunk * nrelem)?;
        row += rows_in_chunk;
    }

    Ok(())
}
