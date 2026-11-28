// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Replace one manager from retained cells, preserving its bucket layout options.
pub(crate) fn rewrite_ssm_columns(
    original: &Path,
    destination: &Path,
    columns: &[ColumnDescContents],
    row_count: usize,
    name: &str,
    cell: impl Fn(usize, usize) -> Result<Option<Value>, StorageError>,
) -> Result<Vec<u8>, StorageError> {
    struct Cells<F> {
        row_count: usize,
        cell: F,
    }
    impl<F: Fn(usize, usize) -> Result<Option<Value>, StorageError>> column_writer::SsmCells
        for Cells<F>
    {
        fn row_count(&self) -> usize {
            self.row_count
        }

        fn cell(
            &self,
            column: usize,
            row: usize,
        ) -> Result<Option<std::borrow::Cow<'_, Value>>, StorageError> {
            Ok((self.cell)(column, row)?.map(std::borrow::Cow::Owned))
        }
    }
    let header = parse_ssm_header(&mut File::open(original)?)?;
    column_writer::write_ssm_file_columns_with_layout(
        destination,
        columns,
        &Cells { row_count, cell },
        header.big_endian,
        false,
        name,
        Some((Some(header.bucket_size), header.pers_cache_size)),
    )
}

pub(crate) fn write_ssm_with_bucket(
    destination: &Path,
    columns: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    big_endian: bool,
    name: &str,
    bucket_and_cache: Option<(Option<u32>, u32)>,
) -> Result<Vec<u8>, StorageError> {
    column_writer::write_ssm_file_columns_with_layout(
        destination,
        columns,
        &column_writer::RowCells::new(rows, None, columns),
        big_endian,
        true,
        name,
        bucket_and_cache,
    )
}

pub(crate) fn write_ssm_file_indexed(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    field_indices: &[usize],
    big_endian: bool,
    manager_name: &str,
) -> Result<Vec<u8>, StorageError> {
    write_ssm_file_impl(
        file_path,
        col_descs,
        rows,
        Some(field_indices),
        big_endian,
        false,
        manager_name,
    )
}

pub(crate) fn write_ssm_file_scalar_columns(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    scalar_columns: &[&[Option<ScalarValue>]],
    big_endian: bool,
    manager_name: &str,
) -> Result<Vec<u8>, StorageError> {
    let nrrow = scalar_columns.first().map_or(0, |values| values.len());
    if scalar_columns.len() != col_descs.len() {
        return Err(StorageError::FormatMismatch(format!(
            "SSM scalar column count mismatch: expected {}, got {}",
            col_descs.len(),
            scalar_columns.len()
        )));
    }
    for values in scalar_columns.iter().skip(1) {
        if values.len() != nrrow {
            return Err(StorageError::FormatMismatch(
                "SSM scalar columns have inconsistent row counts".to_string(),
            ));
        }
    }
    if col_descs
        .iter()
        .any(|col_desc| col_desc.is_array || col_desc.is_record())
    {
        return Err(StorageError::FormatMismatch(
            "SSM scalar column writer only supports scalar non-record columns".to_string(),
        ));
    }
    let column_sources = ColumnSources::materialized(nrrow, scalar_columns);
    column_writer::write_ssm_file_columns(
        file_path,
        col_descs,
        &column_sources,
        big_endian,
        true,
        manager_name,
    )
}

/// Write a StandardStMan data file from column sources: fixed-width scalar
/// groups a bucket at a time, any other mix of scalars, strings and arrays
/// column by column. Both hold a bounded amount whatever the row count.
pub(crate) fn write_ssm_file_column_sources(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    sources: &ColumnSources<'_>,
    big_endian: bool,
    manager_name: &str,
) -> Result<Vec<u8>, StorageError> {
    if sources.column_count() != col_descs.len() {
        return Err(StorageError::FormatMismatch(format!(
            "SSM column source count mismatch: expected {}, got {}",
            col_descs.len(),
            sources.column_count()
        )));
    }
    if col_descs.iter().all(|desc| {
        !desc.is_array && !desc.is_record() && desc.data_type != CasacoreDataType::TpString
    }) {
        return write_ssm_fixed_scalar_sources_streaming(
            file_path,
            col_descs,
            sources,
            big_endian,
            manager_name,
        );
    }
    column_writer::write_ssm_file_columns(
        file_path,
        col_descs,
        sources,
        big_endian,
        false,
        manager_name,
    )
}

pub(super) fn write_ssm_file_impl(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    field_indices: Option<&[usize]>,
    big_endian: bool,
    casacore_default_small: bool,
    dm_name: &str,
) -> Result<Vec<u8>, StorageError> {
    column_writer::write_ssm_file_columns(
        file_path,
        col_descs,
        &column_writer::RowCells::new(rows, field_indices, col_descs),
        big_endian,
        casacore_default_small,
        dm_name,
    )
}

/// The writer [`column_writer`] replaced, which assembles every bucket in
/// memory; kept to check the two write the same bytes.
#[cfg(test)]
#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
pub(super) fn write_ssm_file_in_memory(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    field_indices: Option<&[usize]>,
    scalar_columns: Option<&ColumnSources<'_>>,
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    let nrrow = scalar_columns
        .map(ColumnSources::row_count)
        .unwrap_or(rows.len());
    let ncol = col_descs.len();

    if let Some(scalar_columns) = scalar_columns
        && col_descs.iter().all(|desc| {
            !desc.is_array && !desc.is_record() && desc.data_type != CasacoreDataType::TpString
        })
    {
        return write_ssm_fixed_scalar_sources_streaming(
            file_path,
            col_descs,
            scalar_columns,
            big_endian,
            "SSM",
        );
    }

    // Check if any array columns are stored indirectly.
    let has_indirect = col_descs.iter().any(is_ssm_indirect);

    // Create shared array file for indirect columns (SSM uses version 0, no refcount).
    let mut array_writer = if has_indirect {
        let mut arr_path = file_path.as_os_str().to_os_string();
        arr_path.push("i");
        Some(StManArrayFileWriter::create(
            &std::path::PathBuf::from(arr_path),
            big_endian,
            0,
        )?)
    } else {
        None
    };

    // 1. Compute column sizes in bits (for Bool bit-packing)
    // For indirect columns: 8 bytes (i64 offset) per row.
    // String columns always use 96 bits (12 bytes) per row for a 3-int reference,
    // regardless of array element count — all string data goes to string buckets.
    let col_sizes_bits: Vec<usize> = col_descs
        .iter()
        .map(|c| {
            if is_ssm_fixed_string(c) {
                c.max_length as usize * ssm_cell_element_count(c) * 8
            } else if is_ssm_variable_string(c) {
                96 // 12 bytes = 3 ints (bucketNr, offset, length) per row
            } else if is_ssm_indirect(c) {
                64 // i64 offset = 8 bytes = 64 bits
            } else {
                let nrelem = ssm_cell_element_count(c);
                let (_, bits) = canonical_element_size(c.data_type);
                nrelem * bits
            }
        })
        .collect();

    // 2. Compute rows_per_bucket and bucket_size
    let (rows_per_bucket, bucket_size) = new_ssm_bucket_layout(&col_sizes_bits, nrrow);

    // 3. Compute column offsets within a data bucket
    let mut column_offsets = vec![0u32; ncol];
    let mut offset = 0u32;
    for (i, &bits) in col_sizes_bits.iter().enumerate() {
        column_offsets[i] = offset;
        offset += (rows_per_bucket as usize * bits).div_ceil(8) as u32;
    }

    // 4. Build data buckets
    let nr_data_buckets = if nrrow == 0 {
        0
    } else {
        nrrow.div_ceil(rows_per_bucket as usize)
    };

    let mut buckets: Vec<Vec<u8>> = (0..nr_data_buckets)
        .map(|_| vec![0u8; bucket_size as usize])
        .collect();

    // String handling: collect long strings into string buckets
    let mut string_buckets = StringBuckets::after(nr_data_buckets);

    for (col_idx, col_desc) in col_descs.iter().enumerate() {
        let is_indirect = is_ssm_indirect(col_desc);
        let nrelem = if is_indirect {
            1usize
        } else if col_desc.is_array && !col_desc.shape.is_empty() {
            col_desc.shape.iter().map(|&s| s as usize).product()
        } else {
            1usize
        };
        let col_off = column_offsets[col_idx] as usize;

        if is_indirect {
            // Write array data to shared file, store i64 offsets in buckets.
            let dt = if col_desc.is_record() {
                CasacoreDataType::TpUChar
            } else {
                CasacoreDataType::from_primitive_type(col_desc.require_primitive_type()?, false)
            };
            let writer = array_writer.as_mut().ok_or_else(|| {
                StorageError::FormatMismatch(
                    "no array file writer for indirect SSM column".to_string(),
                )
            })?;

            for row in 0..nrrow {
                let bucket_idx = row / rows_per_bucket as usize;
                let row_in_bucket = row % rows_per_bucket as usize;
                let bucket = &mut buckets[bucket_idx];
                let byte_off = col_off + row_in_bucket * 8;

                let scalar_value;
                let value = if let Some(columns) = scalar_columns {
                    scalar_value = columns.value(col_idx, row)?.map(Value::Scalar);
                    scalar_value.as_ref()
                } else {
                    let row_record = rows.get(row).ok_or_else(|| {
                        StorageError::FormatMismatch(format!(
                            "SSM row writer missing row {row} for column {}",
                            col_desc.col_name
                        ))
                    })?;
                    row_record
                        .fields()
                        .get(
                            field_indices
                                .and_then(|indices| indices.get(col_idx))
                                .copied()
                                .unwrap_or(usize::MAX),
                        )
                        .filter(|field| field.name == col_desc.col_name)
                        .map(|field| &field.value)
                        .or_else(|| {
                            row_record
                                .fields()
                                .iter()
                                .find(|f| f.name == col_desc.col_name)
                                .map(|f| &f.value)
                        })
                };

                let offset: i64 = match value {
                    Some(Value::Record(record)) if col_desc.is_record() => {
                        let bytes = super::super::table_control::serialize_record_to_uchar(record)?;
                        let payload = Value::Array(ArrayValue::UInt8(
                            ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[bytes.len()]), bytes)
                                .map_err(|err| {
                                    StorageError::FormatMismatch(format!(
                                        "SSM record column '{}': invalid byte payload shape: {err}",
                                        col_desc.col_name
                                    ))
                                })?,
                        ));
                        writer.write_array(&payload, dt)?
                    }
                    Some(val @ Value::Array(_)) if !col_desc.is_record() => {
                        writer.write_array(val, dt)?
                    }
                    Some(Value::Array(_)) | Some(Value::Record(_)) => {
                        return Err(StorageError::FormatMismatch(format!(
                            "SSM indirect column '{}' received incompatible value type",
                            col_desc.col_name
                        )));
                    }
                    _ => 0i64, // undefined cell
                };

                // Write i64 offset in canonical byte order.
                if big_endian {
                    write_i64_be(&mut bucket[byte_off..], offset);
                } else {
                    write_i64_le(&mut bucket[byte_off..], offset);
                }
            }
        } else {
            for row in 0..nrrow {
                let bucket_idx = row / rows_per_bucket as usize;
                let row_in_bucket = row % rows_per_bucket as usize;
                let bucket = &mut buckets[bucket_idx];

                let scalar_value;
                let value = if let Some(columns) = scalar_columns {
                    scalar_value = columns.value(col_idx, row)?.map(Value::Scalar);
                    scalar_value.as_ref()
                } else {
                    let row_record = rows.get(row).ok_or_else(|| {
                        StorageError::FormatMismatch(format!(
                            "SSM row writer missing row {row} for column {}",
                            col_desc.col_name
                        ))
                    })?;
                    row_record
                        .fields()
                        .get(
                            field_indices
                                .and_then(|indices| indices.get(col_idx))
                                .copied()
                                .unwrap_or(usize::MAX),
                        )
                        .filter(|field| field.name == col_desc.col_name)
                        .map(|field| &field.value)
                        .or_else(|| {
                            row_record
                                .fields()
                                .iter()
                                .find(|f| f.name == col_desc.col_name)
                                .map(|f| &f.value)
                        })
                };

                write_cell_to_bucket(
                    bucket,
                    col_off,
                    row_in_bucket,
                    col_desc,
                    col_desc.data_type,
                    nrelem,
                    value,
                    &mut |data| Ok(string_buckets.allocate(data, bucket_size as usize)),
                    nr_data_buckets,
                    big_endian,
                )?;
            }
        }
    }

    // Flush the array file header and buffer if we wrote indirect arrays.
    if let Some(ref mut w) = array_writer {
        w.finish()?;
    }

    // 5. Build the SSMIndex
    let mut last_row = Vec::with_capacity(nr_data_buckets);
    let mut bucket_number = Vec::with_capacity(nr_data_buckets);
    for i in 0..nr_data_buckets {
        let end_row = ((i + 1) * rows_per_bucket as usize - 1).min(nrrow - 1);
        last_row.push(end_row as u64);
        bucket_number.push(i as u32);
    }

    // 6. Serialize the SSMIndex to bytes (byte order matches table endian)
    let index_data = serialize_ssm_index(
        nr_data_buckets as u32,
        rows_per_bucket,
        ncol as i32,
        &last_row,
        &bucket_number,
        big_endian,
    )?;

    // 7. Write the index into index buckets
    let chain_overhead = 8usize; // check number + next bucket pointer
    let data_per_index_bucket = bucket_size as usize - chain_overhead;
    let nr_idx_buckets = if index_data.is_empty() {
        0
    } else {
        index_data.len().div_ceil(data_per_index_bucket)
    };

    // String bucket count (each goes after data buckets, before index buckets)
    let nr_string_buckets = string_buckets.len();
    let first_idx_bucket = (nr_data_buckets + nr_string_buckets) as i32;
    let nr_buckets = nr_data_buckets + nr_string_buckets + nr_idx_buckets;

    // 8. Write the 512-byte header (BE AipsIO framing, v3)
    let header_buf = serialize_ssm_header(
        bucket_size,
        nr_buckets as u32,
        0,  // nr_free_buckets
        -1, // first_free_bucket
        nr_idx_buckets as u32,
        first_idx_bucket,
        index::index_bucket_offset(index_data.len(), bucket_size as usize),
        last_string_bucket(nr_data_buckets, nr_string_buckets),
        index_data.len() as u32,
        1, // nr_indices (single index for all columns)
        2, // persistent cache size
        big_endian,
    )?;

    // 9. Assemble the file
    let mut file = File::create(file_path)?;
    file.write_all(&header_buf)?;

    // Data buckets
    for bucket in &buckets {
        file.write_all(bucket)?;
    }

    // String buckets
    for sb in string_buckets.held() {
        file.write_all(sb)?;
    }

    // Index buckets
    let mut idx_remaining = &index_data[..];
    for i in 0..nr_idx_buckets {
        let mut idx_bucket = vec![0u8; bucket_size as usize];
        let next = if i + 1 < nr_idx_buckets {
            (first_idx_bucket as usize + i + 1) as i32
        } else {
            -1i32
        };
        index::write_chain_header(&mut idx_bucket, next);
        // Copy index data
        let chunk = idx_remaining.len().min(data_per_index_bucket);
        idx_bucket[chain_overhead..chain_overhead + chunk].copy_from_slice(&idx_remaining[..chunk]);
        idx_remaining = &idx_remaining[chunk..];
        file.write_all(&idx_bucket)?;
    }

    // 10. Generate the DM data blob (always big-endian, file-based AipsIO)
    let dm_blob = serialize_ssm_dm_blob("SSM", &column_offsets, ncol)?;

    Ok(dm_blob)
}

/// Write fixed-width scalar sources with one resident SSM bucket: the data
/// buckets in row order, then the streamed index ([`index`]).
///
/// Fixed-width scalar groups have no string buckets or array file, so,
/// unlike the column writer, they are written a whole data bucket at a
/// time, each column a block of cells at a time where its source allows.
pub(super) fn write_ssm_fixed_scalar_sources_streaming(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    scalar_columns: &ColumnSources<'_>,
    big_endian: bool,
    manager_name: &str,
) -> Result<Vec<u8>, StorageError> {
    let nrrow = scalar_columns.row_count();
    let ncol = col_descs.len();
    let col_sizes_bits: Vec<usize> = col_descs
        .iter()
        .map(|desc| canonical_element_size(desc.data_type).1)
        .collect();
    let (rows_per_bucket, bucket_size) = new_ssm_bucket_layout(&col_sizes_bits, nrrow);
    let mut column_offsets = vec![0u32; ncol];
    let mut offset = 0u32;
    for (index, &bits) in col_sizes_bits.iter().enumerate() {
        column_offsets[index] = offset;
        offset += (rows_per_bucket as usize * bits).div_ceil(8) as u32;
    }

    let nr_data_buckets = if nrrow == 0 {
        0
    } else {
        nrrow.div_ceil(rows_per_bucket as usize)
    };

    let mut file = File::create(file_path)?;
    // The header, written once the index is.
    file.write_all(&[0u8; SSM_HEADER_SIZE as usize])?;
    // One block of encoded cells, at most 8 bytes each, whichever column
    // fills it.
    let widest_cell = col_sizes_bits
        .iter()
        .map(|bits| bits.div_ceil(8).min(8))
        .max()
        .unwrap_or(0);
    let mut encoded =
        Vec::with_capacity(SCALAR_BLOCK_CELLS.min(rows_per_bucket as usize) * widest_cell);
    for bucket_index in 0..nr_data_buckets {
        let mut bucket = vec![0u8; bucket_size as usize];
        let first_row = bucket_index * rows_per_bucket as usize;
        let end_row = (first_row + rows_per_bucket as usize).min(nrrow);
        let rows_in_bucket = end_row - first_row;
        // A column is written whole when it is one constant run or its
        // source encodes a block of cells; the rest go cell by cell.
        let mut written_columns = vec![false; ncol];
        for (col_idx, col_desc) in col_descs.iter().enumerate() {
            let column_offset = column_offsets[col_idx] as usize;
            if let Some([run]) = scalar_columns.scalar_runs(col_idx)
                && run.start_row() == 0
            {
                written_columns[col_idx] = write_constant_scalar_column_to_bucket(
                    &mut bucket,
                    column_offset,
                    rows_in_bucket,
                    col_desc,
                    run.value(),
                    big_endian,
                );
            }
            if !written_columns[col_idx] {
                written_columns[col_idx] = write_source_cells_to_bucket(
                    &mut bucket[column_offset..],
                    scalar_columns,
                    col_idx,
                    first_row..end_row,
                    col_desc.data_type,
                    big_endian,
                    &mut encoded,
                )?;
            }
        }
        for row in first_row..end_row {
            let row_in_bucket = row - first_row;
            for (col_idx, col_desc) in col_descs.iter().enumerate() {
                if written_columns[col_idx] {
                    continue;
                }
                let scalar_value = scalar_columns.value(col_idx, row)?;
                let value = scalar_value.map(Value::Scalar);
                write_cell_to_bucket(
                    &mut bucket,
                    column_offsets[col_idx] as usize,
                    row_in_bucket,
                    col_desc,
                    col_desc.data_type,
                    1,
                    value.as_ref(),
                    &mut no_string_buckets,
                    nr_data_buckets,
                    big_endian,
                )?;
            }
        }
        file.write_all(&bucket)?;
    }

    index::write_index_and_header(
        &mut file,
        index::IndexLayout {
            nrrow,
            ncol,
            rows_per_bucket,
            bucket_size,
            nr_data_buckets,
            nr_string_buckets: 0,
            cache_size: 2,
        },
        big_endian,
    )?;
    serialize_ssm_dm_blob(manager_name, &column_offsets, ncol)
}

pub(super) fn write_constant_scalar_column_to_bucket(
    bucket: &mut [u8],
    column_offset: usize,
    row_count: usize,
    col_desc: &ColumnDescContents,
    value: Option<&ScalarValue>,
    big_endian: bool,
) -> bool {
    match (col_desc.data_type, value) {
        (_, None) | (CasacoreDataType::TpBool, Some(ScalarValue::Bool(false))) => true,
        (CasacoreDataType::TpBool, Some(ScalarValue::Bool(true))) => {
            let complete_bytes = row_count / 8;
            bucket[column_offset..column_offset + complete_bytes].fill(u8::MAX);
            let remaining_bits = row_count % 8;
            if remaining_bits > 0 {
                bucket[column_offset + complete_bytes] = (1u8 << remaining_bits) - 1;
            }
            true
        }
        (CasacoreDataType::TpInt, Some(ScalarValue::Int32(value))) => {
            let bytes = if big_endian {
                value.to_be_bytes()
            } else {
                value.to_le_bytes()
            };
            repeat_fixed_scalar_bytes(bucket, column_offset, row_count, &bytes);
            true
        }
        (CasacoreDataType::TpFloat, Some(ScalarValue::Float32(value))) => {
            let bytes = if big_endian {
                value.to_be_bytes()
            } else {
                value.to_le_bytes()
            };
            repeat_fixed_scalar_bytes(bucket, column_offset, row_count, &bytes);
            true
        }
        (CasacoreDataType::TpDouble, Some(ScalarValue::Float64(value))) => {
            let bytes = if big_endian {
                value.to_be_bytes()
            } else {
                value.to_le_bytes()
            };
            repeat_fixed_scalar_bytes(bucket, column_offset, row_count, &bytes);
            true
        }
        _ => false,
    }
}

pub(super) fn repeat_fixed_scalar_bytes(
    bucket: &mut [u8],
    column_offset: usize,
    row_count: usize,
    value: &[u8],
) {
    if row_count == 0 {
        return;
    }
    let width = value.len();
    bucket[column_offset..column_offset + width].copy_from_slice(value);
    let mut filled_rows = 1usize;
    while filled_rows < row_count {
        let copy_rows = filled_rows.min(row_count - filled_rows);
        bucket.copy_within(
            column_offset..column_offset + copy_rows * width,
            column_offset + filled_rows * width,
        );
        filled_rows += copy_rows;
    }
}

/// Write `column`'s cells `rows` to its area of a bucket, `dst`, a block of
/// [`SCALAR_BLOCK_CELLS`] at a time through `encoded`, if its source encodes
/// them ([`ColumnSources::encoded_cells`]). Returns whether it did;
/// otherwise the caller writes the cells one by one.
pub(super) fn write_source_cells_to_bucket(
    dst: &mut [u8],
    scalar_columns: &ColumnSources<'_>,
    column: usize,
    rows: std::ops::Range<usize>,
    data_type: CasacoreDataType,
    big_endian: bool,
    encoded: &mut Vec<u8>,
) -> Result<bool, StorageError> {
    let mut written_bytes = 0;
    let mut first = rows.start;
    while first < rows.end {
        let end = rows.end.min(first + SCALAR_BLOCK_CELLS);
        encoded.clear();
        if !scalar_columns.encoded_cells(column, first..end, data_type, big_endian, encoded)? {
            return Ok(false);
        }
        // A block of Bool cells starts on a byte of packed bits.
        let offset = if data_type == CasacoreDataType::TpBool {
            (first - rows.start) / 8
        } else {
            written_bytes
        };
        write_encoded_cells_to_bucket(&mut dst[offset..], data_type, encoded);
        written_bytes += encoded.len();
        first = end;
    }
    Ok(true)
}

/// Write a column's consecutive cells, encoded as
/// [`ColumnSources::encoded_cells`] gives them, from the start of `dst`:
/// a `Bool` as one bit per cell, other types as their bytes.
pub(super) fn write_encoded_cells_to_bucket(
    dst: &mut [u8],
    data_type: CasacoreDataType,
    encoded: &[u8],
) {
    if data_type == CasacoreDataType::TpBool {
        for (cell, &value) in encoded.iter().enumerate() {
            if value != 0 {
                dst[cell / 8] |= 1 << (cell % 8);
            }
        }
    } else {
        dst[..encoded.len()].copy_from_slice(encoded);
    }
}

/// Where a writer puts the bytes of a long string or a string array: it
/// stores them in its string buckets and returns the bucket (counted from
/// the first string bucket) and the offset in its data area where they
/// start ([`StringBuckets`]).
pub(super) type StoreStrings<'a> = dyn FnMut(&[u8]) -> Result<(usize, usize), StorageError> + 'a;

/// The string store of a writer whose columns hold no strings that need
/// string buckets: it refuses, so a locator never names a bucket that is
/// not written.
pub(super) fn no_string_buckets(_: &[u8]) -> Result<(usize, usize), StorageError> {
    Err(StorageError::FormatMismatch(
        "this StandardStMan writer has no string buckets".to_string(),
    ))
}

/// Write a single cell value into a data bucket using the specified byte
/// order, putting long strings and string arrays in `strings`, whose
/// buckets follow the file's `string_bucket_base` data buckets.
#[allow(clippy::too_many_arguments)]
pub(super) fn write_cell_to_bucket(
    bucket: &mut [u8],
    col_offset: usize,
    row_in_bucket: usize,
    col_desc: &ColumnDescContents,
    data_type: CasacoreDataType,
    nrelem: usize,
    value: Option<&casa_core::Value>,
    strings: &mut StoreStrings<'_>,
    string_bucket_base: usize,
    big_endian: bool,
) -> Result<(), StorageError> {
    use casa_core::{ArrayValue, ScalarValue, Value};

    match data_type {
        // A column's Bools are packed one bit each from the start of its
        // area, so a cell starts at bit `row_in_bucket * nrelem`, not on a
        // byte.
        CasacoreDataType::TpBool => {
            let bit_offset = row_in_bucket * nrelem;
            let byte_offset = col_offset + bit_offset / 8;
            let sub_bit = bit_offset % 8;
            write_bool_bits(
                &mut bucket[byte_offset..],
                sub_bit,
                &cell_bools(value.into(), nrelem),
            );
        }
        CasacoreDataType::TpString => {
            if col_desc.max_length > 0 {
                let width = col_desc.max_length as usize;
                let row_offset = col_offset + row_in_bucket * width * nrelem;
                if !col_desc.is_array {
                    let s = match value {
                        Some(Value::Scalar(ScalarValue::String(s))) => s.as_str(),
                        _ => "",
                    };
                    let bytes = s.as_bytes();
                    let copy_len = bytes.len().min(width);
                    bucket[row_offset..row_offset + copy_len].copy_from_slice(&bytes[..copy_len]);
                } else {
                    let strings = match value {
                        Some(Value::Array(ArrayValue::String(arr))) => {
                            arr.t().iter().cloned().collect::<Vec<_>>()
                        }
                        _ => vec![String::new(); nrelem],
                    };
                    for (elem, s) in strings.iter().take(nrelem).enumerate() {
                        let elem_offset = row_offset + elem * width;
                        let bytes = s.as_bytes();
                        let copy_len = bytes.len().min(width);
                        bucket[elem_offset..elem_offset + copy_len]
                            .copy_from_slice(&bytes[..copy_len]);
                    }
                }
                return Ok(());
            }
            // C++ SSM stores ONE 12-byte reference (bucketNr, offset, length)
            // per row, regardless of nrelem. For scalars (nrelem=1), short
            // strings (<=8 bytes) are inlined in the reference slot. For arrays
            // or long scalars, all element data goes to a string bucket as
            // [BE_i32_len][string_bytes] pairs. String bucket metadata is BE.
            let ref_offset = col_offset + row_in_bucket * 12;

            if !col_desc.is_array {
                // Scalar string
                let s = match value {
                    Some(Value::Scalar(ScalarValue::String(s))) => s.as_str(),
                    _ => "",
                };
                let len = s.len() as i32;
                if len <= 8 {
                    bucket[ref_offset..ref_offset + s.len()].copy_from_slice(s.as_bytes());
                    if big_endian {
                        write_i32_be(&mut bucket[ref_offset + 8..], len);
                    } else {
                        write_i32_le(&mut bucket[ref_offset + 8..], len);
                    }
                } else {
                    let (sb_nr, sb_offset) = strings(s.as_bytes())?;
                    let abs_bucket = (string_bucket_base + sb_nr) as i32;
                    if big_endian {
                        write_i32_be(&mut bucket[ref_offset..], abs_bucket);
                        write_i32_be(&mut bucket[ref_offset + 4..], sb_offset as i32);
                        write_i32_be(&mut bucket[ref_offset + 8..], len);
                    } else {
                        write_i32_le(&mut bucket[ref_offset..], abs_bucket);
                        write_i32_le(&mut bucket[ref_offset + 4..], sb_offset as i32);
                        write_i32_le(&mut bucket[ref_offset + 8..], len);
                    }
                }
            } else {
                // String arrays also use one 12-byte locator per row.
                // Fixed-shape arrays store only the element data; variable-shape
                // arrays prefix ndim, shape, and a filled flag. The elements
                // are borrowed in Fortran order and encoded once, as one block.
                let (shape, elements) = match value {
                    Some(Value::Array(ArrayValue::String(arr))) => (
                        stored_shape(arr.shape()),
                        arr.t().into_iter().map(String::as_str).collect::<Vec<_>>(),
                    ),
                    // An unwritten cell of a variable-shape column stays
                    // undefined: casacore's SSMIndStringColumn reads a zero
                    // length as no array in the row.
                    _ if col_desc.shape.is_empty() => return Ok(()),
                    _ => {
                        let shape: Vec<usize> =
                            col_desc.shape.iter().map(|&dim| dim as usize).collect();
                        let count = shape.iter().product();
                        (shape, vec![""; count])
                    }
                };

                // Variable shapes' ndim, shape and filled flag, then each
                // element's length and bytes.
                let prefix = if col_desc.shape.is_empty() {
                    4 * (shape.len() + 2)
                } else {
                    0
                };
                let mut block = Vec::with_capacity(
                    prefix + elements.iter().map(|s| 4 + s.len()).sum::<usize>(),
                );
                if col_desc.shape.is_empty() {
                    block.extend_from_slice(&(shape.len() as u32).to_be_bytes());
                    for &dim in &shape {
                        block.extend_from_slice(&(dim as i32).to_be_bytes());
                    }
                    let filled = if elements.iter().any(|s| !s.is_empty()) {
                        1u32
                    } else {
                        0u32
                    };
                    block.extend_from_slice(&filled.to_be_bytes());
                }
                for s in &elements {
                    block.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    block.extend_from_slice(s.as_bytes());
                }

                if block.is_empty() {
                    // All empty strings — leave reference as zeroes
                } else {
                    let (sb_nr, sb_offset) = strings(&block)?;
                    let abs_bucket = (string_bucket_base + sb_nr) as i32;
                    let total = block.len() as i32;
                    if big_endian {
                        write_i32_be(&mut bucket[ref_offset..], abs_bucket);
                        write_i32_be(&mut bucket[ref_offset + 4..], sb_offset as i32);
                        write_i32_be(&mut bucket[ref_offset + 8..], total);
                    } else {
                        write_i32_le(&mut bucket[ref_offset..], abs_bucket);
                        write_i32_le(&mut bucket[ref_offset + 4..], sb_offset as i32);
                        write_i32_le(&mut bucket[ref_offset + 8..], total);
                    }
                }
            }
        }
        _ => {
            let (elem_bytes, _) = canonical_element_size(data_type);
            let data_start = col_offset + row_in_bucket * elem_bytes * nrelem;
            encode_fixed_cell(
                &mut bucket[data_start..],
                value.into(),
                data_type,
                nrelem,
                big_endian,
            );
        }
    }
    Ok(())
}

/// The header's last string bucket in use, where casacore appends the next
/// string (`SSMBase::itsLastStringBucket`): the last of the
/// `nr_string_buckets` after the `nr_data_buckets`, or -1 for none.
pub(super) fn last_string_bucket(nr_data_buckets: usize, nr_string_buckets: usize) -> i32 {
    match nr_string_buckets {
        0 => -1,
        count => (nr_data_buckets + count - 1) as i32,
    }
}

/// Serialize the SSM header into a 512-byte buffer.
///
/// The AipsIO framing byte order matches the table's endian setting:
/// `CanonicalIO` (big-endian) for BE tables, `LECanonicalIO` (little-endian)
/// for LE tables. C++ casacore creates the corresponding TypeIO based on
/// `asBigEndian()` when reading this header. The `asBigEndian` flag inside
/// the header controls bucket data byte order.
#[allow(clippy::too_many_arguments)]
pub(super) fn serialize_ssm_header(
    bucket_size: u32,
    nr_buckets: u32,
    nr_free_buckets: u32,
    first_free_bucket: i32,
    nr_idx_buckets: u32,
    first_idx_bucket: i32,
    idx_bucket_offset: u32,
    last_string_bucket: i32,
    index_length: u32,
    nr_indices: u32,
    cache_size: u32,
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    // AipsIO framing uses the table's byte order (C++ creates CanonicalIO or
    // LECanonicalIO based on asBigEndian()).
    let io_order = if big_endian {
        ByteOrder::BigEndian
    } else {
        ByteOrder::LittleEndian
    };
    let mut buf = AipsIoBufferWriter::new(io_order);
    buf.putstart("StandardStMan", 3)?;
    buf.put_bool(big_endian);
    buf.put_u32(bucket_size);
    buf.put_u32(nr_buckets);
    buf.put_u32(cache_size);
    buf.put_u32(nr_free_buckets);
    buf.put_i32(first_free_bucket);
    buf.put_u32(nr_idx_buckets);
    buf.put_i32(first_idx_bucket);
    buf.put_u32(idx_bucket_offset);
    buf.put_i32(last_string_bucket);
    buf.put_u32(index_length);
    buf.put_u32(nr_indices);
    buf.putend()?;

    // Pad to 512 bytes
    let mut header = buf.into_bytes();
    header.resize(SSM_HEADER_SIZE as usize, 0);
    Ok(header)
}

/// Serialize SSMIndex to bytes using the table's byte order for AipsIO
/// framing: the index the in-memory writer stores, against which the
/// streamed index ([`index`]) is checked.
#[cfg(test)]
pub(super) fn serialize_ssm_index(
    n_used: u32,
    rows_per_bucket: u32,
    nr_columns: i32,
    last_row: &[u64],
    bucket_number: &[u32],
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    let io_order = if big_endian {
        ByteOrder::BigEndian
    } else {
        ByteOrder::LittleEndian
    };
    let mut buf = AipsIoBufferWriter::new(io_order);
    // SSMIndex is a top-level object (level 0 → has magic)
    buf.putstart("SSMIndex", 2)?;
    buf.put_u32(n_used);
    buf.put_u32(rows_per_bucket);
    buf.put_i32(nr_columns);
    // freeSpace map: empty SimpleOrderedMap
    buf.putstart("SimpleOrderedMap", 1)?;
    buf.put_i32(0); // default_value
    buf.put_u32(0); // nr entries
    buf.put_u32(0); // incr
    buf.putend()?;
    // lastRow: Block<Int64> (version 2)
    buf.putstart("Block", 1)?;
    buf.put_u32(last_row.len() as u32);
    for &v in last_row {
        buf.put_u64(v);
    }
    buf.putend()?;
    // bucketNumber: Block<uInt>
    buf.putstart("Block", 1)?;
    buf.put_u32(bucket_number.len() as u32);
    for &v in bucket_number {
        buf.put_u32(v);
    }
    buf.putend()?;
    buf.putend()?;
    Ok(buf.into_bytes())
}
