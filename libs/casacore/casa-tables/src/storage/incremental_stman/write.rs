// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(crate) fn write_ism_with_layout(
    path: &Path,
    columns: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    big_endian: bool,
    bucket_size: Option<u32>,
    cache_size: u32,
) -> Result<Vec<u8>, StorageError> {
    let sources = columns
        .iter()
        .map(|desc| ColumnSource::RowFields(RowFields::new(rows, None, &desc.col_name)))
        .collect();
    write_columns::write_ism_column_sources_with_layout(
        path,
        columns,
        &ColumnSources::new(rows.len(), sources),
        big_endian,
        bucket_size,
        cache_size,
    )
}

pub(crate) fn write_ism_file_indexed(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    field_indices: &[usize],
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    write_ism_file_impl(file_path, col_descs, rows, Some(field_indices), big_endian)
}

pub(crate) fn write_ism_file_scalar_columns(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    scalar_columns: &[&[Option<casa_core::ScalarValue>]],
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    write_ism_scalar_columns_with_layout(
        file_path,
        col_descs,
        scalar_columns,
        (big_endian, None, 1),
    )
}

pub(crate) fn write_ism_scalar_columns_with_layout(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    scalar_columns: &[&[Option<casa_core::ScalarValue>]],
    (big_endian, bucket_size, cache_size): (bool, Option<u32>, u32),
) -> Result<Vec<u8>, StorageError> {
    let nrrow = scalar_columns.first().map_or(0, |values| values.len());
    if scalar_columns.len() != col_descs.len() {
        return Err(StorageError::FormatMismatch(format!(
            "ISM scalar column count mismatch: expected {}, got {}",
            col_descs.len(),
            scalar_columns.len()
        )));
    }
    for values in scalar_columns.iter().skip(1) {
        if values.len() != nrrow {
            return Err(StorageError::FormatMismatch(
                "ISM scalar columns have inconsistent row counts".to_string(),
            ));
        }
    }
    if col_descs
        .iter()
        .any(|col_desc| col_desc.is_array || col_desc.is_record())
    {
        return Err(StorageError::FormatMismatch(
            "ISM scalar column writer only supports scalar non-record columns".to_string(),
        ));
    }
    let column_sources = ColumnSources::materialized(nrrow, scalar_columns);
    write_columns::write_ism_column_sources_with_layout(
        file_path,
        col_descs,
        &column_sources,
        big_endian,
        bucket_size,
        cache_size,
    )
}

/// Refuse a fixed-shape array column without the Direct option, which
/// casacore stores as an `ISMIndColumn`, its cells in the `table.fNi` array
/// file; the IncrementalStMan writers store fixed-shape arrays in the
/// buckets, as casacore's `ISMColumn` stores Direct ones.
pub(crate) fn refuse_ism_indirect_fixed_arrays(
    col_descs: &[ColumnDescContents],
) -> Result<(), StorageError> {
    match col_descs
        .iter()
        .find(|desc| desc.is_array && !desc.shape.is_empty() && desc.option & 1 == 0)
    {
        Some(desc) => Err(StorageError::IsmIndirectArrayColumn(desc.col_name.clone())),
        None => Ok(()),
    }
}

pub(super) fn write_ism_file_scalar_run_columns(
    file_path: &Path,
    nrrow: usize,
    col_info: &[(CasacoreDataType, usize)],
    scalar_runs: &[&[GeneratedScalarValueRun]],
    bucket_size: u32,
    big_endian: bool,
    cache_size: u32,
) -> Result<Vec<u8>, StorageError> {
    let ncol = col_info.len();
    if scalar_runs.len() != ncol {
        return Err(StorageError::FormatMismatch(format!(
            "ISM scalar run column count mismatch: expected {ncol}, got {}",
            scalar_runs.len()
        )));
    }

    #[derive(Debug)]
    struct BucketBuilder {
        data: Vec<u8>,
        col_indices: Vec<(Vec<u32>, Vec<u32>)>,
    }

    fn new_bucket(encoded_values: &[Vec<u8>]) -> BucketBuilder {
        let mut bucket = BucketBuilder {
            data: Vec::new(),
            col_indices: (0..encoded_values.len())
                .map(|_| (Vec::new(), Vec::new()))
                .collect(),
        };
        for (col_idx, encoded) in encoded_values.iter().enumerate() {
            let offset = bucket.data.len() as u32;
            bucket.data.extend_from_slice(encoded);
            bucket.col_indices[col_idx].0.push(0);
            bucket.col_indices[col_idx].1.push(offset);
        }
        bucket
    }

    fn index_area_len(bucket: &BucketBuilder, extra_entries: &[bool]) -> usize {
        bucket
            .col_indices
            .iter()
            .enumerate()
            .map(|(col_idx, (row_nrs, _))| {
                let entries = row_nrs.len() + usize::from(extra_entries[col_idx]);
                4 + entries * 4 + entries * 4
            })
            .sum()
    }

    let mut buckets = Vec::<BucketBuilder>::new();
    let mut bucket_start_rows = Vec::<usize>::new();

    if nrrow > 0 {
        let mut current_values = Vec::with_capacity(ncol);
        for (col_idx, runs) in scalar_runs.iter().enumerate() {
            let Some(first_run) = runs.first() else {
                return Err(StorageError::FormatMismatch(format!(
                    "ISM scalar run column {col_idx} has no initial run"
                )));
            };
            if first_run.start_row() != 0 {
                return Err(StorageError::FormatMismatch(format!(
                    "ISM scalar run column {col_idx} starts at row {}, expected 0",
                    first_run.start_row()
                )));
            }
            let (dt, nrelem) = col_info[col_idx];
            current_values.push(encode_cell(
                first_run.value().into(),
                dt,
                nrelem,
                big_endian,
            ));
        }

        let mut next_run_indices = vec![1usize; ncol];
        let mut bucket_start = 0usize;
        bucket_start_rows.push(bucket_start);
        let mut current = new_bucket(&current_values);

        loop {
            let next_row = scalar_runs
                .iter()
                .zip(&next_run_indices)
                .filter_map(|(runs, &idx)| runs.get(idx).map(GeneratedScalarValueRun::start_row))
                .filter(|&row| row < nrrow)
                .min();
            let Some(row) = next_row else {
                break;
            };

            let rel_row = row.checked_sub(bucket_start).ok_or_else(|| {
                StorageError::FormatMismatch("ISM scalar run bucket row underflow".to_string())
            })? as u32;
            let mut changed = Vec::new();
            let mut extra_entries = vec![false; ncol];
            for col_idx in 0..ncol {
                let idx = next_run_indices[col_idx];
                if scalar_runs[col_idx]
                    .get(idx)
                    .is_some_and(|run| run.start_row() == row)
                {
                    let run = &scalar_runs[col_idx][idx];
                    let (dt, nrelem) = col_info[col_idx];
                    let encoded = encode_cell(run.value().into(), dt, nrelem, big_endian);
                    extra_entries[col_idx] = true;
                    changed.push((col_idx, encoded));
                }
            }

            let new_data_bytes: usize = changed.iter().map(|(_, encoded)| encoded.len()).sum();
            let total_estimate =
                4 + current.data.len() + new_data_bytes + index_area_len(&current, &extra_entries);

            if rel_row > 0 && total_estimate > bucket_size as usize {
                buckets.push(current);
                for (col_idx, encoded) in changed {
                    current_values[col_idx] = encoded;
                    next_run_indices[col_idx] += 1;
                }
                bucket_start = row;
                bucket_start_rows.push(bucket_start);
                current = new_bucket(&current_values);
                continue;
            }

            for (col_idx, encoded) in changed {
                let offset = current.data.len() as u32;
                current.data.extend_from_slice(&encoded);
                current.col_indices[col_idx].0.push(rel_row);
                current.col_indices[col_idx].1.push(offset);
                current_values[col_idx] = encoded;
                next_run_indices[col_idx] += 1;
            }
        }

        buckets.push(current);
    }

    let nr_buckets = buckets.len();
    let mut raw_buckets = Vec::with_capacity(nr_buckets);
    for bucket in &buckets {
        let mut raw = vec![0u8; bucket_size as usize];
        let data_len = bucket.data.len().min(bucket_size as usize - 4);
        raw[4..4 + data_len].copy_from_slice(&bucket.data[..data_len]);

        let mut index_area = Vec::new();
        for (row_nrs, offsets) in &bucket.col_indices {
            let n = row_nrs.len() as u32;
            if big_endian {
                index_area.extend_from_slice(&n.to_be_bytes());
                for &r in row_nrs {
                    index_area.extend_from_slice(&r.to_be_bytes());
                }
                for &o in offsets {
                    index_area.extend_from_slice(&o.to_be_bytes());
                }
            } else {
                index_area.extend_from_slice(&n.to_le_bytes());
                for &r in row_nrs {
                    index_area.extend_from_slice(&r.to_le_bytes());
                }
                for &o in offsets {
                    index_area.extend_from_slice(&o.to_le_bytes());
                }
            }
        }

        let index_start = bucket_size as usize - index_area.len();
        let index_offset = index_start as u32;
        if big_endian {
            raw[0..4].copy_from_slice(&index_offset.to_be_bytes());
        } else {
            raw[0..4].copy_from_slice(&index_offset.to_le_bytes());
        }
        raw[index_start..index_start + index_area.len()].copy_from_slice(&index_area);
        raw_buckets.push(raw);
    }

    let mut ism_rows = Vec::with_capacity(nr_buckets + 1);
    let mut ism_bucket_nrs = Vec::with_capacity(nr_buckets);
    for (idx, &start_row) in bucket_start_rows.iter().enumerate() {
        ism_rows.push(start_row as u64);
        ism_bucket_nrs.push(idx as u32);
    }
    ism_rows.push(nrrow as u64);

    let index_data = serialize_ism_index(&ism_rows, &ism_bucket_nrs, big_endian)?;
    let header_buf = serialize_ism_header(bucket_size, nr_buckets as u32, big_endian, cache_size)?;

    let mut file = File::create(file_path)?;
    file.write_all(&header_buf)?;
    for raw in &raw_buckets {
        file.write_all(raw)?;
    }
    file.write_all(&index_data)?;

    serialize_ism_dm_blob("ISM")
}

/// Write an IncrementalStMan file from row records, through the
/// column-source writer, each column a source of its rows' fields.
///
/// The two writers once differed: at a bucket's first row this one wrote a
/// copy of every column's previous value as well as the changed ones, and
/// a bucket too small for both lost the tail of its data to its index
/// (#852, ISM bucket layouts). Sharing one writer gives row records the
/// buckets casacore's `ISMBucket` writes when a bucket starts: each
/// column's value at that row, once.
pub(super) fn write_ism_file_impl(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    rows: &[casa_core::RecordValue],
    field_indices: Option<&[usize]>,
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    let columns = col_descs
        .iter()
        .enumerate()
        .map(|(col_idx, desc)| {
            let field_index = field_indices.map(|indices| indices[col_idx]);
            ColumnSource::RowFields(RowFields::new(rows, field_index, &desc.col_name))
        })
        .collect();
    write_ism_file_column_sources(
        file_path,
        col_descs,
        &ColumnSources::new(rows.len(), columns),
        big_endian,
    )
}

pub(crate) fn save_ism_file_scalar_columns_rows_in_place(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    scalar_columns: &[&[Option<casa_core::ScalarValue>]],
    changed_rows: &[usize],
    big_endian: bool,
) -> Result<bool, StorageError> {
    let nrrow = scalar_columns.first().map_or(0, |values| values.len());
    if scalar_columns.len() != col_descs.len() {
        return Err(StorageError::FormatMismatch(format!(
            "ISM scalar column count mismatch: expected {}, got {}",
            col_descs.len(),
            scalar_columns.len()
        )));
    }
    for values in scalar_columns.iter().skip(1) {
        if values.len() != nrrow {
            return Err(StorageError::FormatMismatch(
                "ISM scalar columns have inconsistent row counts".to_string(),
            ));
        }
    }
    if changed_rows.is_empty() || nrrow == 0 {
        return Ok(false);
    }
    if col_descs
        .iter()
        .any(|col_desc| col_desc.is_array || col_desc.is_record())
    {
        return Ok(false);
    }

    let col_info: Vec<(CasacoreDataType, usize)> = col_descs
        .iter()
        .map(|c| {
            let dt = CasacoreDataType::from_primitive_type(c.require_primitive_type()?, false);
            Ok((dt, 1usize))
        })
        .collect::<Result<_, StorageError>>()?;

    let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;
    let header = parse_ism_header(&mut file)?;
    if header.big_endian != big_endian {
        return Err(StorageError::FormatMismatch(format!(
            "ISM endian mismatch for sparse save: file={}, requested={}",
            header.big_endian, big_endian
        )));
    }
    let index = parse_ism_index(&mut file, &header)?;
    if index.rows.len() < 2 || index.bucket_nrs.is_empty() {
        return Ok(false);
    }

    let mut changed_intervals = Vec::new();
    let mut last_interval = None;
    for &row_idx in changed_rows {
        if row_idx >= nrrow {
            continue;
        }
        let interval = index.rows.partition_point(|&start| start <= row_idx as u64);
        if interval == 0 {
            continue;
        }
        let interval_idx = interval - 1;
        if interval_idx >= index.bucket_nrs.len() {
            continue;
        }
        if last_interval != Some(interval_idx) {
            changed_intervals.push(interval_idx);
            last_interval = Some(interval_idx);
        }
    }
    if changed_intervals.is_empty() {
        return Ok(false);
    }

    let mut patched_buckets = Vec::with_capacity(changed_intervals.len());
    for interval_idx in changed_intervals {
        let bucket_start = index.rows[interval_idx] as usize;
        let bucket_end = index.rows[interval_idx + 1] as usize;
        let bucket_nr = index.bucket_nrs[interval_idx];
        let Some(raw) = build_ism_scalar_bucket_for_row_range(
            &col_info,
            scalar_columns,
            bucket_start,
            bucket_end,
            header.bucket_size,
            big_endian,
        )?
        else {
            return Ok(false);
        };
        patched_buckets.push((bucket_nr, raw));
    }

    for (bucket_nr, raw) in patched_buckets {
        let offset = ISM_HEADER_SIZE + (bucket_nr as u64) * (header.bucket_size as u64);
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&raw)?;
    }

    Ok(true)
}

pub(crate) fn save_ism_file_scalar_columns_sparse_rows_in_place(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    sparse_columns: &[Option<SparseScalarRowValues>],
    changed_rows: &[usize],
    big_endian: bool,
) -> Result<bool, StorageError> {
    if sparse_columns.len() != col_descs.len() {
        return Err(StorageError::FormatMismatch(format!(
            "ISM sparse scalar column count mismatch: expected {}, got {}",
            col_descs.len(),
            sparse_columns.len()
        )));
    }
    if changed_rows.is_empty() || col_descs.is_empty() {
        return Ok(false);
    }
    if sparse_columns.iter().all(Option::is_none) {
        return Ok(false);
    }
    if col_descs
        .iter()
        .any(|col_desc| col_desc.is_array || col_desc.is_record())
    {
        return Ok(false);
    }

    let col_info: Vec<(CasacoreDataType, usize)> = col_descs
        .iter()
        .map(|c| {
            let dt = CasacoreDataType::from_primitive_type(c.require_primitive_type()?, false);
            Ok((dt, 1usize))
        })
        .collect::<Result<_, StorageError>>()?;

    let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;
    let header = parse_ism_header(&mut file)?;
    if header.big_endian != big_endian {
        return Err(StorageError::FormatMismatch(format!(
            "ISM endian mismatch for sparse save: file={}, requested={}",
            header.big_endian, big_endian
        )));
    }
    let index = parse_ism_index(&mut file, &header)?;
    if index.rows.len() < 2 || index.bucket_nrs.is_empty() {
        return Ok(false);
    }

    let mut changed_intervals = Vec::new();
    let mut last_interval = None;
    for &row_idx in changed_rows {
        let interval = index.rows.partition_point(|&start| start <= row_idx as u64);
        if interval == 0 {
            continue;
        }
        let interval_idx = interval - 1;
        if interval_idx >= index.bucket_nrs.len() {
            continue;
        }
        if last_interval != Some(interval_idx) {
            changed_intervals.push(interval_idx);
            last_interval = Some(interval_idx);
        }
    }
    if changed_intervals.is_empty() {
        return Ok(false);
    }

    let sparse_lookup: Vec<Option<HashMap<usize, Option<casa_core::ScalarValue>>>> = sparse_columns
        .iter()
        .map(|column| {
            column
                .as_ref()
                .map(|column| column.iter().cloned().collect::<HashMap<_, _>>())
        })
        .collect();

    let mut patched_buckets = Vec::with_capacity(changed_intervals.len());
    for interval_idx in changed_intervals {
        let bucket_start = index.rows[interval_idx] as usize;
        let bucket_end = index.rows[interval_idx + 1] as usize;
        let bucket_nr = index.bucket_nrs[interval_idx];
        let raw_bucket = read_ism_bucket(&mut file, &header, bucket_nr)?;
        let bucket = parse_ism_bucket(&raw_bucket, col_descs.len(), big_endian)?;
        let bucket_rows = load_scalar_bucket_values(
            &bucket,
            col_descs,
            &col_info,
            bucket_end.saturating_sub(bucket_start),
            big_endian,
        )?;

        let mut owned_columns = bucket_rows;
        for (col_idx, overrides) in sparse_lookup.iter().enumerate() {
            let Some(overrides) = overrides else {
                continue;
            };
            for (&row_idx, value) in overrides {
                if row_idx < bucket_start || row_idx >= bucket_end {
                    continue;
                }
                owned_columns[col_idx][row_idx - bucket_start] = value.clone();
            }
        }

        let column_refs: Vec<_> = owned_columns.iter().map(Vec::as_slice).collect();
        let Some(raw) = build_ism_scalar_bucket_for_row_range(
            &col_info,
            &column_refs,
            0,
            bucket_end.saturating_sub(bucket_start),
            header.bucket_size,
            big_endian,
        )?
        else {
            return Ok(false);
        };
        patched_buckets.push((bucket_nr, raw));
    }

    for (bucket_nr, raw) in patched_buckets {
        let offset = ISM_HEADER_SIZE + (bucket_nr as u64) * (header.bucket_size as u64);
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&raw)?;
    }
    Ok(true)
}

pub(super) fn build_ism_scalar_bucket_for_row_range(
    col_info: &[(CasacoreDataType, usize)],
    scalar_columns: &[&[Option<casa_core::ScalarValue>]],
    row_start: usize,
    row_end: usize,
    bucket_size: u32,
    big_endian: bool,
) -> Result<Option<Vec<u8>>, StorageError> {
    if row_end <= row_start {
        return Ok(Some(vec![0u8; bucket_size as usize]));
    }

    let ncol = scalar_columns.len();
    let mut data = Vec::new();
    let mut col_indices: Vec<(Vec<u32>, Vec<u32>)> =
        (0..ncol).map(|_| (Vec::new(), Vec::new())).collect();
    let mut last_values: Vec<Vec<u8>> = vec![Vec::new(); ncol];

    for (row_idx, _) in scalar_columns[0]
        .iter()
        .enumerate()
        .take(row_end)
        .skip(row_start)
    {
        let rel_row = (row_idx - row_start) as u32;
        for col_idx in 0..ncol {
            let (dt, nrelem) = col_info[col_idx];
            let encoded = encode_cell(
                scalar_columns[col_idx][row_idx].as_ref().into(),
                dt,
                nrelem,
                big_endian,
            );
            if rel_row == 0 || encoded != last_values[col_idx] {
                let offset = data.len() as u32;
                data.extend_from_slice(&encoded);
                col_indices[col_idx].0.push(rel_row);
                col_indices[col_idx].1.push(offset);
                last_values[col_idx] = encoded;
            }
        }
    }

    serialize_ism_bucket(bucket_size, &data, &col_indices, big_endian)
}

pub(super) fn load_scalar_bucket_values(
    bucket: &IsmBucket,
    col_descs: &[ColumnDescContents],
    col_info: &[(CasacoreDataType, usize)],
    rows_in_bucket: usize,
    big_endian: bool,
) -> Result<Vec<Vec<Option<casa_core::ScalarValue>>>, StorageError> {
    let mut values = Vec::with_capacity(col_descs.len());
    for (col_idx, col_desc) in col_descs.iter().enumerate() {
        let (dt, _) = col_info[col_idx];
        let mut column = Vec::with_capacity(rows_in_bucket);
        if col_idx >= bucket.col_indices.len() {
            let value = default_value(dt, false, col_desc);
            let scalar = match value {
                casa_core::Value::Scalar(scalar) => scalar,
                other => {
                    return Err(StorageError::FormatMismatch(format!(
                        "ISM default value for column '{}' is not scalar: {other:?}",
                        col_desc.col_name
                    )));
                }
            };
            let value = if (col_desc.option & 2) != 0
                && scalar_value_is_default(
                    &casa_core::Value::Scalar(scalar.clone()),
                    col_desc.require_primitive_type()?,
                ) {
                None
            } else {
                Some(scalar)
            };
            column.resize(rows_in_bucket, value);
            values.push(column);
            continue;
        }

        let col_index = &bucket.col_indices[col_idx];
        for rel_row in 0..rows_in_bucket {
            let interval = get_interval(col_index, rel_row as u32);
            let data_offset = col_index.offsets[interval] as usize;
            let value = read_scalar_at(&bucket.data, data_offset, dt, big_endian)?;
            let scalar = match value {
                casa_core::Value::Scalar(scalar) => scalar,
                other => {
                    return Err(StorageError::FormatMismatch(format!(
                        "ISM sparse bucket value for column '{}' is not scalar: {other:?}",
                        col_desc.col_name
                    )));
                }
            };
            if (col_desc.option & 2) != 0
                && scalar_value_is_default(
                    &casa_core::Value::Scalar(scalar.clone()),
                    col_desc.require_primitive_type()?,
                )
            {
                column.push(None);
            } else {
                column.push(Some(scalar));
            }
        }
        values.push(column);
    }
    Ok(values)
}

pub(super) fn load_scalar_bucket_column_values(
    bucket: &IsmBucket,
    col_desc: &ColumnDescContents,
    col_idx: usize,
    dt: CasacoreDataType,
    rows_in_bucket: usize,
    big_endian: bool,
    preserve_defaults: bool,
) -> Result<Vec<Option<ScalarValue>>, StorageError> {
    if col_idx >= bucket.col_indices.len() {
        let value = default_value(dt, false, col_desc);
        let scalar = match value {
            casa_core::Value::Scalar(scalar) => scalar,
            other => {
                return Err(StorageError::FormatMismatch(format!(
                    "ISM default value for column '{}' is not scalar: {other:?}",
                    col_desc.col_name
                )));
            }
        };
        let value = if !preserve_defaults
            && (col_desc.option & 2) != 0
            && scalar_value_is_default(
                &casa_core::Value::Scalar(scalar.clone()),
                col_desc.require_primitive_type()?,
            ) {
            None
        } else {
            Some(scalar)
        };
        return Ok(vec![value; rows_in_bucket]);
    }

    let col_index = &bucket.col_indices[col_idx];
    let mut values = Vec::with_capacity(rows_in_bucket);
    for rel_row in 0..rows_in_bucket {
        let interval = get_interval(col_index, rel_row as u32);
        let data_offset = col_index.offsets[interval] as usize;
        let value = read_scalar_at(&bucket.data, data_offset, dt, big_endian)?;
        let scalar = match value {
            casa_core::Value::Scalar(scalar) => scalar,
            other => {
                return Err(StorageError::FormatMismatch(format!(
                    "ISM sparse bucket value for column '{}' is not scalar: {other:?}",
                    col_desc.col_name
                )));
            }
        };
        if !preserve_defaults
            && (col_desc.option & 2) != 0
            && scalar_value_is_default(
                &casa_core::Value::Scalar(scalar.clone()),
                col_desc.require_primitive_type()?,
            )
        {
            values.push(None);
        } else {
            values.push(Some(scalar));
        }
    }
    Ok(values)
}

pub(super) fn serialize_ism_bucket(
    bucket_size: u32,
    data: &[u8],
    col_indices: &[(Vec<u32>, Vec<u32>)],
    big_endian: bool,
) -> Result<Option<Vec<u8>>, StorageError> {
    if bucket_size < 4 {
        return Err(StorageError::FormatMismatch(
            "ISM bucket size too small".to_string(),
        ));
    }

    let mut index_area = Vec::new();
    for (row_nrs, offsets) in col_indices {
        if row_nrs.len() != offsets.len() {
            return Err(StorageError::FormatMismatch(
                "ISM bucket row/offset index length mismatch".to_string(),
            ));
        }
        let n = row_nrs.len() as u32;
        if big_endian {
            index_area.extend_from_slice(&n.to_be_bytes());
            for &r in row_nrs {
                index_area.extend_from_slice(&r.to_be_bytes());
            }
            for &o in offsets {
                index_area.extend_from_slice(&o.to_be_bytes());
            }
        } else {
            index_area.extend_from_slice(&n.to_le_bytes());
            for &r in row_nrs {
                index_area.extend_from_slice(&r.to_le_bytes());
            }
            for &o in offsets {
                index_area.extend_from_slice(&o.to_le_bytes());
            }
        }
    }

    let bucket_size = bucket_size as usize;
    if 4 + data.len() + index_area.len() > bucket_size {
        return Ok(None);
    }

    let mut raw = vec![0u8; bucket_size];
    raw[4..4 + data.len()].copy_from_slice(data);

    let index_start = bucket_size - index_area.len();
    let index_offset = index_start as u32;
    if big_endian {
        raw[0..4].copy_from_slice(&index_offset.to_be_bytes());
    } else {
        raw[0..4].copy_from_slice(&index_offset.to_le_bytes());
    }
    raw[index_start..].copy_from_slice(&index_area);
    Ok(Some(raw))
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

pub(super) fn serialize_ism_header(
    bucket_size: u32,
    nr_buckets: u32,
    big_endian: bool,
    cache_size: u32,
) -> Result<Vec<u8>, StorageError> {
    encode_ism_header(&IsmHeader {
        version: if big_endian { 4 } else { 5 },
        bucket_size,
        nr_buckets,
        pers_cache_size: cache_size,
        uniq_nr: 0,
        nr_free_buckets: 0,
        first_free_bucket: -1,
        big_endian,
        io_order: if big_endian {
            ByteOrder::BigEndian
        } else {
            ByteOrder::LittleEndian
        },
    })
}

pub(super) fn encode_ism_header(header: &IsmHeader) -> Result<Vec<u8>, StorageError> {
    let mut buf = AipsIoBufferWriter::new(header.io_order);
    buf.putstart("IncrementalStMan", header.version)?;
    if header.version >= 5 {
        buf.put_bool(header.big_endian);
    }
    buf.put_u32(header.bucket_size);
    buf.put_u32(header.nr_buckets);
    buf.put_u32(header.pers_cache_size);
    buf.put_u32(header.uniq_nr);
    buf.put_u32(header.nr_free_buckets);
    buf.put_i32(header.first_free_bucket);
    buf.putend()?;
    let mut bytes = buf.into_bytes();
    bytes.resize(ISM_HEADER_SIZE as usize, 0);
    Ok(bytes)
}

pub(super) fn serialize_ism_index(
    rows: &[u64],
    bucket_nrs: &[u32],
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    let io_order = if big_endian {
        ByteOrder::BigEndian
    } else {
        ByteOrder::LittleEndian
    };
    encode_ism_index(rows, bucket_nrs, io_order)
}

pub(super) fn encode_ism_index(
    rows: &[u64],
    bucket_nrs: &[u32],
    io_order: ByteOrder,
) -> Result<Vec<u8>, StorageError> {
    let mut buf = AipsIoBufferWriter::new(io_order);
    let nused = bucket_nrs.len() as u32;

    // Use version 2 (64-bit row numbers) for safety
    buf.putstart("ISMIndex", 2)?;
    buf.put_u32(nused);

    // rows: Block<Int64> (version 2)
    buf.putstart("Block", 1)?;
    buf.put_u32(rows.len() as u32);
    for &r in rows {
        buf.put_u64(r);
    }
    buf.putend()?;

    // bucketNr: Block<uInt>
    buf.putstart("Block", 1)?;
    buf.put_u32(bucket_nrs.len() as u32);
    for &b in bucket_nrs {
        buf.put_u32(b);
    }
    buf.putend()?;

    buf.putend()?;
    Ok(buf.into_bytes())
}
