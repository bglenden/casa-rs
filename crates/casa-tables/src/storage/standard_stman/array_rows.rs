// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected SSM array payloads and metadata share column and bucket registration.
use super::*;

trait ReadCell {
    type Cell: Clone;
    fn indirect(
        reader: Option<&mut StManArrayFileReader>,
        offset: i64,
        data_type: CasacoreDataType,
        desc: &ColumnDescContents,
        row: usize,
    ) -> Result<Option<Self::Cell>, StorageError>;
    fn direct(
        raw: &ColumnRawData,
        desc: &ColumnDescContents,
        row: usize,
        rows: usize,
    ) -> Result<Option<Self::Cell>, StorageError>;
}

struct Values;
impl ReadCell for Values {
    type Cell = ArrayValue;
    fn indirect(
        reader: Option<&mut StManArrayFileReader>,
        offset: i64,
        data_type: CasacoreDataType,
        desc: &ColumnDescContents,
        row: usize,
    ) -> Result<Option<Self::Cell>, StorageError> {
        read_ssm_indirect_cell(reader, offset, data_type, desc, row)
    }
    fn direct(
        raw: &ColumnRawData,
        desc: &ColumnDescContents,
        row: usize,
        rows: usize,
    ) -> Result<Option<Self::Cell>, StorageError> {
        match extract_row_value(raw, desc, row, rows)? {
            Value::Array(array) => Ok(Some(array)),
            other => Err(StorageError::FormatMismatch(format!(
                "SSM direct array column '{}' row {row} expected array value, found {:?}",
                desc.col_name,
                other.kind()
            ))),
        }
    }
}

struct Shapes;
impl ReadCell for Shapes {
    type Cell = Vec<usize>;
    fn indirect(
        reader: Option<&mut StManArrayFileReader>,
        offset: i64,
        _data_type: CasacoreDataType,
        desc: &ColumnDescContents,
        row: usize,
    ) -> Result<Option<Self::Cell>, StorageError> {
        match reader {
            Some(reader) => reader.read_shape_at(offset),
            None if offset == 0 => Ok(None),
            None => Err(StorageError::FormatMismatch(format!(
                "SSM array column '{}' row {row} has an offset without an array file",
                desc.col_name
            ))),
        }
    }
    fn direct(
        _raw: &ColumnRawData,
        desc: &ColumnDescContents,
        _row: usize,
        _rows: usize,
    ) -> Result<Option<Self::Cell>, StorageError> {
        Ok(Some(desc.shape.iter().map(|&size| size as usize).collect()))
    }
}

pub(crate) fn read_ssm_array_column_rows(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    target_col_idx: usize,
    selected_rows: &[usize],
) -> Result<Option<Vec<Option<ArrayValue>>>, StorageError> {
    read::<Values>(file_path, dm_blob, col_descs, target_col_idx, selected_rows)
}

pub(crate) fn read_ssm_array_column_shapes(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    target_col_idx: usize,
    selected_rows: &[usize],
) -> Result<Option<Vec<Option<Vec<usize>>>>, StorageError> {
    let Some(desc) = col_descs.get(target_col_idx) else {
        return Err(StorageError::FormatMismatch(
            "SSM shape column index is out of range".to_string(),
        ));
    };
    // Direct fixed-shape arrays are always defined and need no payload read.
    if desc.is_array && !desc.shape.is_empty() && !is_ssm_array_file_indirect(desc) {
        let shape = desc
            .shape
            .iter()
            .map(|&size| size as usize)
            .collect::<Vec<_>>();
        return Ok(Some(vec![Some(shape); selected_rows.len()]));
    }
    read::<Shapes>(file_path, dm_blob, col_descs, target_col_idx, selected_rows)
}

fn read<R: ReadCell>(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    target_col_idx: usize,
    selected_rows: &[usize],
) -> Result<Option<Vec<Option<R::Cell>>>, StorageError> {
    if selected_rows.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut file = File::open(file_path)?;
    let header = parse_ssm_header(&mut file)?;
    let dm_info = parse_ssm_dm_blob(dm_blob)?;
    let indices = parse_ssm_indices(&mut file, &header)?;

    if target_col_idx >= col_descs.len() {
        return Err(StorageError::FormatMismatch(format!(
            "SSM array column index {target_col_idx} is out of range"
        )));
    }
    if target_col_idx >= dm_info.column_offsets.len()
        || target_col_idx >= dm_info.col_index_map.len()
    {
        return Err(StorageError::FormatMismatch(format!(
            "SSM array column index {target_col_idx} missing dm blob metadata"
        )));
    }

    let col_desc = col_descs[target_col_idx];
    if !col_desc.is_array || col_desc.is_record() || is_ssm_variable_string(col_desc) {
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
    let indirect = is_ssm_array_file_indirect(col_desc);
    let nrelem = if indirect {
        1
    } else if col_desc.nrdim > 0 && !col_desc.shape.is_empty() {
        col_desc.shape.iter().map(|&s| s as usize).product()
    } else {
        return Ok(None);
    };
    let mut array_reader = if indirect {
        let mut array_path = file_path.as_os_str().to_os_string();
        array_path.push("i");
        let array_path = std::path::PathBuf::from(array_path);
        array_path
            .exists()
            .then(|| StManArrayFileReader::open(&array_path, header.big_endian))
            .transpose()?
    } else {
        None
    };
    let indirect_type = if indirect {
        Some(CasacoreDataType::from_primitive_type(
            col_desc.require_primitive_type()?,
            false,
        ))
    } else {
        None
    };

    // Read bucket by bucket: each touched bucket once, keeping only the
    // selected rows' cells, so a strided selection never stages the rows
    // between them.
    let mut requests = selected_rows
        .iter()
        .copied()
        .enumerate()
        .map(|(slot, row)| (row, slot))
        .collect::<Vec<_>>();
    requests.sort_unstable();
    let mut values = vec![None; selected_rows.len()];
    let mut start = 0;
    while start < requests.len() {
        let first_row = requests[start].0;
        let (_, _, bucket_last_row) = index.find_bucket(first_row as u64).ok_or_else(|| {
            StorageError::FormatMismatch(format!("SSM index has no bucket for row {first_row}"))
        })?;
        let end =
            start + requests[start..].partition_point(|(row, _)| *row as u64 <= bucket_last_row);
        let group = &requests[start..end];
        let nrrow = group[group.len() - 1].0 - first_row + 1;
        let raw = read_column_from_buckets(
            &mut file,
            &header,
            index,
            column_offset,
            if indirect {
                CasacoreDataType::TpInt64
            } else {
                col_desc.data_type
            },
            if indirect { 0 } else { col_desc.max_length },
            nrelem,
            first_row,
            nrrow,
        )?;
        for &(row, slot) in group {
            values[slot] = match indirect_type {
                Some(data_type) => {
                    let ColumnRawData::Int64(offsets) = &raw else {
                        return Err(StorageError::FormatMismatch(
                            "SSM indirect array: expected Int64 offsets".to_string(),
                        ));
                    };
                    R::indirect(
                        array_reader.as_mut(),
                        offsets[row - first_row],
                        data_type,
                        col_desc,
                        row,
                    )?
                }
                None => R::direct(&raw, col_desc, row - first_row, nrrow)?,
            };
        }
        start = end;
    }
    Ok(Some(values))
}
