// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected SSM array payloads and metadata share column and bucket registration.
use super::*;

pub(super) trait ReadCell {
    type Cell: Clone;
    fn indirect(
        &mut self,
        reader: Option<&mut StManArrayFileReader>,
        offset: i64,
        data_type: CasacoreDataType,
        desc: &ColumnDescContents,
        position: (usize, usize),
    ) -> Result<Option<Self::Cell>, StorageError>;
    fn direct(
        &mut self,
        raw: &ColumnRawData,
        desc: &ColumnDescContents,
        position: (usize, usize),
        rows: usize,
    ) -> Result<Option<Self::Cell>, StorageError>;
}

struct Values;
impl ReadCell for Values {
    type Cell = ArrayValue;
    fn indirect(
        &mut self,
        reader: Option<&mut StManArrayFileReader>,
        offset: i64,
        data_type: CasacoreDataType,
        desc: &ColumnDescContents,
        (row, _slot): (usize, usize),
    ) -> Result<Option<Self::Cell>, StorageError> {
        read_ssm_indirect_cell(reader, offset, data_type, desc, row)
    }
    fn direct(
        &mut self,
        raw: &ColumnRawData,
        desc: &ColumnDescContents,
        (row, _slot): (usize, usize),
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
        &mut self,
        reader: Option<&mut StManArrayFileReader>,
        offset: i64,
        _data_type: CasacoreDataType,
        desc: &ColumnDescContents,
        (row, _slot): (usize, usize),
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
        &mut self,
        _raw: &ColumnRawData,
        desc: &ColumnDescContents,
        (_row, _slot): (usize, usize),
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
    read(
        file_path,
        dm_blob,
        col_descs,
        target_col_idx,
        selected_rows,
        &mut Values,
    )
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
    read(
        file_path,
        dm_blob,
        col_descs,
        target_col_idx,
        selected_rows,
        &mut Shapes,
    )
}

pub(super) fn read<R: ReadCell>(
    file_path: &Path,
    dm_blob: &[u8],
    col_descs: &[&ColumnDescContents],
    target_col_idx: usize,
    selected_rows: &[usize],
    cells: &mut R,
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
    // Decode only selected rows. Read-ahead retains at most 32 KiB of encoded
    // source bytes plus one bucket, independently of gaps in the selection.
    let mut requests = selected_rows
        .iter()
        .copied()
        .enumerate()
        .map(|(slot, row)| (row, slot))
        .collect::<Vec<_>>();
    requests.sort_unstable();
    if indirect {
        let mut array_path = file_path.as_os_str().to_os_string();
        array_path.push("i");
        let array_path = std::path::PathBuf::from(array_path);
        let array_file = array_path
            .exists()
            .then(|| StManArrayFileReader::open(&array_path, header.big_endian))
            .transpose()?;
        return IndirectColumn {
            header: &header,
            index,
            desc: col_desc,
            column_offset,
            array_file,
            data_type: CasacoreDataType::from_primitive_type(
                col_desc.require_primitive_type()?,
                false,
            ),
        }
        .read(file, &requests, cells)
        .map(Some);
    }
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
            col_desc.data_type,
            col_desc.max_length,
            nrelem,
            first_row,
            nrrow,
        )?;
        for &(row, slot) in group {
            values[slot] = cells.direct(&raw, col_desc, (row - first_row, slot), nrrow)?;
        }
        start = end;
    }
    Ok(Some(values))
}

struct IndirectColumn<'a> {
    header: &'a SsmHeader,
    index: &'a SsmIndex,
    desc: &'a ColumnDescContents,
    column_offset: usize,
    array_file: Option<StManArrayFileReader>,
    data_type: CasacoreDataType,
}

impl IndirectColumn<'_> {
    fn read<R: ReadCell>(
        mut self,
        file: File,
        requests: &[(usize, usize)],
        cells: &mut R,
    ) -> Result<Vec<Option<R::Cell>>, StorageError> {
        let mut buckets = ReadBuckets::new(file)?;
        let mut values = vec![None; requests.len()];
        let mut start = 0;
        while start < requests.len() {
            let first_row = requests[start].0;
            let (number, bucket_start, bucket_end) =
                self.index.find_bucket(first_row as u64).ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "SSM index has no bucket for row {first_row}"
                    ))
                })?;
            let end =
                start + requests[start..].partition_point(|(row, _)| *row as u64 <= bucket_end);
            let bucket = buckets.read(self.header, number)?;
            for &(row, slot) in &requests[start..end] {
                let at = (row - bucket_start as usize)
                    .checked_mul(8)
                    .and_then(|at| self.column_offset.checked_add(at))
                    .and_then(|at| at.checked_add(8).map(|end| (at, end)))
                    .and_then(|(at, end)| bucket.get(at..end))
                    .ok_or_else(|| {
                        StorageError::FormatMismatch(format!(
                            "SSM indirect column {} row {row} offset outside bucket",
                            self.desc.col_name
                        ))
                    })?;
                let bytes = at.try_into().expect("checked eight-byte offset");
                let offset = if self.header.big_endian {
                    i64::from_be_bytes(bytes)
                } else {
                    i64::from_le_bytes(bytes)
                };
                values[slot] = cells.indirect(
                    self.array_file.as_mut(),
                    offset,
                    self.data_type,
                    self.desc,
                    (row, slot),
                )?;
            }
            start = end;
        }
        Ok(values)
    }
}
