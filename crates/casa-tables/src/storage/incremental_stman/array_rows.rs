// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected cells and shapes from incremental buckets and indirect arrays.

use casa_types::{ArrayValue, Value};

use super::*;
use crate::storage::stman_array_file::make_empty_array;

pub(crate) fn read_ism_array_column_shapes(
    path: &Path,
    columns: &[&ColumnDescContents],
    column: usize,
    rows: &[usize],
) -> Result<Option<Vec<Option<Vec<usize>>>>, StorageError> {
    if rows.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let descriptor = array_descriptor(columns, column)?;
    if !descriptor.is_array {
        return Ok(None);
    }
    if !is_ism_indirect_array(descriptor) {
        if descriptor.shape.is_empty() {
            return Ok(None);
        }
        let shape = descriptor.shape.iter().map(|&size| size as usize).collect();
        return Ok(Some(vec![Some(shape); rows.len()]));
    }
    let mut source = ArrayRows::open(path, columns.len(), column, true)?;
    rows.iter()
        .map(|&row| match source.indirect_cell(row)? {
            Some((reader, offset)) => reader.read_shape_at(offset),
            None => Ok(None),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub(crate) fn read_ism_array_column_rows(
    path: &Path,
    columns: &[&ColumnDescContents],
    column: usize,
    rows: &[usize],
) -> Result<Option<Vec<Option<ArrayValue>>>, StorageError> {
    if rows.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let descriptor = array_descriptor(columns, column)?;
    if !descriptor.is_array {
        return Ok(None);
    }
    let kind = CasacoreDataType::from_primitive_type(descriptor.require_primitive_type()?, false);
    if is_ism_indirect_array(descriptor) {
        let mut source = ArrayRows::open(path, columns.len(), column, true)?;
        let values = rows
            .iter()
            .map(|&row| match source.indirect_cell(row)? {
                Some((reader, offset)) => {
                    reader.read_array_at(offset, kind)?.map(array).transpose()
                }
                None => Ok(None),
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Some(values));
    }

    let width = ism_element_size(kind);
    if width == 0 || descriptor.shape.is_empty() {
        return Ok(None);
    }
    let shape = descriptor
        .shape
        .iter()
        .map(|&size| size as usize)
        .collect::<Vec<_>>();
    let elements = shape.iter().try_fold(1usize, |count, &size| {
        count
            .checked_mul(size)
            .ok_or_else(|| invalid("array size overflow"))
    })?;
    let bytes = if kind == CasacoreDataType::TpBool {
        elements.div_ceil(8)
    } else {
        elements
            .checked_mul(width)
            .ok_or_else(|| invalid("array size overflow"))?
    };
    let mut source = ArrayRows::open(path, columns.len(), column, false)?;
    let big_endian = source.header.big_endian;
    rows.iter()
        .map(|&row| {
            let value = match source.payload(row)? {
                Some(payload) => decode_array_cell(
                    payload
                        .get(..bytes)
                        .ok_or_else(|| invalid("array cell exceeds bucket"))?,
                    kind,
                    elements,
                    &descriptor.shape,
                    big_endian,
                )?,
                None => make_empty_array(kind, &shape),
            };
            array(value).map(Some)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn array_descriptor<'a>(
    columns: &[&'a ColumnDescContents],
    column: usize,
) -> Result<&'a ColumnDescContents, StorageError> {
    columns
        .get(column)
        .copied()
        .ok_or_else(|| invalid("array column index is out of range"))
}

fn array(value: Value) -> Result<ArrayValue, StorageError> {
    match value {
        Value::Array(value) => Ok(value),
        _ => Err(invalid("invalid array cell")),
    }
}

fn invalid(message: &str) -> StorageError {
    StorageError::FormatMismatch(format!("ISM {message}"))
}

struct ArrayRows {
    file: File,
    header: IsmHeader,
    index: IsmIndex,
    column_count: usize,
    column: usize,
    bucket: Option<(u32, IsmBucket)>,
    array_path: PathBuf,
    indirect: Option<StManArrayFileReader>,
}

impl ArrayRows {
    fn open(
        path: &Path,
        column_count: usize,
        column: usize,
        indirect: bool,
    ) -> Result<Self, StorageError> {
        let mut file = File::open(path)?;
        let header = parse_ism_header(&mut file)?;
        let index = parse_ism_index(&mut file, &header)?;
        let array_path = PathBuf::from(format!("{}i", path.display()));
        let indirect = (indirect && array_path.is_file())
            .then(|| StManArrayFileReader::open(&array_path, header.big_endian))
            .transpose()?;
        Ok(Self {
            file,
            header,
            index,
            column_count,
            column,
            bucket: None,
            array_path,
            indirect,
        })
    }

    fn payload(&mut self, row: usize) -> Result<Option<&[u8]>, StorageError> {
        let interval = self
            .index
            .rows
            .partition_point(|&start| start <= row as u64)
            .checked_sub(1)
            .filter(|&slot| slot < self.index.bucket_nrs.len())
            .ok_or_else(|| invalid(&format!("array row {row} is outside the index")))?;
        let number = self.index.bucket_nrs[interval];
        if self
            .bucket
            .as_ref()
            .is_none_or(|(cached, _)| *cached != number)
        {
            let bytes = read_ism_bucket(&mut self.file, &self.header, number)?;
            let bucket = parse_ism_bucket(&bytes, self.column_count, self.header.big_endian)?;
            self.bucket = Some((number, bucket));
        }
        let (_, bucket) = self.bucket.as_ref().expect("ISM bucket was loaded");
        let Some(indices) = bucket.col_indices.get(self.column) else {
            return Ok(None);
        };
        let relative = u32::try_from(row as u64 - self.index.rows[interval])
            .map_err(|_| invalid("relative array row is too large"))?;
        let entry = get_interval(indices, relative);
        let offset = *indices
            .offsets
            .get(entry)
            .ok_or_else(|| invalid("array cell offset is missing"))?;
        bucket
            .data
            .get(offset as usize..)
            .map(Some)
            .ok_or_else(|| invalid("array cell offset exceeds bucket"))
    }

    fn indirect_cell(
        &mut self,
        row: usize,
    ) -> Result<Option<(&mut StManArrayFileReader, i64)>, StorageError> {
        let big_endian = self.header.big_endian;
        let Some(payload) = self.payload(row)? else {
            return Ok(None);
        };
        let bytes = payload
            .get(..8)
            .ok_or_else(|| invalid("indirect offset exceeds bucket"))?;
        let offset = if big_endian {
            read_i64_be(bytes)
        } else {
            read_i64_le(bytes)
        };
        match self.indirect.as_mut() {
            Some(reader) => Ok(Some((reader, offset))),
            None if offset == 0 => Ok(None),
            None => Err(StorageError::MissingDataFile(self.array_path.clone())),
        }
    }
}
