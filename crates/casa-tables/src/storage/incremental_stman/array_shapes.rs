// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shapes follow the ISM's existing row/bucket/indirect-array registration.
use super::*;

pub(crate) fn read_ism_array_column_shapes(
    path: &Path,
    columns: &[&ColumnDescContents],
    column: usize,
    rows: &[usize],
) -> Result<Option<Vec<Option<Vec<usize>>>>, StorageError> {
    if rows.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let descriptor = columns.get(column).ok_or_else(|| {
        StorageError::FormatMismatch("ISM shape column index is out of range".to_string())
    })?;
    if !descriptor.is_array {
        return Ok(None);
    }
    if !is_ism_indirect_array(descriptor) {
        if descriptor.shape.is_empty() {
            return Ok(None);
        }
        let shape = descriptor
            .shape
            .iter()
            .map(|&size| size as usize)
            .collect::<Vec<_>>();
        return Ok(Some(vec![Some(shape); rows.len()]));
    }
    let mut file = File::open(path)?;
    let header = parse_ism_header(&mut file)?;
    let index = parse_ism_index(&mut file, &header)?;
    let array_path = PathBuf::from(format!("{}i", path.display()));
    let mut reader = array_path
        .is_file()
        .then(|| StManArrayFileReader::open(&array_path, header.big_endian))
        .transpose()?;
    let mut cached = None;
    let mut cached_number = None;
    let mut result = Vec::with_capacity(rows.len());
    for &row in rows {
        let interval = index
            .rows
            .partition_point(|&start| start <= row as u64)
            .checked_sub(1)
            .filter(|&slot| slot < index.bucket_nrs.len())
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("ISM shape row {row} is outside the index"))
            })?;
        let number = index.bucket_nrs[interval];
        if cached_number != Some(number) {
            let bytes = read_ism_bucket(&mut file, &header, number)?;
            cached = Some(parse_ism_bucket(&bytes, columns.len(), header.big_endian)?);
            cached_number = Some(number);
        }
        let bucket = cached.as_ref().expect("bucket initialized");
        let Some(indices) = bucket.col_indices.get(column) else {
            result.push(None);
            continue;
        };
        let entry = get_interval(indices, (row as u64 - index.rows[interval]) as u32);
        let value = read_scalar_at(
            &bucket.data,
            indices.offsets[entry] as usize,
            CasacoreDataType::TpInt64,
            header.big_endian,
        )?;
        let casa_types::Value::Scalar(ScalarValue::Int64(offset)) = value else {
            return Err(StorageError::FormatMismatch(
                "ISM shape offset is not Int64".to_string(),
            ));
        };
        result.push(match reader.as_mut() {
            Some(reader) => reader.read_shape_at(offset)?,
            None if offset == 0 => None,
            None => return Err(StorageError::MissingDataFile(array_path)),
        });
    }
    Ok(Some(result))
}
