// SPDX-License-Identifier: LGPL-3.0-or-later
//! Replace indirect array cells without decoding untouched SSM columns.

use super::*;

type ArrayColumns = Vec<Option<Vec<(usize, Option<ArrayValue>)>>>;

#[cfg(test)]
mod tests;

/// Append replacement records and patch their bucket offsets. SSMBase uses
/// version zero array files (no reference counts). Appending, rather than
/// overwriting an old record, also preserves any other cells sharing it.
/// Unsupported layouts return false before changing any bytes.
pub(crate) fn save_ssm_array_rows_in_place(
    path: &Path,
    dm_blob: &[u8],
    descs: &[ColumnDescContents],
    columns: ArrayColumns,
    big_endian: bool,
) -> Result<bool, StorageError> {
    if columns.len() != descs.len() {
        return Err(StorageError::FormatMismatch(
            "SSM array column count mismatch".into(),
        ));
    }
    if columns.iter().all(Option::is_none) {
        return Ok(false);
    }
    for (desc, values) in descs.iter().zip(&columns) {
        if let Some(values) = values {
            if !is_ssm_array_file_indirect(desc) || desc.is_record() {
                return Ok(false);
            }
            for (_, value) in values {
                if let Some(value) = value
                    && Some(value.primitive_type()) != desc.primitive_type
                {
                    return Err(StorageError::FormatMismatch(format!(
                        "SSM array replacement type mismatch for {}",
                        desc.col_name
                    )));
                }
            }
        }
    }
    let mut array_path = path.as_os_str().to_os_string();
    array_path.push("i");
    let array_path = std::path::PathBuf::from(array_path);
    if array_path.exists() {
        let mut version = [0; 4];
        File::open(&array_path)?.read_exact(&mut version)?;
        // Zero is the same in either byte order.
        if version != [0; 4] {
            return Ok(false);
        }
    }
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let header = parse_ssm_header(&mut file)?;
    if header.big_endian != big_endian {
        return Err(StorageError::FormatMismatch(
            "SSM array save endian mismatch".into(),
        ));
    }
    let dm = parse_ssm_dm_blob(dm_blob)?;
    let indices = parse_ssm_indices(&mut file, &header)?;
    // Complete layout validation before appending or modifying any bucket.
    type Mutation = (usize, CasacoreDataType, Option<Value>);
    let mut buckets: BTreeMap<u32, Vec<Mutation>> = BTreeMap::new();
    for (column, values) in columns.into_iter().enumerate() {
        let Some(values) = values else { continue };
        let (Some(&offset), Some(&index)) =
            (dm.column_offsets.get(column), dm.col_index_map.get(column))
        else {
            return Err(StorageError::FormatMismatch(
                "SSM missing array column metadata".into(),
            ));
        };
        let index = indices
            .get(index as usize)
            .ok_or_else(|| StorageError::FormatMismatch("SSM missing array column index".into()))?;
        for (row, value) in values {
            let (bucket, start, _) = index.find_bucket(row as u64).ok_or_else(|| {
                StorageError::FormatMismatch(format!("SSM missing array row {row}"))
            })?;
            let at = offset as usize + (row as u64 - start) as usize * 8;
            if at
                .checked_add(8)
                .is_none_or(|end| end > header.bucket_size as usize)
            {
                return Err(StorageError::FormatMismatch(
                    "SSM array offset outside bucket".into(),
                ));
            }
            buckets.entry(bucket).or_default().push((
                at,
                CasacoreDataType::from_primitive_type(
                    descs[column].require_primitive_type()?,
                    false,
                ),
                value.map(Value::Array),
            ));
        }
    }
    let mut writer = if array_path.exists() {
        StManArrayFileWriter::open_append(&array_path, big_endian)?
    } else {
        StManArrayFileWriter::create(&array_path, big_endian, 0)?
    };
    for (number, mutations) in buckets {
        let mut bucket = read_bucket(&mut file, &header, number)?;
        for (at, data_type, value) in mutations {
            let offset = match value {
                Some(value) => writer.write_array(&value, data_type)?,
                None => 0,
            };
            if big_endian {
                write_i64_be(&mut bucket[at..], offset);
            } else {
                write_i64_le(&mut bucket[at..], offset);
            }
        }
        file.seek(SeekFrom::Start(bucket_offset(
            number as usize,
            header.bucket_size as usize,
        )))?;
        file.write_all(&bucket)?;
    }
    writer.finish()?;
    Ok(true)
}
