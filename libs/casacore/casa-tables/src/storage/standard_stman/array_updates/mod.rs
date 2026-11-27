// SPDX-License-Identifier: LGPL-3.0-or-later
//! Replace array cells without decoding untouched StandardStMan columns.

use super::*;

type ArrayColumns = Vec<Option<Vec<(usize, Option<ArrayValue>)>>>;

struct ArrayMutation {
    offset: usize,
    data_type: CasacoreDataType,
    direct_elements: Option<usize>,
    value: Option<Value>,
}

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
    let mut has_indirect = false;
    for (desc, values) in descs.iter().zip(&columns) {
        if let Some(values) = values {
            let indirect = is_ssm_array_file_indirect(desc) && !desc.is_record();
            let direct = !is_ssm_indirect(desc)
                && desc.is_array
                && desc.data_type == CasacoreDataType::TpComplex;
            if !indirect && !direct {
                return Ok(false);
            }
            has_indirect |= indirect;
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
    if has_indirect && array_path.exists() {
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
    let mut buckets: BTreeMap<u32, Vec<ArrayMutation>> = BTreeMap::new();
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
        let desc = &descs[column];
        let data_type =
            CasacoreDataType::from_primitive_type(desc.require_primitive_type()?, false);
        let direct_elements = (!is_ssm_indirect(desc)).then(|| ssm_cell_element_count(desc));
        let width = direct_elements.map_or(8, |elements| {
            elements.saturating_mul(canonical_element_size(data_type).0)
        });
        for (row, value) in values {
            let (bucket, start, _) = index.find_bucket(row as u64).ok_or_else(|| {
                StorageError::FormatMismatch(format!("SSM missing array row {row}"))
            })?;
            let at = (offset as usize)
                .saturating_add(((row as u64 - start) as usize).saturating_mul(width));
            if at
                .checked_add(width)
                .is_none_or(|end| end > header.bucket_size as usize)
            {
                return Err(StorageError::FormatMismatch(
                    "SSM array offset outside bucket".into(),
                ));
            }
            buckets.entry(bucket).or_default().push(ArrayMutation {
                offset: at,
                data_type,
                direct_elements,
                value: value.map(Value::Array),
            });
        }
    }
    let file_length = file.metadata()?.len();
    for &number in buckets.keys() {
        let end = SSM_HEADER_SIZE + (u64::from(number) + 1) * u64::from(header.bucket_size);
        if end > file_length {
            return Err(StorageError::FormatMismatch(format!(
                "SSM array bucket {number} is outside the data file"
            )));
        }
    }
    let mut writer = has_indirect
        .then(|| {
            if array_path.exists() {
                StManArrayFileWriter::open_append(&array_path, big_endian)
            } else {
                StManArrayFileWriter::create(&array_path, big_endian, 0)
            }
        })
        .transpose()?;
    write_buckets(&mut file, &header, buckets, writer.as_mut())?;
    if let Some(mut writer) = writer {
        writer.finish()?;
    }
    Ok(true)
}

fn write_buckets(
    file: &mut File,
    header: &SsmHeader,
    buckets: BTreeMap<u32, Vec<ArrayMutation>>,
    mut writer: Option<&mut StManArrayFileWriter>,
) -> Result<(), StorageError> {
    // Patch adjacent touched buckets together, without rewriting a bucket
    // outside the selection. The buffer never grows beyond 64 KiB or one
    // stored bucket, whichever is larger.
    let bucket_size = header.bucket_size as usize;
    let maximum = (64 * 1024 / bucket_size.max(1)).max(1);
    let mut pending = buckets.into_iter().peekable();
    let mut batch = Vec::new();
    let mut buffer = Vec::new();
    while let Some((first, mutations)) = pending.next() {
        batch.push((first, mutations));
        while batch.len() < maximum
            && pending
                .peek()
                .is_some_and(|(number, _)| first.checked_add(batch.len() as u32) == Some(*number))
        {
            batch.push(pending.next().expect("peeked adjacent bucket"));
        }
        let bytes = batch
            .len()
            .checked_mul(bucket_size)
            .ok_or_else(|| StorageError::FormatMismatch("SSM bucket batch size overflow".into()))?;
        buffer
            .try_reserve_exact(bytes.saturating_sub(buffer.len()))
            .map_err(|error| {
                StorageError::FormatMismatch(format!("cannot allocate SSM bucket batch: {error}"))
            })?;
        buffer.resize(bytes, 0);
        let start = SSM_HEADER_SIZE + u64::from(first) * u64::from(header.bucket_size);
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut buffer)?;
        for (slot, (_, mutations)) in batch.drain(..).enumerate() {
            let bucket = &mut buffer[slot * bucket_size..(slot + 1) * bucket_size];
            for mutation in mutations {
                let destination = &mut bucket[mutation.offset..];
                if let Some(elements) = mutation.direct_elements {
                    encode_fixed_cell(
                        destination,
                        mutation.value.as_ref().into(),
                        mutation.data_type,
                        elements,
                        header.big_endian,
                    );
                    continue;
                }
                let offset = match mutation.value {
                    Some(value) => writer
                        .as_mut()
                        .expect("indirect arrays have a writer")
                        .write_array(&value, mutation.data_type)?,
                    None => 0,
                };
                if header.big_endian {
                    write_i64_be(destination, offset);
                } else {
                    write_i64_le(destination, offset);
                }
            }
        }
        file.seek(SeekFrom::Start(start))?;
        file.write_all(&buffer)?;
    }
    Ok(())
}
