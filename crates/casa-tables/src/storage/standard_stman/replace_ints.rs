// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stage whole Int32 columns using the existing fixed-width bucket layout.

use std::fs;

use super::*;
use crate::storage::table_control::DataManagerEntry;

pub(in crate::storage) fn stage_int32_replacement(
    original: &Path,
    staged: &Path,
    manager: &DataManagerEntry,
    columns: &[ColumnDescContents],
    replacements: &BTreeMap<String, Vec<Option<Value>>>,
    rows: u64,
) -> Result<bool, StorageError> {
    let mut selected = Vec::new();
    for (index, column) in columns.iter().enumerate() {
        let Some(values) = replacements.get(&column.col_name) else {
            continue;
        };
        if column.data_type != CasacoreDataType::TpInt {
            return Ok(false);
        }
        let Some(values) = values
            .iter()
            .map(|value| match value {
                Some(Value::Scalar(ScalarValue::Int32(value))) => Some(*value),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(false);
        };
        selected.push((index, values));
    }
    let selected = selected
        .iter()
        .map(|(column, values)| (*column, values.as_slice()))
        .collect::<Vec<_>>();
    replace_int32_columns(original, staged, manager, &selected, rows)
}

pub(in crate::storage) fn replace_int32_columns(
    original: &Path,
    target: &Path,
    manager: &DataManagerEntry,
    selected: &[(usize, &[i32])],
    rows: u64,
) -> Result<bool, StorageError> {
    if rows == 0 || selected.is_empty() {
        return Ok(false);
    }

    let mut input = File::open(original)?;
    let header = parse_ssm_header(&mut input)?;
    let dm_info = parse_ssm_dm_blob(&manager.data)?;
    let indices = parse_ssm_indices(&mut input, &header)?;
    let mut edits = BTreeMap::<u32, Vec<(usize, &[i32])>>::new();
    for (column, values) in selected {
        let (&offset, index) = dm_info
            .column_offsets
            .get(*column)
            .zip(dm_info.col_index_map.get(*column))
            .and_then(|(offset, &index)| Some((offset, indices.get(index as usize)?)))
            .ok_or_else(|| invalid("missing StandardStMan column layout"))?;
        let mut start = 0;
        for (&last, &bucket) in index
            .last_row
            .iter()
            .zip(&index.bucket_number)
            .take(index.n_used as usize)
        {
            let end = last
                .checked_add(1)
                .and_then(|end| usize::try_from(end).ok());
            let chunk = end
                .and_then(|end| values.get(start..end))
                .filter(|chunk| !chunk.is_empty())
                .ok_or_else(|| invalid("invalid StandardStMan data bucket rows"))?;
            edits
                .entry(bucket)
                .or_default()
                .push((offset as usize, chunk));
            start += chunk.len();
        }
        if start as u64 != rows {
            return Err(invalid("StandardStMan index does not cover the table"));
        }
    }

    if original != target {
        fs::copy(original, target)?;
    }
    let mut output = OpenOptions::new().write(true).open(target)?;
    let mut buckets = ReadBuckets::new(input)?;
    let mut bytes = Vec::new();
    for (bucket, edits) in edits {
        bytes.clear();
        bytes.extend_from_slice(buckets.read(&header, bucket)?);
        for (offset, values) in edits {
            let target = bytes
                .get_mut(offset..offset + std::mem::size_of_val(values))
                .ok_or_else(|| invalid("StandardStMan column exceeds its data bucket"))?;
            for (slot, &value) in target.as_chunks_mut::<4>().0.iter_mut().zip(values) {
                if header.big_endian {
                    slot.copy_from_slice(&value.to_be_bytes());
                } else {
                    slot.copy_from_slice(&value.to_le_bytes());
                }
            }
        }
        output.seek(SeekFrom::Start(bucket_offset(
            bucket as usize,
            header.bucket_size as usize,
        )))?;
        output.write_all(&bytes)?;
    }
    Ok(true)
}

fn invalid(message: &str) -> StorageError {
    StorageError::FormatMismatch(message.into())
}
