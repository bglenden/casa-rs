// SPDX-License-Identifier: LGPL-3.0-or-later
//! Stage a row append without moving the buckets that existing readers use.

use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use casa_core::RecordValue;

use super::column_writer::write_indirect_cell;
use super::index::{IndexBucketSink, index_bucket_offset};
use super::string_buckets::StringBuckets;
use super::{
    ColumnDescContents, SsmHeader, SsmIndex, StManArrayFileWriter, StorageError, bucket_offset,
    is_ssm_indirect, last_string_bucket, parse_ssm_dm_blob, parse_ssm_header, parse_ssm_indices,
    read_bucket, read_i32_be, serialize_ssm_header, ssm_cell_element_count, write_cell_to_bucket,
    write_i64_be, write_i64_le,
};

pub(crate) struct StagedRecords {
    pub(crate) retained_end: u64,
    pub(crate) new_end: u64,
    pub(crate) header: Vec<u8>,
    pub(crate) tail: tempfile::SpooledTempFile,
}

/// Stage the new header and tail without copying the retained buckets.
pub(crate) fn stage_records(
    original: &Path,
    destination: &Path,
    columns: &[ColumnDescContents],
    layout: &[u8],
    old_rows: u64,
    records: &[RecordValue],
    defaults: &RecordValue,
) -> Result<StagedRecords, StorageError> {
    let mut source = File::open(original)?;
    let header = parse_ssm_header(&mut source)?;
    let mut indices = parse_ssm_indices(&mut source, &header)?;
    let layout = parse_ssm_dm_blob(layout)?;
    let malformed = || StorageError::FormatMismatch("invalid StandardStMan append layout".into());
    if indices.is_empty()
        || indices.iter().any(|index| index.rows_per_bucket == 0)
        || layout.col_index_map.len() < columns.len()
        || layout.column_offsets.len() < columns.len()
        || layout
            .col_index_map
            .iter()
            .take(columns.len())
            .any(|&index| index as usize >= indices.len())
    {
        return Err(malformed());
    }

    let indirect_path = |path: &Path| {
        let mut name = path.as_os_str().to_os_string();
        name.push("i");
        PathBuf::from(name)
    };
    let mut arrays = if columns.iter().any(is_ssm_indirect) {
        let original = indirect_path(original);
        let destination = indirect_path(destination);
        Some(if original.exists() {
            fs::copy(original, &destination)?;
            StManArrayFileWriter::open_append(&destination, header.big_endian)?
        } else {
            StManArrayFileWriter::create(&destination, header.big_endian, 0)?
        })
    } else {
        None
    };
    let bucket_bytes = header.bucket_size as usize;
    let first_changed_bucket = append_start(&mut source, &header, &indices)? as usize;
    let retained_end = bucket_offset(first_changed_bucket, bucket_bytes);
    let tail_offset = |bucket| bucket_offset(bucket, bucket_bytes) - retained_end;
    // A small append commonly uses one data bucket per index, one string
    // bucket, and one index bucket. Larger tails spill to a temporary file.
    let mut tail = tempfile::spooled_tempfile_in(
        bucket_bytes.saturating_mul(indices.len().saturating_add(2)),
        destination.parent().expect("staged manager directory"),
    );
    let mut next_bucket = first_changed_bucket;
    let string_start = next_bucket
        + indices
            .iter()
            .map(|index| records.len().div_ceil(index.rows_per_bucket as usize))
            .sum::<usize>();
    let mut strings = StringBuckets::after(string_start);
    let mut columns_per_index = vec![0; indices.len()];
    let mut data = vec![0; bucket_bytes];
    for (number, index) in indices.iter_mut().enumerate() {
        let managed = columns
            .iter()
            .enumerate()
            .filter(|(slot, _)| layout.col_index_map[*slot] as usize == number)
            .collect::<Vec<_>>();
        columns_per_index[number] = managed.len() as i32;
        index.last_row.truncate(index.n_used as usize);
        index.bucket_number.truncate(index.n_used as usize);
        let rows_per_bucket = index.rows_per_bucket as usize;
        for (block, rows) in records.chunks(rows_per_bucket).enumerate() {
            data.fill(0);
            for &(slot, column) in &managed {
                let offset = layout.column_offsets[slot] as usize;
                for (row, record) in rows.iter().enumerate() {
                    let value = record
                        .get(&column.col_name)
                        .or_else(|| defaults.get(&column.col_name));
                    if is_ssm_indirect(column) {
                        let array_offset = write_indirect_cell(
                            arrays.as_mut().expect("indirect array file"),
                            column,
                            value,
                        )?;
                        let bytes = data
                            .get_mut(offset + row * 8..offset + row * 8 + 8)
                            .ok_or_else(malformed)?;
                        if header.big_endian {
                            write_i64_be(bytes, array_offset);
                        } else {
                            write_i64_le(bytes, array_offset);
                        }
                    } else {
                        write_cell_to_bucket(
                            &mut data,
                            offset,
                            row,
                            column,
                            column.data_type,
                            ssm_cell_element_count(column),
                            value,
                            &mut |payload| {
                                strings.stream(payload, bucket_bytes, |number, bucket| {
                                    tail.seek(SeekFrom::Start(tail_offset(string_start + number)))?;
                                    tail.write_all(bucket)?;
                                    Ok(())
                                })
                            },
                            string_start,
                            header.big_endian,
                        )?;
                    }
                }
            }
            tail.seek(SeekFrom::Start(tail_offset(next_bucket)))?;
            tail.write_all(&data)?;
            index
                .last_row
                .push(old_rows + (block * rows_per_bucket + rows.len() - 1) as u64);
            index
                .bucket_number
                .push(u32::try_from(next_bucket).map_err(|_| malformed())?);
            next_bucket += 1;
        }
    }
    strings.release(|number, bucket| {
        tail.seek(SeekFrom::Start(tail_offset(string_start + number)))?;
        tail.write_all(bucket)?;
        Ok(())
    })?;
    if let Some(writer) = arrays.as_mut() {
        writer.finish()?;
    }
    let first_index = string_start + strings.len();
    let mut sink =
        IndexBucketSink::at_offset(tail, first_index, bucket_bytes, tail_offset(first_index));
    for (index, columns) in indices.iter().zip(columns_per_index) {
        sink = sink.write_index(
            index.bucket_number.len() as u32,
            index.rows_per_bucket,
            columns,
            index.last_row.iter().copied(),
            index.bucket_number.iter().copied(),
            header.big_endian,
        )?;
    }
    let (mut tail, index_buckets, index_length) = sink.finish()?;
    let nr_buckets = first_index + index_buckets;
    let string_bucket = if strings.len() == 0 {
        header.last_string_bucket
    } else {
        last_string_bucket(string_start, strings.len())
    };
    let revised = serialize_ssm_header(
        header.bucket_size,
        u32::try_from(nr_buckets).map_err(|_| malformed())?,
        header.nr_free_buckets,
        header.first_free_bucket,
        index_buckets as u32,
        first_index as i32,
        index_bucket_offset(index_length, bucket_bytes),
        string_bucket,
        index_length as u32,
        header.nr_indices,
        header.pers_cache_size,
        header.big_endian,
    )?;
    tail.rewind()?;
    Ok(StagedRecords {
        retained_end,
        new_end: bucket_offset(nr_buckets, bucket_bytes),
        header: revised,
        tail,
    })
}

fn append_start(
    file: &mut File,
    header: &SsmHeader,
    indices: &[SsmIndex],
) -> Result<u32, StorageError> {
    let Ok(first) = u32::try_from(header.first_idx_bucket) else {
        return Ok(header.nr_buckets);
    };
    // Reclaim the previous index when it occupies the file tail. Ordinary
    // repeated appends then use space for the latest index only.
    if header.nr_idx_buckets == 0
        || first.checked_add(header.nr_idx_buckets) != Some(header.nr_buckets)
        || header.last_string_bucket >= header.first_idx_bucket
        || indices.iter().any(|index| {
            index
                .bucket_number
                .iter()
                .take(index.n_used as usize)
                .any(|&number| number >= first)
        })
    {
        return Ok(header.nr_buckets);
    }
    for number in first..header.nr_buckets - 1 {
        let current = read_bucket(file, header, number)?;
        if read_i32_be(&current[4..8]) != (number + 1) as i32 {
            return Ok(header.nr_buckets);
        }
    }
    Ok(first)
}
