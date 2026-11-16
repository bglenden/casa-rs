// SPDX-License-Identifier: LGPL-3.0-or-later
//! Validate the sparse row indexes in IncrementalStMan buckets.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use super::{ISM_HEADER_SIZE, StorageError, parse_ism_header, parse_ism_index};

/// Location of a non-ascending row number in an incremental storage bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalCorruption {
    /// First table row covered by the bucket.
    pub bucket_start_row: u64,
    /// Number of table rows covered by the bucket.
    pub bucket_rows: u64,
    /// Number of the bucket in its storage file.
    pub bucket_number: u32,
    /// One-based cursor into the bucket index.
    pub cursor: usize,
    /// Zero-based storage-manager column index.
    pub column: usize,
    /// Zero-based position of the offending entry.
    pub index: usize,
    /// The offending row number.
    pub row: u64,
    /// The preceding row number.
    pub previous_row: u64,
}

fn format_error(message: &str) -> StorageError {
    StorageError::FormatMismatch(message.into())
}

fn read_number(input: &mut impl Read, width: usize, big_endian: bool) -> Result<u64, StorageError> {
    let mut bytes = [0u8; 8];
    input.read_exact(&mut bytes[..width])?;
    Ok(match (width, big_endian) {
        (4, true) => u32::from_be_bytes(bytes[..4].try_into().unwrap()) as u64,
        (4, false) => u32::from_le_bytes(bytes[..4].try_into().unwrap()) as u64,
        (8, true) => u64::from_be_bytes(bytes),
        (8, false) => u64::from_le_bytes(bytes),
        _ => unreachable!(),
    })
}

fn check_bucket(
    input: &mut (impl Read + Seek),
    start: u64,
    size: u64,
    ncols: usize,
    big_endian: bool,
    mut location: IncrementalCorruption,
) -> Result<Option<IncrementalCorruption>, StorageError> {
    let end = start
        .checked_add(size)
        .ok_or_else(|| format_error("ISM bucket end overflow"))?;
    if size < 4 {
        return Err(format_error("ISM bucket too small"));
    }
    input.seek(SeekFrom::Start(start))?;
    let raw_offset = read_number(input, 4, big_endian)?;
    let row_width = if raw_offset & 0xf000_0000 == 0 { 4 } else { 8 };
    let offset = raw_offset & 0x0fff_ffff;
    if offset < 4 || offset > size {
        return Err(format_error("invalid ISM bucket index offset"));
    }
    input.seek(SeekFrom::Start(start + offset))?;
    for column in 0..ncols {
        let pos = input.stream_position()?;
        if end.saturating_sub(pos) < 4 {
            return Err(format_error("truncated ISM column index"));
        }
        let count = read_number(input, 4, big_endian)?;
        let bytes = count
            .checked_mul(row_width as u64 + 4)
            .ok_or_else(|| format_error("ISM index size overflow"))?;
        if bytes > end.saturating_sub(input.stream_position()?) {
            return Err(format_error("ISM column index exceeds bucket"));
        }
        let mut previous = None;
        for index in 0..count {
            let row = read_number(input, row_width, big_endian)?;
            if let Some(prev) = previous
                && row <= prev
            {
                location.column = column;
                location.index = index as usize;
                location.row = row;
                location.previous_row = prev;
                return Ok(Some(location));
            }
            previous = Some(row);
        }
        input.seek(SeekFrom::Current((count * 4) as i64))?;
    }
    Ok(None)
}

pub(crate) fn check_incremental_layout(
    path: &Path,
    ncols: usize,
) -> Result<Option<IncrementalCorruption>, StorageError> {
    let mut file = File::open(path)?;
    let header = parse_ism_header(&mut file)?;
    let bucket_end = ISM_HEADER_SIZE
        .checked_add(u64::from(header.nr_buckets) * u64::from(header.bucket_size))
        .ok_or_else(|| format_error("ISM file size overflow"))?;
    if bucket_end > file.metadata()?.len() {
        return Err(format_error("ISM buckets exceed file"));
    }
    let index = parse_ism_index(&mut file, &header)?;
    if index.used > index.bucket_nrs.len() || index.used >= index.rows.len() {
        return Err(format_error("invalid ISM bucket index"));
    }
    if index.rows[index.used] == 0 {
        return Ok(None);
    }
    for i in 0..index.used {
        let bucket_number = index.bucket_nrs[i];
        if bucket_number >= header.nr_buckets {
            return Err(format_error("invalid ISM bucket number"));
        }
        let bucket_rows = index.rows[i + 1]
            .checked_sub(index.rows[i])
            .ok_or_else(|| format_error("invalid ISM bucket row range"))?;
        let location = IncrementalCorruption {
            bucket_start_row: index.rows[i],
            bucket_rows,
            bucket_number,
            cursor: i + 1,
            column: 0,
            index: 0,
            row: 0,
            previous_row: 0,
        };
        let start = ISM_HEADER_SIZE + u64::from(bucket_number) * u64::from(header.bucket_size);
        if let Some(corruption) = check_bucket(
            &mut file,
            start,
            u64::from(header.bucket_size),
            ncols,
            header.big_endian,
            location,
        )? {
            return Ok(Some(corruption));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn finds_duplicate_and_decreasing_rows_in_both_widths_and_byte_orders() {
        for big in [false, true] {
            for wide in [false, true] {
                let u32bytes = |n: u32| {
                    if big {
                        n.to_be_bytes()
                    } else {
                        n.to_le_bytes()
                    }
                };
                let mut bytes = Vec::new();
                bytes.extend(u32bytes(if wide { 0x8000_0004 } else { 4 }));
                bytes.extend(u32bytes(3));
                for row in [0u64, 2, 2] {
                    if wide {
                        bytes.extend(if big {
                            row.to_be_bytes()
                        } else {
                            row.to_le_bytes()
                        });
                    } else {
                        bytes.extend(u32bytes(row as u32));
                    }
                }
                bytes.extend([0u8; 12]);
                let location = IncrementalCorruption {
                    bucket_start_row: 10,
                    bucket_rows: 4,
                    bucket_number: 2,
                    cursor: 1,
                    column: 0,
                    index: 0,
                    row: 0,
                    previous_row: 0,
                };
                let found = check_bucket(
                    &mut Cursor::new(&bytes),
                    0,
                    bytes.len() as u64,
                    1,
                    big,
                    location.clone(),
                )
                .unwrap()
                .unwrap();
                assert_eq!((found.index, found.row, found.previous_row), (2, 2, 2));
                let last = if wide { 8 + 2 * 8 } else { 8 + 2 * 4 };
                bytes[last..last + if wide { 8 } else { 4 }].fill(0);
                let found = check_bucket(
                    &mut Cursor::new(&bytes),
                    0,
                    bytes.len() as u64,
                    1,
                    big,
                    location,
                )
                .unwrap()
                .unwrap();
                assert_eq!((found.index, found.row, found.previous_row), (2, 0, 2));
            }
        }
    }

    #[test]
    fn rejects_truncated_indexes() {
        let bytes = [4, 0, 0, 0, 255, 255, 255, 255];
        let location = IncrementalCorruption {
            bucket_start_row: 0,
            bucket_rows: 1,
            bucket_number: 0,
            cursor: 1,
            column: 0,
            index: 0,
            row: 0,
            previous_row: 0,
        };
        assert!(
            check_bucket(
                &mut Cursor::new(bytes),
                0,
                bytes.len() as u64,
                1,
                false,
                location
            )
            .is_err()
        );
    }
}
