// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use super::super::stman_array_file::StManArrayFileWriter;
use super::{
    CasacoreDataType, ColumnDescContents, ColumnSources, EncodedCells, ISM_BUCKET_TARGET_ROWS,
    ISM_HEADER_SIZE, SCALAR_BLOCK_CELLS, StorageError, encode_cell, is_ism_indirect_array,
    ism_element_size, serialize_ism_dm_blob, serialize_ism_header, serialize_ism_index,
    write_ism_file_scalar_run_columns,
};

/// Each column's element type and element count, and the bucket size, of
/// an IncrementalStMan file: room
/// for [`ISM_BUCKET_TARGET_ROWS`] rows of every value and its index entry,
/// a string counted as 12 bytes. Indirect arrays occupy one Int64 offset.
fn ism_source_layout(
    col_descs: &[ColumnDescContents],
) -> Result<(Vec<(CasacoreDataType, usize)>, u32), StorageError> {
    if let Some(col_desc) = col_descs.iter().find(|col_desc| col_desc.is_record()) {
        return Err(StorageError::FormatMismatch(format!(
            "ISM column source writer stores scalars and fixed-shape arrays, not column {}",
            col_desc.col_name
        )));
    }
    let col_info: Vec<(CasacoreDataType, usize)> = col_descs
        .iter()
        .map(|c| {
            if is_ism_indirect_array(c) {
                return Ok((CasacoreDataType::TpInt64, 1));
            }
            let dt = CasacoreDataType::from_primitive_type(c.require_primitive_type()?, false);
            let nrelem = c.shape.iter().map(|&dim| dim as usize).product::<usize>();
            Ok((dt, if c.is_array { nrelem } else { 1 }))
        })
        .collect::<Result<_, StorageError>>()?;
    let fixed_bytes_per_row: usize = col_info
        .iter()
        .map(|&(dt, nrelem)| {
            if dt == CasacoreDataType::TpBool {
                nrelem.div_ceil(8)
            } else {
                let elem = ism_element_size(dt);
                if elem > 0 { elem * nrelem } else { nrelem * 12 }
            }
        })
        .sum();
    let index_overhead_per_row = col_descs.len() * 8;
    let bytes_per_row = fixed_bytes_per_row + index_overhead_per_row;
    let bucket_size = if bytes_per_row == 0 {
        128u32
    } else {
        let target = (bytes_per_row * ISM_BUCKET_TARGET_ROWS + 4) as u32;
        target.clamp(128, 327680)
    };
    Ok((col_info, bucket_size))
}

/// Write an IncrementalStMan file from column sources, a block of cells at
/// a time: scalars and fixed-shape arrays, in buckets of
/// [`ism_source_layout`]'s size. A bucket starts with each column's value
/// at its first row and then holds each value that changes; a row whose
/// values do not fit an empty bucket is refused.
pub(crate) fn write_ism_file_column_sources(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    scalar_columns: &ColumnSources<'_>,
    big_endian: bool,
) -> Result<Vec<u8>, StorageError> {
    write_ism_column_sources_with_layout(file_path, col_descs, scalar_columns, big_endian, None, 1)
}

pub(super) fn write_ism_column_sources_with_layout(
    file_path: &Path,
    col_descs: &[ColumnDescContents],
    scalar_columns: &ColumnSources<'_>,
    big_endian: bool,
    bucket: Option<u32>,
    cache_size: u32,
) -> Result<Vec<u8>, StorageError> {
    if scalar_columns.column_count() != col_descs.len() {
        return Err(StorageError::FormatMismatch(format!(
            "ISM column source count mismatch: expected {}, got {}",
            col_descs.len(),
            scalar_columns.column_count()
        )));
    }
    let (col_info, default_bucket) = ism_source_layout(col_descs)?;
    let bucket_size = bucket.unwrap_or(default_bucket);
    let nrrow = scalar_columns.row_count();
    let ncol = col_descs.len();
    if (bucket_size as usize) < 4 + 12 * ncol {
        return Err(StorageError::FormatMismatch(format!(
            "IncrementalStMan bucket size {bucket_size} cannot hold an index for {ncol} columns"
        )));
    }

    if let Some(scalar_runs) = (0..ncol)
        .map(|col_idx| scalar_columns.scalar_runs(col_idx))
        .collect::<Option<Vec<_>>>()
    {
        return write_ism_file_scalar_run_columns(
            file_path,
            nrrow,
            &col_info,
            &scalar_runs,
            bucket_size,
            big_endian,
            cache_size,
        );
    }

    let mut arrays = col_descs
        .iter()
        .any(is_ism_indirect_array)
        .then(|| {
            let mut array_name = file_path.as_os_str().to_os_string();
            array_name.push("i");
            StManArrayFileWriter::create(Path::new(&array_name), big_endian, 1)
        })
        .transpose()?;

    let mut bucket_start_rows: Vec<usize> = Vec::new();
    let mut last_values: Vec<Vec<u8>> = vec![Vec::new(); ncol];
    let mut file = File::create(file_path)?;
    file.write_all(&vec![0u8; ISM_HEADER_SIZE as usize])?;
    let mut nr_buckets = 0usize;

    if nrrow > 0 {
        let mut current = BucketBuilder {
            data: Vec::new(),
            col_indices: (0..ncol).map(|_| (Vec::new(), Vec::new())).collect(),
        };
        bucket_start_rows.push(0);

        // Each column's cells are encoded a block of rows at a time; the
        // bucket decisions then run row by row over the encoded bytes.
        let mut cells = (0..ncol)
            .map(|_| EncodedCells::default())
            .collect::<Vec<_>>();
        for block_start in (0..nrrow).step_by(SCALAR_BLOCK_CELLS) {
            let block = block_start..nrrow.min(block_start + SCALAR_BLOCK_CELLS);
            for (col_idx, cells) in cells.iter_mut().enumerate() {
                let (dt, nrelem) = col_info[col_idx];
                let desc = &col_descs[col_idx];
                if is_ism_indirect_array(desc) {
                    let writer = arrays.as_mut().expect("shared ISM array file");
                    let element_type = CasacoreDataType::from_primitive_type(
                        desc.require_primitive_type()?,
                        false,
                    );
                    cells.fill_with(
                        scalar_columns,
                        col_idx,
                        block.clone(),
                        dt,
                        big_endian,
                        |value| {
                            let offset = value
                                .map(|v| writer.write_array(v, element_type))
                                .transpose()?
                                .unwrap_or(0);
                            Ok(if big_endian {
                                offset.to_be_bytes()
                            } else {
                                offset.to_le_bytes()
                            }
                            .to_vec())
                        },
                    )?;
                } else {
                    cells.fill(
                        scalar_columns,
                        col_idx,
                        block.clone(),
                        dt,
                        big_endian,
                        |value| encode_cell(value.into(), dt, nrelem, big_endian),
                    )?;
                }
            }
            for row_idx in block {
                let cell = |col_idx: usize| cells[col_idx].cell(row_idx - block_start);
                let rel_row = (row_idx - *bucket_start_rows.last().unwrap()) as u32;

                let new_data_estimate: usize = (0..ncol)
                    .map(cell)
                    .zip(&last_values)
                    .filter(|(encoded, last)| rel_row == 0 || encoded != last)
                    .map(|(encoded, _)| encoded.len())
                    .sum();

                let current_index_size: usize = current
                    .col_indices
                    .iter()
                    .map(|(row_nrs, _)| 4 + (row_nrs.len() + 1) * 4 + (row_nrs.len() + 1) * 4)
                    .sum();
                let total_estimate =
                    4 + current.data.len() + new_data_estimate + current_index_size + ncol * 12;

                let starts_bucket = rel_row > 0 && total_estimate > bucket_size as usize;
                if starts_bucket {
                    write_bucket(&mut file, &current, bucket_size, big_endian)?;
                    nr_buckets += 1;
                    current = BucketBuilder {
                        data: Vec::new(),
                        col_indices: (0..ncol).map(|_| (Vec::new(), Vec::new())).collect(),
                    };
                    bucket_start_rows.push(row_idx);
                }
                let rel_row = if starts_bucket { 0 } else { rel_row };
                for (col_idx, last) in last_values.iter_mut().enumerate() {
                    let encoded = cell(col_idx);
                    if rel_row == 0 || encoded != last.as_slice() {
                        let offset = current.data.len() as u32;
                        current.data.extend_from_slice(encoded);
                        current.col_indices[col_idx].0.push(rel_row);
                        current.col_indices[col_idx].1.push(offset);
                        last.clear();
                        last.extend_from_slice(encoded);
                    }
                }
            }
        }

        write_bucket(&mut file, &current, bucket_size, big_endian)?;
        nr_buckets += 1;
    }

    let mut ism_rows: Vec<u64> = Vec::with_capacity(nr_buckets + 1);
    let mut ism_bucket_nrs: Vec<u32> = Vec::with_capacity(nr_buckets);
    for (i, &start) in bucket_start_rows.iter().enumerate() {
        ism_rows.push(start as u64);
        ism_bucket_nrs.push(i as u32);
    }
    ism_rows.push(nrrow as u64);

    if let Some(arrays) = &mut arrays {
        arrays.finish()?;
    }
    let index_data = serialize_ism_index(&ism_rows, &ism_bucket_nrs, big_endian)?;
    let header_buf = serialize_ism_header(bucket_size, nr_buckets as u32, big_endian, cache_size)?;

    file.write_all(&index_data)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header_buf)?;

    serialize_ism_dm_blob("ISM")
}

struct BucketBuilder {
    data: Vec<u8>,
    col_indices: Vec<(Vec<u32>, Vec<u32>)>,
}

fn write_bucket(
    file: &mut File,
    bucket: &BucketBuilder,
    bucket_size: u32,
    big_endian: bool,
) -> Result<(), StorageError> {
    let data_len = bucket.data.len();
    let index_len: usize = bucket
        .col_indices
        .iter()
        .map(|(row_nrs, _)| 4 + row_nrs.len() * 8)
        .sum();
    if 4 + data_len + index_len > bucket_size as usize {
        return Err(StorageError::FormatMismatch(format!(
            "ISM bucket requires {} bytes, exceeds planned bucket size {}",
            4 + data_len + index_len,
            bucket_size
        )));
    }
    let mut raw = vec![0u8; bucket_size as usize];
    raw[4..4 + data_len].copy_from_slice(&bucket.data);
    let index_start = raw.len() - index_len;
    let index_offset = index_start as u32;
    if big_endian {
        raw[0..4].copy_from_slice(&index_offset.to_be_bytes());
    } else {
        raw[0..4].copy_from_slice(&index_offset.to_le_bytes());
    }
    let mut cursor = index_start;
    for (row_nrs, offsets) in &bucket.col_indices {
        let count = row_nrs.len() as u32;
        let encoded_count = if big_endian {
            count.to_be_bytes()
        } else {
            count.to_le_bytes()
        };
        raw[cursor..cursor + 4].copy_from_slice(&encoded_count);
        cursor += 4;
        for &row in row_nrs {
            let encoded = if big_endian {
                row.to_be_bytes()
            } else {
                row.to_le_bytes()
            };
            raw[cursor..cursor + 4].copy_from_slice(&encoded);
            cursor += 4;
        }
        for &offset in offsets {
            let encoded = if big_endian {
                offset.to_be_bytes()
            } else {
                offset.to_le_bytes()
            };
            raw[cursor..cursor + 4].copy_from_slice(&encoded);
            cursor += 4;
        }
    }
    file.write_all(&raw)?;
    Ok(())
}
