// SPDX-License-Identifier: LGPL-3.0-or-later
//! StManAipsIO indirect offsets lead to shape headers, never numerical cells.
use super::*;

pub(crate) fn read_stman_array_column_shapes(
    path: &Path,
    columns: &[ColumnDescContents],
    column: usize,
    rows: &[usize],
    byte_order: ByteOrder,
) -> Result<Option<Vec<Option<Vec<usize>>>>, StorageError> {
    if rows.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let descriptor = columns.get(column).ok_or_else(|| {
        StorageError::FormatMismatch("StManAipsIO shape column index is out of range".to_string())
    })?;
    if descriptor.is_array && descriptor.option & 1 != 0 && !descriptor.shape.is_empty() {
        let shape = descriptor
            .shape
            .iter()
            .map(|&size| size as usize)
            .collect::<Vec<_>>();
        return Ok(Some(vec![Some(shape); rows.len()]));
    }
    let layouts = parse_sparse_column_layouts(path, columns, byte_order)?;
    let Some(Some(SparseColumnLayout::IndirectArrayOffsets { extents, .. })) = layouts.get(column)
    else {
        return Ok(None);
    };
    let mut file = std::fs::File::open(path)?;
    let mut offsets = Vec::with_capacity(rows.len());
    for &row in rows {
        file.seek(SeekFrom::Start(locate_extent_slot(extents, row)?))?;
        offsets.push(read_indirect_offset(&mut file)?);
    }
    let array_path = indirect_array_file_path(path);
    if offsets.iter().all(|&offset| offset == 0) {
        return Ok(Some(vec![None; rows.len()]));
    }
    let mut reader = StManArrayFileReader::open(&array_path, byte_order == ByteOrder::BigEndian)?;
    offsets
        .into_iter()
        .map(|offset| reader.read_shape_at(offset))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// Large AipsIO offsets are marker+i64, interleaved with ordinary u32s.
/// Keep contiguous runs of equal-width entries so requested rows can seek
/// directly without reading any array payload.
pub(super) fn parse_indirect_offsets_layout(
    file: &mut std::fs::File,
    nrrow: usize,
    data_type: CasacoreDataType,
    large_offsets: bool,
) -> Result<Option<SparseColumnLayout>, StorageError> {
    let mut extents: Vec<ExtentLayout> = Vec::new();
    let mut row_start = 0usize;
    while row_start < nrrow {
        let raw_count = read_be_u32(file)? as usize;
        let row_count = if raw_count == 0 {
            nrrow - row_start
        } else {
            raw_count
        };
        if row_count > nrrow - row_start {
            return Err(StorageError::FormatMismatch(
                "AipsIO offset extent exceeds row count".into(),
            ));
        }
        if !large_offsets {
            let values_start = file.stream_position()?;
            file.seek(SeekFrom::Current((row_count * size_of::<u32>()) as i64))?;
            extents.push(ExtentLayout {
                row_start,
                row_count,
                values_start,
                row_width_bytes: size_of::<u32>(),
            });
            row_start += row_count;
            continue;
        }
        for row in row_start..row_start + row_count {
            let values_start = file.stream_position()?;
            let _offset = read_indirect_offset(file)?;
            let width = (file.stream_position()? - values_start) as usize;
            if let Some(previous) = extents.last_mut().filter(|previous| {
                previous.row_width_bytes == width
                    && previous.values_start + (previous.row_count * width) as u64 == values_start
            }) {
                previous.row_count += 1;
            } else {
                extents.push(ExtentLayout {
                    row_start: row,
                    row_count: 1,
                    values_start,
                    row_width_bytes: width,
                });
            }
        }
        row_start += row_count;
    }
    Ok(Some(SparseColumnLayout::IndirectArrayOffsets {
        data_type,
        extents,
    }))
}

fn read_indirect_offset(file: &mut std::fs::File) -> Result<i64, StorageError> {
    let offset = read_be_u32(file)?;
    if offset != LARGE_OFFSET_MARKER {
        return Ok(i64::from(offset));
    }
    let mut bytes = [0u8; 8];
    file.read_exact(&mut bytes)?;
    Ok(i64::from_be_bytes(bytes))
}
