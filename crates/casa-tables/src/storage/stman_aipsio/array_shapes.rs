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
    let Some(Some(SparseColumnLayout::IndirectArrayOffsetsU32 { extents, .. })) =
        layouts.get(column)
    else {
        return Ok(None);
    };
    let mut file = std::fs::File::open(path)?;
    let mut offsets = Vec::with_capacity(rows.len());
    for &row in rows {
        file.seek(SeekFrom::Start(locate_extent_slot(extents, row)?))?;
        offsets.push(read_be_u32(&mut file)?);
    }
    let array_path = indirect_array_file_path(path);
    if offsets.iter().all(|&offset| offset == 0) {
        return Ok(Some(vec![None; rows.len()]));
    }
    if std::fs::metadata(&array_path)?.len() >= LARGE_OFFSET_MARKER as u64 {
        return Ok(None);
    }
    let mut reader = StManArrayFileReader::open(&array_path, byte_order == ByteOrder::BigEndian)?;
    offsets
        .into_iter()
        .map(|offset| reader.read_shape_at(i64::from(offset)))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}
