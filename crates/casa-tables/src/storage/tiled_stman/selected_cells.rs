// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read complete selected cells in tile order without retaining whole cubes.

use std::collections::BTreeMap;
use std::path::Path;

use super::{
    ArrayValue, CasacoreDataType, ColumnDescContents, SHARED_TILE_BIG_ENDIAN, SelectedCubeRow,
    SelectedTiles, StorageError, TILE_READ_SESSION_RECENT_TILE_COUNT, TileReadSession,
    TiledStManHeader, TsmCubeInfo, Value, copy_tile_to_cube, decode_array_value, linear_to_nd,
};

pub(crate) fn cell_read_buffer_bytes(
    table_path: &Path,
    dm_seq_nr: u32,
    table_desc: &super::TableDescContents,
    bound_cols: &[(usize, &super::PlainColumnEntry)],
    target_desc_idx: usize,
) -> Result<usize, StorageError> {
    let (variant, header) =
        super::read_tiled_header(&table_path.join(format!("table.f{dm_seq_nr}")))?;
    let (header, target) = super::tile_target(
        &variant,
        std::borrow::Cow::Owned(header),
        table_desc,
        bound_cols,
        target_desc_idx,
        (dm_seq_nr, &[]),
    )?;
    header.cubes.iter().try_fold(0, |maximum, cube| {
        if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() || cube.cube_shape.contains(&0) {
            return Ok(maximum);
        }
        let cell_manager = matches!(variant, super::TiledVariant::Cell { .. });
        let cell_axes = cube.cube_shape.len() - usize::from(!cell_manager);
        single_cell_buffers(
            &cube.cube_shape[..cell_axes],
            &cube.tile_shape,
            &header.col_data_types,
            target,
            cell_manager,
        )
        .map(|bytes| maximum.max(bytes))
    })
}

/// Scratch beside the decoded cell: recent shared tiles or one streamed tile
/// plus its file read-ahead. A CellStMan cell is an entire cube and always
/// uses shared tiles; other managers stream cells spanning more than 8 tiles.
fn single_cell_buffers(
    cell_shape: &[usize],
    tile_shape: &[usize],
    types: &[CasacoreDataType],
    target: usize,
    cell_manager: bool,
) -> Result<usize, StorageError> {
    let invalid = || {
        StorageError::FormatMismatch(
            "selected cell buffer size overflow or invalid tile shape".into(),
        )
    };
    if tile_shape.len() != cell_shape.len() + usize::from(!cell_manager) || tile_shape.contains(&0)
    {
        return Err(invalid());
    }
    let pixels = super::checked_storage_product(tile_shape, "selected cell tile")?;
    let tiles = cell_shape
        .iter()
        .zip(tile_shape)
        .try_fold(1usize, |n, (&cell, &tile)| {
            n.checked_mul(cell.div_ceil(tile)).ok_or_else(invalid)
        })?;
    let dt = *types.get(target).ok_or_else(invalid)?;
    let decoded = pixels
        .checked_mul(super::tile_element_size(dt))
        .ok_or_else(invalid)?;
    if cell_manager || tiles <= TILE_READ_SESSION_RECENT_TILE_COUNT {
        // Vec -> Arc briefly duplicates the newest tile. When the cache is
        // already full it evicts only after this allocation has completed.
        return decoded
            .checked_mul(tiles.min(TILE_READ_SESSION_RECENT_TILE_COUNT + 1) + 1)
            .ok_or_else(invalid);
    }
    let stored = |dt| {
        if dt == CasacoreDataType::TpBool {
            Ok(pixels.div_ceil(8))
        } else {
            pixels
                .checked_mul(super::tile_element_size(dt))
                .ok_or_else(invalid)
        }
    };
    let bucket = types.iter().try_fold(0usize, |n, &dt| {
        n.checked_add(stored(dt)?).ok_or_else(invalid)
    })?;
    let file_tile = stored(dt)?;
    let span = super::read_ahead_span(tiles, bucket, file_tile).ok_or_else(invalid)?;
    let ahead = span
        .min(super::STREAMING_TILED_READ_AHEAD_BYTES)
        .max(file_tile);
    decoded.checked_add(ahead).ok_or_else(invalid)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn load_selected_rows_from_touched_cubes(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    output_len: usize,
    patches_by_cube: BTreeMap<usize, Vec<SelectedCubeRow>>,
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let column = SelectedColumn {
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        col_desc,
        dt,
        elem_size,
    };
    let mut outputs = vec![None; output_len];
    for (cube_idx, patches) in patches_by_cube {
        let Some(cube) = header.cubes.get(cube_idx) else {
            continue;
        };
        if cube.file_seq_nr >= 0 && !cube.cube_shape.is_empty() {
            // A completed cube no longer needs its file's read-ahead buffer.
            let mut session = TileReadSession::default();
            column.read_cube(cube_idx, cube, &patches, &mut outputs, &mut session)?;
        }
    }
    Ok(outputs)
}

struct SelectedColumn<'a> {
    table_path: &'a Path,
    dm_seq_nr: u32,
    header: &'a TiledStManHeader,
    target_col_idx: usize,
    col_desc: &'a ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
}

impl SelectedColumn<'_> {
    fn read_cube(
        &self,
        cube_idx: usize,
        cube: &TsmCubeInfo,
        patches: &[SelectedCubeRow],
        outputs: &mut [Option<ArrayValue>],
        session: &mut TileReadSession,
    ) -> Result<(), StorageError> {
        let tiles = SelectedTiles::new(
            self.table_path,
            self.dm_seq_nr,
            self.header,
            self.target_col_idx,
            self.dt,
            cube_idx,
            cube,
        )?;
        let cell_axes = cube.cube_shape.len() - 1;
        let cell_shape = &cube.cube_shape[..cell_axes];
        let tile_shape = &cube.tile_shape[..cell_axes];
        let tile_row_bytes = tile_shape.iter().product::<usize>() * self.elem_size;
        let rows_by_tile = tiles.rows_by_tile(patches);
        if tile_shape == cell_shape {
            for (row_tile, rows) in rows_by_tile {
                let tile = tiles.shared(row_tile, session)?;
                for row in rows {
                    let start = tiles.row_offset(row) * self.elem_size;
                    outputs[row.out_idx] =
                        Some(self.decode(&tile[start..start + tile_row_bytes], cell_shape)?);
                }
            }
            return Ok(());
        }

        let count_per_axis: Vec<_> = cell_shape
            .iter()
            .zip(tile_shape)
            .map(|(&cell, &tile)| cell.div_ceil(tile))
            .collect();
        let tiles_per_cell = count_per_axis.iter().product::<usize>();
        let cell_bytes = cell_shape.iter().product::<usize>() * self.elem_size;
        let stream =
            rows_by_tile.len().saturating_mul(tiles_per_cell) > TILE_READ_SESSION_RECENT_TILE_COUNT;
        let mut streamed_tile = Vec::new();
        for (row_tile, rows) in rows_by_tile {
            // Assemble just this stripe. Each tile supplies all selected rows
            // it contains, even when one stripe exceeds the shared cache.
            let mut raw_cells = vec![vec![0; cell_bytes]; rows.len()];
            for cell_tile in 0..tiles_per_cell {
                let loaded = tiles.load(
                    row_tile * tiles_per_cell + cell_tile,
                    stream.then_some(tiles_per_cell - cell_tile),
                    session,
                    &mut streamed_tile,
                )?;
                let tile = loaded.as_deref().unwrap_or(&streamed_tile);
                let start: Vec<_> = linear_to_nd(cell_tile, &count_per_axis)
                    .into_iter()
                    .zip(tile_shape)
                    .map(|(index, &tile)| index * tile)
                    .collect();
                let extent: Vec<_> = cell_shape
                    .iter()
                    .zip(&start)
                    .zip(tile_shape)
                    .map(|((&cell, &start), &tile)| (cell - start).min(tile))
                    .collect();
                for (&row, raw) in rows.iter().zip(&mut raw_cells) {
                    let offset = tiles.row_offset(row) * self.elem_size;
                    copy_tile_to_cube(
                        &tile[offset..offset + tile_row_bytes],
                        tile_shape,
                        raw,
                        cell_shape,
                        &start,
                        &extent,
                        self.elem_size,
                    );
                }
            }
            for (row, raw) in rows.into_iter().zip(raw_cells) {
                outputs[row.out_idx] = Some(self.decode(&raw, cell_shape)?);
            }
        }
        Ok(())
    }

    fn decode(&self, raw: &[u8], shape: &[usize]) -> Result<ArrayValue, StorageError> {
        match decode_array_value(raw, shape, self.dt, SHARED_TILE_BIG_ENDIAN)? {
            Value::Array(array) => Ok(array),
            other => Err(StorageError::FormatMismatch(format!(
                "tiled array column {} decoded as non-array value {:?}",
                self.col_desc.col_name,
                other.kind()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_read_buffers_follow_physical_tiles() {
        let complex = [CasacoreDataType::TpComplex];
        assert_eq!(
            single_cell_buffers(&[1, 9], &[1, 1, 4_194_304], &complex, 0, false).unwrap(),
            64 << 20
        );
        // Bool file bytes are packed, while its decoded tile uses one byte/pixel.
        assert_eq!(
            single_cell_buffers(
                &[1, 9],
                &[1, 1, 8 << 20],
                &[CasacoreDataType::TpBool],
                0,
                false
            )
            .unwrap(),
            17 << 20
        );
        assert_eq!(
            single_cell_buffers(&[1, 8], &[1, 1, 8], &complex, 0, false).unwrap(),
            9 * 64
        );
        assert_eq!(
            single_cell_buffers(&[1, 9], &[1, 1, 8], &[CasacoreDataType::TpInt], 0, false).unwrap(),
            320
        );
        assert_eq!(
            single_cell_buffers(&[1, 100], &[1, 1], &complex, 0, true).unwrap(),
            10 * 8
        );
        assert!(single_cell_buffers(&[1, 9], &[1, 0, 8], &complex, 0, false).is_err());
        assert!(single_cell_buffers(&[1, 9], &[1, usize::MAX, 8], &complex, 0, false).is_err());
    }
}
